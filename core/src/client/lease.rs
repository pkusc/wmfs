//! 会话租约的持有方（design.md 3.4.4、8.7、3.5.6，M10）：心跳、0.8 × lease 自停、被驱逐之后换新会话。都在 MetaClient 上
//! （实现注记（M10）：M6 的会话心跳在 FileClient 里；没有 FileClient 的会话——`wmfs fs`、测试客户端——不发心跳，M10 起 30 s 后就会
//! 被驱逐，所以挪到这里，打开文件集合由 FileClient 登记 [`OpenFiles`] 提供）。
//!
//! - **心跳**：`session-hb` 线程每 3 s 一次（[`MetaClient::start_heartbeat`]），上报打开集合的增量（[`MetaClient::note`]；leader 要
//!   全集——换过 leader——时发全集，没有登记 OpenFiles = 没有打开的文件）；集群图的 map_epoch 变大了就拉新图（C6）。fail point
//!   `client.heartbeat.stop`（3.11）：这一次不发（模拟失联，自停时的续租也不发）。
//! - **自停**：距最近一次成功心跳（或 SessionOpen）的**发送时刻**超过 0.8 × lease → 元数据请求、数据请求（FileClient 的 `with_cap`）
//!   与句柄上的操作先等一次续租结果（[`MetaClient::gate`]：自己发心跳；几个线程同时等时只有一个在发），最多等 `--op-timeout`，
//!   之后 `EIO`。SIGSTOP 恢复后的第一个操作也这样（8.7）。
//! - **被驱逐**（任何请求回 `SESSION_EVICTED`）：开一个新会话（新的随机 id，[`MetaClient::reopen`]）。先让新操作都在 gate 上等，
//!   换上新会话号，再叫 FileClient 丢掉没提交的写（脏区与在途的）与 cap（[`OpenFiles::evicted`]）。之后旧句柄一律 `EIO`（句柄记着
//!   打开时的会话号）；旧会话写进的数据只在旧会话里发（[`MetaClient::read_in`]），换过会话就 `SESSION_EVICTED`、丢掉——不会在新会话
//!   里被写出、提交（M10 审查）；新打开的文件正常。先换会话号、后清理：清理之后，经旧句柄的写一定过不了"会话还是它的吗"这一关（核对
//!   与写进脏区在同一把 inode 锁里，`writeback.rs`）。日志"会话 X 已被驱逐……新会话 Y"（`wmfs dev wait session-evicted` 从挂载日志里
//!   找最后一个会话号，[`NEW_SESSION`]）。

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use wmfs_api::failpoint::fail_point_hit;

use super::meta_client::{MetaClient, new_session_id};
use crate::base::{ErrCode, clock, lock, log};
use crate::proto::meta::MetaReply;

/// 会话心跳的间隔（3.5.6）。
const HEARTBEAT_EVERY_MS: u64 = 3000;
/// 换了新会话时日志里新会话号前面的字样：`wmfs dev wait session-evicted` 按它从挂载日志里找最后一个会话号
/// （`cli/src/dev/mount.rs::last_session`），改措辞要一起改。
pub const NEW_SESSION: &str = "新会话 ";

/// 会话的打开文件集合与被驱逐之后的清理：FileClient 实现（[`MetaClient::set_open_files`]）。
pub trait OpenFiles: Send + Sync {
    /// 打开着的文件的全集（ino, 是否写打开）。
    fn open_set(&self) -> Vec<(u64, bool)>;
    /// 会话被驱逐了、新会话已经开好并换上了会话号（还没放行新操作）：丢掉没提交的写（脏区与在途的）与 cap，清内核缓存。
    fn evicted(&self);
}

/// 心跳要报的打开集合增量（3.5.6）。
#[derive(Default)]
pub(super) struct OpenDelta {
    changes: HashMap<u64, Option<bool>>, // ino → 现在的状态（Some(是否写打开) / None = 关掉了）：同一个 ino 只报最后的状态
    need_full: bool,
}

/// 租约的持有方状态（见文件头）。
#[derive(Default)]
pub(super) struct Lease {
    renewed_ms: AtomicU64, // 最近一次成功心跳（或 SessionOpen）的发送时刻（mono_ms；0 = 正在换会话，先别放行）
    lease_ms: AtomicU64,
    beating: AtomicBool,   // 有线程正在发心跳
    reopening: AtomicBool, // 有线程正在开新会话
    hb_started: AtomicBool,
    pub(super) closed: AtomicBool, // 已经 SessionClose：不再发心跳、也不再开新会话
}

impl Lease {
    /// 续上了：`sent` 是这次请求的发送时刻。
    pub(super) fn renewed(&self, sent: u64, lease_ms: u64) {
        self.lease_ms.store(lease_ms, Ordering::SeqCst);
        self.renewed_ms.store(sent, Ordering::SeqCst);
    }

    /// 还在 0.8 × lease 之内，而且没有在换会话。实现注记（M10 审查）：换会话的途中别的线程的心跳可能已用新会话续上，只看时刻的话
    /// 闸门会在清理（`OpenFiles::evicted`）做完之前放行，新会话写进的数据会被清理当成旧的丢掉。
    fn fresh(&self) -> bool {
        let at = self.renewed_ms.load(Ordering::SeqCst);
        let lease = self.lease_ms.load(Ordering::SeqCst);
        at != 0 && !self.reopening.load(Ordering::SeqCst) && clock::mono_ms().saturating_sub(at) <= lease * 4 / 5
    }
}

impl MetaClient {
    /// 起 `session-hb` 线程（只起一次；线程只持有 Weak，MetaClient 被丢弃后自己退出）。挂载（`FileClient::start`）、`wmfs fs` 与测试
    /// 客户端都起它。
    pub fn start_heartbeat(self: &Arc<Self>) {
        if self.lease.hb_started.swap(true, Ordering::SeqCst) {
            return;
        }
        let w = Arc::downgrade(self);
        let r = std::thread::Builder::new().name("session-hb".into()).spawn(move || {
            loop {
                std::thread::sleep(Duration::from_millis(HEARTBEAT_EVERY_MS));
                match w.upgrade() {
                    Some(c) => c.beat(),
                    None => return,
                }
            }
        });
        if let Err(e) = r {
            log::error("meta-client", format_args!("起线程 session-hb 失败：{e}（会话会因租约到期被驱逐）"));
        }
    }

    /// 登记打开文件集合的来源（FileClient）。
    pub fn set_open_files(&self, f: Weak<dyn OpenFiles>) {
        let _ = self.files.set(f);
    }

    /// 记下一个 ino 的打开状态变了（`None` = 关掉了）。同一个 ino 在一次心跳里只报最后的状态（"关掉、又打开"不能报成"打开、关掉"，
    /// 否则一个正打开着的已删文件会被 gc 当成没人打开）。
    pub fn note(&self, ino: u64, open: Option<bool>) {
        lock(&self.delta).changes.insert(ino, open);
    }

    /// 发一次心跳（见文件头）；别的线程正在发就不发。
    pub(super) fn beat(&self) {
        if self.lease.closed.load(Ordering::SeqCst) {
            return;
        }
        if fail_point_hit("client.heartbeat.stop") || self.lease.beating.swap(true, Ordering::SeqCst) {
            return; // fail point：模拟客户端失联
        }
        let (mut opened, mut closed) = (Vec::new(), Vec::new());
        let need_full = {
            let mut d = lock(&self.delta);
            for (ino, st) in std::mem::take(&mut d.changes) {
                match st {
                    Some(w) => opened.push((ino, w)),
                    None => closed.push(ino),
                }
            }
            d.need_full
        };
        let files = self.files.get().and_then(Weak::upgrade);
        let full = need_full.then(|| files.map(|f| f.open_set()).unwrap_or_default()); // 不拿着 delta 锁去锁 inode
        let sent = clock::mono_ms();
        match self.heartbeat(opened, closed, full) {
            Ok(MetaReply::Heartbeat { lease_ms, map_epoch, need_full_open_set }) => {
                self.lease.renewed(sent, lease_ms);
                lock(&self.delta).need_full = need_full_open_set;
                if map_epoch > self.map().map_epoch {
                    let _ = self.refresh_map();
                }
            }
            Ok(_) | Err(_) => lock(&self.delta).need_full = true, // 增量丢了：下一次发全集
        }
        self.lease.beating.store(false, Ordering::SeqCst);
    }

    /// 自停（见文件头）：租约还新鲜就直接放行；否则自己发心跳续租，等到续上（或换好新会话）为止，最多 `--op-timeout`，之后 `EIO`。
    pub fn gate(&self) -> Result<(), ErrCode> {
        let deadline = clock::mono_ms().saturating_add(self.op_timeout().as_millis() as u64);
        while !self.lease.fresh() {
            if clock::mono_ms() >= deadline {
                return Err(ErrCode::EIO);
            }
            self.beat();
            if !self.lease.fresh() {
                std::thread::sleep(Duration::from_millis(50));
            }
        }
        Ok(())
    }

    /// 会话 `old` 被驱逐了：开一个新会话（见文件头）。已经换过（别的线程先发现）或正在换就什么也不做。
    pub(super) fn reopen(&self, old: u64) {
        if self.session() != old
            || self.lease.closed.load(Ordering::SeqCst)
            || self.lease.reopening.swap(true, Ordering::SeqCst)
        {
            return;
        }
        self.lease.renewed_ms.store(0, Ordering::SeqCst); // 换好之前新操作都在 gate 上等
        let new = new_session_id();
        let sent = clock::mono_ms();
        match self.open_session(new) {
            Ok(lease_ms) => {
                *lock(&self.delta) = OpenDelta::default(); // 新会话在服务端没有打开的文件
                self.set_session(new); // 先换会话号、再清理（见文件头）
                if let Some(f) = self.files.get().and_then(Weak::upgrade) {
                    f.evicted();
                }
                self.lease.renewed(sent, lease_ms);
                let lost = "已打开的句柄此后一律返回 EIO，没提交的写丢失";
                log::warn("meta-client", format_args!("会话 {old:x} 已被驱逐（{lost}）；{NEW_SESSION}{new:x}"));
            }
            Err(e) => {
                self.lease.renewed_ms.store(sent, Ordering::SeqCst); // 不卡住别的操作：它们照旧拿到 SESSION_EVICTED（EIO）
                log::warn("meta-client", format_args!("会话 {old:x} 已被驱逐，开新会话失败：{e}（下一次请求再试）"));
            }
        }
        self.lease.reopening.store(false, Ordering::SeqCst);
    }
}
