//! 会话心跳与 relatime 批量（design.md 3.5.6、8.3、8.7、1.2 的 `wmfs mount` 线程表）：
//! - `session-hb`：每 3 s 会话心跳，上报打开集合的增量（leader 要全集——换过 leader——时发全集）；集群图的 map_epoch 变大了就拉一张
//!   新的（C6）。fail point `client.heartbeat.stop`（3.11）：这一轮不发心跳（M10 的驱逐测试模拟客户端失联）。
//! - `atime`：每 5 s 把 relatime 队列按调用者分批发 `SetAtime`（尽力而为）。
//!
//! 后台写回的线程（4 个写线程与 flusher）在 `flusher.rs`。后台线程只持有 Weak：FileClient 被丢弃后各自退出（[`every`]）。

use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::time::Duration;

use wmfs_api::failpoint::fail_point_hit;

use super::files::FileClient;
use super::meta_client::Caller;
use crate::base::types::Ts;
use crate::base::{lock, log};
use crate::proto::meta::{MetaOp, MetaReply, SET_ATIME_MAX_ITEMS};

/// 会话心跳的间隔（3.5.6）。
const HEARTBEAT_EVERY_MS: u64 = 3000;
/// relatime 批量的间隔（8.3）。
const ATIME_EVERY_MS: u64 = 5000;

/// 心跳要报的打开集合增量（3.5.6）。
#[derive(Default)]
pub(super) struct OpenDelta {
    changes: HashMap<u64, Option<bool>>, // ino → 现在的状态（Some(是否写打开) / None = 关掉了）：同一个 ino 只报最后的状态
    need_full: bool,
}

/// relatime 队列里的一项：哪个文件、读的时刻、谁读的（SetAtime 以他的身份发，服务端检查他能不能读）。
pub(super) struct AtimeItem {
    pub ino: u64,
    pub at: Ts,
    pub who: Caller,
}

/// 起 session-hb 与 atime 线程（`FileClient::start` 调用）。
pub(super) fn start_threads(fc: &Arc<FileClient>) {
    let w = Arc::downgrade(fc);
    spawn("session-hb", move || every(&w, HEARTBEAT_EVERY_MS, |fc| fc.heartbeat()));
    let w = Arc::downgrade(fc);
    spawn("atime", move || every(&w, ATIME_EVERY_MS, |fc| fc.send_atimes()));
}

impl FileClient {
    /// 记下一个 ino 的打开状态变了。服务端先加 opened、再删 closed，所以同一个 ino 在一次心跳里只能出现在其中一边——
    /// 只留最后的状态（"关掉、又打开"不能报成"打开、关掉"，否则一个正打开着的已删文件会被 gc 当成没人打开）。
    pub(super) fn note(&self, ino: u64, open: Option<bool>) {
        lock(&self.delta).changes.insert(ino, open);
    }

    /// session-hb：上报打开集合增量（leader 要全集时发全集）；集群图的 map_epoch 变大了就拉一张新的（C6）。
    fn heartbeat(&self) {
        if fail_point_hit("client.heartbeat.stop") {
            return; // M10 的驱逐测试用：模拟客户端失联
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
        let full = need_full.then(|| self.open_set()); // 不拿着 delta 锁去锁 inode（register 是先 inode 后 delta）
        match self.meta.heartbeat(opened, closed, full) {
            Ok(MetaReply::Heartbeat { map_epoch, need_full_open_set, .. }) => {
                lock(&self.delta).need_full = need_full_open_set;
                if map_epoch > self.meta.map().map_epoch {
                    let _ = self.meta.refresh_map();
                }
            }
            Ok(_) | Err(_) => lock(&self.delta).need_full = true, // 增量丢了：下一次发全集
        }
    }

    fn open_set(&self) -> Vec<(u64, bool)> {
        let m = lock(&self.inodes);
        m.values()
            .filter_map(|i| {
                let st = lock(&i.st);
                (st.readers + st.writers > 0).then_some((i.ino, st.writers > 0))
            })
            .collect()
    }

    /// atime 线程：把排队的 relatime 更新按调用者分批发出去（尽力而为，失败只写日志）。
    fn send_atimes(&self) {
        let mut items = std::mem::take(&mut *lock(&self.atime));
        items.sort_by_key(|a| (a.who.uid, a.who.gid, a.ino));
        items.dedup_by_key(|a| (a.who.uid, a.who.gid, a.ino));
        for group in items.chunk_by(|a, b| (a.who.uid, a.who.gid) == (b.who.uid, b.who.gid)) {
            for part in group.chunks(SET_ATIME_MAX_ITEMS) {
                let batch = part.iter().map(|a| (a.ino, a.at)).collect();
                if let Err(e) = self.meta.write(&part[0].who, MetaOp::SetAtime { items: batch }) {
                    log::info("client", format_args!("SetAtime 失败（尽力而为，丢掉这一批）：{e}"));
                }
            }
        }
    }
}

/// 起一个有名字的后台线程（起不来只写日志：少一个后台线程不影响前台的读写）。
pub(super) fn spawn(name: &str, f: impl FnOnce() + Send + 'static) {
    if let Err(e) = std::thread::Builder::new().name(name.into()).spawn(f) {
        log::error("client", format_args!("起线程 {name} 失败：{e}"));
    }
}

/// 每隔 `ms` 做一次 `f`，直到 FileClient 被丢弃。
pub(super) fn every(w: &Weak<FileClient>, ms: u64, f: impl Fn(&FileClient)) {
    loop {
        std::thread::sleep(Duration::from_millis(ms));
        match w.upgrade() {
            Some(fc) => f(&fc),
            None => return,
        }
    }
}
