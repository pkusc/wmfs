//! relatime 批量与后台线程的小工具（design.md 8.3、1.2 的 `wmfs mount` 线程表）：
//! - `atime`：每 5 s 把 relatime 队列按调用者分批发 `SetAtime`（尽力而为）。
//! - 会话的打开文件集合（[`OpenFiles`]）：心跳本身 M10 起在 MetaClient 上（`lease.rs`），FileClient 只提供打开集合（增量经
//!   `MetaClient::note` 交给 MetaClient，全集由心跳线程要的时候取），并在会话被驱逐之后清理（[`OpenFiles::evicted`]）：丢掉
//!   没提交的写——脏区与在途的（3.4.4："写回缓冲里未提交的数据丢失"；新句柄读自己的写时也看不到它们）——与全部 cap（旧会话的 cap
//!   已被数据节点拒绝），对已知 inode 调调用方登记的回调（FUSE 层在那里 `inval_inode` 清内核缓存，[`FileClient::on_evict`]）。旧句柄
//!   由 `FileClient::handle` 按会话号判成 `EIO`。
//!
//! 后台写回的线程（4 个写线程与 flusher）在 `flusher.rs`。后台线程只持有 Weak：FileClient 被丢弃后各自退出（[`every`]）。

use std::sync::{Arc, Weak};
use std::time::Duration;

use super::files::FileClient;
use super::inode::Cap;
use super::lease::OpenFiles;
use super::meta_client::Caller;
use crate::base::types::Ts;
use crate::base::{lock, log};
use crate::proto::meta::{MetaOp, SET_ATIME_MAX_ITEMS};

/// relatime 批量的间隔（8.3）。
const ATIME_EVERY_MS: u64 = 5000;

/// relatime 队列里的一项：哪个文件、读的时刻、谁读的（SetAtime 以他的身份发，服务端检查他能不能读）。
pub(super) struct AtimeItem {
    pub ino: u64,
    pub at: Ts,
    pub who: Caller,
}

/// 起 atime 线程，并把自己登记为会话的打开集合（`FileClient::start` 调用；心跳线程由 MetaClient 起）。
pub(super) fn start_threads(fc: &Arc<FileClient>) {
    let me = Arc::downgrade(fc);
    fc.meta.set_open_files(me); // Weak<FileClient> → Weak<dyn OpenFiles>
    fc.meta.start_heartbeat();
    let w = Arc::downgrade(fc);
    spawn("atime", move || every(&w, ATIME_EVERY_MS, |fc| fc.send_atimes()));
}

impl OpenFiles for FileClient {
    fn open_set(&self) -> Vec<(u64, bool)> {
        let m = lock(&self.inodes);
        m.values()
            .filter_map(|i| {
                let st = lock(&i.st);
                (st.readers + st.writers > 0).then_some((i.ino, st.writers > 0))
            })
            .collect()
    }

    fn evicted(&self) {
        let (mut inos, mut dropped, mut flying) = (Vec::new(), 0, 0);
        for inode in self.all_inodes() {
            let mut st = lock(&inode.st);
            dropped += std::mem::take(&mut st.dirty.chunks).len();
            // 在途的：槽位由写出它的线程放（它的请求只在旧会话里发，一定失败，`writeback.rs` 的 finish_flying）
            flying += std::mem::take(&mut st.dirty.flying).len();
            (st.rcap, st.wcap, st.write_err) = (Cap::default(), Cap::default(), None);
            st.unsynced.clear();
            inos.push(inode.ino);
        }
        self.slots.release(dropped);
        if dropped + flying > 0 {
            let n = dropped + flying;
            log::error(
                "client",
                format_args!("会话被驱逐：丢掉 {n} 个 chunk 没提交的写（旧句柄上的 write/fsync/close 报 EIO）"),
            );
        }
        if let Some(f) = self.evict_hook.get() {
            f(inos);
        }
    }
}

impl FileClient {
    /// 登记"会话被驱逐之后、对已知 inode 清内核缓存"的回调（FUSE 层：在别的线程上 `inval_inode`）。
    pub fn on_evict(&self, f: Box<dyn Fn(Vec<u64>) + Send + Sync>) {
        let _ = self.evict_hook.set(f);
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
