//! 客户端的锁（design.md 8.6、3.5.3、3.1.3 第 5 条；实现注记（M12））：FUSE 适配层的 getlk / setlk / flush / release 经这里发
//! `GetLock` / `SetLock`（只在句柄的会话里发：被驱逐的会话里打开的句柄 → `EIO`）。end 一律先归一（≥ `i64::MAX` → `LOCK_EOF`）。
//!
//! - **锁即同步点**：解锁与加读锁（含降级）之前，先把这个 inode 的脏数据写出去并提交——尽力而为：写出失败不挡锁操作（否则 close 时
//!   一失败，锁就再也解不掉），错误（含之前记下的）留给写者之后的 write/fsync/close 报告，不在这里"报掉"（NFS 的 do_unlk 也是刷完照样
//!   解锁）。拿到锁之后（调用方回复内核之前）经句柄重新取一次属性、layout 与 cap（`ReopenHandle`：别的挂载在放锁之前提交的写从此看得到；
//!   调用方再 `inval_inode` 丢内核的属性与页缓存）。
//! - **F_SETLKW 不在 FUSE 线程上等**（[`FileClient::wait_lock`]）：第一次 SetLock 冲突后，等待者连同"回复内核"的回调放进队列，FUSE 线程
//!   立即返回；`lock-wait` 线程（有等待者时才在）按退避（50 ms 起、×2、上限 1 s）重试。每次重试前读发起请求的线程的
//!   `/proc/<tid>/status`：已经不在、或挂着 SIGKILL（任何致命信号都会让内核给它挂上）→ 回 `EINTR`——它在内核里等我们回复才能退出，
//!   绝不把锁授给正在死去的进程；拿到锁 → 回调 Ok；别的错误 → 回调那个错误。不保证先来先得（放松 ⑭）。
//! - **释放**：本挂载记着哪些 (inode, owner) 拿过锁、最近一次经哪个句柄加的（[`Locks`]）。FUSE flush（每次 close，owner = 关掉 fd 的
//!   进程）→ 解掉这个 owner 的锁（POSIX 锁随 close 释放）；release（最后一个 close）→ 解掉记在这个句柄上的全部锁：flock 与 OFD 锁
//!   （`F_OFD_SETLK`）的 owner 都是 struct file，而 OFD 锁内核在 close 时既不经 FLUSH、也不在 RELEASE 里带 owner（实现注记（M12 审查））；
//!   POSIX 锁在这之前已经经 flush 解了。只对记着的发"解锁整个文件"，别的 close 不多发请求；会话被驱逐时清空（服务端在驱逐第二阶段释放
//!   它们，3.4.4）。

use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::time::Duration;

use super::files::{FileClient, Reopen};
use super::inode::Inode;
use super::meta_client::Caller;
use super::session::spawn;
use crate::base::{ErrCode, clock, lock, log};
use crate::meta::locks::{LOCK_READ, LOCK_UNLOCK, LOCK_WRITE};
use crate::meta::schema::records::LOCK_EOF;
use crate::proto::meta::{LockOut, MetaOp, MetaReply};

/// lock-wait 线程多久看一次到期的等待者；F_SETLKW 的重试退避从它起、×2、最长 [`MAX_MS`]（8.6）。
const TICK_MS: u64 = 50;
const MAX_MS: u64 = 1000;
/// `/proc/<tid>/status` 的 SigPnd / ShdPnd 位图里的第 9 号信号（SIGKILL）。
const SIGKILL_BIT: u64 = 1 << (9 - 1);

/// 一次 `SetLock` 的参数：`kind` 是 `SetLock.kind`（1 读 2 写 3 解锁），`pid` 是内核给的、写进锁记录。
#[derive(Clone, Copy, Debug)]
pub struct LockReq {
    pub owner: u64,
    pub start: u64,
    pub end: u64,
    pub kind: u8,
    pub pid: u32,
}

/// 一个被挂起的 F_SETLKW（8.2 的 `LockWaiter`；回复内核的那一半在回调里，core 不碰 fuser）。
pub struct LockWaiter {
    pub who: Caller,
    pub fh: u64,
    pub req: LockReq,
    pub tid: u32, // 发起请求的线程（读它的 /proc 判断是不是正在被杀）
    pub done: Box<dyn FnOnce(Result<(), ErrCode>) + Send>,
}

struct Waiting {
    w: LockWaiter,
    next_ms: u64,
    backoff: u64,
}

/// 本挂载的锁的状态。
#[derive(Default)]
pub struct Locks {
    held: HashMap<(u64, u64), u64>, // 拿过锁、还没整个解掉的 (inode, owner) → 最近一次经哪个句柄加的
    waiting: Vec<Waiting>,
    thread: bool, // lock-wait 线程在不在
}

impl FileClient {
    /// F_GETLK：第一个与 `r` 冲突的锁（没有 → None；`r.kind` 只看是不是写锁，`r.pid` 不用）。
    pub fn getlk(&self, who: &Caller, fh: u64, r: &LockReq) -> Result<Option<LockOut>, ErrCode> {
        let h = self.handle(fh)?;
        let (end, write) = (r.end.min(LOCK_EOF), r.kind == LOCK_WRITE);
        let op = MetaOp::GetLock { ino: h.ino, owner: r.owner, start: r.start, end, write };
        match self.meta.read_in(h.session, who, op)? {
            MetaReply::Lock { conflict } => Ok(conflict),
            _ => Err(ErrCode::INTERNAL),
        }
    }

    /// 一次非阻塞的 SetLock（见文件头的同步点）：冲突 → `EAGAIN`。
    pub fn setlk(&self, who: &Caller, fh: u64, r: &LockReq) -> Result<(), ErrCode> {
        let h = self.handle(fh)?;
        let inode = self.inode(h.ino).ok_or(ErrCode::EBADF)?;
        if r.kind != LOCK_WRITE {
            self.sync_point(&inode); // 解锁、加读锁之前先提交（锁即同步点；尽力而为）
        }
        let (ino, end) = (h.ino, r.end.min(LOCK_EOF));
        let op = MetaOp::SetLock { ino, owner: r.owner, pid: r.pid, start: r.start, end, kind: r.kind };
        self.meta.write_in(h.session, who, op)?;
        let mut l = lock(&self.locks);
        if r.kind != LOCK_UNLOCK {
            l.held.insert((ino, r.owner), fh);
        } else if r.start == 0 && end == LOCK_EOF {
            l.held.remove(&(ino, r.owner));
        }
        drop(l);
        if r.kind != LOCK_UNLOCK {
            let how = if h.write { Reopen::Handle } else { Reopen::Read };
            if let Err(e) = self.reopen(&inode, who, h.session, how) {
                log::warn("client", format_args!("ino {ino}：拿到锁之后重新取属性失败（{e}），读可能看到旧数据"));
            }
        }
        Ok(())
    }

    /// 锁即同步点（见文件头）：把这个 inode 的写入提交出去；失败（含之前记下的写错误）放回去，留给写者之后报告。
    fn sync_point(&self, inode: &Inode) {
        let pending = lock(&inode.st).write_err.take(); // 先拿开：这次照样试着写出去
        let r = self.flush_inode(inode, false, false);
        if let Some(e) = pending.or(r.err()).filter(|e| *e != ErrCode::SESSION_EVICTED) {
            lock(&inode.st).write_err.get_or_insert(e);
        }
    }

    /// FUSE flush（每次 close）：本挂载拿过 `owner`（关掉 fd 的进程）在这个文件上的锁才解锁整个文件（见文件头）。
    pub fn drop_locks(&self, fh: u64, owner: u64) {
        let Ok(h) = self.handle(fh) else { return };
        if lock(&self.locks).held.contains_key(&(h.ino, owner)) {
            self.unlock_on_close(fh, h.ino, owner);
        }
    }

    /// FUSE release（最后一个 close）：解掉记在这个句柄上的全部锁（flock、OFD 锁，见文件头）。
    pub fn release_locks(&self, fh: u64) {
        let mine: Vec<(u64, u64)> = lock(&self.locks).held.iter().filter(|(_, f)| **f == fh).map(|(k, _)| *k).collect();
        for (ino, owner) in mine {
            self.unlock_on_close(fh, ino, owner);
        }
    }

    /// close 时解锁整个文件：以挂载进程自己的身份发（内核的 RELEASE 不带调用者；解锁的授权只看会话与 owner）。
    fn unlock_on_close(&self, fh: u64, ino: u64, owner: u64) {
        let r = LockReq { owner, start: 0, end: LOCK_EOF, kind: LOCK_UNLOCK, pid: 0 };
        if let Err(e) = self.setlk(&self.process_caller(), fh, &r) {
            log::warn("client", format_args!("ino {ino}：close 时解锁失败：{e}（会话关闭或被驱逐时由服务端释放）"));
        }
    }

    /// 会话被驱逐之后：旧会话的锁由服务端释放，这里忘掉它们（`session.rs` 的 `evicted`）。
    pub(super) fn forget_locks(&self) {
        lock(&self.locks).held.clear();
    }

    /// F_SETLKW：交给 lock-wait 线程（见文件头）；`w.done` 恰好被调用一次。
    pub fn wait_lock(self: &Arc<Self>, w: LockWaiter) {
        let mut l = lock(&self.locks);
        l.waiting.push(Waiting { w, next_ms: clock::mono_ms() + TICK_MS, backoff: TICK_MS });
        if !l.thread {
            l.thread = true;
            let me = Arc::downgrade(self);
            spawn("lock-wait", move || lock_wait(me));
        }
    }
}

/// 内核的锁类型（`F_RDLCK` / `F_WRLCK` / `F_UNLCK`）→ `SetLock.kind`；不认识 → None。
pub fn setlock_kind(kernel_type: i32) -> Option<u8> {
    match kernel_type {
        libc::F_RDLCK => Some(LOCK_READ),
        libc::F_WRLCK => Some(LOCK_WRITE),
        libc::F_UNLCK => Some(LOCK_UNLOCK),
        _ => None,
    }
}

/// F_GETLK 回给内核的锁类型：冲突的锁是写锁吗；没有冲突 → `F_UNLCK`。
pub fn kernel_type(conflict_write: Option<bool>) -> i32 {
    match conflict_write {
        Some(true) => libc::F_WRLCK,
        Some(false) => libc::F_RDLCK,
        None => libc::F_UNLCK,
    }
}

/// lock-wait 线程：每 [`TICK_MS`] 看一次到期的等待者；没有等待者了就退出（下一个 F_SETLKW 再起一个）。
fn lock_wait(me: Weak<FileClient>) {
    loop {
        std::thread::sleep(Duration::from_millis(TICK_MS));
        let Some(fc) = me.upgrade() else { return };
        let now = clock::mono_ms();
        let due: Vec<Waiting> = {
            let mut l = lock(&fc.locks);
            if l.waiting.is_empty() {
                l.thread = false;
                return;
            }
            let (due, rest) = std::mem::take(&mut l.waiting).into_iter().partition(|x| x.next_ms <= now);
            l.waiting = rest;
            due
        };
        for mut x in due {
            let w = &x.w;
            let r = if dying(w.tid) { Err(ErrCode::EINTR) } else { fc.setlk(&w.who, w.fh, &w.req) };
            match r {
                Err(ErrCode::EAGAIN) => {
                    x.backoff = (x.backoff * 2).min(MAX_MS);
                    x.next_ms = clock::mono_ms() + x.backoff;
                    lock(&fc.locks).waiting.push(x);
                }
                r => (x.w.done)(r),
            }
        }
    }
}

/// 发起 F_SETLKW 的线程已经不在、或挂着 SIGKILL（8.6）。
fn dying(tid: u32) -> bool {
    let Ok(s) = std::fs::read_to_string(format!("/proc/{tid}/status")) else { return true };
    let pending = s.lines().filter_map(|l| l.strip_prefix("SigPnd:").or_else(|| l.strip_prefix("ShdPnd:")));
    pending.filter_map(|x| u64::from_str_radix(x.trim(), 16).ok()).any(|m| m & SIGKILL_BIT != 0)
}

#[cfg(test)]
mod tests {
    use super::dying;

    #[test]
    fn a_live_thread_is_not_dying_and_a_missing_one_is() {
        assert!(!dying(std::process::id()), "自己：活着、没挂着 SIGKILL");
        assert!(dying(u32::MAX - 7), "不存在的线程：当作已经不在了");
    }
}
