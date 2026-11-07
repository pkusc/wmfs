//! FUSE 的 getlk / setlk（design.md 8.6、3.5.3；实现注记（M12））：集群范围的 fcntl 锁与 flock（init 协商了 `FUSE_POSIX_LOCKS` 与
//! `FUSE_FLOCK_LOCKS`；fuser 0.18 不把 `FUSE_LK_FLOCK` 传上来，两种锁共用一个锁空间，放松 ②）。
//!
//! - `getlk` → `GetLock`：第一个冲突的锁原样回给内核（end 是 `LOCK_EOF` = 内核的 `OFFSET_MAX`，内核据此回 `l_len = 0`）；没有冲突 → `F_UNLCK`。
//! - `setlk(sleep = false)` → 一次 `SetLock`（冲突 `EAGAIN`）；`setlk(sleep = true)`（F_SETLKW）先试一次，冲突就把回复交给 lock-wait 线程
//!   （`client/locks.rs`），**FUSE 线程立即返回**——等锁的进程再多，挂载点上别的请求照样有线程处理。
//! - 拿到锁 → 先 `inval_inode(ino, 0, 0)`（丢掉内核的属性与页缓存：锁即同步点），再回复内核。在 setlk 回调里（非阻塞锁）与 lock-wait 线程上
//!   （阻塞锁）这样做都不会与内核的锁交互出问题：内核在 fcntl/flock 的 SETLK 请求上不持有 inode 锁与页锁（8.6 要求的测试在
//!   `cli/tests/fuse_locks.rs`）。

use fuser::{INodeNo, ReplyEmpty, ReplyLock, Request};
use wmfs_core::base::ErrCode;
use wmfs_core::client::locks::{LockReq, LockWaiter, kernel_type};
use wmfs_core::meta::locks::LOCK_UNLOCK;

use super::WmfsFs;
use super::util::errno;

impl WmfsFs {
    /// `r.kind` 已经由内核的锁类型换好（`setlock_kind`；F_GETLK 只问读锁或写锁）。
    pub(super) fn do_getlk(&self, req: &Request, fh: u64, r: LockReq, reply: ReplyLock) {
        match self.fc.getlk(&self.who(req), fh, &r) {
            Ok(Some(l)) => reply.locked(l.start, l.end, kernel_type(Some(l.write)), l.pid),
            Ok(None) => reply.locked(0, 0, kernel_type(None), 0),
            Err(e) => reply.error(errno(e)),
        }
    }

    /// `r.kind` 已经由内核的锁类型换好（`setlock_kind`）。
    pub(super) fn do_setlk(&self, req: &Request, ino: INodeNo, fh: u64, r: LockReq, sleep: bool, reply: ReplyEmpty) {
        let notifier = self.notifier.clone();
        let done = move |res: Result<(), ErrCode>| match res {
            Ok(()) => {
                if r.kind != LOCK_UNLOCK
                    && let Some(n) = notifier.get()
                {
                    let _ = n.inval_inode(ino, 0, 0); // 锁即同步点（见文件头）
                }
                reply.ok()
            }
            Err(e) => reply.error(errno(e)),
        };
        let who = self.who(req);
        match self.fc.setlk(&who, fh, &r) {
            Err(ErrCode::EAGAIN) if sleep => {
                let w = LockWaiter { who, fh, req: r, tid: req.pid(), done: Box::new(done) };
                self.fc.wait_lock(w); // FUSE 线程不等（见文件头）
            }
            res => done(res),
        }
    }
}
