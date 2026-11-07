//! 字节范围锁与 flock（design.md 3.5.3，键空间 0x0A；实现注记（M12））：锁是**复制状态**，每个 inode 一条 `LockTableV1`（按
//! (start, session, owner) 排序），会话记录里记着它持有锁的 inode（`SessionRecV1.locked_inos`：会话关闭与驱逐第二阶段按它点删，不扫描）。
//!
//! - **所有者** = (会话, owner)；owner 是内核给的 lock owner（POSIX 锁是进程的 files_struct，flock 是 struct file）。
//! - **区间** `[start, end]` 闭区间；"到文件尾" = [`LOCK_EOF`]（`i64::MAX`，与内核的 `OFFSET_MAX` 一致）：≥ 它的 end 一律归一为它。
//! - **冲突**：区间相交 ∧ 至少一方是写锁 ∧ 所有者不同（[`conflict`]）。
//! - **`SetLock` 永远非阻塞**（[`set_lock`]）：有冲突 → `EAGAIN`（与别的 POSIX 错误一样写去重记录）；否则删掉/切开同一所有者在这个区间里的
//!   旧锁、插入新锁、合并相交或相邻的同类锁（[`apply`]，纯函数，单元测试与逐字节的暴力模型对比）。`F_SETLKW` 由客户端轮询（3.5.3），服务端
//!   没有等待队列。解锁永远成功。
//! - **`GetLock`**（只读路径，[`get_lock`]）：第一个与请求冲突的锁（区间、类型、pid）。
//! - 授权（ADR-0037 第 11 条）：`AuthzOp::Handle { read: false, write: false }`——"经已打开的句柄"、不看读写位：fcntl 锁要求 fd 的打开方式
//!   由内核检查（读锁要读打开、写锁要写打开），flock 本来就不要求，fuser 又不区分两者（3.5.3）；path-caps 照样限制子树。
//! - **释放**：显式解锁；客户端在 FUSE flush（POSIX 锁）与 release（flock）时解锁；会话关闭与驱逐第二阶段（[`release_session`]）；
//!   孤儿被 gc 删掉时连同 LOCKS 一起删（`gc_meta::finish_orphan`）。

use crate::base::ErrCode;
use crate::hooks::traits::AuthzOp;
use crate::meta::engine::txn::{KvRead, KvTxn, OpCtx};
use crate::meta::ns::common::{authz, get};
use crate::meta::schema::keys;
use crate::meta::schema::kv;
use crate::meta::schema::records::{LOCK_EOF, LockRecV1, LockTableV1, SessionRecV1};
use crate::proto::meta::{LockOut, MetaReply};

/// `SetLock.kind` 的取值（3.3.4：1 读 2 写 3 解锁）。
pub const LOCK_READ: u8 = 1;
pub const LOCK_WRITE: u8 = 2;
pub const LOCK_UNLOCK: u8 = 3;

fn same_owner(a: &LockRecV1, b: &LockRecV1) -> bool {
    a.session == b.session && a.owner == b.owner
}

fn overlaps(a: &LockRecV1, b: &LockRecV1) -> bool {
    a.start <= b.end && b.start <= a.end
}

/// 和 `req` 冲突的第一个锁（见文件头）。
fn conflict<'a>(locks: &'a [LockRecV1], req: &LockRecV1) -> Option<&'a LockRecV1> {
    locks.iter().find(|l| !same_owner(l, req) && overlaps(l, req) && (l.write || req.write))
}

/// 加锁（`unlock = false`）或解锁之后的锁表（见文件头）。不变式：同一所有者的锁互不相交，同类的也不相邻（相邻的已合并）。
fn apply(locks: &[LockRecV1], req: &LockRecV1, unlock: bool) -> Result<Vec<LockRecV1>, ErrCode> {
    if !unlock && conflict(locks, req).is_some() {
        return Err(ErrCode::EAGAIN);
    }
    let (mut out, mut new) = (Vec::with_capacity(locks.len() + 2), req.clone());
    for l in locks {
        if !same_owner(l, req) {
            out.push(l.clone());
            continue;
        }
        let touches = l.end.checked_add(1) == Some(req.start) || req.end.checked_add(1) == Some(l.start);
        if !unlock && l.write == req.write && (overlaps(l, req) || touches) {
            (new.start, new.end) = (new.start.min(l.start), new.end.max(l.end)); // 同类、相交或相邻：并进新锁
        } else if !overlaps(l, req) {
            out.push(l.clone());
        } else {
            // 相交的别类锁（或解锁）：留下请求区间两边的部分（l.start < req.start ⇒ req.start ≥ 1；l.end > req.end ⇒ req.end < LOCK_EOF）
            if l.start < req.start {
                out.push(LockRecV1 { end: req.start - 1, ..l.clone() });
            }
            if l.end > req.end {
                out.push(LockRecV1 { start: req.end + 1, ..l.clone() });
            }
        }
    }
    if !unlock {
        out.push(new);
    }
    out.sort_by_key(|l| (l.start, l.session, l.owner));
    Ok(out)
}

/// 一个 inode 的锁表（没有 = 空）。
fn table(v: &dyn KvRead, ino: u64) -> Result<Vec<LockRecV1>, ErrCode> {
    Ok(kv::get::<LockTableV1>(v, &keys::locks(ino))?.map_or(vec![], |t| t.locks))
}

fn save(v: &mut dyn KvTxn, ino: u64, locks: Vec<LockRecV1>) {
    if locks.is_empty() { v.delete(&keys::locks(ino)) } else { kv::put(v, &keys::locks(ino), &LockTableV1 { locks }) }
}

/// 请求里的区间：end 归一（≥ `i64::MAX` → [`LOCK_EOF`]），start 不能大于 end（`EINVAL`）。
fn range(start: u64, end: u64) -> Result<(u64, u64), ErrCode> {
    let end = end.min(LOCK_EOF);
    if start > end { Err(ErrCode::EINVAL) } else { Ok((start, end)) }
}

/// 一次 `SetLock` 的参数（`MetaOp::SetLock` 的字段）。
pub struct SetLockIn {
    pub ino: u64,
    pub owner: u64,
    pub pid: u32,
    pub start: u64,
    pub end: u64,
    pub kind: u8,
}

/// 执行器闭包：`SetLock`（见文件头）。会话记录的 `locked_inos` 同批维护（这个会话在这个 inode 上还有没有锁）。
pub fn set_lock(v: &mut dyn KvTxn, c: &OpCtx, r: &SetLockIn) -> Result<MetaReply, ErrCode> {
    let n = get(v, r.ino)?;
    authz(v, c, AuthzOp::Handle { read: false, write: false }, Some(&n), None, None)?;
    if !(LOCK_READ..=LOCK_UNLOCK).contains(&r.kind) {
        return Err(ErrCode::EINVAL);
    }
    let (start, end) = range(r.start, r.end)?;
    let sid = c.req.session;
    let req = LockRecV1 { session: sid, owner: r.owner, pid: r.pid, start, end, write: r.kind == LOCK_WRITE };
    let locks = apply(&table(v, r.ino)?, &req, r.kind == LOCK_UNLOCK)?;
    let mine = locks.iter().any(|l| l.session == sid);
    save(v, r.ino, locks);
    let key = keys::session(sid);
    let mut s: SessionRecV1 = kv::get(v, &key)?.ok_or(ErrCode::SESSION_EVICTED)?;
    if mine != s.locked_inos.contains(&r.ino) {
        if mine {
            s.locked_inos.push(r.ino)
        } else {
            s.locked_inos.retain(|i| *i != r.ino)
        }
        kv::put(v, &key, &s);
    }
    Ok(MetaReply::Ok)
}

/// 只读路径：`GetLock`（见文件头）。回复前断言 `start ≤ end ≤ LOCK_EOF`（3.5.3：内核看到 end > OFFSET_MAX 会直接回 EIO）。
pub fn get_lock(
    v: &dyn KvRead,
    c: &OpCtx,
    ino: u64,
    owner: u64,
    start: u64,
    end: u64,
    write: bool,
) -> Result<MetaReply, ErrCode> {
    let n = get(v, ino)?;
    authz(v, c, AuthzOp::Handle { read: false, write: false }, Some(&n), None, None)?;
    let (start, end) = range(start, end)?;
    let req = LockRecV1 { session: c.req.session, owner, pid: 0, start, end, write };
    let locks = table(v, ino)?;
    let conflict = conflict(&locks, &req).map(|l| LockOut { start: l.start, end: l.end, write: l.write, pid: l.pid });
    assert!(conflict.as_ref().is_none_or(|l| l.start <= l.end && l.end <= LOCK_EOF), "不变量破坏：锁表里的区间越界");
    Ok(MetaReply::Lock { conflict })
}

/// 释放会话 `sid` 在 `inos` 上的全部锁（会话关闭、驱逐第二阶段；点操作）。
pub fn release_session(v: &mut dyn KvTxn, sid: u64, inos: &[u64]) -> Result<(), ErrCode> {
    for ino in inos {
        let t = table(v, *ino)?;
        if t.iter().any(|l| l.session == sid) {
            save(v, *ino, t.into_iter().filter(|l| l.session != sid).collect());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
