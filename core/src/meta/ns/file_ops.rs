//! 写入相关的元数据操作（design.md 3.3.4、3.5.1 的 CommitWrite / Fallocate 两行、3.5.5、ADR-0034）：CommitWrite（提交写入的
//! 长度、inline 补丁、清除 INLINE 的 CAS）、Fallocate（mode 0）、GetInline、RenewCap，以及服务端 uninline 的只读前半段
//! （`meta/uninline.rs` 用）。
//!
//! - **CommitWrite**：调用者要对文件有写权限；`epoch` 与当前 layout 不符 → `STALE_LAYOUT`（客户端重新 Open、按新 layout 重写，
//!   3.1.4 C4）；`new_size` 超过单个文件的最大长度（`common::max_file_size`）→ `EFBIG`；`size = max(size, new_size)`；mtime/ctime 取 leader 的时钟（放松 ⑫：请求里的 `mtime` 不用）；非属主写入时
//!   去掉 setuid/setgid；写 changelog `SetSize`（`extra` 写 `via=CommitWrite`）。
//!   - inline 文件的 `inline_patches`：按区间打补丁，补丁越过当前 INLINE 长度时中间补 0；补完 `size` 至少到补丁的末尾，所以
//!     `len(INLINE) ≤ size` 的不变式一直成立。补丁越过 `inline_max` → `EINVAL`（这次会长过阈值时客户端先 Uninline，ADR-0034）；
//!     文件已经不是 inline（别人先把它转成了 chunk）→ `STALE_LAYOUT`，客户端重新 Open、按 chunk 重写。
//!   - `uninline_expect_change_id = Some(c)`：文件仍是 inline 且 change_id 仍是 c 才清除 F_INLINE 与 INLINE（CAS），否则
//!     `STALE_LAYOUT`（服务端的 uninline 从头重做）。内容与大小都没变、只换了存放位置，所以不写 changelog（ADR-0034）。
//!   - `want_quota`、`close` 在 M10、M6 才有用（回复的 `quota_end` 先一律是 `u64::MAX`）。
//! - **Fallocate**：只支持 mode 0（别的 → `EOPNOTSUPP`）；`size = max(size, offset + len)`（超过最大长度 → `EFBIG`）；inline 文件的
//!   INLINE 不变（新增的区域按不变式读 0）。实现注记（M5）：规格没写回复，取 `Entry { attr }`；M6 的 FUSE 才有调用者。
//! - **GetInline**：只给能读这个文件的人（ADR-0033 第 11 条）；文件已经不是 inline → `STALE_LAYOUT`（重新 Open 取 layout）。
//! - **RenewCap**：按 `write` 重新检查读/写权限，回一张新 cap。RenewCap 与 Uninline 走只读路径：签 cap 之前先核对会话
//!   （`sessions::may_sign_cap`）。

use super::common::{
    Node, attr_out, authz, bump_ctime, get, is_file, kill_suid, max_file_size, may_read, open_out, save, sign_cap,
    write_rights,
};
use crate::base::ErrCode;
use crate::hooks::traits::AuthzOp;
use crate::meta::changelog::{self, Change};
use crate::meta::engine::txn::{KvRead, KvTxn, OpCtx};
use crate::meta::schema::keys;
use crate::meta::schema::records::F_INLINE;
use crate::meta::sessions;
use crate::proto::cap::{RIGHT_READ, RIGHT_WRITE};
use crate::proto::meta::{MetaReply, OpenOut};

/// 一次 CommitWrite 的参数（`MetaOp::CommitWrite` 里本里程碑用得到的部分）。
pub struct CommitIn {
    pub ino: u64,
    pub epoch: u32,
    pub new_size: u64,
    pub patches: Vec<(u64, Vec<u8>)>,
    pub expect_change_id: Option<u64>, // uninline_expect_change_id：Some = 清除 INLINE 的 CAS
}

/// 只有普通文件能被写：目录 → `EISDIR`，别的种类 → `EINVAL`；调用者要有写权限。
fn writable_file(v: &dyn KvRead, c: &OpCtx, n: &Node) -> Result<(), ErrCode> {
    if n.is_dir() {
        return Err(ErrCode::EISDIR);
    }
    if !is_file(n) {
        return Err(ErrCode::EINVAL);
    }
    authz(v, c, AuthzOp::Open { read: false, write: true }, Some(n), None, None)
}

/// 内容变了：mtime = ctime = now、change_id + 1；非属主（也不是 root）写入时去掉 setuid/setgid。**不写回**。
fn touch_data(n: &mut Node, c: &OpCtx) {
    n.rec.mtime = c.req.now;
    bump_ctime(n, c);
    let uid = c.req.who.uid;
    if uid != n.rec.uid && uid != 0 {
        kill_suid(n);
    }
}

pub fn commit_write(v: &mut dyn KvTxn, c: &OpCtx, w: &CommitIn) -> Result<MetaReply, ErrCode> {
    let mut n = get(v, w.ino)?;
    writable_file(v, c, &n)?;
    let l = n.rec.layout.clone().ok_or(ErrCode::EINVAL)?;
    if w.epoch != l.epoch {
        return Err(ErrCode::STALE_LAYOUT);
    }
    if w.new_size > max_file_size(&l) {
        return Err(ErrCode::EFBIG);
    }
    let inline = n.rec.flags & F_INLINE != 0;
    if let Some(cid) = w.expect_change_id {
        if !w.patches.is_empty() {
            return Err(ErrCode::EINVAL);
        }
        if !inline || n.rec.change_id != cid {
            return Err(ErrCode::STALE_LAYOUT); // CAS 失败：期间有人改过 inline 内容（或已经转过了）
        }
        n.rec.flags &= !F_INLINE;
        v.delete(&keys::inline(w.ino));
        bump_ctime(&mut n, c);
        save(v, &n);
        return Ok(MetaReply::Committed { attr: attr_out(&n), quota_end: u64::MAX });
    }
    let mut size = n.rec.size.max(w.new_size);
    if !w.patches.is_empty() {
        if !inline {
            return Err(ErrCode::STALE_LAYOUT);
        }
        let mut data = v.get(&keys::inline(w.ino))?.unwrap_or_default();
        for (off, bytes) in &w.patches {
            let end = off.checked_add(bytes.len() as u64).filter(|e| *e <= l.inline_max as u64);
            let end = end.ok_or(ErrCode::EINVAL)? as usize; // 越过 inline_max：客户端应先 Uninline
            if data.len() < end {
                data.resize(end, 0); // 补丁越过当前长度：中间补 0
            }
            data[*off as usize..end].copy_from_slice(bytes);
            size = size.max(end as u64);
        }
        v.put(&keys::inline(w.ino), data);
    } else if inline && w.new_size > n.rec.size.max(l.inline_max as u64) {
        return Err(ErrCode::EINVAL); // inline 文件没有 chunk：越过阈值的长度只能来自 Uninline 之后的写
    }
    n.rec.size = size;
    touch_data(&mut n, c);
    save(v, &n);
    let extra = vec![("via".to_string(), "CommitWrite".to_string())];
    changelog::append(v, c, Change { op: "SetSize", ino: w.ino, size, extra, ..Default::default() })?;
    Ok(MetaReply::Committed { attr: attr_out(&n), quota_end: u64::MAX })
}

pub fn fallocate(
    v: &mut dyn KvTxn,
    c: &OpCtx,
    ino: u64,
    offset: u64,
    len: u64,
    mode: u32,
) -> Result<MetaReply, ErrCode> {
    if mode != 0 {
        return Err(ErrCode::EOPNOTSUPP);
    }
    if len == 0 {
        return Err(ErrCode::EINVAL);
    }
    let mut n = get(v, ino)?;
    writable_file(v, c, &n)?;
    let max = n.rec.layout.as_ref().map_or(i64::MAX as u64, max_file_size);
    let end = offset.checked_add(len).filter(|e| *e <= max).ok_or(ErrCode::EFBIG)?;
    if end > n.rec.size {
        n.rec.size = end;
        touch_data(&mut n, c);
        save(v, &n);
        let extra = vec![("via".to_string(), "Fallocate".to_string())];
        changelog::append(v, c, Change { op: "SetSize", ino, size: end, extra, ..Default::default() })?;
    }
    Ok(MetaReply::Entry { attr: attr_out(&n) })
}

pub fn get_inline(v: &dyn KvRead, c: &OpCtx, ino: u64) -> Result<MetaReply, ErrCode> {
    let n = get(v, ino)?;
    if !is_file(&n) {
        return Err(if n.is_dir() { ErrCode::EISDIR } else { ErrCode::EINVAL });
    }
    authz(v, c, AuthzOp::Open { read: true, write: false }, Some(&n), None, None)?;
    if n.rec.flags & F_INLINE == 0 {
        return Err(ErrCode::STALE_LAYOUT);
    }
    let data = v.get(&keys::inline(ino))?.unwrap_or_default();
    Ok(MetaReply::Inline { data, change_id: n.rec.change_id })
}

pub fn renew_cap(v: &dyn KvRead, c: &OpCtx, ino: u64, write: bool) -> Result<MetaReply, ErrCode> {
    let n = get(v, ino)?;
    if !is_file(&n) {
        return Err(if n.is_dir() { ErrCode::EISDIR } else { ErrCode::EINVAL });
    }
    authz(v, c, AuthzOp::Open { read: !write, write }, Some(&n), None, None)?;
    sessions::may_sign_cap(v, c.req)?;
    let rights = if write { write_rights(may_read(v, c, &n)) } else { RIGHT_READ };
    Ok(MetaReply::Cap { cap: sign_cap(c, &n, rights, c.req.session) })
}

/// 服务端 uninline 要做的事（ADR-0034 第 3 条第 1 步的结果）。
pub struct UninlineTodo {
    pub change_id: u64,
    pub content: Vec<u8>,
    pub cap: Vec<u8>, // leader 自己签的写 cap（session 0，不属于任何会话）
}

/// 服务端 uninline 的只读前半段（线性一致读之后）：调用者要有写权限；回给调用者的 OpenOut（写 cap，能读时也带读权限；
/// 不带 inline 内容——它可能只有写权限）；文件仍是 inline 时另给出要搬的内容。
pub fn uninline_prepare(v: &dyn KvRead, c: &OpCtx, ino: u64) -> Result<(OpenOut, Option<UninlineTodo>), ErrCode> {
    let n = get(v, ino)?;
    writable_file(v, c, &n)?;
    sessions::may_sign_cap(v, c.req)?;
    let open = open_out(v, c, &n, false, write_rights(may_read(v, c, &n)))?;
    if n.rec.flags & F_INLINE == 0 {
        return Ok((open, None)); // 已经不是 inline：幂等，直接成功
    }
    let content = v.get(&keys::inline(ino))?.unwrap_or_default();
    let todo = UninlineTodo { change_id: n.rec.change_id, content, cap: sign_cap(c, &n, RIGHT_WRITE, 0) };
    Ok((open, Some(todo)))
}
