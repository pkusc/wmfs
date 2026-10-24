//! 写入相关的元数据操作（design.md 3.3.4、3.5.1 的 CommitWrite / Fallocate 两行、3.5.5、ADR-0034）：CommitWrite（提交写入的
//! 长度、inline 补丁、清除 INLINE 的 CAS）、Fallocate（mode 0）、GetInline、RenewCap，以及服务端 uninline 的只读前半段
//! （`meta/uninline.rs` 用）。
//!
//! - **CommitWrite**：调用者要能经写句柄写它（`AuthzOp::Handle`，ADR-0036：属主不看权限位，其他人按写权限）；`epoch` 与当前 layout 不符 → `STALE_LAYOUT`（客户端重新 Open、按新 layout 重写，
//!   3.1.4 C4）；`new_size` 超过单个文件的最大长度（`common::max_file_size`）→ `EFBIG`；`size = max(size, new_size)`；mtime/ctime 取 leader 的时钟（放松 ⑫：请求里的 `mtime` 不用）；非属主写入时
//!   去掉 setuid/setgid；写 changelog `SetSize`（`extra` 写 `via=CommitWrite`）。
//!   - inline 文件的 `inline_patches`：按区间打补丁，补丁越过当前 INLINE 长度时中间补 0；补完 `size` 至少到补丁的末尾，所以
//!     `len(INLINE) ≤ size` 的不变式一直成立。补丁越过 `inline_max` → `EINVAL`（这次会长过阈值时客户端先 Uninline，ADR-0034）；
//!     文件已经不是 inline（别人先把它转成了 chunk）→ `STALE_LAYOUT`，客户端重新 Open、按 chunk 重写。
//!   - `uninline_expect_change_id = Some(c)`：文件仍是 inline 且 change_id 仍是 c 才清除 F_INLINE 与 INLINE（CAS），否则
//!     `STALE_LAYOUT`（服务端的 uninline 从头重做）。内容与大小都没变、只换了存放位置，所以不写 changelog（ADR-0034）。
//!   - `want_quota` 在 M10 才有用（回复的 `quota_end` 先一律是 `u64::MAX`）；`close`（M6 起客户端在写句柄 close 时带上）服务端
//!     还不用（拿它做什么规格没写，实现注记（M6）：等主管决定，3.3.4 的 M6 注记 ③）。
//! - 实现注记（M7）：CommitWrite 与 Fallocate 改了长度 → 祖先 RSTAT 按差值更新（3.9；孤儿不计）。
//! - **Fallocate**：经句柄（`AuthzOp::Handle`）；只支持 mode 0（别的 → `EOPNOTSUPP`）；`size = max(size, offset + len)`（超过最大长度 → `EFBIG`）；inline 文件的
//!   INLINE 不变（新增的区域按不变式读 0）。实现注记（M5）：规格没写回复，取 `Entry { attr }`；M6 的 FUSE 才有调用者。
//! - **GetInline**：只给能读这个文件的人（ADR-0033 第 11 条；它不是句柄操作，按当前的读权限）；文件已经不是 inline →
//!   `STALE_LAYOUT`（重新 Open 取 layout）。
//! - **RenewCap**：经已打开的句柄续一张新 cap（`AuthzOp::Handle`：属主不看权限位，其他人按 `write` 检查读/写权限）。RenewCap 与 Uninline 走只读路径：签 cap 之前先核对会话
//!   （`sessions::may_sign_cap`）。
//! - **服务端 uninline 的意图与切换**（M8，ADR-0035 第 11 条：换 leader 之后旧 leader 上没做完的 uninline 的数据面 fencing，与 3.10
//!   迁移原语第 2–6 步同一个模式）：[`uninline_intent`] 先提交一个新 epoch（`max_epoch += 1`、`pending_epoch = max_epoch`；以前没做完的
//!   `pending_epoch` 记为垃圾，不复用），uninline 把内容写进这个 epoch 的 chunk；[`uninline_switch`] 再 CAS（change_id、pending_epoch
//!   都没变）切到它。旧 leader 迟到的写只会落进作废的 epoch，没人读，由 gc 清理。空内容没有东西要搬：不提意图，直接切换（epoch 不变）。
//! - **ReopenHandle**（M7，ADR-0037 第 4、12 条）：`Open` 的"经已打开的句柄"版本——layout 变了（3.1.4 C4）之后写句柄用它重新取
//!   layout 与 cap：授权 `AuthzOp::Handle { read: !write, write }`（属主不看权限位）、只读路径、签 cap 之前核对会话，回 `Opened`；
//!   cap 的权限位与 RenewCap 相同（写：能读时再加读），inline 内容只给能读的人（ADR-0033 第 11 条）。

use super::common::{
    Node, attr_out, authz, bump_ctime, get, is_file, kill_suid, max_file_size, may_read, open_out, save, sign_cap,
    write_rights,
};
use crate::base::ErrCode;
use crate::base::types::Ts;
use crate::hooks::traits::AuthzOp;
use crate::meta::changelog::{self, Change};
use crate::meta::engine::txn::{KvRead, KvTxn, OpCtx};
use crate::meta::rstat::{self, Delta};
use crate::meta::schema::keys::{self, GC_EPOCH};
use crate::meta::schema::kv;
use crate::meta::schema::records::{F_INLINE, GcRecV1, LayoutV1};
use crate::meta::sessions;
use crate::proto::cap::{RIGHT_READ, RIGHT_WRITE};
use crate::proto::meta::{MetaReply, OpenOut, SET_ATIME_MAX_ITEMS};

/// 一次 CommitWrite 的参数（`MetaOp::CommitWrite` 里本里程碑用得到的部分）。
pub struct CommitIn {
    pub ino: u64,
    pub epoch: u32,
    pub new_size: u64,
    pub patches: Vec<(u64, Vec<u8>)>,
    pub expect_change_id: Option<u64>, // uninline_expect_change_id：Some = 清除 INLINE 的 CAS
}

/// 只有普通文件能被写：目录 → `EISDIR`，别的种类 → `EINVAL`；调用者要能**经已打开的写句柄**写它（`AuthzOp::Handle`，
/// ADR-0036：属主不看权限位，其他人与写打开相同）。
fn writable_file(v: &dyn KvRead, c: &OpCtx, n: &Node) -> Result<(), ErrCode> {
    if n.is_dir() {
        return Err(ErrCode::EISDIR);
    }
    if !is_file(n) {
        return Err(ErrCode::EINVAL);
    }
    authz(v, c, AuthzOp::Handle { read: false, write: true }, Some(n), None, None)
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
        return Ok(MetaReply::Committed { attr: attr_out(v, c, &n)?, quota_end: u64::MAX });
    }
    let before = Delta::of(v, &n)?; // 递归计数按长度的差值更新（3.9）
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
    rstat::changed(v, c, &before, &n)?;
    let extra = vec![("via".to_string(), "CommitWrite".to_string())];
    changelog::append(v, c, Change { op: "SetSize", ino: w.ino, size, extra, ..Default::default() })?;
    Ok(MetaReply::Committed { attr: attr_out(v, c, &n)?, quota_end: u64::MAX })
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
        let before = Delta::of(v, &n)?;
        n.rec.size = end;
        touch_data(&mut n, c);
        save(v, &n);
        rstat::changed(v, c, &before, &n)?;
        let extra = vec![("via".to_string(), "Fallocate".to_string())];
        changelog::append(v, c, Change { op: "SetSize", ino, size: end, extra, ..Default::default() })?;
    }
    Ok(MetaReply::Entry { attr: attr_out(v, c, &n)? })
}

/// 普通文件 `ino`：目录 → `EISDIR`，别的种类 → `EINVAL`（GetInline、RenewCap、ReopenHandle）。
fn file_node(v: &dyn KvRead, ino: u64) -> Result<Node, ErrCode> {
    let n = get(v, ino)?;
    if !is_file(&n) {
        return Err(if n.is_dir() { ErrCode::EISDIR } else { ErrCode::EINVAL });
    }
    Ok(n)
}

pub fn get_inline(v: &dyn KvRead, c: &OpCtx, ino: u64) -> Result<MetaReply, ErrCode> {
    let n = file_node(v, ino)?;
    authz(v, c, AuthzOp::Open { read: true, write: false }, Some(&n), None, None)?;
    if n.rec.flags & F_INLINE == 0 {
        return Err(ErrCode::STALE_LAYOUT);
    }
    let data = v.get(&keys::inline(ino))?.unwrap_or_default();
    Ok(MetaReply::Inline { data, change_id: n.rec.change_id })
}

/// RenewCap 与 ReopenHandle 共用的前半段：经已打开的句柄（`AuthzOp::Handle`，ADR-0036：属主不看权限位）、签 cap 之前核对会话；
/// 返回 (inode, 能不能读, cap 的权限位)——读句柄只给读；写句柄给写，能读时再加读。
fn handle_rights(v: &dyn KvRead, c: &OpCtx, ino: u64, write: bool) -> Result<(Node, bool, u8), ErrCode> {
    let n = file_node(v, ino)?;
    authz(v, c, AuthzOp::Handle { read: !write, write }, Some(&n), None, None)?;
    sessions::may_sign_cap(v, c.req)?;
    let can_read = !write || may_read(v, c, &n); // 读句柄：上面的 Handle 已经查过读
    let rights = if write { write_rights(can_read) } else { RIGHT_READ };
    Ok((n, can_read, rights))
}

pub fn renew_cap(v: &dyn KvRead, c: &OpCtx, ino: u64, write: bool) -> Result<MetaReply, ErrCode> {
    let (n, _, rights) = handle_rights(v, c, ino, write)?;
    Ok(MetaReply::Cap { cap: sign_cap(c, &n, rights, c.req.session) })
}

pub fn reopen_handle(v: &dyn KvRead, c: &OpCtx, ino: u64, write: bool) -> Result<MetaReply, ErrCode> {
    let (n, can_read, rights) = handle_rights(v, c, ino, write)?;
    Ok(MetaReply::Opened(open_out(v, c, &n, can_read, rights)?))
}

/// `SetAtime`（relatime 批量、尽力而为；实现注记（M6））：调用者能读这个文件才更新它的 atime（读文件本来就会更新 atime）；atime 只往后
/// 走、不超过 leader 的现在；**不改 ctime 与 change_id**（atime 不是内容变化，否则每次读都会让别的客户端的缓存失效），不写 changelog
/// （ADR-0029 第 5 条）。已经删掉或读不了的项跳过。超过 [`SET_ATIME_MAX_ITEMS`] 项 → `EINVAL`。
pub fn set_atime(v: &mut dyn KvTxn, c: &OpCtx, items: &[(u64, Ts)]) -> Result<MetaReply, ErrCode> {
    if items.len() > SET_ATIME_MAX_ITEMS {
        return Err(ErrCode::EINVAL);
    }
    for (ino, ts) in items {
        let mut n = match get(v, *ino) {
            Ok(n) => n,
            Err(ErrCode::ENOENT) => continue,
            Err(e) => return Err(e),
        };
        let t = (*ts).min(c.req.now);
        if t > n.rec.atime && may_read(v, c, &n) {
            n.rec.atime = t;
            save(v, &n);
        }
    }
    Ok(MetaReply::Ok)
}

/// relatime（3.1.1 放松第 1 条）：atime 不晚于 mtime 或 ctime，或者已经旧了 24 小时 → 这次读打开该排队更新 atime（`OpenOut.atime_due`）。
pub fn atime_due(n: &Node, now: Ts) -> bool {
    let day_ago = Ts { secs: now.secs - 86_400, nanos: now.nanos };
    n.rec.atime <= n.rec.mtime || n.rec.atime <= n.rec.ctime || n.rec.atime <= day_ago
}

/// 服务端 uninline 要做的事（ADR-0034 第 3 条第 1 步的结果）。
pub struct UninlineTodo {
    pub change_id: u64,
    pub content: Vec<u8>,
}

/// 服务端 uninline 的只读前半段（线性一致读之后）：调用者要能经写句柄写它（`AuthzOp::Handle`，同 CommitWrite）；回给调用者的 OpenOut（写 cap，能读时也带读权限；
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
    Ok((open, Some(UninlineTodo { change_id: n.rec.change_id, content })))
}

/// `(ino, epoch)` 记为垃圾（GC 记录 kind 2，宽限 `epoch_gc_grace` 之后由 gc 删掉它在这些链上的数据）。
pub fn gc_epoch(v: &mut dyn KvTxn, c: &OpCtx, ino: u64, epoch: u32, chains: &[u32]) {
    let now = c.req.now_ms;
    let rec = GcRecV1 { due_ms: now + c.settings.epoch_gc_grace_ms, chains: chains.to_vec(), created_ms: now };
    kv::put(v, &keys::gc(GC_EPOCH, ino, epoch), &rec);
}

/// 仍是 inline、change_id 仍是 `cid`（否则 `STALE_LAYOUT`：uninline 从头再来），调用者能经写句柄写它；返回 (inode, layout)。
fn still_inline(v: &dyn KvRead, c: &OpCtx, ino: u64, cid: u64) -> Result<(Node, LayoutV1), ErrCode> {
    let n = get(v, ino)?;
    writable_file(v, c, &n)?;
    let l = n.rec.layout.clone().ok_or(ErrCode::EINVAL)?;
    if n.rec.flags & F_INLINE == 0 || n.rec.change_id != cid {
        return Err(ErrCode::STALE_LAYOUT);
    }
    Ok((n, l))
}

/// uninline 的意图（见文件头）：别人留下的 `pending_epoch`（旧 leader 没做完的、或崩溃留下的）记为垃圾，分一个新 epoch 记进
/// `pending_epoch`（change_id 不变：它不是对外可见的属性）。回 `Opened`：layout 是"按新 epoch 写"的（链与其余参数不变），cap 是
/// leader 自己签的写 cap（session 0，不属于任何会话，3.3.5 的 M5 注记）。
pub fn uninline_intent(v: &mut dyn KvTxn, c: &OpCtx, ino: u64, cid: u64) -> Result<MetaReply, ErrCode> {
    let (mut n, mut l) = still_inline(v, c, ino, cid)?;
    if n.rec.pending_epoch != 0 {
        gc_epoch(v, c, ino, n.rec.pending_epoch, &l.chains);
    }
    let e = n.rec.max_epoch.checked_add(1).ok_or(ErrCode::EOVERFLOW)?;
    (n.rec.max_epoch, n.rec.pending_epoch) = (e, e);
    save(v, &n);
    l.epoch = e;
    // 只用来签 cap 的副本：layout 换成新 epoch（cap 里的 epoch 要是它），不保存——文件的 layout 要到切换时才换
    let mut for_cap = n.clone();
    for_cap.rec.layout = Some(l.clone());
    let cap = sign_cap(c, &for_cap, RIGHT_WRITE, 0);
    Ok(MetaReply::Opened(OpenOut {
        attr: attr_out(v, c, &n)?,
        layout: Some(l),
        cap,
        inline: None,
        quota_end: u64::MAX,
        atime_due: false,
    }))
}

/// uninline 的切换（CAS，见文件头）。内容与大小都没变，不写 changelog（ADR-0034）。回给调用者新 layout 的 `Opened`（写 cap，能读时也带
/// 读权限；不带 inline 内容——它可能只有写权限）。`intent`：
/// - `Some(e)`：这次尝试的意图的 epoch，数据已经写进去、落盘了。`pending_epoch` 仍是 `e` → layout 切到 `e`、清 `pending_epoch`、清
///   F_INLINE 与 INLINE，旧 epoch 记为垃圾（M8 之前没做完的 uninline 可能在它下面留了 chunk）；不是 `e`（别人换过意图）→ `STALE_LAYOUT`。
/// - `None`：空内容，没写数据面，epoch 不变；别人留下的意图照样记为垃圾。
pub fn uninline_switch(
    v: &mut dyn KvTxn,
    c: &OpCtx,
    ino: u64,
    cid: u64,
    intent: Option<u32>,
) -> Result<MetaReply, ErrCode> {
    let (mut n, mut l) = still_inline(v, c, ino, cid)?;
    sessions::may_sign_cap(v, c.req)?;
    match intent {
        Some(e) => {
            if n.rec.pending_epoch != e {
                return Err(ErrCode::STALE_LAYOUT); // 别人（另一个 leader 上的 uninline）换过意图
            }
            gc_epoch(v, c, ino, l.epoch, &l.chains);
            l.epoch = e;
        }
        None if n.rec.pending_epoch != 0 => gc_epoch(v, c, ino, n.rec.pending_epoch, &l.chains), // 别人留下的意图记为垃圾
        None => {}
    }
    (n.rec.layout, n.rec.pending_epoch) = (Some(l), 0);
    n.rec.flags &= !F_INLINE;
    v.delete(&keys::inline(ino));
    bump_ctime(&mut n, c);
    save(v, &n);
    Ok(MetaReply::Opened(open_out(v, c, &n, false, write_rights(may_read(v, c, &n)))?))
}
