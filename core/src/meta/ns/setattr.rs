//! setattr（design.md 3.5.1 的 setattr 三行、3.4.2.9）：chmod、chown、utimensat、truncate。
//!
//! - **截断到 0**（含 `O_TRUNC`、覆盖式 `cp`）：只有元数据，一个闭包——`epoch = max_epoch + 1`，**按当前生效属性重建整个
//!   layout**（与 create 共用 `new_layout`，必要时生成新链），size = 0，`class_id` 改为新类，F_INLINE 按新 layout 重新判定、
//!   INLINE 清空；旧 epoch 写 GC 记录（kind 2，宽限 `epoch_gc_grace` 后由 gc 任务删除）。实现注记（M5）：旧 inode 有 F_INLINE 时
//!   也写——ADR-0034 之后 inline 文件的 epoch 下面也可能有 chunk（Uninline 写完 chunk、提交之前崩溃，或 CAS 因截断失败）。
//! - 新长度超过单个文件的最大长度（`common::max_file_size`）→ `EFBIG`。
//! - **截断到 s > 0**：数据面的截断已由客户端先完成（3.4.2.9），这里只改 size；F_INLINE 文件把 INLINE 截到 `min(len, s)`
//!   （`len(INLINE) ≤ size` 不变式）；已经不是 inline 的文件，`epoch_seen` 与当前 layout 的 epoch 不同 → `STALE_LAYOUT`（客户端
//!   截断的是旧 epoch）。实现注记（M6，ADR-0035 第 9 条）：`epoch_seen = 0` 表示"我看到的是 inline、没有发 TruncateChunk"（epoch 从
//!   1 起，0 不会是真的 epoch），对已经不是 inline 的文件**同样**回 `STALE_LAYOUT`（客户端重新 Open、按 chunk 截断）——否则按旧的
//!   inline 视图截断一个刚被 Uninline 的文件，chunk 里 s 之后的旧数据没清零，之后扩长就会冒出来。
//! - 经已打开的写句柄改长度（ADR-0036 的 `TruncateHandle`，[`truncate_handle`]）：同一段代码，只是授权用 `AuthzOp::Handle`
//!   （属主不看权限位）；按路径的 truncate 仍是 SetAttr，照常按当前权限检查。
//! - 改了 size 的 SetAttr 回 `Opened(OpenOut)`（新 attr、layout，3.1.4 C4；调用者能读这个文件时才带 inline 内容），
//!   否则回 `Entry`。实现注记（M5）：OpenOut 带一张写 cap（能读时也带读权限），截断到 0 之后的写直接用它进新 epoch。
//! - chmod：只有属主或 root（authz 钩子查）；不在文件所属组里的非 root 设不了 setgid（静默去掉，与 Linux 相同）；有 ACL 时
//!   同步 `system.posix_acl_access` 的 USER_OBJ / MASK / OTHER。chown：只有 root 能改 uid；属主只能把 gid 改成自己所在的组；
//!   非目录 chown 之后去掉 setuid、以及带组执行位时的 setgid。非属主改长度也去掉它们（"非属主写入时清除 suid/sgid"）。
//! - 时间：设成"现在"只要有写权限（或是属主）；设成指定值要是属主或 root。任何改动都更新 ctime、change_id + 1。
//! - 实现注记（M7）：改了长度（截断到 0 时还可能换了存储类）→ 祖先 RSTAT 按差值更新（3.9；孤儿不计）。

use super::common::{
    MODE_MASK, Node, S_ISGID, attr_out, authz, bump_ctime, get, in_group, is_file, kill_suid, max_file_size, may_read,
    open_out, save, set_new_layout, write_rights,
};
use super::xattr::sync_acl_mode;
use crate::base::ErrCode;
use crate::hooks::traits::AuthzOp;
use crate::meta::changelog::{self, Change};
use crate::meta::engine::txn::{KvTxn, OpCtx};
use crate::meta::rstat::{self, Delta};
use crate::meta::schema::keys::{self, GC_EPOCH};
use crate::meta::schema::kv;
use crate::meta::schema::records::{F_HAS_ACL, F_INLINE, GcRecV1};
use crate::proto::meta::{MetaReply, SetAttrIn, TimeSet};

pub fn setattr(v: &mut dyn KvTxn, c: &OpCtx, ino: u64, set: &SetAttrIn) -> Result<MetaReply, ErrCode> {
    let mut n = get(v, ino)?;
    let who = &c.req.who;
    let root = who.uid == 0;
    let now = |t: &Option<TimeSet>| matches!(t, Some(TimeSet::Now));
    let given = |t: &Option<TimeSet>| matches!(t, Some(TimeSet::At(_)));
    let op = AuthzOp::SetAttr {
        mode: set.mode.is_some(),
        owner: set.uid.is_some() || set.gid.is_some(),
        size: set.size.is_some(),
        times: given(&set.atime) || given(&set.mtime),
        times_now: now(&set.atime) || now(&set.mtime),
    };
    authz(v, c, op, Some(&n), None, None)?;
    // chown 的取值规则（插件只核对"属主或 root"，看不到新值）
    if set.uid.is_some_and(|u| u != n.rec.uid) && !root {
        return Err(ErrCode::EPERM);
    }
    if set.gid.is_some_and(|g| g != n.rec.gid && !in_group(who, g)) && !root {
        return Err(ErrCode::EPERM);
    }
    if let Some(size) = set.size {
        truncate(v, c, &mut n, size, set.epoch_seen)?;
    }
    if let Some(m) = set.mode {
        n.rec.mode = m & MODE_MASK;
        if !root && !in_group(who, n.rec.gid) {
            n.rec.mode &= !S_ISGID;
        }
    }
    if set.uid.is_some() || set.gid.is_some() {
        n.rec.uid = set.uid.unwrap_or(n.rec.uid);
        n.rec.gid = set.gid.unwrap_or(n.rec.gid);
        if !n.is_dir() {
            kill_suid(&mut n);
        }
    }
    let when = |t: &Option<TimeSet>| t.map(|t| if let TimeSet::At(ts) = t { ts } else { c.req.now });
    if let Some(t) = when(&set.atime) {
        n.rec.atime = t;
    }
    if let Some(t) = when(&set.mtime) {
        n.rec.mtime = t;
    }
    if set.mode.is_some() && n.rec.flags & F_HAS_ACL != 0 {
        sync_acl_mode(v, &n)?;
    }
    finish(v, c, n, set.size.is_some())
}

/// `TruncateHandle`（ADR-0036）：经已打开的写句柄改长度——与 SetAttr{size} 同一段代码，授权用 `AuthzOp::Handle { write }`。
pub fn truncate_handle(
    v: &mut dyn KvTxn,
    c: &OpCtx,
    ino: u64,
    size: u64,
    epoch_seen: u32,
) -> Result<MetaReply, ErrCode> {
    let mut n = get(v, ino)?;
    authz(v, c, AuthzOp::Handle { read: false, write: true }, Some(&n), None, None)?;
    truncate(v, c, &mut n, size, epoch_seen)?;
    finish(v, c, n, true)
}

/// 改完属性：ctime、change_id、写回、changelog；改了长度回 `Opened`（调用者能读时才带 inline 内容），否则回 `Entry`。
fn finish(v: &mut dyn KvTxn, c: &OpCtx, mut n: Node, sized: bool) -> Result<MetaReply, ErrCode> {
    bump_ctime(&mut n, c);
    save(v, &n);
    changelog::append(v, c, Change { op: "SetAttr", ino: n.ino, size: n.rec.size, ..Default::default() })?;
    if !sized {
        return Ok(MetaReply::Entry { attr: attr_out(v, c, &n)? });
    }
    let r = may_read(v, c, &n);
    Ok(MetaReply::Opened(open_out(v, c, &n, r, write_rights(r))?))
}

/// 改普通文件的长度（见文件头）。
fn truncate(v: &mut dyn KvTxn, c: &OpCtx, n: &mut Node, size: u64, epoch_seen: u32) -> Result<(), ErrCode> {
    if n.is_dir() {
        return Err(ErrCode::EISDIR);
    }
    if !is_file(n) {
        return Err(ErrCode::EINVAL);
    }
    if n.rec.layout.as_ref().is_some_and(|l| size > max_file_size(l)) {
        return Err(ErrCode::EFBIG);
    }
    let before = Delta::of(v, n)?; // 递归计数：长度（截断到 0 时可能还有存储类）变了（3.9）
    let was_inline = n.rec.flags & F_INLINE != 0;
    if size == 0 {
        let old = n.rec.layout.clone();
        set_new_layout(v, c, n)?;
        if let Some(l) = old {
            let now = c.req.now_ms;
            let rec = GcRecV1 { due_ms: now + c.settings.epoch_gc_grace_ms, chains: l.chains, created_ms: now };
            kv::put(v, &keys::gc(GC_EPOCH, n.ino, l.epoch), &rec);
        }
        v.delete(&keys::inline(n.ino));
    } else if was_inline {
        if let Some(mut d) = v.get(&keys::inline(n.ino))?.filter(|d| d.len() as u64 > size) {
            d.truncate(size as usize);
            v.put(&keys::inline(n.ino), d);
        }
    } else if n.rec.layout.as_ref().map(|l| l.epoch) != Some(epoch_seen) {
        return Err(ErrCode::STALE_LAYOUT); // 包括 epoch_seen = 0（客户端以为还是 inline，ADR-0035 第 9 条）
    }
    if c.req.who.uid != n.rec.uid && c.req.who.uid != 0 {
        kill_suid(n);
    }
    n.rec.size = size;
    n.rec.mtime = c.req.now;
    rstat::changed(v, c, &before, n)
}
