//! setattr（design.md 3.5.1 的 setattr 三行、3.4.2.9）：chmod、chown、utimensat、truncate。
//!
//! - **截断到 0**（含 `O_TRUNC`、覆盖式 `cp`）：只有元数据，一个闭包——`epoch = max_epoch + 1`，**按当前生效属性重建整个
//!   layout**（与 create 共用 `new_layout`，必要时生成新链），size = 0，`class_id` 改为新类，F_INLINE 按新 layout 重新判定、
//!   INLINE 清空；旧 epoch 写 GC 记录（kind 2，宽限 `epoch_gc_grace` 后由 gc 任务删除；旧 inode 有 F_INLINE 时没有 chunk，不写）。
//! - **截断到 s > 0**：数据面的截断已由客户端先完成（3.4.2.9），这里只改 size；F_INLINE 文件把 INLINE 截到 `min(len, s)`
//!   （`len(INLINE) ≤ size` 不变式）；`epoch_seen` 与当前 layout 的 epoch 不同 → `STALE_LAYOUT`（客户端截断的是旧 epoch）。
//! - 改了 size 的 SetAttr 回 `Opened(OpenOut)`（新 attr、layout，3.1.4 C4；调用者能读这个文件时才带 inline 内容），
//!   否则回 `Entry`。
//! - chmod：只有属主或 root（authz 钩子查）；不在文件所属组里的非 root 设不了 setgid（静默去掉，与 Linux 相同）；有 ACL 时
//!   同步 `system.posix_acl_access` 的 USER_OBJ / MASK / OTHER。chown：只有 root 能改 uid；属主只能把 gid 改成自己所在的组；
//!   非目录 chown 之后去掉 setuid、以及带组执行位时的 setgid。非属主改长度也去掉它们（"非属主写入时清除 suid/sgid"）。
//! - 时间：设成"现在"只要有写权限（或是属主）；设成指定值要是属主或 root。任何改动都更新 ctime、change_id + 1。

use super::common::{
    MODE_MASK, Node, S_ISGID, S_ISUID, S_IXGRP, attr_out, authz, bump_ctime, get, in_group, is_file, may_read,
    open_out, save, set_new_layout,
};
use super::xattr::sync_acl_mode;
use crate::base::ErrCode;
use crate::hooks::traits::AuthzOp;
use crate::meta::changelog::{self, Change};
use crate::meta::engine::txn::{KvTxn, OpCtx};
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
    bump_ctime(&mut n, c);
    save(v, &n);
    changelog::append(v, c, Change { op: "SetAttr", ino, size: n.rec.size, ..Default::default() })?;
    match set.size {
        Some(_) => Ok(MetaReply::Opened(open_out(v, &n, may_read(v, c, &n))?)),
        None => Ok(MetaReply::Entry { attr: attr_out(&n) }),
    }
}

/// 去掉 setuid，以及带组执行位时的 setgid（不带组执行位的 setgid 表示强制锁，Linux 保留它）。
fn kill_suid(n: &mut Node) {
    n.rec.mode &= !S_ISUID;
    if n.rec.mode & S_IXGRP != 0 {
        n.rec.mode &= !S_ISGID;
    }
}

/// 改普通文件的长度（见文件头）。
fn truncate(v: &mut dyn KvTxn, c: &OpCtx, n: &mut Node, size: u64, epoch_seen: u32) -> Result<(), ErrCode> {
    if n.is_dir() {
        return Err(ErrCode::EISDIR);
    }
    if !is_file(n) {
        return Err(ErrCode::EINVAL);
    }
    let was_inline = n.rec.flags & F_INLINE != 0;
    if size == 0 {
        let old = n.rec.layout.clone();
        set_new_layout(v, c, n)?;
        if let Some(l) = old.filter(|_| !was_inline) {
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
    } else if epoch_seen != 0 && n.rec.layout.as_ref().map(|l| l.epoch) != Some(epoch_seen) {
        return Err(ErrCode::STALE_LAYOUT);
    }
    if c.req.who.uid != n.rec.uid && c.req.who.uid != 0 {
        kill_suid(n);
    }
    n.rec.size = size;
    n.rec.mtime = c.req.now;
    Ok(())
}
