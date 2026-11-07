//! 删除（design.md 3.5.1 的 unlink/rmdir 行、3.5.6）：unlink 与 rmdir，以及 rename 覆盖目标时共用的部分。
//!
//! unlink：删目录项；nlink − 1；删的是主链接且还有别的链接 → 主链接改为 extra_links[0]；nlink 为 0 → 标 F_ORPHAN +
//! GC 记录 kind 1（打开着它的客户端照常读写，无人打开后由 gc 任务删除，3.5.6）。rmdir：只能删空目录（`size == 0`，
//! 否则 `ENOTEMPTY`）；删目录项、inode、它的 XATTR、RSTAT、DIRATTR（连同它的配额键的软限起点 QUOTA_STATE，M10 审查）、QOS（删了
//! QOS 就 `map_epoch += 1`：QoS 表在集群图里，3.3.6）；父目录 nlink − 1、size − 1。查名字之前先要能在父目录里查名字（执行权限，
//! 与 lookup 相同），所以没有权限的人从 `ENOENT` 与 `EACCES` 的区别里看不出目录里有哪些名字。
//! 实现注记（M4）：`trash` 属性（回收站）在 M12。实现注记（M7）：祖先 RSTAT 的扣除（3.9）：最后一个链接 → bytes − size、files − 1；
//! 删的是主链接、还有别的链接 → 计数搬到 extra_links[0] 的目录；rmdir → dirs − 1。实现注记（M12）：`trash` 生效时 unlink 改为移进回收站
//! （`trash.rs`；changelog `Unlink` 的 `extra` 写 `trash=<回收站里的新名字>`）；ttl 任务与 trash-expire 任务经 [`unlink_as`] 删
//! （`reason=ttl` / `TrashExpire`，ADR-0029）。

use super::common::{
    Node, authz, bump_ctime, check_name, child, get, orphan, remove_link, save, searchable_dir, update_dir,
};
use super::trash;
use crate::base::ErrCode;
use crate::hooks::traits::AuthzOp;
use crate::meta::attrs::bump_attr_epoch;
use crate::meta::changelog::{self, Change};
use crate::meta::engine::txn::{KvTxn, OpCtx};
use crate::meta::quota;
use crate::meta::rstat::{self, Delta};
use crate::meta::schema::keys;
use crate::meta::schema::kv;
use crate::meta::schema::records::AttrMapV1;
use crate::proto::meta::MetaReply;

/// 去掉文件 `n` 在 (parent, name) 的链接（目录项已由调用方处理）：nlink − 1，归零 → 孤儿（立即从祖先 RSTAT 扣掉）；删的是主链接、
/// 还有别的链接 → 计数搬到新的主父目录（3.9）；写回。
pub fn unlink_file(v: &mut dyn KvTxn, c: &OpCtx, n: &mut Node, parent: u64, name: &[u8]) -> Result<(), ErrCode> {
    let (before, old_parent) = (Delta::of(v, n)?, n.rec.parent);
    remove_link(n, (parent, name));
    bump_ctime(n, c);
    if n.rec.nlink == 0 {
        rstat::apply(v, c, old_parent, &before.neg())?;
        orphan(v, c, n);
    } else {
        rstat::transfer(v, c, old_parent, n.rec.parent, &before)?;
    }
    save(v, n);
    Ok(())
}

/// 删掉一个（空）目录自己的记录：inode、XATTR、RSTAT、DIRATTR、QOS（点删除；QOS 在的话同一批里 `map_epoch += 1`；DIRATTR 在的话
/// `attr_epoch += 1`——属性变了，解析缓存要作废，M7——并删掉它的配额键的软限起点，M10 审查）。
pub fn delete_dir_records(v: &mut dyn KvTxn, ino: u64) -> Result<(), ErrCode> {
    if v.get(&keys::qos(ino))?.is_some() {
        v.delete(&keys::qos(ino));
        kv::bump_map_epoch(v)?;
    }
    if let Some(m) = kv::get::<AttrMapV1>(v, &keys::dirattr(ino))? {
        for k in m.entries.keys() {
            quota::forget(v, ino, k)?;
        }
        bump_attr_epoch(v)?;
    }
    for k in [keys::inode(ino), keys::xattr(ino), keys::rstat(ino), keys::dirattr(ino)] {
        v.delete(&k);
    }
    Ok(())
}

/// unlink 是谁发起的（changelog 不同，ADR-0029）。
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum UnlinkBy {
    User,
    Ttl,         // ttl 任务：Unlink 带 `reason=ttl`
    TrashExpire, // trash-expire 任务：记成 `TrashExpire`，不再进回收站
}

pub fn unlink(v: &mut dyn KvTxn, c: &OpCtx, parent: u64, name: &[u8]) -> Result<MetaReply, ErrCode> {
    unlink_as(v, c, parent, name, UnlinkBy::User)
}

/// unlink（见文件头）：`trash` 生效（且不是 trash-expire 自己）→ 移进回收站，否则照常删。
pub fn unlink_as(v: &mut dyn KvTxn, c: &OpCtx, parent: u64, name: &[u8], by: UnlinkBy) -> Result<MetaReply, ErrCode> {
    let (use_trash, op, reason) = match by {
        UnlinkBy::User => (true, "Unlink", None),
        UnlinkBy::Ttl => (true, "Unlink", Some("ttl")),
        UnlinkBy::TrashExpire => (false, "TrashExpire", None),
    };
    check_name(name)?;
    let p = searchable_dir(v, c, parent)?;
    let mut n = child(v, parent, name)?.ok_or(ErrCode::ENOENT)?;
    if n.is_dir() {
        return Err(ErrCode::EISDIR);
    }
    authz(v, c, AuthzOp::Unlink, None, Some(&p), Some(&n))?;
    v.delete(&keys::dentry(parent, name));
    let bin = if use_trash { trash::dir_for(v, c, &p, &n)? } else { None };
    let mut extra: Vec<(String, String)> = reason.map(|r| ("reason".into(), r.into())).into_iter().collect();
    extra.extend(trash::trash_or_unlink(v, c, &mut n, (parent, name), bin)?);
    let mut p = get(v, parent)?; // 回收站可能刚建在它下面（dir_for 改过它）：改最新的那份
    update_dir(v, c, &mut p, -1, 0);
    changelog::append(v, c, Change { op, ino: n.ino, parent, name, size: n.rec.size, extra, ..Default::default() })?;
    Ok(MetaReply::Ok)
}

pub fn rmdir(v: &mut dyn KvTxn, c: &OpCtx, parent: u64, name: &[u8]) -> Result<MetaReply, ErrCode> {
    check_name(name)?;
    let mut p = searchable_dir(v, c, parent)?;
    let n = child(v, parent, name)?.ok_or(ErrCode::ENOENT)?;
    if !n.is_dir() {
        return Err(ErrCode::ENOTDIR);
    }
    authz(v, c, AuthzOp::Rmdir, None, Some(&p), Some(&n))?;
    if n.rec.size != 0 {
        return Err(ErrCode::ENOTEMPTY);
    }
    v.delete(&keys::dentry(parent, name));
    let d = Delta::of(v, &n)?.neg(); // 空目录：dirs − 1
    rstat::apply(v, c, parent, &d)?;
    update_dir(v, c, &mut p, -1, -1);
    changelog::append(v, c, Change { op: "Rmdir", ino: n.ino, parent, name, ..Default::default() })?; // 要解析属性：先写
    delete_dir_records(v, n.ino)?; // 有 DIRATTR 时 attr_epoch += 1：之后不再解析属性（attrs.rs）
    Ok(MetaReply::Ok)
}
