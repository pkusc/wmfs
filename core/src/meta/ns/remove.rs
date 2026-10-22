//! 删除（design.md 3.5.1 的 unlink/rmdir 行、3.5.6）：unlink 与 rmdir，以及 rename 覆盖目标时共用的部分。
//!
//! unlink：删目录项；nlink − 1；删的是主链接且还有别的链接 → 主链接改为 extra_links[0]；nlink 为 0 → 标 F_ORPHAN +
//! GC 记录 kind 1（打开着它的客户端照常读写，无人打开后由 gc 任务删除，3.5.6）。rmdir：只能删空目录（`size == 0`，
//! 否则 `ENOTEMPTY`）；删目录项、inode、它的 XATTR、RSTAT、DIRATTR、QOS（删了 QOS 就 `map_epoch += 1`：QoS 表在集群图里，
//! 3.3.6）；父目录 nlink − 1、size − 1。查名字之前先要能在父目录里查名字（执行权限，与 lookup 相同），所以没有权限的人
//! 从 `ENOENT` 与 `EACCES` 的区别里看不出目录里有哪些名字。
//! 实现注记（M4）：`trash` 属性（回收站）在 M12；祖先 RSTAT 的扣除在 M7。

use super::common::{
    Node, authz, bump_ctime, check_name, child, orphan, remove_link, save, searchable_dir, update_dir,
};
use crate::base::ErrCode;
use crate::hooks::traits::AuthzOp;
use crate::meta::changelog::{self, Change};
use crate::meta::engine::txn::{KvTxn, OpCtx};
use crate::meta::schema::keys;
use crate::meta::schema::kv;
use crate::proto::meta::MetaReply;

/// 去掉文件 `n` 在 (parent, name) 的链接（目录项已由调用方处理）：nlink − 1，归零 → 孤儿；写回。
pub fn unlink_file(v: &mut dyn KvTxn, c: &OpCtx, n: &mut Node, parent: u64, name: &[u8]) {
    remove_link(n, (parent, name));
    bump_ctime(n, c);
    if n.rec.nlink == 0 {
        orphan(v, c, n);
    }
    save(v, n);
}

/// 删掉一个（空）目录自己的记录：inode、XATTR、RSTAT、DIRATTR、QOS（点删除；QOS 在的话同一批里 `map_epoch += 1`）。
pub fn delete_dir_records(v: &mut dyn KvTxn, ino: u64) -> Result<(), ErrCode> {
    if v.get(&keys::qos(ino))?.is_some() {
        v.delete(&keys::qos(ino));
        kv::bump_map_epoch(v)?;
    }
    for k in [keys::inode(ino), keys::xattr(ino), keys::rstat(ino), keys::dirattr(ino)] {
        v.delete(&k);
    }
    Ok(())
}

pub fn unlink(v: &mut dyn KvTxn, c: &OpCtx, parent: u64, name: &[u8]) -> Result<MetaReply, ErrCode> {
    check_name(name)?;
    let mut p = searchable_dir(v, c, parent)?;
    let mut n = child(v, parent, name)?.ok_or(ErrCode::ENOENT)?;
    if n.is_dir() {
        return Err(ErrCode::EISDIR);
    }
    authz(v, c, AuthzOp::Unlink, None, Some(&p), Some(&n))?;
    v.delete(&keys::dentry(parent, name));
    unlink_file(v, c, &mut n, parent, name);
    update_dir(v, c, &mut p, -1, 0);
    let size = n.rec.size;
    changelog::append(v, c, Change { op: "Unlink", ino: n.ino, parent, name, size, ..Default::default() })?;
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
    delete_dir_records(v, n.ino)?;
    update_dir(v, c, &mut p, -1, -1);
    changelog::append(v, c, Change { op: "Rmdir", ino: n.ino, parent, name, ..Default::default() })?;
    Ok(MetaReply::Ok)
}
