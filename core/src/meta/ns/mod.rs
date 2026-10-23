//! 命名空间操作（design.md 3.1、3.5.1）：普通的顺序代码，修改型的跑在执行器闭包里（一个操作 = 一个原子的覆盖层，
//! 失败整体丢弃），只读的跑在 meta-read 线程上（ReadIndex 之后的一个 redb 读事务）。authz、placement 钩子都在闭包**内部**
//! 对着同一个视图调用，判定与修改之间没有 TOCTOU 窗口。
//!
//! - [`common`]：读写 inode 与目录项、名字检查、属主与权限位、AttrOut/OpenOut、authz、new_layout、孤儿
//! - [`read_ops`]：lookup、getattr、readdir、readlink、按路径解析、open
//! - [`create`]：create、mkdir、mknod、symlink、link
//! - [`remove`]：unlink、rmdir（孤儿标记）
//! - [`rename`]：rename（含 RENAME_NOREPLACE、RENAME_EXCHANGE）
//! - [`setattr`]：chmod、chown、utimensat、truncate（截断到 0 按当前属性重建 layout）
//! - [`xattr`]：扩展属性与 POSIX ACL 的存储（mode 与 F_HAS_ACL 同步）
//! - [`acl`]：ACL 的二进制格式、新文件继承默认 ACL（umask 规则）
//! - [`file_ops`]：CommitWrite、Fallocate、GetInline、RenewCap，以及服务端 uninline 的只读前半段（M5）
//!
//! 实现注记（M4）：`SetAtime`（M6）、`GetLock`/`SetLock`/`AppendReserve`（M12）还没有实现，回 `FEATURE_UNSUPPORTED`；回收站
//! （M12）、递归计数（M7）、配额（M10）、目录属性的继承（M7）随各自的里程碑加入。实现注记（M5）：`StatFs`（要心跳报上来的
//! target 容量，`meta/cluster/targets.rs`）与 `Uninline`（要经数据面写 chunk，`meta/uninline.rs`）由元数据服务另外处理。

pub mod acl;
pub mod common;
pub mod create;
pub mod file_ops;
pub mod read_ops;
pub mod remove;
pub mod rename;
pub mod setattr;
pub mod xattr;

use crate::base::ErrCode;
use crate::meta::engine::txn::{KvScan, OpCtx, TxnOp};
use crate::meta::sessions::{self, OpenInfo};
use crate::proto::meta::{MetaOp, MetaReply};

/// 修改型操作 → 执行器闭包；本里程碑还没有实现的（或不是修改型的）→ None。
pub fn txn_op(op: MetaOp) -> Option<TxnOp> {
    Some(match op {
        MetaOp::SessionOpen { client_name, host, host_id, mount, version: _ } => {
            let info = OpenInfo { client_name, host, host_id, mount };
            Box::new(move |v, c| sessions::open(v, c, info))
        }
        MetaOp::SessionClose => Box::new(|v, c| sessions::close(v, c)),
        MetaOp::Create { parent, name, mode, umask, excl } => {
            Box::new(move |v, c| create::create(v, c, parent, &name, mode, umask, excl))
        }
        MetaOp::Mkdir { parent, name, mode, umask } => {
            Box::new(move |v, c| create::mkdir(v, c, parent, &name, mode, umask))
        }
        MetaOp::Mknod { parent, name, mode, umask, kind, rdev } => {
            Box::new(move |v, c| create::mknod(v, c, parent, &name, mode, umask, kind, rdev))
        }
        MetaOp::Symlink { parent, name, target } => Box::new(move |v, c| create::symlink(v, c, parent, &name, &target)),
        MetaOp::Link { ino, new_parent, new_name } => {
            Box::new(move |v, c| create::link(v, c, ino, new_parent, &new_name))
        }
        MetaOp::Unlink { parent, name } => Box::new(move |v, c| remove::unlink(v, c, parent, &name)),
        MetaOp::Rmdir { parent, name } => Box::new(move |v, c| remove::rmdir(v, c, parent, &name)),
        MetaOp::Rename { parent, name, new_parent, new_name, flags } => {
            Box::new(move |v, c| rename::rename(v, c, parent, &name, new_parent, &new_name, flags))
        }
        MetaOp::SetAttr { ino, set } => Box::new(move |v, c| setattr::setattr(v, c, ino, &set)),
        MetaOp::SetXattr { ino, name, value, flags } => {
            Box::new(move |v, c| xattr::setxattr(v, c, ino, &name, &value, flags))
        }
        MetaOp::RemoveXattr { ino, name } => Box::new(move |v, c| xattr::removexattr(v, c, ino, &name)),
        MetaOp::CommitWrite { ino, epoch, new_size, inline_patches, uninline_expect_change_id, .. } => {
            let w = file_ops::CommitIn {
                ino,
                epoch,
                new_size,
                patches: inline_patches,
                expect_change_id: uninline_expect_change_id,
            };
            Box::new(move |v, c| file_ops::commit_write(v, c, &w))
        }
        MetaOp::Fallocate { ino, offset, len, mode } => {
            Box::new(move |v, c| file_ops::fallocate(v, c, ino, offset, len, mode))
        }
        _ => return None,
    })
}

/// 走只读路径（meta-read 线程、ReadIndex 之后）的命名空间操作。
pub fn is_read(op: &MetaOp) -> bool {
    matches!(
        op,
        MetaOp::Lookup { .. }
            | MetaOp::GetAttr { .. }
            | MetaOp::ReadDir { .. }
            | MetaOp::ReadLink { .. }
            | MetaOp::GetXattr { .. }
            | MetaOp::ListXattr { .. }
            | MetaOp::ResolvePath { .. }
            | MetaOp::Open { .. }
            | MetaOp::GetInline { .. }
            | MetaOp::RenewCap { .. }
    )
}

/// 执行一个只读的命名空间操作（`is_read` 为真的那些）。
pub fn read(v: &dyn KvScan, c: &OpCtx, op: &MetaOp) -> Result<MetaReply, ErrCode> {
    match op {
        MetaOp::Lookup { parent, name } => read_ops::lookup(v, c, *parent, name),
        MetaOp::GetAttr { ino } => read_ops::getattr(v, c, *ino),
        MetaOp::ReadDir { ino, after, limit, plus } => read_ops::readdir(v, c, *ino, after, *limit, *plus),
        MetaOp::ReadLink { ino } => read_ops::readlink(v, c, *ino),
        MetaOp::GetXattr { ino, name } => xattr::getxattr(v, c, *ino, name),
        MetaOp::ListXattr { ino } => xattr::listxattr(v, c, *ino),
        MetaOp::ResolvePath { path } => read_ops::resolve(v, c, path),
        MetaOp::Open { ino, flags } => read_ops::open(v, c, *ino, *flags),
        MetaOp::GetInline { ino } => file_ops::get_inline(v, c, *ino),
        MetaOp::RenewCap { ino, write } => file_ops::renew_cap(v, c, *ino, *write),
        _ => Err(ErrCode::FEATURE_UNSUPPORTED),
    }
}
