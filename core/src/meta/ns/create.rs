//! 创建（design.md 3.5.1 的示例代码与 KV 影响表）：create、mkdir、mknod、symlink、link。
//!
//! 顺序与 3.5.1 的示例相同：名字检查 → 父目录（`ENOENT`/`ENOTDIR`）→ authz（对父目录要写 + 执行）→ 已存在 → `EEXIST` →
//! 分配 ino → 权限位（父目录有默认 ACL → 按 ACL 规则、不用 umask；否则 `mode & !umask`）→ 普通文件按当前生效属性选好链
//! （`new_layout`，必要时 `gen_chains`；inline_max > 0 时置 F_INLINE）→ 写 inode、目录项、继承的 ACL、父目录 → changelog。
//! 字符设备与块设备节点只有 root 能建（`EPERM`，与 Linux 的 CAP_MKNOD 相同）。
//! `Create` 不带 O_EXCL 时名字可能已存在（要按名字打开它），所以先查"能不能在父目录里查名字"（执行权限，与 lookup 相同；
//! 没有 → `EACCES`，不泄露名字在不在），再看名字：已存在 → O_EXCL 回 `EEXIST`、目录回 `EISDIR`、否则要对它有写权限。

use super::acl;
use super::common::{
    MAX_LINKS, MAX_SYMLINK, Node, S_ISVTX, alloc_ino, attr_out, authz, bump_ctime, check_name, child, dir, get,
    link_dentry, may_read, new_node, open_out, save, searchable_dir, set_new_layout, update_dir,
};
use super::xattr::{ACL_DEFAULT, get_xattr, put_acls};
use crate::base::ErrCode;
use crate::hooks::traits::AuthzOp;
use crate::meta::changelog::{self, Change};
use crate::meta::engine::txn::{KvTxn, OpCtx};
use crate::meta::schema::records::{
    F_ORPHAN, KIND_BLK, KIND_CHR, KIND_DIR, KIND_FIFO, KIND_FILE, KIND_SOCK, KIND_SYMLINK,
};
use crate::proto::meta::MetaReply;

/// 一个待建的 inode。
struct NewInode<'a> {
    name: &'a [u8],
    kind: u8,
    mode: u32,
    umask: u32,
    rdev: u32,
    target: &'a [u8], // 符号链接的目标
    op: &'static str, // changelog 的操作名
}

/// 在目录 `p` 里建一个新 inode（名字已检查、确认不存在、authz 已通过；见文件头）。
fn make(v: &mut dyn KvTxn, c: &OpCtx, p: &mut Node, o: NewInode) -> Result<Node, ErrCode> {
    let ino = alloc_ino(v)?;
    let perms = if o.kind == KIND_SYMLINK {
        acl::NewPerms { mode: 0o777, access_acl: None, default_acl: None } // 符号链接的权限位恒为 0777，也不带 ACL
    } else {
        let pacl = get_xattr(v, p.ino, ACL_DEFAULT)?;
        acl::on_create(pacl.as_deref(), o.mode, o.umask, o.kind == KIND_DIR)?
    };
    let mut n = new_node(c, ino, o.kind, perms.mode, p, o.name);
    n.rec.rdev = o.rdev;
    if o.kind == KIND_SYMLINK {
        (n.rec.symlink, n.rec.size) = (o.target.to_vec(), o.target.len() as u64);
    }
    if o.kind == KIND_FILE {
        set_new_layout(v, c, &mut n)?; // epoch 1；生成了新链时本批在这个操作之后结束（3.4.1.4 规则 5）
    }
    put_acls(v, &mut n, perms.access_acl, perms.default_acl);
    save(v, &n);
    link_dentry(v, p.ino, o.name, &n);
    update_dir(v, c, p, 1, (o.kind == KIND_DIR) as i64);
    changelog::append(v, c, Change { op: o.op, ino, parent: p.ino, name: o.name, ..Default::default() })?;
    Ok(n)
}

/// 父目录、authz、不得已存在：mkdir、mknod、symlink 的公共前半段。
fn prepare(v: &mut dyn KvTxn, c: &OpCtx, parent: u64, name: &[u8], op: AuthzOp) -> Result<Node, ErrCode> {
    check_name(name)?;
    let p = dir(v, parent)?;
    authz(v, c, op, None, Some(&p), None)?;
    if child(v, parent, name)?.is_some() {
        return Err(ErrCode::EEXIST);
    }
    Ok(p)
}

/// `Create`：普通文件，回 `Opened(OpenOut)`。已存在且不是 O_EXCL → 打开已有的文件（要有写权限；见文件头）。
pub fn create(
    v: &mut dyn KvTxn,
    c: &OpCtx,
    parent: u64,
    name: &[u8],
    mode: u32,
    umask: u32,
    excl: bool,
) -> Result<MetaReply, ErrCode> {
    check_name(name)?;
    let mut p = searchable_dir(v, c, parent)?;
    if let Some(n) = child(v, parent, name)? {
        if excl {
            return Err(ErrCode::EEXIST);
        }
        if n.is_dir() {
            return Err(ErrCode::EISDIR);
        }
        authz(v, c, AuthzOp::Open { read: false, write: true }, Some(&n), None, None)?;
        return Ok(MetaReply::Opened(open_out(v, &n, may_read(v, c, &n))?));
    }
    authz(v, c, AuthzOp::Create, None, Some(&p), None)?;
    let o = NewInode { name, kind: KIND_FILE, mode, umask, rdev: 0, target: b"", op: "Create" };
    let n = make(v, c, &mut p, o)?;
    Ok(MetaReply::Opened(open_out(v, &n, true)?))
}

pub fn mkdir(
    v: &mut dyn KvTxn,
    c: &OpCtx,
    parent: u64,
    name: &[u8],
    mode: u32,
    umask: u32,
) -> Result<MetaReply, ErrCode> {
    let mut p = prepare(v, c, parent, name, AuthzOp::Mkdir)?;
    let mode = mode & (0o777 | S_ISVTX); // mkdir 只认权限位与粘滞位；setgid 只从父目录继承
    let o = NewInode { name, kind: KIND_DIR, mode, umask, rdev: 0, target: b"", op: "Mkdir" };
    Ok(MetaReply::Entry { attr: attr_out(&make(v, c, &mut p, o)?) })
}

/// `Mknod`：fifo、socket、字符/块设备节点（只存元数据），也接受普通文件（与 create 一样选好链，回 `Entry`）。
#[allow(clippy::too_many_arguments)] // 参数就是 MetaOp::Mknod 的字段
pub fn mknod(
    v: &mut dyn KvTxn,
    c: &OpCtx,
    parent: u64,
    name: &[u8],
    mode: u32,
    umask: u32,
    kind: u8,
    rdev: u32,
) -> Result<MetaReply, ErrCode> {
    if ![KIND_FILE, KIND_FIFO, KIND_SOCK, KIND_CHR, KIND_BLK].contains(&kind) {
        return Err(ErrCode::EINVAL);
    }
    if (kind == KIND_CHR || kind == KIND_BLK) && c.req.who.uid != 0 {
        return Err(ErrCode::EPERM);
    }
    let mut p = prepare(v, c, parent, name, AuthzOp::Mknod)?;
    let rdev = if kind == KIND_CHR || kind == KIND_BLK { rdev } else { 0 };
    let o = NewInode { name, kind, mode, umask, rdev, target: b"", op: "Mknod" };
    Ok(MetaReply::Entry { attr: attr_out(&make(v, c, &mut p, o)?) })
}

/// `Symlink`：目标 1–4096 字节、不含 NUL。
pub fn symlink(v: &mut dyn KvTxn, c: &OpCtx, parent: u64, name: &[u8], target: &[u8]) -> Result<MetaReply, ErrCode> {
    if target.is_empty() {
        return Err(ErrCode::ENOENT);
    }
    if target.len() > MAX_SYMLINK {
        return Err(ErrCode::ENAMETOOLONG);
    }
    if target.contains(&0) {
        return Err(ErrCode::EINVAL);
    }
    let mut p = prepare(v, c, parent, name, AuthzOp::Symlink)?;
    let o = NewInode { name, kind: KIND_SYMLINK, mode: 0o777, umask: 0, rdev: 0, target, op: "Symlink" };
    Ok(MetaReply::Entry { attr: attr_out(&make(v, c, &mut p, o)?) })
}

/// `Link`：硬链接（只能链接文件，目录 → `EPERM`；每个文件 ≤ 1000 个链接 → `EMLINK`）。
pub fn link(v: &mut dyn KvTxn, c: &OpCtx, ino: u64, new_parent: u64, new_name: &[u8]) -> Result<MetaReply, ErrCode> {
    check_name(new_name)?;
    let mut n = get(v, ino)?;
    if n.is_dir() {
        return Err(ErrCode::EPERM);
    }
    let mut p = dir(v, new_parent)?;
    authz(v, c, AuthzOp::Link, Some(&n), Some(&p), None)?;
    if child(v, new_parent, new_name)?.is_some() {
        return Err(ErrCode::EEXIST);
    }
    if n.rec.nlink == 0 || n.rec.flags & F_ORPHAN != 0 {
        return Err(ErrCode::ENOENT); // 已经删光了链接的文件（只剩打开着的人）不能再被链接回来
    }
    if n.rec.nlink >= MAX_LINKS {
        return Err(ErrCode::EMLINK);
    }
    n.rec.nlink += 1;
    n.rec.extra_links.push((new_parent, new_name.to_vec()));
    bump_ctime(&mut n, c);
    save(v, &n);
    link_dentry(v, new_parent, new_name, &n);
    update_dir(v, c, &mut p, 1, 0);
    changelog::append(v, c, Change { op: "Link", ino, parent: new_parent, name: new_name, ..Default::default() })?;
    Ok(MetaReply::Entry { attr: attr_out(&n) })
}
