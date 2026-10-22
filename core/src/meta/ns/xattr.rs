//! 扩展属性（design.md 3.1.1、3.5.1 的 setxattr/removexattr 行；键空间 0x03：每个 inode 一条 `XattrsV1`，按名字排序）。
//!
//! 支持的名字空间：`user.*`（只能挂在普通文件与目录上）、`security.*`（写只给 root）、`trusted.*`（读写都只给 root）、
//! `system.posix_acl_access`、`system.posix_acl_default`（只有属主或 root 能设；默认 ACL 只能挂在目录上）；其余 → `EOPNOTSUPP`。
//! 名字 ≤ 255 字节（`ERANGE`）、单个值 ≤ 64 KiB（`E2BIG`）、一个 inode 合计 ≤ 64 KiB（`ENOSPC`）。
//! 设 `system.posix_acl_access` 同时改 mode 与 `F_HAS_ACL`：与权限位等价的 ACL 不存，只改权限位（与 Linux 相同）。
//!
//! 实现注记（M4）：名字空间的规则（哪些名字空间存在、trusted/security 只给 root、粘滞目录上 user.* 只有属主能写、ACL 只有
//! 属主能设）在 core 里检查——`AuthzInput` 里没有 xattr 的名字，插件看不到；对 inode 本身的读/写权限（user.*）仍交给 authz 钩子
//! （`GetXattr`/`SetXattr`/`RemoveXattr`）。规则照 Linux 的 xattr_permission 与 cap_inode_setxattr。

use super::acl;
use super::common::{Node, S_ISGID, S_ISVTX, authz, bump_ctime, get, in_group, is_file, save};
use crate::base::ErrCode;
use crate::hooks::traits::AuthzOp;
use crate::meta::changelog::{self, Change};
use crate::meta::engine::txn::{KvRead, KvTxn, OpCtx};
use crate::meta::schema::keys;
use crate::meta::schema::kv;
use crate::meta::schema::records::{F_HAS_ACL, XattrsV1};
use crate::proto::meta::{MetaReply, XATTR_CREATE, XATTR_REPLACE};

pub const ACL_ACCESS: &[u8] = b"system.posix_acl_access";
pub const ACL_DEFAULT: &[u8] = b"system.posix_acl_default";
const MAX_XNAME: usize = 255;
const MAX_VALUE: usize = 64 << 10;
const MAX_TOTAL: usize = 64 << 10;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Space {
    User,
    Trusted,
    Security,
    Acl, // system.posix_acl_access / default
}

fn space(name: &[u8]) -> Result<Space, ErrCode> {
    if name.is_empty() || name.len() > MAX_XNAME {
        return Err(ErrCode::ERANGE);
    }
    let after = |p: &[u8]| name.len() > p.len() && name.starts_with(p);
    match name {
        n if n == ACL_ACCESS || n == ACL_DEFAULT => Ok(Space::Acl),
        _ if after(b"user.") => Ok(Space::User),
        _ if after(b"trusted.") => Ok(Space::Trusted),
        _ if after(b"security.") => Ok(Space::Security),
        _ => Err(ErrCode::EOPNOTSUPP),
    }
}

fn load(v: &dyn KvRead, ino: u64) -> Result<XattrsV1, ErrCode> {
    Ok(kv::get(v, &keys::xattr(ino))?.unwrap_or(XattrsV1 { entries: vec![] }))
}

fn store(v: &mut dyn KvTxn, ino: u64, x: &XattrsV1) {
    if x.entries.is_empty() { v.delete(&keys::xattr(ino)) } else { kv::put(v, &keys::xattr(ino), x) }
}

/// 一个 xattr 的原始值（不做任何权限检查；authz 取 ACL、create 取父目录的默认 ACL 用）。
pub fn get_xattr(v: &dyn KvRead, ino: u64, name: &[u8]) -> Result<Option<Vec<u8>>, ErrCode> {
    Ok(load(v, ino)?.entries.into_iter().find(|(k, _)| k == name).map(|(_, x)| x))
}

/// 新建 inode 时写它继承来的 ACL（访问 ACL 与新目录的默认 ACL；都没有就什么也不写）。
pub fn put_acls(v: &mut dyn KvTxn, n: &mut Node, access: Option<Vec<u8>>, default: Option<Vec<u8>>) {
    let mut entries = vec![];
    if let Some(a) = access {
        n.rec.flags |= F_HAS_ACL;
        entries.push((ACL_ACCESS.to_vec(), a));
    }
    entries.extend(default.map(|d| (ACL_DEFAULT.to_vec(), d)));
    store(v, n.ino, &XattrsV1 { entries });
}

/// chmod 之后同步访问 ACL 的 USER_OBJ / MASK / OTHER（3.5.1 的 setattr 行）。
pub fn sync_acl_mode(v: &mut dyn KvTxn, n: &Node) -> Result<(), ErrCode> {
    let mut x = load(v, n.ino)?;
    if let Some((_, val)) = x.entries.iter_mut().find(|(k, _)| k == ACL_ACCESS) {
        let mut a = acl::parse(val)?;
        acl::chmod(&mut a, n.rec.mode);
        *val = acl::encode(&a);
        store(v, n.ino, &x);
    }
    Ok(())
}

fn root(c: &OpCtx) -> bool {
    c.req.who.uid == 0
}

pub fn getxattr(v: &dyn KvRead, c: &OpCtx, ino: u64, name: &[u8]) -> Result<MetaReply, ErrCode> {
    let n = get(v, ino)?;
    match space(name)? {
        Space::Trusted if !root(c) => return Err(ErrCode::ENODATA),
        Space::User if !is_file(&n) && !n.is_dir() => return Err(ErrCode::ENODATA),
        Space::User => authz(v, c, AuthzOp::GetXattr, Some(&n), None, None)?,
        _ => {} // security.* 与 ACL 谁都能读（与 Linux 相同）
    }
    get_xattr(v, ino, name)?.map(|value| MetaReply::Xattr { value }).ok_or(ErrCode::ENODATA)
}

pub fn listxattr(v: &dyn KvRead, c: &OpCtx, ino: u64) -> Result<MetaReply, ErrCode> {
    let n = get(v, ino)?;
    authz(v, c, AuthzOp::ListXattr, Some(&n), None, None)?;
    let names = load(v, ino)?.entries.into_iter().map(|(k, _)| k);
    let names = names.filter(|k| root(c) || !k.starts_with(b"trusted.")).collect();
    Ok(MetaReply::XattrNames { names })
}

/// 写类操作的名字空间规则（见文件头），之后 user.* 再过 authz 钩子（对 inode 要有写权限）。
fn may_write(v: &dyn KvRead, c: &OpCtx, n: &Node, s: Space, op: AuthzOp) -> Result<(), ErrCode> {
    let (owner, r) = (c.req.who.uid == n.rec.uid, root(c));
    match s {
        Space::Trusted | Space::Security if !r => Err(ErrCode::EPERM),
        Space::Acl if !r && !owner => Err(ErrCode::EPERM),
        Space::User if !is_file(n) && !n.is_dir() => Err(ErrCode::EPERM),
        Space::User if n.is_dir() && n.rec.mode & S_ISVTX != 0 && !r && !owner => Err(ErrCode::EPERM),
        Space::User => authz(v, c, op, Some(n), None, None),
        _ => Ok(()),
    }
}

pub fn setxattr(
    v: &mut dyn KvTxn,
    c: &OpCtx,
    ino: u64,
    name: &[u8],
    value: &[u8],
    flags: u32,
) -> Result<MetaReply, ErrCode> {
    let mut n = get(v, ino)?;
    let s = space(name)?;
    if flags & !(XATTR_CREATE | XATTR_REPLACE) != 0 || flags == XATTR_CREATE | XATTR_REPLACE {
        return Err(ErrCode::EINVAL);
    }
    if value.len() > MAX_VALUE {
        return Err(ErrCode::E2BIG);
    }
    may_write(v, c, &n, s, AuthzOp::SetXattr)?;
    let mut x = load(v, ino)?;
    let pos = x.entries.iter().position(|(k, _)| k == name);
    match (pos, flags) {
        (Some(_), XATTR_CREATE) => return Err(ErrCode::EEXIST),
        (None, XATTR_REPLACE) => return Err(ErrCode::ENODATA),
        _ => {}
    }
    let mut keep = Some(value.to_vec()); // None = 不存这一项（与权限位等价的访问 ACL、空的默认 ACL）
    if name == ACL_ACCESS {
        let a = acl::parse(value)?;
        let (mode, extended) = acl::equiv_mode(&a, n.rec.mode);
        if !a.is_empty() {
            n.rec.mode = mode;
            if !root(c) && !in_group(&c.req.who, n.rec.gid) {
                n.rec.mode &= !S_ISGID; // 与 chmod 相同：不在文件所属组里的人设不了 setgid
            }
        }
        keep = keep.filter(|_| extended);
        n.rec.flags = if extended { n.rec.flags | F_HAS_ACL } else { n.rec.flags & !F_HAS_ACL };
    } else if name == ACL_DEFAULT {
        if !n.is_dir() {
            return Err(ErrCode::EACCES); // 默认 ACL 只能挂在目录上（与 Linux 相同的错误码）
        }
        if acl::parse(value)?.is_empty() {
            keep = None; // 只有头的默认 ACL = 删除它
        }
    }
    match (pos, keep) {
        (Some(i), Some(val)) => x.entries[i].1 = val,
        (Some(i), None) => {
            x.entries.remove(i);
        }
        (None, Some(val)) => {
            x.entries.insert(x.entries.partition_point(|(k, _)| k.as_slice() < name), (name.to_vec(), val))
        }
        (None, None) => {}
    }
    if x.entries.iter().map(|(k, val)| k.len() + val.len()).sum::<usize>() > MAX_TOTAL {
        return Err(ErrCode::ENOSPC);
    }
    store(v, ino, &x);
    bump_ctime(&mut n, c);
    save(v, &n);
    changelog::append(v, c, Change { op: "SetXattr", ino, name, ..Default::default() })?;
    Ok(MetaReply::Ok)
}

pub fn removexattr(v: &mut dyn KvTxn, c: &OpCtx, ino: u64, name: &[u8]) -> Result<MetaReply, ErrCode> {
    let mut n = get(v, ino)?;
    let s = space(name)?;
    if s == Space::Trusted && !root(c) {
        return Err(ErrCode::ENODATA); // 看不见的名字：与读一致
    }
    may_write(v, c, &n, s, AuthzOp::RemoveXattr)?;
    let mut x = load(v, ino)?;
    let i = x.entries.iter().position(|(k, _)| k == name).ok_or(ErrCode::ENODATA)?;
    x.entries.remove(i);
    store(v, ino, &x);
    if name == ACL_ACCESS {
        n.rec.flags &= !F_HAS_ACL; // 权限位不变（与 Linux 相同）
    }
    bump_ctime(&mut n, c);
    save(v, &n);
    changelog::append(v, c, Change { op: "RemoveXattr", ino, name, ..Default::default() })?;
    Ok(MetaReply::Ok)
}
