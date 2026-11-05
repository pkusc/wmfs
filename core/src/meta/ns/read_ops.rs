//! 只读的命名空间操作（design.md 3.3.4 的"R"一组；在 meta-read 线程上、ReadIndex 之后执行）：lookup、getattr、readdir
//! （按名字分页）、readlink、按路径解析（CLI / 调试客户端）、open（取最新属性与 layout，不写 raft）。
//!
//! readdir：前缀扫描 `02 ino`，从 `after` 之后（不含）开始，最多 `limit`（≤ 1024）条，回复编码 ≤ 512 KiB（先到先停，
//! `eof = false`）；`plus` 时每条带属性（多一次点查）。目录里没有 `.` 与 `..` 这两项（FUSE 适配层自己补，M6）。
//! 实现注记（M4）：`Open` 先只回属性、layout 与 inline 内容（只在读打开时带，`common` 的实现注记；`wmfs fs stat` 用它看
//! layout）；配额额度 M10 已实现（`meta/quota.rs`：open/create/CommitWrite 的 `quota_end`、精确检查与护栏）。实现注记（M5）：
//! cap 按打开方式带读/写权限位（`common::sign_cap`）；签之前先核对会话（`sessions::may_sign_cap`：只读路径不经执行器的会话检查）。
//! 实现注记（M6）：读打开普通文件时按 relatime 规则给出 `atime_due`（`file_ops::atime_due`），客户端据此排队 `SetAtime`。
//! `ResolvePath` 不跟随中间的符号链接（中间的一段是符号链接 → `ENOTDIR`），最后一段是符号链接时返回它本身（像 lstat）。

use super::common::{Node, attr_out, authz, check_name, child, dir, get, is_file, open_out, searchable_dir};
use super::file_ops::atime_due;
use crate::base::{ErrCode, Failed};
use crate::hooks::traits::AuthzOp;
use crate::meta::attrs::MAX_DEPTH;
use crate::meta::engine::txn::{KvRead, KvScan, OpCtx};
use crate::meta::schema::keys::{self, ROOT_INO};
use crate::meta::schema::records::{DentryV1, KIND_SYMLINK, decode_v1};
use crate::meta::sessions;
use crate::proto::cap::{RIGHT_READ, RIGHT_WRITE};
use crate::proto::encode_body;
use crate::proto::meta::{DirEntryOut, MetaReply, O_ACCMODE, O_RDONLY, O_WRONLY};

/// readdir 一页的上限：条数、回复的编码字节数（3.3.4）。
pub const MAX_READDIR: u32 = 1024;
const MAX_REPLY: usize = 512 << 10;
/// 路径的长度上限（与 Linux 的 PATH_MAX 相同）。
const MAX_PATH: usize = 4096;

/// 按文件系统内的绝对路径找 inode（管理 API 用，与 `ResolvePath` 相同：逐段查名字、每段要执行权限）。实现注记（M10，ADR-0038
/// 第 2 条）：再按"读这个对象的属性"（`AuthzOp::GetXattr`）过一次授权——管理 API 以 uid 0 解析路径，posix-mode 放行，有路径限制的
/// key 由 path-caps 判：只能查它允许的子树（越界 → `EACCES`，管理 API 回 403）。M10 从 `attr_ops` 挪来（M7 写在那里）。
pub fn node_at(v: &dyn KvRead, c: &OpCtx, path: &str) -> Result<Node, Failed> {
    let ino = match resolve(v, c, path.as_bytes()) {
        Ok(MetaReply::Resolved { ino, .. }) => ino,
        Ok(_) => return Err(ErrCode::INTERNAL.into()),
        Err(ErrCode::ENOENT) => return Err(Failed::new(ErrCode::ENOENT, format!("路径 {path} 不存在"))),
        Err(ErrCode::EINVAL) => {
            return Err(Failed::new(ErrCode::EINVAL, format!("路径应是文件系统内的绝对路径（以 / 开头）：{path}")));
        }
        Err(e) => return Err(Failed::new(e, format!("{path}：{e}"))),
    };
    let n = get(v, ino)?;
    authz(v, c, AuthzOp::GetXattr, Some(&n), None, None).map_err(|e| {
        Failed::new(e, format!("{path}：这把 key 不能访问它（{e}；有路径限制的 key 只能查允许的子树）"))
    })?;
    Ok(n)
}

/// 一个 inode 的路径（沿主链接的 parent/name 拼到根；走了 `MAX_DEPTH` 步还没到根 = parent 指针损坏）。
pub fn path_of(v: &dyn KvRead, ino: u64) -> Result<String, ErrCode> {
    let mut names = Vec::new();
    let mut cur = get(v, ino)?;
    while cur.ino != ROOT_INO {
        if names.len() >= MAX_DEPTH {
            return Err(ErrCode::INTERNAL);
        }
        names.push(String::from_utf8_lossy(&cur.rec.name).into_owned());
        cur = get(v, cur.rec.parent)?;
    }
    names.reverse();
    Ok(format!("/{}", names.join("/")))
}

pub fn lookup(v: &dyn KvRead, c: &OpCtx, parent: u64, name: &[u8]) -> Result<MetaReply, ErrCode> {
    check_name(name)?;
    searchable_dir(v, c, parent)?;
    let n = child(v, parent, name)?.ok_or(ErrCode::ENOENT)?;
    Ok(MetaReply::Entry { attr: attr_out(v, c, &n)? })
}

pub fn getattr(v: &dyn KvRead, c: &OpCtx, ino: u64) -> Result<MetaReply, ErrCode> {
    let n = get(v, ino)?;
    authz(v, c, AuthzOp::GetAttr, Some(&n), None, None)?;
    Ok(MetaReply::Entry { attr: attr_out(v, c, &n)? })
}

pub fn readdir(
    v: &dyn KvScan,
    c: &OpCtx,
    ino: u64,
    after: &[u8],
    limit: u32,
    plus: bool,
) -> Result<MetaReply, ErrCode> {
    let d = dir(v, ino)?;
    authz(v, c, AuthzOp::ReadDir, Some(&d), None, None)?;
    let limit = limit.clamp(1, MAX_READDIR) as usize;
    let prefix = keys::dentry_prefix(ino);
    let start = if after.is_empty() { prefix.clone() } else { [prefix.as_slice(), after, &[0]].concat() };
    let rows = v.scan(&start, &keys::dentry_prefix(ino + 1), limit + 1)?;
    let mut eof = rows.len() <= limit;
    let (mut entries, mut bytes) = (Vec::new(), 0);
    for (k, b) in rows.into_iter().take(limit) {
        let de: DentryV1 = decode_v1(&b)?;
        let attr = if plus { Some(attr_out(v, c, &get(v, de.ino)?)?) } else { None };
        let e = DirEntryOut { name: k[prefix.len()..].to_vec(), ino: de.ino, kind: de.kind, attr };
        bytes += encode_body(&e).len();
        if bytes > MAX_REPLY && !entries.is_empty() {
            eof = false; // 这一页满了：客户端从最后一项之后接着要
            break;
        }
        entries.push(e);
    }
    Ok(MetaReply::Dir { entries, eof })
}

pub fn readlink(v: &dyn KvRead, c: &OpCtx, ino: u64) -> Result<MetaReply, ErrCode> {
    let n = get(v, ino)?;
    if n.rec.kind != KIND_SYMLINK {
        return Err(ErrCode::EINVAL);
    }
    authz(v, c, AuthzOp::ReadLink, Some(&n), None, None)?;
    Ok(MetaReply::Link { target: n.rec.symlink })
}

/// 绝对路径 → inode：从根逐段 lookup（每一段都要对所在目录有执行权限）；`.` 跳过，`..` 回到父目录（根的 `..` 是根）。
pub fn resolve(v: &dyn KvRead, c: &OpCtx, path: &[u8]) -> Result<MetaReply, ErrCode> {
    if path.first() != Some(&b'/') {
        return Err(ErrCode::EINVAL);
    }
    if path.len() > MAX_PATH {
        return Err(ErrCode::ENAMETOOLONG);
    }
    let mut cur: Node = get(v, ROOT_INO)?;
    for comp in path.split(|b| *b == b'/').filter(|s| !s.is_empty() && *s != b".") {
        if !cur.is_dir() {
            return Err(ErrCode::ENOTDIR);
        }
        authz(v, c, AuthzOp::Lookup, None, Some(&cur), None)?;
        cur = match comp {
            b".." if cur.ino == ROOT_INO => cur,
            b".." => get(v, cur.rec.parent)?,
            _ => {
                check_name(comp)?;
                child(v, cur.ino, comp)?.ok_or(ErrCode::ENOENT)?
            }
        };
    }
    Ok(MetaReply::Resolved { ino: cur.ino, kind: cur.rec.kind })
}

/// `Open`：按打开方式（O_ACCMODE）要读或写权限；目录不能写打开（`EISDIR`）。只在读打开时带 inline 内容。
pub fn open(v: &dyn KvRead, c: &OpCtx, ino: u64, flags: u32) -> Result<MetaReply, ErrCode> {
    let n = get(v, ino)?;
    let (read, write) = (flags & O_ACCMODE != O_WRONLY, flags & O_ACCMODE != O_RDONLY);
    if n.is_dir() && write {
        return Err(ErrCode::EISDIR);
    }
    authz(v, c, AuthzOp::Open { read, write }, Some(&n), None, None)?;
    sessions::may_sign_cap(v, c.req)?;
    let rights = if read { RIGHT_READ } else { 0 } | if write { RIGHT_WRITE } else { 0 };
    let mut o = open_out(v, c, &n, read, rights)?;
    o.atime_due = read && is_file(&n) && atime_due(&n, c.req.now); // relatime：客户端排队一次 SetAtime（M6）
    Ok(MetaReply::Opened(o))
}
