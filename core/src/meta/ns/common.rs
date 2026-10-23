//! 命名空间操作的公共部分（design.md 3.5.1、3.12）：按 inode 号与目录项读写、名字检查、分配 inode 号、新 inode 的
//! 属主与权限位（setgid 继承）、`AttrOut`/`OpenOut`、调 authz 钩子、`new_layout`（选链，必要时生成新链）、目录的
//! mtime/ctime/size/nlink/change_id、硬链接列表、孤儿标记。命名空间代码只用这些函数碰 KV（全部是点查，3.4.1.4 规则 1）。
//!
//! 现状：`AttrOut` 的 `consistency`、`ttl_ms` 按管辖目录（目录自己、文件的主父目录）的生效属性给出（[`governing`]）；新文件的
//! layout 在 `ns/layout.rs`（`new_layout`/`set_new_layout`，这里原样转出）；递归计数 RSTAT 由各操作调 `meta/rstat.rs` 更新；
//! `quota_end = u64::MAX`（配额在 M10）。`OpenOut.cap` 在闭包里签（[`sign_cap`]，有效期 `cap_ttl`）：权限位按这次打开的方式给
//! （Open 按 O_ACCMODE；Create 新建的文件读写都给，打开已有的文件给写、能读才再给读；改了长度的 SetAttr 同后者）；`qos_id` 先填 0
//! （qos 属性在 M13）。`OpenOut.inline`（文件的完整内容）只给能读这个文件的人：Open 按这次是否读打开，Create 打开已有的文件、
//! 改了 size 的 SetAttr 按调用者有没有读权限（[`may_read`]）；不带时为 None，要内容就 GetInline。
//!
//! 历史注记：M4 时 `consistency`、`ttl_ms` 取内置默认值（cto、attr_ttl 1 s），RSTAT 与配额都还不碰（M7 已实现前两者：见
//! `attrs.rs`、`rstat.rs`）；cap 与 inline 的规则是 M5 加的。

use std::sync::Arc;

use crate::base::ErrCode;
use crate::base::types::Principal;
use crate::hooks::traits::{AuthzInput, AuthzOp, InodeView};
use crate::meta::attrs::{CONSISTENCY_STRONG, EffectiveAttrs};
use crate::meta::engine::txn::{KvRead, KvTxn, OpCtx};
use crate::meta::ns::xattr::{ACL_ACCESS, get_xattr};
use crate::meta::schema::keys::{self, G_NEXT_INO, GC_ORPHAN, ROOT_INO};
use crate::meta::schema::kv;
use crate::meta::schema::records::{
    DentryV1, F_HAS_ACL, F_INLINE, F_ORPHAN, GcRecV1, InodeV1, KIND_BLK, KIND_CHR, KIND_DIR, KIND_FIFO, KIND_FILE,
    KIND_SOCK, LayoutV1,
};
use crate::proto::cap::{self, CapV1, RIGHT_READ, RIGHT_WRITE};
use crate::proto::meta::{AttrOut, OpenOut};

pub use super::layout::{new_layout, set_new_layout};

pub const S_ISUID: u32 = 0o4000;
pub const S_ISGID: u32 = 0o2000;
pub const S_ISVTX: u32 = 0o1000;
pub const S_IXGRP: u32 = 0o010;
/// InodeV1.mode 只存权限位 + suid/sgid/sticky（不含类型位）。
pub const MODE_MASK: u32 = 0o7777;

/// mknod 的 mode 里的类型位 → 记录里的种类（没给类型位 = 普通文件，与 mknod(2) 相同）；不认识 → None。实现注记（M6）：给 FUSE
/// 适配层用，cli 因此不必直接依赖 libc。
pub fn kind_from_mode(mode: u32) -> Option<u8> {
    match mode & libc::S_IFMT {
        0 => Some(KIND_FILE), // 没给类型位 = 普通文件（mknod(2)）
        libc::S_IFREG => Some(KIND_FILE),
        libc::S_IFIFO => Some(KIND_FIFO),
        libc::S_IFSOCK => Some(KIND_SOCK),
        libc::S_IFCHR => Some(KIND_CHR),
        libc::S_IFBLK => Some(KIND_BLK),
        _ => None,
    }
}
/// 名字 1–255 字节；每个文件 ≤ 1000 个硬链接；符号链接目标 ≤ 4096 字节（3.1.1）。
pub const MAX_NAME: usize = 255;
pub const MAX_LINKS: u32 = 1000;
pub const MAX_SYMLINK: usize = 4096;
/// chunk 里块的大小（3.2.4：min(64 KiB, chunk_size)）。
pub const BLOCK_SIZE: u32 = 64 << 10;
/// 孤儿的 GC 记录多久之后才到期（3.5.6：2 个心跳周期，确保"刚 open 的客户端"已上报）。
const ORPHAN_DELAY_MS: u64 = 6000;

/// 一个 inode：号 + 记录。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Node {
    pub ino: u64,
    pub rec: InodeV1,
}

impl Node {
    pub fn is_dir(&self) -> bool {
        self.rec.kind == KIND_DIR
    }
}

/// 按 inode 号读；不存在 → `ENOENT`。
pub fn get(v: &dyn KvRead, ino: u64) -> Result<Node, ErrCode> {
    let rec = kv::get::<InodeV1>(v, &keys::inode(ino))?.ok_or(ErrCode::ENOENT)?;
    Ok(Node { ino, rec })
}

/// 按 inode 号读一个目录；不是目录 → `ENOTDIR`。
pub fn dir(v: &dyn KvRead, ino: u64) -> Result<Node, ErrCode> {
    let n = get(v, ino)?;
    if !n.is_dir() {
        return Err(ErrCode::ENOTDIR);
    }
    Ok(n)
}

/// 父目录，并且调用者能在里面查名字（执行权限，`AuthzOp::Lookup`）：按名字找目录项之前先过这一关——lookup、unlink、rmdir、
/// rename、按名字打开已有文件的 create 都这样，没有权限的人从 `ENOENT`/`EEXIST` 与 `EACCES` 的区别里看不出目录里有哪些名字。
pub fn searchable_dir(v: &dyn KvRead, c: &OpCtx, parent: u64) -> Result<Node, ErrCode> {
    let p = dir(v, parent)?;
    authz(v, c, AuthzOp::Lookup, None, Some(&p), None)?;
    Ok(p)
}

pub fn save(v: &mut dyn KvTxn, n: &Node) {
    kv::put(v, &keys::inode(n.ino), &n.rec);
}

/// 目录 `parent` 里名为 `name` 的 inode（没有 → None）。
pub fn child(v: &dyn KvRead, parent: u64, name: &[u8]) -> Result<Option<Node>, ErrCode> {
    match kv::get::<DentryV1>(v, &keys::dentry(parent, name))? {
        Some(d) => get(v, d.ino).map(Some),
        None => Ok(None),
    }
}

pub fn link_dentry(v: &mut dyn KvTxn, parent: u64, name: &[u8], n: &Node) {
    kv::put(v, &keys::dentry(parent, name), &DentryV1 { ino: n.ino, kind: n.rec.kind });
}

/// 名字：1–255 字节，不含 `/` 与 NUL，不是 `.`、`..`（3.1.1）。
pub fn check_name(name: &[u8]) -> Result<(), ErrCode> {
    match name {
        [] => Err(ErrCode::ENOENT),
        n if n.len() > MAX_NAME => Err(ErrCode::ENAMETOOLONG),
        b"." | b".." => Err(ErrCode::EINVAL),
        n if n.iter().any(|b| *b == b'/' || *b == 0) => Err(ErrCode::EINVAL),
        _ => Ok(()),
    }
}

pub fn in_group(who: &Principal, gid: u32) -> bool {
    who.gid == gid || who.groups.contains(&gid)
}

/// 新 inode（还没写进 KV）。属主 = 调用者；父目录有 setgid → gid 继承父目录，新目录也带 setgid；新文件带着
/// setgid 与组执行位、而调用者不在那个组里（也不是 root）→ 去掉 setgid（与 Linux 的 inode_init_owner 相同）。
pub fn new_node(c: &OpCtx, ino: u64, kind: u8, mode: u32, parent: &Node, name: &[u8]) -> Node {
    let (who, now) = (&c.req.who, c.req.now);
    let mut mode = mode & MODE_MASK;
    let gid = if parent.rec.mode & S_ISGID == 0 {
        who.gid
    } else {
        if kind == KIND_DIR {
            mode |= S_ISGID;
        } else if mode & (S_ISGID | S_IXGRP) == (S_ISGID | S_IXGRP) && !in_group(who, parent.rec.gid) && who.uid != 0 {
            mode &= !S_ISGID;
        }
        parent.rec.gid
    };
    let rec = InodeV1 {
        kind,
        mode,
        uid: who.uid,
        gid,
        nlink: if kind == KIND_DIR { 2 } else { 1 },
        size: 0,
        atime: now,
        mtime: now,
        ctime: now,
        btime: now,
        rdev: 0,
        parent: parent.ino,
        name: name.to_vec(),
        extra_links: vec![],
        flags: 0,
        change_id: 1,
        class_id: 0,
        layout: None,
        pending_epoch: 0,
        max_epoch: 0,
        trash_expire_ms: 0,
        symlink: vec![],
    };
    Node { ino, rec }
}

/// 分配一个新的 inode 号（单调、永不复用）。
pub fn alloc_ino(v: &mut dyn KvTxn) -> Result<u64, ErrCode> {
    kv::next_id(v, G_NEXT_INO)
}

/// 属性变了：ctime = now，change_id + 1（**不写回**，调用方之后 [`save`]）。
pub fn bump_ctime(n: &mut Node, c: &OpCtx) {
    n.rec.ctime = c.req.now;
    n.rec.change_id += 1;
}

/// 目录内容变了：size（直接子项数）与 nlink 按增量改，mtime = ctime = now，change_id + 1，**并写回**。
pub fn update_dir(v: &mut dyn KvTxn, c: &OpCtx, d: &mut Node, dsize: i64, dnlink: i64) {
    d.rec.size = d.rec.size.saturating_add_signed(dsize);
    d.rec.nlink = (d.rec.nlink as i64 + dnlink).clamp(0, u32::MAX as i64) as u32;
    d.rec.mtime = c.req.now;
    bump_ctime(d, c);
    save(v, d);
}

/// 一个 inode 的属性管辖目录的生效属性：目录 = 它自己，其他 = 主父目录（实现注记（M7），3.1.4 C1、3.3.4 的
/// `AttrOut.consistency`）；孤儿（nlink = 0）的主父目录可能已经删掉了 → 用根目录的。先点查它还在不在：解析缓存里可能还留着
/// 已删目录的项（删掉没有 DIRATTR 的目录不加 attr_epoch），不能靠解析失败来判断。
pub fn governing(v: &dyn KvRead, c: &OpCtx, n: &Node) -> Result<Arc<EffectiveAttrs>, ErrCode> {
    let mut dir = if n.is_dir() { n.ino } else { n.rec.parent };
    if !n.is_dir() && n.rec.nlink == 0 && kv::get::<InodeV1>(v, &keys::inode(dir))?.is_none() {
        dir = ROOT_INO;
    }
    c.attrs.resolve(v, dir)
}

/// 回给客户端的属性：`consistency` 与 `ttl_ms` 按管辖目录的生效属性（cto = attr_ttl，strong = 0，3.1.4 C1）。
pub fn attr_out(v: &dyn KvRead, c: &OpCtx, n: &Node) -> Result<AttrOut, ErrCode> {
    let e = governing(v, c, n)?;
    let ttl_ms = if e.consistency == CONSISTENCY_STRONG { 0 } else { e.attr_ttl_ms };
    let r = &n.rec;
    Ok(AttrOut {
        ino: n.ino,
        kind: r.kind,
        mode: r.mode,
        uid: r.uid,
        gid: r.gid,
        nlink: r.nlink,
        size: r.size,
        atime: r.atime,
        mtime: r.mtime,
        ctime: r.ctime,
        btime: r.btime,
        rdev: r.rdev,
        change_id: r.change_id,
        epoch: r.layout.as_ref().map_or(0, |l| l.epoch),
        consistency: e.consistency,
        ttl_ms,
        flags: r.flags,
    })
}

/// Open、Create、改了 size 的 SetAttr 共用的回复：属性、layout、带 `rights` 权限位的 cap（普通文件才有）；`with_inline`
/// 时再带上 inline 文件的完整内容（只给能读它的人，见文件头的实现注记）。
pub fn open_out(v: &dyn KvRead, c: &OpCtx, n: &Node, with_inline: bool, rights: u8) -> Result<OpenOut, ErrCode> {
    let inline = if with_inline && n.rec.flags & F_INLINE != 0 {
        Some(v.get(&keys::inline(n.ino))?.unwrap_or_default())
    } else {
        None
    };
    Ok(OpenOut {
        attr: attr_out(v, c, n)?,
        layout: n.rec.layout.clone(),
        cap: sign_cap(c, n, rights, c.req.session),
        inline,
        quota_end: u64::MAX,
        atime_due: false,
    })
}

/// 写权限 + （`can_read` 时）读权限：打开已有的文件来写（Create 不带 O_EXCL、改长度的 SetAttr、Uninline）的 cap 权限位。
pub fn write_rights(can_read: bool) -> u8 {
    RIGHT_WRITE | if can_read { RIGHT_READ } else { 0 }
}

/// 单个文件的最大长度（实现注记（M5），规格没写）：chunk 号 `ChunkId.idx` 是 u32，所以 ≤ chunk_size × 2³²（4 MiB 的 chunk 是
/// 16 PiB）；`st_size` 是有符号 64 位，所以 ≤ i64::MAX。改长度、CommitWrite、Fallocate 超过它 → `EFBIG`。
pub fn max_file_size(l: &LayoutV1) -> u64 {
    ((l.chunk_size as u64) << 32).min(i64::MAX as u64)
}

/// 给普通文件 `n` 签一张 cap（3.3.5）：layout 的链与变换参数、调用者的租户与作业、过期时间 = now + `cap_ttl`。
/// 没有 layout（目录等）或 `rights` 为 0 → 空。
pub fn sign_cap(c: &OpCtx, n: &Node, rights: u8, session: u64) -> Vec<u8> {
    let Some(l) = n.rec.layout.as_ref().filter(|_| rights != 0) else { return vec![] };
    let cap = CapV1 {
        ino: n.ino,
        epoch: l.epoch,
        rights,
        session,
        tenant: c.req.who.tenant,
        job_id: c.req.who.job_id,
        chains: l.chains.clone(),
        class_id: n.rec.class_id,
        qos_id: 0, // qos 属性在 M13
        chunk_size: l.chunk_size,
        block_size: l.block_size,
        checksum: l.checksum,
        compress: l.compress,
        compress_level: l.compress_level,
        encrypt: l.encrypt,
        encrypt_key: l.encrypt_key,
        expires_ms: c.req.now_ms.saturating_add(c.settings.cap_ttl_ms),
    };
    cap::sign(c.cap_key, &cap)
}

/// 去掉 setuid，以及带组执行位时的 setgid（不带组执行位的 setgid 表示强制锁，Linux 保留它）。chown 之后、非属主改长度或
/// 写入（CommitWrite）时调用（"非属主写入时清除 suid/sgid"）。
pub fn kill_suid(n: &mut Node) {
    n.rec.mode &= !S_ISUID;
    if n.rec.mode & S_IXGRP != 0 {
        n.rec.mode &= !S_ISGID;
    }
}

/// 调用者能不能读 `n` 的内容（只用来决定回复里带不带 inline 内容，不拒绝请求）。
pub fn may_read(v: &dyn KvRead, c: &OpCtx, n: &Node) -> bool {
    authz(v, c, AuthzOp::Open { read: true, write: false }, Some(n), None, None).is_ok()
}

/// 调 authz 钩子（posix-mode → posix-acl → path-caps，3.7.2）。inode 带扩展 ACL 时把它的 `system.posix_acl_access`
/// 一并交给插件。实现注记（M4）：`ancestors` 先给空（path-caps 在 M10，到时按 attr_epoch 缓存祖先链）。
pub fn authz(
    v: &dyn KvRead,
    c: &OpCtx,
    op: AuthzOp,
    target: Option<&Node>,
    parent: Option<&Node>,
    victim: Option<&Node>,
) -> Result<(), ErrCode> {
    let acl = |n: Option<&Node>| match n {
        Some(n) if n.rec.flags & F_HAS_ACL != 0 => get_xattr(v, n.ino, ACL_ACCESS),
        _ => Ok(None),
    };
    let (ta, pa, va) = (acl(target)?, acl(parent)?, acl(victim)?);
    let (t, p, w) = (view(target, &ta), view(parent, &pa), view(victim, &va));
    let input =
        AuthzInput { who: &c.req.who, op, target: t.as_ref(), parent: p.as_ref(), victim: w.as_ref(), ancestors: &[] };
    c.pipeline.authz(&input)
}

fn view<'a>(n: Option<&Node>, acl: &'a Option<Vec<u8>>) -> Option<InodeView<'a>> {
    n.map(|n| InodeView {
        ino: n.ino,
        kind: n.rec.kind,
        mode: n.rec.mode,
        uid: n.rec.uid,
        gid: n.rec.gid,
        acl_access: acl.as_deref(),
    })
}

/// 把 `n` 在 `from` = (目录, 名字) 的链接改到 `to`（主链接或 extra_links 里的那一项）。
pub fn relink(n: &mut Node, from: (u64, &[u8]), to: (u64, &[u8])) {
    if n.rec.parent == from.0 && n.rec.name == from.1 {
        (n.rec.parent, n.rec.name) = (to.0, to.1.to_vec());
    } else if let Some(l) = n.rec.extra_links.iter_mut().find(|(p, nm)| *p == from.0 && nm == from.1) {
        *l = (to.0, to.1.to_vec());
    }
}

/// 去掉 `n` 在 `at` 的链接、nlink − 1；删的是主链接且还有别的链接 → 主链接改为 extra_links[0]（3.5.1 的 unlink 行）。
pub fn remove_link(n: &mut Node, at: (u64, &[u8])) {
    if n.rec.parent == at.0 && n.rec.name == at.1 {
        if !n.rec.extra_links.is_empty() {
            (n.rec.parent, n.rec.name) = n.rec.extra_links.remove(0);
        }
    } else {
        n.rec.extra_links.retain(|(p, nm)| !(*p == at.0 && nm == at.1));
    }
    n.rec.nlink = n.rec.nlink.saturating_sub(1);
}

/// 最后一个链接被删除（3.5.6）：标 F_ORPHAN、写 GC 记录（kind 1；6 s 后才可回收）。打开着它的客户端照常读写，
/// 无人打开后由 gc 任务（M5）删除数据与 inode。
pub fn orphan(v: &mut dyn KvTxn, c: &OpCtx, n: &mut Node) {
    n.rec.flags |= F_ORPHAN;
    let chains = n.rec.layout.as_ref().map(|l| l.chains.clone()).unwrap_or_default();
    let now = c.req.now_ms;
    let rec = GcRecV1 { due_ms: now + ORPHAN_DELAY_MS, chains, created_ms: now };
    kv::put(v, &keys::gc(GC_ORPHAN, n.ino, 0), &rec);
}

/// 普通文件吗（截断、Open 用）。
pub fn is_file(n: &Node) -> bool {
    n.rec.kind == KIND_FILE
}
