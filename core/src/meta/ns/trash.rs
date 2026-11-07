//! 回收站（design.md 第 5 章"回收站细节"、3.9 表的"移入/移出回收站"、semantics.md 3.3 第 16 条；实现注记（M12）；按文件属主分子目录：
//! ADR-0043 第 1 条）。
//!
//! - **移进去**：`trash` 在目录里生效时，unlink 掉一个文件的最后一个链接、或 rename 覆盖掉的文件目标，改为移进 `trash` 来源目录下的
//!   `.wmfs-trash/<uid>/`（[`dir_for`]；`<uid>` 是**文件的属主**，不是删它的人），名字 `<unix秒>-<ino>-<原名>`（超过 255 字节截断；原名是
//!   UTF-8 时不切在一个字符中间），xattr `user.wmfs.origin` = 原路径，`trash_expire_ms = now + 保留期` 并写 TRASH_INDEX。计数照 rename 搬
//!   （回收站在来源目录之下，所以仍计入那棵子树的配额；中间目录上的配额随之释放）。还有别的硬链接的文件只删这一个名字、不进回收站（数据还在，
//!   规格没写多链接的情形：最小方案）；回收站里再删 = 真删；目录的 rmdir 不进回收站。unlink 与 rename 共用 [`trash_or_unlink`]：有回收站
//!   就移进去，否则照常去掉这个链接。
//! - **两层目录**都在第一次用时建、都标 F_TRASH_DIR，建它们不检查配额（rm 不该因为配额失败）：`.wmfs-trash` mode 0711、属主同来源目录
//!   （谁都走得进自己的子目录，但列不出有哪些用户）；`<uid>` mode 0700、属主是这个 uid、属组取这个文件的（通常就是属主的主组；0700 下属组
//!   没有权限，取它只是看着自然）——只有属主与 root 看得到、恢复得了，同 Linux 桌面的 `.Trash-<uid>`。已有同名的：`.wmfs-trash` 是目录就
//!   标上 F_TRASH_DIR 用它；`<uid>` 还要属于这个 uid 才用（不往别人建的目录里放）；用不了 → 照常删。空了的 `<uid>` 留着。
//! - **恢复**：普通的 `mv` 把它移出 F_TRASH_DIR 目录 → 清掉到期时间与 TRASH_INDEX（[`restore`]）。
//! - **到期**：trash-expire 任务按 TRASH_INDEX 真删（[`expire`]：changelog `TrashExpire`，进普通的孤儿 / GC 流程；按索引找，与目录结构
//!   无关）；ttl 任务的删除见 [`ttl`]（"过期了吗"是 [`ttl_due`]，ttl 任务与 `wmfs status` 也用它）。两个都回"真删了没有"（插件只数真删的）。
//!
//! 调用方（`remove.rs`、`rename.rs`）手上拿着的父目录副本在 [`dir_for`] 与 [`trash_or_unlink`] 之后要重新读：回收站可能刚建在它下面，
//! 也可能就是它（刚往里放了被覆盖的文件）。

use super::common::{
    MAX_NAME, Node, alloc_ino, bump_ctime, child, get, link_dentry, new_node, relink, save, update_dir,
};
use super::read_ops::path_of;
use super::remove::{UnlinkBy, unlink_as, unlink_file};
use super::xattr::put_raw;
use crate::base::{ErrCode, clock};
use crate::meta::engine::txn::{KvTxn, OpCtx};
use crate::meta::rstat::{self, Delta};
use crate::meta::schema::keys;
use crate::meta::schema::records::{F_TRASH_DIR, InodeV1, KIND_DIR};

pub const TRASH_DIR: &[u8] = b".wmfs-trash";
pub const ORIGIN: &[u8] = b"user.wmfs.origin";

/// 一个文件要移进的回收站（[`dir_for`]）：属主的子目录、`.wmfs-trash` 与来源目录的 inode 号、保留期。
pub struct Bin {
    dir: Node,
    top: u64,
    root: u64,
    keep_ms: u64,
}

/// 从目录 `dir` 删掉 `n` 的最后一个链接时要不要进回收站（见文件头）：要 → 回收站（没有的那层现在建）；否则 None（照常删）。
pub fn dir_for(v: &mut dyn KvTxn, c: &OpCtx, dir: &Node, n: &Node) -> Result<Option<Bin>, ErrCode> {
    if n.is_dir() || n.rec.nlink != 1 || dir.rec.flags & F_TRASH_DIR != 0 {
        return Ok(None);
    }
    let eff = c.attrs.resolve(v, dir.ino)?;
    if eff.trash_ms == 0 {
        return Ok(None);
    }
    let one = Delta { dirs: 1, ..Delta::default() };
    let mut root = get(v, eff.trash_root)?;
    let mut top = match child(v, root.ino, TRASH_DIR)? {
        Some(t) if t.is_dir() => flagged(v, t),
        Some(_) => return Ok(None), // 同名的不是目录：用不了，照常删
        None => {
            let owner = (root.rec.uid, root.rec.gid);
            let t = make(v, c, &mut root, TRASH_DIR, 0o711, owner)?;
            rstat::apply(v, c, root.ino, &one)?;
            t
        }
    };
    let uid = n.rec.uid.to_string().into_bytes();
    let sub = match child(v, top.ino, &uid)? {
        Some(s) if s.is_dir() && s.rec.uid == n.rec.uid => flagged(v, s),
        Some(_) => return Ok(None), // 不是这个用户的目录：不往里放，照常删
        None => {
            let s = make(v, c, &mut top, &uid, 0o700, (n.rec.uid, n.rec.gid))?;
            rstat::apply(v, c, root.ino, &one)?;
            rstat::add_to(v, top.ino, &one)?; // 回收站里的各层只加自己的（见 move_in）
            s
        }
    };
    Ok(Some(Bin { dir: sub, top: top.ino, root: root.ino, keep_ms: eff.trash_ms }))
}

/// 已有的同名目录：标上 F_TRASH_DIR，从此就是回收站的一层。
fn flagged(v: &mut dyn KvTxn, mut t: Node) -> Node {
    if t.rec.flags & F_TRASH_DIR == 0 {
        t.rec.flags |= F_TRASH_DIR;
        save(v, &t);
    }
    t
}

/// 在 `p` 下建回收站的一层 `name`：mode、属主与属组 `ids` 见文件头，标 F_TRASH_DIR，不检查配额；递归计数由调用方加。
fn make(v: &mut dyn KvTxn, c: &OpCtx, p: &mut Node, name: &[u8], mode: u32, ids: (u32, u32)) -> Result<Node, ErrCode> {
    let mut t = new_node(c, alloc_ino(v)?, KIND_DIR, mode, p, name);
    (t.rec.mode, t.rec.uid, t.rec.gid, t.rec.flags) = (mode, ids.0, ids.1, F_TRASH_DIR);
    save(v, &t);
    link_dentry(v, p.ino, name, &t);
    rstat::init_dir(v, t.ino);
    update_dir(v, c, p, 1, 1);
    Ok(t)
}

/// 去掉 `n` 在 `from` = (目录, 名字) 的链接（`from` 的目录项与那个目录的 size 由调用方处理；见文件头）：有回收站 `bin` → 移进去，回
/// changelog `Unlink` 要带的一项 `trash=<回收站里的新名字>`；没有（或回收站里已有同名的，不会发生）→ 照常删，回 None。
pub fn trash_or_unlink(
    v: &mut dyn KvTxn,
    c: &OpCtx,
    n: &mut Node,
    from: (u64, &[u8]),
    bin: Option<Bin>,
) -> Result<Option<(String, String)>, ErrCode> {
    if let Some(b) = bin
        && let Some(name) = move_in(v, c, n, from, b)?
    {
        return Ok(Some(("trash".into(), String::from_utf8_lossy(&name).into_owned())));
    }
    leave(v, n);
    unlink_file(v, c, n, from.0, from.1)?;
    Ok(None)
}

/// 把 `n` 的链接 `from` 移进回收站 `b`，回新名字；回收站里已有同名的（不会发生）→ None。
fn move_in(v: &mut dyn KvTxn, c: &OpCtx, n: &mut Node, from: (u64, &[u8]), b: Bin) -> Result<Option<Vec<u8>>, ErrCode> {
    let mut t = b.dir;
    let mut name = format!("{}-{}-", c.req.now.secs, n.ino).into_bytes();
    name.extend_from_slice(from.1);
    name.truncate(MAX_NAME);
    if std::str::from_utf8(from.1).is_ok()
        && let Err(e) = std::str::from_utf8(&name)
    {
        name.truncate(e.valid_up_to()); // 截在一个多字节字符中间：退到它之前（changelog 里 trash= 的名字才对得上）
    }
    if child(v, t.ino, &name)?.is_some() {
        return Ok(None);
    }
    let dir = path_of(v, from.0)?;
    let origin = format!("{}/{}", dir.trim_end_matches('/'), String::from_utf8_lossy(from.1));
    link_dentry(v, t.ino, &name, n);
    relink(n, from, (t.ino, &name));
    bump_ctime(n, c);
    n.rec.trash_expire_ms = c.req.now_ms.saturating_add(b.keep_ms);
    v.put(&keys::trash_index(n.rec.trash_expire_ms, n.ino), vec![]); // 值是空的（3.2.7）
    put_raw(v, n.ino, ORIGIN, origin.into_bytes())?;
    save(v, n);
    // 计数：从原来的祖先链搬到来源目录，再加到回收站的两层自己的 RSTAT 上——不按回收站算祖先链（它可能是这个操作里刚建的，算出的
    // 祖先链会进按 attr_epoch 的缓存）
    let d = Delta::of(v, n)?;
    rstat::transfer(v, c, from.0, b.root, &d)?;
    rstat::add_to(v, b.top, &d)?;
    rstat::add_to(v, t.ino, &d)?;
    update_dir(v, c, &mut t, 1, 0);
    Ok(Some(name))
}

/// 文件离开回收站：清掉到期时间与 TRASH_INDEX（真删的时候也调：不留过时的索引）。
pub fn leave(v: &mut dyn KvTxn, n: &mut Node) {
    if n.rec.trash_expire_ms != 0 {
        v.delete(&keys::trash_index(n.rec.trash_expire_ms, n.ino));
        n.rec.trash_expire_ms = 0;
    }
}

/// rename 把 `n` 移到目录 `to`：`to` 不是回收站（的任何一层）→ 恢复了（见文件头）。
pub fn restore(v: &mut dyn KvTxn, n: &mut Node, to: &Node) {
    if to.rec.flags & F_TRASH_DIR == 0 {
        leave(v, n);
    }
}

/// trash-expire 任务的一项（执行器闭包，内部身份）：TRASH_INDEX(expire_ms, ino) 还在、文件还在回收站里、到期时间就是它 → 真删
/// （changelog `TrashExpire`），回 true；否则只删掉这条过时的索引，回 false。
pub fn expire(v: &mut dyn KvTxn, c: &OpCtx, expire_ms: u64, ino: u64) -> Result<bool, ErrCode> {
    v.delete(&keys::trash_index(expire_ms, ino));
    let Ok(n) = get(v, ino) else { return Ok(false) };
    let in_trash = get(v, n.rec.parent).is_ok_and(|p| p.rec.flags & F_TRASH_DIR != 0);
    if n.rec.trash_expire_ms != expire_ms || n.rec.nlink == 0 || !in_trash {
        return Ok(false);
    }
    let name = n.rec.name.clone();
    unlink_as(v, c, n.rec.parent, &name, UnlinkBy::TrashExpire).map(|_| true)
}

/// 这个文件按 `ttl_ms` 过期了吗：ttl 生效、不是目录、还有链接、不在回收站里、mtime + ttl ≤ now（ttl 任务挑、批次里再判、`wmfs status`
/// 的 ttl 一行，三处共用）。
pub fn ttl_due(i: &InodeV1, ttl_ms: u64, now_ms: u64) -> bool {
    ttl_ms > 0
        && i.kind != KIND_DIR
        && i.nlink > 0
        && i.trash_expire_ms == 0
        && clock::ts_ms(i.mtime).saturating_add(ttl_ms) <= now_ms
}

/// ttl 任务的一项（执行器闭包，内部身份）：`parent` 里的 `name` 还是 `ino`、按当前状态仍然过期（[`ttl_due`]）→ unlink（`trash` 生效就进
/// 回收站；changelog `Unlink` 带 `reason=ttl`，ADR-0029），回 true；否则什么也不做，回 false。
pub fn ttl(v: &mut dyn KvTxn, c: &OpCtx, parent: u64, name: &[u8], ino: u64) -> Result<bool, ErrCode> {
    let Some(n) = child(v, parent, name)?.filter(|n| n.ino == ino) else { return Ok(false) };
    if !ttl_due(&n.rec, c.attrs.resolve(v, parent)?.ttl_ms, c.req.now_ms) {
        return Ok(false);
    }
    unlink_as(v, c, parent, name, UnlinkBy::Ttl).map(|_| true)
}
