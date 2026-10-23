//! rename（design.md 3.1.2）：执行器里的**一个闭包**，写集进一个 raft 条目——对所有观察者要么旧名存在、要么新名存在。
//!
//! 顺序：标志（`RENAME_WHITEOUT` 与不认识的位 → `EINVAL`；NOREPLACE 与 EXCHANGE 不能同时给）→ 两个父目录是目录、且都能在里面
//! 查名字（执行权限，与 lookup 相同：没有权限的人看不出名字在不在）→ 源目录项存在 → authz（源父、目标父要写 + 执行；粘滞位
//! 约束源与被覆盖的目标；换了父目录的目录要对它自己有写权限——更新它的 `..`）→ 不得把目录移进自己的子树（沿目标父的 parent
//! 指针点查到根——`AttrResolver::ancestors`——遇到源 → `EINVAL`，O(深度)）→ 两个名字是同一个文件的两个链接 → 什么也不做
//! （POSIX）→ `RENAME_NOREPLACE`（目标存在 → `EEXIST`）/ `RENAME_EXCHANGE`（目标必须存在，两个目录项对调）→ 覆盖目标时
//! 类型要匹配（目录 ← 非目录 `ENOTDIR`、非目录 ← 目录 `EISDIR`），目标目录必须为空（`ENOTEMPTY`）→ 改目录项、被移动 inode 的
//! 链接（主链接或 extra_links）、两个父目录的 size/nlink/mtime/ctime/change_id → 被覆盖的目标：目录直接删掉，文件 nlink − 1、
//! 归零即孤儿 → changelog。
//! 实现注记（M4）：跨配额域的检查（M10）、递归计数的搬移（M7）、移动目录时 `attr_epoch += 1` 与 RECONCILE_Q（M7、M11）、
//! 回收站（M12）随各自的里程碑加入。

use super::common::{
    Node, authz, bump_ctime, check_name, child, link_dentry, relink, save, searchable_dir, update_dir,
};
use super::remove::{delete_dir_records, unlink_file};
use crate::base::ErrCode;
use crate::hooks::traits::AuthzOp;
use crate::meta::changelog::{self, Change};
use crate::meta::engine::txn::{KvRead, KvTxn, OpCtx};
use crate::meta::schema::keys;
use crate::proto::meta::{MetaReply, RENAME_EXCHANGE, RENAME_NOREPLACE};

/// 目录 `dir` 在不在 `anc` 的子树里（`dir` 就是 `anc` 也算）：沿 parent 指针点查到根。
fn inside(v: &dyn KvRead, c: &OpCtx, dir: u64, anc: u64) -> Result<bool, ErrCode> {
    Ok(c.attrs.ancestors(v, dir)?.contains(&anc))
}

/// 换了父目录的目录：要能写它自己（更新它的 ..），在 RenameTo 里作为 target 交给 authz 插件。
fn moved(across: bool, n: &Node) -> Option<&Node> {
    (across && n.is_dir()).then_some(n)
}

/// 一次 rename 涉及的两个父目录：源父 `from` 与目标父 `to`（同一个目录时 `to` 为 None）。
struct Parents {
    from: Node,
    to: Option<Node>,
}

impl Parents {
    /// 目标父目录（同一个目录时就是源父）。
    fn to(&self) -> &Node {
        self.to.as_ref().unwrap_or(&self.from)
    }
    /// 按增量 (size, nlink) 改两个父目录并写回（同一个目录时两份增量加在一起）。
    fn apply(mut self, v: &mut dyn KvTxn, c: &OpCtx, from: (i64, i64), to: (i64, i64)) {
        match self.to.as_mut() {
            Some(t) => {
                update_dir(v, c, &mut self.from, from.0, from.1);
                update_dir(v, c, t, to.0, to.1);
            }
            None => update_dir(v, c, &mut self.from, from.0 + to.0, from.1 + to.1),
        }
    }
}

/// `RENAME_EXCHANGE` 的一边：目录项 (parent, name) 和它现在指向的 inode。
struct Side<'a> {
    node: Node,
    parent: u64,
    name: &'a [u8],
}

#[allow(clippy::too_many_arguments)] // 参数就是 MetaOp::Rename 的字段
pub fn rename(
    v: &mut dyn KvTxn,
    c: &OpCtx,
    parent: u64,
    name: &[u8],
    new_parent: u64,
    new_name: &[u8],
    flags: u32,
) -> Result<MetaReply, ErrCode> {
    if flags & !(RENAME_NOREPLACE | RENAME_EXCHANGE) != 0 || flags == RENAME_NOREPLACE | RENAME_EXCHANGE {
        return Err(ErrCode::EINVAL); // 含 RENAME_WHITEOUT（不支持，3.1.1）
    }
    check_name(name)?;
    check_name(new_name)?;
    let across = new_parent != parent;
    let from = searchable_dir(v, c, parent)?;
    let to = if across { Some(searchable_dir(v, c, new_parent)?) } else { None };
    let ps = Parents { from, to };
    let mut src = child(v, parent, name)?.ok_or(ErrCode::ENOENT)?;
    let victim = child(v, new_parent, new_name)?; // 目标名字现在指向的 inode（要被覆盖或对调的）
    authz(v, c, AuthzOp::RenameFrom, None, Some(&ps.from), Some(&src))?;
    authz(v, c, AuthzOp::RenameTo, moved(across, &src), Some(ps.to()), victim.as_ref())?;
    if src.is_dir() && across && inside(v, c, new_parent, src.ino)? {
        return Err(ErrCode::EINVAL); // 不得把目录移进自己的子树
    }
    if flags & RENAME_EXCHANGE != 0 {
        let other = victim.ok_or(ErrCode::ENOENT)?;
        authz(v, c, AuthzOp::RenameFrom, None, Some(ps.to()), Some(&other))?;
        authz(v, c, AuthzOp::RenameTo, moved(across, &other), Some(&ps.from), Some(&src))?;
        let a = Side { node: src, parent, name };
        let b = Side { node: other, parent: new_parent, name: new_name };
        return exchange(v, c, ps, a, b);
    }
    if let Some(d) = &victim {
        if flags & RENAME_NOREPLACE != 0 {
            return Err(ErrCode::EEXIST);
        }
        if d.ino == src.ino {
            return Ok(MetaReply::Ok); // 同一个文件的两个链接：什么也不做
        }
        match (src.is_dir(), d.is_dir()) {
            (true, false) => return Err(ErrCode::ENOTDIR),
            (false, true) => return Err(ErrCode::EISDIR),
            (true, true) if d.rec.size != 0 => return Err(ErrCode::ENOTEMPTY),
            _ => {}
        }
    }
    v.delete(&keys::dentry(parent, name));
    link_dentry(v, new_parent, new_name, &src);
    relink(&mut src, (parent, name), (new_parent, new_name));
    bump_ctime(&mut src, c);
    save(v, &src);
    // 父目录的增量 (size, nlink)：源父少一项；目标父多一项（覆盖了目标就抵掉）；目录换了父目录，它的 .. 跟着走
    let dir_moves = (across && src.is_dir()) as i64;
    let (mut dsize, mut dnlink) = (1, dir_moves);
    if let Some(mut d) = victim {
        dsize -= 1;
        if d.is_dir() {
            dnlink -= 1;
            delete_dir_records(v, d.ino)?;
        } else {
            unlink_file(v, c, &mut d, new_parent, new_name);
        }
    }
    ps.apply(v, c, (-1, -dir_moves), (dsize, dnlink));
    let ch = Change {
        op: "Rename",
        ino: src.ino,
        parent,
        name,
        new_parent,
        new_name,
        size: src.rec.size,
        ..Default::default()
    };
    changelog::append(v, c, ch)?;
    Ok(MetaReply::Ok)
}

/// `RENAME_EXCHANGE`：两个目录项对调（两边都存在；目录互换时各自的 .. 跟着走）。
fn exchange(v: &mut dyn KvTxn, c: &OpCtx, ps: Parents, mut a: Side, mut b: Side) -> Result<MetaReply, ErrCode> {
    if a.node.ino == b.node.ino {
        return Ok(MetaReply::Ok);
    }
    let across = a.parent != b.parent;
    if across && b.node.is_dir() && inside(v, c, a.parent, b.node.ino)? {
        return Err(ErrCode::EINVAL); // b 会被移进它自己的子树
    }
    link_dentry(v, a.parent, a.name, &b.node);
    link_dentry(v, b.parent, b.name, &a.node);
    relink(&mut a.node, (a.parent, a.name), (b.parent, b.name));
    relink(&mut b.node, (b.parent, b.name), (a.parent, a.name));
    for n in [&mut a.node, &mut b.node] {
        bump_ctime(n, c);
        save(v, n);
    }
    let (ad, bd) = ((across && a.node.is_dir()) as i64, (across && b.node.is_dir()) as i64);
    ps.apply(v, c, (0, bd - ad), (0, ad - bd));
    let (ino, size) = (a.node.ino, a.node.rec.size);
    let (parent, name, new_parent, new_name) = (a.parent, a.name, b.parent, b.name);
    let ch = Change { op: "Rename", ino, parent, name, new_parent, new_name, size, ..Default::default() };
    changelog::append(v, c, ch)?;
    Ok(MetaReply::Ok)
}
