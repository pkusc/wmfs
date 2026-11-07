//! 可续跑的命名空间遍历（design.md 3.10 的 `Walker`，实现注记（M11））：协调器、`wmfs status` 的逐文件统计、（以后的）tiering 共用。
//!
//! - **子树 DFS**（[`Walker::subtree`]）：游标 = 栈 `Vec<(目录 ino, 这个目录里最后处理的名字)>` 的 postcard 编码（空 = 从 `root` 开始）。
//!   每次最多读 256 个目录项：取栈顶目录、按名字顺序往后读；遇到子目录就把它压栈、先进去（DFS），一个目录读完就弹出。每项读出 inode 与
//!   它的生效属性（目录 = 它自己的，其他 = 主父目录的，`ns::common::governing`；按 attr_epoch 缓存；M13 起文件再套用 tier 规则——
//!   `bg::status::declared`，所以协调器、tiering 与 `wmfs status` 拿到的都是同一份"声明"）。
//! - **全量扁平扫描**（[`Walker::flat`]）：按 ino 顺序扫 INODE 键空间，游标 = 上次的 ino（最简单、天然可续跑）。
//!
//! 每次调用是一个线性一致读（一个一致的快照）；两次调用之间命名空间可能变了（rename、删除）：子树里新出现的东西可能这一遍看不到、
//! 搬走的可能看两次——用它的任务都按"每项幂等"写（协调器比较声明与实际，已经一样就跳过），漏掉的由每 24 h 一次的全量扫描兜住。
//! 硬链接按每个名字出现一次（同一个 inode 可能出现多次，`parent`/`name` 是这一次看到它的目录与名字；生效属性一律按主父目录算）。

use std::sync::Arc;

use super::status::declared;
use crate::base::ErrCode;
use crate::base::types::ReqCtx;
use crate::meta::attrs::EffectiveAttrs;
use crate::meta::engine::txn::{KvScan, MetaEngine, OpCtx};
use crate::meta::ns::common::{Node, governing};
use crate::meta::schema::keys;
use crate::meta::schema::kv;
use crate::meta::schema::records::{DentryV1, InodeV1, KIND_DIR, decode_v1};
use crate::proto::meta::MetaReply;

/// 子树 DFS 每次最多读多少个目录项（3.10）。
pub const PAGE: usize = 256;

/// 遍历到的一项：inode 与它的生效属性（`parent`、`name` = 这次是从哪个目录、哪个名字看到它的；扁平扫描与 [`Walker::one`] 是 0 与空）。
pub struct Walked {
    pub ino: u64,
    pub inode: InodeV1,
    pub parent: u64,
    pub name: Vec<u8>,
    pub eff: Arc<EffectiveAttrs>,
}

/// 一批结果：这一批的项、下一次的游标、走完了没有。
pub struct WalkPage<C> {
    pub items: Vec<Walked>,
    pub cursor: C,
    pub done: bool,
}

#[derive(Default)]
pub struct Walker {}

impl Walker {
    pub fn new() -> Walker {
        Walker {}
    }

    /// `root` 子树的下一批（见文件头）。游标解不开 → `EINVAL`（任务的游标坏了：从头再来由调用方决定）。
    pub fn subtree(&self, eng: &dyn MetaEngine, root: u64, cursor: &[u8]) -> Result<WalkPage<Vec<u8>>, ErrCode> {
        let start: Vec<(u64, Vec<u8>)> = if cursor.is_empty() {
            vec![(root, vec![])]
        } else {
            postcard::from_bytes(cursor).map_err(|_| ErrCode::EINVAL)?
        };
        let mut out = None;
        // 这个闭包只用来在 ReadIndex 之后的视图上取出这一批，它的返回值丢弃
        eng.read(&ReqCtx::internal(), &mut |v, c| {
            let (mut stack, mut items) = (start.clone(), Vec::new());
            dfs(v, c, &mut stack, &mut items)?;
            out = Some((stack, items));
            Ok(MetaReply::Ok)
        })?;
        let (stack, items) = out.ok_or(ErrCode::INTERNAL)?;
        let done = stack.is_empty();
        let cursor = if done { vec![] } else { postcard::to_stdvec(&stack).map_err(|_| ErrCode::INTERNAL)? };
        Ok(WalkPage { items, cursor, done })
    }

    /// 全量扁平扫描：ino 大于 `after` 的最多 `limit` 个 inode（见文件头）。
    pub fn flat(&self, eng: &dyn MetaEngine, after: u64, limit: usize) -> Result<WalkPage<u64>, ErrCode> {
        let mut out = None;
        eng.read(&ReqCtx::internal(), &mut |v, c| {
            let (_, end) = keys::tag_range(keys::T_INODE);
            let rows = v.scan(&keys::inode(after.saturating_add(1)), &end, limit)?;
            let mut items = Vec::new();
            for (k, b) in &rows {
                let n = Node { ino: kv::key_id(k), rec: decode_v1(b)? };
                items.push(walked(v, c, n, 0, vec![])?);
            }
            out = Some((items, rows.len() < limit));
            Ok(MetaReply::Ok)
        })?;
        let (items, done) = out.ok_or(ErrCode::INTERNAL)?;
        let cursor = items.last().map_or(after, |w: &Walked| w.ino);
        Ok(WalkPage { items, cursor, done })
    }

    /// 只读一个 inode（`wmfs status <文件>`）；不在了 → None。
    pub fn one(&self, eng: &dyn MetaEngine, ino: u64) -> Result<Option<Walked>, ErrCode> {
        let mut out = None;
        eng.read(&ReqCtx::internal(), &mut |v, c| {
            out = match kv::get::<InodeV1>(v, &keys::inode(ino))? {
                Some(rec) => Some(walked(v, c, Node { ino, rec }, 0, vec![])?),
                None => None,
            };
            Ok(MetaReply::Ok)
        })?;
        Ok(out)
    }
}

fn walked(v: &dyn KvScan, c: &OpCtx, n: Node, parent: u64, name: Vec<u8>) -> Result<Walked, ErrCode> {
    let eff = declared(c, n.ino, &n.rec, governing(v, c, &n)?); // 实现注记（M13）：再套用 tier 规则（`bg::status::declared`）
    Ok(Walked { ino: n.ino, inode: n.rec, parent, name, eff })
}

/// DFS 的一批（见文件头）：最多读 `PAGE` 个目录项；根目录自己不算一项。
fn dfs(v: &dyn KvScan, c: &OpCtx, stack: &mut Vec<(u64, Vec<u8>)>, out: &mut Vec<Walked>) -> Result<(), ErrCode> {
    let mut budget = PAGE;
    while budget > 0 {
        let Some((dir, last)) = stack.last().cloned() else { return Ok(()) };
        let (prefix, end) = keys::dentry_range(dir);
        // 从这个目录里最后处理的名字之后接着读
        let start = if last.is_empty() { prefix.clone() } else { keys::after(&keys::dentry(dir, &last)) };
        let rows = v.scan(&start, &end, budget)?;
        let full = rows.len() == budget;
        let mut descend = None;
        for (k, b) in &rows {
            budget -= 1;
            let d: DentryV1 = decode_v1(b)?;
            let name = k[prefix.len()..].to_vec();
            if let Some(top) = stack.last_mut() {
                top.1 = name.clone();
            }
            let Some(rec) = kv::get::<InodeV1>(v, &keys::inode(d.ino))? else { continue }; // 刚被删掉
            out.push(walked(v, c, Node { ino: d.ino, rec }, dir, name)?);
            if d.kind == KIND_DIR {
                descend = Some(d.ino);
                break;
            }
        }
        match descend {
            Some(child) => stack.push((child, vec![])), // 先进子目录（读完它再回来接着读这个目录）
            None if !full => {
                stack.pop(); // 这个目录读完了
            }
            None => {} // 读满了这一批：下一次从 last 往后接着读
        }
    }
    Ok(())
}
