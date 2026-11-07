//! tiering：后台任务插件（design.md 4.12 的 tiering 一行、第 5 章的 `tier` 属性、6.1 的 `[lifecycle]`，bg_task；实现注记（M13））——
//! 分层规则 `tier=<from>-><to>:<条件>`（或存储类的 `[lifecycle] tier_to/when`，等于在绑定这个类的目录上隐式设了它）生效的子树里，存储类是
//! `from`、条件对它成立的文件迁到 `to`。只在元数据 leader 上跑，每轮结束隔 24 h（`wmfs task run tiering` 马上来一轮）。
//!
//! - 一轮：先扫 DIRATTR 找"规则根"——显式设了 `tier` 的目录、`class` 指向一个有 `[lifecycle]` 的类的目录，default 类有 `[lifecycle]` 时还有
//!   根目录——按 ino 排好，逐个子树 DFS（`bg::walker`）。遍历器给出的生效属性就是 `declared(file)`（主父目录的属性再套用 tier 规则，
//!   `bg::status::declared`；条件经 `PolicyPoint::TierCondition` 求值），声明的类已经换成 `to`、实际还不是的文件交给迁移原语
//!   （`Migrator::migrate`）。协调器用同一个 `declared`，所以分层之后不会被它迁回去；已经在 `to` 里的文件以后再被访问也留在那里
//!   （理念文档 8.1：不按热度挪数据）。嵌套的规则根会被走两遍（每一项幂等）。
//! - 一步最多迁 100 个文件、或大约一秒的限速量（没设限速时 64 MiB）、或 1 s（3.10，`bg::task::StepBudget`）；每迁完一个文件、每走完一页都
//!   看一次额度（M13 审查：一页里没有该迁的文件时也受 1 s 的约束，不会一步走完全部子树）。在一页里用完就停，游标退回这一页的开头（已经迁过的
//!   文件下一步一比较就跳过）；这一轮里迁不成（`Retry`）、写打开着（`SkippedOpen`）的文件记在内存里，重走这一页时跳过它们（不然一个每次都要
//!   花 1 s 以上、最后失败的文件会让这一轮永远停在这一页；下一轮再试）。游标 = (规则根, DFS 游标) 的 postcard，换 leader 后接着走。积压 =
//!   这一轮还没走完的规则根个数。
//! - 指标 `wmfs_tiering_files_moved_total`；fail point `plugin.tiering.bg_task`（本体包在每一步外层）。

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use wmfs_core::api::metrics::{self, Counter};
use wmfs_core::base::ErrCode;
use wmfs_core::base::types::ReqCtx;
use wmfs_core::bg::migrator::Migrated;
use wmfs_core::bg::status::tier_pending;
use wmfs_core::bg::task::{BgTask, RunOn, StepBudget, StepResult, TaskCtx};
use wmfs_core::hooks::manifest::{CORE_ABI_VERSION, Hook, Manifest};
use wmfs_core::meta::attrs::DEFAULT_CLASS;
use wmfs_core::meta::schema::keys::{ROOT_INO, T_DIRATTR};
use wmfs_core::meta::schema::kv;
use wmfs_core::meta::schema::records::{AttrMapV1, decode_v1};
use wmfs_core::proto::meta::MetaReply;
use wmfs_core::proto::{decode_body, encode_body};

pub static MANIFEST: Manifest = Manifest {
    name: "tiering",
    version: "0.1.0",
    hooks: &[Hook::BgTask],
    altitude: 0, // bg_task 不排序
    hot_path: false,
    config_schema: "",
    abi_version: CORE_ABI_VERSION,
    attrs: &["tier"],
    budget_ns: 0,
    fail_points: &["plugin.tiering.bg_task"],
};

pub struct Tiering {
    moved: Arc<Counter>,
    failed: HashSet<u64>, // 这一轮里迁不成、写打开着的文件（见文件头）
}

impl Tiering {
    pub fn new() -> Tiering {
        let moved = metrics::counter("wmfs_tiering_files_moved_total", "tiering 按分层规则迁走的文件数", &[]);
        Tiering { moved, failed: HashSet::new() }
    }
}

impl Default for Tiering {
    fn default() -> Tiering {
        Tiering::new()
    }
}

/// 规则根，按 ino 排好（见文件头）。
fn roots(ctx: &TaskCtx) -> Result<Vec<u64>, ErrCode> {
    let mut out = vec![];
    ctx.meta.read(&ReqCtx::internal(), &mut |v, c| {
        let life = |name: &str| c.map.classes.iter().any(|k| k.name == name && k.tier_to.is_some());
        out = if life(DEFAULT_CLASS) { vec![ROOT_INO] } else { vec![] };
        for (k, b) in kv::scan_tag(v, T_DIRATTR)? {
            let m: AttrMapV1 = decode_v1(&b)?;
            if m.entries.contains_key("tier") || m.entries.get("class").is_some_and(|n| life(n)) {
                out.push(kv::key_id(&k));
            }
        }
        out.sort();
        out.dedup();
        Ok(MetaReply::Ok)
    })?;
    Ok(out)
}

impl BgTask for Tiering {
    fn manifest(&self) -> &'static Manifest {
        &MANIFEST
    }

    fn kind(&self) -> &'static str {
        "tiering"
    }

    fn runs_on(&self) -> RunOn {
        RunOn::MetaLeader
    }

    fn interval(&self) -> Duration {
        Duration::from_secs(24 * 3600)
    }

    fn step(&mut self, ctx: &mut TaskCtx, cursor: &[u8]) -> Result<StepResult, ErrCode> {
        if cursor.is_empty() {
            self.failed.clear(); // 一轮的开头
        }
        let (mut root, mut dfs): (u64, Vec<u8>) = decode_body(cursor).unwrap_or_default();
        let (roots, mut b) = (roots(ctx)?, StepBudget::new(ctx));
        let paused = |root: u64, dfs: Vec<u8>, b: &StepBudget| -> Result<StepResult, ErrCode> {
            let backlog = roots.iter().filter(|r| **r >= root).count() as u64;
            Ok(StepResult {
                cursor: encode_body(&(root, dfs)),
                items: b.items,
                bytes: b.bytes,
                backlog,
                pass_done: false,
            })
        };
        while let Some(&r) = roots.iter().find(|r| **r >= root) {
            if r != root {
                (root, dfs) = (r, vec![]);
            }
            let page = ctx.walker.subtree(ctx.meta, root, &dfs)?;
            for w in &page.items {
                if !tier_pending(w) || self.failed.contains(&w.ino) {
                    continue;
                }
                match ctx.migrate.migrate(ctx, w.ino, &w.eff)? {
                    Migrated::Done { bytes } => {
                        b.add(bytes);
                        self.moved.inc();
                    }
                    Migrated::Same => {}
                    Migrated::Retry | Migrated::SkippedOpen => {
                        self.failed.insert(w.ino);
                    }
                }
                if !b.more() {
                    return paused(root, dfs, &b); // 这一页用完了额度：下一步从这一页的开头重走（见文件头）
                }
            }
            (root, dfs) = if page.done { (root + 1, vec![]) } else { (root, page.cursor) };
            if !b.more() {
                return paused(root, dfs, &b);
            }
        }
        self.failed.clear();
        Ok(StepResult { cursor: vec![], items: b.items, bytes: b.bytes, backlog: 0, pass_done: true })
    }
}

#[cfg(test)]
mod tests;
