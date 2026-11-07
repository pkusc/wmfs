//! rebalance：后台任务插件（design.md 4.12 的 rebalance 一行，bg_task；实现注记（M13））——同一个存储类（同副本数）的链里，最满的与最空的
//! 使用率相差超过 20% 时，把最满的链上的文件迁到别的链上（迁移原语 `Migrator::migrate_away`：placement 带 exclude 提示），差距小于 10% 停；
//! 受限速。只在元数据 leader 上跑，每轮结束隔 10 min。
//!
//! - 链的使用率 = 成员里最满的 target 的 已用 / 容量（leader 内存里的 target 心跳，`meta::cluster::chains::view_of`）；[`pick`] 是纯函数。
//! - 一轮：选出差距最大的那一组；超过 20% 才开始，记下"这一轮最多搬多少字节"= (最满的已用 − 最空的已用) / 2——用量要等心跳（≤ 1 s）与旧布局
//!   的回收（GC 记录 kind 3：宽限期之后）才降下来，只看用量的话会搬过头、来回振荡。然后按 ino 扫 inode（`Walker::flat`），用着最满那条链的
//!   普通文件逐个迁走；扫完、搬够了、或差距已经小于 10% 就结束这一轮。
//! - 一步最多 100 个文件、或大约一秒的限速量（没设限速时 64 MiB）、或 1 s（3.10，`bg::task::StepBudget`）：每迁完一个文件、每扫完一页都看
//!   一次额度与这一轮还能搬的字节（M13 审查：以前只在每页开头看，一页能迁 100 个文件）；游标 = (链, 扫到的 ino, 还能搬的字节) 的 postcard。
//!   迁移原语不会降低文件的条带数：别的链不够放下它的条带时这个文件不搬（`Migrator::migrate_away` 回 `Same`）。
//! - 指标 `wmfs_rebalance_files_moved_total`；fail point `plugin.rebalance.bg_task`（本体包在每一步外层）。

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use wmfs_core::api::metrics::{self, Counter};
use wmfs_core::base::ErrCode;
use wmfs_core::base::types::ReqCtx;
use wmfs_core::bg::migrator::Migrated;
use wmfs_core::bg::task::{BgTask, RunOn, StepBudget, StepResult, TaskCtx};
use wmfs_core::bg::walker::Walked;
use wmfs_core::hooks::manifest::{CORE_ABI_VERSION, Hook, Manifest};
use wmfs_core::hooks::traits::ChainView;
use wmfs_core::meta::cluster::chains::view_of;
use wmfs_core::meta::schema::records::{F_INLINE, KIND_FILE};
use wmfs_core::proto::meta::MetaReply;
use wmfs_core::proto::{decode_body, encode_body};

pub static MANIFEST: Manifest = Manifest {
    name: "rebalance",
    version: "0.1.0",
    hooks: &[Hook::BgTask],
    altitude: 0, // bg_task 不排序
    hot_path: false,
    config_schema: "",
    abi_version: CORE_ABI_VERSION,
    attrs: &[],
    budget_ns: 0,
    fail_points: &["plugin.rebalance.bg_task"],
};

/// 差距超过它才开始、小于它就停（4.12）。
const START: f64 = 0.20;
const STOP: f64 = 0.10;
const PAGE: usize = 100;

fn ratio(c: &ChainView) -> f64 {
    c.used as f64 / c.capacity as f64
}

/// 差距最大的那一组（同存储类、同副本数，至少两条有容量、有 Serving 成员的链）：(差距, 最满的链, 要搬的字节 = 两者已用之差的一半)。
fn pick(views: &[ChainView]) -> Option<(f64, u32, u64)> {
    let mut groups: BTreeMap<(u16, u8), Vec<&ChainView>> = BTreeMap::new();
    for c in views.iter().filter(|c| c.capacity > 0 && c.serving > 0) {
        groups.entry((c.class_id, c.replicas)).or_default().push(c);
    }
    let gap = |g: &Vec<&ChainView>| {
        let full = g.iter().max_by(|a, b| ratio(a).total_cmp(&ratio(b)))?;
        let empty = g.iter().min_by(|a, b| ratio(a).total_cmp(&ratio(b)))?;
        Some((ratio(full) - ratio(empty), full.id, full.used.saturating_sub(empty.used) / 2))
    };
    groups.values().filter(|g| g.len() >= 2).filter_map(gap).max_by(|a, b| a.0.total_cmp(&b.0))
}

pub struct Rebalance {
    moved: Arc<Counter>,
}

impl Rebalance {
    pub fn new() -> Rebalance {
        Rebalance {
            moved: metrics::counter("wmfs_rebalance_files_moved_total", "rebalance 从最满的链上迁走的文件数", &[]),
        }
    }
}

impl Default for Rebalance {
    fn default() -> Rebalance {
        Rebalance::new()
    }
}

/// 用着链 `chain`、能搬的文件：普通文件（不是孤儿）、不是 inline（inline 文件没有链）。
fn on_chain(w: &Walked, chain: u32) -> bool {
    let i = &w.inode;
    let on = i.layout.as_ref().is_some_and(|l| l.chains.contains(&chain));
    i.kind == KIND_FILE && i.nlink > 0 && i.flags & F_INLINE == 0 && on
}

/// 这一轮结束（这一步迁了 `items` 个文件、`bytes` 字节）。
fn finished(items: u64, bytes: u64) -> Result<StepResult, ErrCode> {
    Ok(StepResult { items, bytes, pass_done: true, ..Default::default() })
}

/// 此刻差距最大的那一组（见 [`pick`]）。
fn now_gap(ctx: &TaskCtx) -> Result<Option<(f64, u32, u64)>, ErrCode> {
    let mut out = None;
    ctx.meta.read(&ReqCtx::internal(), &mut |_v, c| {
        let views: Vec<ChainView> = c.map.chains.iter().map(|(id, r)| view_of(c.map, c.usage, *id, r)).collect();
        out = pick(&views);
        Ok(MetaReply::Ok)
    })?;
    Ok(out)
}

impl BgTask for Rebalance {
    fn manifest(&self) -> &'static Manifest {
        &MANIFEST
    }

    fn kind(&self) -> &'static str {
        "rebalance"
    }

    fn runs_on(&self) -> RunOn {
        RunOn::MetaLeader
    }

    fn interval(&self) -> Duration {
        Duration::from_secs(600)
    }

    fn step(&mut self, ctx: &mut TaskCtx, cursor: &[u8]) -> Result<StepResult, ErrCode> {
        let gap = now_gap(ctx)?;
        let resume: Option<(u32, u64, u64)> = decode_body(cursor).ok();
        // 三种情况：有游标、差距还没降到 10% 以下 → 接着搬；没有游标、差距超过 20% → 开新一轮；其余 → 这一轮结束
        let (chain, mut after, mut left) = if let Some(c) = resume.filter(|_| gap.is_some_and(|g| g.0 >= STOP)) {
            c
        } else if let Some((_, full, bytes)) = gap.filter(|g| cursor.is_empty() && g.0 > START) {
            (full, 0, bytes)
        } else {
            return finished(0, 0);
        };
        let mut b = StepBudget::new(ctx);
        let paused = |after: u64, left: u64, b: &StepBudget| -> Result<StepResult, ErrCode> {
            let cursor = encode_body(&(chain, after, left)); // 下一步从 after 之后接着扫
            Ok(StepResult { cursor, items: b.items, bytes: b.bytes, backlog: 1, pass_done: false })
        };
        loop {
            let page = ctx.walker.flat(ctx.meta, after, PAGE)?;
            for w in &page.items {
                if left == 0 {
                    return finished(b.items, b.bytes); // 这一轮搬够了
                }
                if !b.more() {
                    return paused(after, left, &b);
                }
                after = w.ino;
                if on_chain(w, chain)
                    && let Migrated::Done { bytes } = ctx.migrate.migrate_away(ctx, w.ino, &w.eff, &[chain])?
                {
                    b.add(bytes);
                    left = left.saturating_sub(bytes);
                    self.moved.inc();
                }
            }
            if page.done || left == 0 {
                return finished(b.items, b.bytes); // 扫完了（或搬够了）：这一轮结束
            }
            if !b.more() {
                return paused(after, left, &b);
            }
        }
    }
}

#[cfg(test)]
mod tests;
