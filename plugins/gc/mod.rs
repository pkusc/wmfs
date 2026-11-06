//! gc：后台任务插件（design.md 4.12 的 gc 一行、3.5.6、3.4.2.9，bg_task）——删除废弃的 epoch 与无人打开的孤儿 inode 的数据，
//! 清理已经没有用的元数据（去重记录、过期的驱逐记录、修剪 changelog），以及（`local.rs`）数据节点上的本地孤儿扫描。
//!
//! leader 部分 [`Gc`]（`RunOn::MetaLeader`，每轮结束隔 1 s 再开始下一轮）。一轮按键空间分几段，游标 = 最后处理的那个 key（它的第一个字节
//! 就是键空间标签，所以游标也说明了做到哪一段；只有一个标签字节 = 那一段的开头）：
//! 1. GC 记录（0x0D，每页 ≤ 100 条）：
//! - kind 2（废弃 epoch：截断到 0、覆盖写换下来的，作废的 uninline/迁移意图）：过了宽限期（`due_ms`，= 写记录时 + `epoch_gc_grace`，
//!   默认 60 s，演示时可调成 0）就对记录里的每条链发 `DeleteEpoch`（cap 带管理权限位，走 QoS 低优先级队列），然后删记录。之后拿着旧
//!   layout 的读者按 C5 看到截断（cto 允许）。
//! - 实现注记（M11，ADR-0042 第 5 条）：kind 3（迁移换下来的旧 layout，内容与新的相同）：除了宽限期，还要没有人打开着这个文件——同孤儿的
//!   条件 ①③，并且距写记录已超过 2 个心跳周期（在那之前打开它的会话都已报上来）；拿着旧 layout 的读者照样读得到，关了才删。
//! - kind 1（孤儿：最后一个链接被删了）：3.5.6 的三个条件全部满足才删——① leader 内存里没有任何会话打开着它；② 距 unlink 已超过
//!   2 个心跳周期（记录的 `due_ms`）；③ 本 leader 就绪已超过 2 个心跳周期（6 s），且所有存活会话都在这个 term 里报过打开文件的全集。
//!   然后对 inode 当前 layout 的每条链 `DeleteEpoch`，最后一个批次删 inode、XATTR、INLINE、LOCKS 与记录。
//!
//! 2. 实现注记（M11）：去重记录（0x0C，每页 ≤ 1000 条）：会话记录已经不在了的那些点删（`gc_meta::dead_dedup`，ADR-0041 第 9 条）。
//! 3. 实现注记（M11）：changelog（0x0E）：从最旧的起修剪（`gc_meta::trim_changelog`：被所有消费者游标消费过的、超过 7 天的、比最新的旧
//!    1000 万条以上的）。
//! 4. 过期的 EVICTED 记录（M10），这一轮做完。
//!
//! 每一步都幂等（DeleteEpoch 删过的再删是 0 个文件；记录与 inode 的删除是带前提的批次，点删删过的再删无害），所以崩溃后从旧游标重放不会出错（3.10）。
//! 实现注记（M9）：一条记录的 DeleteEpoch 失败（多链时某条链的成员暂时不可用）只跳过这一条（计入积压），下一轮再试；链此刻没有
//! Serving 成员时不发请求、直接跳过（不然每条这样的记录都要等满数据面的总期限 30 s）。
//! 指标 `wmfs_gc_epochs_deleted_total`、`wmfs_gc_orphans_deleted_total`、`wmfs_gc_dedup_deleted_total`、`wmfs_gc_changelog_trimmed_total`
//! （本地孤儿扫描另有 `wmfs_gc_local_epochs_deleted_total`、`wmfs_gc_local_chain_dirs_deleted_total`）；fail point `plugin.gc.bg_task`
//! （本体包在每一步外层，两个任务都是）。

use std::sync::Arc;
use std::time::Duration;

use wmfs_core::api::metrics::{self, Counter};
use wmfs_core::base::types::ReqCtx;
use wmfs_core::base::{ErrCode, clock, log};
use wmfs_core::bg::task::{BgTask, RunOn, StepResult, TaskCtx};
use wmfs_core::hooks::manifest::{CORE_ABI_VERSION, Hook, Manifest};
use wmfs_core::meta::gc_meta::{self, GcItem};
use wmfs_core::meta::schema::keys::{GC_EPOCH, GC_MOVED, GC_ORPHAN, T_CHANGELOG, T_DEDUP, T_GC};

mod local;
pub use local::GcLocal;

pub static MANIFEST: Manifest = Manifest {
    name: "gc",
    version: "0.1.0",
    hooks: &[Hook::BgTask],
    altitude: 0, // bg_task 不排序
    hot_path: false,
    config_schema: "",
    abi_version: CORE_ABI_VERSION,
    attrs: &[],
    budget_ns: 0,
    fail_points: &["plugin.gc.bg_task"],
};

/// 每一步最多处理多少条记录（3.10："做一小步，≤ 100 个对象"；去重记录与 changelog 只是点删，一页 1000 条）。
const PAGE: usize = 100;
const DELETE_PAGE: usize = 1000;
/// 2 个心跳周期（3.5.6 的条件 ③）。
const SETTLE_MS: u64 = 6000;

pub struct Gc {
    term: u64,        // 上一次看到的 term（换了 = 换过 leader）
    ready_since: u64, // 这个 term 里第一次跑的时刻（mono_ms）：本 leader 大约从这时起就绪
    epochs: Arc<Counter>,
    orphans: Arc<Counter>,
    dedup: Arc<Counter>,
    trimmed: Arc<Counter>,
    backlog: u64, // 这一轮第 1 段里还没能删的 GC 记录（之后几段照样报它：一轮的积压）
}

impl Gc {
    pub fn new() -> Gc {
        Gc {
            term: 0,
            ready_since: 0,
            epochs: metrics::counter("wmfs_gc_epochs_deleted_total", "gc 删掉的废弃 epoch 数", &[]),
            orphans: metrics::counter("wmfs_gc_orphans_deleted_total", "gc 删掉的孤儿 inode 数", &[]),
            dedup: metrics::counter("wmfs_gc_dedup_deleted_total", "gc 删掉的去重记录数（会话已不在）", &[]),
            trimmed: metrics::counter("wmfs_gc_changelog_trimmed_total", "gc 修剪掉的 changelog 记录数", &[]),
            backlog: 0,
        }
    }
}

impl Default for Gc {
    fn default() -> Gc {
        Gc::new()
    }
}

/// 游标里标签字节之后的部分（只有一个标签字节 = 那一段的开头，见文件头）。
fn after_tag(cursor: &[u8]) -> &[u8] {
    if cursor.len() > 1 { cursor } else { &[] }
}

/// 对一条记录要删的每个 (epoch, 链) 发 DeleteEpoch。链此刻没有 Serving 成员（等最后的副本）→ 不发、马上算失败（不等满总期限）。
fn delete_data(ctx: &TaskCtx, it: &GcItem) -> Result<(), ErrCode> {
    for d in &it.deletes {
        if !ctx.data.chain_serving(d.chain) {
            return Err(ErrCode::NOT_SERVING);
        }
        ctx.data.delete_epoch(&d.cap, d.chain, it.ino, d.epoch)?;
    }
    Ok(())
}

impl BgTask for Gc {
    fn manifest(&self) -> &'static Manifest {
        &MANIFEST
    }

    fn kind(&self) -> &'static str {
        "gc"
    }

    fn runs_on(&self) -> RunOn {
        RunOn::MetaLeader
    }

    fn interval(&self) -> Duration {
        Duration::from_secs(1)
    }

    fn step(&mut self, ctx: &mut TaskCtx, cursor: &[u8]) -> Result<StepResult, ErrCode> {
        let term = ctx.meta.role().term;
        if term != self.term {
            (self.term, self.ready_since) = (term, clock::mono_ms());
        }
        match cursor.first().copied() {
            None | Some(T_GC) => self.records(ctx, cursor),
            Some(T_DEDUP) => {
                let page = gc_meta::dead_dedup(ctx.meta, after_tag(cursor), DELETE_PAGE)?;
                let n = page.dead.len() as u64;
                gc_meta::delete_keys(ctx.meta, page.dead)?;
                self.dedup.add(n);
                let cursor = if page.more { page.last } else { vec![T_CHANGELOG] };
                Ok(StepResult { cursor, items: n, bytes: 0, backlog: self.backlog, pass_done: false })
            }
            _ => {
                let (n, more) = gc_meta::trim_changelog(ctx.meta, ctx.now_ms, DELETE_PAGE)?;
                self.trimmed.add(n as u64);
                let mut items = n as u64;
                if !more {
                    items += gc_meta::expire_evicted(ctx.meta, ctx.now_ms)? as u64; // 一遍扫完：顺带删过期的 EVICTED 记录（M10）
                }
                let cursor = if more { vec![T_CHANGELOG] } else { vec![] };
                Ok(StepResult { cursor, items, bytes: 0, backlog: self.backlog, pass_done: !more })
            }
        }
    }
}

impl Gc {
    /// 第 1 段：GC 记录（见文件头）。
    fn records(&mut self, ctx: &mut TaskCtx, cursor: &[u8]) -> Result<StepResult, ErrCode> {
        if cursor.is_empty() {
            self.backlog = 0; // 新的一轮
        }
        let page = gc_meta::scan(ctx.meta, &ReqCtx::internal(), after_tag(cursor), PAGE)?;
        let settled = clock::mono_ms().saturating_sub(self.ready_since) >= SETTLE_MS
            && ctx.live.covers(self.term, &page.sessions);
        let (mut items, mut backlog) = (0, 0);
        for it in &page.items {
            let due = it.rec.due_ms <= ctx.now_ms;
            let closed = settled && !ctx.live.is_open(it.ino); // 没人打开着（打开集合可信了才算数）
            let ready = match it.kind {
                GC_EPOCH => due,
                // 迁移换下来的（见文件头）：距写记录也已超过 2 个心跳周期，在那之前打开它的会话都报上来了
                GC_MOVED => due && closed && it.rec.created_ms.saturating_add(SETTLE_MS) <= ctx.now_ms,
                GC_ORPHAN => due && closed,
                _ => false,
            };
            // 实现注记（M9）：某条链的成员暂时不可用、这一条的 DeleteEpoch 失败了 → 跳过它、下一轮再试，不挡住同一页里别的记录
            if ready && let Err(e) = delete_data(ctx, it) {
                log::warn(
                    "gc",
                    format_args!("inode {} epoch {} 的数据这次没删成：{e}（下一轮再试）", it.ino, it.epoch),
                );
                backlog += 1;
                continue;
            }
            match it.kind {
                GC_EPOCH | GC_MOVED if ready => {
                    gc_meta::drop_record(ctx.meta, it)?;
                    self.epochs.inc();
                    items += 1;
                }
                GC_ORPHAN if ready => {
                    gc_meta::finish_orphan(ctx.meta, it)?;
                    self.orphans.inc();
                    items += 1;
                }
                GC_EPOCH | GC_MOVED | GC_ORPHAN => backlog += 1, // 还没到期 / 可能还有人打开着：下一轮再看
                _ => {}
            }
        }
        let cursor = match page.items.last() {
            Some(i) if page.more => i.key.clone(),
            _ => vec![T_DEDUP], // GC 记录扫完了：下一段
        };
        self.backlog += backlog;
        Ok(StepResult { cursor, items, bytes: 0, backlog: self.backlog, pass_done: false })
    }
}

#[cfg(test)]
mod tests;
