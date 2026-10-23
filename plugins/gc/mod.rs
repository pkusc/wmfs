//! gc：后台任务插件（design.md 4.12 的 gc 一行、3.5.6、3.4.2.9，bg_task）——删除废弃的 epoch 与无人打开的孤儿 inode 的数据。
//!
//! 在元数据 leader 上运行（`RunOn::MetaLeader`，每轮结束隔 1 s 再开始下一轮）。每一步分页读 GC 记录（`meta::gc_meta`，每页 ≤ 100 条，
//! 游标 = 最后一条记录的 key）：
//! - kind 2（废弃 epoch：截断到 0、迁移换下来的旧 layout）：过了宽限期（`due_ms`，= 写记录时 + `epoch_gc_grace`，默认 60 s，演示时可
//!   调成 0）就对记录里的每条链发 `DeleteEpoch`（cap 带管理权限位，走 QoS 低优先级队列），然后删记录。
//! - kind 1（孤儿：最后一个链接被删了）：3.5.6 的三个条件全部满足才删——① leader 内存里没有任何会话打开着它；② 距 unlink 已超过
//!   2 个心跳周期（记录的 `due_ms`）；③ 本 leader 就绪已超过 2 个心跳周期（6 s），且所有存活会话都在这个 term 里报过打开文件的全集。
//!   然后对 inode 当前 layout 的每条链 `DeleteEpoch`，最后一个批次删 inode、XATTR、INLINE、LOCKS 与记录。
//!
//! 每一步都幂等（DeleteEpoch 删过的再删是 0 个文件；记录与 inode 的删除是带前提的批次），所以崩溃后从旧游标重放不会出错（3.10）。
//! 实现注记（M5）：这是 gc 的 M5 部分；被驱逐/已关闭会话的去重记录、EVICTED 过期、changelog 修剪、数据节点上的本地孤儿扫描
//! （`CheckEpochs`）在 M11 的完整版。指标 `wmfs_gc_epochs_deleted_total`、`wmfs_gc_orphans_deleted_total`；fail point
//! `plugin.gc.bg_task`（本体包在每一步外层）。

use std::sync::Arc;
use std::time::Duration;

use wmfs_core::api::metrics::{self, Counter};
use wmfs_core::base::types::ReqCtx;
use wmfs_core::base::{ErrCode, clock};
use wmfs_core::bg::task::{BgTask, RunOn, StepResult, TaskCtx};
use wmfs_core::hooks::manifest::{CORE_ABI_VERSION, Hook, Manifest};
use wmfs_core::meta::gc_meta::{self, GcItem};
use wmfs_core::meta::schema::keys::{GC_EPOCH, GC_ORPHAN};

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

/// 每一步最多处理多少条记录（3.10："做一小步，≤ 100 个对象"）。
const PAGE: usize = 100;
/// 2 个心跳周期（3.5.6 的条件 ③）。
const SETTLE_MS: u64 = 6000;

pub struct Gc {
    term: u64,        // 上一次看到的 term（换了 = 换过 leader）
    ready_since: u64, // 这个 term 里第一次跑的时刻（mono_ms）：本 leader 大约从这时起就绪
    epochs: Arc<Counter>,
    orphans: Arc<Counter>,
}

impl Gc {
    pub fn new() -> Gc {
        Gc {
            term: 0,
            ready_since: 0,
            epochs: metrics::counter("wmfs_gc_epochs_deleted_total", "gc 删掉的废弃 epoch 数", &[]),
            orphans: metrics::counter("wmfs_gc_orphans_deleted_total", "gc 删掉的孤儿 inode 数", &[]),
        }
    }
}

impl Default for Gc {
    fn default() -> Gc {
        Gc::new()
    }
}

/// 对一条记录要删的每个 (epoch, 链) 发 DeleteEpoch。
fn delete_data(ctx: &TaskCtx, it: &GcItem) -> Result<(), ErrCode> {
    for d in &it.deletes {
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
        let page = gc_meta::scan(ctx.meta, &ReqCtx::internal(), cursor, PAGE)?;
        let settled =
            clock::mono_ms().saturating_sub(self.ready_since) >= SETTLE_MS && ctx.live.covers(term, &page.sessions);
        let (mut items, mut backlog) = (0, 0);
        for it in &page.items {
            let due = it.rec.due_ms <= ctx.now_ms;
            match it.kind {
                GC_EPOCH if due => {
                    delete_data(ctx, it)?;
                    gc_meta::drop_record(ctx.meta, it)?;
                    self.epochs.inc();
                    items += 1;
                }
                GC_ORPHAN if due && settled && !ctx.live.is_open(it.ino) => {
                    delete_data(ctx, it)?;
                    gc_meta::finish_orphan(ctx.meta, it)?;
                    self.orphans.inc();
                    items += 1;
                }
                GC_EPOCH | GC_ORPHAN => backlog += 1, // 还没到期 / 可能还有人打开着：下一轮再看
                _ => {}
            }
        }
        let cursor = if page.more { page.items.last().map(|i| i.key.clone()).unwrap_or_default() } else { vec![] };
        Ok(StepResult { cursor, items, bytes: 0, backlog, pass_done: !page.more })
    }
}

#[cfg(test)]
mod tests;
