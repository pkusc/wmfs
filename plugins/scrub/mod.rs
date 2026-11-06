//! scrub：后台任务插件（design.md 4.12 的 scrub 一行、3.2.5 R5、ADR-0040 第 10 条，bg_task）——每个数据节点定期把自己的每个副本读一遍、
//! 核对校验和；坏了就从同一条链的另一个正常副本修好，修不了就如实上报数据丢失。
//!
//! `RunOn::EveryDataNode`（每个有 target 的节点各跑各的，进度在本地的游标文件里），一轮做完隔 24 h 再来（`wmfs task run scrub` 马上从头
//! 来一轮），默认限速 50 MiB/s（`wmfs task set scrub bw=…`）。一轮 = 本节点每个 target 上、它是成员的每条链（链号升序）：
//! 1. 本地已提交的 chunk（`list_chain`，按 ChunkId 升序）逐个检查（`data::repair::verify`：读出每一块、解码、校验）。坏了 → 再看一眼已提交的
//!    版本还是不是查的那个（查的这段时间里提交了新版本——例如整块覆盖——就不是它坏了，下一轮再查）→ 标 Corrupt（`wmfs_chunk_corrupt_total`）、
//!    从别的 Serving 成员修复并上报（`data::repair::repair_and_report`）：修好了 → leader 写 `ChecksumRepaired`；链上没有别的 Serving 成员、
//!    或它们的副本也都是坏的 → leader 写 `DataLossDetected`（之后读它照旧 `CORRUPT`，客户端得到 `EIO`）；现在修不了（对方有在途的写、连不上）
//!    → 留着，下一轮再看。查不了（还拿不到数据密钥、少了变换插件）不算坏。
//! 2. 本节点是这条链的 Serving 成员、又不是 last serving（L）时，再把 L 的 chunk 列表过一遍（每页 1000 个）：L 有、本地没有或比本地新（双方都
//!    没有在途的写）→ 从别的副本补上（"Serving 成员缺基础版本"的更大范围检查，ADR-0040 第 10 条），同样上报 `ChecksumRepaired`。
//!
//! 列表留在内存里：每条链的本地列表一轮只列一次、L 的列表一页只要一次（local-fs 列一次要遍历整条链的目录）；游标照旧持久化，
//! 重启之后按游标重列一次；新的一轮（空游标，含 `wmfs task run` 让这一轮从头再来）先丢掉缓存的列表。一步至少处理一项，
//! 之后到了大约一秒的限速量（`RateLimiter::step_bytes`）或 1 s 就停。游标（插件私有的字节）= postcard (`Cursor`)，
//! 解不开就从这一轮的开头来。每一步都幂等（再查一遍、再装一份同版本的副本都无害）。指标
//! `wmfs_scrub_chunks_total{result="good"|"repaired"|"lost"|"later"}`（另有框架的 `wmfs_task_*{task="scrub"}`）；fail point
//! `plugin.scrub.bg_task`（本体包在每一步外层）。

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use wmfs_core::api::metrics::{self, Counter};
use wmfs_core::base::types::ChunkId;
use wmfs_core::base::{ErrCode, clock, log};
use wmfs_core::bg::task::{BgTask, RunOn, StepResult, TaskCtx};
use wmfs_core::data::repair::{self, Repair, Verdict};
use wmfs_core::data::{DataNode, Target, chain, read};
use wmfs_core::hooks::manifest::{CORE_ABI_VERSION, Hook, Manifest};
use wmfs_core::proto::{decode_body, encode_body};

pub static MANIFEST: Manifest = Manifest {
    name: "scrub",
    version: "0.1.0",
    hooks: &[Hook::BgTask],
    altitude: 0, // bg_task 不排序
    hot_path: false,
    config_schema: "",
    abi_version: CORE_ABI_VERSION,
    attrs: &[],
    budget_ns: 0,
    fail_points: &["plugin.scrub.bg_task"],
};

const DEFAULT_RATE: u64 = 50 << 20;
const MAX_STEP_BYTES: u64 = 64 << 20;
const MAX_STEP_MS: u64 = 1000;
const L_PAGE: u32 = 1000;

/// 游标：(target, 链, 段（0 = 本地检查，1 = 与 L 比对）, 这一段做到了哪个 chunk 之后)。
#[derive(Serialize, Deserialize, Default, Clone, Copy, Debug, PartialEq, Eq)]
struct Cursor {
    target: u32,
    chain: u32,
    phase: u8,
    after: Option<ChunkId>,
}

/// 这一段的列表（见文件头）：第 1 段 = 本地的 chunk；第 2 段 = L 的一页里要补的 (chunk, 本地已提交的版本)，`last`/`more` 是这一页的
/// 最后一个与 L 还有没有下一页。
struct Listing {
    key: (u32, u32, u8),
    items: Vec<(ChunkId, Option<u64>)>,
    last: Option<ChunkId>,
    more: bool,
}

/// 一步做了多少：做到了哪个 chunk 之后、读过（或修复时取回）的字节、处理的个数、这一段做完了没有。
#[derive(Default)]
struct Batch {
    last: Option<ChunkId>,
    bytes: u64,
    items: u64,
    done: bool,
}

pub struct Scrub {
    list: Option<Listing>,
    good: Arc<Counter>,
    repaired: Arc<Counter>,
    lost: Arc<Counter>,
    later: Arc<Counter>,
}

impl Scrub {
    pub fn new() -> Scrub {
        let c = |r| metrics::counter("wmfs_scrub_chunks_total", "scrub 检查过的 chunk 副本", &[("result", r)]);
        Scrub { list: None, good: c("good"), repaired: c("repaired"), lost: c("lost"), later: c("later") }
    }

    /// 修复一个坏了（或缺了）的副本、上报、计数（见文件头）。
    fn repair_and_count(&self, node: &Arc<DataNode>, t: &Target, id: &ChunkId, ours: Option<u64>) {
        match repair::repair_and_report(node, t, id, ours) {
            Repair::Repaired { .. } => self.repaired.inc(),
            Repair::Lost => self.lost.inc(),
            Repair::Later(_) => self.later.inc(),
        }
    }

    /// 第 1 段的一批（见文件头）。
    fn check_local_batch(
        &mut self,
        node: &Arc<DataNode>,
        t: &Target,
        cur: &Cursor,
        budget: u64,
    ) -> Result<Batch, ErrCode> {
        let key = (cur.target, cur.chain, 0);
        if self.list.as_ref().is_none_or(|l| l.key != key) {
            let mut ids = node.store(|| t.store.list_chain(cur.chain, None, usize::MAX))?;
            ids.sort();
            self.list =
                Some(Listing { key, items: ids.into_iter().map(|id| (id, None)).collect(), last: None, more: false });
        }
        let Some(l) = &self.list else { return Err(ErrCode::INTERNAL) };
        let from = cur.after.map_or(0, |a| l.items.partition_point(|(id, _)| *id <= a));
        let (t0, mut b) = (clock::mono_ms(), Batch { last: cur.after, ..Default::default() });
        for (id, _) in &l.items[from..] {
            if b.items > 0 && (b.bytes >= budget || clock::mono_ms().saturating_sub(t0) >= MAX_STEP_MS) {
                return Ok(b);
            }
            match repair::verify(node, t, id) {
                Ok(Verdict::Good { bytes, .. }) => {
                    b.bytes += bytes;
                    self.good.inc();
                }
                Ok(Verdict::Missing) => {}
                Ok(Verdict::Bad { version }) if read::committed_version(node, t, id) == version => {
                    node.report_corrupt(t, id, ErrCode::CHECKSUM_MISMATCH);
                    self.repair_and_count(node, t, id, version);
                }
                Ok(Verdict::Bad { .. }) => log::info("scrub", format_args!("{id:?} 查的时候提交了新版本：下一轮再查")),
                Err(e) => log::info("scrub", format_args!("{id:?} 现在查不了：{e}")),
            }
            (b.items, b.last) = (b.items + 1, Some(*id));
        }
        b.done = true;
        Ok(b)
    }

    /// 第 2 段的一批（见文件头）：当前这一页里要补的做完了（或还没取），就从这一页的最后一个之后取下一页。修复取回的字节不计进限速
    /// （本来就少：Serving 成员缺了 L 有的版本是不变量破坏），只受 1 s 的限制。
    fn fill_from_last_batch(&mut self, node: &Arc<DataNode>, t: &Target, cur: &Cursor) -> Result<Batch, ErrCode> {
        let key = (cur.target, cur.chain, 1);
        let from = |l: &Listing| cur.after.map_or(0, |a| l.items.partition_point(|(id, _)| *id <= a));
        if !self.list.as_ref().is_some_and(|l| l.key == key && from(l) < l.items.len()) {
            let p = repair::missing_from_last(node, t, cur.chain, cur.after, L_PAGE)?;
            self.list = Some(Listing { key, items: p.todo, last: p.last, more: p.more });
        }
        let Some(l) = &self.list else { return Err(ErrCode::INTERNAL) };
        let (t0, mut b) = (clock::mono_ms(), Batch { last: cur.after, ..Default::default() });
        for (id, mine) in &l.items[from(l)..] {
            if b.items > 0 && clock::mono_ms().saturating_sub(t0) >= MAX_STEP_MS {
                return Ok(b);
            }
            self.repair_and_count(node, t, id, *mine);
            (b.items, b.last) = (b.items + 1, Some(*id));
        }
        (b.last, b.done) = (l.last, !l.more); // 这一页处理完了
        Ok(b)
    }
}

impl Default for Scrub {
    fn default() -> Scrub {
        Scrub::new()
    }
}

/// 本节点每个 target 上它是成员的链：(target, 链)，升序。
fn work_of(node: &DataNode) -> Vec<(u32, u32)> {
    let Some(map) = node.map.get() else { return vec![] };
    let mut work: Vec<(u32, u32)> = vec![];
    for t in node.targets.values() {
        let mine = map.chains.iter().filter(|(_, c)| c.members.iter().any(|(m, _)| *m == t.id));
        work.extend(mine.map(|(id, _)| (t.id, *id)));
    }
    work.sort();
    work
}

impl BgTask for Scrub {
    fn manifest(&self) -> &'static Manifest {
        &MANIFEST
    }

    fn kind(&self) -> &'static str {
        "scrub"
    }

    fn runs_on(&self) -> RunOn {
        RunOn::EveryDataNode
    }

    fn interval(&self) -> Duration {
        Duration::from_secs(24 * 3600)
    }

    fn default_rate(&self) -> u64 {
        DEFAULT_RATE
    }

    fn step(&mut self, ctx: &mut TaskCtx, cursor: &[u8]) -> Result<StepResult, ErrCode> {
        let node = ctx.node.ok_or(ErrCode::NOT_READY)?;
        let map = node.map.get().ok_or(ErrCode::NOT_READY)?;
        if cursor.is_empty() {
            self.list = None; // 新的一轮（含 wmfs task run 让这一轮从头再来）：重新列，不漏掉之后才写的 chunk（ADR-0042 第 8 条）
        }
        let mut cur: Cursor = decode_body(cursor).unwrap_or_default();
        let Some(&(tid, ch)) = work_of(node).iter().find(|w| **w >= (cur.target, cur.chain)) else {
            self.list = None;
            return Ok(StepResult { pass_done: true, ..Default::default() });
        };
        if (tid, ch) != (cur.target, cur.chain) {
            cur = Cursor { target: tid, chain: ch, ..Default::default() };
        }
        let t = node.targets.get(&tid).ok_or(ErrCode::NOT_SERVING)?;
        let budget = ctx.limiter.step_bytes(MAX_STEP_BYTES);
        let b = if cur.phase == 0 {
            self.check_local_batch(node, t, &cur, budget)?
        } else {
            self.fill_from_last_batch(node, t, &cur).unwrap_or_else(|e| {
                log::info("scrub", format_args!("链 {ch}：这一轮没能与 last serving 比对（{e}），下一轮再比"));
                Batch { done: true, ..Default::default() } // 不让一次连不上把这一轮剩下的部分推迟 24 h
            })
        };
        cur.after = b.last;
        if b.done {
            let check_l = chain::find(&map, ch).is_some_and(|c| {
                chain::last_serving(c) != Some(tid)
                    && chain::local(node, c).is_some_and(|(_, p)| chain::serving_at(c, p))
            });
            cur = match cur.phase {
                0 if check_l => Cursor { phase: 1, after: None, ..cur },
                _ => Cursor { target: tid, chain: ch.saturating_add(1), ..Default::default() }, // 下一条链
            };
            self.list = None;
        }
        Ok(StepResult { cursor: encode_body(&cur), items: b.items, bytes: b.bytes, backlog: 0, pass_done: false })
    }
}

#[cfg(test)]
mod tests;
