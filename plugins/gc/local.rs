//! gc 的数据节点部分 [`GcLocal`]（design.md 4.12 的 gc 一行"数据节点：…"，`RunOn::EveryDataNode`，任务名 `gc-local`；实现注记（M11））：
//! 本地孤儿扫描——删掉那些元数据里已经没有、却还留在本节点 target 上的数据。
//!
//! - **死了的 epoch**：`list_epochs` 列出本 target 上的 `(链, ino, epoch, 目录 mtime)`（一轮每个 target 只列一次，留在内存里；
//!   重启之后按游标重列一次；新的一轮——空游标——先丢掉缓存的列表），按批（≤ 200）问元数据 leader `CheckEpochs`（死了 = inode 不在了，
//!   或这个 epoch 既不是它当前的、也不是正在迁移的 `pending_epoch`，也没有 leader 的 gc 还管着的 GC 记录，`meta::data_reports`）；
//!   死了的、而且目录 mtime 早于 max(1 h, `epoch_gc_grace`)（测试用 fail point `gc.local.min_age_ms` 的 `return(<毫秒>)` 缩短）→
//!   放弃它的 pending、删掉整个目录。这样清理的是 leader 的 gc 已经不管的残留：节点离线期间错过的 DeleteEpoch、gc 删过之后才落下的
//!   chunk（旧 leader 迟到的 uninline、迁移作废的 epoch，ADR-0039 第 1 条）。leader 的 gc 还管着的旧 epoch（有 GC 记录：还在宽限期里
//!   的截断、文件还打开着的迁移，3.5.6 的 M11 注记）不归它。
//! - **旧链目录**（这个 target 的 epoch 扫完之后）：这个 target 已经不是成员、也不是它的 `last_serving`（链等它回来）、`removed` 里也没有它
//!   （离线超过 `rejoin_window` 之后就被清掉了）的链，整个链目录删掉——它回不去了，链管理器要用它也会以 Syncing 身份重新同步。宁可不删：
//!   本节点的集群图不新（I5 不成立：多半与 leader 分区）、集群图里没有这条链（列表比集群图新：刚生成的链）、这条链在本 target 上最近
//!   （min_age 之内）还写过东西，都不删；每删一条之前再取一次最新的集群图核对。判定是 [`keep_chain_dir`]。
//!
//! 游标（插件私有的字节）：postcard(`Cursor`) = 做到哪个 target 的哪一项之后（解不开就从第一个 target 开始）。每一项都幂等（删过的再删
//! 什么也不做）。

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use wmfs_core::api::failpoint::fail_point_ms;
use wmfs_core::api::metrics::{self, Counter};
use wmfs_core::base::{ErrCode, clock, log};
use wmfs_core::bg::task::{BgTask, RunOn, StepResult, TaskCtx};
use wmfs_core::data::node_hb::leader_call;
use wmfs_core::data::{DataNode, Target, chain, replica};
use wmfs_core::hooks::manifest::Manifest;
use wmfs_core::meta::schema::records::ChainRecV1;
use wmfs_core::proto::peer::{NodeOp, NodeReply};
use wmfs_core::proto::{decode_body, encode_body};

use super::MANIFEST;

const PAGE: usize = 200;
const MIN_AGE_MS: u64 = 3600 * 1000;

/// 一个 epoch 目录：(链, ino, epoch)。
type Pos = (u32, u64, u32);
/// `list_epochs` 列出的一项：(链, ino, epoch, 目录 mtime)。
type EpochDir = (u32, u64, u32, u64);

/// 游标：(target, 这个 target 上做到了哪个 epoch 目录之后)。
#[derive(Serialize, Deserialize, Default, Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Cursor {
    pub(super) target: u32,
    pub(super) after: Option<Pos>,
}

pub struct GcLocal {
    list: Option<(u32, Vec<EpochDir>)>, // 这一轮正在扫的 target 与它的 epoch 目录（见文件头）
    epochs: Arc<Counter>,
    dirs: Arc<Counter>,
}

impl Default for GcLocal {
    fn default() -> GcLocal {
        GcLocal::new()
    }
}

/// 本地孤儿扫描等多久才删（max(1 h, `epoch_gc_grace`)，见文件头）。
fn min_age(node: &DataNode) -> u64 {
    let grace = node.map.get().map_or(0, |m| m.settings.epoch_gc_grace_ms);
    fail_point_ms("gc.local.min_age_ms", MIN_AGE_MS.max(grace))
}

/// target `t` 上链 `c` 的目录要留着吗（见文件头；`c` = None：集群图里没有这条链）。
pub fn keep_chain_dir(c: Option<&ChainRecV1>, t: u32, rejoin_window_ms: u64, now: u64) -> bool {
    c.is_none_or(|c| {
        c.members.iter().any(|(m, _)| *m == t)
            || c.last_serving == Some(t)
            || c.removed.iter().any(|(m, at)| *m == t && at.saturating_add(rejoin_window_ms) > now)
    })
}

impl GcLocal {
    pub fn new() -> GcLocal {
        GcLocal {
            list: None,
            epochs: metrics::counter("wmfs_gc_local_epochs_deleted_total", "本地孤儿扫描删掉的 epoch 目录数", &[]),
            dirs: metrics::counter("wmfs_gc_local_chain_dirs_deleted_total", "本地孤儿扫描删掉的旧链目录数", &[]),
        }
    }

    /// 一个 target 上的一批（见文件头）；回 (删了几个, 这一批的最后一项（None = 这个 target 扫完了）)。
    fn batch(
        &mut self,
        node: &Arc<DataNode>,
        t: &Target,
        after: Option<Pos>,
        now: u64,
    ) -> Result<(u64, Option<Pos>), ErrCode> {
        if self.list.as_ref().is_none_or(|(id, _)| *id != t.id) {
            let mut all = node.store(|| t.store.list_epochs(None, usize::MAX))?;
            all.sort();
            self.list = Some((t.id, all));
        }
        let Some((_, all)) = &self.list else { return Err(ErrCode::INTERNAL) };
        let from = after.map_or(0, |a| all.partition_point(|(c, i, e, _)| (*c, *i, *e) <= a));
        let page = &all[from..(from + PAGE).min(all.len())];
        if page.is_empty() {
            return Ok((0, None));
        }
        let items: Vec<(u64, u32)> = page.iter().map(|(_, ino, e, _)| (*ino, *e)).collect();
        let dead = match leader_call(node, &NodeOp::CheckEpochs { items })? {
            NodeReply::Epochs { dead } if dead.len() == page.len() => dead,
            _ => return Err(ErrCode::INTERNAL),
        };
        let min_age = min_age(node);
        let mut n = 0;
        for ((ch, ino, e, mtime), d) in page.iter().zip(dead) {
            if d && mtime.saturating_add(min_age) <= now {
                replica::drop_epoch(node, *ch, *ino, *e);
                node.store(|| t.store.delete_epoch(*ch, *ino, *e))?;
                n += 1;
            }
        }
        let last = page.last().map(|(c, i, e, _)| (*c, *i, *e));
        Ok((n, if from + page.len() < all.len() { last } else { None }))
    }

    /// 这个 target 已经回不去的链目录（见文件头）：用这一轮的列表（它比下面取的集群图旧）。
    fn old_chains(&self, node: &DataNode, t: &Target, now: u64) -> Result<u64, ErrCode> {
        let Some((_, all)) = self.list.as_ref().filter(|(id, _)| *id == t.id) else { return Ok(0) };
        let mut newest: BTreeMap<u32, u64> = BTreeMap::new(); // 链 → 本 target 上它最近一次写的时刻（epoch 目录的 mtime）
        for (ch, _, _, mtime) in all {
            let m = newest.entry(*ch).or_default();
            *m = (*m).max(*mtime);
        }
        let min_age = min_age(node);
        let mut n = 0;
        for (ch, mtime) in newest.into_iter().filter(|(_, m)| m.saturating_add(min_age) <= now) {
            let Some(map) = node.map.get() else { return Ok(n) };
            if chain::check_i5(node, &map, t.id).is_err() {
                return Ok(n); // 集群图不一定新：不删
            }
            if !keep_chain_dir(chain::find(&map, ch), t.id, map.settings.rejoin_window_ms, now) {
                log::info(
                    "gc",
                    format_args!("target {} 已经回不去链 {ch}（最近一次写在 {mtime}）：删掉它的目录", t.id),
                );
                node.store(|| t.store.delete_chain_dir(ch))?;
                n += 1;
            }
        }
        Ok(n)
    }
}

impl BgTask for GcLocal {
    fn manifest(&self) -> &'static Manifest {
        &MANIFEST
    }

    fn kind(&self) -> &'static str {
        "gc-local"
    }

    fn runs_on(&self) -> RunOn {
        RunOn::EveryDataNode
    }

    fn interval(&self) -> Duration {
        Duration::from_secs(600)
    }

    fn step(&mut self, ctx: &mut TaskCtx, cursor: &[u8]) -> Result<StepResult, ErrCode> {
        let node = ctx.node.ok_or(ErrCode::NOT_READY)?;
        if cursor.is_empty() {
            self.list = None; // 新的一轮（含 wmfs task run 让这一轮从头再来）：重新列，不漏掉之后才落下的目录（ADR-0042 第 8 条）
        }
        let cur: Cursor = decode_body(cursor).unwrap_or_default();
        let Some(t) = node.targets.values().find(|t| t.id >= cur.target && !t.read_only()) else {
            self.list = None;
            return Ok(StepResult { pass_done: true, ..Default::default() }); // 每个 target 都扫完了
        };
        let now = clock::wall_ms();
        let (mut items, last) = self.batch(node, t, if t.id == cur.target { cur.after } else { None }, now)?;
        self.epochs.add(items);
        let next = match last {
            Some(p) => Cursor { target: t.id, after: Some(p) },
            None => {
                let d = self.old_chains(node, t, now)?; // 这个 target 的 epoch 扫完了：再看旧链目录
                self.dirs.add(d);
                items += d;
                self.list = None;
                Cursor { target: t.id + 1, after: None }
            }
        };
        Ok(StepResult { cursor: encode_body(&next), items, bytes: 0, backlog: 0, pass_done: false })
    }
}
