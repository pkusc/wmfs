//! target 心跳的 leader 一侧（design.md 3.3.6 的 NodeHeartbeat、3.4.2.10）与 `StatFs`（3.3.4）。
//!
//! 心跳走只读路径（先过 ReadIndex，确认自己仍是 leader）：心跳里报的 `phys_devs`、`read_only` 与已提交的 TargetRecV1 不同才提交一个内部
//! 批次改它（同批 `map_epoch += 1`，集群图按 map_epoch 缓存，ADR-0033 第 9 条；`last_up_ms` 一并更新）——每秒的心跳本身不写 raft。
//! 每个 target 报的容量与已用、心跳的时刻与 `boot_id`、各节点上报的转发失败记在 leader 内存里（[`TargetUsage`]，带着记下时的 term：
//! 换了 leader 就当作不知道，从下一次心跳重新记起，3.4.1.10），`StatFs` 与链管理器（`chain_mgr.rs`）用它。回复
//! `HeartbeatOk { lease_ms = target_lease, map_epoch, leader_wall_ms }`。
//!
//! 实现注记（M5）：只认本节点自己的 target（TargetRecV1.node 与心跳的 node 一致，否则忽略那一项——M9 审查后内存里的心跳情况也这样：
//! 它决定 target 是否存活，复制出来的 target 目录在别的节点上心跳不能让死掉的 target 显得还活着）。StatFs：`bsize` 4096、`blocks`/`bfree`
//! 来自各 Up target 最近一次心跳的容量与已用之和，inode 数不设上限（`files = ffree = 2^32 − 1`），`namelen` 255。
//! 实现注记（M9）：`boot_id` 的变化不在这里写——它要与链管理器的降级（或唯一幸存者的 incarnation + 1）在同一个批次里生效，否则集群图里
//! 先出现新 boot_id、重启过的成员却还是 Serving，它就会带着丢掉的 pending 服务读（R6）。

use std::collections::HashMap;
use std::sync::Mutex;

use crate::base::types::ReqCtx;
use crate::base::{ErrCode, clock, lock};
use crate::hooks::traits::StoreStat;
use crate::meta::cluster::chain_plan::{FRESH_MS, FwdFailure, Seen};
use crate::meta::cluster::chains::TARGET_UP;
use crate::meta::engine::txn::{MetaEngine, TxnOp, txn_blocking};
use crate::meta::schema::keys;
use crate::meta::schema::kv;
use crate::meta::schema::records::TargetRecV1;
use crate::proto::meta::MetaReply;
use crate::proto::peer::{NodeHeartbeat, NodeReply, TargetReport};

/// StatFs 的块大小、inode 数（不设上限）与名字的最大长度（3.1.1）。
const BSIZE: u64 = 4096;
const FILES: u64 = u32::MAX as u64;
const NAMELEN: u32 = 255;

/// 一个 target 最近一次心跳（mono_ms、boot_id、记下时的 term）与它报的容量、已用。
struct Beat {
    at_ms: u64,
    boot: u64,
    term: u64,
    stat: StoreStat,
}

/// leader 内存里的心跳情况（见文件头）。
#[derive(Default)]
pub struct TargetUsage {
    m: Mutex<HashMap<u32, Beat>>,
    fails: Mutex<HashMap<u32, Vec<FwdFailure>>>, // 节点 → 它最近一次心跳里报的持续转发失败
}

impl TargetUsage {
    /// 记下一次心跳：只记 `mine` 里的 target（集群图里属于发心跳的节点的）。转发失败的时长按报告节点自己的钟算（心跳里的 wall_ms − 起点）。
    fn record(&self, hb: &NodeHeartbeat, term: u64, mine: &[u32]) {
        let (mut m, at_ms) = (lock(&self.m), clock::mono_ms());
        for r in hb.targets.iter().filter(|r| mine.contains(&r.target)) {
            let stat = StoreStat { capacity: r.capacity, used: r.used };
            m.insert(r.target, Beat { at_ms, boot: hb.boot_id, term, stat });
        }
        let f = |&(chain, down, since): &(u32, u32, u64)| FwdFailure {
            node: hb.node,
            chain,
            down,
            lasted_ms: hb.wall_ms.saturating_sub(since),
            heard_ms: at_ms,
        };
        lock(&self.fails).insert(hb.node, hb.forward_failures.iter().map(f).collect());
    }

    /// 这些 target 的 (容量, 已用) 之和。
    fn sum(&self, ids: &[u32]) -> (u64, u64) {
        let m = lock(&self.m);
        ids.iter().filter_map(|t| m.get(t)).fold((0, 0), |(c, u), b| (c + b.stat.capacity, u + b.stat.used))
    }

    /// 本任期内、最近 `lease_ms` 里来过心跳的 target：报的 boot_id、最近 2 s 内来过没有（链管理器用，3.4.2.7）。
    pub fn seen(&self, term: u64, now_ms: u64, lease_ms: u64) -> HashMap<u32, Seen> {
        let live = |b: &&Beat| b.term == term && now_ms.saturating_sub(b.at_ms) <= lease_ms;
        let m = lock(&self.m);
        m.iter()
            .filter(|(_, b)| live(b))
            .map(|(t, b)| (*t, Seen { boot: b.boot, fresh: now_ms.saturating_sub(b.at_ms) <= FRESH_MS }))
            .collect()
    }

    /// 本任期内最近 FRESH_MS 里听到的 target `t` 的 boot_id（没听到 → None；管理 API 的链成员状态用）。
    pub fn heard(&self, t: u32, term: u64, now_ms: u64) -> Option<u64> {
        let m = lock(&self.m);
        m.get(&t).filter(|b| b.term == term && now_ms.saturating_sub(b.at_ms) <= FRESH_MS).map(|b| b.boot)
    }

    /// 各节点最近一次心跳里报的持续转发失败。
    pub fn failures(&self) -> Vec<FwdFailure> {
        lock(&self.fails).values().flatten().copied().collect()
    }
}

/// 心跳里的一项与已提交的记录不同（需要改 TargetRecV1）吗（boot_id 归链管理器，见文件头）。
fn stale(rec: &TargetRecV1, hb: &NodeHeartbeat, r: &TargetReport) -> bool {
    rec.node == hb.node && (rec.phys_devs != r.phys_devs || rec.read_only != r.read_only)
}

/// 处理一次 target 心跳（见文件头），返回编码好的回复。
pub fn heartbeat(
    eng: &dyn MetaEngine,
    ctx: &ReqCtx,
    usage: &TargetUsage,
    hb: NodeHeartbeat,
) -> Result<Vec<u8>, ErrCode> {
    let (mut lease_ms, mut map_epoch, mut changed, mut mine) = (0, 0, false, Vec::new());
    eng.read(ctx, &mut |_v, c| {
        (lease_ms, map_epoch) = (c.settings.target_lease_ms, c.map.map_epoch);
        let rec = |t: u32| c.map.targets.iter().find(|(id, _)| *id == t).map(|(_, r)| r);
        changed = hb.targets.iter().any(|r| rec(r.target).is_some_and(|rec| stale(rec, &hb, r)));
        mine = hb.targets.iter().map(|r| r.target).filter(|t| rec(*t).is_some_and(|r| r.node == hb.node)).collect();
        Ok(MetaReply::Ok)
    })?;
    usage.record(&hb, eng.role().term, &mine);
    if changed {
        let hb2 = hb.clone();
        let op: TxnOp = Box::new(move |v, c| {
            let mut any = false;
            for r in &hb2.targets {
                let key = keys::target(r.target);
                let Some(mut rec) = kv::get::<TargetRecV1>(v, &key)?.filter(|rec| stale(rec, &hb2, r)) else {
                    continue;
                };
                (rec.phys_devs, rec.read_only) = (r.phys_devs.clone(), r.read_only);
                rec.last_up_ms = c.req.now_ms;
                kv::put(v, &key, &rec);
                any = true;
            }
            if any {
                kv::bump_map_epoch(v)?; // 改了 target 记录：同批 map_epoch += 1（3.3.6）
            }
            Ok(MetaReply::Ok)
        });
        let mut c = ctx.clone();
        (c.seq, c.internal) = (0, true);
        txn_blocking(eng, c, op)?;
        map_epoch += 1; // 新的集群图：让数据节点马上去拉（并发的心跳也可能已经改过它，多拉一次无害）
    }
    let leader_wall_ms = clock::wall_ms();
    Ok(crate::proto::encode_body(&NodeReply::HeartbeatOk { lease_ms, map_epoch, leader_wall_ms }))
}

/// `StatFs`（见文件头）。
pub fn statfs(eng: &dyn MetaEngine, ctx: &ReqCtx, usage: &TargetUsage) -> Result<MetaReply, ErrCode> {
    let mut ids = Vec::new();
    eng.read(ctx, &mut |_v, c| {
        ids = c.map.targets.iter().filter(|(_, t)| t.state == TARGET_UP).map(|(id, _)| *id).collect();
        Ok(MetaReply::Ok)
    })?;
    let (cap, used) = usage.sum(&ids);
    let (blocks, bfree) = (cap / BSIZE, cap.saturating_sub(used) / BSIZE);
    Ok(MetaReply::StatFs { bsize: BSIZE as u32, blocks, bfree, files: FILES, ffree: FILES, namelen: NAMELEN })
}
