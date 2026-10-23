//! target 心跳的 leader 一侧（design.md 3.3.6 的 NodeHeartbeat、3.4.2.10）与 `StatFs`（3.3.4）。
//!
//! 心跳走只读路径（先过 ReadIndex，确认自己仍是 leader）：心跳里报的 `boot_id`、`phys_devs`、`read_only` 与已提交的 TargetRecV1
//! 不同才提交一个内部批次改它（同批 `map_epoch += 1`，集群图按 map_epoch 缓存，ADR-0033 第 9 条；`last_up_ms` 一并更新）——每秒的
//! 心跳本身不写 raft。每个 target 报的容量与已用记在 leader 内存里（[`TargetUsage`]，换了 leader 就从下一次心跳重新记起），
//! `StatFs` 用它求和。回复 `HeartbeatOk { lease_ms = target_lease, map_epoch, leader_wall_ms }`。
//!
//! 实现注记（M5）：这是心跳的最小版——不判 target 离线、没有租约与自我隔离（M9）；只认本节点自己的 target（TargetRecV1.node 与
//! 心跳的 node 一致，否则忽略那一项）。用量还没有接进 placement 的 `ChainView.used`（集群图里没有用量；rebalance 与按用量放置在 M13）。
//! StatFs：`bsize` 4096、`blocks`/`bfree` 来自各 Up target 最近一次心跳的容量与已用之和，inode 数不设上限（`files = ffree = 2^32 − 1`），
//! `namelen` 255。

use std::collections::HashMap;
use std::sync::Mutex;

use crate::base::types::ReqCtx;
use crate::base::{ErrCode, clock, lock};
use crate::hooks::traits::StoreStat;
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

/// leader 内存里各 target 最近一次心跳报的容量与已用。
#[derive(Default)]
pub struct TargetUsage {
    m: Mutex<HashMap<u32, StoreStat>>,
}

impl TargetUsage {
    fn record(&self, reports: &[TargetReport]) {
        let mut m = lock(&self.m);
        for r in reports {
            m.insert(r.target, StoreStat { capacity: r.capacity, used: r.used });
        }
    }

    /// 这些 target 的 (容量, 已用) 之和。
    fn sum(&self, ids: &[u32]) -> (u64, u64) {
        let m = lock(&self.m);
        ids.iter().filter_map(|t| m.get(t)).fold((0, 0), |(c, u), s| (c + s.capacity, u + s.used))
    }
}

/// 心跳里的一项与已提交的记录不同（需要改 TargetRecV1）吗。
fn stale(rec: &TargetRecV1, hb: &NodeHeartbeat, r: &TargetReport) -> bool {
    rec.node == hb.node && (rec.boot_id != hb.boot_id || rec.phys_devs != r.phys_devs || rec.read_only != r.read_only)
}

/// 处理一次 target 心跳（见文件头），返回编码好的回复。
pub fn heartbeat(
    eng: &dyn MetaEngine,
    ctx: &ReqCtx,
    usage: &TargetUsage,
    hb: NodeHeartbeat,
) -> Result<Vec<u8>, ErrCode> {
    let (mut lease_ms, mut map_epoch, mut changed) = (0, 0, false);
    eng.read(ctx, &mut |_v, c| {
        (lease_ms, map_epoch) = (c.settings.target_lease_ms, c.map.map_epoch);
        let rec = |t: u32| c.map.targets.iter().find(|(id, _)| *id == t).map(|(_, r)| r);
        changed = hb.targets.iter().any(|r| rec(r.target).is_some_and(|rec| stale(rec, &hb, r)));
        Ok(MetaReply::Ok)
    })?;
    usage.record(&hb.targets);
    if changed {
        let hb2 = hb.clone();
        let op: TxnOp = Box::new(move |v, c| {
            let mut any = false;
            for r in &hb2.targets {
                let key = keys::target(r.target);
                let Some(mut rec) = kv::get::<TargetRecV1>(v, &key)?.filter(|rec| stale(rec, &hb2, r)) else {
                    continue;
                };
                (rec.boot_id, rec.phys_devs, rec.read_only) = (hb2.boot_id, r.phys_devs.clone(), r.read_only);
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
