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
//! 实现注记（M10 审查）：链管理器"因转发失败已降级、等第二步移出"的成员（ADR-0041 第 13 条）也记在这里（[`TargetUsage::parting`]）：
//! 链管理器在 cluster-mgr 线程上写，`SyncDone` 在 meta-read 线程上看（不再提升它们）；这个结构本来就是两边共用的 leader 内存。
//! 实现注记（M13）：各节点心跳报的保底欠额（`qos_deficit`）也记在这里：`wmfs status` 的 qos 一行取 5 s 之内的心跳之和
//! （[`TargetUsage::deficit`]）；某个 qos_id 的欠额连续 10 s 都大于 0（数据节点报的已经是"上一个 10 s 窗口大半时间有积压"的），
//! cluster-mgr 写一次 `PolicyViolation{kind=qos_reservation_unmet}`（[`TargetUsage::unmet`]，3.11、6.2 第 4 层；回到 0 之后再出现再写）。
//! 实现注记（M13 收尾，ADR-0044 第 5 条）：心跳报的各类 p99 延迟（`class_p99`）同样记下：某个节点上某个类的 p99 连续 10 s 超过它的
//! `latency_target`，cluster-mgr 写一次 `PolicyViolation{kind=latency_target_exceeded}`（[`TargetUsage::slow`]；与保底欠额共用
//! [`episodes`]）。

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::Hash;
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

/// 一个 target 最近一次心跳（mono_ms、boot_id、记下时的 term）、它报的容量与已用、节点报的 `map_epoch_seen`（M10 的驱逐看它）。
struct Beat {
    at_ms: u64,
    boot: u64,
    term: u64,
    stat: StoreStat,
    map_epoch_seen: u64,
}

/// 一个节点最近一次心跳报的 QoS 量：保底欠额与各类的 p99 延迟。
struct QosBeat {
    heard_ms: u64,              // 收到时的 mono_ms
    per_qos: Vec<(u64, u32)>,   // (qos_id, 成本单位/秒)
    class_p99: Vec<(u16, u32)>, // (类 id, 微秒)
}

/// 心跳报的欠额与 p99 多久之内算数；欠额大于 0、p99 超过目标连续多久才写一次 PolicyViolation（见文件头）。
const DEFICIT_FRESH_MS: u64 = 5_000;
const UNMET_AFTER_MS: u64 = 10_000;

/// leader 内存里的心跳情况（见文件头）。
#[derive(Default)]
pub struct TargetUsage {
    m: Mutex<HashMap<u32, Beat>>,
    fails: Mutex<HashMap<u32, Vec<FwdFailure>>>, // 节点 → 它最近一次心跳里报的持续转发失败
    parting: Mutex<BTreeSet<(u32, u32)>>,        // 链管理器记着要移出的 (链, target)（见文件头）
    qos: Mutex<HashMap<u32, QosBeat>>,           // 实现注记（M13）：节点 → 它最近一次心跳报的保底欠额与 p99
    unmet: Mutex<HashMap<u64, (u64, bool)>>, // qos_id → (欠额从这个时刻起一直大于 0, 这一段报过 PolicyViolation 没有)
    slow: Mutex<HashMap<(u16, u32), (u64, bool)>>, // (类, 节点) → (p99 从这个时刻起一直超过目标, 报过没有)
}

impl TargetUsage {
    /// 记下一次心跳：只记 `mine` 里的 target（集群图里属于发心跳的节点的）。转发失败的时长按报告节点自己的钟算（心跳里的 wall_ms − 起点）。
    fn record(&self, hb: &NodeHeartbeat, term: u64, mine: &[u32]) {
        let (mut m, at_ms) = (lock(&self.m), clock::mono_ms());
        for r in hb.targets.iter().filter(|r| mine.contains(&r.target)) {
            let stat = StoreStat { capacity: r.capacity, used: r.used };
            m.insert(r.target, Beat { at_ms, boot: hb.boot_id, term, stat, map_epoch_seen: hb.map_epoch_seen });
        }
        let f = |&(chain, down, since): &(u32, u32, u64)| FwdFailure {
            node: hb.node,
            chain,
            down,
            lasted_ms: hb.wall_ms.saturating_sub(since),
            heard_ms: at_ms,
        };
        lock(&self.fails).insert(hb.node, hb.forward_failures.iter().map(f).collect());
        let (per_qos, class_p99) = (hb.qos_deficit.clone(), hb.class_p99.clone());
        lock(&self.qos).insert(hb.node, QosBeat { heard_ms: at_ms, per_qos, class_p99 });
    }

    /// target `t` 最近一次心跳报的容量与已用（按用量放置与 rebalance 用，M13；没听到过 → None）。
    pub fn stat(&self, t: u32) -> Option<StoreStat> {
        lock(&self.m).get(&t).map(|b| b.stat)
    }

    /// 各节点 [`DEFICIT_FRESH_MS`] 之内的心跳报的保底欠额，按 qos_id 加起来（成本单位/秒）。
    fn deficits(&self, now_ms: u64) -> BTreeMap<u64, u64> {
        let mut out = BTreeMap::new();
        for d in lock(&self.qos).values().filter(|d| now_ms.saturating_sub(d.heard_ms) <= DEFICIT_FRESH_MS) {
            for (q, x) in &d.per_qos {
                *out.entry(*q).or_default() += *x as u64;
            }
        }
        out
    }

    /// 各节点 [`DEFICIT_FRESH_MS`] 之内的心跳报的、超过了目标的类 p99：(类, 节点) → p99（微秒）。`targets` = (类, latency_target 微秒)。
    fn over_target(&self, now_ms: u64, targets: &[(u16, u32)]) -> BTreeMap<(u16, u32), u64> {
        let mut out = BTreeMap::new();
        for (node, b) in lock(&self.qos).iter().filter(|(_, b)| now_ms.saturating_sub(b.heard_ms) <= DEFICIT_FRESH_MS) {
            for &(class, p99) in &b.class_p99 {
                if targets.iter().any(|&(k, t)| k == class && p99 > t) {
                    out.insert((class, *node), p99 as u64);
                }
            }
        }
        out
    }

    /// p99 已经连续 [`UNMET_AFTER_MS`] 超过目标、这一段还没报过的 ((类, 节点), 此刻的 p99)（cluster-mgr 写 PolicyViolation，见文件头）。
    pub fn slow(&self, now_ms: u64, targets: &[(u16, u32)]) -> Vec<((u16, u32), u64)> {
        episodes(&mut lock(&self.slow), self.over_target(now_ms, targets), now_ms)
    }

    /// `qos_id` 的保底欠额（各节点之和，3.10 的 qos 一行，M13）。
    pub fn deficit(&self, qos_id: u64, now_ms: u64) -> u64 {
        self.deficits(now_ms).get(&qos_id).copied().unwrap_or(0)
    }

    /// 欠额已经连续 [`UNMET_AFTER_MS`] 大于 0、这一段还没报过的 (qos_id, 此刻的欠额)（cluster-mgr 写 PolicyViolation，见文件头）。
    pub fn unmet(&self, now_ms: u64) -> Vec<(u64, u64)> {
        episodes(&mut lock(&self.unmet), self.deficits(now_ms), now_ms)
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

    /// 驱逐第二阶段的等待条件（3.4.4 第 2 步，`meta/evict.rs`）：target `t` 在本任期的心跳里报过 `map_epoch_seen ≥ epoch`，或已超过
    /// `lease_ms` 没有心跳——计时从本任期最近一次心跳与就绪时刻 `since_ms` 中较晚的那个算起（换了 leader：当作就绪那一刻刚听到过）。
    pub fn caught_up(&self, t: u32, term: u64, epoch: u64, now_ms: u64, since_ms: u64, lease_ms: u64) -> bool {
        let m = lock(&self.m);
        let b = m.get(&t).filter(|b| b.term == term);
        let quiet_since = b.map_or(0, |b| b.at_ms).max(since_ms); // 本任期最近一次心跳与就绪时刻中较晚的那个
        b.is_some_and(|b| b.map_epoch_seen >= epoch) || now_ms.saturating_sub(quiet_since) > lease_ms
    }

    /// 各节点最近一次心跳里报的持续转发失败。
    pub fn failures(&self) -> Vec<FwdFailure> {
        lock(&self.fails).values().flatten().copied().collect()
    }

    /// 本任期心跳里各 target 报的 `map_epoch_seen`（链管理器两步移出的第二步，ADR-0041 第 13 条）。
    pub fn epochs_seen(&self, term: u64) -> HashMap<u32, u64> {
        lock(&self.m).iter().filter(|(_, b)| b.term == term).map(|(t, b)| (*t, b.map_epoch_seen)).collect()
    }

    /// 链管理器记着要移出的 (链, target)（见文件头）：先只留 `keep` 的、再加上 `add`，返回现在的集合。
    pub fn parting(&self, keep: impl Fn(&(u32, u32)) -> bool, add: &[(u32, u32)]) -> BTreeSet<(u32, u32)> {
        let mut p = lock(&self.parting);
        p.retain(|k| keep(k));
        p.extend(add.iter().copied());
        p.clone()
    }
}

/// [`TargetUsage::unmet`] 与 [`TargetUsage::slow`] 的记账："一段"连续 [`UNMET_AFTER_MS`] 才报、每段只报一次。`seen` = 键（qos_id，或
/// (类, 节点)）→ (这一段从这个时刻起, 报过没有)，`now` = 此刻在"段"里的键与它的量（欠额、p99）。
fn episodes<K: Copy + Ord + Hash>(
    seen: &mut HashMap<K, (u64, bool)>,
    now: BTreeMap<K, u64>,
    now_ms: u64,
) -> Vec<(K, u64)> {
    seen.retain(|q, _| now.contains_key(q)); // 回到 0（回落到目标以下）了：这一段结束，下次再出现再报
    let mut out = vec![];
    for (q, d) in now {
        let (since, reported) = seen.entry(q).or_insert((now_ms, false));
        if !*reported && now_ms.saturating_sub(*since) >= UNMET_AFTER_MS {
            *reported = true;
            out.push((q, d));
        }
    }
    out
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

#[cfg(test)]
mod tests {
    use super::*;

    fn beat(node: u32, qos_deficit: Vec<(u64, u32)>) -> NodeHeartbeat {
        let (host_id, machine_id, targets, forward_failures, features) =
            (String::new(), String::new(), vec![], vec![], vec![]);
        let (boot_id, map_epoch_seen, wall_ms, class_p99) = (1, 0, 0, vec![]);
        NodeHeartbeat {
            node,
            boot_id,
            host_id,
            machine_id,
            map_epoch_seen,
            targets,
            forward_failures,
            features,
            wall_ms,
            qos_deficit,
            class_p99,
        }
    }

    #[test]
    fn deficits_are_summed_over_fresh_heartbeats() {
        let u = TargetUsage::default();
        u.record(&beat(1, vec![(7, 30)]), 1, &[]);
        u.record(&beat(2, vec![(7, 20), (8, 5)]), 1, &[]);
        let t = clock::mono_ms();
        assert_eq!((u.deficit(7, t), u.deficit(8, t), u.deficit(9, t)), (50, 5, 0), "各节点之和");
        assert_eq!(u.deficit(7, t + DEFICIT_FRESH_MS + 1), 0, "5 s 之前的心跳不算");
    }

    #[test]
    fn a_sustained_deficit_is_reported_once_per_episode() {
        let mut seen = HashMap::new();
        let d = |v: &[(u64, u64)]| v.iter().copied().collect::<BTreeMap<u64, u64>>();
        assert!(episodes(&mut seen, d(&[(7, 50), (8, 5)]), 1000).is_empty(), "刚出现：还没持续 10 s");
        assert!(episodes(&mut seen, d(&[(7, 50), (8, 5)]), 10_999).is_empty());
        assert_eq!(episodes(&mut seen, d(&[(7, 40), (8, 5)]), 11_000), vec![(7, 40), (8, 5)], "持续 10 s：各报一次");
        assert!(episodes(&mut seen, d(&[(7, 40)]), 30_000).is_empty(), "这一段只报一次；8 回到 0 了");
        assert!(episodes(&mut seen, d(&[(7, 1), (8, 2)]), 31_000).is_empty());
        assert_eq!(episodes(&mut seen, d(&[(8, 2)]), 41_000), vec![(8, 2)], "回到 0 之后再出现：再报");
    }

    /// ADR-0044 第 5 条：某个节点上某个类的 p99 超过了它的 latency_target 才算（5 s 之内的心跳）；连续 10 s 才报、每段只报一次、
    /// 回落之后再超再报（与保底欠额同一个 `episodes`，键是 (类, 节点)）。
    #[test]
    fn a_class_p99_over_its_target_is_reported_once_per_episode() {
        let u = TargetUsage::default();
        let p99 =
            |node: u32, v: Vec<(u16, u32)>| u.record(&NodeHeartbeat { class_p99: v, ..beat(node, vec![]) }, 1, &[]);
        p99(1, vec![(3, 900), (4, 50)]);
        p99(2, vec![(3, 400), (5, 9_000)]);
        let (t, targets) = (clock::mono_ms(), [(3, 500), (4, 100)]);
        let over: Vec<_> = u.over_target(t, &targets).into_iter().collect();
        assert_eq!(over, vec![((3, 1), 900)], "节点 2 的没超；类 4 没超；类 5 没设目标（不在 targets 里）");
        assert!(u.over_target(t + DEFICIT_FRESH_MS + 1, &targets).is_empty(), "5 s 之前的心跳不算");
        assert!(u.slow(t, &targets).is_empty(), "刚超：还没持续 10 s");
        let mut seen = HashMap::new();
        let d = |v: &[((u16, u32), u64)]| v.iter().copied().collect::<BTreeMap<(u16, u32), u64>>();
        assert!(episodes(&mut seen, d(&[((3, 1), 900)]), 1000).is_empty());
        assert_eq!(episodes(&mut seen, d(&[((3, 1), 990)]), 11_000), vec![((3, 1), 990)], "持续 10 s：报此刻的 p99");
        assert!(episodes(&mut seen, d(&[((3, 1), 990), ((3, 2), 600)]), 20_000).is_empty(), "这一段只报一次");
        assert!(episodes(&mut seen, d(&[((3, 2), 600)]), 21_000).is_empty(), "节点 1 回落了：它的这一段结束");
        let both = d(&[((3, 1), 800), ((3, 2), 600)]);
        assert!(episodes(&mut seen, both.clone(), 22_000).is_empty(), "节点 1 回落之后又超了：重新起算");
        assert_eq!(episodes(&mut seen, both, 30_000), vec![((3, 2), 600)], "节点 2 持续满 10 s：各节点分开算");
        assert_eq!(
            episodes(&mut seen, d(&[((3, 1), 800)]), 32_000),
            vec![((3, 1), 800)],
            "回落之后再超、又持续 10 s：再报"
        );
    }
}
