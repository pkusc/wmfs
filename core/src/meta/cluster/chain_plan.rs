//! 链管理器的决定（design.md 3.4.2.7、3.4.2.10）：纯函数——输入是已提交的集群图与 leader 内存里的心跳情况，输出是要写回的 target 与链
//! 记录（每条改过的链 `ver += 1`）以及要新建的链。cluster-mgr（`chain_mgr.rs`）每 200 ms 算一次，在一个 exclusive 批次里 CAS 写回。
//!
//! 按顺序：
//! 1. target：租约内没有心跳 → Offline；心跳带来新的 `boot_id` → 记下（与下面的降级同一个批次：集群图里出现新 boot_id 的同时它已不是
//!    Serving，所以重启过的成员在被确认之前一直自我隔离，I5、R6；第一次心跳——记录里是 0——直接记下，不算重启）；Offline 的恢复心跳 → Up。
//! 2. 每条链：Offline 的成员移出（记进 `removed`，最后一个 Serving 被移除时记 `last_serving`）；重启过的 Serving 成员：还有别的存活、
//!    最近 2 s 内以未变的 boot_id 心跳过的 Serving 成员 → 降为 Syncing 并移到末尾（I0）；没有（唯一幸存者、整链重启）→ 按链序最后一个
//!    存活的 Serving 成员保持 Serving、`incarnation += 1`、其余全部 Syncing；`last_serving` 回来了而链上没有 Serving 成员 → 它恢复为
//!    唯一 Serving、`incarnation += 1`（只有别的成员回来**不得**顶替）；`removed` 里离线 < `rejoin_window` 的 target 回来了（这一轮之前
//!    是 Offline，或重启过）、链长度不够 → 以 Syncing 追加；持续 10 s 转发不到后继 → 移除下游那一个（记进 `removed`：它一直在心跳，
//!    不算"回来"，按"修复"一行过了 `repair_delay` 才可能再补进来）；drain 的节点上的成员（与变成只读的成员）：其余 Serving 成员够了（`want`）
//!    才移除；长度不够（不算 drain 的）→ 追加一个合格的空闲 target（Syncing）：新 target 与 drain 的替代成员（链上还有要迁出的成员）
//!    马上补，别的要没有刚离线的成员（或已超过 `repair_delay`）。
//! 3. 新 target（Up、在租约内、不在任何链里、也不在任何 `removed` 里）→ 为已在用的每个 (存储类, 副本数) 生成以它为 head 的新链。
//!
//! 本任期就绪还不到一个 target 租约时（`Facts.grace`）只做第 1 步里来过心跳的部分、重启的处理、last_serving 回来与 rejoin；
//! 重启的处理要等有重启过的 Serving 成员的链都听全了（在那之前一个也不处理，新 boot_id 也先不写：重启过的成员一直自我隔离）。
//!
//! `want` = min(声明副本数, 能用的故障域数)：故障域 = 节点；能用 = 有 Up、在租约内、不是只读、标签匹配、不在 drain 的节点上的 target。

use std::collections::{BTreeMap, BTreeSet, HashMap};

use super::chains::{SERVING, SYNCING, TARGET_OFFLINE, TARGET_UP, candidates, plan as ring};
use crate::meta::schema::records::{ChainRecV1, ClusterSettingsV1, NODE_ACTIVE, StorageClassV1, TargetRecV1};
use crate::proto::peer::ClusterMap;

/// "持续 10 s 无法把转发送达后继"（3.4.2.10）。
pub const FWD_FAIL_MS: u64 = 10_000;
/// "最近 2 s 内以未变的 boot_id 心跳过"（3.4.2.7）；转发失败的报告也只认这么新的（报告者还活着）。
pub const FRESH_MS: u64 = 2000;

/// leader 内存里一个 target 的心跳情况（只含本任期内、租约内来过心跳的）。
#[derive(Clone, Copy, Debug)]
pub struct Seen {
    pub boot: u64,
    pub fresh: bool, // 最近 FRESH_MS 内来过
}

/// 一个节点上报的"转发不到后继"（3.4.2.10）。时长全程只用同一台机器的钟：报告节点按自己的墙钟算到发心跳时已持续多久，leader 再
/// 加上自己收到之后过去的时间（mono_ms），两台机器的墙钟差不进来。
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FwdFailure {
    pub node: u32, // 报告的节点
    pub chain: u32,
    pub down: u32,      // 它的后继（下游 target）
    pub lasted_ms: u64, // 已持续多久（chain_mgr 交给 plan 时已算到"现在"）
    pub heard_ms: u64,  // leader 收到这次心跳的时刻（leader 的 mono_ms）
}

/// 决定要用的一切（除了集群图）。`grace` = 本任期就绪还不到一个 target 租约（3.4.1.10 的计时重置）：没来心跳不算离线、不修、不迁、
/// 不建新链；有重启过的 Serving 成员的链都听全了才处理重启（知道了谁活着，唯一幸存者与整链重启的判断不用再等）——只处理一部分的话，
/// 先写下的新 boot_id 会让那个成员在下一轮看起来"没重启过"，整链重启就不会加 incarnation（C-8）。
pub struct Facts {
    pub now_ms: u64, // 墙钟（removed 的时间戳）
    pub settings: ClusterSettingsV1,
    pub alive: HashMap<u32, Seen>,
    pub failures: Vec<FwdFailure>, // 只含报告者最近还在心跳的（chain_mgr 过滤过）
    pub grace: bool,
}

/// 要写回的东西。
#[derive(Default, Debug, PartialEq)]
pub struct Plan {
    pub targets: Vec<(u32, TargetRecV1)>,
    pub chains: Vec<(u32, ChainRecV1)>,
    pub new_chains: Vec<ChainRecV1>,
}

impl Plan {
    pub fn is_empty(&self) -> bool {
        self.targets.is_empty() && self.chains.is_empty() && self.new_chains.is_empty()
    }
}

/// 把 `t` 移出链（记进 `removed`；它是最后一个 Serving → `last_serving = t`）。
pub fn remove(c: &mut ChainRecV1, t: u32, now_ms: u64, record: bool) {
    let was_last = c.members.iter().filter(|(_, s)| *s == SERVING).map(|(x, _)| *x).collect::<Vec<_>>() == [t];
    c.members.retain(|(x, _)| *x != t);
    if was_last {
        c.last_serving = Some(t);
    }
    if record {
        c.removed.retain(|(x, _)| *x != t);
        c.removed.push((t, now_ms));
    }
}

/// 降为 Syncing 并移到末尾（I0）。
pub fn demote(c: &mut ChainRecV1, t: u32) {
    c.members.retain(|(x, _)| *x != t);
    c.members.push((t, SYNCING));
}

/// `t` 成为唯一 Serving（唯一幸存者、整链重启、last_serving 回来、force-serving）：`incarnation += 1`，其余全部 Syncing、排在它之后。
pub fn sole(c: &mut ChainRecV1, t: u32) {
    let rest = c.members.iter().filter(|(x, _)| *x != t).map(|(x, _)| (*x, SYNCING));
    c.members = std::iter::once((t, SERVING)).chain(rest).collect();
    c.incarnation += 1;
    c.last_serving = None;
    c.removed.retain(|(x, _)| *x != t);
}

/// `SyncDone`：`t` 是紧跟最后一个 Serving 的第一个 Syncing 成员、链版本没变 → 变 Serving（3.4.2.8 第 4 步）。
pub fn promote(c: &mut ChainRecV1, t: u32, chain_ver: u64) -> bool {
    let first_syncing = c.members.iter().position(|(_, s)| *s != SERVING);
    let ok = c.ver == chain_ver && first_syncing.is_some_and(|i| c.members[i].0 == t);
    if let Some(i) = first_syncing.filter(|_| ok) {
        c.members[i].1 = SERVING;
        c.ver += 1;
    }
    ok
}

fn node_of(m: &ClusterMap, t: u32) -> Option<u32> {
    m.targets.iter().find(|(id, _)| *id == t).map(|(_, r)| r.node)
}

/// 节点在 drain（或已被移除）：它上面的 target 要迁出链。
fn leaving(m: &ClusterMap, node: Option<u32>) -> bool {
    node.and_then(|n| m.nodes.iter().find(|(id, _)| *id == n)).is_some_and(|(_, r)| r.state != NODE_ACTIVE)
}

/// 链成员 `t` 要迁出写路径：它的节点在 drain，或它变成了只读（3.2.3 的 ro_compat 位：已有链里的成员变成只读，3.4.2.10 的 M5 注记）。
fn target_leaving(m: &ClusterMap, t: u32) -> bool {
    leaving(m, node_of(m, t)) || m.targets.iter().any(|(id, r)| *id == t && r.read_only)
}

/// 链所属存储类能用的 (target, node)：Up、在租约内、不在 drain 的节点上（见文件头）。
fn usable(m: &ClusterMap, f: &Facts, class: Option<&StorageClassV1>) -> Vec<(u32, u32)> {
    let Some(class) = class else { return vec![] };
    let ok = |(t, n): &(u32, u32)| f.alive.contains_key(t) && !leaving(m, Some(*n));
    candidates(m, class).into_iter().filter(ok).collect()
}

/// 算一轮（见文件头）。
pub fn plan(m: &ClusterMap, f: &Facts) -> Plan {
    let mut targets: BTreeMap<u32, TargetRecV1> = m.targets.iter().cloned().collect();
    // 第一次心跳（记录里的 boot_id 是 0：从没启动过，被确认之前也从没服务过读写）直接记下，不算重启
    let mut rebooted: BTreeSet<u32> = targets
        .iter()
        .filter(|(id, t)| t.boot_id != 0 && f.alive.get(id).is_some_and(|s| s.boot != t.boot_id))
        .map(|(id, _)| *id)
        .collect();
    let unheard = |c: &ChainRecV1| !c.members.iter().all(|(t, _)| f.alive.contains_key(t));
    let serves_rebooted = |c: &ChainRecV1| c.members.iter().any(|(t, s)| *s == SERVING && rebooted.contains(t));
    if f.grace && m.chains.iter().any(|(_, c)| unheard(c) && serves_rebooted(c)) {
        rebooted.clear(); // 还没听全：重启一律先不处理，新 boot_id 也先不写（重启过的成员在被处理之前一直自我隔离，I5）
    }
    for (id, t) in targets.iter_mut() {
        match f.alive.get(id) {
            None if t.state == TARGET_UP && !f.grace => t.state = TARGET_OFFLINE,
            Some(s) => {
                if t.boot_id == 0 || rebooted.contains(id) {
                    t.boot_id = s.boot;
                }
                if t.state == TARGET_OFFLINE {
                    (t.state, t.last_up_ms) = (TARGET_UP, f.now_ms);
                }
            }
            None => {}
        }
    }
    let offline = |t: &u32| targets.get(t).is_none_or(|r| r.state == TARGET_OFFLINE);
    let mut out = Plan::default();
    for (cid, c0) in &m.chains {
        let mut c = c0.clone();
        let class = m.classes.iter().find(|k| k.id == c.class_id);
        let cands = usable(m, f, class);
        let want = (c.replicas as usize).min(cands.iter().map(|(_, n)| *n).collect::<BTreeSet<_>>().len());
        for t in c0.members.iter().map(|(t, _)| *t).filter(|t| offline(t)) {
            remove(&mut c, t, f.now_ms, true); // 记进 removed
        }
        reboots(&mut c, &rebooted, f);
        if let Some(ls) = c.last_serving.filter(|ls| f.alive.contains_key(ls))
            && c.members.iter().all(|(_, s)| *s != SERVING)
        {
            sole(&mut c, ls); // 最后的副本回来了：它有最新的已提交数据
        }
        let window = f.settings.rejoin_window_ms;
        // 回来了 = 这一轮之前是 Offline（或重启过）、现在有心跳。因转发失败被移除的一直在心跳，不算（否则 200 ms 后又被追加回来）
        let returned =
            |t: &u32| rebooted.contains(t) || m.targets.iter().any(|(i, r)| i == t && r.state == TARGET_OFFLINE);
        let back: Vec<u32> = c
            .removed
            .iter()
            .filter(|(t, ms)| f.alive.contains_key(t) && f.now_ms < ms + window && returned(t))
            .map(|(t, _)| *t)
            .collect();
        for t in back {
            let node = node_of(m, t);
            let taken = c.members.iter().any(|(x, _)| node_of(m, *x) == node);
            if c.members.len() < c.replicas as usize && !taken && !leaving(m, node) {
                c.members.push((t, SYNCING));
                c.removed.retain(|(x, _)| *x != t);
            }
        }
        c.removed.retain(|(_, ms)| f.now_ms < ms + window);
        for x in f.failures.iter().filter(|x| !f.grace && x.chain == *cid && x.lasted_ms >= FWD_FAIL_MS) {
            let pos = c.members.iter().position(|(t, _)| node_of(m, *t) == Some(x.node));
            if pos.and_then(|p| c.members.get(p + 1)).map(|(t, _)| *t) == Some(x.down) {
                remove(&mut c, x.down, f.now_ms, true); // 成员之间分区：保住更靠前、数据更新的那个；记进 removed
            }
        }
        if !f.grace {
            drain_and_repair(m, f, &mut c, &cands, want);
        }
        if c != *c0 {
            c.ver = c0.ver + 1;
            out.chains.push((*cid, c));
        }
    }
    out.new_chains = if f.grace { vec![] } else { new_heads(m, f) };
    out.targets = targets.into_iter().filter(|(id, t)| m.targets.iter().any(|(i, o)| i == id && o != t)).collect();
    out
}

/// 3.4.2.7：重启过的 Serving 成员。
fn reboots(c: &mut ChainRecV1, rebooted: &BTreeSet<u32>, f: &Facts) {
    let changed: Vec<u32> =
        c.members.iter().filter(|(t, s)| *s == SERVING && rebooted.contains(t)).map(|(t, _)| *t).collect();
    if changed.is_empty() {
        return;
    }
    let stable = c
        .members
        .iter()
        .any(|(t, s)| *s == SERVING && !rebooted.contains(t) && f.alive.get(t).is_some_and(|x| x.fresh));
    if stable {
        for t in changed {
            demote(c, t);
        }
    } else if let Some((s, _)) = c.members.iter().rev().find(|(t, s)| *s == SERVING && f.alive.contains_key(t)) {
        let s = *s;
        sole(c, s); // 唯一幸存者 / 整链重启：按链序最后一个存活的原 Serving 成员（已提交版本最全，I2）
    }
}

/// drain 的节点上的成员：其余 Serving 成员够了才移除；不够（不算 drain 的）且可以修 → 追加一个空闲 target（Syncing）。
fn drain_and_repair(m: &ClusterMap, f: &Facts, c: &mut ChainRecV1, cands: &[(u32, u32)], want: usize) {
    let staying = |c: &ChainRecV1, serving_only: bool| {
        c.members.iter().filter(|(t, s)| !target_leaving(m, *t) && (!serving_only || *s == SERVING)).count()
    };
    let gone: Vec<u32> = c.members.iter().map(|(t, _)| *t).filter(|t| target_leaving(m, *t)).collect();
    for t in gone {
        if staying(c, true) >= want.max(1) {
            remove(c, t, f.now_ms, false); // 迁走的不记进 removed（不会"回来"）
        }
    }
    let delay = f.settings.repair_delay_ms;
    let recently = c.removed.iter().any(|(_, ms)| f.now_ms < ms + delay); // 刚离线的成员可能很快回来：先别拿空闲 target 顶上
    // 还有要迁出的成员（drain、变成只读）：替代成员马上补——drain 一行"先为相关链追加替代成员"不等 repair_delay（M9 验证发现：否则
    // 唯一能替代的是 removed 里回来时链已被新 target 补满、没能 rejoin 的 target 时，drain 要等 30 min）
    let draining = c.members.iter().any(|(t, _)| target_leaving(m, *t));
    if staying(c, false) < want {
        let used: BTreeSet<Option<u32>> = c.members.iter().map(|(t, _)| node_of(m, *t)).collect();
        // 新加入的 target（还不在任何链里）马上补；别的空闲 target 要等过了 repair_delay（3.4.2.10 的最后一行与"修复"一行）
        let ok = |(t, n): &&(u32, u32)| !used.contains(&Some(*n)) && (draining || !recently || !placed(m, *t));
        if let Some((t, _)) = cands.iter().find(ok) {
            c.members.push((*t, SYNCING));
            c.removed.retain(|(x, _)| x != t); // 它回到链上了
        }
    }
}

/// `t` 在某条链里（成员，或最近被移除的）。
fn placed(m: &ClusterMap, t: u32) -> bool {
    m.chains.iter().any(|(_, c)| c.members.iter().any(|(x, _)| *x == t) || c.removed.iter().any(|(x, _)| *x == t))
}

/// 新 target：为已在用的每个 (存储类, 副本数) 生成以它为 head 的新链（成员全部 Serving：新链里还没有数据）。
fn new_heads(m: &ClusterMap, f: &Facts) -> Vec<ChainRecV1> {
    let groups: BTreeSet<(u16, u8)> = m.chains.iter().map(|(_, c)| (c.class_id, c.replicas)).collect();
    let mut out = Vec::new();
    for (class_id, replicas) in groups {
        let class = m.classes.iter().find(|k| k.id == class_id);
        let cands = usable(m, f, class);
        for (i, members) in ring(&cands, replicas).into_iter().enumerate() {
            if !placed(m, cands[i].0) {
                let members = members.iter().map(|(t, _)| (*t, SERVING)).collect();
                let (last_serving, removed) = (None, vec![]);
                out.push(ChainRecV1 { ver: 1, incarnation: 1, class_id, replicas, members, last_serving, removed });
            }
        }
    }
    out
}

#[cfg(test)]
mod tests;
