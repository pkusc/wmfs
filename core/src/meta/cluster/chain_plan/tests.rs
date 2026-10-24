//! 3.4.2.7 与 3.4.2.10 的每一行（9.1 的"CRAQ 状态机"：3.4.2.7 的每一行），以及随机输入下 I0 一直成立。

use super::*;
use crate::base::rand::XorShift64;
use crate::init::default_class;
use crate::meta::cluster::chains::{SERVING, want};
use crate::meta::schema::records::{NODE_DRAINING, NodeRecV1};

const NOW: u64 = 10_000_000;

fn target(node: u32, boot: u64) -> TargetRecV1 {
    let (path, labels, phys_devs) = (String::new(), vec![], vec![]);
    TargetRecV1 {
        node,
        path,
        labels,
        phys_devs,
        capacity: 0,
        boot_id: boot,
        state: TARGET_UP,
        last_up_ms: 0,
        read_only: false,
    }
}

fn chain(members: &[(u32, u8)]) -> ChainRecV1 {
    let (last_serving, removed) = (None, vec![]);
    ChainRecV1 { ver: 5, incarnation: 1, class_id: 1, replicas: 3, members: members.to_vec(), last_serving, removed }
}

/// n 个节点、每个节点一个 target（target id = 节点号，boot_id = 100 + id），给定的链。
fn map(n: u32, chains: Vec<ChainRecV1>) -> ClusterMap {
    let node = |id| {
        let (addrs, mgmt_addr, host_id, machine_id, hostname) =
            (vec![], String::new(), String::new(), String::new(), String::new());
        let rec = NodeRecV1 {
            addrs,
            mgmt_addr,
            host_id,
            machine_id,
            hostname,
            meta_role: 2,
            features: vec![],
            joined_ms: 0,
            state: NODE_ACTIVE,
        };
        (id, rec)
    };
    ClusterMap {
        nodes: (1..=n).map(node).collect(),
        targets: (1..=n).map(|id| (id, target(id, 100 + id as u64))).collect(),
        chains: chains.into_iter().zip(1u32..).map(|(c, id)| (id, c)).collect(),
        classes: vec![default_class(3)],
        ..Default::default()
    }
}

/// 活着的 target：(id, boot_id)，都在最近 2 s 内来过。
fn facts(alive: &[(u32, u64)]) -> Facts {
    let alive = alive.iter().map(|(t, b)| (*t, Seen { boot: *b, fresh: true })).collect();
    Facts { now_ms: NOW, settings: ClusterSettingsV1::default(), alive, failures: vec![], grace: false }
}

const S: u8 = SERVING;
const Y: u8 = SYNCING;

#[test]
fn a_target_without_heartbeats_goes_offline_and_leaves_its_chains() {
    let m = map(3, vec![chain(&[(1, S), (2, S), (3, S)]), chain(&[(2, S)])]);
    let p = plan(&m, &facts(&[(1, 101), (3, 103)]));
    assert_eq!(p.targets.len(), 1);
    assert_eq!((p.targets[0].0, p.targets[0].1.state), (2, TARGET_OFFLINE));
    let (c1, c2) = (&p.chains[0].1, &p.chains[1].1);
    assert_eq!(c1.members, vec![(1, S), (3, S)], "Serving 成员被移除时其余保持顺序（I0）");
    assert_eq!((c1.ver, c1.removed.clone(), c1.last_serving), (6, vec![(2, NOW)], None));
    assert!(c2.members.is_empty() && c2.last_serving == Some(2), "最后一个 Serving 成员被移除：记 last_serving");
}

#[test]
fn a_restarted_member_with_live_peers_becomes_syncing_at_the_end() {
    let m = map(3, vec![chain(&[(1, S), (2, S), (3, S)])]);
    let p = plan(&m, &facts(&[(1, 101), (2, 999), (3, 103)]));
    assert_eq!(p.targets[0].1.boot_id, 999, "新 boot_id 与降级在同一个批次里写");
    let c = &p.chains[0].1;
    assert_eq!(c.members, vec![(1, S), (3, S), (2, Y)], "降为 Syncing 并移到末尾（I0、C-4）");
    assert_eq!((c.ver, c.incarnation), (6, 1), "还有存活的 Serving 成员：incarnation 不变");
}

/// 第一次心跳（记录里的 boot_id 是 0：join 或 init 之后从没启动过）不是重启：直接记下，链不动（新链的成员不必重新同步）。
#[test]
fn a_first_heartbeat_is_not_a_reboot() {
    let mut m = map(3, vec![chain(&[(1, S), (2, S), (3, S)])]);
    m.targets[1].1.boot_id = 0;
    for grace in [false, true] {
        let mut f = facts(&[(1, 101), (2, 555), (3, 103)]);
        f.grace = grace;
        let p = plan(&m, &f);
        assert!(p.chains.is_empty(), "grace = {grace}：链不动 {p:?}");
        assert_eq!(
            p.targets.iter().map(|(id, t)| (*id, t.boot_id)).collect::<Vec<_>>(),
            vec![(2, 555)],
            "记下 boot_id"
        );
    }
}

#[test]
fn a_sole_survivor_stays_serving_with_a_new_incarnation() {
    let m = map(1, vec![ChainRecV1 { replicas: 1, ..chain(&[(1, S)]) }]);
    let c = &plan(&m, &facts(&[(1, 999)])).chains[0].1;
    assert_eq!((c.members.clone(), c.incarnation, c.ver), (vec![(1, S)], 2, 6), "唯一幸存者：incarnation += 1（C-8）");
}

#[test]
fn a_whole_chain_restart_keeps_the_last_original_serving_member() {
    let m = map(3, vec![chain(&[(1, S), (2, S), (3, S)])]);
    let c = &plan(&m, &facts(&[(1, 991), (2, 992), (3, 993)])).chains[0].1;
    assert_eq!(c.members, vec![(3, S), (1, Y), (2, Y)], "按链序最后一个（已提交版本最全，I2）");
    assert_eq!(c.incarnation, 2);
    // 最后一个原 Serving 成员没回来：选它前面那个
    let c = &plan(&m, &facts(&[(1, 991), (2, 992)])).chains[0].1;
    assert_eq!(c.members, vec![(2, S), (1, Y)]);
}

#[test]
fn only_the_last_serving_member_may_bring_an_unavailable_chain_back() {
    let mut c0 = chain(&[]);
    (c0.last_serving, c0.removed) = (Some(2), vec![(1, NOW - 10), (2, NOW - 5)]);
    let m = map(3, vec![c0]);
    let mut m = m;
    m.targets[0].1.state = TARGET_OFFLINE;
    m.targets[1].1.state = TARGET_OFFLINE;
    let c = &plan(&m, &facts(&[(1, 991)])).chains[0].1;
    assert_eq!(c.members, vec![(1, Y)], "只有别的成员回来：以 Syncing 追加，不得顶替（链保持不可用）");
    assert_eq!((c.last_serving, c.incarnation), (Some(2), 1));
    let c = &plan(&m, &facts(&[(1, 991), (2, 992)])).chains[0].1;
    assert_eq!(c.members[0], (2, S), "last_serving 回来：它恢复为 Serving");
    assert!(c.members.contains(&(1, Y)));
    assert_eq!((c.last_serving, c.incarnation), (None, 2));
}

#[test]
fn removed_members_rejoin_within_the_window_only() {
    let mut c0 = chain(&[(1, S), (3, S)]);
    c0.removed = vec![(2, NOW - 1000)];
    let mut m = map(3, vec![c0.clone()]);
    m.targets[1].1.state = TARGET_OFFLINE;
    let p = plan(&m, &facts(&[(1, 101), (2, 555), (3, 103)]));
    assert_eq!(p.chains[0].1.members, vec![(1, S), (3, S), (2, Y)], "rejoin_window 内回来：以 Syncing 追加");
    assert!(p.chains[0].1.removed.is_empty());
    assert_eq!(p.targets[0].1.state, TARGET_UP);
    c0.removed = vec![(2, NOW - ClusterSettingsV1::default().rejoin_window_ms - 1)];
    let m = map(3, vec![c0]);
    let p = plan(&m, &facts(&[(1, 101), (3, 103)]));
    let c = &p.chains[0].1;
    assert!(c.removed.is_empty() && c.members.len() == 2, "离线超过 rejoin_window：不再放回旧链，从 removed 清除");
}

fn fwd_fail(node: u32, down: u32, lasted_ms: u64) -> FwdFailure {
    FwdFailure { node, chain: 1, down, lasted_ms, heard_ms: 0 }
}

#[test]
fn a_member_that_cannot_reach_its_successor_for_10s_loses_the_successor() {
    let m = map(3, vec![chain(&[(1, S), (2, S), (3, S)])]);
    let mut f = facts(&[(1, 101), (2, 102), (3, 103)]);
    f.failures = vec![fwd_fail(1, 2, FWD_FAIL_MS - 1)];
    assert!(plan(&m, &f).is_empty(), "还不到 10 s");
    f.failures = vec![fwd_fail(1, 2, FWD_FAIL_MS)];
    assert_eq!(plan(&m, &f).chains[0].1.members, vec![(1, S), (3, S)], "移除下游那一个");
    f.failures = vec![fwd_fail(1, 3, FWD_FAIL_MS)];
    assert!(plan(&m, &f).is_empty(), "报的下游不是它的后继：不动");
}

/// 审查发现：因转发失败被移除的成员一直在心跳（从没 Offline），下一轮（200 ms 后）不能被当作"离线后回来"追加回链尾——否则成员之间
/// 持续分区时链会循环"追加 → 10 s 转发不到 → 移除"，写一直卡住。它留在 removed 里，过了 repair_delay 才可能按"修复"补回来。
#[test]
fn a_member_removed_for_forward_failures_is_not_readded_at_once() {
    let m = map(3, vec![chain(&[(1, S), (2, S), (3, S)])]);
    let mut f = facts(&[(1, 101), (2, 102), (3, 103)]);
    f.failures = vec![fwd_fail(1, 2, FWD_FAIL_MS)];
    let p = plan(&m, &f);
    assert_eq!(p.chains[0].1.members, vec![(1, S), (3, S)]);
    let mut m2 = m.clone();
    m2.chains = p.chains.clone(); // 第一轮的结果已提交
    f.failures = vec![]; // 链改过之后 1.5 s 内的报告不算（chain_mgr）
    f.now_ms = NOW + 200;
    let p2 = plan(&m2, &f);
    assert!(p2.is_empty(), "第二轮：不追加回来：{p2:?}");
    f.now_ms = NOW + ClusterSettingsV1::default().repair_delay_ms + 200;
    let c = &plan(&m2, &f).chains[0].1;
    assert_eq!(c.members, vec![(1, S), (3, S), (2, Y)], "过了 repair_delay：按修复补回（Syncing）");
}

#[test]
fn draining_moves_targets_out_after_replacements_serve() {
    let mut m = map(3, vec![chain(&[(1, S), (2, S), (3, S)])]);
    m.nodes[2].1.state = NODE_DRAINING;
    let all = facts(&[(1, 101), (2, 102), (3, 103)]);
    let c = &plan(&m, &all).chains[0].1;
    assert_eq!(c.members, vec![(1, S), (2, S)], "3 个节点：没有替代的，其余两个够了就移除");
    assert_eq!(want(&m, c), 2, "管理 API 的期望长度与链管理器同一口径（drain 中的节点不算）");
    let mut m4 = map(4, vec![chain(&[(1, S), (2, S), (3, S)])]);
    m4.nodes[2].1.state = NODE_DRAINING;
    let f4 = facts(&[(1, 101), (2, 102), (3, 103), (4, 104)]);
    let c = &plan(&m4, &f4).chains[0].1;
    assert_eq!(c.members, vec![(1, S), (2, S), (3, S), (4, Y)], "先追加替代成员（Syncing）");
    m4.chains[0].1 = ChainRecV1 { members: vec![(1, S), (2, S), (3, S), (4, S)], ..m4.chains[0].1.clone() };
    assert_eq!(plan(&m4, &f4).chains[0].1.members, vec![(1, S), (2, S), (4, S)], "替代成员 Serving 之后再移除");
}

/// M9 验证发现：t2 离线后回来时链已被新加入的 t4 补满（没能 rejoin，留在 removed 里）；之后 drain t4，能替代的只有 t2——drain 的替代
/// 成员不等 repair_delay（3.4.2.10 的 drain 一行），否则 t4 要 30 min 才迁得走。不在 drain 时短链照样等（见下面"修复"的用例）。
#[test]
fn a_drain_replacement_does_not_wait_for_the_repair_delay() {
    let mut c0 = chain(&[(3, S), (1, S), (4, S)]);
    c0.removed = vec![(2, NOW - 1000)];
    let mut m = map(4, vec![c0.clone(), ChainRecV1 { replicas: 1, ..chain(&[(2, S)]) }]);
    m.nodes[3].1.state = NODE_DRAINING;
    let f = facts(&[(1, 101), (2, 102), (3, 103), (4, 104)]);
    let p = plan(&m, &f);
    assert_eq!(p.chains.len(), 1, "{p:?}");
    let c = &p.chains[0].1;
    assert_eq!(c.members, vec![(3, S), (1, S), (4, S), (2, Y)], "先追加替代成员（Syncing），不等 repair_delay");
    assert!(c.removed.is_empty(), "t2 回到链上了：不再记在 removed 里");
    m.chains[0].1.members = vec![(3, S), (1, S), (4, S), (2, S)];
    assert_eq!(plan(&m, &f).chains[0].1.members, vec![(3, S), (1, S), (2, S)], "替代成员 Serving 之后再移除 t4");
}

/// 已有链里的成员变成只读（3.2.3 的 ro_compat 位）：像 drain 一样迁出写路径——先补一个成员，够了再移除它。
#[test]
fn read_only_members_leave_the_write_path_like_drained_ones() {
    let mut m = map(4, vec![chain(&[(1, S), (2, S), (3, S)])]);
    m.targets[2].1.read_only = true;
    let f = facts(&[(1, 101), (2, 102), (3, 103), (4, 104)]);
    assert_eq!(plan(&m, &f).chains[0].1.members, vec![(1, S), (2, S), (3, S), (4, Y)]);
    m.chains[0].1.members = vec![(1, S), (2, S), (3, S), (4, S)];
    assert_eq!(plan(&m, &f).chains[0].1.members, vec![(1, S), (2, S), (4, S)]);
}

#[test]
fn short_chains_are_repaired_with_spare_targets_after_the_delay() {
    let mut c0 = chain(&[(1, S), (2, S)]);
    let m = map(3, vec![c0.clone()]);
    let f = facts(&[(1, 101), (2, 102), (3, 103)]);
    assert_eq!(plan(&m, &f).chains[0].1.members, vec![(1, S), (2, S), (3, Y)], "建的时候就短（没有刚离线的）：马上补");
    c0.removed = vec![(9, NOW - 1000)];
    let other = ChainRecV1 { replicas: 1, ..chain(&[(3, S)]) }; // t3 已在别的链里：不是新加入的 target
    let p = plan(&map(3, vec![c0.clone(), other.clone()]), &f);
    assert!(p.chains.is_empty(), "刚有成员离线：repair_delay 内先不拿空闲 target 补：{p:?}");
    let p = plan(&map(3, vec![c0.clone()]), &f);
    assert_eq!(p.chains[0].1.members, vec![(1, S), (2, S), (3, Y)], "新加入的 target（不在任何链里）马上补");
    c0.removed = vec![(9, NOW - ClusterSettingsV1::default().repair_delay_ms)];
    let c = &plan(&map(3, vec![c0, other]), &f).chains[0].1;
    assert!(c.members.contains(&(3, Y)), "超过 repair_delay：补上");
}

#[test]
fn a_new_target_gets_head_chains_for_every_group_in_use() {
    let m = map(
        4,
        vec![chain(&[(1, S), (2, S), (3, S)]), chain(&[(2, S), (3, S), (1, S)]), chain(&[(3, S), (1, S), (2, S)])],
    );
    let p = plan(&m, &facts(&[(1, 101), (2, 102), (3, 103), (4, 104)]));
    assert_eq!(p.new_chains.len(), 1);
    let c = &p.new_chains[0];
    assert_eq!(
        (c.members[0], c.members.len(), c.ver, c.incarnation),
        ((4, S), 3, 1, 1),
        "以它为 head、成员全部 Serving"
    );
    assert!(
        plan(&m, &facts(&[(1, 101), (2, 102), (3, 103)])).new_chains.is_empty(),
        "不在租约内的 target 不算新 target"
    );
}

/// 就绪还不到一个租约（计时重置，3.4.1.10）：没来心跳不算离线；有重启过的 Serving 成员的链都听全了才处理重启，在那之前新 boot_id 也
/// 先不写——只写下 t1 的 boot_id 的话，下一轮 t1 看起来"没重启过"，整链重启就成了"t1 稳定、降级别的"，不加 incarnation（C-8）。
#[test]
fn during_the_grace_period_reboots_wait_until_every_chain_with_them_is_heard() {
    let m = map(3, vec![chain(&[(1, S), (2, S), (3, S)]), ChainRecV1 { replicas: 1, ..chain(&[(1, S)]) }]);
    for alive in [&[(1, 991)][..], &[(1, 991), (2, 102)]] {
        let mut f = facts(alive);
        f.grace = true;
        let p = plan(&m, &f);
        assert!(p.is_empty(), "t1 所在的三个成员的链还没听全：一个也不处理、boot_id 也不写、没来心跳不算离线：{p:?}");
    }
    let mut f = facts(&[(1, 991), (2, 992), (3, 993)]);
    f.grace = true;
    let p = plan(&m, &f);
    assert_eq!(p.chains[0].1.members[0], (3, S), "都来过心跳了：整链重启照样选最后一个");
    assert_eq!((p.chains[1].1.incarnation, p.targets.len()), (2, 3), "新 boot_id 与决定在同一个批次里");
    let one = map(1, vec![ChainRecV1 { replicas: 1, ..chain(&[(1, S)]) }]);
    let mut f = facts(&[(1, 991)]);
    f.grace = true;
    assert_eq!(plan(&one, &f).chains[0].1.incarnation, 2, "单节点重启：唯一幸存者不用干等 8 s");
}

#[test]
fn sync_done_promotes_only_the_first_syncing_member_of_the_same_version() {
    let mut c = chain(&[(1, S), (2, Y), (3, Y)]);
    assert!(!promote(&mut c, 3, 5), "不是紧跟最后一个 Serving 的那个");
    assert!(!promote(&mut c, 2, 4), "链版本变了：重跑同步");
    assert!(promote(&mut c, 2, 5));
    assert_eq!((c.members.clone(), c.ver), (vec![(1, S), (2, S), (3, Y)], 6));
}

/// 随机的心跳情况下：I0（Syncing 成员都在 Serving 之后）一直成立，改过的链 ver 只加一，链上没有重复的 target 与节点。
#[test]
fn random_plans_keep_invariant_i0() {
    for seed in 1..300u64 {
        let mut r = XorShift64::new(seed);
        let n = 3 + r.below(3) as u32;
        let mut members: Vec<(u32, u8)> = (1..=n).filter(|_| r.below(4) > 0).map(|t| (t, S)).collect();
        let syncing = r.below(members.len() as u64 + 1) as usize;
        for m in members.iter_mut().rev().take(syncing) {
            m.1 = Y;
        }
        let m = map(n, vec![chain(&members)]);
        let mut alive = Vec::new();
        for t in 1..=n {
            if r.below(3) > 0 {
                alive.push((t, if r.below(3) == 0 { 900 + t as u64 } else { 100 + t as u64 }));
            }
        }
        let p = plan(&m, &facts(&alive));
        for (_, c) in &p.chains {
            let first_syncing = c.members.iter().position(|(_, s)| *s == Y).unwrap_or(c.members.len());
            assert!(c.members[first_syncing..].iter().all(|(_, s)| *s == Y), "种子 {seed}：I0 不成立 {c:?}");
            assert_eq!(c.ver, 6, "种子 {seed}");
            let ids: BTreeSet<u32> = c.members.iter().map(|(t, _)| *t).collect();
            assert_eq!(ids.len(), c.members.len(), "种子 {seed}：重复的成员");
        }
    }
}
