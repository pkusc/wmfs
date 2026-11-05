//! 3.4.2.7 与 3.4.2.10 的每一行（9.1 的"CRAQ 状态机"：3.4.2.7 的每一行），以及随机输入下 I0 一直成立。活着的成员分两步移出
//! （drain、只读、10 s 转发不到，ADR-0041 第 13、16 条）的用例在 `tests/two_step.rs`。

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
    Facts {
        now_ms: NOW,
        settings: ClusterSettingsV1::default(),
        alive,
        failures: vec![],
        grace: false,
        target_lease_shrinking: false,
        syncing: HashMap::new(),
        epoch_seen: HashMap::new(),
        parting: BTreeSet::new(),
    }
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

/// 随机的心跳情况（另有随机的 drain、10 s 转发失败、已可做第二步的 Syncing 成员、调小租约的过渡期）下：I0（Syncing 成员都在 Serving
/// 之后）一直成立，改过的链 ver 只加一，链上没有重复的 target 与节点；活着的、已提交的图里是 Serving 的成员不会在一轮里被移出（两步，
/// ADR-0041 第 13 条），原来有活着的 Serving 成员的链之后也还有 Serving 成员。
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
        let mut m = map(n, vec![chain(&members)]);
        if r.below(3) == 0 {
            m.nodes[r.below(n as u64) as usize].1.state = NODE_DRAINING;
        }
        let mut alive = Vec::new();
        for t in 1..=n {
            if r.below(3) > 0 {
                alive.push((t, if r.below(3) == 0 { 900 + t as u64 } else { 100 + t as u64 }));
            }
        }
        let mut f = facts(&alive);
        f.target_lease_shrinking = r.below(4) == 0;
        if let Some(w) = members.windows(2).nth(r.below(members.len() as u64 + 1) as usize).filter(|_| r.below(3) == 0)
        {
            f.failures = vec![fwd_fail(w[0].0, w[1].0, FWD_FAIL_MS)];
        }
        for (t, _) in members.iter().filter(|(_, s)| *s == Y && r.below(2) == 0) {
            f.syncing.insert((1, *t), (0, f.settings.target_lease_ms)); // 已可做第二步
        }
        let p = plan(&m, &f);
        for (_, c) in &p.chains {
            let first_syncing = c.members.iter().position(|(_, s)| *s == Y).unwrap_or(c.members.len());
            assert!(c.members[first_syncing..].iter().all(|(_, s)| *s == Y), "种子 {seed}：I0 不成立 {c:?}");
            assert_eq!(c.ver, 6, "种子 {seed}");
            let ids: BTreeSet<u32> = c.members.iter().map(|(t, _)| *t).collect();
            assert_eq!(ids.len(), c.members.len(), "种子 {seed}：重复的成员");
            let live: Vec<u32> =
                members.iter().filter(|(t, s)| *s == S && f.alive.contains_key(t)).map(|(t, _)| *t).collect();
            for t in &live {
                assert!(ids.contains(t), "种子 {seed}：活着的 Serving 成员 t{t} 被一步移出 {c:?}");
            }
            assert!(
                live.is_empty() || c.members.iter().any(|(_, s)| *s == S),
                "种子 {seed}：没有 Serving 成员了 {c:?}"
            );
        }
    }
}

/// ADR-0040 第 13 条：调小 target 租约的过渡期里只做移除——离线的成员照样移出；离线回来的不 rejoin、短链不补、drain 的替代不追加；
/// 过渡期结束（settings_prev 删掉）之后照常。
#[test]
fn while_the_target_lease_shrinks_the_manager_only_removes() {
    let mut c0 = chain(&[(1, S), (3, S)]);
    c0.removed = vec![(2, NOW - 1000)];
    let mut m = map(4, vec![c0, chain(&[(1, S), (2, S), (3, S)])]);
    m.targets[1].1.state = TARGET_OFFLINE;
    let mut f = facts(&[(1, 101), (2, 102), (3, 103)]);
    f.target_lease_shrinking = true;
    let p = plan(&m, &f);
    assert!(p.chains.is_empty(), "t2 回来了：不 rejoin、短链也不补 {p:?}");
    assert!(p.targets.iter().any(|(id, t)| *id == 2 && t.state == TARGET_UP), "target 照样记为 Up");
    let mut drain = map(4, vec![chain(&[(1, S), (2, S), (3, S)])]);
    drain.nodes[2].1.state = NODE_DRAINING;
    let mut f4 = facts(&[(1, 101), (2, 102), (3, 103), (4, 104)]);
    f4.target_lease_shrinking = true;
    assert!(plan(&drain, &f4).chains.is_empty(), "drain：替代成员（Syncing）也不追加，于是 t3 也还不能移除");
    let gone = map(3, vec![chain(&[(1, S), (2, S), (3, S)])]);
    let mut f = facts(&[(1, 101), (3, 103)]);
    f.target_lease_shrinking = true;
    assert_eq!(plan(&gone, &f).chains[0].1.members, vec![(1, S), (3, S)], "移除照常");
    f4.target_lease_shrinking = false;
    assert_eq!(plan(&drain, &f4).chains[0].1.members.last(), Some(&(4, Y)), "过渡期结束：照常追加");
}

mod two_step;
