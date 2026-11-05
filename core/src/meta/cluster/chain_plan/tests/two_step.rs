//! 活着的成员分两步移出（ADR-0041 第 13、16 条）：drain、只读、10 s 转发不到。第一步降为 Syncing（最后一个 Serving 原地降，其余移到
//! 末尾），等它装上新图（或满一个租约）再移出；判断看已提交的图；调小 target 租约的过渡期里不降级活着的成员，离线的照旧一步移出。

use super::*;

/// `p` 已提交之后的集群图（改过的链换上，map_epoch + 1）。
fn after(m: &ClusterMap, p: &Plan) -> ClusterMap {
    let mut m2 = m.clone();
    for (id, c) in &p.chains {
        m2.chains.iter_mut().filter(|(i, _)| i == id).for_each(|(_, x)| *x = c.clone());
    }
    m2.map_epoch += 1;
    m2
}

/// 本 leader 看到链 1 的 `t` 是 Syncing（从 `m` 起）已 `ms`；`reported` = 它的心跳报过 `m` 这张图。
fn syncing(f: &mut Facts, m: &ClusterMap, t: u32, ms: u64, reported: bool) {
    f.syncing.insert((1, t), (m.map_epoch, ms));
    if reported {
        f.epoch_seen.insert(t, m.map_epoch);
    }
}

#[test]
fn a_member_that_cannot_reach_its_successor_for_10s_loses_the_successor() {
    let m = map(3, vec![chain(&[(1, S), (2, S), (3, S)])]);
    let mut f = facts(&[(1, 101), (2, 102), (3, 103)]);
    f.failures = vec![fwd_fail(1, 2, FWD_FAIL_MS - 1)];
    assert!(plan(&m, &f).is_empty(), "还不到 10 s");
    f.failures = vec![fwd_fail(1, 2, FWD_FAIL_MS)];
    let p = plan(&m, &f);
    assert_eq!(
        p.chains[0].1.members,
        vec![(1, S), (3, S), (2, Y)],
        "第一步：下游那一个降为 Syncing、移到末尾（还在写路径上）"
    );
    assert_eq!(p.parting, vec![(1, 2)], "记着要移出它");
    let m1 = after(&m, &p);
    let mut f1 = facts(&[(1, 101), (2, 102), (3, 103)]);
    f1.parting = [(1, 2)].into(); // 降级之后它的前驱换了，失败报告也就没了
    syncing(&mut f1, &m1, 2, 0, false);
    assert!(plan(&m1, &f1).is_empty(), "它还没装上降级的图：先不移出");
    syncing(&mut f1, &m1, 2, 0, true);
    let c = &plan(&m1, &f1).chains[0].1;
    assert_eq!(c.members, vec![(1, S), (3, S)], "第二步：它报过新图之后移出（报告没了也照样）");
    assert_eq!(c.removed, vec![(2, NOW)], "记进 removed");
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
    let m1 = after(&m, &plan(&m, &f)); // 第一步（降级）已提交
    f.failures = vec![];
    f.parting = [(1, 2)].into();
    syncing(&mut f, &m1, 2, 0, true);
    let p = plan(&m1, &f);
    assert_eq!(p.chains[0].1.members, vec![(1, S), (3, S)]);
    let m2 = after(&m1, &p); // 第二步已提交
    (f.parting, f.syncing) = Default::default(); // 它不再是 Syncing：chain_mgr 忘掉它
    f.now_ms = NOW + 200;
    let p2 = plan(&m2, &f);
    assert!(p2.is_empty(), "下一轮：不追加回来：{p2:?}");
    f.now_ms = NOW + ClusterSettingsV1::default().repair_delay_ms + 200;
    let c = &plan(&m2, &f).chains[0].1;
    assert_eq!(c.members, vec![(1, S), (3, S), (2, Y)], "过了 repair_delay：按修复补回（Syncing）");
}

#[test]
fn draining_moves_targets_out_after_replacements_serve() {
    let mut m = map(3, vec![chain(&[(1, S), (2, S), (3, S)])]);
    m.nodes[2].1.state = NODE_DRAINING;
    let mut all = facts(&[(1, 101), (2, 102), (3, 103)]);
    let p = plan(&m, &all);
    assert_eq!(
        p.chains[0].1.members,
        vec![(1, S), (2, S), (3, Y)],
        "3 个节点、没有替代的：其余两个够了，L 先原地降为 Syncing"
    );
    let m1 = after(&m, &p);
    syncing(&mut all, &m1, 3, 0, false);
    assert!(plan(&m1, &all).is_empty(), "它还没装上降级的图：先不移出");
    syncing(&mut all, &m1, 3, 0, true);
    let c = &plan(&m1, &all).chains[0].1;
    assert_eq!(c.members, vec![(1, S), (2, S)], "再移出（迁走的不记进 removed）");
    assert!(c.removed.is_empty());
    assert_eq!(want(&m, c), 2, "管理 API 的期望长度与链管理器同一口径（drain 中的节点不算）");
    let mut m4 = map(4, vec![chain(&[(1, S), (2, S), (3, S)])]);
    m4.nodes[2].1.state = NODE_DRAINING;
    let mut f4 = facts(&[(1, 101), (2, 102), (3, 103), (4, 104)]);
    let c = &plan(&m4, &f4).chains[0].1;
    assert_eq!(c.members, vec![(1, S), (2, S), (3, S), (4, Y)], "先追加替代成员（Syncing）");
    m4.chains[0].1 = ChainRecV1 { members: vec![(1, S), (2, S), (3, S), (4, S)], ..m4.chains[0].1.clone() };
    let p = plan(&m4, &f4);
    assert_eq!(p.chains[0].1.members, vec![(1, S), (2, S), (4, S), (3, Y)], "替代成员 Serving 之后：t3 降级、移到末尾");
    let m5 = after(&m4, &p);
    syncing(&mut f4, &m5, 3, 0, true);
    assert_eq!(plan(&m5, &f4).chains[0].1.members, vec![(1, S), (2, S), (4, S)], "再移出");
}

/// M9 验证发现：t2 离线后回来时链已被新加入的 t4 补满（没能 rejoin，留在 removed 里）；之后 drain t4，能替代的只有 t2——drain 的替代
/// 成员不等 repair_delay（3.4.2.10 的 drain 一行），否则 t4 要 30 min 才迁得走。不在 drain 时短链照样等（见"修复"的用例）。
#[test]
fn a_drain_replacement_does_not_wait_for_the_repair_delay() {
    let mut c0 = chain(&[(3, S), (1, S), (4, S)]);
    c0.removed = vec![(2, NOW - 1000)];
    let mut m = map(4, vec![c0.clone(), ChainRecV1 { replicas: 1, ..chain(&[(2, S)]) }]);
    m.nodes[3].1.state = NODE_DRAINING;
    let mut f = facts(&[(1, 101), (2, 102), (3, 103), (4, 104)]);
    let p = plan(&m, &f);
    assert_eq!(p.chains.len(), 1, "{p:?}");
    let c = &p.chains[0].1;
    assert_eq!(c.members, vec![(3, S), (1, S), (4, S), (2, Y)], "先追加替代成员（Syncing），不等 repair_delay");
    assert!(c.removed.is_empty(), "t2 回到链上了：不再记在 removed 里");
    m.chains[0].1.members = vec![(3, S), (1, S), (4, S), (2, S)];
    let p = plan(&m, &f);
    assert_eq!(p.chains[0].1.members, vec![(3, S), (1, S), (2, S), (4, Y)], "替代成员 Serving 之后：t4 先降级");
    let m1 = after(&m, &p);
    syncing(&mut f, &m1, 4, 0, true);
    assert_eq!(plan(&m1, &f).chains[0].1.members, vec![(3, S), (1, S), (2, S)], "再移出 t4");
}

/// 已有链里的成员变成只读（3.2.3 的 ro_compat 位）：像 drain 一样迁出写路径——先补一个成员，够了再分两步移出它。
#[test]
fn read_only_members_leave_the_write_path_like_drained_ones() {
    let mut m = map(4, vec![chain(&[(1, S), (2, S), (3, S)])]);
    m.targets[2].1.read_only = true;
    let mut f = facts(&[(1, 101), (2, 102), (3, 103), (4, 104)]);
    assert_eq!(plan(&m, &f).chains[0].1.members, vec![(1, S), (2, S), (3, S), (4, Y)]);
    m.chains[0].1.members = vec![(1, S), (2, S), (3, S), (4, S)];
    let p = plan(&m, &f);
    assert_eq!(p.chains[0].1.members, vec![(1, S), (2, S), (4, S), (3, Y)]);
    let m1 = after(&m, &p);
    syncing(&mut f, &m1, 3, 0, true);
    assert_eq!(plan(&m1, &f).chains[0].1.members, vec![(1, S), (2, S), (4, S)]);
}

/// 第 13 条：要移出的是最后一个 Serving 时原地降——它后面已有的 Syncing 成员不能排到它前面（否则那个成员对重发回的"重复确认"会让
/// 写者拿到确认，而它还在旧图上、自认是 L、只有 pending，回旧版本）。
#[test]
fn the_last_serving_member_is_demoted_in_place() {
    let m = map(3, vec![chain(&[(1, S), (2, S), (3, Y)])]);
    let mut f = facts(&[(1, 101), (2, 102), (3, 103)]);
    f.failures = vec![fwd_fail(1, 2, FWD_FAIL_MS)];
    assert_eq!(plan(&m, &f).chains[0].1.members, vec![(1, S), (2, Y), (3, Y)], "转发失败：t3 仍在它之后");
    let mut m = map(4, vec![ChainRecV1 { replicas: 2, ..chain(&[(1, S), (2, S), (3, S), (4, Y)]) }]);
    m.nodes[2].1.state = NODE_DRAINING;
    let f = facts(&[(1, 101), (2, 102), (3, 103), (4, 104)]);
    assert_eq!(plan(&m, &f).chains[0].1.members, vec![(1, S), (2, S), (3, Y), (4, Y)], "drain：同样原地降");
}

/// M10 审查（Jepsen-lite 与 drain 同时跑时发现）：t1 重启过、正在重同步时被 drain，链 [2, 1(Y), 3(Y)]——其余 Serving 成员不够 `want`，
/// 而 t3 排在 t1 后面、提升不了（只提升紧跟最后一个 Serving 的那个），t1 又不再被提升（`sync_done` 回 RETRY）。已是 Syncing 的要迁出的
/// 成员不看"其余 Serving 成员够了"，照常做第二步，链才走得下去。
#[test]
fn a_leaving_member_that_is_already_syncing_does_not_block_the_one_behind_it() {
    let mut m = map(3, vec![chain(&[(2, S), (1, Y), (3, Y)])]);
    m.nodes[0].1.state = NODE_DRAINING;
    let mut f = facts(&[(1, 101), (2, 102), (3, 103)]);
    syncing(&mut f, &m, 1, 0, false);
    assert!(plan(&m, &f).is_empty(), "它还没装上这张图：先不移出");
    syncing(&mut f, &m, 1, 0, true);
    assert_eq!(plan(&m, &f).chains[0].1.members, vec![(2, S), (3, Y)], "移出之后 t3 紧跟最后一个 Serving，可以被提升");
}

/// 第 13 条：Serving 还是 Syncing 看已提交的图——同一轮里转发失败与 drain 指向同一个成员时只降级，即使它看起来已满足第二步的条件。
#[test]
fn a_member_hit_by_forward_failures_and_drain_in_one_round_is_only_demoted() {
    let mut m = map(3, vec![chain(&[(1, S), (2, S), (3, S)])]);
    m.nodes[1].1.state = NODE_DRAINING;
    let mut f = facts(&[(1, 101), (2, 102), (3, 103)]);
    f.failures = vec![fwd_fail(1, 2, FWD_FAIL_MS)];
    let lease = f.settings.target_lease_ms;
    syncing(&mut f, &m, 2, lease, true);
    let p = plan(&m, &f);
    assert_eq!(p.chains[0].1.members, vec![(1, S), (3, S), (2, Y)], "只降级、不移出");
}

/// 第 13 条：它心跳成功、却一直没报新图（装不上：它已因 I5 自我隔离）——本 leader 看到它是 Syncing 满一个 target 租约（取 max(当前值,
/// settings_prev)，chain_mgr 交进来的 settings 已取过）之后也移出，写不会一直卡住。
#[test]
fn a_syncing_member_that_never_reports_the_new_map_leaves_after_one_lease() {
    let m = map(3, vec![chain(&[(1, S), (3, S), (2, Y)])]);
    let mut f = facts(&[(1, 101), (2, 102), (3, 103)]);
    f.parting = [(1, 2)].into();
    let lease = f.settings.target_lease_ms;
    syncing(&mut f, &m, 2, lease - 1, false);
    assert!(plan(&m, &f).is_empty(), "还不到一个租约");
    syncing(&mut f, &m, 2, lease, false);
    assert_eq!(plan(&m, &f).chains[0].1.members, vec![(1, S), (3, S)], "满一个租约：移出");
    f.settings.target_lease_ms = 3 * lease; // settings_prev 里的旧租约更长
    assert!(plan(&m, &f).is_empty(), "按较长的那个租约算");
}

/// 第 16 条：调小 target 租约的过渡期里不降级活着的成员（数据节点按新图里较短的租约算新 L 的读窗口）；已降级成员的第二步照常；离线的
/// 照旧一步移出（它已自我隔离）。
#[test]
fn while_the_target_lease_shrinks_live_members_are_not_demoted() {
    let mut m = map(3, vec![chain(&[(1, S), (2, S), (3, S)])]);
    m.nodes[2].1.state = NODE_DRAINING;
    let mut f = facts(&[(1, 101), (2, 102), (3, 103)]);
    f.target_lease_shrinking = true;
    assert!(plan(&m, &f).is_empty(), "drain：活着的 L 先不降级");
    f.failures = vec![fwd_fail(1, 2, FWD_FAIL_MS)];
    assert!(plan(&m, &f).is_empty(), "转发失败：活着的下游也先不降级");
    let mut gone = facts(&[(1, 101), (2, 102)]);
    gone.target_lease_shrinking = true;
    assert_eq!(plan(&m, &gone).chains[0].1.members, vec![(1, S), (2, S)], "离线的：照旧一步移出");
    m.chains[0].1.members = vec![(1, S), (2, S), (3, Y)];
    f.failures = vec![];
    syncing(&mut f, &m, 3, 0, true);
    assert_eq!(plan(&m, &f).chains[0].1.members, vec![(1, S), (2, S)], "已降级的：第二步照常");
}
