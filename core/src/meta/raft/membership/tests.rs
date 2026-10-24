//! 成员变更决定（`plan`、`heir`）与 `Membership::tick` 的单测：3.4.1.9 的每一条规则，以及 leave-joint 兜底（坑 P13）。

use std::cell::RefCell;

use raft::eraftpb::ConfChangeType::{self, AddLearnerNode, AddNode, RemoveNode};

use super::*;

const V: u8 = META_VOTER;
const L: u8 = META_LEARNER;
const ACT: u8 = NODE_ACTIVE;

fn rec(meta_role: u8, state: u8) -> NodeRecV1 {
    NodeRecV1 {
        addrs: vec![],
        mgmt_addr: String::new(),
        host_id: String::new(),
        machine_id: String::new(),
        hostname: String::new(),
        meta_role,
        features: vec![],
        joined_ms: 0,
        state,
    }
}

/// voters、learners 与各成员的 matched（commit = 5000；都在最近一个选举超时里来过消息）。
fn st(voters: &[u32], learners: &[u32], matched: &[(u32, u64)]) -> EngineStatus {
    EngineStatus {
        commit: 5000,
        voters: voters.to_vec(),
        learners: learners.to_vec(),
        progress: matched.to_vec(),
        active: matched.iter().map(|(id, _)| *id).collect(),
        ..Default::default()
    }
}

fn changes(s: Option<Step>) -> Vec<(ConfChangeType, u64)> {
    match s {
        Some(Step::Conf(cc, _)) => cc.get_changes().iter().map(|c| (c.get_change_type(), c.node_id)).collect(),
        other => panic!("应是一个 conf change：{other:?}"),
    }
}

#[test]
fn learners_join_then_one_joint_change_makes_three_voters() {
    let nodes = vec![(1, rec(V, ACT)), (2, rec(L, ACT)), (3, rec(L, ACT))];
    assert_eq!(changes(plan(&st(&[1], &[], &[(1, 5000)]), &nodes, 1)), [(AddLearnerNode, 2)], "先以 learner 加入");
    assert_eq!(changes(plan(&st(&[1], &[2], &[(1, 5000), (2, 5000)]), &nodes, 1)), [(AddLearnerNode, 3)]);
    let s = st(&[1], &[2, 3], &[(1, 5000), (2, 4500), (3, 100)]);
    assert_eq!(plan(&s, &nodes, 1), None, "只有一个 learner 追上：保持 1 voter（不经过 2 voter 的中间态）");
    let s = st(&[1], &[2, 3], &[(1, 5000), (2, 4500), (3, 4100)]);
    assert_eq!(changes(plan(&s, &nodes, 1)), [(AddNode, 2), (AddNode, 3)], "一个 ConfChangeV2 带两个 AddNode（joint）");
    let s = st(&[1, 2, 3], &[], &[(1, 5000), (2, 5000), (3, 5000)]);
    assert_eq!(plan(&s, &nodes, 1), Some(Step::Roles(vec![(2, V), (3, V)])), "记录跟上实际：成了 voter");
}

/// 审查发现的 blocker：commit 还不到 1000 的新集群里，一个条目、一个快照都没收到的 learner（matched 0）不算追上。
#[test]
fn learners_that_never_acknowledged_anything_are_not_caught_up_in_a_young_cluster() {
    let nodes = vec![(1, rec(V, ACT)), (2, rec(L, ACT)), (3, rec(L, ACT))];
    let young = |m2, m3| EngineStatus { commit: 40, ..st(&[1], &[2, 3], &[(1, 40), (2, m2), (3, m3)]) };
    assert!(!young(0, 0).caught_up(2), "matched 0：没确认过任何数据");
    assert_eq!(plan(&young(0, 0), &nodes, 1), None, "都没追上：不提升（以前会进入 voters [1,2,3]、无法写入）");
    assert_eq!(plan(&young(40, 0), &nodes, 1), None, "只有一个追上");
    assert_eq!(changes(plan(&young(40, 38), &nodes, 1)), [(AddNode, 2), (AddNode, 3)]);
}

#[test]
fn two_node_clusters_keep_one_voter_and_one_learner() {
    let nodes = vec![(1, rec(V, ACT)), (2, rec(L, ACT))];
    assert_eq!(plan(&st(&[1], &[2], &[(1, 5000), (2, 5000)]), &nodes, 1), None);
    let nodes = vec![(1, rec(V, ACT)), (2, rec(V, ACT))]; // wmfs node promote 2：指定为 voter
    assert_eq!(changes(plan(&st(&[1], &[2], &[(1, 5000), (2, 5000)]), &nodes, 1)), [(AddNode, 2)]);
    let nodes = vec![(1, rec(V, ACT)), (2, rec(META_NONE, ACT))]; // --meta none：只做数据节点
    assert_eq!(plan(&st(&[1], &[], &[(1, 5000)]), &nodes, 1), None);
}

#[test]
fn removing_a_voter_promotes_a_caught_up_learner_in_the_same_change() {
    let nodes = vec![(1, rec(V, ACT)), (2, rec(V, ACT)), (3, rec(V, NODE_REMOVED)), (4, rec(L, ACT))];
    let s = st(&[1, 2, 3], &[4], &[(1, 5000), (2, 5000), (3, 5000), (4, 4900)]);
    assert_eq!(changes(plan(&s, &nodes, 1)), [(RemoveNode, 3), (AddNode, 4)]);
    let s = st(&[1, 2, 3], &[4], &[(1, 5000), (2, 5000), (3, 5000), (4, 10)]);
    assert_eq!(changes(plan(&s, &nodes, 1)), [(RemoveNode, 3)], "learner 没追上：只移除");
    let nodes = vec![(1, rec(V, NODE_REMOVED)), (2, rec(V, ACT)), (3, rec(V, ACT))];
    let s = st(&[1, 2, 3], &[], &[(1, 5000), (2, 4000), (3, 4990)]);
    assert!(
        matches!(plan(&s, &nodes, 1), Some(Step::Transfer(3, _))),
        "要移除 leader 自己：先让给 matched 最大的 voter"
    );
    let s = st(&[1, 2], &[], &[(1, 5000), (2, 5000)]);
    let nodes2 = vec![(1, rec(V, ACT)), (2, rec(V, NODE_REMOVED))];
    assert_eq!(changes(plan(&s, &nodes2, 1)), [(RemoveNode, 2)]);
    let s = st(&[1], &[], &[(1, 5000)]);
    assert_eq!(
        plan(&s, &[(1, rec(V, NODE_REMOVED))], 1),
        None,
        "要移除的是唯一的 voter（自己）、没有能接替的：什么也不做"
    );
    let s = st(&[1, 2], &[], &[(1, 5000), (2, 5000)]);
    let gone = vec![(1, rec(V, ACT)), (2, rec(V, NODE_REMOVED)), (3, rec(V, NODE_REMOVED))];
    assert_eq!(changes(plan(&s, &gone, 1)), [(RemoveNode, 2)]);
    let s = st(&[1], &[], &[(1, 5000)]);
    let done = [(1, rec(V, ACT)), (2, rec(V, NODE_REMOVED))];
    assert_eq!(plan(&s, &done, 1), Some(Step::Roles(vec![(2, META_NONE)])), "移除完：记录改成 none");
}

#[test]
fn a_draining_leader_hands_over_and_drained_learners_are_not_promoted() {
    let nodes = vec![(1, rec(V, NODE_DRAINING)), (2, rec(V, ACT)), (3, rec(V, ACT))];
    let s = st(&[1, 2, 3], &[], &[(1, 5000), (2, 5000), (3, 4000)]);
    assert!(matches!(plan(&s, &nodes, 1), Some(Step::Transfer(2, _))));
    let nodes = vec![(1, rec(V, ACT)), (2, rec(L, NODE_DRAINING)), (3, rec(L, ACT))];
    assert_eq!(
        plan(&st(&[1], &[2, 3], &[(1, 5000), (2, 5000), (3, 5000)]), &nodes, 1),
        None,
        "drain 中的 learner 不提升"
    );
}

/// 审查发现：唯一 active 的 voter 死了时，被 drain 的 leader 不能每秒把 leader 让给它（raft-rs 转让期间丢掉所有提议）。
#[test]
fn the_heir_must_be_alive_and_caught_up() {
    let nodes = vec![(1, rec(V, ACT)), (2, rec(V, NODE_DRAINING)), (3, rec(V, NODE_DRAINING))];
    let mut s = st(&[1, 2, 3], &[], &[(1, 5000), (2, 5000), (3, 5000)]);
    s.active = vec![2, 3]; // node1 停了：最近一个选举超时里没来过消息
    assert_eq!(heir(&s, &nodes, 2), None, "唯一能接替的 node1 死了：不让位");
    assert_eq!(plan(&s, &nodes, 2), None, "drain 中的 leader 留在原位，照常服务");
    s.active = vec![1, 2, 3];
    assert_eq!(heir(&s, &nodes, 2), Some(1));
    s.progress = vec![(1, 3000), (2, 5000), (3, 5000)];
    assert_eq!(heir(&s, &nodes, 2), None, "没追上的不接替（转让要先等它追上，期间丢掉所有提议）");
}

/// 测试替身：记下 tick 提议的 conf change。
#[derive(Default)]
struct Fake {
    nodes: Vec<(u32, NodeRecV1)>,
    proposed: RefCell<Vec<ConfChangeV2>>,
}

impl Members for Fake {
    fn node_id(&self) -> u32 {
        1
    }
    fn nodes(&self) -> Vec<(u32, NodeRecV1)> {
        self.nodes.clone()
    }
    fn propose_conf_change(&self, cc: ConfChangeV2) -> Receiver<ProposeOutcome> {
        self.proposed.borrow_mut().push(cc);
        crossbeam_channel::bounded(1).1 // 发送端已丢：下一轮当作"结果不确定"
    }
    fn transfer_leader(&self, _to: u32) {}
    fn set_roles(&self, _roles: &[(u32, u8)]) {}
}

/// 坑 P13 的兜底：joint 配置（`voters_outgoing` 非空）超过 5 s 没人退出 → 提议空的 ConfChangeV2；不到 5 s 什么也不做。
#[test]
fn a_joint_config_left_for_more_than_5_s_is_left_by_an_empty_conf_change() {
    let m = Fake { nodes: vec![(1, rec(V, ACT)), (2, rec(V, ACT)), (3, rec(V, ACT))], ..Default::default() };
    let joint = EngineStatus { voters_outgoing: vec![1], ..st(&[1, 2, 3], &[], &[(1, 50), (2, 50), (3, 50)]) };
    let mut ms = Membership::default();
    ms.tick(&m, &joint, 10_000);
    ms.tick(&m, &joint, 14_000);
    assert!(m.proposed.borrow().is_empty(), "joint 才 4 s：等 raft-rs 自己退出");
    ms.tick(&m, &joint, 15_001);
    assert_eq!(*m.proposed.borrow(), vec![ConfChangeV2::default()], "超过 5 s：空的 ConfChangeV2 退出 joint");
    ms.tick(&m, &joint, 15_100); // 上一个的结局：结果不确定 → 重新计时
    ms.tick(&m, &joint, 16_000);
    assert_eq!(m.proposed.borrow().len(), 1, "重新计时之后又要等 5 s");
}
