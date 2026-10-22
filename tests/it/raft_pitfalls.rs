//! 附录 A（raft-rs 0.7 集成的坑）的回归用例：每个 M3 能触发的坑一个用例（design.md 9.2 ⑤）。
//! 单节点之外的情形用"测试扮演的对端"：节点 1 是我们的 Driver，节点 2 的消息由测试手写（不需要第二个 RawNode）。
//! 需要 fail point 的（P2 的严格先后、`raft.persist.*`）在 failpoint_raft_*.rs；P8 在 raft_compact.rs；
//! P11（report_snapshot）、P13（joint 无人退出的兜底）、P17（快照 + 已提交条目）属于 M8 的快照与 cluster-mgr。

use crossbeam_channel::bounded;
use raft::eraftpb::{ConfChangeSingle, ConfChangeType, ConfChangeV2, Entry, EntryType, Message, MessageType};
use raft::{ReadOnlyOption, StateRole};
use wmfs_core::base::ErrCode;
use wmfs_core::meta::raft::driver::{ProposeOutcome, RaftInput, config};
use wmfs_core::meta::schema::keys::T_DEBUG;
use wmfs_core::meta::schema::raft_state::Applied;
use wmfs_core::meta::schema::records::{KvOp, WriteBatchV1};
use wmfs_testkit::harness::meta::{
    Sim, ack_all, bootstrap, dkey, elect, msg, plugins, ready_leader_with_fake_peer, with_fake_peer,
};

fn ent(index: u64, term: u64, data: Vec<u8>) -> Entry {
    let mut e = Entry::default();
    (e.index, e.term, e.data) = (index, term, data.into());
    e
}

fn batch(expect: u64, k: &str) -> Vec<u8> {
    let ops = vec![KvOp::Put { key: dkey(k), value: k.as_bytes().to_vec() }];
    WriteBatchV1 { expect_version: expect, leader_term: 2, ops, trace_ids: vec![] }.encode()
}

fn solo() -> Sim {
    let mut s = Sim::start(bootstrap(&[1]), 1);
    s.until_ready();
    s
}

/// 节点 2（term `term` 的 leader）发来一个 MsgAppend，然后推进一步。
fn append(s: &mut Sim, term: u64, prev: (u64, u64), ents: Vec<Entry>, commit: u64) {
    let mut m = msg(MessageType::MsgAppend, 2, term);
    (m.index, m.log_term, m.commit) = (prev.0, prev.1, commit);
    m.entries = ents.into();
    s.driver.handle_input(RaftInput::Msg(m));
    s.driver.process_ready();
}

#[test]
fn p2_p3_follower_replies_after_persisting_and_applies_old_commits_with_new_entries() {
    let mut s = with_fake_peer();
    append(&mut s, 2, (1, 1), vec![ent(2, 2, vec![]), ent(3, 2, batch(1, "a"))], 1);
    let out = s.driver.take_outbox();
    assert!(
        out.iter().any(|m| m.get_msg_type() == MessageType::MsgAppendResponse && m.index == 3 && !m.reject),
        "{out:?}"
    );
    let c = Sim::start(s.backend.crash(), 1);
    assert_eq!(c.driver.raw().raft.raft_log.last_index(), 3, "P2：回复发出时条目已经落盘（掉电重启后还在）");
    assert_eq!(s.driver.applied().index, 1, "leader 的 commit 还是 1：只追加、不应用");
    append(&mut s, 2, (3, 2), vec![ent(4, 2, batch(3, "b"))], 3);
    let want = Applied { index: 3, term: 2, state_version: 3 };
    assert_eq!(s.driver.applied(), want, "2 是空条目（不改 state_version）、3 是批次");
    let c = Sim::start(s.backend.crash(), 1);
    assert_eq!(c.raft_state().1.index, 3, "P3：已提交的 2、3 与新条目 4 同一个 Immediate 事务落盘");
    assert_eq!(c.driver.raw().raft.raft_log.last_index(), 4);
    let kv = c.kv(T_DEBUG);
    assert!(kv.contains_key(&dkey("a")) && !kv.contains_key(&dkey("b")), "{kv:?}");
}

#[test]
fn p19_followers_drop_proposals_instead_of_forwarding_them() {
    let mut s = with_fake_peer();
    append(&mut s, 2, (1, 1), vec![], 1);
    assert_eq!((s.driver.raw().raft.state, s.driver.raw().raft.leader_id), (StateRole::Follower, 2));
    s.driver.take_outbox();
    let (done, rx) = bounded(1);
    s.driver.handle_input(RaftInput::Propose { data: batch(1, "x"), leader_term: 2, done });
    s.driver.process_ready();
    assert_eq!(rx.try_recv(), Ok(ProposeOutcome::Dropped));
    assert!(
        !s.driver.take_outbox().iter().any(|m| m.get_msg_type() == MessageType::MsgPropose),
        "过期写集绝不能转发给 leader"
    );
}

/// 3.4.1.4 规则 3：就绪还要求本节点的插件清单 == 已提交的 GLOBAL plugins。执行器看的是发布出来的角色快照（可能已经
/// 过时），Driver 在提议前自己再查一次（3.4.1.3 的 propose）：当选了但清单不一致的 leader，提议直接 Dropped。
#[test]
fn proposals_to_a_leader_whose_plugins_differ_are_dropped() {
    let mut mine = plugins();
    mine.plugins.push("authz:posix-mode".into()); // 本节点多装了一个授权插件
    let mut s = Sim::start_with(bootstrap(&[1]), 1, mine);
    for _ in 0..5 {
        s.driver.process_ready();
        s.driver.tick();
    }
    let raft = &s.driver.raw().raft;
    assert!(raft.state == StateRole::Leader && raft.apply_to_current_term(), "单节点照常当选、应用了本任期的空条目");
    assert!(!s.driver.is_ready(), "插件清单不一致：不就绪");
    let (done, rx) = bounded(1);
    let term = raft.term;
    s.driver.handle_input(RaftInput::Propose { data: batch(1, "x"), leader_term: term, done });
    s.driver.process_ready();
    assert_eq!(rx.try_recv(), Ok(ProposeOutcome::Dropped), "不就绪的 leader 不提议");
    assert!(s.kv(T_DEBUG).is_empty());
}

#[test]
fn p15_responses_from_unknown_peers_are_ignored() {
    let mut s = solo();
    let term = s.driver.raw().raft.term;
    s.driver.handle_input(RaftInput::Msg(msg(MessageType::MsgAppendResponse, 9, term))); // StepPeerNotFound：只记 debug 日志
    s.driver.process_ready();
    assert!(s.driver.is_ready());
    assert_eq!(s.write(vec![KvOp::Put { key: dkey("k"), value: vec![1] }]), ProposeOutcome::Applied { ok: true });
}

#[test]
#[should_panic(expected = "v1 EntryConfChange")]
fn p22_v1_conf_change_entries_are_an_invariant_violation() {
    let mut s = with_fake_peer();
    let mut e = ent(2, 2, vec![]);
    e.set_entry_type(EntryType::EntryConfChange);
    append(&mut s, 2, (1, 1), vec![e], 2);
}

fn cc(changes: &[(ConfChangeType, u64)]) -> ConfChangeV2 {
    let mut c = ConfChangeV2::default();
    let singles: Vec<ConfChangeSingle> = changes
        .iter()
        .map(|(t, id)| {
            let mut s = ConfChangeSingle::default();
            (s.change_type, s.node_id) = (*t, *id);
            s
        })
        .collect();
    c.changes = singles.into();
    c
}

#[test]
fn p4_p5_p21_conf_changes_are_applied_left_and_persisted() {
    let mut s = solo();
    let (d1, r1) = bounded(1);
    let (d2, r2) = bounded(1);
    use ConfChangeType::AddLearnerNode as Learner;
    s.driver.handle_input(RaftInput::ConfChange { cc: cc(&[(Learner, 2), (Learner, 3)]), done: d1 }); // 多个变更 → joint
    s.driver.handle_input(RaftInput::ConfChange { cc: cc(&[(Learner, 4)]), done: d2 }); // 已有未应用的 conf change
    for _ in 0..5 {
        s.driver.process_ready();
    }
    assert_eq!(r1.try_recv(), Ok(ProposeOutcome::Applied { ok: true }));
    assert_eq!(r2.try_recv(), Ok(ProposeOutcome::Applied { ok: false }), "P21：被改写成空的 EntryNormal");
    let cs = s.driver.raw().store().conf_state().clone();
    assert_eq!((cs.voters.clone(), cs.learners.clone()), (vec![1], vec![2, 3]));
    assert!(
        cs.voters_outgoing.is_empty(),
        "P4：raft-rs 自动发起的 leave-joint（空 data 的 ConfChangeV2）被应用了：{cs:?}"
    );
    let (_, applied, _) = s.raft_state();
    let mut c = Sim::start(s.backend.crash(), 1); // 最后一个第二事务（None）没落盘
    let (_, durable, _) = c.raft_state();
    let disk = c.driver.raw().store().conf_state().clone();
    let joint = durable.index < applied.index && !disk.voters_outgoing.is_empty();
    assert!(
        disk == cs || joint,
        "P5：落盘的 ConfState 与落盘的 applied 同事务（applied 停在 leave-joint 之前就还是 joint）：{disk:?}"
    );
    c.until_ready(); // 重放 (applied, commit]：leave-joint 再应用一次
    assert_eq!(c.driver.raw().store().conf_state(), &cs, "重放之后与掉电前相同");
    for _ in 0..10 {
        s.driver.tick(); // 1 s：补一次 Immediate 提交（规则 M2），之前的 None 提交全部落盘
    }
    let c = s.crash();
    assert_eq!(c.driver.raw().store().conf_state(), &cs, "补过 Immediate 之后，掉电重启时 ConfState 原样在盘上");
}

#[test]
fn p9_p20_losing_the_second_txn_to_a_crash_is_not_fatal_and_replays_once() {
    let mut s = solo();
    for k in ["a", "b", "c"] {
        assert_eq!(s.write(vec![KvOp::Put { key: dkey(k), value: k.into() }]), ProposeOutcome::Applied { ok: true });
        let (hs, a, _) = s.raft_state();
        assert_eq!(hs.commit, a.index, "P20：第二个写事务写的是含新 commit 的硬状态");
    }
    let (_, before, _) = s.raft_state();
    let mut c = s.crash(); // 最后一个批次的第二个事务（None）没落盘：applied 落后，日志里有条目
    assert!(c.raft_state().1.index < before.index);
    c.until_ready(); // P9：Config.applied 取自盘上，applied ≤ hs.commit：不 fatal，(applied, commit] 重放一次
    assert_eq!(c.kv(T_DEBUG).len(), 3);
    assert_eq!(
        c.driver.applied().state_version,
        before.state_version,
        "重放后 state_version 与掉电前相同（没有重复应用）"
    );
}

#[test]
fn p10_proposals_are_matched_by_index_and_term() {
    let mut s = solo();
    let term = s.driver.raw().raft.term;
    let (done, rx) = bounded(1);
    s.driver.handle_input(RaftInput::Propose { data: batch(1, "x"), leader_term: term + 1, done });
    assert_eq!(rx.try_recv(), Ok(ProposeOutcome::Dropped), "任期不对：不提议");
    let sv = s.driver.applied().state_version;
    let rxs: Vec<_> = (0..3).map(|i| s.propose(vec![KvOp::Put { key: dkey("x"), value: vec![i] }], Some(sv))).collect();
    s.driver.process_ready();
    let got: Vec<_> = rxs.iter().map(|r| r.try_recv()).collect();
    let want = [
        Ok(ProposeOutcome::Applied { ok: true }),
        Ok(ProposeOutcome::Applied { ok: false }),
        Ok(ProposeOutcome::Applied { ok: false }),
    ];
    assert_eq!(got, want, "三个提议各自认领自己的 (index, term)；后两个 expect_version 已过期");
}

#[test]
fn p14_only_a_sole_voter_campaigns() {
    for voters in [vec![], vec![1, 2]] {
        let mut s = Sim::start(bootstrap(&voters), 1);
        s.driver.process_ready();
        assert_eq!(
            (s.driver.raw().raft.state, s.driver.raw().raft.term),
            (StateRole::Follower, 1),
            "voters {voters:?}"
        );
    }
    let mut s = Sim::start(bootstrap(&[]), 1);
    for _ in 0..40 {
        s.driver.tick();
        s.driver.process_ready();
    }
    assert_eq!(
        s.driver.raw().raft.state,
        StateRole::Follower,
        "空 ConfState 的节点永远不自己 campaign（不会形成第二个集群）"
    );
}

#[test]
fn p7_p16_p18_raft_parameters() {
    let c = config(3, 77);
    assert_eq!((c.id, c.applied, c.election_tick, c.heartbeat_tick), (3, 77, 10, 2));
    assert_eq!(c.read_only_option, ReadOnlyOption::Safe, "P7：绝不用 LeaseBased");
    assert!(c.check_quorum && c.pre_vote, "P16");
    assert_eq!((c.max_committed_size_per_ready, c.max_size_per_msg), (16 << 20, 512 << 10), "P18");
    assert!(c.validate().is_ok());
}

/// P6：Safe 模式下 leader 本任期还没提交时，raft-rs 会静默丢弃 MsgReadIndex——读闸门在就绪之前不发、2 s 后回 RETRY；
/// 丢了的一轮 500 ms 后用新 ctx 重发（2 个 voter，节点 2 由测试扮演）。
#[test]
fn p6_reads_wait_for_the_leader_to_commit_in_its_term_and_lost_rounds_are_resent() {
    let mut s = with_fake_peer();
    elect(&mut s);
    let term = s.driver.raw().raft.term;
    let (done, reader) = bounded(1);
    s.driver.handle_input(RaftInput::ReadIndex { done });
    for _ in 0..20 {
        s.driver.tick();
        s.driver.process_ready();
        let hb: Vec<Message> =
            s.driver.take_outbox().into_iter().filter(|m| m.get_msg_type() == MessageType::MsgHeartbeat).collect();
        assert!(hb.iter().all(|m| m.context.is_empty()), "未就绪的 leader 不发 read_index");
        for _ in hb {
            s.driver.handle_input(RaftInput::Msg(msg(MessageType::MsgHeartbeatResponse, 2, term))); // 让 check_quorum 满意
        }
    }
    assert_eq!(reader.try_recv(), Ok(Err(ErrCode::RETRY)), "未就绪的 leader：等待超过 2 s 的读者回 RETRY");
    ack_all(&mut s);
    assert!(s.driver.is_ready(), "本任期的空条目提交并应用之后才就绪");
    let (done, reader) = bounded(1);
    s.driver.handle_input(RaftInput::ReadIndex { done });
    s.driver.process_ready();
    let ctx_of = |out: Vec<Message>| out.into_iter().find(|m| !m.context.is_empty()).map(|m| m.context.to_vec());
    let first = ctx_of(s.driver.take_outbox()).expect("发了带 ctx 的心跳");
    let mut resent = None;
    for _ in 0..5 {
        s.driver.tick(); // 不回应这一轮（丢了）
        s.driver.process_ready();
        resent = ctx_of(s.driver.take_outbox()).filter(|c| *c != first).or(resent);
    }
    let ctx = resent.expect("500 ms 没有 ReadState → 用新 ctx 重发");
    let mut hb = msg(MessageType::MsgHeartbeatResponse, 2, term);
    hb.context = ctx.into();
    s.driver.handle_input(RaftInput::Msg(hb));
    s.driver.process_ready();
    assert_eq!(reader.try_recv(), Ok(Ok(())), "多数派确认之后放行");
}

/// 3.4.1.3 ①：角色从 Leader 变为其他 → 在途提议全部判 `Lost`（结果不确定，靠去重表兜底），读闸门全部 `NOT_LEADER`。
#[test]
fn stepping_down_turns_pending_proposals_into_lost_and_readers_into_not_leader() {
    let mut s = ready_leader_with_fake_peer();
    let term = s.driver.raw().raft.term;
    let pending = s.propose(vec![KvOp::Put { key: dkey("x"), value: vec![1] }], None); // 节点 2 不确认：一直在途
    s.driver.process_ready();
    let (done, reader) = bounded(1);
    s.driver.handle_input(RaftInput::ReadIndex { done });
    s.driver.process_ready();
    assert!(pending.try_recv().is_err() && reader.try_recv().is_err(), "还在等多数派");
    let mut hb = msg(MessageType::MsgHeartbeat, 2, term + 1); // 节点 2 以更高的 term 当了 leader
    hb.commit = 1;
    s.driver.handle_input(RaftInput::Msg(hb));
    s.driver.process_ready();
    assert_eq!(s.driver.raw().raft.state, StateRole::Follower);
    assert_eq!(pending.try_recv(), Ok(ProposeOutcome::Lost));
    assert_eq!(reader.try_recv(), Ok(Err(ErrCode::NOT_LEADER)));
}
