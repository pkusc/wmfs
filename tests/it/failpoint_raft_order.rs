//! 坑 P2 的严格先后（design.md 3.4.1.3 ② ④）：leader 的消息可在落盘前发出，follower 的消息必须在落盘之后发出。
//! 用 fail point `raft.persist.before_commit = panic` 让第一个写事务在提交前"崩溃"，再看 outbox：follower 的
//! MsgAppendResponse 一定还没发出；leader 的 MsgAppend 已经发出（论文 10.2.1：leader 并行复制）。
//! fail point 是进程级的，所以单独一个测试程序、一个用例。

use std::panic::{AssertUnwindSafe, catch_unwind};

use raft::eraftpb::{Entry, MessageType};
use wmfs_core::api::failpoint;
use wmfs_core::meta::raft::driver::RaftInput;
use wmfs_core::meta::schema::records::KvOp;
use wmfs_testkit::harness::meta::{Sim, dkey, msg, ready_leader_with_fake_peer, with_fake_peer};

/// 推进一步，提交前 panic；返回 panic 之前已经放进 outbox 的消息类型。
fn step_crashing_before_commit(s: &mut Sim) -> Vec<MessageType> {
    failpoint::set("raft.persist.before_commit", "panic").unwrap();
    let r = catch_unwind(AssertUnwindSafe(|| s.driver.process_ready()));
    failpoint::clear("raft.persist.before_commit");
    assert!(r.is_err(), "fail point 应当触发");
    s.driver.take_outbox().iter().map(|m| m.get_msg_type()).collect()
}

#[test]
fn leader_sends_before_persisting_but_followers_only_after() {
    // follower：节点 2（测试扮演的 leader）发来 MsgAppend
    let mut f = with_fake_peer();
    let mut m = msg(MessageType::MsgAppend, 2, 2);
    (m.index, m.log_term, m.commit) = (1, 1, 1);
    let mut e = Entry::default();
    (e.index, e.term) = (2, 2);
    m.entries = vec![e].into();
    f.driver.handle_input(RaftInput::Msg(m));
    let sent = step_crashing_before_commit(&mut f);
    assert!(!sent.contains(&MessageType::MsgAppendResponse), "follower 的回复不能先于落盘发出：{sent:?}");

    // leader：2 个 voter，节点 2 投票、确认本任期的空条目，然后提议一个批次
    let mut l = ready_leader_with_fake_peer();
    l.driver.take_outbox();
    let _ = l.propose(vec![KvOp::Put { key: dkey("x"), value: vec![1] }], None);
    let sent = step_crashing_before_commit(&mut l);
    assert!(sent.contains(&MessageType::MsgAppend), "leader 的 MsgAppend 在落盘之前就已发出：{sent:?}");
}
