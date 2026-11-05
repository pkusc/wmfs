//! 集群插件清单的比对在写事务提交之后才发布（design.md 3.7.2 的实现注记（M10））：raft 线程应用了改 GLOBAL plugins 的批次，
//! 写事务还没提交时别的线程看不到 `mismatch`（以前在写事务里就发布，cluster-mgr 看到之后读 kv，读到的还是提交之前的旧清单，
//! 退出日志里的差异是空的）；提交之后发布的 `mismatch` 带着差异（cluster-mgr 照抄它写退出日志）。
//! 用 fail point `raft.persist.before_commit = panic` 让应用这个批次的写事务在提交前"崩溃"。fail point 是进程级的，所以单独一个
//! 测试程序、一个用例。

use std::panic::{AssertUnwindSafe, catch_unwind};

use raft::eraftpb::MessageType;
use wmfs_core::api::failpoint;
use wmfs_core::meta::raft::driver::RaftInput;
use wmfs_core::meta::schema::keys::{self, G_PLUGINS};
use wmfs_core::meta::schema::records::{KvOp, encode_v1};
use wmfs_testkit::harness::meta::{Sim, msg, plugins, ready_leader_with_fake_peer};

/// 就绪的 leader（2 个 voter，节点 2 由测试扮演）提议把集群插件清单改成多一个 authz:deny-uid，推进一步（条目落盘），
/// 再让节点 2 确认（批次随之提交，下一步 process_ready 在第一个写事务里应用它）。
fn leader_about_to_apply_a_list_change() -> Sim {
    let mut s = ready_leader_with_fake_peer();
    assert_eq!(s.driver.published().plugins_mismatch(), None, "开始时清单一致");
    let mut list = plugins();
    list.plugins.push("authz:deny-uid".into());
    list.plugins.sort();
    let _ = s.propose(vec![KvOp::Put { key: keys::global(G_PLUGINS), value: encode_v1(&list) }], None);
    s.driver.process_ready();
    let mut ack = msg(MessageType::MsgAppendResponse, 2, s.driver.raw().raft.term);
    ack.index = s.driver.raw().raft.raft_log.last_index();
    s.driver.handle_input(RaftInput::Msg(ack));
    s
}

#[test]
fn plugin_mismatch_is_published_only_after_the_apply_commits_and_carries_the_diff() {
    // 应用这个批次的写事务在提交前崩溃：还没发布
    let mut s = leader_about_to_apply_a_list_change();
    failpoint::set("raft.persist.before_commit", "panic").unwrap();
    let r = catch_unwind(AssertUnwindSafe(|| s.driver.process_ready()));
    failpoint::clear("raft.persist.before_commit");
    assert!(r.is_err(), "fail point 应当触发（批次在第一个写事务里应用）");
    assert_eq!(s.driver.published().plugins_mismatch(), None, "写事务还没提交就发布了 mismatch");

    // 照常提交：发布的 mismatch 带着差异，leader 不再就绪
    let mut s = leader_about_to_apply_a_list_change();
    s.driver.process_ready();
    let why = s.driver.published().plugins_mismatch().expect("提交之后发布 mismatch");
    assert!(why.contains("本节点缺少 authz:deny-uid"), "差异应列出缺少的插件：{why:?}");
    assert!(!s.driver.is_ready(), "插件清单不一致：不就绪");
}
