//! 附录 A 里 M8 才碰得到的坑的回归用例（3 个真的 Driver + 内存总线 `harness::sim_net`）：
//! - P11：快照没送到（下载失败）→ 向 leader 报告失败 → leader 不卡在 Snapshot 状态，之后重发、装上；
//! - P13：joint 配置可能无人退出——`Explicit` 的 joint 变更停在 joint 里，提议空的 `ConfChangeV2::default()`（leave-joint 兜底的那一步）
//!   之后退出，成为 3 voter；
//! - P17 / 规则 M5：快照安装与已提交条目不同时出现；装完立即掉电重启，快照的状态原样在盘上（Immediate）、之后照常追日志。

use crossbeam_channel::Receiver;
use raft::eraftpb::{ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2, MessageType};
use wmfs_core::base::rand::XorShift64;
use wmfs_core::meta::raft::driver::ProposeOutcome;
use wmfs_core::meta::schema::keys::T_DEBUG;
use wmfs_core::meta::schema::records::KvOp;
use wmfs_testkit::harness::meta::{Sim, bootstrap, dkey, empty};
use wmfs_testkit::harness::sim_net::Net;

fn cc(t: ConfChangeType, ids: &[u64]) -> ConfChangeV2 {
    let mut c = ConfChangeV2::default();
    let one = |id: &u64| {
        let mut s = ConfChangeSingle::default();
        (s.change_type, s.node_id) = (t, *id);
        s
    };
    c.changes = ids.iter().map(one).collect::<Vec<_>>().into();
    c
}

/// 推进：投递全部消息、各节点处理 Ready，`n` 轮（每 3 轮 tick 一次）。
fn spin(net: &mut Net, r: &mut XorShift64, n: u32) {
    for i in 0..n {
        net.ready_all();
        while !net.flight.is_empty() {
            let k = r.below(net.flight.len() as u64) as usize;
            net.deliver(k, r);
        }
        if i % 3 == 0 {
            (1..=3).for_each(|id| net.tick(id));
        }
    }
}

fn wait(net: &mut Net, r: &mut XorShift64, rx: &Receiver<ProposeOutcome>) -> ProposeOutcome {
    for _ in 0..200 {
        spin(net, r, 1);
        if let Ok(o) = rx.try_recv() {
            return o;
        }
    }
    panic!("提议 200 轮没有结局");
}

/// 1 个 voter + 2 个装好快照、追上了的 learner。
fn with_learners(r: &mut XorShift64) -> Net {
    let mut net = Net::new(vec![Sim::start(bootstrap(&[1]), 1), Sim::start(empty(), 2), Sim::start(empty(), 3)]);
    spin(&mut net, r, 3);
    assert!(net.node(1).driver.is_ready());
    for id in [2, 3] {
        let rx = net.conf_change(1, cc(ConfChangeType::AddLearnerNode, &[id]));
        assert_eq!(wait(&mut net, r, &rx), ProposeOutcome::Applied { ok: true });
    }
    spin(&mut net, r, 30);
    assert_eq!(net.snapshots, 2, "两个空节点各装了一个快照（leader 的日志从 index 2 起）");
    net
}

#[test]
fn p13_a_joint_config_that_nobody_leaves_is_left_by_an_empty_conf_change() {
    let mut r = XorShift64::new(13);
    let mut net = with_learners(&mut r);
    let mut joint = cc(ConfChangeType::AddNode, &[2, 3]);
    joint.set_transition(ConfChangeTransition::Explicit); // 不自动退出：模拟"leader 中途换了、没人退出 joint"
    let rx = net.conf_change(1, joint);
    assert_eq!(wait(&mut net, &mut r, &rx), ProposeOutcome::Applied { ok: true });
    spin(&mut net, &mut r, 30);
    let st = net.node(1).driver.published().status();
    assert_eq!((st.voters.clone(), st.voters_outgoing.clone()), (vec![1, 2, 3], vec![1]), "停在 joint 里：{st:?}");
    let rx = net.conf_change(1, ConfChangeV2::default()); // leave-joint 兜底（cluster-mgr 在 joint 超过 5 s 时提的就是它）
    assert_eq!(wait(&mut net, &mut r, &rx), ProposeOutcome::Applied { ok: true });
    spin(&mut net, &mut r, 10);
    for id in 1..=3 {
        let st = net.node(id).driver.published().status();
        assert!(st.voters == [1, 2, 3] && st.voters_outgoing.is_empty(), "node{id} 退出了 joint：{st:?}");
    }
}

#[test]
fn p11_a_lost_snapshot_is_reported_and_sent_again() {
    let mut r = XorShift64::new(11);
    let mut net = Net::new(vec![Sim::start(bootstrap(&[1]), 1), Sim::start(empty(), 2), Sim::start(empty(), 3)]);
    spin(&mut net, &mut r, 3);
    let rx = net.conf_change(1, cc(ConfChangeType::AddLearnerNode, &[2]));
    net.cut.insert(2); // 发给它的快照都"下载失败"
    for _ in 0..30 {
        net.ready_all();
        while let Some(m) = net.flight.pop() {
            let is_snap = m.get_msg_type() == MessageType::MsgSnapshot;
            net.flight.push(m);
            let last = net.flight.len() - 1;
            net.deliver(last, &mut r); // 被挡住的 MsgSnapshot 向 leader 报告失败（坑 P11）
            if is_snap {
                break;
            }
        }
        net.tick(1);
    }
    assert_eq!(rx.try_recv(), Ok(ProposeOutcome::Applied { ok: true }));
    assert_eq!(net.snapshots, 0);
    net.cut.clear();
    spin(&mut net, &mut r, 60);
    assert_eq!(net.snapshots, 1, "报告失败之后 leader 没有卡住：重发、装上了");
    let (a1, h1) = net.state(1);
    assert_eq!(net.state(2), (a1, h1), "learner 追平了");
}

#[test]
fn p17_m5_an_installed_snapshot_is_durable_and_the_log_continues_after_it() {
    let mut r = XorShift64::new(17);
    let mut net = with_learners(&mut r);
    let put = |k: &str| vec![KvOp::Put { key: dkey(k), value: k.as_bytes().to_vec() }];
    let rx = net.node(1).propose(put("after-snap"), None);
    assert_eq!(wait(&mut net, &mut r, &rx), ProposeOutcome::Applied { ok: true });
    spin(&mut net, &mut r, 10);
    net.crash(2); // 装完快照、又追了日志之后掉电：快照的状态与之后的条目都在盘上
    spin(&mut net, &mut r, 20);
    let kv = net.node(2).kv(T_DEBUG);
    assert!(kv.contains_key(&dkey("after-snap")), "掉电重启之后：快照 + 之后的日志都在");
    assert_eq!(net.state(2), net.state(1));
    let (hs, a, snap) = net.node(2).raft_state();
    assert!(
        a.index <= hs.commit && snap.index >= 1,
        "规则 M1/M5：applied ≤ commit，snap_meta 是快照点：{hs:?} {a:?} {snap:?}"
    );
}
