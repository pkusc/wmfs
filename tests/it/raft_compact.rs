//! 日志压缩（design.md 3.4.1.8、3.2.6 的写入规则 M3，坑 P8；roadmap M3 验收"写 20 万条后日志被压缩"）：单节点写 20 万个批次（每批
//! 一个键），每应用 10 000 条检查一次，压缩点 = durable_applied − 10 000；之后掉电重启不 fatal（hs.commit ≥ first_index − 1），
//! 数据都在。存储是模拟掉电的内存后端（不做真实 fsync，20 万次提交几秒钟）。

use std::time::Instant;

use wmfs_core::meta::raft::driver::{COMPACT_EVERY, COMPACT_KEEP, ProposeOutcome};
use wmfs_core::meta::schema::keys::T_DEBUG;
use wmfs_core::meta::schema::records::KvOp;
use wmfs_testkit::harness::meta::{Sim, bootstrap, dkey};

const N: u64 = 200_000;

#[test]
fn log_is_compacted_after_200k_writes_and_restart_is_not_fatal() {
    let t0 = Instant::now();
    let mut s = Sim::start(bootstrap(&[1]), 1);
    s.until_ready();
    for i in 0..N {
        let op = KvOp::Put { key: dkey(&format!("c{i:06}")), value: i.to_le_bytes().to_vec() };
        assert_eq!(s.write(vec![op]), ProposeOutcome::Applied { ok: true }, "第 {i} 条");
        if i % 100 == 0 {
            s.driver.tick(); // 逻辑时钟照常走：读闸门、每 1 s 补 Immediate（规则 M2）
        }
        // 规则 M3 任何时刻都成立：压缩点之下的条目都已经落盘应用（压缩点 + 10 000 ≤ durable_applied；init 的压缩点是 1）
        let snap = s.driver.raw().store().snap_index();
        assert!(
            snap == 1 || snap + COMPACT_KEEP <= s.driver.durable_applied(),
            "第 {i} 条：压缩点 {snap} 越过了规则 M3"
        );
    }
    let raw = s.driver.raw();
    let (snap, last) = (raw.store().snap_index(), raw.raft.raft_log.last_index());
    let (applied, durable) = (s.driver.applied().index, s.driver.durable_applied());
    println!(
        "写 {N} 条用时 {:.1} s：压缩点 {snap}，日志 ({snap}, {last}]，applied {applied}，durable {durable}",
        t0.elapsed().as_secs_f64()
    );
    assert!(snap >= applied - COMPACT_EVERY - COMPACT_KEEP - 2, "日志被压缩了：压缩点 {snap} 跟得上 applied {applied}");
    assert!(snap <= durable - COMPACT_KEEP, "压缩点 ≤ durable_applied − 10 000（给慢 follower 留余量）");
    assert!(last - snap <= COMPACT_EVERY + COMPACT_KEEP + 2, "日志里最多留约 2 万条：({snap}, {last}]");
    let (hs, a, sm) = s.raft_state();
    assert_eq!(sm.index, snap, "压缩点（snap_meta）在 raft_state 里");
    assert!(hs.commit >= a.index && a.index >= sm.index, "坑 P8：hs.commit ≥ applied ≥ 压缩点");

    let mut c = s.crash();
    c.until_ready(); // raft-rs 的 load_state 检查 hs.commit ≥ first_index − 1：不 fatal
    let kv = c.kv(T_DEBUG);
    assert!(kv.len() as u64 >= N - 1, "掉电只可能丢最后一个批次的第二事务（重放后补回）：{}", kv.len());
    assert_eq!(kv.get(&dkey("c000000")), Some(&0u64.to_le_bytes().to_vec()), "早已被压缩掉的条目写的数据还在 KV 里");
    assert!(c.driver.raw().store().snap_index() >= snap, "重启后从压缩点继续");
}
