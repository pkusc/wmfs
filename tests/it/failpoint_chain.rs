//! `chain.*` fail point（design.md 9.4、3.11）：3 个数据服务经 mem 传输互连（`harness::craq`）。fail point 是进程级的，所以全部场景在
//! 一个用例里按顺序跑：
//! - `chain.forward.drop`：head 的转发丢了 → head 留着 pending；这时读 head 得到 `CHUNK_DIRTY`，客户端改问 last serving、读到旧版本
//!   （CRAQ，3.4.2.5）；chain-timer 3 s 后完整重发，写成功，各成员一致；
//! - `chain.ack.drop`：确认丢了 → 3 s 后重新驱动，后继回重复的 `Ack{committed}`，写成功；
//! - `chain.forward.delay` 期间链变了（后继被移除）：按 3.4.2.6 规则 4 把 pending 完整重发给新后继，不等 chain-timer；
//! - 附录 C 的 C-7：一直丢转发时，多于 io 线程数的等确认的写也不占住线程（转发不占线程：`call_cb` + pending 表）；
//! - `chain.head.crash_after_pending`（abort）在多进程的 `scripts/accept.d/m9.sh` 里（要真的杀掉进程再起来）。

use std::time::{Duration, Instant};

use wmfs_core::api::failpoint;
use wmfs_core::meta::cluster::chains::SERVING;
use wmfs_testkit::harness::craq::{CHUNK, Craq, chain, id};

const S: u8 = SERVING;

#[test]
fn chain_fail_points_are_rolled_forward() {
    let c = Craq::start(3, &[(1, S), (2, S), (3, S)]);
    c.write(0, 0, b"old").unwrap();

    // chain.forward.drop：脏读重定向 + 3 s 后重新驱动
    failpoint::set("chain.forward.drop", "times(1,return(EIO))").unwrap();
    let t0 = Instant::now();
    let w = std::thread::scope(|s| {
        let h = s.spawn(|| c.write(0, 0, b"new"));
        let deadline = Instant::now() + Duration::from_secs(2);
        while !c.members[0].node.chunks.has_dirty(&id(0)) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(c.members[0].node.chunks.has_dirty(&id(0)), "转发丢了：head 留着 pending");
        assert_eq!(&c.read(0, 3).unwrap(), b"old", "head 有未提交版本：读被重定向到 last serving，读到旧的已提交版本");
        assert!(c.members[0].node.craq.dirty_reads.get() > 0, "CHUNK_DIRTY 重定向过");
        h.join().unwrap()
    });
    assert!(w.is_ok(), "{w:?}");
    assert!(t0.elapsed() >= Duration::from_millis(2500), "等 chain-timer（3 s 未确认）重新驱动");
    assert!(c.members[0].node.craq.redrive.get() > 0);
    assert_eq!(&c.read(0, 3).unwrap(), b"new");
    c.check(Duration::from_secs(5));

    // chain.ack.drop：确认丢了 → 重新驱动 → 重复的确认
    failpoint::set("chain.ack.drop", "times(1,return(EIO))").unwrap();
    let t0 = Instant::now();
    c.write(1, 0, &[7u8; CHUNK as usize]).unwrap();
    assert!(t0.elapsed() >= Duration::from_millis(2500), "等了重新驱动");
    assert_eq!(c.read(1, CHUNK).unwrap(), vec![7u8; CHUNK as usize]);
    c.check(Duration::from_secs(5));

    // chain.forward.delay 期间后继被移除：规则 4（后继变了 → 完整重发给新后继），不用等 3 s
    failpoint::set("chain.forward.delay", "times(1,sleep(1500))").unwrap();
    let t0 = Instant::now();
    std::thread::scope(|s| {
        let h = s.spawn(|| c.write(2, 0, b"moved"));
        while !c.members[0].node.chunks.has_dirty(&id(2)) {
            std::thread::sleep(Duration::from_millis(5));
        }
        c.set_chains(&[(1, chain(&[(1, S), (3, S)], 2, 1))]); // target 2 被移出链（离线）
        assert!(h.join().unwrap().is_ok());
    });
    assert!(t0.elapsed() < Duration::from_millis(2900), "规则 4 马上重发，没有等 chain-timer：{:?}", t0.elapsed());
    assert_eq!(&c.read(2, 5).unwrap(), b"moved");
    let v = c.check(Duration::from_secs(5));
    assert_eq!(v.len(), 3);
    failpoint::clear("chain.forward.delay");

    // 附录 C 的 C-7：转发不占线程——后继一直收不到转发时，head 上 24 个等确认的写（多于 16 个 io 线程）也不占住线程，别的读照样马上回
    failpoint::set("chain.forward.drop", "return(EIO)").unwrap();
    let c = &c;
    std::thread::scope(|s| {
        let hs: Vec<_> = (10..34).map(|i| s.spawn(move || c.write(i, 0, b"held"))).collect();
        let t0 = Instant::now();
        while c.members[0].node.chunks.pendings(|_, _| true).len() < 24 {
            assert!(t0.elapsed() < Duration::from_secs(10), "24 个写都该在 head 上登记成 pending");
            std::thread::sleep(Duration::from_millis(10));
        }
        let t0 = Instant::now();
        assert_eq!(&c.read(1, 1).unwrap(), &[7u8], "io 线程没有被等确认的转发占住");
        assert!(t0.elapsed() < Duration::from_secs(1), "读马上回：{:?}", t0.elapsed());
        failpoint::clear("chain.forward.drop");
        for h in hs {
            assert!(h.join().unwrap().is_ok(), "放开之后 chain-timer 前滚，写都成功");
        }
    });
    c.check(Duration::from_secs(10));
}
