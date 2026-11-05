//! ADR-0040 第 11 条（M10 第一步，3.4.2.5 的 M9 审查注记）：Syncing 的尾 S 被提升为 last serving 的那一刻，旧的 L 还在旧图上、还只有
//! V 的 pending——S 在物理链尾，已经提交了 V。组件测试里"拖住 L 换图"就是只把新图交给 S（多进程的开发集群里用 fail point
//! `chain.map.delay`，Jepsen-lite 的"提升"故障）；`chain.ack.drop` 丢掉 S 的确认，让 L 一直留着 pending。修之前 S 回 V、L 回旧版本：
//! 一个读者先读到新的、另一个读者之后读到旧的。现在 S 一个 target 租约内对读回可重试的 `BUSY`（组件测试的集群图里租约是 1 s），L 照旧
//! 回旧版本；租约过了 S 照常服务读。fail point 是进程级的：单独一个测试程序（里面的两个用例一个一个跑）。
//!
//! 第二个用例（ADR-0041 第 13 条）：要移出的 L 后面还有 Syncing 成员时，它原地降为 Syncing、不移到末尾——head 的重发仍经过它，它换上
//! 新图之前写确认不了。

use std::sync::Mutex;
use std::time::{Duration, Instant};

use wmfs_core::api::failpoint;
use wmfs_core::base::{ErrCode, clock, lock};
use wmfs_core::data::redrive::REDRIVE_MS;
use wmfs_core::meta::cluster::chains::{SERVING, SYNCING};
use wmfs_core::proto::data::{DataOp, DataReply};
use wmfs_testkit::harness::craq::{Craq, chain, data_call, id};

const S: u8 = SERVING;
const Y: u8 = SYNCING;

/// fail point 是进程级的：两个用例一个一个跑。
static ONE_AT_A_TIME: Mutex<()> = Mutex::new(());

/// 直接问节点 `n` 读 chunk 0（链版本 `ver`）：回的已提交版本号，或错误码。
fn version_at(c: &Craq, n: u32, ver: u64) -> Result<u64, ErrCode> {
    match data_call(c, n, DataOp::ReadChunk { chunk: id(0), chain_ver: ver, off: 0, len: 3 }) {
        Ok(DataReply::Read { version, .. }) => Ok(version),
        Ok(_) => Err(ErrCode::INTERNAL),
        Err((e, _)) => Err(e),
    }
}

#[test]
fn a_tail_promoted_to_last_serving_answers_busy_for_one_lease() {
    let _one = lock(&ONE_AT_A_TIME);
    let c = Craq::start(2, &[(1, S), (2, Y)]);
    c.write(0, 0, b"old").unwrap();
    let old = c.committed_version(1, 0).unwrap();
    failpoint::set("chain.ack.drop", "times(1,return(EIO))").unwrap();
    std::thread::scope(|sc| {
        let w = sc.spawn(|| c.write(0, 0, b"new"));
        let t0 = Instant::now();
        while c.committed_version(2, 0) == Some(old) || !c.members[0].node.chunks.has_dirty(&id(0)) {
            assert!(t0.elapsed() < Duration::from_secs(2), "S（物理链尾）应先提交新版本、L 留着 pending");
            std::thread::sleep(Duration::from_millis(2));
        }
        let new = c.committed_version(2, 0).unwrap();
        let m = c.new_map(&[(1, chain(&[(1, S), (2, S)], 2, 1))]);
        c.deliver(&m, &[2]); // S 提升、换上新图；L 还在旧图上
        assert_eq!(version_at(&c, 2, 2), Err(ErrCode::BUSY), "S 刚被提升为 last serving：一个租约内读回 BUSY");
        assert_eq!(version_at(&c, 1, 1), Ok(old), "L 还在旧图上、只有 pending：回旧版本（不会先新后旧）");
        c.deliver(&m, &[1]);
        c.set_client_map(m);
        assert!(w.join().unwrap().is_ok(), "确认丢了也只是 3 s 后重新驱动：写成功");
        let t0 = Instant::now();
        while version_at(&c, 2, 2) == Err(ErrCode::BUSY) {
            assert!(t0.elapsed() < Duration::from_secs(3), "一个租约（1 s）之后 S 照常服务读");
            std::thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(version_at(&c, 2, 2), Ok(new));
    });
    failpoint::clear("chain.ack.drop");
    assert_eq!(&c.read(0, 3).unwrap(), b"new");
    c.check(Duration::from_secs(5));
}

/// 链 [1, 2(L), 3(Y)]：t3（物理链尾）已提交 v2、给 t2 的确认被丢，t2 还只有 pending。第一步原地降（[1, 2(Y), 3(Y)]）的图只交给 1、3：
/// head 的重发仍经过还在旧图上的 t2（回 STALE_CHAIN），写者拿不到确认，t2 这期间按旧图回 v1 是合法的。移到末尾（[1, 3(Y), 2(Y)]）的话，
/// t3 对重发回"重复确认"，写者拿到确认，而 t2 还自认是 L、回 v1——先新后旧。t2 换上新图之后写照常确认。
#[test]
fn a_last_serving_member_demoted_in_place_holds_the_ack_until_it_switches() {
    let _one = lock(&ONE_AT_A_TIME);
    let c = Craq::start(3, &[(1, S), (2, S), (3, Y)]);
    // 换图时 head 让 chain-timer 下一拍就重发（last_sent = 0），这要单调时钟读数过了 3 s 才生效（读数从 1000 起）：先等到那之后，
    // 下面的"写确认不了"才不是因为 head 还没重发
    while clock::mono_ms() < REDRIVE_MS + 1000 {
        std::thread::sleep(Duration::from_millis(20));
    }
    c.write(0, 0, b"v1").unwrap();
    let old = c.committed_version(2, 0).unwrap();
    failpoint::set("chain.ack.drop", "times(1,return(EIO))").unwrap();
    std::thread::scope(|sc| {
        let w = sc.spawn(|| c.write(0, 0, b"v2"));
        let t0 = Instant::now();
        while c.committed_version(3, 0) == Some(old) || !c.members[1].node.chunks.has_dirty(&id(0)) {
            assert!(t0.elapsed() < Duration::from_secs(2), "t3 应先提交新版本、t2 留着 pending");
            std::thread::sleep(Duration::from_millis(2));
        }
        let e1 = c.new_map(&[(1, chain(&[(1, S), (2, Y), (3, Y)], 2, 1))]);
        c.deliver(&e1, &[1, 3]); // t2 还在旧图上
        let t0 = Instant::now();
        while t0.elapsed() < Duration::from_millis(1500) {
            assert!(!w.is_finished(), "t2 换上新图之前：写者拿不到确认");
            assert_eq!(version_at(&c, 2, 1), Ok(old), "t2（旧图里的 L）回 v1：写还没确认，合法");
            std::thread::sleep(Duration::from_millis(20));
        }
        c.deliver(&e1, &[2]);
        c.set_client_map(e1);
        assert!(w.join().unwrap().is_ok(), "t2 换上新图之后照常确认");
    });
    failpoint::clear("chain.ack.drop");
    c.set_chains(&[(1, chain(&[(1, S), (3, Y)], 3, 1))]); // 第二步：移出 t2
    assert_eq!(&c.read(0, 2).unwrap(), b"v2");
}
