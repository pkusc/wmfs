//! CRAQ 组件测试（续，design.md 9.3）：M9 审查发现的边界情况，每个都有回归。3 个数据服务经 mem 传输互连，集群图由测试下发。
//! - 同步期间写到中间的 Syncing 成员（链 [L, S1(Y), S2(Y)]）：S1 收到后继的确认、本地提交了的新 chunk 要进本轮的 touched，不能被
//!   第 3 步当成"源端没有的"删掉（审查前丢了约六成）；
//! - 新的 Syncing 成员先于同步源换上新图：等 0.8 × target_lease 才开始第一轮（同步源那时还当自己是尾，本地提交的写不会转发过来）；
//! - 3.4.2.6 规则 2 之后前驱重新驱动同一个版本：新 L 要等后面的 Syncing 成员确认了才回（I3、附录 C 的 C-9）；
//! - I5：租约没续上、过期、本地集群图比心跳回复里的旧、boot_id 不符 → 读与转发都回 `FENCED`；后继回 `FENCED` 时前驱约 0.5 s 后就重发；
//! - head 本地提交失败（后继已提交）：pending 留着、版本号不让出（I6）；
//! - DeleteEpoch 沿链传播到每个成员（3.4.2.9）；
//! - 多块 chunk：部分覆盖（没改动的块从已提交版本复制）、截断到块中间（变成洞的块随增量转发一次就成，不等 3 s 的重新驱动）、再写。
//!
//! 不用 fail point（进程级），所以这些用例可以在同一个进程里并行跑；计数器是全进程共用的，只按时间与内容断言。

use std::time::{Duration, Instant};

use wmfs_core::base::{ErrCode, clock, lock};
use wmfs_core::hooks::traits::TargetStore;
use wmfs_core::meta::cluster::chains::{SERVING, SYNCING};
use wmfs_core::proto::data::DataOp;
use wmfs_core::proto::peer::{ForwardWrite, PeerOp};
use wmfs_testkit::harness::craq::{CHUNK, Craq, chain, data_call, id, peer_call};
use wmfs_testkit::harness::data::{EPOCH, INO};

const S: u8 = SERVING;
const Y: u8 = SYNCING;

fn bytes(seed: u8, n: usize) -> Vec<u8> {
    (0..n).map(|i| (i as u8).wrapping_mul(29).wrapping_add(seed)).collect()
}

fn wait_sync_done(c: &Craq, n: u32, ver: u64) {
    let t0 = Instant::now();
    while !lock(&c.members[n as usize - 1].node.sync_done).contains(&(1, n, ver)) {
        assert!(t0.elapsed() < Duration::from_secs(30), "node{n} 30 s 内没有同步完");
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn a_middle_syncing_member_keeps_chunks_written_during_its_sync() {
    let c = Craq::start(3, &[(1, S)]);
    for i in 0..200 {
        c.write(i, 0, &bytes(i as u8, CHUNK as usize)).unwrap();
    }
    c.set_chains(&[(1, chain(&[(1, S), (2, Y), (3, Y)], 2, 1))]);
    let mut next = 1000;
    while !lock(&c.members[1].node.sync_done).contains(&(1, 2, 2)) {
        c.write(next, 0, &bytes(next as u8, 4096)).unwrap(); // 同步期间的新 chunk：经过 node2 转发给 node3
        next += 1;
        assert!(next < 100_000, "node2 一直没有同步完");
    }
    c.set_chains(&[(1, chain(&[(1, S), (2, S), (3, Y)], 3, 1))]);
    wait_sync_done(&c, 3, 3);
    c.set_chains(&[(1, chain(&[(1, S), (2, S), (3, S)], 4, 1))]);
    let v = c.check(Duration::from_secs(10));
    assert_eq!(v.len(), 200 + (next - 1000) as usize, "同步期间写的 {} 个新 chunk 一个也没丢", next - 1000);
    assert_eq!(c.read(next - 1, 4096).unwrap(), bytes((next - 1) as u8, 4096));
}

#[test]
fn a_new_member_waits_before_its_first_sync_round() {
    let c = Craq::start(3, &[(1, S)]);
    c.write(0, 0, b"v1").unwrap();
    let m = c.new_map(&[(1, chain(&[(1, S), (3, Y)], 2, 1))]);
    c.deliver(&m, &[3]); // 新的 Syncing 成员先换上新图；node1（同步源）与客户端还在旧图上
    std::thread::sleep(Duration::from_millis(300)); // 比同步线程的轮询（200 ms）久、比 0.8 × target_lease（800 ms）短
    c.write(0, 0, b"v2").unwrap(); // node1 还当自己是尾：本地提交，不转发
    c.write(1, 0, b"new").unwrap();
    c.deliver(&m, &[1]); // 0.8 × 租约之内 node1 换上新图（真集群里换不上就按 I5 自我隔离）
    c.set_client_map(m);
    wait_sync_done(&c, 3, 2);
    c.set_chains(&[(1, chain(&[(1, S), (3, S)], 3, 1))]);
    let v = c.check(Duration::from_secs(5));
    assert_eq!(v.len(), 2, "新成员第一轮取到的列表里已经有 node1 换图之前提交的写");
    assert_eq!(&c.read(0, 2).unwrap(), b"v2");
}

/// 附录 C 的 C-9：[1, 2, 9] 里 9 降为 Syncing（还连不上）→ node2 成了新 L、本地提交（规则 2）；node1 重新驱动同一个版本时（链版本
/// 变了：换图之后的下一拍，之后每 3 s）node2 不能因为"已提交"马上确认——9 还没有它，写者不能收到确认（I3）。9 被移除之后 node2 成了尾
/// （规则 3），写才成功。
#[test]
fn a_redrive_after_rule_2_waits_for_the_syncing_member() {
    let c = Craq::start(3, &[(1, S), (2, S)]);
    c.set_chains(&[(1, chain(&[(1, S), (2, S), (9, S)], 2, 1))]); // target 9 不存在：转发给它一直失败
    std::thread::scope(|sc| {
        let w = sc.spawn(|| c.write(0, 0, b"c9"));
        let t0 = Instant::now();
        while c.members[1].node.chunks.pendings(|_, _| true).is_empty() {
            assert!(t0.elapsed() < Duration::from_secs(5), "node2 上应有 pending");
            std::thread::sleep(Duration::from_millis(5));
        }
        c.set_chains(&[(1, chain(&[(1, S), (2, S), (9, Y)], 3, 1))]);
        assert!(c.committed_version(2, 0).is_some(), "规则 2：新 L 本地提交");
        std::thread::sleep(Duration::from_millis(4000)); // node1 重新驱动过（换图之后的下一拍，再加 3 s 一次）
        assert_eq!(c.committed_version(1, 0), None, "head 还不能提交：后面的 Syncing 成员没确认");
        assert!(!w.is_finished(), "写者还不能收到确认");
        c.set_chains(&[(1, chain(&[(1, S), (2, S)], 4, 1))]);
        assert!(w.join().unwrap().is_ok());
    });
    assert_eq!(&c.read(0, 2).unwrap(), b"c9");
    c.check(Duration::from_secs(5));
}

/// I5（3.4.2.2）：hb 线程打开租约之后，第一次心跳成功之前、租约过期、本地集群图比心跳回复里的旧、集群图里的 boot_id 不是本进程的，
/// 数据服务（读）与后继的转发处理都回 `FENCED`。
#[test]
fn i5_fences_reads_and_forwards() {
    let c = Craq::start(3, &[(1, S), (2, S), (3, S)]);
    c.write(0, 0, b"x").unwrap();
    c.set_boot(1, 7);
    c.set_boot(2, 5);
    c.set_chains(&[(1, chain(&[(1, S), (2, S), (3, S)], 2, 1))]);
    let read = |n: u32| data_call(&c, n, DataOp::ReadChunk { chunk: id(0), chain_ver: 2, off: 0, len: 1 });
    let fenced = |r: Result<_, (ErrCode, u64)>| r.err().map(|e| e.0) == Some(ErrCode::FENCED);
    assert!(read(1).is_ok(), "没打开租约（没有 hb 线程）：不核对");
    let n1 = &c.members[0].node;
    n1.lease.enforce(7);
    assert!(fenced(read(1)), "第一次心跳成功之前");
    let epoch = n1.map.epoch();
    n1.lease.renew(clock::mono_ms(), 100, epoch);
    assert!(read(1).is_ok(), "续上了");
    std::thread::sleep(Duration::from_millis(150));
    assert!(fenced(read(1)), "过了 0.8 × 租约：自停");
    n1.lease.renew(clock::mono_ms(), 8000, epoch + 1);
    assert!(fenced(read(1)), "心跳回复里的 map_epoch 更新：先拉到新集群图");
    n1.lease.renew(clock::mono_ms(), 8000, epoch);
    assert!(read(1).is_ok());
    n1.lease.enforce(8);
    assert!(fenced(read(1)), "集群图里的 boot_id 不是本进程的（刚重启、还没被确认）");
    c.members[1].node.lease.enforce(5);
    let fw = ForwardWrite {
        chunk: id(0),
        chain_ver: 2,
        from_target: 1,
        base_version: 0,
        version: 9,
        full: false,
        header: vec![],
        blocks: vec![],
        sync: false,
    };
    let r = peer_call(&c, 2, &PeerOp::ForwardWrite(fw), vec![]);
    assert_eq!(r.err().map(|e| e.0), Some(ErrCode::FENCED), "后继处理转发之前也核对 I5");
}

/// 后继已经确认了 V、head 本地提交 V 失败（rename 之前的 fdatasync 出错）：不能删掉 pending、让出这个版本号——否则下一个写重新分配到 V，
/// 后继按"committed ≥ V → 重复"直接确认，同版本不同内容（I6）。pending 留着、写者收到 RETRY 后重试（排在它后面）；chain-timer 重新
/// 驱动时再试提交。
#[test]
fn a_failed_local_commit_on_the_head_keeps_its_version() {
    let c = Craq::start(3, &[(1, S), (2, S), (3, S)]);
    c.write(0, 0, b"first").unwrap();
    c.members[0].store.fail_commits(1);
    assert!(c.write(0, 0, b"second").is_ok(), "写者重试之后成功");
    c.write(0, 0, b"third").unwrap();
    assert_eq!(&c.read(0, 5).unwrap(), b"third");
    c.check(Duration::from_secs(5)); // 各成员同版本同内容
}

/// 后继还没被确认（I5：集群图里还没有它的 boot_id——刚起来的节点）时回 `FENCED`：它很快就能收，前驱约 0.5 s 后就重发，不等满 3 s
/// （审查后：以前只有 `STALE_CHAIN` 走快速重发，刚建好的集群上第一批写要等 3 s 的重新驱动）。
#[test]
fn a_fenced_successor_is_retried_soon() {
    let c = Craq::start(3, &[(1, S), (2, S), (3, S)]);
    c.write(0, 0, b"warm").unwrap();
    let n3 = &c.members[2].node;
    n3.lease.enforce(9); // 集群图里 target 3 的 boot_id 还是 0：自我隔离
    n3.lease.renew(clock::mono_ms(), 60_000, 0);
    let t0 = Instant::now();
    std::thread::scope(|sc| {
        let w = sc.spawn(|| c.write(0, 0, b"next"));
        std::thread::sleep(Duration::from_millis(300));
        c.set_boot(3, 9);
        c.set_chains(&[(1, chain(&[(1, S), (2, S), (3, S)], 1, 1))]); // 链没变，集群图里有了它的 boot_id
        assert!(w.join().unwrap().is_ok());
    });
    assert!(t0.elapsed() < Duration::from_millis(2500), "不等满 3 s 的重新驱动：{:?}", t0.elapsed());
    assert_eq!(&c.read(0, 4).unwrap(), b"next");
}

#[test]
fn delete_epoch_reaches_every_member() {
    let c = Craq::start(3, &[(1, S), (2, S), (3, S)]);
    for i in 0..4 {
        c.write(i, 0, b"to be deleted").unwrap();
    }
    assert_eq!(c.client.delete_epoch(&c.admin_cap(), 1, INO, EPOCH), Ok(4), "head 删了 4 个文件");
    for (i, m) in c.members.iter().enumerate() {
        assert!(m.store.list_chain(1, None, usize::MAX).unwrap().is_empty(), "node{} 上也删掉了", i + 1);
    }
}

/// 256 KiB 的 chunk = 4 个 64 KiB 的块：部分覆盖只转发改动的块；截断到第 1 块中间时第 2、3 块变成洞（增量转发带着这些条目，没有
/// 存储字节），边界块重新编码——一次转发就成，不等 3 s 的重新驱动；之后再写、各成员一致（附录 C 的 C-8"截断写新版本"）。
#[test]
fn partial_overwrites_and_truncates_of_multi_block_chunks_forward_incrementally() {
    const CS: u32 = 256 << 10;
    let c = Craq::start_with(3, &[(1, S), (2, S), (3, S)], CS);
    let mut want = bytes(3, CS as usize);
    c.write(0, 0, &want).unwrap();
    c.write(0, 70_000, b"patch").unwrap();
    want[70_000..70_005].copy_from_slice(b"patch");
    let t0 = Instant::now();
    c.truncate(0, 100_000).unwrap();
    assert!(t0.elapsed() < Duration::from_secs(2), "变成洞的块随增量转发一次就成：{:?}", t0.elapsed());
    want[100_000..].fill(0);
    c.write(0, 200_000, b"after").unwrap();
    want[200_000..200_005].copy_from_slice(b"after");
    assert_eq!(c.read(0, CS).unwrap(), want);
    let v = c.check(Duration::from_secs(5));
    assert_eq!(v[&id(0)], (1 << 40) + 3, "4 个版本");
}
