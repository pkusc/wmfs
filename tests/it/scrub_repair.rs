//! scrub 的检查与修复（design.md 3.2.5 R5、4.12 的 scrub、ADR-0040 第 10 条；M11）。前几个是 CRAQ 组件测试（3 个数据服务经 mem 传输
//! 互连、集群图由测试下发，直接调 `data::repair` 的检查与修复——scrub 插件的一步就是它们）：
//! - 一个副本的数据块坏了 / 头坏了 → 查出来、从别的 Serving 成员修好，各成员同版本同内容；
//! - 每个副本都坏了（块坏、头坏）→ 修不了（`Lost`，如实上报数据丢失），不拿坏的副本"修"；对方有在途的版本 → 稍后；
//!   取回数据的途中本地变了（提交了更新的版本、有了 pending、头坏的又读得出了）→ 不装（不拿旧版本把它换回去）；
//! - R5 的写路径：head 上的副本用不了（头坏了、要保留的块坏了）→ 先从别的成员修好再写（客户端只多重试几次），各成员一致；都坏了 →
//!   部分写 `EIO`、整 chunk 覆盖照常；头读不出又没有可以先修复的来源：链上只有自己 → 照常写，有 Syncing 的后继 → `EIO`
//!   （ADR-0042 第 6 条）；
//! - ADR-0040 第 10 条 ②：同步源（L）上的坏 chunk 让重同步做不完，L 的 scrub 修好它之后重同步自然完成，新成员拿到的是好的；
//! - ADR-0040 第 10 条 ③（"Serving 成员缺基础版本"的大范围检查）：Serving 成员缺了 L 有的 chunk、或比 L 旧 → `missing_from_last` 找出来、修好；
//! - ADR-0040 第 10 条 ①：本地提交连续失败 → target 只读（写回可重试的 `NOT_SERVING`、读照常）；只读的 target 当后继时转发回
//!   `NOT_SERVING`、不写盘，链管理器把它移出之后写照常；当 Syncing 成员时不同步。
//!
//! 最后一个在完整的单节点上跑 scrub 插件本身：唯一的副本坏了 → `wmfs task run scrub` → changelog `DataLossDetected`（extra 的字段名
//! 与次序是金样）。
//! 不用 fail point（进程级），这些用例可以在同一个进程里并行跑。

use std::sync::Arc;
use std::time::{Duration, Instant};

use wmfs_core::base::types::ChunkId;
use wmfs_core::base::{ErrCode, lock};
use wmfs_core::buf::{Buf, MemPool, PoolConfig};
use wmfs_core::data::repair::{self, Repair, Verdict};
use wmfs_core::data::{FAILED_COMMITS, Target, packed};
use wmfs_core::hooks::traits::TargetStore;
use wmfs_core::meta::cluster::chains::{SERVING, SYNCING};
use wmfs_core::meta::schema::keys::ROOT_INO;
use wmfs_core::proto::peer::{ForwardWrite, PeerOp};
use wmfs_plugins::local_fs::committed_path;
use wmfs_testkit::harness::bg::{api, extra_keys, has_event, task};
use wmfs_testkit::harness::craq::{CHUNK, Craq, chain, id, inject_pending, peer_call};
use wmfs_testkit::harness::data::{node_on, read_at, write_at};
use wmfs_testkit::harness::mem_target::MemTargetStore;
use wmfs_testkit::harness::node::{TempDir, client, get, init, put, set_full_bg, start, target_dir, wait_for, who};

const S: u8 = SERVING;
const Y: u8 = SYNCING;
/// 固定头里 write_time_ms 的一位（头 CRC 覆盖它：翻了 → 头坏了，CORRUPT）。
const HEADER_BIT: usize = 100 * 8;

fn bytes(seed: u8, n: usize) -> Vec<u8> {
    (0..n).map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed)).collect()
}

fn target(c: &Craq, n: u32) -> &Target {
    c.members[n as usize - 1].node.targets.get(&n).unwrap()
}

fn verify(c: &Craq, n: u32, idx: u32) -> Verdict {
    repair::verify(&c.members[n as usize - 1].node, target(c, n), &id(idx)).unwrap()
}

fn repair(c: &Craq, n: u32, idx: u32, ours: Option<u64>) -> Repair {
    repair::repair(&c.members[n as usize - 1].node, target(c, n), &id(idx), ours)
}

/// 翻转节点 `n` 上 chunk `idx` 的最后一位（数据区最后一块的最后一个字节：块校验和不符）。
fn break_data(c: &Craq, n: u32, idx: u32) {
    let s = &c.members[n as usize - 1].store;
    s.flip_bit(&id(idx), s.committed_len(&id(idx)).unwrap() * 8 - 1);
}

fn same_everywhere(c: &Craq, idx: u32, members: &[u32]) {
    let want = c.members[members[0] as usize - 1].store.committed_bytes(&id(idx));
    for n in members {
        assert!(c.members[*n as usize - 1].store.committed_bytes(&id(idx)) == want, "node{n} 上 chunk {idx} 不同");
    }
}

fn sync_done(c: &Craq, n: u32, ver: u64) -> bool {
    lock(&c.members[n as usize - 1].node.sync_done).contains(&(1, n, ver))
}

#[test]
fn a_bad_replica_is_found_and_repaired_from_another_serving_member() {
    let c = Craq::start(3, &[(1, S), (2, S), (3, S)]);
    let data = bytes(1, CHUNK as usize);
    c.write(0, 0, &data).unwrap();
    c.check(Duration::from_secs(5));
    let v = c.committed_version(1, 0).unwrap();
    assert!(matches!(verify(&c, 2, 0), Verdict::Good { version, .. } if version == v));

    break_data(&c, 2, 0); // 数据块坏了：头还读得出来，知道版本
    assert_eq!(verify(&c, 2, 0), Verdict::Bad { version: Some(v) });
    assert!(matches!(repair(&c, 2, 0, Some(v)), Repair::Repaired { from: 1 }));
    assert!(matches!(verify(&c, 2, 0), Verdict::Good { version, .. } if version == v));

    c.members[2].store.flip_bit(&id(0), HEADER_BIT); // 头坏了：不知道版本
    assert_eq!(verify(&c, 3, 0), Verdict::Bad { version: None });
    assert!(matches!(repair(&c, 3, 0, None), Repair::Repaired { .. }));
    assert_eq!(verify(&c, 3, 0), Verdict::Good { version: v, bytes: CHUNK as u64 });
    c.check(Duration::from_secs(5)); // 各 Serving 成员同版本同内容
    assert_eq!(c.read(0, CHUNK).unwrap(), data);
    assert_eq!(verify(&c, 1, 7), Verdict::Missing, "没写过的 chunk");
}

#[test]
fn when_every_replica_is_bad_nothing_is_repaired_and_the_loss_is_reported() {
    let c = Craq::start(3, &[(1, S), (2, S), (3, S)]);
    c.write(0, 0, &bytes(2, CHUNK as usize)).unwrap();
    c.check(Duration::from_secs(5));
    let v = c.committed_version(1, 0).unwrap();
    for n in 1..=3 {
        break_data(&c, n, 0);
    }
    let before = c.members[0].store.committed_bytes(&id(0));
    assert_eq!(repair(&c, 1, 0, Some(v)), Repair::Lost, "别的副本也都是坏的：数据丢失");
    assert!(c.members[0].store.committed_bytes(&id(0)) == before, "没有拿坏的副本来\"修\"");
    assert!(c.read(0, 16).is_err(), "读它照旧报错，不返回坏数据");
    let single = Craq::start(1, &[(1, S)]);
    single.write(0, 0, b"only copy").unwrap();
    break_data(&single, 1, 0);
    assert_eq!(repair(&single, 1, 0, None), Repair::Lost, "链上没有别的成员");
    let heads = Craq::start(3, &[(1, S), (2, S), (3, S)]);
    heads.write(0, 0, b"three copies").unwrap();
    heads.check(Duration::from_secs(5));
    for m in &heads.members {
        m.store.flip_bit(&id(0), HEADER_BIT);
    }
    assert_eq!(repair(&heads, 1, 0, None), Repair::Lost, "别的副本的头也读不出（FetchChunk 回 CORRUPT）：也算坏的");
}

/// 一份已提交的 chunk 文件的 packed 形式（像 FetchChunk 取回的）。
fn packed_form(file: &[u8]) -> Buf {
    let mut at = |off: u64, b: &mut [u8]| {
        let rest = file.get(off as usize..).unwrap_or_default();
        let n = b.len().min(rest.len());
        b[..n].copy_from_slice(&rest[..n]);
        Ok(n)
    };
    packed::pack(&MemPool::new(&PoolConfig::small()).unwrap(), &mut at).unwrap().0
}

/// 修复的"稍后"：对方有在途的版本（ListChunks 报的比它已提交的新）；取回数据之后、装之前本地变了（不拿取回的旧版本把它换回去）——
/// 本地提交了更新的版本、本地有了 pending、修复开始时头读不出（ours = None）的现在又读得出了。
#[test]
fn repair_waits_for_in_flight_versions_and_never_installs_an_older_one() {
    let c = Craq::start(2, &[(1, S), (2, S)]);
    c.write(0, 0, &bytes(6, CHUNK as usize)).unwrap();
    c.check(Duration::from_secs(5));
    let v = c.committed_version(1, 0).unwrap();
    break_data(&c, 2, 0);
    inject_pending(&c, 1, 0, &bytes(7, CHUNK as usize), 2); // head 上有一个在途的 v+1
    assert_eq!(repair(&c, 2, 0, Some(v)), Repair::Later(ErrCode::BUSY), "对方有在途的版本：稍后");

    let c = Craq::start(2, &[(1, S), (2, S)]);
    c.write(0, 0, &bytes(8, CHUNK as usize)).unwrap();
    c.check(Duration::from_secs(5));
    let v = c.committed_version(2, 0).unwrap();
    let fetched = packed_form(&c.members[0].store.committed_bytes(&id(0)).unwrap());
    let newer = bytes(9, CHUNK as usize);
    c.write(0, 0, &newer).unwrap(); // 取回之后、装之前：转发在本地提交了 v+1
    let (n2, t2) = (&c.members[1].node, target(&c, 2));
    let r = repair::install(n2, t2, &id(0), fetched.as_slice(), Some(v));
    assert_eq!(r, Err(ErrCode::BUSY), "本地已经不是修复开始时的版本：不装");
    assert_eq!(c.committed_version(2, 0), Some(v + 1), "v+1 还在");

    let c = Craq::start(2, &[(1, S), (2, S)]);
    c.write(0, 0, &bytes(17, CHUNK as usize)).unwrap();
    c.check(Duration::from_secs(5));
    let file = c.members[1].store.committed_bytes(&id(0)).unwrap();
    let v = c.committed_version(2, 0).unwrap();
    let (n2, t2) = (&c.members[1].node, target(&c, 2));
    inject_pending(&c, 2, 0, &bytes(18, CHUNK as usize), 1); // 本地有一个还没确认的版本
    let r = repair::install(n2, t2, &id(0), packed_form(&file).as_slice(), Some(v));
    assert_eq!(r, Err(ErrCode::BUSY), "本地有 pending：不装");
    let c = Craq::start(2, &[(1, S), (2, S)]);
    c.write(0, 0, &bytes(19, CHUNK as usize)).unwrap();
    c.check(Duration::from_secs(5));
    let file = c.members[1].store.committed_bytes(&id(0)).unwrap();
    c.members[1].store.flip_bit(&id(0), HEADER_BIT); // 修复开始时本地的头读不出（ours = None）
    c.members[1].store.put_committed(&id(0), file.clone()); // 取回的途中本地又有了读得出的版本
    let (n2, t2) = (&c.members[1].node, target(&c, 2));
    let r = repair::install(n2, t2, &id(0), packed_form(&file).as_slice(), None);
    assert_eq!(r, Err(ErrCode::BUSY), "头坏的现在读得出了：不装");
}

/// 头读不出、又没有可以先修复的 Serving 成员（ADR-0042 第 6 条）：链上只有自己 → 整 chunk 覆盖照常写（版本号 `max(1, incarnation << 40)`：
/// 没有别的副本会与它冲突）；有后继、却都在 Syncing → `EIO`，不猜版本号（猜小了，后继当它是重复的、不落盘，确认过的写就丢了）。
#[test]
fn a_full_write_on_an_unreadable_head_without_a_repair_source() {
    let alone = Craq::start(1, &[(1, S)]);
    alone.write(0, 0, &bytes(13, CHUNK as usize)).unwrap();
    alone.write(0, 0, &bytes(14, CHUNK as usize)).unwrap();
    alone.members[0].store.flip_bit(&id(0), HEADER_BIT);
    let full = bytes(15, CHUNK as usize);
    assert_eq!(alone.write(0, 0, &full), Ok(1 << 40), "链上只有自己：照常写，版本号 incarnation << 40");
    assert_eq!(alone.read(0, CHUNK).unwrap(), full);
    let c = Craq::start(2, &[(1, S), (2, Y)]);
    c.write(0, 0, &bytes(16, CHUNK as usize)).unwrap();
    let theirs = c.members[1].store.committed_bytes(&id(0));
    assert!(theirs.is_some(), "Syncing 的后继收到了转发");
    c.members[0].store.flip_bit(&id(0), HEADER_BIT);
    assert_eq!(c.write(0, 0, &bytes(17, CHUNK as usize)), Err(ErrCode::EIO), "有后继、没有可以先修复的来源：EIO");
    assert!(c.members[1].store.committed_bytes(&id(0)) == theirs, "后继手里的没有变");
}

/// 3.2.5 R5 的写路径：head 上本地的副本用不了时先从别的 Serving 成员修复再写（`repair` 线程；客户端看到的只是 BUSY 重试），
/// 各成员同版本同内容——尤其是头读不出的整 chunk 覆盖写，不会用一个比后继手里还小的版本号"写成功"。
#[test]
fn a_write_on_an_unusable_head_replica_repairs_it_first() {
    let c = Craq::start(3, &[(1, S), (2, S), (3, S)]);
    c.write(0, 0, &bytes(10, CHUNK as usize)).unwrap();
    c.check(Duration::from_secs(5));
    c.members[0].store.flip_bit(&id(0), HEADER_BIT); // head 的头坏了：不知道它的版本
    let full = bytes(11, CHUNK as usize);
    c.write(0, 0, &full).unwrap();
    c.check(Duration::from_secs(5));
    assert_eq!(c.read(0, CHUNK).unwrap(), full);
    break_data(&c, 1, 0); // 要保留的块坏了：部分写
    c.write(0, 0, b"patch").unwrap();
    c.check(Duration::from_secs(5));
    let mut want = full.clone();
    want[..5].copy_from_slice(b"patch");
    assert_eq!(c.read(0, CHUNK).unwrap(), want);
    for n in 1..=3 {
        break_data(&c, n, 0); // 都坏了：修不了 → 记为丢失，部分写 EIO；整 chunk 覆盖照常，之后各成员一致
    }
    assert_eq!(c.write(0, 0, b"x"), Err(ErrCode::EIO));
    let last = bytes(12, CHUNK as usize);
    c.write(0, 0, &last).unwrap();
    c.check(Duration::from_secs(5));
    assert_eq!(c.read(0, CHUNK).unwrap(), last);
}

/// ADR-0040 第 10 条：同步源上的坏 chunk——这条链的重同步做不完（不跳过它：跳过会让新成员带着缺失的版本被提升）；L 自己的 scrub
/// 修好它之后，重同步自然完成。
#[test]
fn a_bad_chunk_on_the_sync_source_holds_the_resync_until_scrub_repairs_it() {
    let c = Craq::start(3, &[(1, S), (2, S)]);
    let (d0, d1) = (bytes(3, CHUNK as usize), bytes(4, CHUNK as usize));
    c.write(0, 0, &d0).unwrap();
    c.write(1, 0, &d1).unwrap();
    c.check(Duration::from_secs(5));
    c.members[1].store.flip_bit(&id(0), HEADER_BIT); // L（target 2）的 chunk 0 头坏了
    c.set_chains(&[(1, chain(&[(1, S), (2, S), (3, Y)], 2, 1))]);
    std::thread::sleep(Duration::from_millis(3000)); // 新 Syncing 成员先等一个 target 租约（1 s），再试过一两轮
    assert!(!sync_done(&c, 3, 2), "同步源的坏 chunk 修好之前，重同步做不完");
    assert_eq!(verify(&c, 2, 0), Verdict::Bad { version: None });
    assert!(matches!(repair(&c, 2, 0, None), Repair::Repaired { from: 1 }), "L 的 scrub 从别的 Serving 成员修好");
    let t0 = Instant::now();
    while !sync_done(&c, 3, 2) {
        assert!(t0.elapsed() < Duration::from_secs(30), "修好之后 30 s 内没有同步完");
        std::thread::sleep(Duration::from_millis(20));
    }
    same_everywhere(&c, 0, &[1, 2, 3]);
    same_everywhere(&c, 1, &[1, 2, 3]);
}

/// ADR-0040 第 10 条（"Serving 成员缺基础版本"的大范围检查）：Serving 成员缺了 L 有的 chunk、或比 L 的旧 → 找出来、从别的成员修好。
#[test]
fn a_serving_member_missing_what_the_last_serving_has_is_filled_in() {
    let c = Craq::start(3, &[(1, S), (2, S), (3, S)]);
    c.write(0, 0, b"v1").unwrap();
    let old = c.members[1].store.committed_bytes(&id(0)).unwrap();
    c.write(0, 0, b"v2").unwrap();
    c.write(1, 0, b"other").unwrap();
    c.check(Duration::from_secs(5));
    let (n2, t2) = (&c.members[1].node, target(&c, 2));
    t2.store.delete_chunk(&id(1)).unwrap(); // 缺了 chunk 1
    c.members[1].store.put_committed(&id(0), old); // chunk 0 还是旧版本
    let v1 = c.committed_version(2, 0);
    let p = repair::missing_from_last(n2, t2, 1, None, 100).unwrap();
    assert_eq!(p.todo, vec![(id(0), v1), (id(1), None)], "L（target 3）有、本地没有或更旧的");
    assert_eq!((p.last, p.more), (Some(id(1)), false));
    for (chunk, mine) in p.todo {
        assert!(matches!(repair::repair(n2, t2, &chunk, mine), Repair::Repaired { .. }), "{chunk:?}");
    }
    c.check(Duration::from_secs(5));
    let l = repair::missing_from_last(&c.members[2].node, target(&c, 3), 1, None, 100);
    assert_eq!(l.err(), Some(ErrCode::NOT_SERVING), "L 自己不做这项检查");
}

/// ADR-0040 第 10 条 ①（ADR-0041 第 18 条最后一句）：连续提交失败 → 只读；只读的 target 不进写路径。
#[test]
fn a_target_whose_commits_keep_failing_turns_read_only_and_leaves_the_write_path() {
    // 一个数据节点：真的提交失败 FAILED_COMMITS 次
    let store = MemTargetStore::new();
    let node = node_on(Arc::new(store.clone()) as Arc<dyn TargetStore>, 1);
    write_at(&node, CHUNK, 0, &[(0, 3)], b"one").unwrap();
    store.fail_commits(FAILED_COMMITS);
    for _ in 0..FAILED_COMMITS {
        assert!(write_at(&node, CHUNK, 0, &[(0, 3)], b"two").is_err());
    }
    let t = node.targets.get(&1).unwrap();
    assert!(t.read_only() && t.broken(), "连续 {FAILED_COMMITS} 次提交失败：只读");
    assert_eq!(write_at(&node, CHUNK, 0, &[(0, 3)], b"two"), Err(ErrCode::NOT_SERVING), "可重试：等它被移出写路径");
    assert_eq!(&read_at(&node, 0, 0, 3).unwrap().0[..], b"one", "读照常");

    // 链上的后继变成只读：转发回 NOT_SERVING、不写盘；链管理器把它移出之后写照常
    let c = Craq::start(3, &[(1, S), (2, S), (3, S)]);
    c.write(0, 0, b"one").unwrap();
    c.check(Duration::from_secs(5));
    let t3 = target(&c, 3);
    for _ in 0..FAILED_COMMITS {
        t3.note_commit(false);
    }
    assert!(t3.read_only());
    let v = c.committed_version(3, 0);
    let fw = ForwardWrite {
        chunk: id(0),
        chain_ver: 1,
        from_target: 2,
        base_version: v.unwrap(),
        version: v.unwrap() + 1,
        full: false,
        header: vec![],
        blocks: vec![],
        sync: false,
    };
    let r = peer_call(&c, 3, &PeerOp::ForwardWrite(fw), vec![]);
    assert_eq!(r.err().map(|e| e.0), Some(ErrCode::NOT_SERVING), "只读的后继不收转发");
    std::thread::scope(|sc| {
        let w = sc.spawn(|| c.write(0, 0, b"two"));
        std::thread::sleep(Duration::from_millis(500));
        // 心跳报了 read_only：链管理器按"变成只读"把它移出（这里直接下发结果）
        c.set_chains(&[(1, chain(&[(1, S), (2, S)], 2, 1))]);
        assert!(w.join().unwrap().is_ok(), "移出之后写照常");
    });
    assert_eq!(&c.read(0, 3).unwrap(), b"two");
    assert_eq!(c.committed_version(3, 0), v, "只读的 target 没有写盘");
    // 让它回来当 Syncing 成员：不同步（它要被移出链）
    c.set_chains(&[(1, chain(&[(1, S), (2, S), (3, Y)], 3, 1))]);
    std::thread::sleep(Duration::from_millis(2500));
    assert!(!sync_done(&c, 3, 3) && c.committed_version(3, 0) == v, "只读的 Syncing 成员不同步");
}

/// scrub 插件本身（完整的单节点）：唯一的副本坏了 → `wmfs task run scrub` → 修不了，changelog `DataLossDetected`（chunk 的位置在 extra）。
#[test]
fn the_scrub_task_reports_a_chunk_without_any_good_replica() {
    set_full_bg();
    let d = TempDir::new("scrub-lost");
    init(&d.0);
    let run = start(&d.0);
    let (a, c, me) = (api(&d.0, &run.mgmt_addr), client(&d.0, &run, "admin"), who(1000, 1000));
    let o = put(&c, &me, ROOT_INO, "f", &bytes(5, 300_000)).unwrap();
    let l = o.layout.as_ref().unwrap();
    let path =
        committed_path(&target_dir(&d.0), &ChunkId { chain: l.chains[0], ino: o.attr.ino, epoch: l.epoch, idx: 0 });
    let mut b = std::fs::read(&path).unwrap();
    let data_at = u16::from_le_bytes([b[10], b[11]]) as usize * 4096; // header_len_4k：数据区从这里开始（3.2.4）
    b[data_at + 10] ^= 0x40; // 第 0 块里的一个字节（同 wmfs debug corrupt-chunk）
    std::fs::write(&path, &b).unwrap();
    let t = task(&a, "scrub", "run", None);
    assert!(t.runs >= 1, "{t:?}");
    let lost = Some(("idx", "0"));
    assert!(wait_for(30, || has_event(&a, "DataLossDetected", lost)), "一个好副本也没有：如实上报");
    let keys = extra_keys(&a, "DataLossDetected", lost);
    assert_eq!(keys, "chain,epoch,idx", "金样：extra 的字段名与次序（M11 审查，ADR-0042 第 2 条）");
    assert!(get(&c, &me, o.attr.ino).is_err(), "读它照旧报错（客户端 EIO），不返回坏数据");
    run.stop();
}
