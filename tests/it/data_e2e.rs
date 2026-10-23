//! 单节点数据路径的端到端测试（roadmap M5 的验收标准、ADR-0034），在一个完整的节点上（`init_node` + `server::start`，插件与
//! `wmfs server` 默认装的一样），客户端走真实的 tcp：
//! - 多 chunk 文件往返一致、小文件走 inline、覆盖写换 epoch；截断到 s > 0 与到 0 都正确；
//! - cap 被篡改、过期、chains 不符、权限位不够、会话被驱逐（集群图的 evicted 列表）→ 数据节点拒绝；DeleteEpoch 要管理权限位；
//! - 人为损坏一个块 → 读回 EIO（单副本修不了，诚实报错），之后把那个 chunk 截掉照常成功；
//! - target 心跳把 phys_devs、boot_id 写进 TargetRecV1，StatFs 报出容量；
//! - ADR-0034 ①：只有写权限的人（0622 文件，其他用户 O_WRONLY 追加）把一个非空的 inline 文件写过 64 KiB → 成功、内容逐字节正确，
//!   而这个人 GetInline 仍是 EACCES，他拿到的只写 cap 也读不了 chunk；
//! - 新集群上 12 个客户端同时把文件写过 64 KiB（12 个并发 Uninline，数据节点还不认识刚生成的链）：几秒内全部成功，期间无关的
//!   GetAttr 照常应答（Uninline 最多占一半 meta-read 线程，心跳与集群图总有线程可用）；
//! - 16 个线程同时写同一个 chunk 里互不相交的块（同一 chunk 的写排队串行，队列满了 BUSY、客户端退避重试）：一个块也不丢。

use std::path::PathBuf;
use std::time::{Duration, Instant};

use wmfs_core::base::rand::XorShift64;
use wmfs_core::base::types::ChunkId;
use wmfs_core::base::{ErrCode, clock};
use wmfs_core::buf::Lane;
use wmfs_core::init::read_master;
use wmfs_core::meta::schema::keys::{self, ROOT_INO};
use wmfs_core::meta::schema::records::{EvictedV1, F_INLINE, TargetRecV1, decode_v1, encode_v1};
use wmfs_core::proto::cap::{self, RIGHT_ADMIN, RIGHT_READ};
use wmfs_core::proto::handshake::cap_key;
use wmfs_core::proto::meta::{MetaOp, MetaReply, O_RDWR, O_WRONLY, SetAttrIn};
use wmfs_plugins::local_fs::committed_path;
use wmfs_testkit::harness::meta::{get_raw, put_raw};
use wmfs_testkit::harness::node::{
    Client, chunk_of, client, commit, get, init, node_dir, opened, put, start, target_dir, wait_for, who, write_chunks,
};

fn tmp(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("wmfs-e2e-{name}-{}", std::process::id()))
}

fn random(seed: u64, n: usize) -> Vec<u8> {
    let mut v = vec![0u8; n];
    XorShift64::new(seed).fill(&mut v);
    v
}

fn read_chunk(c: &Client, capb: &[u8], o: &wmfs_core::proto::meta::OpenOut, idx: u32) -> Result<Vec<u8>, ErrCode> {
    let into = c.rpc.pool().get(4 << 20, Lane::Normal, Duration::from_secs(5)).unwrap();
    let l = o.layout.as_ref().unwrap();
    c.data.read(capb, l, chunk_of(o, idx), 0, 4096, into, None).map(|(b, n)| b.as_slice()[..n as usize].to_vec())
}

#[test]
fn files_round_trip_inline_small_files_and_truncate() {
    let d = tmp("rt");
    init(&d);
    let run = start(&d);
    let c = client(&d, &run, "admin");
    let me = who(1000, 1000);
    let big = random(1, (9 << 20) + 12345);
    let o = put(&c, &me, ROOT_INO, "big", &big).unwrap();
    assert_eq!(o.attr.flags & F_INLINE, 0);
    assert_eq!(get(&c, &me, o.attr.ino).unwrap(), big, "3 个 chunk 的文件往返一致");
    let small = put(&c, &me, ROOT_INO, "small", b"hello\n").unwrap();
    assert!(small.attr.flags & F_INLINE != 0, "小文件走 inline");
    assert_eq!(get(&c, &me, small.attr.ino).unwrap(), b"hello\n");
    // 覆盖写：截断到 0 换 epoch，旧 epoch 写 GC 记录
    let again = put(&c, &me, ROOT_INO, "big", &big[..5_000_000]).unwrap();
    assert_eq!(again.layout.as_ref().unwrap().epoch, 2);
    assert_eq!(get(&c, &me, o.attr.ino).unwrap(), &big[..5_000_000]);
    // 截断到 s > 0：先截数据（被截掉的部分清零），再改长度；再扩回来，读到的是 0 而不是旧数据
    let ino = o.attr.ino;
    let wr = opened(c.meta.read(&me, MetaOp::Open { ino, flags: O_WRONLY })).unwrap();
    let cs = 4u64 << 20;
    let s = 4_200_000u64;
    for idx in s / cs..5_000_000u64.div_ceil(cs) {
        let n = s.saturating_sub(idx * cs).min(cs) as u32;
        c.data.truncate(&wr.cap, chunk_of(&wr, idx as u32), n).unwrap();
    }
    let set = |size| SetAttrIn {
        mode: None,
        uid: None,
        gid: None,
        size: Some(size),
        atime: None,
        mtime: None,
        epoch_seen: 2,
    };
    opened(c.meta.write(&me, MetaOp::SetAttr { ino, set: set(s) })).unwrap();
    opened(c.meta.write(&me, MetaOp::SetAttr { ino, set: set(5_000_000) })).unwrap();
    let got = get(&c, &me, ino).unwrap();
    assert_eq!(&got[..s as usize], &big[..s as usize]);
    assert!(got[s as usize..].iter().all(|b| *b == 0), "截掉之后再扩展：读为 0");
    let z = opened(c.meta.write(&me, MetaOp::SetAttr { ino, set: set(0) })).unwrap();
    assert_eq!((z.attr.size, z.layout.as_ref().unwrap().epoch), (0, 3), "截断到 0：新 epoch");
    assert!(get(&c, &me, ino).unwrap().is_empty());
    c.meta.close();
    run.stop();
    std::fs::remove_dir_all(&d).unwrap();
}

#[test]
fn data_nodes_reject_bad_caps() {
    let d = tmp("cap");
    init(&d);
    let run = start(&d);
    let c = client(&d, &run, "admin");
    let me = who(1000, 1000);
    let o = put(&c, &me, ROOT_INO, "f", &random(2, 100_000)).unwrap();
    let o = opened(c.meta.read(&me, MetaOp::Open { ino: o.attr.ino, flags: O_RDWR })).unwrap();
    assert!(read_chunk(&c, &o.cap, &o, 0).is_ok());
    let mut tampered = o.cap.clone();
    tampered[3] ^= 1;
    assert_eq!(read_chunk(&c, &tampered, &o, 0).err(), Some(ErrCode::BAD_CAP), "篡改过的 cap");
    let k = cap_key(&read_master(&node_dir(&d)).unwrap());
    let base = cap::verify(&k, &o.cap).unwrap();
    let resign = |f: &dyn Fn(&mut cap::CapV1)| {
        let mut x = base.clone();
        f(&mut x);
        cap::sign(&k, &x)
    };
    let expired = resign(&|x| x.expires_ms = clock::wall_ms() - 6 * 60 * 1000);
    assert_eq!(read_chunk(&c, &expired, &o, 0).err(), Some(ErrCode::BAD_CAP), "过期（超过 5 分钟容差）");
    let other_chain = resign(&|x| x.chains = vec![99]);
    assert_eq!(read_chunk(&c, &other_chain, &o, 0).err(), Some(ErrCode::BAD_CAP), "chains 与 chunk 所在的链不符");
    let read_only = resign(&|x| x.rights = RIGHT_READ);
    assert_eq!(c.data.truncate(&read_only, chunk_of(&o, 0), 0).err(), Some(ErrCode::BAD_CAP), "只读 cap 不能写");
    let l = o.layout.as_ref().unwrap();
    assert_eq!(
        c.data.delete_epoch(&o.cap, l.chains[0], o.attr.ino, l.epoch),
        Err(ErrCode::BAD_CAP),
        "DeleteEpoch 要管理权限位"
    );
    let admin = resign(&|x| x.rights = RIGHT_ADMIN);
    assert_eq!(c.data.delete_epoch(&admin, l.chains[0], o.attr.ino, l.epoch), Ok(1));
    // 会话被驱逐：EVICTED 记录进集群图（map_epoch + 1），数据节点拿到新集群图之后拒绝它的 cap
    let ev = EvictedV1 { evicted_ms: clock::wall_ms(), map_epoch: 0 };
    put_raw(&run.meta, keys::evicted(base.session), encode_v1(&ev)).unwrap();
    let epoch: u64 = decode_v1(&get_raw(&run.meta, &keys::global(keys::G_MAP_EPOCH)).unwrap().unwrap()).unwrap();
    put_raw(&run.meta, keys::global(keys::G_MAP_EPOCH), encode_v1(&(epoch + 1))).unwrap();
    let w = write_chunks(&c, &o, 0, b"x");
    assert!(w.is_ok() || w == Err(ErrCode::SESSION_EVICTED));
    assert!(
        wait_for(10, || c.data.truncate(&o.cap, chunk_of(&o, 0), 0) == Err(ErrCode::SESSION_EVICTED)),
        "被驱逐会话的 cap 被拒绝"
    );
    run.stop();
    std::fs::remove_dir_all(&d).unwrap();
}

#[test]
fn a_corrupt_block_reads_as_eio_and_truncating_it_away_still_works() {
    let d = tmp("corrupt");
    init(&d);
    let run = start(&d);
    let c = client(&d, &run, "admin");
    let me = who(1000, 1000);
    let data = random(3, 9 << 20);
    let o = put(&c, &me, ROOT_INO, "f", &data).unwrap();
    let l = o.layout.clone().unwrap();
    let id = ChunkId { chain: l.chains[0], ino: o.attr.ino, epoch: l.epoch, idx: 2 };
    let file = committed_path(&target_dir(&d), &id);
    let mut bytes = std::fs::read(&file).unwrap();
    bytes[8192 + 100] ^= 0xFF; // 第 0 块的数据（数据区从 8 KiB 开始）
    std::fs::write(&file, &bytes).unwrap();
    assert_eq!(get(&c, &me, o.attr.ino).err(), Some(ErrCode::EIO), "单副本：校验和不符 → EIO，而不是错误的数据");
    let wr = opened(c.meta.read(&me, MetaOp::Open { ino: o.attr.ino, flags: O_WRONLY })).unwrap();
    c.data.truncate(&wr.cap, chunk_of(&wr, 2), 0).unwrap(); // 坏的 chunk 整块截掉，不需要读它
    c.data.truncate(&wr.cap, chunk_of(&wr, 1), 3 << 20).unwrap();
    let set = SetAttrIn {
        mode: None,
        uid: None,
        gid: None,
        size: Some(7 << 20),
        atime: None,
        mtime: None,
        epoch_seen: l.epoch,
    };
    opened(c.meta.write(&me, MetaOp::SetAttr { ino: o.attr.ino, set })).unwrap();
    let got = get(&c, &me, o.attr.ino).unwrap();
    assert_eq!(&got[..(7 << 20)], &data[..(7 << 20)]);
    run.stop();
    std::fs::remove_dir_all(&d).unwrap();
}

#[test]
fn heartbeats_fill_in_the_target_record_and_statfs() {
    let d = tmp("hb");
    init(&d);
    let run = start(&d);
    let c = client(&d, &run, "admin");
    let rec = || -> TargetRecV1 { decode_v1(&get_raw(&run.meta, &keys::target(1)).unwrap().unwrap()).unwrap() };
    assert!(wait_for(10, || rec().boot_id != 0), "第一次心跳写进本进程的 boot_id");
    let devs = wmfs_core::base::devinfo::phys_devs(&target_dir(&d));
    assert_eq!(rec().phys_devs, devs, "心跳报的物理盘");
    assert!(
        wait_for(10, || matches!(
            c.meta.read(&who(1000, 1000), MetaOp::StatFs),
            Ok(MetaReply::StatFs { blocks, bfree, .. }) if blocks > 0 && bfree <= blocks
        )),
        "StatFs 报出 target 的容量"
    );
    run.stop();
    std::fs::remove_dir_all(&d).unwrap();
}

#[test]
fn a_write_only_user_can_grow_an_inline_file_past_64k_without_seeing_it() {
    let d = tmp("wonly");
    init(&d);
    let run = start(&d);
    let c = client(&d, &run, "admin");
    let (owner, other) = (who(1000, 1000), who(2000, 2000));
    let head = random(4, 10_000);
    let o = put(&c, &owner, ROOT_INO, "log", &head).unwrap();
    assert!(o.attr.flags & F_INLINE != 0);
    let chmod =
        SetAttrIn { mode: Some(0o622), uid: None, gid: None, size: None, atime: None, mtime: None, epoch_seen: 0 };
    c.meta.write(&owner, MetaOp::SetAttr { ino: o.attr.ino, set: chmod }).unwrap();
    assert_eq!(c.meta.read(&other, MetaOp::GetInline { ino: o.attr.ino }), Err(ErrCode::EACCES));
    // 其他用户 O_WRONLY 追加 60 000 字节：结果 70 000 > 64 KiB → 先请服务端 Uninline，再按 chunk 写
    let w = opened(c.meta.read(&other, MetaOp::Open { ino: o.attr.ino, flags: O_WRONLY })).unwrap();
    assert_eq!(w.inline, None, "写打开拿不到内容");
    let w = opened(c.meta.read(&other, MetaOp::Uninline { ino: o.attr.ino })).unwrap();
    assert_eq!(
        (w.attr.flags & F_INLINE, w.inline.clone()),
        (0, None),
        "服务端把旧内容搬进了 chunk，内容不回给只能写的人"
    );
    let tail = random(5, 60_000);
    write_chunks(&c, &w, head.len() as u64, &tail).unwrap();
    commit(&c, &other, &w, 70_000, vec![]).unwrap();
    assert_eq!(c.meta.read(&other, MetaOp::GetInline { ino: o.attr.ino }), Err(ErrCode::EACCES), "仍然拿不到内容");
    let all = get(&c, &owner, o.attr.ino).unwrap();
    assert_eq!(all.len(), 70_000);
    assert!(all[..10_000] == head[..] && all[10_000..] == tail[..], "内容逐字节正确");
    let again = opened(c.meta.read(&other, MetaOp::Uninline { ino: o.attr.ino })).unwrap();
    assert_eq!(again.attr.size, 70_000, "已经不是 inline：幂等，直接成功");
    assert_eq!(
        read_chunk(&c, &w.cap, &w, 0).err(),
        Some(ErrCode::BAD_CAP),
        "只写的 cap 读不了 chunk（内容只给能读的人）"
    );
    run.stop();
    std::fs::remove_dir_all(&d).unwrap();
}

#[test]
fn concurrent_uninlines_on_a_new_cluster_finish_quickly_and_reads_stay_responsive() {
    let d = tmp("uninl");
    init(&d);
    let run = start(&d);
    let c = client(&d, &run, "admin");
    let me = who(1000, 1000);
    let data = random(7, 100_000);
    let t0 = Instant::now();
    std::thread::scope(|s| {
        let (c, me, data) = (&c, &me, &data);
        let puts: Vec<_> = (0..12).map(|i| s.spawn(move || put(c, me, ROOT_INO, &format!("u{i}"), data))).collect();
        std::thread::sleep(Duration::from_millis(300)); // Uninline 正在进行
        let t = Instant::now();
        c.meta.read(me, MetaOp::GetAttr { ino: ROOT_INO }).unwrap();
        assert!(t.elapsed() < Duration::from_secs(3), "无关的 GetAttr 照常应答（{:?}）", t.elapsed());
        for (i, p) in puts.into_iter().enumerate() {
            let o = p.join().unwrap().unwrap_or_else(|e| panic!("第 {i} 个文件写失败：{e}"));
            assert_eq!(o.attr.flags & F_INLINE, 0);
        }
    });
    assert!(t0.elapsed() < Duration::from_secs(15), "12 个 Uninline 几秒内全部完成（{:?}）", t0.elapsed());
    for i in 0..12 {
        let r = c.meta.read(&me, MetaOp::ResolvePath { path: format!("/u{i}").into_bytes() }).unwrap();
        let MetaReply::Resolved { ino, .. } = r else { panic!("{r:?}") };
        assert!(get(&c, &me, ino).unwrap() == data, "第 {i} 个文件的内容");
    }
    run.stop();
    std::fs::remove_dir_all(&d).unwrap();
}

#[test]
fn sixteen_writers_on_one_chunk_lose_nothing() {
    let d = tmp("conc");
    init(&d);
    let run = start(&d);
    let c = client(&d, &run, "admin");
    let me = who(1000, 1000);
    let o = put(&c, &me, ROOT_INO, "f", &random(6, 100_000)).unwrap();
    let o = opened(c.meta.read(&me, MetaOp::Open { ino: o.attr.ino, flags: O_RDWR })).unwrap();
    std::thread::scope(|s| {
        for i in 0..16u8 {
            let (c, o) = (&c, &o);
            s.spawn(move || write_chunks(c, o, i as u64 * 65536, &[i + 1; 65536]).unwrap());
        }
    });
    let into = c.rpc.pool().get(1 << 20, Lane::Normal, Duration::from_secs(5)).unwrap();
    let l = o.layout.as_ref().unwrap();
    let (b, n) = c.data.read(&o.cap, l, chunk_of(&o, 0), 0, 1 << 20, into, None).unwrap();
    assert_eq!(n, 1 << 20);
    for i in 0..16usize {
        let block = &b.as_slice()[i * 65536..(i + 1) * 65536];
        assert!(block.iter().all(|x| *x == i as u8 + 1), "第 {i} 块是第 {i} 个写者写的内容");
    }
    run.stop();
    std::fs::remove_dir_all(&d).unwrap();
}
