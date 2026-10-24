//! ADR-0035 第 11 条（M8 必须解决）：换 leader 之后旧 leader 上没做完的 uninline 的数据面 fencing（`meta/uninline.rs`）。
//! 同一个进程里的 3 节点集群；fail point `meta.uninline.before_write = times(1,sleep(5000))` 把旧 leader 的 uninline 卡在"意图已提交、
//! 写 chunk 之前"→ 换 leader → 换 leader 之后才连上的客户端 b（直接连到新 leader）上的 uninline 把旧的意图记为垃圾、换新的 epoch、
//! 成功，b 往新 epoch 写入新数据并提交——这些都在旧 leader 睡醒之前做完（断言了）→ 旧 leader 醒来，把旧的 inline 内容写进它自己那个
//! 已作废的 epoch（断言这个 chunk 文件确实出现了），切换失败（它已经不是 leader）→ 文件内容仍是新数据，旧的意图恰好有 GC 记录。
//! 没有 fencing（M5 的做法：按文件当前的 layout、同一个 epoch 写）时，旧 leader 迟到的写会把新数据的 chunk 0 换成旧内容。
//! 实现注记（M8 审查）：b 若在换 leader 之前就连着旧 leader，它的 uninline 会在旧 leader 上排队（旧 leader 的 InFlight 里挂着那次卡住的
//! uninline）到迟到的写做完之后——测不到要测的情形；现在 follower 对 Uninline 先回 NOT_LEADER（`service.rs`），b 也是换 leader 之后才连。
//! fail point 是进程级的：单独一个测试程序。

use std::time::{Duration, Instant};

use wmfs_core::api::{failpoint, metrics};
use wmfs_core::base::types::ChunkId;
use wmfs_core::meta::raft::membership::Members;
use wmfs_core::meta::schema::keys::{self, GC_EPOCH, ROOT_INO};
use wmfs_core::meta::schema::records::{F_INLINE, InodeV1, decode_v1};
use wmfs_core::proto::meta::MetaOp;
use wmfs_plugins::local_fs::committed_path;
use wmfs_testkit::harness::cluster::Cluster;
use wmfs_testkit::harness::node::{commit, get, opened, put, who, write_chunks};

const OLD: &[u8] = b"OLD INLINE CONTENT";
/// 旧 leader 卡在写 chunk 之前多久（新 leader 上的 uninline、写新数据、提交都要在这之内做完）。
const STALL: Duration = Duration::from_secs(5);

fn hits() -> u64 {
    metrics::counter("wmfs_failpoint_hits_total", "", &[("name", "meta.uninline.before_write")]).get()
}

/// 节点 `i` 本地已应用的 inode 记录。
fn inode(c: &Cluster, i: usize, ino: u64) -> InodeV1 {
    decode_v1(&c.run(i).meta.local_get(&keys::inode(ino)).unwrap().unwrap()).unwrap()
}

#[test]
fn a_stale_leaders_late_uninline_write_lands_in_a_dead_epoch() {
    let c = Cluster::start("fence", 3);
    let a = c.client("admin");
    let me = who(1000, 1000);
    let o = put(&a, &me, ROOT_INO, "f", OLD).unwrap();
    let ino = o.attr.ino;
    assert!(o.attr.flags & F_INLINE != 0);
    let old = c.wait_leader();
    failpoint::set("meta.uninline.before_write", &format!("times(1,sleep({}))", STALL.as_millis())).unwrap();
    let meta = a.meta.clone();
    let stale = std::thread::spawn(move || (meta.read(&who(1000, 1000), MetaOp::Uninline { ino }), Instant::now()));
    let deadline = Instant::now() + Duration::from_secs(10);
    while hits() == 0 {
        assert!(Instant::now() < deadline, "旧 leader 的 uninline 应停在写 chunk 之前");
        std::thread::sleep(Duration::from_millis(10));
    }
    let hit = Instant::now();
    let dead = inode(&c, old, ino).pending_epoch; // 旧 leader 的意图已经提交：它迟到的写会落在这个 epoch
    assert!(dead > o.layout.as_ref().unwrap().epoch, "意图分了新的 epoch {dead}");
    let new = (1..=3).find(|i| *i != old).unwrap();
    c.run(old).meta.transfer_leader(new as u32);
    while c.leader() != Some(new) {
        assert!(Instant::now() < deadline, "应换到 node{new}");
        std::thread::sleep(Duration::from_millis(10));
    }
    let b = c.client("admin"); // 换 leader 之后才连：直接找到新 leader
    let u = opened(b.meta.read(&me, MetaOp::Uninline { ino })).unwrap();
    assert_eq!(u.attr.flags & F_INLINE, 0, "新 leader 上的 uninline 成功");
    let layout = u.layout.clone().unwrap();
    assert!(layout.epoch > dead, "新 leader 不复用旧意图的 epoch {dead}，换了新的 {}", layout.epoch);
    let fresh = vec![b'N'; 100_000];
    write_chunks(&b, &u, 0, &fresh).unwrap();
    commit(&b, &me, &u, fresh.len() as u64, vec![]).unwrap();
    let committed = Instant::now();
    assert!(committed - hit < STALL - Duration::from_millis(500), "新数据要在旧 leader 睡醒之前提交，才测得到迟到的写");
    let (r, returned) = stale.join().unwrap();
    assert!(returned > committed, "旧请求在新数据提交之后才返回");
    assert!(r.is_ok(), "旧的请求跟随新 leader 重试，幂等成功：{r:?}");
    let got = get(&b, &me, ino).unwrap();
    assert!(got.len() == fresh.len() && got == fresh, "文件内容仍是新数据（旧 leader 迟到的写落进了作废的 epoch）");
    // 迟到的写确实发生了：作废的 epoch 下面有 chunk 0，长度（固定头偏移 60 的 logical_len）是旧的 inline 内容的长度
    let chain = layout.chains[0];
    let m = c.run(new).meta.cluster_map().unwrap();
    let head = m.chains.iter().find(|(id, _)| *id == chain).map(|(_, ch)| ch.members[0].0).unwrap();
    let node = m.targets.iter().find(|(id, _)| *id == head).map(|(_, t)| t.node).unwrap();
    let path =
        committed_path(&c.node_dir(node as usize).join("targets/1"), &ChunkId { chain, ino, epoch: dead, idx: 0 });
    let bytes =
        std::fs::read(&path).unwrap_or_else(|e| panic!("作废的 epoch 下面应有旧 leader 写的 chunk {path:?}：{e}"));
    assert_eq!(u32::from_le_bytes(bytes[60..64].try_into().unwrap()) as usize, OLD.len(), "写进去的是旧内容");
    let gc = c.run(new).meta.local_get(&keys::gc(GC_EPOCH, ino, dead)).unwrap();
    assert!(gc.is_some(), "旧 leader 的意图（epoch {dead}）有 GC 记录");
}
