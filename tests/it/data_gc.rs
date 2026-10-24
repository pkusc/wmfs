//! gc 任务（design.md 4.12、3.5.6、3.4.2.9；roadmap M5 的验收"废弃 epoch 在宽限期后被删除"），在一个完整的节点上：
//! - 截断到 0 换下来的旧 epoch：宽限期（`epoch_gc_grace`，这里调成 2 s）之内不删，过了宽限期 gc 对它的链 DeleteEpoch、删 GC 记录；
//! - 孤儿：有会话打开着时不删；关掉之后（且过了 6 s、本 leader 就绪超过 6 s、存活会话都报过全集）gc 删数据、删 inode。

use std::path::PathBuf;
use std::time::Duration;

use wmfs_core::base::rand::XorShift64;
use wmfs_core::base::types::ChunkId;
use wmfs_core::meta::schema::keys::{self, GC_EPOCH, GC_ORPHAN, ROOT_INO};
use wmfs_core::proto::meta::MetaOp;
use wmfs_plugins::local_fs::committed_path;
use wmfs_testkit::harness::meta::get_raw;
use wmfs_testkit::harness::node::{client, init, put, set_settings, start, target_dir, wait_for, who};

fn tmp(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("wmfs-gc-{name}-{}", std::process::id()))
}

fn data(n: usize) -> Vec<u8> {
    let mut v = vec![0u8; n];
    XorShift64::new(9).fill(&mut v);
    v
}

/// target 上 (chain, ino, epoch) 的目录还在不在（local-fs 的路径规则：chunk 文件所在的目录）。
fn epoch_dir_exists(d: &std::path::Path, chain: u32, ino: u64, epoch: u32) -> bool {
    let chunk0 = committed_path(&target_dir(d), &ChunkId { chain, ino, epoch, idx: 0 });
    chunk0.parent().unwrap().exists()
}

#[test]
fn obsolete_epochs_are_deleted_after_the_grace_period() {
    let d = tmp("epoch");
    init(&d);
    let run = start(&d);
    set_settings(&run.meta, |s| s.epoch_gc_grace_ms = 2000);
    let c = client(&d, &run, "admin");
    let me = who(1000, 1000);
    let o = put(&c, &me, ROOT_INO, "f", &data(5 << 20)).unwrap();
    // 数据所在的 epoch 以 layout 为准（实现注记（M8）：非空内容的服务端 Uninline 会换 epoch，ADR-0035 第 11 条；这里是新建的空文件，仍是 1）
    let (ino, l) = (o.attr.ino, o.layout.as_ref().unwrap());
    let (chain, e) = (l.chains[0], l.epoch);
    assert!(epoch_dir_exists(&d, chain, ino, e));
    put(&c, &me, ROOT_INO, "f", b"short").unwrap(); // 覆盖：截断到 0，epoch e 作废（GC 记录 kind 2）
    assert!(get_raw(&run.meta, &keys::gc(GC_EPOCH, ino, e)).unwrap().is_some(), "截断到 0 写了 GC 记录");
    std::thread::sleep(Duration::from_millis(800));
    assert!(epoch_dir_exists(&d, chain, ino, e), "宽限期之内不删（还拿着旧 layout 的读写来得及刷新）");
    assert!(wait_for(15, || !epoch_dir_exists(&d, chain, ino, e)), "过了宽限期，旧 epoch 的数据被删除");
    assert!(wait_for(5, || get_raw(&run.meta, &keys::gc(GC_EPOCH, ino, e)).unwrap().is_none()), "GC 记录也删了");
    let ok = wmfs_core::api::metrics::counter("wmfs_gc_epochs_deleted_total", "", &[]);
    assert!(wait_for(5, || ok.get() >= 1), "gc 删完记录之后才计数：等它一下"); // 测试线程可能先看到记录没了
    run.stop();
    std::fs::remove_dir_all(&d).unwrap();
}

#[test]
fn orphans_wait_until_nobody_has_them_open() {
    let d = tmp("orphan");
    init(&d);
    let run = start(&d);
    let c = client(&d, &run, "admin");
    let me = who(1000, 1000);
    let o = put(&c, &me, ROOT_INO, "f", &data(1 << 20)).unwrap();
    let (ino, chain, epoch) = (o.attr.ino, o.layout.as_ref().unwrap().chains[0], o.layout.as_ref().unwrap().epoch);
    c.meta.heartbeat(vec![(ino, false)], vec![], None).unwrap(); // 这个会话打开着它
    c.meta.write(&me, MetaOp::Unlink { parent: ROOT_INO, name: b"f".to_vec() }).unwrap();
    assert!(get_raw(&run.meta, &keys::gc(GC_ORPHAN, ino, 0)).unwrap().is_some(), "最后一个链接删掉：孤儿 + GC 记录");
    std::thread::sleep(Duration::from_secs(9)); // 过了 6 s 的等待与 6 s 的就绪期
    assert!(get_raw(&run.meta, &keys::inode(ino)).unwrap().is_some(), "还有人打开着：不删");
    assert!(epoch_dir_exists(&d, chain, ino, epoch), "数据也还在（打开着它的人照常读写）");
    c.meta.heartbeat(vec![], vec![ino], None).unwrap(); // 关掉了
    assert!(wait_for(10, || get_raw(&run.meta, &keys::inode(ino)).unwrap().is_none()), "没人打开之后删掉 inode");
    assert!(!epoch_dir_exists(&d, chain, ino, epoch), "数据先删（DeleteEpoch），inode 后删");
    assert!(get_raw(&run.meta, &keys::gc(GC_ORPHAN, ino, 0)).unwrap().is_none());
    run.stop();
    std::fs::remove_dir_all(&d).unwrap();
}
