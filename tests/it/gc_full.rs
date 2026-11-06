//! gc 的完整版（design.md 4.12 的 gc 一行、3.5.6、3.11；roadmap M11），在完整的单节点上（[`set_full_bg`]：另装 gc 的本地孤儿扫描）：
//! - 废弃 epoch 按来源分两种回收（ADR-0042 第 5 条）：迁移换下来的旧 layout（GC 记录 kind 3）在文件还被打开着时不删（roadmap M11 的
//!   验收：拿着旧 layout 的读者照样读得到），关掉之后才删；截断到 0 换下来的（kind 2）过了宽限期就删，文件一直打开着也一样（像
//!   copytruncate 式的日志轮转），拿着旧 layout 的读者按 C5 看到截断；
//! - 关掉了的会话留下的去重记录被删掉（ADR-0041 第 9 条留给 M11 的）；
//! - changelog 修剪到所有消费者游标都消费过的位置，再读更早的 → `410 Gone`（缺口显式），之后的照常读；
//! - 本地孤儿扫描（`gc-local`，fail point `gc.local.min_age_ms` 把 1 h 缩成 0）：元数据里已经死了的 epoch 目录（节点离线时错过的
//!   DeleteEpoch、文件删掉之后才落下的 chunk）被删掉，当前的留着；leader 的 gc 还管着的旧 epoch（有 GC 记录：还在宽限期里的截断、
//!   文件还打开着的迁移）它不碰。
//!
//! fail point 是进程级的：`gc.local.min_age_ms` 只让本地孤儿扫描不再等目录变旧，同一个进程里别的用例只删死了的数据，不受影响。

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use wmfs_core::api::failpoint;
use wmfs_core::api::types::{ChangelogPage, CursorReply, CursorReq, NodeInfoReply, STATE_DONE};
use wmfs_core::base::types::{ChunkId, ReqCtx};
use wmfs_core::meta::engine::EmbeddedEngine;
use wmfs_core::meta::engine::txn::MetaEngine;
use wmfs_core::meta::schema::keys::{self, ROOT_INO};
use wmfs_core::meta::schema::records::{ChainRecV1, TargetRecV1, decode_v1};
use wmfs_core::proto::meta::{MetaOp, MetaReply, O_RDONLY, O_RDWR, OpenOut};
use wmfs_mgr::api::ApiClient;
use wmfs_plugins::local_fs::committed_path;
use wmfs_testkit::harness::bg::{api, api_at, attr, changelog, mkdir, set, status, task, tasks, text, wait_converted};
use wmfs_testkit::harness::cluster::Cluster;
use wmfs_testkit::harness::meta::get_raw;
use wmfs_testkit::harness::node::{
    TempDir, client, file_client, get, init, put, set_full_bg, set_settings, start, target_dir, wait_for, who,
};

/// target 上 `o` 的 layout 那个 epoch 的目录（local-fs：chunk 文件所在的目录）。
fn epoch_dir(d: &Path, ino: u64, chain: u32, epoch: u32) -> PathBuf {
    committed_path(&target_dir(d), &ChunkId { chain, ino, epoch, idx: 0 }).parent().unwrap().to_path_buf()
}

fn dir_of(d: &Path, o: &OpenOut) -> PathBuf {
    let l = o.layout.as_ref().unwrap();
    epoch_dir(d, o.attr.ino, l.chains[0], l.epoch)
}

fn gc_passes(a: &ApiClient) -> u64 {
    tasks(a).iter().find(|t| t.kind == "gc").map_or(0, |t| t.pass)
}

/// 会话 `sid` 的去重记录有几条。
fn dedup_rows(eng: &EmbeddedEngine, sid: u64) -> usize {
    let mut n = 0;
    eng.read(&ReqCtx::internal(), &mut |v, _c| {
        n = v.scan(&keys::dedup(sid, 0), &keys::dedup(sid, u64::MAX), 100_000)?.len();
        Ok(MetaReply::Ok)
    })
    .unwrap();
    n
}

/// 迁移换下来的旧 layout（kind 3）：只读打开着这个文件的读者照样读得到，gc 跑了几轮也不删；关掉之后才删。
#[test]
fn a_layout_replaced_by_a_migration_is_kept_while_the_file_is_open_and_deleted_after_close() {
    set_full_bg();
    let d = TempDir::new("gc-open");
    init(&d.0);
    let run = start(&d.0);
    set_settings(&run.meta, |s| s.epoch_gc_grace_ms = 0);
    let (a, c, me) = (api(&d.0, &run.mgmt_addr), client(&d.0, &run, "admin"), who(1000, 1000));
    let m = mkdir(&c, ROOT_INO, "m");
    let data = text(1, 300_000);
    let w = put(&c, &me, m, "f", &data).unwrap();
    let old = dir_of(&d.0, &w);
    assert!(old.exists());
    // 一个挂载只读打开着它（迁移只跳过写打开的）；协调器把它换到 lz4：旧 layout 记为垃圾（kind 3，宽限 0）
    let fc = file_client(&d.0, &run);
    let o = fc.open(&me, w.attr.ino, O_RDONLY).unwrap();
    set(&a, "/m", "compress=lz4").unwrap();
    assert!(wait_converted(&a, "/m", "compress", 60), "{:?}", attr(&status(&a, "/m"), "compress"));
    let (t0, p0) = (Instant::now(), gc_passes(&a));
    // 记录写下之后 2 个心跳周期（之前打开它的会话都报上来了），gc 又跑了两轮
    let settled = || t0.elapsed() >= Duration::from_secs(7) && gc_passes(&a) >= p0 + 2;
    assert!(wait_for(30, settled), "gc 又跑了两轮");
    assert!(old.exists(), "文件还打开着：迁移换下来的旧 layout 不删");
    assert!(fc.read(&me, &fc.handle(o.fh).unwrap(), 0, 300_000).unwrap() == data, "拿着旧 layout 的读者照样读得到");
    fc.release(o.fh);
    assert!(wait_for(30, || !old.exists()), "关掉之后删掉");
    assert!(get(&c, &me, w.attr.ino).unwrap() == data);
    run.stop();
}

/// 截断到 0 换下来的旧 epoch（kind 2）：过了宽限期就删，不等文件关闭（一直开着日志文件的程序在 copytruncate 式轮转之后不泄漏空间）；
/// 拿着旧 layout 的读者按 C5 看到截断之后的新内容。
#[test]
fn an_epoch_truncated_away_is_deleted_after_the_grace_period_even_while_the_file_is_open() {
    set_full_bg();
    let d = TempDir::new("gc-trunc");
    init(&d.0);
    let run = start(&d.0);
    set_settings(&run.meta, |s| s.epoch_gc_grace_ms = 0);
    let me = who(1000, 1000);
    let fc = file_client(&d.0, &run);
    let o = fc.create(&me, ROOT_INO, b"log", 0o644, 0, O_RDWR).unwrap();
    fc.write(&me, &fc.handle(o.fh).unwrap(), 0, &[3u8; 200_000]).unwrap();
    fc.flush(o.fh).unwrap();
    let l = fc.inode(o.attr.ino).unwrap().st.lock().unwrap().layout.clone().unwrap();
    let old = epoch_dir(&d.0, o.attr.ino, l.chains[0], l.epoch);
    assert!(old.exists());
    // 另一个客户端截断、改写：旧 epoch 记为垃圾（kind 2，宽限 0）。这个挂载一直打开着它
    let c = client(&d.0, &run, "admin");
    let now = put(&c, &me, ROOT_INO, "log", &[4u8; 150_000]).unwrap();
    assert!(wait_for(30, || !old.exists()), "过了宽限期就删，文件还打开着也一样");
    let got = fc.read(&me, &fc.handle(o.fh).unwrap(), 0, 200_000).unwrap();
    assert!(got == vec![4u8; 150_000], "拿着旧 layout 的读者按 C5 看到截断：{} 字节", got.len());
    assert!(dir_of(&d.0, &now).exists(), "当前 epoch 的留着");
    fc.release(o.fh);
    run.stop();
}

#[test]
fn dedup_records_of_closed_sessions_and_consumed_changelog_are_cleaned_up() {
    set_full_bg();
    let d = TempDir::new("gc-meta");
    init(&d.0);
    let run = start(&d.0);
    let (a, me) = (api(&d.0, &run.mgmt_addr), who(1000, 1000));
    // 去重记录：会话关掉之后被删掉
    let c = client(&d.0, &run, "admin");
    let sid = c.meta.session();
    for i in 0..5 {
        let op = MetaOp::Mkdir { parent: ROOT_INO, name: format!("d{i}").into_bytes(), mode: 0o755, umask: 0 };
        c.meta.write(&me, op).unwrap();
    }
    assert!(dedup_rows(&run.meta, sid) >= 5, "每个写请求一条去重记录");
    c.meta.close();
    assert!(wait_for(20, || dedup_rows(&run.meta, sid) == 0), "会话不在了：它的去重记录被 gc 删掉");

    // changelog：消费者游标消费过的被修剪；再读更早的 → 410
    let last = changelog(&a, 0).last_seq;
    assert!(last > 5);
    let cur: CursorReply = a.post("/api/v1/changelog/cursor", &CursorReq { name: "t-gc".into(), seq: last }).unwrap();
    assert_eq!(cur.seq, last);
    let gone =
        || a.get::<ChangelogPage>("/api/v1/changelog?after=0&limit=10&wait_ms=0").is_err_and(|e| e.status == 410);
    assert!(wait_for(20, gone), "游标之前的已修剪，缺口是显式的");
    let c2 = client(&d.0, &run, "admin");
    put(&c2, &me, ROOT_INO, "after", b"x").unwrap();
    let p0 = gc_passes(&a);
    assert!(wait_for(20, || gc_passes(&a) >= p0 + 3), "gc 又跑了三轮");
    let kept = changelog(&a, last).events;
    assert!(!kept.is_empty() && kept[0].seq == last + 1, "游标之后的一条也没修剪：{:?}", kept.first().map(|e| e.seq));
    run.stop();
}

/// 本地孤儿扫描不碰 leader 的 gc 还管着的旧 epoch（有 GC 记录，`CheckEpochs` 当它活着）——旧 epoch 的目录 mtime 早就"够旧"了也一样
/// （fail point 把 1 h 缩成 0，像一个一小时前写的文件被覆盖、被迁移）：截断换下来的（kind 2）还在宽限期里；迁移换下来的（kind 3）、
/// 文件还打开着，等关了由 leader 的 gc 删。
#[test]
fn the_local_orphan_scan_keeps_epochs_the_leader_gc_still_tracks() {
    set_full_bg();
    failpoint::set("gc.local.min_age_ms", "return(0)").unwrap();
    let d = TempDir::new("gc-local-open");
    init(&d.0);
    let run = start(&d.0);
    let (a, c, me) = (api(&d.0, &run.mgmt_addr), client(&d.0, &run, "admin"), who(1000, 1000));
    set_settings(&run.meta, |s| s.epoch_gc_grace_ms = 600_000);
    let first = put(&c, &me, ROOT_INO, "t", &[1u8; 200_000]).unwrap();
    let truncated = dir_of(&d.0, &first);
    put(&c, &me, ROOT_INO, "t", &[2u8; 100_000]).unwrap(); // 截断、改写：旧 epoch 在 10 min 的宽限期里
    set_settings(&run.meta, |s| s.epoch_gc_grace_ms = 0);
    let m = mkdir(&c, ROOT_INO, "m");
    let w = put(&c, &me, m, "f", &text(2, 300_000)).unwrap();
    let moved = dir_of(&d.0, &w);
    let fc = file_client(&d.0, &run);
    let o = fc.open(&me, w.attr.ino, O_RDONLY).unwrap(); // 只读打开着：迁移照常，换下来的旧 layout 要等它关了
    set(&a, "/m", "compress=lz4").unwrap();
    assert!(wait_converted(&a, "/m", "compress", 60));
    let t = task(&a, "gc-local", "run", None);
    let done = || {
        let r: NodeInfoReply = a.get("/api/v1/node").unwrap();
        r.tasks.iter().any(|x| x.kind == "gc-local" && x.runs >= t.runs && x.state == STATE_DONE)
    };
    assert!(wait_for(30, done), "本地孤儿扫描做完了一轮");
    assert!(truncated.exists(), "还在宽限期里：本地孤儿扫描不删截断换下来的");
    assert!(moved.exists(), "文件还打开着：本地孤儿扫描不删迁移换下来的");
    fc.release(o.fh);
    assert!(wait_for(30, || !moved.exists()), "关掉之后 leader 的 gc 删掉它");
    run.stop();
}

#[test]
fn the_local_orphan_scan_deletes_epoch_directories_the_metadata_no_longer_knows() {
    set_full_bg();
    failpoint::set("gc.local.min_age_ms", "return(0)").unwrap();
    let d = TempDir::new("gc-local");
    init(&d.0);
    let run = start(&d.0);
    set_settings(&run.meta, |s| s.epoch_gc_grace_ms = 0);
    let (a, c, me) = (api(&d.0, &run.mgmt_addr), client(&d.0, &run, "admin"), who(1000, 1000));
    let first = put(&c, &me, ROOT_INO, "g", &[1u8; 200_000]).unwrap();
    let dead = dir_of(&d.0, &first);
    let saved: Vec<(PathBuf, Vec<u8>)> =
        std::fs::read_dir(&dead).unwrap().flatten().map(|e| (e.path(), std::fs::read(e.path()).unwrap())).collect();
    let now = put(&c, &me, ROOT_INO, "g", &[2u8; 200_000]).unwrap(); // 截断：第一个 epoch 作废，gc 删掉它
    assert!(wait_for(30, || !dead.exists()), "gc 删掉了废弃的 epoch");
    // 像节点离线时错过了 DeleteEpoch 一样，把它放回去；再来一个文件删掉之后才落下的 epoch 目录
    std::fs::create_dir_all(&dead).unwrap();
    for (p, b) in &saved {
        std::fs::write(p, b).unwrap();
    }
    let gone = put(&c, &me, ROOT_INO, "h", &[5u8; 100_000]).unwrap();
    let gone_dir = dir_of(&d.0, &gone);
    let gone_saved: Vec<(PathBuf, Vec<u8>)> =
        std::fs::read_dir(&gone_dir).unwrap().flatten().map(|e| (e.path(), std::fs::read(e.path()).unwrap())).collect();
    c.meta.write(&me, MetaOp::Unlink { parent: ROOT_INO, name: b"h".to_vec() }).unwrap();
    assert!(wait_for(30, || !gone_dir.exists()), "孤儿的数据被 gc 删掉");
    std::fs::create_dir_all(&gone_dir).unwrap();
    for (p, b) in &gone_saved {
        std::fs::write(p, b).unwrap();
    }
    let t = task(&a, "gc-local", "run", None);
    assert!(t.runs >= 1);
    assert!(wait_for(30, || !dead.exists() && !gone_dir.exists()), "本地孤儿扫描删掉了元数据里已经死了的 epoch 目录");
    assert!(dir_of(&d.0, &now).exists(), "当前 epoch 的留着");
    assert_eq!(get(&c, &me, now.attr.ino).unwrap(), vec![2u8; 200_000]);
    run.stop();
}

/// 本地孤儿扫描的旧链目录（`GcLocal::old_chains`，审查 #50）：3 个节点、`replicas=1`，节点 1 的 target 上放进别的链的目录（像它很早
/// 以前是成员）——这个 target 不是成员、也不是 last serving、不在 `removed` 里的链，目录 mtime 早于 min_age（fail point 缩成 0）就整个
/// 删掉；min_age 之内写过的（mtime 在将来）、集群图里没有的链（列表比集群图新：刚生成的）都留着。放进去的 epoch 是活的（同一个文件的
/// 当前 layout），删它的只会是"旧链目录"这一步。
#[test]
fn the_local_orphan_scan_deletes_old_chain_directories_but_keeps_unknown_and_recent_ones() {
    set_full_bg();
    failpoint::set("gc.local.min_age_ms", "return(0)").unwrap();
    let c = Cluster::start("gc-chains", 3);
    let (cl, me) = (c.client("admin"), who(1000, 1000));
    let addrs: Vec<String> = (1..=3).map(|i| c.run(i).mgmt_addr.clone()).collect();
    let a = api_at(&c.node_dir(1), &addrs.join(","), "admin");
    let raw = |k: &[u8]| get_raw(&c.run(c.wait_leader()).meta, k).unwrap();
    let node_of = |t: u32| raw(&keys::target(t)).map(|b| decode_v1::<TargetRecV1>(&b).unwrap().node);
    assert!(wait_for(60, || (1..=3).all(|t| node_of(t).is_some())), "三个 target 都登记了");
    set(&a, "/", "replicas=1").unwrap();
    // 一个副本不在节点 1 上的文件：节点 1 的 target 不是它那条链的成员
    let elsewhere = |i: u8| {
        let o = put(&cl, &me, ROOT_INO, &format!("f{i}"), &[i; 100_000]).unwrap();
        let l = o.layout.clone().unwrap();
        let m = decode_v1::<ChainRecV1>(&raw(&keys::chain(l.chains[0])).unwrap()).unwrap().members;
        let id = ChunkId { chain: l.chains[0], ino: o.attr.ino, epoch: l.epoch, idx: 0 };
        (m.len() == 1 && node_of(m[0].0) != Some(1)).then_some((i, id))
    };
    let (i, id) = (0..40).find_map(elsewhere).expect("40 个文件里总有不放在节点 1 上的");
    let t1 = c.node_dir(1).join("targets/1");
    // 在节点 1 的 target 上放一份链 `chain` 的目录；`age` 是它的 mtime 离现在多久（负数 = 在将来）
    let plant = |chain: u32, age: i64| {
        let p = committed_path(&t1, &ChunkId { chain, ..id });
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(&p, b"stale copy").unwrap();
        let at = std::time::SystemTime::now();
        let at = if age >= 0 { at - Duration::from_secs(age as u64) } else { at + Duration::from_secs((-age) as u64) };
        std::fs::File::open(p.parent().unwrap()).unwrap().set_modified(at).unwrap();
        t1.join(format!("c/{chain:08x}"))
    };
    let n1 = api_at(&c.node_dir(1), &addrs[0], "admin");
    let pass = || {
        let r0 = task(&a, "gc-local", "run", None).runs;
        let done = || {
            let r: NodeInfoReply = n1.get("/api/v1/node").unwrap();
            r.tasks.iter().any(|x| x.kind == "gc-local" && x.runs >= r0 && x.state == STATE_DONE)
        };
        assert!(wait_for(30, done), "节点 1 的本地孤儿扫描做完了一轮");
    };
    let (recent, unknown) = (plant(id.chain, -3600), plant(99_999, 3600));
    pass();
    assert!(recent.exists(), "min_age 之内写过的旧链目录留着");
    assert!(unknown.exists(), "集群图里没有的链留着（宁可不删）");
    let old = plant(id.chain, 10);
    pass();
    assert!(!old.exists(), "回不去的旧链目录整个删掉");
    assert!(unknown.exists());
    assert!(get(&cl, &me, id.ino).unwrap() == vec![i; 100_000], "别的节点上的那份不受影响");
}
