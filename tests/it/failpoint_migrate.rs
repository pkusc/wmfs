//! 迁移原语与协调器的 fail point（design.md 3.10、3.9、3.11）：
//! - `bg.migrate.before_switch`（数据都按新布局写好、切换之前）：拖住迁移，期间另一个客户端改写这个文件——切换的 CAS 失败、新 epoch
//!   记为垃圾，协调器下一次按新内容再迁移：内容不丢、最后一致；
//! - `reconciler.full_every_ms`（全量扫描的间隔 24 h → 0.5 s）：人为改坏一个目录的递归计数，全量扫描的在线自检把它修正回来，changelog
//!   有 `RStatDrift`（extra 是差值；字段名与次序是金样）。
//!
//! - `plugin.reconciler.bg_task`、`plugin.scrub.bg_task`（框架包在每一步外层的插件 fail point）：这一步失败 → 计入
//!   `wmfs_task_errors_total`，稍后从同一个游标重试，转换与 scrub 的一轮照样做完；
//! - `chunk.sync.eio`（迁移把新布局的数据 SyncChunks 时失败）：不切换（换布局之前数据必须持久）、这一次作废，之后重做成功；
//! - `reconciler.full_every_ms` 之下，旧 leader 留下的迁移意图在声明已经一致时也被放弃（ADR-0042 第 10 条）；
//! - `chunk.commit.before_rename` + `clock.wall.skew_ms`：拷得久的迁移途中重签 cap，一次迁成（审查 #28）。
//!
//! fail point 是进程级的：单独一个测试程序，几个用例互斥地跑（同一时刻只有一个节点在迁移）。

use std::sync::Mutex;
use std::time::Duration;

use wmfs_core::api::types::{NodeInfoReply, STATE_DONE};
use wmfs_core::api::{failpoint, metrics};
use wmfs_core::data::format::CMP_LZ4;
use wmfs_core::meta::schema::keys::{self, GC_EPOCH, ROOT_INO};
use wmfs_core::meta::schema::records::{F_INLINE, InodeV1, RStatV1, decode_v1, encode_v1};
use wmfs_core::proto::meta::{MetaOp, O_RDONLY};
use wmfs_testkit::harness::bg::{api, changelog, extra_keys, mkdir, set, task, wait_converted};
use wmfs_testkit::harness::meta::{get_raw, put_raw};
use wmfs_testkit::harness::node::{
    TempDir, client, get, init, opened, put, set_full_bg, set_settings, start, wait_for, who,
};

static ONE_AT_A_TIME: Mutex<()> = Mutex::new(());

fn hits(name: &str) -> u64 {
    metrics::counter("wmfs_failpoint_hits_total", "", &[("name", name)]).get()
}

#[test]
fn a_write_while_the_migration_copies_is_not_lost() {
    let _one = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    set_full_bg();
    let d = TempDir::new("fp-migrate-write");
    init(&d.0);
    let run = start(&d.0);
    set_settings(&run.meta, |s| s.epoch_gc_grace_ms = 0);
    let (a, c, me) = (api(&d.0, &run.mgmt_addr), client(&d.0, &run, "admin"), who(1000, 1000));
    let m = mkdir(&c, ROOT_INO, "m");
    let old = vec![b'o'; 300_000];
    let ino = put(&c, &me, m, "f", &old).unwrap().attr.ino;
    let n0 = hits("bg.migrate.before_switch");
    failpoint::set("bg.migrate.before_switch", "times(1,sleep(3000))").unwrap();
    set(&a, "/m", "compress=lz4").unwrap();
    assert!(wait_for(20, || hits("bg.migrate.before_switch") > n0), "迁移走到了切换之前");
    // 迁移睡着的这 3 s 里：截断、写入新内容、提交（参考客户端的 put）
    let new: Vec<u8> = (0..400_000u32).map(|i| (i % 251) as u8).collect();
    put(&c, &me, m, "f", &new).unwrap();
    let retried = metrics::counter("wmfs_migrations_total", "", &[("result", "retry")]);
    assert!(wait_for(20, || retried.get() >= 1), "切换的 CAS 失败，这一次作废");
    assert!(wait_converted(&a, "/m", "compress", 60), "协调器按新内容再迁移一次");
    let o = opened(c.meta.read(&me, MetaOp::Open { ino, flags: O_RDONLY })).unwrap();
    assert_eq!(o.layout.unwrap().compress, CMP_LZ4);
    assert!(get(&c, &me, ino).unwrap() == new, "迁移途中写的新内容没有丢");
    run.stop();
}

#[test]
fn a_drifted_recursive_count_is_fixed_by_the_full_scan() {
    let _one = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    set_full_bg();
    let d = TempDir::new("fp-rstat-drift");
    init(&d.0);
    let run = start(&d.0);
    let (a, c, me) = (api(&d.0, &run.mgmt_addr), client(&d.0, &run, "admin"), who(1000, 1000));
    let r = mkdir(&c, ROOT_INO, "r");
    put(&c, &me, r, "x", &[1u8; 70_000]).unwrap();
    let rstat = |dir: u64| -> RStatV1 { decode_v1(&get_raw(&run.meta, &keys::rstat(dir)).unwrap().unwrap()).unwrap() };
    let (good_r, good_root) = (rstat(r), rstat(ROOT_INO));
    assert_eq!((good_r.files, good_r.bytes), (1, 70_000));
    let bad = RStatV1 { bytes: good_r.bytes + 777, files: good_r.files + 2, ..good_r.clone() };
    put_raw(&run.meta, keys::rstat(r), encode_v1(&bad)).unwrap(); // 人为改坏（不经命名空间操作）
    failpoint::set("reconciler.full_every_ms", "return(500)").unwrap();
    let fixed = || {
        changelog(&a, 0).events.iter().any(|e| {
            let has = |k: &str, v: &str| e.extra.iter().any(|(x, y)| x == k && y == v);
            e.op == "RStatDrift" && e.ino == r && has("bytes", "-777") && has("files", "-2")
        })
    };
    assert!(wait_for(30, fixed), "全量扫描的在线自检写了 RStatDrift（extra 是差值）");
    let keys = extra_keys(&a, "RStatDrift", None);
    assert_eq!(keys, "bytes,files,dirs", "金样：extra 的字段名与次序（M11 审查，ADR-0042 第 2 条）");
    assert!(
        wait_for(10, || rstat(r) == good_r && rstat(ROOT_INO) == good_root),
        "{:?} {:?}",
        rstat(r),
        rstat(ROOT_INO)
    );
    failpoint::clear("reconciler.full_every_ms");
    std::thread::sleep(Duration::from_millis(100));
    run.stop();
}

#[test]
fn a_failing_reconciler_or_scrub_step_is_retried_from_the_same_cursor() {
    let _one = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    set_full_bg();
    let d = TempDir::new("fp-bg-plugins");
    init(&d.0);
    let run = start(&d.0);
    let (a, c, me) = (api(&d.0, &run.mgmt_addr), client(&d.0, &run, "admin"), who(1000, 1000));
    let n = mkdir(&c, ROOT_INO, "n");
    let data = vec![b'n'; 100_000];
    let ino = put(&c, &me, n, "f", &data).unwrap().attr.ino;
    let errors = |t: &str| metrics::counter("wmfs_task_errors_total", "", &[("task", t)]).get();
    let (r0, s0) = (errors("reconciler"), errors("scrub"));
    failpoint::set("plugin.reconciler.bg_task", "times(1,return(EIO))").unwrap();
    failpoint::set("plugin.scrub.bg_task", "times(1,return(EIO))").unwrap();
    set(&a, "/n", "compress=lz4").unwrap();
    let runs = task(&a, "scrub", "run", None).runs;
    assert!(wait_converted(&a, "/n", "compress", 60), "失败的一步之后照样转换完");
    assert!(
        wait_for(30, || errors("reconciler") > r0 && errors("scrub") > s0),
        "两次失败都计入了 wmfs_task_errors_total"
    );
    let scrubbed = || {
        let r: NodeInfoReply = a.get("/api/v1/node").unwrap();
        r.tasks.iter().any(|t| t.kind == "scrub" && t.runs >= runs && t.state == STATE_DONE && t.backlog == 0)
    };
    assert!(wait_for(60, scrubbed), "scrub 的这一轮也做完了");
    assert!(get(&c, &me, ino).unwrap() == data);
    run.stop();
}

#[test]
fn a_migration_whose_new_data_cannot_be_synced_does_not_switch() {
    let _one = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    set_full_bg();
    let d = TempDir::new("fp-migrate-sync");
    init(&d.0);
    let run = start(&d.0);
    let (a, c, me) = (api(&d.0, &run.mgmt_addr), client(&d.0, &run, "admin"), who(1000, 1000));
    let s = mkdir(&c, ROOT_INO, "s");
    let data = vec![b's'; 300_000];
    let ino = put(&c, &me, s, "f", &data).unwrap().attr.ino;
    let retried = metrics::counter("wmfs_migrations_total", "", &[("result", "retry")]);
    let r0 = retried.get();
    failpoint::set("chunk.sync.eio", "times(1,return(EIO))").unwrap();
    set(&a, "/s", "compress=lz4").unwrap();
    assert!(wait_for(20, || retried.get() > r0), "SyncChunks 失败：这一次作废");
    assert!(wait_converted(&a, "/s", "compress", 60), "之后重做成功");
    let o = opened(c.meta.read(&me, MetaOp::Open { ino, flags: O_RDONLY })).unwrap();
    assert_eq!(o.layout.unwrap().compress, CMP_LZ4);
    assert!(get(&c, &me, ino).unwrap() == data);
    failpoint::clear("chunk.sync.eio");
    run.stop();
}

/// 旧 leader 留下的迁移意图（`pending_epoch ≠ 0`）、而文件的声明已经一致了（ADR-0042 第 10 条）：协调器的全量扫描照样把它交给迁移原语，
/// 放弃那个意图——清掉 `pending_epoch`、写它的 GC 记录（kind 2，带当前布局的链）；不然它一直被当成活的，写了一半的数据永不回收。
#[test]
fn a_stale_migration_intent_is_abandoned_even_if_the_file_already_matches() {
    let _one = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    set_full_bg();
    let d = TempDir::new("fp-stale-intent");
    init(&d.0);
    let run = start(&d.0);
    let (c, me) = (client(&d.0, &run, "admin"), who(1000, 1000));
    let data = vec![b'p'; 100_000];
    let ino = put(&c, &me, ROOT_INO, "f", &data).unwrap().attr.ino;
    let inode = || decode_v1::<InodeV1>(&get_raw(&run.meta, &keys::inode(ino)).unwrap().unwrap()).unwrap();
    let mut i = inode();
    assert!(i.flags & F_INLINE == 0 && i.pending_epoch == 0);
    i.max_epoch += 1;
    i.pending_epoch = i.max_epoch; // 像旧 leader 写下意图、还没拷完就没了
    let stale = i.pending_epoch;
    put_raw(&run.meta, keys::inode(ino), encode_v1(&i)).unwrap();
    failpoint::set("reconciler.full_every_ms", "return(500)").unwrap();
    assert!(wait_for(30, || inode().pending_epoch == 0), "全量扫描放弃了那个意图");
    let gc = get_raw(&run.meta, &keys::gc(GC_EPOCH, ino, stale)).unwrap();
    assert!(gc.is_some(), "那个 epoch 记为垃圾（宽限期 60 s 里还在）");
    assert_eq!(inode().layout.unwrap().epoch, i.layout.unwrap().epoch, "布局没换");
    assert!(get(&c, &me, ino).unwrap() == data);
    failpoint::clear("reconciler.full_every_ms");
    run.stop();
}

/// 迁移途中 cap 的有效期过了一半就重签（审查 #28，3.10 注记 ⑤）：`cap_ttl` 调成 2 s、每个 chunk 的提交拖 300 ms
/// （`chunk.commit.before_rename` = `sleep(300)`），拷到一半时把墙钟往前拨 10 min（`clock.wall.skew_ms`，像拷了很久）——
/// 不重签的话之后的写 `BAD_CAP`、这一次作废重做；重签了就一次迁成。墙钟拨过之后管理 API 的签名对不上（客户端用的是真实时钟），
/// 这段时间直接看 inode。"一次迁成"按这个文件自己的 `max_epoch` 判断（每次尝试的意图都用一个新 epoch）：重做计数
/// `wmfs_migrations_total{result="retry"}` 是整个进程共用的，同一测试程序里别的用例留下的节点也会往上加（M9 交接 ⑦）。
#[test]
fn a_long_copy_renews_its_caps_and_switches_in_one_go() {
    let _one = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    set_full_bg();
    let d = TempDir::new("fp-migrate-renew");
    init(&d.0);
    let run = start(&d.0);
    set_settings(&run.meta, |s| s.cap_ttl_ms = 2000);
    let (a, c, me) = (api(&d.0, &run.mgmt_addr), client(&d.0, &run, "admin"), who(1000, 1000));
    let r = mkdir(&c, ROOT_INO, "r");
    set(&a, "/r", "chunk=1M").unwrap();
    let data: Vec<u8> = (0..(6u32 << 20)).map(|i| (i % 253) as u8).collect();
    let ino = put(&c, &me, r, "f", &data).unwrap().attr.ino;
    let inode = || decode_v1::<InodeV1>(&get_raw(&run.meta, &keys::inode(ino)).unwrap().unwrap()).unwrap();
    let (e0, n0) = (inode().max_epoch, hits("chunk.commit.before_rename"));
    failpoint::set("chunk.commit.before_rename", "sleep(300)").unwrap();
    set(&a, "/r", "compress=lz4").unwrap();
    assert!(wait_for(30, || hits("chunk.commit.before_rename") >= n0 + 2), "迁移拷了两个 chunk");
    failpoint::set("clock.wall.skew_ms", "return(600000)").unwrap();
    let switched = wait_for(60, || inode().layout.is_some_and(|l| l.compress == CMP_LZ4));
    failpoint::clear("clock.wall.skew_ms");
    failpoint::clear("chunk.commit.before_rename");
    assert!(switched, "迁移完成了");
    assert_eq!(inode().max_epoch, e0 + 1, "途中重签了 cap：只用了一个新 epoch，没有作废重做");
    assert!(get(&c, &me, ino).unwrap() == data);
    run.stop();
}
