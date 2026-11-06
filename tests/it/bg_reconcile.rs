//! 协调器与迁移原语（design.md 3.10、4.12；roadmap M11 的验收）：完整的单节点（[`set_full_bg`]：装上协调器、scrub 与 gc 的本地孤儿
//! 扫描），经管理 API（与 `wmfs set`、`wmfs task`、`wmfs datakey` 同一个 `ApiClient`）：
//! - `compress=lz4`、`compress=zstd:5`、`encrypt=tenant:k1`：已有的 chunk 文件与 inline 小文件都换到新布局、内容不变，status 的"实际"
//!   跟上声明；压缩之后落盘的明显变小；加密之后 target 目录里找不到明文、inline 文件转成了 chunk；
//! - 一直写打开着的文件：协调器暂停时 status 已经单独数到它（open_files）；恢复之后被跳过、fail point `bg.migrate.open_alert_ms` 把 24 h
//!   缩成 0 → `PolicyViolation`（kind=migration_blocked_by_open）、指标 `wmfs_migration_skipped_open_files`；关掉之后迁移完成，写的内容都在；
//! - 任务控制：暂停期间不迁移；限速写进任务记录；不认识的任务 404；
//! - 改回 `encrypt=none`：迁移的意图用声明的 inline 阈值，status 的 inline 一行也一致（ADR-0042 第 11 条）；
//! - 节点任务重启之后本地游标文件里是 Done：隔满 interval 才开始下一轮；
//! - 遍历器（`bg::walker`）：子树 DFS 每批 ≤ 256 个目录项、游标续跑、每项恰好一次、目录先于它的内容；全量扁平扫描按 ino 升序；
//! - 数据密钥的包裹（`data::datakey::Wrapper`，加密插件 aes-gcm 做的）：附加数据含集群号与名字，换了就解不开；M11 包好的一份照样解得开；
//! - 金样（M11 审查）：`SwitchLayout`、`DataKeyCreate`、`PolicyViolation`（kind=migration_blocked_by_open）、`TaskControl`
//!   的 extra 字段名与次序；compress、encrypt 在 DIRATTR 里存的形式。
//!
//! fail point 是进程级的：这里只用 `bg.migrate.open_alert_ms`（只影响告警的时刻，同一个进程里别的用例不受影响）；在切换之前插一脚的
//! 用例在 `failpoint_migrate.rs`。

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use wmfs_core::api::hex::unhex;
use wmfs_core::api::types::{NodeInfoReply, STATE_DONE, STATE_PAUSED, STATE_RUNNING, TaskInfo, TaskSetReq};
use wmfs_core::api::{failpoint, metrics};
use wmfs_core::base::{ErrCode, clock};
use wmfs_core::bg::progress::{DONE, LocalTaskV1, new_rec, save_local};
use wmfs_core::bg::walker::{PAGE, Walker};
use wmfs_core::buf::{MemPool, PoolConfig};
use wmfs_core::data::datakey::Wrapper;
use wmfs_core::data::format::{CMP_LZ4, CMP_ZSTD, ENC_AES256GCM};
use wmfs_core::hooks::pipeline::Pipeline;
use wmfs_core::hooks::registry::PluginSet;
use wmfs_core::meta::raft::membership::Members;
use wmfs_core::meta::schema::keys::{self, ROOT_INO};
use wmfs_core::meta::schema::records::{AttrMapV1, F_INLINE, KIND_DIR, LayoutV1, TaskRecV1, decode_v1};
use wmfs_core::proto::meta::{MetaOp, O_RDONLY, O_RDWR};
use wmfs_plugins::aes_gcm::AesGcm;
use wmfs_testkit::harness::bg::{
    MARK, api, api_at, attr, datakey, extra_keys, has_event, mkdir, set, status, task, tasks, text, wait_converted,
};
use wmfs_testkit::harness::cluster::Cluster;
use wmfs_testkit::harness::meta::get_raw;
use wmfs_testkit::harness::node::{
    Client, TempDir, client, file_client, get, init, node_dir, opened, put, set_full_bg, set_settings, start,
    target_dir, wait_for, who,
};

/// 一个文件此刻的 layout 与 flags（只读 Open：参考客户端不登记打开，不挡 gc）。
fn now(c: &Client, ino: u64) -> (LayoutV1, u32) {
    let o = opened(c.meta.read(&who(1000, 1000), MetaOp::Open { ino, flags: O_RDONLY })).unwrap();
    (o.layout.unwrap(), o.attr.flags)
}

/// `dir` 下全部文件（递归）：(路径, 内容)。gc 正在删的目录读不到就跳过。
fn files(dir: &Path) -> Vec<(PathBuf, Vec<u8>)> {
    let (mut out, mut stack) = (vec![], vec![dir.to_path_buf()]);
    while let Some(d) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&d) else { continue };
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if let Ok(b) = std::fs::read(&p) {
                out.push((p, b));
            }
        }
    }
    out
}

/// `dir` 下的文件实际占用的空间（chunk 文件是稀疏的：压缩省下的空间靠洞省下来，看分配的块，不看文件长度）。
fn stored(dir: &Path) -> u64 {
    use std::os::unix::fs::MetadataExt;
    files(dir).iter().filter_map(|(p, _)| std::fs::metadata(p).ok()).map(|m| m.blocks() * 512).sum()
}

fn plaintext(dir: &Path) -> Vec<PathBuf> {
    files(dir).into_iter().filter(|(_, b)| b.windows(MARK.len()).any(|w| w == MARK)).map(|(p, _)| p).collect()
}

#[test]
fn compress_and_encrypt_convert_existing_files_without_changing_them() {
    set_full_bg();
    let d = TempDir::new("reconcile-convert");
    init(&d.0);
    let run = start(&d.0);
    set_settings(&run.meta, |s| s.epoch_gc_grace_ms = 0); // 旧 epoch 一换下来就可以删（演示也这样）
    let (a, c, me) = (api(&d.0, &run.mgmt_addr), client(&d.0, &run, "admin"), who(1000, 1000));
    let p = mkdir(&c, ROOT_INO, "p");
    let mut all: Vec<(u64, Vec<u8>)> = (0..3u32)
        .map(|i| {
            let data = text(i, 300_000 + i as usize * 1000);
            (put(&c, &me, p, &format!("f{i}"), &data).unwrap().attr.ino, data)
        })
        .collect();
    let small = text(9, 100);
    let s = put(&c, &me, p, "small", &small).unwrap().attr.ino;
    assert!(now(&c, s).1 & F_INLINE != 0, "小文件是 inline 的");
    all.push((s, small));
    let same = |what: &str| {
        for (ino, data) in &all {
            assert!(get(&c, &me, *ino).unwrap() == *data, "{what}：inode {ino} 的内容变了");
        }
    };
    let td = target_dir(&d.0);
    let before = stored(&td);

    // lz4：提醒已有文件要等协调器；转换完成后 status 的实际与声明一致、内容不变、落盘的明显变小
    let r = set(&a, "/p", "compress=lz4").unwrap();
    assert!(r.warnings.iter().any(|w| w.contains("已有文件由协调器在后台转换")), "{:?}", r.warnings);
    assert!(wait_converted(&a, "/p", "compress", 60), "{:?}", attr(&status(&a, "/p"), "compress"));
    let st = status(&a, "/p");
    let row = attr(&st, "compress");
    assert_eq!((row.actual.files, row.actual.mismatched_files, row.actual.migrates), (Some(4), Some(0), Some(true)));
    assert!(st.reconciler.is_some(), "status 带着协调器的进度");
    for (ino, _) in &all[..3] {
        assert_eq!(now(&c, *ino).0.compress, CMP_LZ4);
    }
    same("lz4");
    assert!(wait_for(30, || stored(&td) * 3 < before), "压缩之后落盘的明显变小：{} → {}", before, stored(&td));

    // zstd:5（级别也算：只有 zstd 有级别）
    set(&a, "/p", "compress=zstd:5").unwrap();
    assert!(wait_converted(&a, "/p", "compress", 60));
    let l = now(&c, all[0].0).0;
    assert_eq!((l.compress, l.compress_level), (CMP_ZSTD, 5));
    same("zstd:5");

    // 数据密钥 + 加密：inline 的小文件也转成 chunk（inline 内容不能加密），target 上找不到明文
    let k = datakey(&a, "k1");
    assert!(k >= 1);
    set(&a, "/p", "encrypt=tenant:k1").unwrap();
    assert!(wait_converted(&a, "/p", "encrypt", 60), "{:?}", attr(&status(&a, "/p"), "encrypt"));
    for (ino, _) in &all {
        let (l, flags) = now(&c, *ino);
        assert_eq!((l.encrypt, l.encrypt_key, flags & F_INLINE), (ENC_AES256GCM, k, 0), "inode {ino}");
    }
    assert_eq!(attr(&status(&a, "/p"), "encrypt").declared, "tenant:k1");
    // 金样（M11 审查）：DIRATTR 里存的形式——compress 存 `zstd:<级别>`，encrypt 存数据密钥的十进制 id、不存名字
    // （3.8 的 M11 注记）
    let m = decode_v1::<AttrMapV1>(&get_raw(&run.meta, &keys::dirattr(p)).unwrap().unwrap()).unwrap().entries;
    assert_eq!((m["compress"].as_str(), m["encrypt"].as_str()), ("zstd:5", k.to_string().as_str()));
    put(&c, &me, p, "after", &text(5, 1000)).unwrap(); // 加密之后新建的：不 inline
    let inline = attr(&status(&a, "/p"), "inline");
    assert_eq!((inline.actual.mismatched_files, inline.mismatched), (Some(0), false), "加密决定的不 inline 不算不一致");
    same("encrypt");
    assert!(wait_for(30, || plaintext(&td).is_empty()), "target 上还有明文：{:?}", plaintext(&td));

    // 再改回不加密（ADR-0042 第 11 条）：迁移的意图用声明的 inline 阈值（不沿用加密布局里的 0），转换完 inline 一行也一致
    set(&a, "/p", "encrypt=none").unwrap();
    assert!(wait_converted(&a, "/p", "encrypt", 60), "{:?}", attr(&status(&a, "/p"), "encrypt"));
    let inline = attr(&status(&a, "/p"), "inline");
    assert_eq!((inline.actual.mismatched_files, inline.mismatched), (Some(0), false), "解密之后 inline 阈值回到声明的");
    same("encrypt=none");
    // 金样（M11 审查，ADR-0042 第 2 条）：这两个操作的 extra 字段名与次序（只增）
    assert_eq!(extra_keys(&a, "SwitchLayout", None), "old_epoch,new_epoch,replicas,compress,encrypt_key");
    assert_eq!(extra_keys(&a, "DataKeyCreate", None), "id,name,tenant");
    run.stop();
}

/// 节点任务重启之后本地游标文件里是 Done（那一轮刚做完）：隔满 `interval()`（scrub 是 24 h）才开始下一轮，不因为重启就把数据再读一遍
/// （3.10 注记 ①）。没有游标文件的 gc-local 一启动就做一轮；它做完之后 scrub 照样还是那一轮。
#[test]
fn a_node_task_done_before_a_restart_waits_out_its_interval() {
    set_full_bg();
    let d = TempDir::new("bg-restart-done");
    init(&d.0);
    let rec = TaskRecV1 { state: DONE, pass: 5, updated_ms: clock::wall_ms(), ..new_rec() };
    save_local(&node_dir(&d.0).join("run"), "scrub", &LocalTaskV1 { rec, run_seen: 0 }).unwrap();
    let run = start(&d.0);
    let a = api(&d.0, &run.mgmt_addr);
    let node = |kind: &str| {
        let r: NodeInfoReply = a.get("/api/v1/node").unwrap();
        r.tasks.into_iter().find(|t| t.kind == kind).unwrap()
    };
    let first = || node("gc-local").pass >= 1 && node("gc-local").state == STATE_DONE;
    assert!(wait_for(30, first), "没有游标文件的 gc-local 一启动就做完了一轮");
    assert!(!wait_for(2, || node("scrub").pass != 5), "scrub 那一轮刚做完：不因为重启就再来一轮");
    assert_eq!(node("scrub").state, STATE_DONE);
    run.stop();
}

#[test]
fn a_file_kept_open_for_writing_is_skipped_counted_alerted_and_migrated_after_close() {
    set_full_bg();
    failpoint::set("bg.migrate.open_alert_ms", "return(0)").unwrap(); // 24 h → 第一次跳过就告警
    let d = TempDir::new("reconcile-open");
    init(&d.0);
    let run = start(&d.0);
    set_settings(&run.meta, |s| s.epoch_gc_grace_ms = 0);
    let (a, c, me) = (api(&d.0, &run.mgmt_addr), client(&d.0, &run, "admin"), who(1000, 1000));
    let q = mkdir(&c, ROOT_INO, "q");
    let closed = text(1, 200_000);
    let other = put(&c, &me, q, "closed", &closed).unwrap().attr.ino;
    // 一个挂载写打开着 w、写了一段（已提交），一直不关
    let fc = file_client(&d.0, &run);
    let o = fc.create(&me, q, b"w", 0o644, 0, O_RDWR).unwrap();
    let mut data = text(2, 200_000);
    fc.write(&me, &fc.handle(o.fh).unwrap(), 0, &data).unwrap();
    fc.flush(o.fh).unwrap();

    // 暂停协调器：status 先数到写打开着的那个（会话心跳 3 s 一次报上打开集合）
    assert_eq!(task(&a, "reconciler", "pause", None).state, STATE_PAUSED);
    let sched = || tasks(&a).into_iter().find(|t| t.kind == "reconciler").map(|t| t.state);
    assert!(wait_for(10, || sched().as_deref() == Some(STATE_PAUSED)), "调度器每秒读一次控制字段：等它自己也停了");
    set(&a, "/q", "compress=lz4").unwrap();
    let row = || attr(&status(&a, "/q"), "compress").actual;
    assert!(wait_for(15, || row().open_files == Some(1)), "{:?}", row());
    std::thread::sleep(std::time::Duration::from_millis(2500)); // 暂停着的任务不迁移（协调器每秒一轮）
    assert_eq!(row().mismatched_files, Some(2), "暂停期间不迁移");
    let t = task(&a, "reconciler", "set", Some(TaskSetReq { bw: Some("8M".into()), items: Some(500) }));
    assert_eq!((t.bw, t.items, t.state.as_str()), (8 << 20, 500, STATE_PAUSED), "限速写进任务记录，不改暂停");

    // 恢复：关着的那个迁移了；写打开着的被跳过、计数、告警
    assert_eq!(task(&a, "reconciler", "resume", None).state, STATE_RUNNING);
    assert!(wait_for(30, || row().mismatched_files == Some(1)), "{:?}", row());
    let blocked = Some(("kind", "migration_blocked_by_open"));
    assert!(wait_for(15, || has_event(&a, "PolicyViolation", blocked)), "跳过超过告警时长 → PolicyViolation");
    // 指标是进程级的：同一个进程里别的用例的节点各加各的（都 ≥ 0，`Migrator::show`），所以看 ≥ 1
    let skipped = metrics::gauge("wmfs_migration_skipped_open_files", "", &[]);
    assert!(skipped.get() >= 1.0, "因写打开被跳过的文件数：{}", skipped.get());
    assert_eq!(now(&c, other).0.compress, CMP_LZ4);
    assert_eq!(get(&c, &me, other).unwrap(), closed);
    assert_eq!(now(&c, o.attr.ino).0.compress, 0, "写打开着：还是旧布局");

    // 再写一段、关掉：心跳报上关闭之后，协调器过一会儿重走这一项，迁移完成，两段都在
    let more = text(3, 50_000);
    fc.write(&me, &fc.handle(o.fh).unwrap(), data.len() as u64, &more).unwrap();
    fc.flush(o.fh).unwrap();
    fc.release(o.fh);
    data.extend_from_slice(&more);
    assert!(wait_converted(&a, "/q", "compress", 60), "{:?}", row());
    assert_eq!(row().open_files, Some(0));
    assert_eq!(now(&c, o.attr.ino).0.compress, CMP_LZ4);
    assert!(get(&c, &me, o.attr.ino).unwrap() == data, "写打开期间写的内容都在");
    let r: Vec<TaskInfo> = tasks(&a).into_iter().filter(|t| t.kind == "reconciler").collect();
    assert!(r.len() == 1 && r[0].done >= 2 && r[0].bw == 8 << 20, "{r:?}");
    let e = a.post::<_, TaskInfo>("/api/v1/tasks/nope/pause", &TaskSetReq::default()).unwrap_err();
    assert_eq!(e.status, 404, "不认识的任务");
    // 金样（M11 审查，ADR-0042 第 2 条）：extra 的字段名与次序（只增）
    assert_eq!(extra_keys(&a, "PolicyViolation", blocked), "kind,skipped_ms");
    assert_eq!(extra_keys(&a, "TaskControl", Some(("action", "pause"))), "kind,action");
    assert_eq!(extra_keys(&a, "TaskControl", Some(("action", "set"))), "kind,action,bw,items");
    run.stop();
}

/// 暂停 → 换 leader → 恢复：新 leader 读回的记录是 Paused，那只是控制字段——恢复之后照常把已有文件转换完，不会做一步就又把自己停了；
/// 暂停之后库里一直是 Paused（调度器写进度时保留它）。
#[test]
fn resuming_after_a_leader_change_keeps_the_reconciler_running() {
    set_full_bg();
    let c = Cluster::start("bg-pause", 3);
    let (cl, me) = (c.client("admin"), who(1000, 1000));
    let p = mkdir(&cl, ROOT_INO, "p");
    for i in 0..6 {
        put(&cl, &me, p, &format!("f{i}"), &vec![b'a' + i as u8; 1 << 20]).unwrap();
    }
    let addrs: Vec<String> = (1..=3).map(|i| c.run(i).mgmt_addr.clone()).collect();
    let a = api_at(&c.node_dir(1), &addrs.join(","), "admin");
    task(&a, "reconciler", "set", Some(TaskSetReq { bw: Some("1M".into()), items: None }));
    set(&a, "/p", "compress=lz4").unwrap();
    assert!(wait_for(20, || attr(&status(&a, "/p"), "compress").actual.mismatched_files < Some(6)), "开始转换了");
    assert_eq!(task(&a, "reconciler", "pause", None).state, STATE_PAUSED);
    let state = || tasks(&a).into_iter().find(|t| t.kind == "reconciler").map(|t| t.state);
    assert!(wait_for(10, || state().as_deref() == Some(STATE_PAUSED)), "leader 上的调度器停了");
    std::thread::sleep(std::time::Duration::from_secs(6)); // 过了一个持久化间隔（5 s）：库里仍是 Paused
    assert_eq!(state().as_deref(), Some(STATE_PAUSED));
    let left = attr(&status(&a, "/p"), "compress").actual.mismatched_files;
    assert!(left > Some(0), "暂停时还没转换完：{left:?}");
    let old = c.wait_leader();
    let new = (1..=3).find(|i| *i != old).unwrap();
    c.run(old).meta.transfer_leader(new as u32);
    assert!(wait_for(20, || c.leader() == Some(new)), "换到 node{new}");
    assert_eq!(task(&a, "reconciler", "resume", None).state, STATE_RUNNING);
    let row = || attr(&status(&a, "/p"), "compress").actual.mismatched_files;
    assert!(wait_for(60, || row() == Some(0)), "恢复之后转换完；还剩 {:?}，任务 {:?}", row(), state());
}

#[test]
fn the_walker_pages_through_a_subtree_once_and_scans_inodes_in_order() {
    let d = TempDir::new("walker");
    init(&d.0);
    let run = start(&d.0);
    let (c, me) = (client(&d.0, &run, "admin"), who(1000, 1000));
    let create = |parent: u64, name: &str| {
        let op = MetaOp::Create { parent, name: name.as_bytes().to_vec(), mode: 0o644, umask: 0, excl: true };
        opened(c.meta.write(&me, op)).unwrap().attr.ino
    };
    let w = mkdir(&c, ROOT_INO, "w");
    let mut want: HashSet<u64> = (0..300).map(|i| create(w, &format!("f{i:03}"))).collect();
    let s1 = mkdir(&c, w, "s1");
    let s2 = mkdir(&c, s1, "s2");
    want.extend([s1, s2]);
    want.extend((0..10).map(|i| create(s2, &format!("g{i}"))));
    let (walker, mut cursor, mut seen, mut pages) = (Walker::new(), vec![], Vec::new(), 0);
    loop {
        let page = walker.subtree(&*run.meta, w, &cursor).unwrap();
        assert!(page.items.len() <= PAGE, "一批最多 {PAGE} 个目录项");
        seen.extend(page.items.iter().map(|x| (x.ino, x.parent, x.inode.kind)));
        pages += 1;
        if page.done {
            break;
        }
        cursor = page.cursor;
    }
    assert!(pages >= 2, "300 多项要分几批");
    let inos: Vec<u64> = seen.iter().map(|x| x.0).collect();
    assert_eq!(inos.iter().copied().collect::<HashSet<_>>(), want, "每项恰好一次");
    assert_eq!(inos.len(), want.len());
    let at = |ino: u64| inos.iter().position(|x| *x == ino).unwrap();
    assert!(at(s1) < at(s2) && seen.iter().filter(|x| x.1 == s2).all(|x| at(s2) < at(x.0)), "目录先于它的内容");
    assert!(seen.iter().all(|x| x.2 != KIND_DIR || x.0 == s1 || x.0 == s2));
    assert_eq!(walker.subtree(&*run.meta, w, b"bad").err(), Some(ErrCode::EINVAL), "游标坏了");
    let (mut after, mut all) = (0, vec![]);
    loop {
        let page = walker.flat(&*run.meta, after, 100).unwrap();
        all.extend(page.items.iter().map(|x| x.ino));
        after = page.cursor;
        if page.done {
            break;
        }
    }
    assert!(all.windows(2).all(|x| x[0] < x[1]), "按 ino 升序");
    assert!(all.contains(&ROOT_INO) && all.contains(&w) && want.iter().all(|i| all.contains(i)));
    run.stop();
}

#[test]
fn data_keys_are_wrapped_with_the_cluster_and_name_as_associated_data() {
    let mut set = PluginSet::new(&[]);
    set.transform(Arc::new(AesGcm::new()));
    set.validate(&BTreeMap::new()).unwrap();
    let pool = MemPool::new(&PoolConfig::small()).unwrap();
    let w = Wrapper { pipeline: Arc::new(Pipeline::new(set)), pool: pool.clone(), key: [7; 32], cluster_id: [1; 16] };
    let k = [9u8; 32];
    let b = w.seal_datakey("k1", &k).unwrap();
    assert_eq!(b.len(), 12 + 32 + 16, "nonce ‖ 密文 ‖ tag");
    // 金样（M11 审查）：M11 包好的一份 DATAKEY 内容（nonce ‖ 密文 ‖ tag，附加数据 WMFS-DKEY1 ‖ cluster_id ‖ 名字）以后照样解得开
    let m11 = [
        "1cf8cdadeac7d79e1c2aba31",
        "33ea707ab7b0e339c2337f985f07056c017428fb8a5a72612ea4bf063a2aa543",
        "900ef95f84785960d84eb04a4d19cbe7",
    ];
    let mut old = [0u8; 60];
    assert!(unhex(&m11.concat(), &mut old));
    assert_eq!(w.open_datakey("k1", &old).unwrap(), k, "包裹的格式与附加数据没有变");
    assert!(!b.windows(32).any(|x| x == k), "包好的里面没有明文的密钥");
    assert_eq!(w.open_datakey("k1", &b).unwrap(), k);
    assert_ne!(w.seal_datakey("k1", &k).unwrap(), b, "每次包裹的 nonce 都是新的");
    assert!(w.open_datakey("k2", &b).is_err(), "换了名字解不开");
    let other = Wrapper { cluster_id: [2; 16], pipeline: w.pipeline.clone(), pool: pool.clone(), key: w.key };
    assert!(other.open_datakey("k1", &b).is_err(), "换了集群解不开");
    let mut bad = b.clone();
    bad[20] ^= 1;
    assert!(w.open_datakey("k1", &bad).is_err() && w.open_datakey("k1", &b[1..]).is_err(), "改过、截短的解不开");
    let none =
        Wrapper { pipeline: Arc::new(Pipeline::new(PluginSet::new(&[]))), pool, key: [7; 32], cluster_id: [1; 16] };
    assert_eq!(none.seal_datakey("k1", &k).err(), Some(ErrCode::EINVAL), "没有加密插件：建不了数据密钥");
}
