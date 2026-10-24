//! fail point `raft.persist.before_commit` / `raft.persist.after_commit` / `raft.apply.before` = `abort`（design.md 3.11、9.4：
//! "raft.persist.before_commit=abort——重启后不丢已确认写"；roadmap M3 验收）。
//!
//! abort 会结束整个进程，所以由子进程来做：本测试程序把自己当子进程再跑一次（`child` 用例，只在设了 `WMFS_FP_CHILD`
//! 时干活），子进程在**真实的 redb 文件**上起完整的元数据引擎（raft + 执行器线程、真实 fsync），先写 K 个批次（每个确认
//! 打印一行 `CONFIRMED <i>`），然后打开 fail point 接着写——raft 线程在落盘的某一步 abort。父进程确认子进程死于 SIGABRT，
//! 再打开同一个文件、等它成为就绪 leader（重启不 fatal），逐个核对：每个确认过的写都还在。

use std::io::{BufRead, BufReader, Write};
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;

use redb::Database;
use wmfs_core::api::failpoint;
use wmfs_core::hooks::pipeline::Pipeline;
use wmfs_core::hooks::registry::PluginSet;
use wmfs_core::init::initial_kv;
use wmfs_core::meta::engine::{EmbeddedEngine, EngineOpts};
use wmfs_core::meta::raft::storage;
use wmfs_testkit::harness::meta::{CAP_KEY, dkey, get_raw, plugins, put_raw, quiet_logs, sample_init, wait_ready};

const CHILD: &str = "WMFS_FP_CHILD";
const CONFIRMED_BEFORE_FP: u64 = 100;
const SIGABRT: i32 = 6;

fn db_path(dir: &Path) -> PathBuf {
    dir.join("meta.redb")
}

fn open_engine(dir: &Path) -> Arc<EmbeddedEngine> {
    quiet_logs();
    let pipeline = Arc::new(Pipeline::new(PluginSet::new(&[])));
    let snap_dir = dir.to_path_buf();
    let (rpc, adopt_local_plugins) = (None, false);
    let opts =
        EngineOpts { node_id: 1, plugins: plugins(), pipeline, cap_key: CAP_KEY, rpc, snap_dir, adopt_local_plugins };
    let eng = EmbeddedEngine::open(&db_path(dir), opts).expect("打开元数据库");
    wait_ready(&eng);
    eng
}

/// 子进程：写 K 个确认的批次，然后打开 fail point 继续写，直到 raft 线程 abort。
#[test]
#[ignore = "只作为子进程运行（由本文件的其他用例拉起）"]
fn child() {
    let Ok(spec) = std::env::var(CHILD) else { return };
    let (dir, fp) = spec.split_once('|').expect("WMFS_FP_CHILD = 目录|fail point");
    let eng = open_engine(Path::new(dir));
    let mut out = std::io::stdout().lock();
    for i in 0.. {
        if i == CONFIRMED_BEFORE_FP {
            failpoint::set(fp, "abort").unwrap();
        }
        if put_raw(&eng, dkey(&format!("fp-{i}")), vec![i as u8]).is_ok() {
            writeln!(out, "CONFIRMED {i}").unwrap();
            out.flush().unwrap();
        }
        assert!(i < CONFIRMED_BEFORE_FP + 50, "fail point {fp} 一直没有触发");
    }
}

/// 父进程：拉起子进程，等它被 abort，然后重启并核对确认过的写。
fn run(fp: &str) {
    let dir = std::env::temp_dir().join(format!("wmfs-fp-{}-{}", fp.replace('.', "-"), std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let db = Database::create(db_path(&dir)).unwrap();
    storage::bootstrap(&db, &[1], &initial_kv(&sample_init())).unwrap();
    drop(db);
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["child", "--exact", "--ignored", "--nocapture", "--test-threads=1"])
        .env(CHILD, format!("{}|{fp}", dir.display()))
        .env_remove("WMFS_FAILPOINTS")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let lines = BufReader::new(child.stdout.take().unwrap()).lines();
    // libtest 先打印 "test child ... " 不换行，所以第一行是 "test child ... CONFIRMED 0"：按子串找
    let parse = |l: String| l.split("CONFIRMED ").nth(1).and_then(|n| n.trim().parse().ok());
    let confirmed: Vec<u64> = lines.map_while(Result::ok).filter_map(parse).collect();
    let status = child.wait().unwrap();
    assert_eq!(status.signal(), Some(SIGABRT), "{fp}：子进程应死于 abort（实际 {status:?}）");
    assert!(confirmed.len() as u64 >= CONFIRMED_BEFORE_FP, "{fp}：子进程只确认了 {} 个", confirmed.len());
    let eng = open_engine(&dir); // 重启：不 fatal、能成为就绪 leader
    for i in &confirmed {
        assert_eq!(get_raw(&eng, &dkey(&format!("fp-{i}"))), Ok(Some(vec![*i as u8])), "{fp}：确认过的第 {i} 个写丢了");
    }
    eng.shutdown();
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn abort_before_commit_loses_no_confirmed_write() {
    run("raft.persist.before_commit");
}

#[test]
fn abort_after_commit_loses_no_confirmed_write() {
    run("raft.persist.after_commit");
}

#[test]
fn abort_before_apply_loses_no_confirmed_write() {
    run("raft.apply.before");
}
