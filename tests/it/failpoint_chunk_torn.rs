//! fail point `chunk.pending.torn`（design.md 3.11、9.4；roadmap M5 的验收"注入'pending 文件写到一半崩溃'后重启无损坏读"）：只写一半
//! pending 文件然后 abort 整个进程。abort 会结束整个进程，所以由子进程来做：本测试程序把自己当子进程再跑一次（`child` 用例，只在设了
//! `WMFS_FP_CHILD` 时干活），子进程在真实的目录上起完整的节点、写好 v1（打印 `CONFIRMED`）、打开 fail point 再写 v2——io 线程写到
//! 一半 abort。父进程确认子进程死于 SIGABRT，再在同一个目录上重启节点：启动清理删掉 `p/` 里写了一半的文件，读到的正好是 v1。

use std::io::{BufRead, BufReader};
use std::os::unix::process::ExitStatusExt;
use std::path::Path;
use std::process::{Command, Stdio};

use wmfs_core::api::failpoint;
use wmfs_core::meta::schema::keys::ROOT_INO;
use wmfs_core::proto::meta::{MetaOp, O_RDWR};
use wmfs_testkit::harness::node::{client, get, init, opened, put, start, target_dir, who, write_chunks};

const CHILD: &str = "WMFS_FP_CHILD";

fn v1() -> Vec<u8> {
    (0..6_000_000u32).map(|i| (i % 251) as u8).collect()
}

#[test]
#[ignore = "只作为子进程运行（由本文件的其他用例拉起）"]
fn child() {
    let Ok(dir) = std::env::var(CHILD) else { return };
    let dir = Path::new(&dir);
    let run = start(dir);
    let c = client(dir, &run, "admin");
    let me = who(1000, 1000);
    let o = put(&c, &me, ROOT_INO, "f", &v1()).unwrap();
    let o = opened(c.meta.read(&me, MetaOp::Open { ino: o.attr.ino, flags: O_RDWR })).unwrap();
    println!("CONFIRMED");
    failpoint::set("chunk.pending.torn", "abort").unwrap();
    let _ = write_chunks(&c, &o, 0, &vec![9u8; 3_000_000]);
    panic!("fail point chunk.pending.torn 没有触发");
}

#[test]
fn a_torn_pending_file_is_never_read_after_restart() {
    let d = std::env::temp_dir().join(format!("wmfs-fp-torn-{}", std::process::id()));
    init(&d);
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["child", "--exact", "--ignored", "--nocapture", "--test-threads=1"])
        .env(CHILD, &d)
        .env_remove("WMFS_FAILPOINTS")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let out: Vec<String> = BufReader::new(child.stdout.take().unwrap()).lines().map_while(Result::ok).collect();
    let status = child.wait().unwrap();
    assert_eq!(status.signal(), Some(6), "子进程应死于 abort（实际 {status:?}）");
    assert!(out.iter().any(|l| l.contains("CONFIRMED")), "子进程写好了 v1：{out:?}");
    assert!(
        std::fs::read_dir(target_dir(&d).join("p")).unwrap().count() > 0,
        "崩溃时 p/ 里留下了写了一半的 pending 文件"
    );
    let run = start(&d); // 重启：启动清理
    assert_eq!(std::fs::read_dir(target_dir(&d).join("p")).unwrap().count(), 0, "启动时清空 p/（R5）");
    let c = client(&d, &run, "admin");
    let me = who(1000, 1000);
    let ino = match c.meta.read(&me, MetaOp::ResolvePath { path: b"/f".to_vec() }).unwrap() {
        wmfs_core::proto::meta::MetaReply::Resolved { ino, .. } => ino,
        other => panic!("{other:?}"),
    };
    assert!(get(&c, &me, ino).unwrap() == v1(), "读到的正好是 v1：没有损坏读");
    run.stop();
    std::fs::remove_dir_all(&d).unwrap();
}
