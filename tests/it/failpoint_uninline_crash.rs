//! ADR-0034 ③ 与 M8 的数据面 fencing（ADR-0035 第 11 条）：服务端 uninline 提交了意图、写完 chunk、切换之前崩溃（fail point
//! `meta.uninline.before_commit`，3.11 的命名习惯）→ 重启后读到的仍是原 inline 内容，chunk 写在意图的 epoch（`pending_epoch`）下面；
//! 之后再 Uninline 正常——它把上一次留下的意图记为垃圾（GC 记录）、换一个新的 epoch，所以上一次留在 chunk 里的旧内容不会冒出来
//! （先把 inline 内容截短、再扩回来，第二次 Uninline 之后读到的是截短后的内容加 0）。abort 结束整个进程，所以由子进程来做（同
//! failpoint_chunk_torn.rs）。

use std::io::{BufRead, BufReader};
use std::os::unix::process::ExitStatusExt;
use std::path::Path;
use std::process::{Command, Stdio};

use wmfs_core::api::failpoint;
use wmfs_core::base::ErrCode;
use wmfs_core::base::types::ChunkId;
use wmfs_core::meta::schema::keys::{self, GC_EPOCH, ROOT_INO};
use wmfs_core::meta::schema::records::{F_INLINE, InodeV1, decode_v1};
use wmfs_core::proto::meta::{MetaOp, MetaReply, O_RDONLY, SetAttrIn};
use wmfs_plugins::local_fs::committed_path;
use wmfs_testkit::harness::node::{client, get, init, opened, put, start, target_dir, who};

const CHILD: &str = "WMFS_FP_CHILD";

#[test]
#[ignore = "只作为子进程运行（由本文件的其他用例拉起）"]
fn child() {
    let Ok(dir) = std::env::var(CHILD) else { return };
    let dir = Path::new(&dir);
    let run = start(dir);
    let c = client(dir, &run, "admin");
    let me = who(1000, 1000);
    let o = put(&c, &me, ROOT_INO, "x", b"ORIGINAL!").unwrap();
    println!("CONFIRMED {}", o.attr.ino);
    failpoint::set("meta.uninline.before_commit", "abort").unwrap();
    let _ = c.meta.read(&me, MetaOp::Uninline { ino: o.attr.ino });
    panic!("fail point meta.uninline.before_commit 没有触发");
}

fn set_size(size: u64) -> SetAttrIn {
    SetAttrIn { mode: None, uid: None, gid: None, size: Some(size), atime: None, mtime: None, epoch_seen: 0 }
}

#[test]
fn a_crash_between_writing_the_chunk_and_committing_keeps_the_inline_content() {
    let d = std::env::temp_dir().join(format!("wmfs-fp-uninline-{}", std::process::id()));
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
    let ino: u64 = out.iter().find_map(|l| l.split("CONFIRMED ").nth(1)).expect("CONFIRMED").trim().parse().unwrap();
    let run = start(&d);
    let c = client(&d, &run, "admin");
    let me = who(1000, 1000);
    let o = opened(c.meta.read(&me, MetaOp::Open { ino, flags: O_RDONLY })).unwrap();
    assert!(o.attr.flags & F_INLINE != 0, "提交之前崩溃：文件仍是 inline");
    assert_eq!(o.inline.as_deref(), Some(&b"ORIGINAL!"[..]), "读到的仍是原 inline 内容");
    let l = o.layout.clone().unwrap();
    let rec = |run: &wmfs_core::server::Running| {
        decode_v1::<InodeV1>(&run.meta.local_get(&keys::inode(ino)).unwrap().unwrap()).unwrap()
    };
    let pending = rec(&run).pending_epoch;
    assert!(pending > l.epoch, "意图已经提交：pending_epoch {pending} 是新分的（layout 还是 {}）", l.epoch);
    let chunk0 = ChunkId { chain: l.chains[0], ino, epoch: pending, idx: 0 };
    assert!(committed_path(&target_dir(&d), &chunk0).exists(), "崩溃之前 chunk 已经写进意图的 epoch（只是还没切换）");
    // 截短再扩回来：inline 内容 = "ORI"，大小 9；意图的 epoch 里还留着上一次的 "ORIGINAL!"
    opened(c.meta.write(&me, MetaOp::SetAttr { ino, set: set_size(3) })).unwrap();
    opened(c.meta.write(&me, MetaOp::SetAttr { ino, set: set_size(9) })).unwrap();
    let u = opened(c.meta.read(&me, MetaOp::Uninline { ino })).unwrap();
    assert_eq!(u.attr.flags & F_INLINE, 0, "之后再 Uninline 正常");
    let now = u.layout.as_ref().unwrap().epoch;
    assert!(now > pending && rec(&run).pending_epoch == 0, "换了新的 epoch {now}（不复用上一次的意图 {pending}）");
    assert!(
        run.meta.local_get(&keys::gc(GC_EPOCH, ino, pending)).unwrap().is_some(),
        "上一次的意图记为垃圾（GC 记录）"
    );
    assert_eq!(get(&c, &me, ino).unwrap(), b"ORI\0\0\0\0\0\0", "上一次留在 chunk 里的旧内容不会冒出来");
    assert_eq!(c.meta.read(&me, MetaOp::GetInline { ino }), Err(ErrCode::STALE_LAYOUT), "已经不是 inline 了");
    let _ = put(&c, &me, ROOT_INO, "y", b"still works").unwrap();
    let r = c.meta.read(&me, MetaOp::ResolvePath { path: b"/y".to_vec() }).unwrap();
    assert!(matches!(r, MetaReply::Resolved { .. }));
    run.stop();
    std::fs::remove_dir_all(&d).unwrap();
}
