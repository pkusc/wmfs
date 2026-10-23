//! ADR-0034 ②：服务端的 Uninline 与另一个客户端的 inline 补丁并发 → CAS 失败、从头重做 → 结果正确。用 fail point
//! `meta.uninline.before_commit = sleep(500)` 把"写完 chunk、提交之前"拉长：这期间另一个客户端提交一个 inline 补丁（change_id 变了），
//! 于是第一次 CAS 失败，第二次读到补丁之后的内容、重新写 chunk、CAS 成功（fail point 被经过两次）。最后按 chunk 读到的是补丁之后的内容。
//! 第二段：同一个 inode 的两个 Uninline 并发——服务端同时只做一个（另一个回 BUSY，客户端退避重试时已经不是 inline，直接成功），
//! 所以只有一个进入写 chunk 的阶段（fail point 只经过一次），先完成的那个之后客户端写进 chunk 的数据不会被落后的那个覆盖。
//! fail point 是进程级的：单独一个测试程序，两段按顺序在同一个测试里跑。

use std::time::Duration;

use wmfs_core::api::{failpoint, metrics};
use wmfs_core::meta::schema::keys::ROOT_INO;
use wmfs_core::meta::schema::records::F_INLINE;
use wmfs_core::proto::meta::{MetaOp, O_RDWR};
use wmfs_testkit::harness::node::{client, commit, get, init, opened, put, start, who, write_chunks};

fn hits() -> u64 {
    metrics::counter("wmfs_failpoint_hits_total", "", &[("name", "meta.uninline.before_commit")]).get()
}

#[test]
fn an_inline_patch_during_uninline_makes_the_cas_retry_and_wins() {
    let d = std::env::temp_dir().join(format!("wmfs-fp-race-{}", std::process::id()));
    init(&d);
    let run = start(&d);
    let a = client(&d, &run, "admin");
    let b = client(&d, &run, "admin"); // 另一个客户端（另一个会话）
    let me = who(1000, 1000);
    let o = put(&a, &me, ROOT_INO, "f", b"abcdefghij").unwrap();
    let ino = o.attr.ino;
    failpoint::set("meta.uninline.before_commit", "sleep(500)").unwrap();
    let meta = a.meta.clone();
    let mover = std::thread::spawn(move || meta.read(&who(1000, 1000), MetaOp::Uninline { ino }));
    std::thread::sleep(Duration::from_millis(200)); // 服务端已经把旧内容写进 chunk，正睡在提交之前
    let ob = opened(b.meta.read(&me, MetaOp::Open { ino, flags: O_RDWR })).unwrap();
    commit(&b, &me, &ob, 10, vec![(0, b"PATCHED".to_vec())]).expect("文件还是 inline：补丁照常提交");
    let u = opened(mover.join().unwrap()).unwrap();
    assert_eq!(u.attr.flags & F_INLINE, 0);
    assert_eq!(hits(), 2, "第一次 CAS 失败（期间有人改了 inline 内容），从头重做一次");
    assert_eq!(get(&a, &me, ino).unwrap(), b"PATCHEDhij", "chunk 里是补丁之后的内容");

    // 第二段：同一个 inode 的两个 Uninline 并发
    let o = put(&a, &me, ROOT_INO, "g", b"0123456789").unwrap();
    let ino = o.attr.ino;
    let before = hits();
    let meta = a.meta.clone();
    let first = std::thread::spawn(move || meta.read(&who(1000, 1000), MetaOp::Uninline { ino }));
    std::thread::sleep(Duration::from_millis(100)); // 第一个正睡在提交之前
    let second = opened(b.meta.read(&me, MetaOp::Uninline { ino })).unwrap();
    assert_eq!(second.attr.flags & F_INLINE, 0, "第二个等第一个做完（BUSY → 退避重试），之后已经不是 inline");
    let first = opened(first.join().unwrap()).unwrap();
    failpoint::clear("meta.uninline.before_commit");
    assert_eq!(first.attr.flags & F_INLINE, 0);
    assert_eq!(hits() - before, 1, "同一个 inode 同时只有一个 Uninline 在写 chunk");
    let tail = vec![7u8; 70_000];
    write_chunks(&b, &second, 10, &tail).unwrap();
    commit(&b, &me, &second, 70_010, vec![]).unwrap();
    std::thread::sleep(Duration::from_millis(600)); // 没有落后的 Uninline 还会再写 chunk 0
    let all = get(&a, &me, ino).unwrap();
    assert!(
        all[..10] == b"0123456789"[..] && all[10..] == tail[..],
        "客户端写进 chunk 的数据没有被旧的 inline 内容覆盖"
    );
    run.stop();
    std::fs::remove_dir_all(&d).unwrap();
}
