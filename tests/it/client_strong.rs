//! M12 的 strong 模式与锁在客户端文件层上（`FileClient`，不经 FUSE；两个 FileClient = 两个挂载，design.md 8.5、8.6）：
//! - strong 目录：写打开 inline 文件前先转出；`O_APPEND` 经 AppendReserve——两个客户端并发追加、互不覆盖、各自有序；B 打开着、A 写完，
//!   B 的下一次读立即看到（读前 GetAttr）；
//! - 锁：setlk / getlk（到文件尾）；F_SETLKW 交给 lock-wait 线程（回调在锁放开之后才来，拿到锁时读得到对方放锁之前提交的写——锁即同步点）；
//!   正在被杀（已经不在）的等待者回 `EINTR`、不拿锁；close 时释放（flush 的 `drop_locks`、release 的 `release_locks`）。

use std::sync::Arc;
use std::sync::mpsc::channel;
use std::thread;
use std::time::Duration;

use wmfs_core::base::ErrCode;
use wmfs_core::client::files::FileClient;
use wmfs_core::client::locks::{LockReq, LockWaiter};
use wmfs_core::meta::locks::{LOCK_READ, LOCK_UNLOCK, LOCK_WRITE};
use wmfs_core::meta::schema::records::{F_INLINE, LOCK_EOF};
use wmfs_core::proto::meta::{MetaOp, MetaReply, O_RDONLY, O_RDWR, O_WRONLY};
use wmfs_core::server::Running;
use wmfs_mgr::api::ApiClient;
use wmfs_testkit::harness::bg::{api, set};
use wmfs_testkit::harness::node::{TempDir, file_client, init, start, who};

const O_APPEND: u32 = 0o2000; // Linux（x86_64 与 aarch64 相同）
const HOME: u64 = 16;

fn node(name: &str) -> (TempDir, Running, ApiClient) {
    let d = TempDir::new(&format!("strong-{name}"));
    init(&d.0);
    let run = start(&d.0);
    let a = api(&d.0, &run.mgmt_addr);
    (d, run, a)
}

fn mkdir(fc: &FileClient, a: &ApiClient, name: &str, spec: &str) -> u64 {
    let op = MetaOp::Mkdir { parent: HOME, name: name.as_bytes().to_vec(), mode: 0o777, umask: 0 };
    let MetaReply::Entry { attr } = fc.meta.write(&who(1000, 1000), op).unwrap() else { panic!() };
    set(a, &format!("/home/{name}"), spec).unwrap();
    attr.ino
}

fn lk(owner: u64, start: u64, end: u64, kind: u8) -> LockReq {
    LockReq { owner, start, end, kind, pid: std::process::id() }
}

fn read_all(fc: &FileClient, ino: u64) -> Vec<u8> {
    let me = who(1000, 1000);
    let o = fc.open(&me, ino, O_RDONLY).unwrap();
    let d = fc.read(&me, &fc.handle(o.fh).unwrap(), 0, 1 << 20).unwrap();
    fc.release(o.fh);
    d
}

#[test]
fn strong_appends_from_two_clients_interleave_without_loss() {
    let (d, run, api) = node("append");
    let me = who(1000, 1000);
    let (fa, fb) = (file_client(&d.0, &run), file_client(&d.0, &run));
    let s = mkdir(&fa, &api, "s", "consistency=strong");
    let o = fa.create(&me, s, b"log", 0o644, 0, O_WRONLY | O_APPEND).unwrap();
    assert!(o.direct, "strong：FOPEN_DIRECT_IO");
    let ino = o.attr.ino;
    fa.release(o.fh);
    let go = |fc: Arc<FileClient>, tag: char| {
        thread::spawn(move || {
            let me = who(1000, 1000);
            let o = fc.open(&me, ino, O_WRONLY | O_APPEND).unwrap();
            let h = fc.handle(o.fh).unwrap();
            for i in 0..150 {
                let line = format!("{tag} {i}\n");
                fc.write(&me, &h, 0, line.as_bytes()).unwrap(); // 偏移不用：AppendReserve 分
            }
            fc.flush(o.fh).unwrap();
            fc.release(o.fh);
        })
    };
    let (ta, tb) = (go(fa.clone(), 'A'), go(fb.clone(), 'B'));
    ta.join().unwrap();
    tb.join().unwrap();
    let text = String::from_utf8(read_all(&fb, ino)).unwrap();
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 300, "两个客户端各 150 行，一行不丢");
    for tag in ["A", "B"] {
        let mine: Vec<u32> =
            lines.iter().filter_map(|l| l.strip_prefix(tag).map(|n| n.trim().parse().unwrap())).collect();
        assert_eq!(mine, (0..150).collect::<Vec<u32>>(), "{tag} 的行各自有序、完整");
    }
}

#[test]
fn strong_reads_see_another_clients_write_at_once_and_inline_files_are_converted() {
    let (d, run, api) = node("read");
    let me = who(1000, 1000);
    let (fa, fb) = (file_client(&d.0, &run), file_client(&d.0, &run));
    // 先在 cto 目录里写一个 inline 小文件，再把目录改成 strong：写打开时先转出
    let s = mkdir(&fa, &api, "s", "inline=64K");
    let o = fa.create(&me, s, b"x", 0o644, 0, O_RDWR).unwrap();
    fa.write(&me, &fa.handle(o.fh).unwrap(), 0, b"v1").unwrap();
    fa.flush(o.fh).unwrap();
    fa.release(o.fh);
    set(&api, "/home/s", "consistency=strong").unwrap();
    let ino = o.attr.ino;
    let r = fb.open(&me, ino, O_RDONLY).unwrap(); // B 先打开着
    let w = fa.open(&me, ino, O_RDWR).unwrap();
    let MetaReply::Entry { attr } = fa.meta.read(&me, MetaOp::GetAttr { ino }).unwrap() else { panic!() };
    assert_eq!(attr.flags & F_INLINE, 0, "strong 文件写打开之前已经转成 chunk");
    fa.write(&me, &fa.handle(w.fh).unwrap(), 0, b"version-two").unwrap(); // strong：同步写、同步提交
    let got = fb.read(&me, &fb.handle(r.fh).unwrap(), 0, 4096).unwrap();
    assert_eq!(got, b"version-two", "B 一直开着，写完立即读到（读前 GetAttr）");
    fa.release(w.fh);
    fb.release(r.fh);
}

#[test]
fn blocking_locks_wait_off_thread_and_are_sync_points() {
    let (d, run, api) = node("locks");
    let me = who(1000, 1000);
    let (fa, fb) = (file_client(&d.0, &run), file_client(&d.0, &run));
    let dir = mkdir(&fa, &api, "l", "inline=0");
    let oa = fa.create(&me, dir, b"c", 0o644, 0, O_RDWR).unwrap();
    let ob = fb.open(&me, oa.attr.ino, O_RDWR).unwrap();
    let pid = std::process::id();
    assert_eq!(fa.setlk(&me, oa.fh, &lk(1, 0, u64::MAX, LOCK_WRITE)), Ok(()));
    assert_eq!(fb.setlk(&me, ob.fh, &lk(2, 0, 10, LOCK_READ)), Err(ErrCode::EAGAIN));
    assert_eq!(
        fb.getlk(&me, ob.fh, &lk(2, 5, 5, LOCK_READ)).unwrap().map(|l| (l.end, l.write)),
        Some((LOCK_EOF, true))
    );
    // F_SETLKW：交给 lock-wait 线程；锁放开之前不回调
    let (tx, rx) = channel();
    let done = Box::new(move |r| tx.send(r).unwrap());
    fb.wait_lock(LockWaiter { who: me.clone(), fh: ob.fh, req: lk(2, 0, LOCK_EOF, LOCK_WRITE), tid: pid, done });
    assert!(rx.recv_timeout(Duration::from_millis(400)).is_err(), "A 还拿着锁：B 继续等");
    // A 写、解锁（解锁之前先提交）→ B 拿到锁，读得到 A 的写（同步点）
    fa.write(&me, &fa.handle(oa.fh).unwrap(), 0, b"from-a").unwrap();
    assert_eq!(fa.setlk(&me, oa.fh, &lk(1, 0, LOCK_EOF, LOCK_UNLOCK)), Ok(()));
    assert_eq!(rx.recv_timeout(Duration::from_secs(5)), Ok(Ok(())), "A 放锁之后 B 的轮询拿到锁");
    assert_eq!(fb.read(&me, &fb.handle(ob.fh).unwrap(), 0, 64).unwrap(), b"from-a");
    // 正在死去（已经不在）的等待者：回 EINTR，不拿锁
    let mut child = std::process::Command::new("true").spawn().unwrap();
    let dead = child.id();
    child.wait().unwrap();
    let (tx, rx) = channel();
    let done = Box::new(move |r| tx.send(r).unwrap());
    fa.wait_lock(LockWaiter { who: me.clone(), fh: oa.fh, req: lk(1, 0, 0, LOCK_READ), tid: dead, done });
    assert_eq!(rx.recv_timeout(Duration::from_secs(5)), Ok(Err(ErrCode::EINTR)));
    assert_eq!(fa.setlk(&me, oa.fh, &lk(1, 0, 0, LOCK_READ)), Err(ErrCode::EAGAIN), "B 仍持有写锁");
    // close 时释放：flush（drop_locks：这个 owner 的）之后 A 拿得到；release（release_locks：记在这个句柄上的）也解掉
    fb.drop_locks(ob.fh, 2);
    assert_eq!(fa.setlk(&me, oa.fh, &lk(1, 0, 0, LOCK_READ)), Ok(()));
    assert_eq!(fb.setlk(&me, ob.fh, &lk(2, 0, 0, LOCK_WRITE)), Err(ErrCode::EAGAIN));
    fa.release_locks(oa.fh);
    assert_eq!(fb.setlk(&me, ob.fh, &lk(2, 0, 0, LOCK_WRITE)), Ok(()), "A 的句柄 release 时解掉了记在它上面的锁");
    fb.release_locks(ob.fh);
    fa.release(oa.fh);
    fb.release(ob.fh);
}
