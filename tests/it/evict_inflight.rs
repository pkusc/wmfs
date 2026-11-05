//! 会话被驱逐时**正在写出去**的写回数据（M10 审查发现的阻断问题，design.md 3.4.4："写回缓冲里未提交的数据丢失"）：这批数据的请求
//! 只在它所属的旧会话里发，换了新会话之后不能被写出、提交；新句柄读自己的写时也看不到它。两种在途的情形同时发生在一次驱逐里
//! （会话租约 10 s，目录 `chunk=64K`）：
//! - A：写满一个 chunk（后台写线程马上写出）、chunk 已写进链，`client.flush.before_commit` 让它在提交之前睡 20 s——醒来时会话已经换了；
//! - B：停掉心跳 4.5 s 之后覆盖写开头（已有长度之内：一旦写进链就可见），5 s 后 flusher 取走它时已过 0.8 × 租约，卡在自停闸门上，
//!   直到驱逐做完、恢复心跳、换了新会话。
//!
//! 之后：两个文件新打开读到的都是驱逐之前提交的内容，A 的长度没有被提交上去；旧句柄 `EIO`。fail point 是进程级的，所以单独一个测试程序。

use std::time::{Duration, Instant};

use wmfs_core::api::failpoint;
use wmfs_core::api::types::{AttrSetReply, AttrSetReq, KeyFile};
use wmfs_core::base::ErrCode;
use wmfs_core::meta::schema::keys::{self, ROOT_INO};
use wmfs_core::meta::schema::records::{InodeV1, decode_v1};
use wmfs_core::proto::meta::{MetaOp, MetaReply, O_RDWR};
use wmfs_mgr::api::ApiClient;
use wmfs_testkit::harness::node::{TempDir, file_client, init, node_dir, set_settings, start, wait_for, who};

const K: usize = 64 << 10;

#[test]
fn writes_in_flight_when_the_session_is_evicted_are_never_written_out() {
    let d = TempDir::new("evict-inflight");
    init(&d.0);
    let run = start(&d.0);
    set_settings(&run.meta, |s| s.session_lease_ms = 10_000);
    let key = KeyFile::parse(&std::fs::read_to_string(node_dir(&d.0).join("keys/admin.key")).unwrap()).unwrap();
    let api = ApiClient::new(vec![run.mgmt_addr.clone()], key);
    let me = who(1000, 1000);
    let (fc, root) = (file_client(&d.0, &run), who(0, 0));
    let MetaReply::Entry { attr } =
        fc.meta.write(&root, MetaOp::Mkdir { parent: ROOT_INO, name: b"e".to_vec(), mode: 0o777, umask: 0 }).unwrap()
    else {
        panic!()
    };
    let _: AttrSetReply =
        api.post("/api/v1/attr", &AttrSetReq { path: "/e".into(), spec: "inline=0,chunk=64K".into() }).unwrap();
    let mut files = Vec::new();
    for name in [b"a", b"b"] {
        let o = fc.create(&me, attr.ino, name, 0o644, 0, O_RDWR).unwrap();
        fc.write(&me, &fc.handle(o.fh).unwrap(), 0, b"committed").unwrap();
        fc.fsync(o.fh).unwrap();
        files.push(o);
    }
    let (a, b) = (&files[0], &files[1]);
    let size = |ino: u64| decode_v1::<InodeV1>(&run.meta.local_get(&keys::inode(ino)).unwrap().unwrap()).unwrap().size;

    // A：写满第二个 chunk → 后台写线程马上写出，提交之前睡 20 s
    failpoint::set("client.flush.before_commit", "times(1,sleep(20000))").unwrap();
    let t_a = Instant::now();
    fc.write(&me, &fc.handle(a.fh).unwrap(), K as u64, &vec![b'L'; K]).unwrap();
    let sid = fc.meta.session();
    failpoint::set("client.heartbeat.stop", "return(1)").unwrap();
    // B：4.5 s 之后（租约还新鲜）覆盖写开头；5 s 后 flusher 取走它时已经自停
    std::thread::sleep(Duration::from_millis(4500));
    fc.write(&me, &fc.handle(b.fh).unwrap(), 0, b"STALE").unwrap();

    let get = |k: Vec<u8>| run.meta.local_get(&k).unwrap();
    assert!(wait_for(30, || get(keys::evicted(sid)).is_some()), "10 s 没有心跳：第一阶段");
    assert!(wait_for(15, || get(keys::session(sid)).is_none()), "数据节点确认之后：第二阶段");
    failpoint::clear("client.heartbeat.stop");
    assert!(wait_for(15, || fc.meta.session() != sid), "恢复心跳之后换了新会话");
    assert!(t_a.elapsed() < Duration::from_secs(19), "A 还在提交之前睡着（否则这个测试测不到在途的那一批）");

    // A 那一批还在途（睡着）：新句柄读不到它（读自己的写也不看已被驱逐的会话的数据）
    for o in &files {
        let n = fc.open(&me, o.attr.ino, O_RDWR).unwrap();
        assert_eq!(fc.read(&me, &fc.handle(n.fh).unwrap(), 0, 2 * K as u32).unwrap(), b"committed");
        fc.release(n.fh);
    }
    assert_eq!(fc.handle(a.fh).err(), Some(ErrCode::EIO), "旧句柄：EIO");
    // 等 A 那一批醒来（20 s 的睡眠过去）、B 那一批过了闸门，再看一次：都没有在新会话里写出、提交
    while t_a.elapsed() < Duration::from_secs(23) {
        std::thread::sleep(Duration::from_millis(200));
    }
    assert_eq!((size(a.attr.ino), size(b.attr.ino)), (9, 9), "A 的长度没有被提交上去");
    for o in &files {
        let n = fc.open(&me, o.attr.ino, O_RDWR).unwrap();
        assert_eq!(fc.read(&me, &fc.handle(n.fh).unwrap(), 0, 2 * K as u32).unwrap(), b"committed", "旧数据没有写出去");
        fc.release(n.fh);
    }
    for o in &files {
        fc.release(o.fh);
    }
    failpoint::clear("client.flush.before_commit");
}
