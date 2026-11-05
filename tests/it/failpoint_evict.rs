//! 会话驱逐（design.md 3.4.4、3.11 的 `client.heartbeat.stop`，ADR-0023）：完整的单节点 + FileClient（与 `wmfs mount` 里的一样）
//! 与一个参考客户端，会话租约调成 10 s（最小值）。停掉心跳之后：
//! - 约 10 s 后第一阶段（EVICTED 记录、map_epoch + 1），然后第二阶段删掉会话记录（EVICTED 记录留着）；第二阶段做完的时候数据节点
//!   一定已经拒绝这个会话的 cap（`SESSION_EVICTED`：锁只在数据节点确认之后才释放）；changelog 里有 `SessionEvicted`；
//! - 恢复心跳之后：FileClient 自动开了新会话；旧句柄（文件与目录的，M10 审查）`EIO`，没提交的写（`client.flush.before_commit`
//!   让它一直提交不了）丢失、不会被新会话写出去；新打开的文件正常读写。在途的写回见 `evict_inflight.rs`。
//!
//! fail point 是进程级的，所以单独一个测试程序。

use wmfs_core::api::failpoint;
use wmfs_core::api::types::{AttrSetReply, AttrSetReq, ChangelogPage, KeyFile};
use wmfs_core::base::ErrCode;
use wmfs_core::meta::schema::keys::{self, ROOT_INO};
use wmfs_core::proto::meta::{MetaOp, O_RDWR};
use wmfs_mgr::api::ApiClient;
use wmfs_testkit::harness::node::{
    TempDir, client, file_client, init, node_dir, opened, set_settings, start, wait_for, who, write_chunks,
};

#[test]
fn a_silent_client_is_evicted_in_two_phases() {
    let d = TempDir::new("evict");
    init(&d.0);
    let run = start(&d.0);
    set_settings(&run.meta, |s| s.session_lease_ms = 10_000);
    let key = KeyFile::parse(&std::fs::read_to_string(node_dir(&d.0).join("keys/admin.key")).unwrap()).unwrap();
    let api = ApiClient::new(vec![run.mgmt_addr.clone()], key);
    let _: AttrSetReply = api.post("/api/v1/attr", &AttrSetReq { path: "/".into(), spec: "inline=0".into() }).unwrap();
    let (fc, raw, me) = (file_client(&d.0, &run), client(&d.0, &run, "client"), who(1000, 1000));
    let o = fc.create(&me, ROOT_INO, b"f", 0o644, 0, O_RDWR).unwrap();
    let ino = o.attr.ino;
    fc.write(&me, &fc.handle(o.fh).unwrap(), 0, b"committed").unwrap();
    fc.fsync(o.fh).unwrap();
    let ro = opened(raw.meta.read(&me, MetaOp::Open { ino, flags: O_RDWR })).unwrap(); // 参考客户端的写 cap
    write_chunks(&raw, &ro, 0, b"c").unwrap();

    let dh = fc.open_dir(&me, ROOT_INO).unwrap();
    failpoint::set("client.flush.before_commit", "return(EIO)").unwrap();
    fc.write(&me, &fc.handle(o.fh).unwrap(), 9, b"LOST").unwrap(); // 长度之外的追加：一直提交不了
    failpoint::set("client.heartbeat.stop", "return(1)").unwrap();
    let (sid, rsid) = (fc.meta.session(), raw.meta.session());
    let get = |k: Vec<u8>| run.meta.local_get(&k).unwrap();
    assert!(wait_for(30, || get(keys::evicted(sid)).is_some()), "10 s 没有心跳：第一阶段");
    assert!(wait_for(15, || get(keys::session(sid)).is_none()), "数据节点确认之后：第二阶段删掉会话记录");
    assert!(wait_for(15, || get(keys::session(rsid)).is_none()), "参考客户端也一样");
    assert_eq!(write_chunks(&raw, &ro, 0, b"x"), Err(ErrCode::SESSION_EVICTED), "第二阶段之后数据节点一定拒绝它的 cap");
    assert!(get(keys::evicted(sid)).is_some(), "EVICTED 记录保留 24 h");
    let page: ChangelogPage = api.get("/api/v1/changelog?after=0&limit=1000").unwrap();
    assert!(page.events.iter().any(|e| e.op == "SessionEvicted" && e.session == sid), "changelog 有 SessionEvicted");

    failpoint::clear("client.heartbeat.stop");
    failpoint::clear("client.flush.before_commit");
    // 恢复之后的第一个操作是一次新的 open：它在自停闸门上发现被驱逐、换了新会话，打开的句柄属于新会话
    let n = fc.open(&me, ino, O_RDWR).unwrap();
    let h = fc.handle(n.fh).unwrap();
    assert_ne!(fc.meta.session(), sid, "自动开了新会话");
    assert_eq!(fc.handle(o.fh).err(), Some(ErrCode::EIO), "旧句柄：EIO");
    assert_eq!(fc.fsync(o.fh), Err(ErrCode::EIO));
    assert_eq!(fc.dir(dh).err(), Some(ErrCode::EIO), "旧的目录句柄：EIO（快照就在本地也一样）");
    fc.close_dir(dh);
    fc.release(o.fh);
    assert_eq!(fc.read(&me, &h, 0, 100).unwrap(), b"committed", "没提交的写丢了");
    fc.write(&me, &h, 9, b"new").unwrap();
    fc.fsync(n.fh).unwrap();
    assert_eq!(fc.read(&me, &h, 0, 100).unwrap(), b"committednew", "新会话正常写；旧的脏数据没有被写出去");
    fc.release(n.fh);
}
