//! 会话驱逐里不用 fail point 的用例（`evict_inflight.rs` 的 fail point 是进程级的，所以分开）：
//! - 调小会话租约的过渡（ADR-0038 第 5 条，M10 的会话驱逐照做）：一个客户端按旧租约（20 s）开了会话、随后失联（不发心跳），
//!   紧接着租约被调小到 10 s——授予方判定过期用 max(当前值, settings_prev)，所以它在旧租约期满之前**不**被驱逐，期满之后才被驱逐。
//! - 换了会话之后经旧句柄的写（ADR-0041 第 9 条，核对时实测复现的回归）：它回 `SESSION_EVICTED`、不在脏区里留下属于旧会话的空
//!   chunk；新句柄随后写同一个 chunk 的数据照常写出、提交。

use std::collections::BTreeMap;
use std::time::Instant;

use wmfs_core::api::types::{AttrSetReply, AttrSetReq, ClusterSettingsReply, KeyFile, SessionRow};
use wmfs_core::base::ErrCode;
use wmfs_core::buf::PoolConfig;
use wmfs_core::meta::schema::keys::{self, ROOT_INO};
use wmfs_core::net::client::Endpoint;
use wmfs_core::proto::meta::{MetaOp, MetaReply, O_RDWR};
use wmfs_mgr::api::ApiClient;
use wmfs_testkit::harness::node::{
    TempDir, connect_silent, file_client, init, node_dir, set_settings, start, wait_for, who,
};

#[test]
fn shrinking_the_session_lease_does_not_evict_before_the_old_lease_ends() {
    let d = TempDir::new("evict-lease");
    init(&d.0);
    let run = start(&d.0);
    set_settings(&run.meta, |s| s.session_lease_ms = 20_000);
    let ep = Endpoint { node: 1, addrs: run.addrs.clone() };
    let c = connect_silent(&node_dir(&d.0), &[ep], "client", PoolConfig::small()); // 不发心跳：开完会话就"失联"
    let (t0, sid) = (Instant::now(), c.meta.session());
    let key = KeyFile::parse(&std::fs::read_to_string(node_dir(&d.0).join("keys/admin.key")).unwrap()).unwrap();
    let api = ApiClient::new(vec![run.mgmt_addr.clone()], key);
    let m: BTreeMap<String, String> = [("session_lease".to_string(), "10s".to_string())].into();
    let r: ClusterSettingsReply = api.post("/api/v1/cluster/settings", &m).unwrap();
    assert_eq!(r.settings["session_lease"], "10s");
    let evicted = || run.meta.local_get(&keys::evicted(sid)).unwrap().is_some();
    assert!(!wait_for(16, evicted), "旧租约（20 s）期满之前不驱逐：按调小之后的 10 s 就提前了");
    assert!(wait_for(20, evicted), "旧租约期满之后驱逐");
    let at = t0.elapsed().as_secs_f64();
    assert!(at >= 19.0, "被驱逐的时刻 {at:.1} s 应在旧租约（20 s）期满前后");
}

/// 手动驱逐、换了新会话之后，经驱逐之前取到的旧句柄写 chunk 1（FUSE 的 write 已经过了 `handle()` 的核对、还没写进脏区）→
/// `SESSION_EVICTED`；新句柄写满 chunk 1（后台写线程写出）、再写 chunk 2 的开头、close → 成功，chunk 1 的数据都在。修之前：旧句柄
/// 先插入一个属于旧会话的空 chunk 再回错，新句柄的数据写进它、被当成旧会话的一批丢掉，close 却返回成功。
#[test]
fn an_old_handle_write_after_the_switch_leaves_nothing_behind() {
    const K: usize = 64 << 10;
    let d = TempDir::new("evict-switch");
    init(&d.0);
    let run = start(&d.0);
    let key = KeyFile::parse(&std::fs::read_to_string(node_dir(&d.0).join("keys/admin.key")).unwrap()).unwrap();
    let api = ApiClient::new(vec![run.mgmt_addr.clone()], key);
    let (fc, me, root) = (file_client(&d.0, &run), who(1000, 1000), who(0, 0));
    let mkdir = MetaOp::Mkdir { parent: ROOT_INO, name: b"e".to_vec(), mode: 0o777, umask: 0 };
    let MetaReply::Entry { attr: e } = fc.meta.write(&root, mkdir).unwrap() else { panic!() };
    let spec = "inline=0,chunk=64K".into();
    let _: AttrSetReply = api.post("/api/v1/attr", &AttrSetReq { path: "/e".into(), spec }).unwrap();
    let o = fc.create(&me, e.ino, b"a", 0o644, 0, O_RDWR).unwrap();
    fc.write(&me, &fc.handle(o.fh).unwrap(), 0, b"committed").unwrap();
    fc.fsync(o.fh).unwrap();
    let (old, sid) = (fc.handle(o.fh).unwrap(), fc.meta.session());
    let _: SessionRow = api.post(&format!("/api/v1/sessions/{sid:x}/evict"), &()).unwrap();
    let switched = || {
        let _ = fc.meta.read(&root, MetaOp::GetAttr { ino: ROOT_INO }); // 回 SESSION_EVICTED → 换新会话
        fc.meta.session() != sid
    };
    assert!(wait_for(20, switched), "换了新会话");
    assert_eq!(fc.write(&me, &old, K as u64, b"old"), Err(ErrCode::SESSION_EVICTED), "旧句柄：不写进脏区");
    let n = fc.open(&me, o.attr.ino, O_RDWR).unwrap();
    let h = fc.handle(n.fh).unwrap();
    fc.write(&me, &h, K as u64, &vec![b'N'; K]).unwrap(); // 写满 chunk 1：交给后台写线程
    fc.write(&me, &h, 2 * K as u64, b"tail").unwrap();
    assert_eq!(fc.flush(n.fh), Ok(()), "新句柄的 close");
    fc.release(n.fh);
    let m = fc.open(&me, o.attr.ino, O_RDWR).unwrap();
    assert_eq!(m.attr.size, 2 * K as u64 + 4);
    assert_eq!(
        fc.read(&me, &fc.handle(m.fh).unwrap(), K as u64, K as u32).unwrap(),
        vec![b'N'; K],
        "chunk 1 的数据都在"
    );
    fc.release(m.fh);
    fc.release(o.fh);
}
