//! MetaCall 消息族的服务端（design.md 3.3.3、3.3.4）经真实的 RPC（tcp，127.0.0.1:0）：调试键空间的 DebugPut/DebugGet、
//! 只给 admin key 且要 `debug_ops`、还没实现的操作回 FEATURE_UNSUPPORTED（用 GetLock 举例，锁在 M12）；握手认证用已应用的
//! KEYMETA（M3 的 KeyLookup 切换：撤销一把 key 之后新连接被拒）。元数据引擎跑在模拟掉电的内存后端上。

use std::sync::Arc;

use wmfs_core::api::metrics;
use wmfs_core::api::types::KeyFile;
use wmfs_core::base::ErrCode;
use wmfs_core::base::config::{NetConfig, PluginTable};
use wmfs_core::base::types::Role;
use wmfs_core::hooks::pipeline::Pipeline;
use wmfs_core::hooks::registry::PluginSet;
use wmfs_core::init::initial_keymeta;
use wmfs_core::meta::engine::EmbeddedEngine;
use wmfs_core::meta::schema::keys;
use wmfs_core::meta::schema::records::{KeyMetaV1, encode_v1};
use wmfs_core::meta::service::MetaService;
use wmfs_core::net::client::{Endpoint, Family, check_reply};
use wmfs_core::net::server::{RpcServer, ServerId};
use wmfs_core::proto::frame::{FrameHeader, FrameKind, MSG_META_CALL, OutFrame};
use wmfs_core::proto::meta::{MetaCall, MetaOp, MetaReply, ReqHead};
use wmfs_core::proto::{decode_body, encode_body};
use wmfs_plugins::shared_key::SharedKey;
use wmfs_plugins::tcp::TcpTransport;
use wmfs_testkit::harness::meta::{bootstrap, engine, plugins, put_raw, sample_init};
use wmfs_testkit::harness::net::{CLUSTER, MASTER, T, client, key, pool};

/// 在 tcp 的 127.0.0.1:0 上起一个带元数据服务的 RPC 服务端；认证读引擎里已应用的 KEYMETA。
fn server(debug_ops: bool) -> (Arc<RpcServer>, Endpoint, Arc<EmbeddedEngine>) {
    let eng = engine(&bootstrap(&[1]), plugins());
    let cfg: PluginTable = "listen = \"127.0.0.1:0\"".parse().unwrap();
    let mut set = PluginSet::new(&[]);
    set.transport(Arc::new(TcpTransport::new(&cfg, 5000).unwrap()));
    set.authn(Arc::new(SharedKey::new()));
    let pipeline = Arc::new(Pipeline::new(set));
    let keys = Arc::new(eng.keys(MASTER));
    let rpc =
        RpcServer::new(ServerId { cluster_id: CLUSTER, node_id: 1 }, pipeline.clone(), keys, &NetConfig::default());
    rpc.add_service(MSG_META_CALL, MetaService::start(eng.clone(), debug_ops).unwrap());
    let p = pool();
    let t = &pipeline.transports()[0];
    t.register_region(&p.region()).unwrap();
    let addr = rpc.listen(t, p).unwrap().unwrap();
    (rpc, Endpoint { node: 1, addrs: vec![("tcp".into(), addr)] }, eng)
}

fn meta(ep: &Endpoint, k: KeyFile, role: Role, op: MetaOp) -> Result<MetaReply, ErrCode> {
    let head = ReqHead { session: 0, seq: 0, uid: 0, gid: 0, groups: vec![], pid: 1, job_id: 0 };
    let f = OutFrame {
        hdr: FrameHeader::new(FrameKind::Request, MSG_META_CALL, 0, 0),
        body: encode_body(&MetaCall { head, op }),
        bulk: vec![],
        recv_into: None,
    };
    let r = client("tcp", k, role).call(ep, Family::Control, f, T).and_then(check_reply)?;
    decode_body(&r.body)
}

fn put(k: &str, v: &str) -> MetaOp {
    MetaOp::DebugPut { key: k.as_bytes().to_vec(), value: v.as_bytes().to_vec() }
}

fn get(k: &str) -> MetaOp {
    MetaOp::DebugGet { key: k.as_bytes().to_vec() }
}

#[test]
fn debug_put_and_get_through_rpc() {
    let (_rpc, ep, eng) = server(true);
    assert_eq!(meta(&ep, key(1), Role::Admin, put("hello", "world")), Ok(MetaReply::Ok));
    assert_eq!(
        meta(&ep, key(1), Role::Admin, get("hello")),
        Ok(MetaReply::DebugValue { value: Some(b"world".to_vec()) })
    );
    assert_eq!(meta(&ep, key(1), Role::Admin, get("nope")), Ok(MetaReply::DebugValue { value: None }));
    let ok = metrics::counter("wmfs_meta_ops_total", "", &[("op", "DebugPut"), ("code", "OK")]);
    assert!(ok.get() >= 1);
    eng.shutdown();
}

#[test]
fn debug_ops_need_an_admin_key_and_the_debug_ops_switch() {
    let (_rpc, ep, eng) = server(true);
    assert_eq!(meta(&ep, key(2), Role::Client, put("k", "v")), Err(ErrCode::EACCES), "client key 不能用调试操作");
    assert_eq!(meta(&ep, key(2), Role::Client, get("k")), Err(ErrCode::EACCES));
    let lock = MetaOp::GetLock { ino: 1, owner: 1, start: 0, end: 1, write: true };
    assert_eq!(meta(&ep, key(1), Role::Admin, lock), Err(ErrCode::FEATURE_UNSUPPORTED), "GetLock 在 M12");
    eng.shutdown();
    let (_rpc, ep, eng) = server(false);
    assert_eq!(meta(&ep, key(1), Role::Admin, put("k", "v")), Err(ErrCode::EACCES), "服务端没开 debug_ops");
    eng.shutdown();
}

#[test]
fn authentication_reads_the_applied_keymeta() {
    let (_rpc, ep, eng) = server(true);
    assert_eq!(meta(&ep, key(2), Role::Client, get("k")), Err(ErrCode::EACCES), "key 2 认证通过（KEYMETA 里有它）");
    assert_eq!(meta(&ep, key(3), Role::Client, get("k")), Err(ErrCode::AUTH_FAILED), "KEYMETA 里没有 key 3");
    let created = sample_init().created_ms;
    let revoked = KeyMetaV1 { revoked: true, ..initial_keymeta(created)[1].1.clone() };
    assert_eq!(put_raw(&eng, keys::keymeta(2), encode_v1(&revoked)), Ok(MetaReply::Ok));
    assert_eq!(meta(&ep, key(2), Role::Client, get("k")), Err(ErrCode::AUTH_FAILED), "撤销之后新连接被拒");
    assert_eq!(meta(&ep, key(1), Role::Admin, get("k")), Ok(MetaReply::DebugValue { value: None }));
    eng.shutdown();
}
