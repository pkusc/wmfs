//! mem 传输插件的测试：这个独立 crate 只依赖 wmfs-core，就能跑 core 的握手、认证规则、角色权限表、Ping 与 4 MiB bulk——
//! 证明"新增一种传输插件不改本体一行"。（同一套用例对 tcp 也跑一遍，在 wmfs-testkit 的 tests/it/net_rpc.rs。）

mod common;

use std::sync::Arc;

use common::{AcceptAll, T, TestAuthn, client, frame, key, ping, pool, server};
use plugin_mem_transport::{MANIFEST, MemTransport};
use wmfs_core::api::types::KeyFile;
use wmfs_core::base::ErrCode;
use wmfs_core::base::types::Role;
use wmfs_core::buf::Lane;
use wmfs_core::hooks::manifest::check_manifest_toml;
use wmfs_core::net::client::{Endpoint, Family, check_reply};
use wmfs_core::proto::encode_body;
use wmfs_core::proto::frame::{MSG_PEER_CALL, MSG_PING};
use wmfs_core::proto::handshake::PingBody;

#[test]
fn manifest_toml_matches_code() {
    check_manifest_toml(&MANIFEST, include_str!("../manifest.toml")).unwrap();
}

#[test]
fn ping_and_4m_bulk_round_trip() {
    let (_rpc, ep) = server(MemTransport::listening("mem-ping"), Arc::new(TestAuthn));
    let c = client(key(2), Role::Client);
    for i in 0..1000u32 {
        let r = ping(&c, &ep, &i.to_le_bytes()).unwrap();
        assert_eq!(r.body, encode_body(&PingBody { payload: i.to_le_bytes().to_vec() }), "Ping 原样回显");
    }
    let p = pool();
    let mut data = p.get(4 << 20, Lane::Normal, T).unwrap();
    data.as_mut_slice().iter_mut().enumerate().for_each(|(i, b)| *b = (i % 251) as u8);
    let data = data.freeze();
    for _ in 0..20 {
        let mut f = frame(MSG_PING, b"bulk");
        f.bulk = vec![data.slice(0, 1 << 20), data.slice(1 << 20, 3 << 20)]; // 两段拼成 4 MiB
        f.recv_into = Some(p.get(4 << 20, Lane::Normal, T).unwrap());
        let r = check_reply(c.call(&ep, Family::Data, f, T).unwrap()).unwrap();
        assert_eq!(r.bulk.unwrap().as_slice(), data.as_slice(), "bulk 原样回显");
    }
}

#[test]
fn authentication_and_role_rules() {
    let (_rpc, ep) = server(MemTransport::listening("mem-auth"), Arc::new(TestAuthn));
    let ok = |k: KeyFile, r: Role| ping(&client(k, r), &ep, b"x").map(|_| ());
    assert_eq!(ok(key(1), Role::Admin), Ok(()));
    assert_eq!(ok(key(0), Role::Server), Ok(()), "key 0 是服务端之间用的");
    assert_eq!(ok(KeyFile { key_id: 2, secret: [7; 32] }, Role::Client), Err(ErrCode::AUTH_FAILED), "错误的密钥");
    assert_eq!(ok(key(2), Role::Server), Err(ErrCode::AUTH_FAILED), "client key 申请 server 角色被拒");
    assert_eq!(ok(key(0), Role::Client), Err(ErrCode::AUTH_FAILED), "key 0 只能是 Server");
    assert_eq!(ok(key(9), Role::Client), Err(ErrCode::AUTH_FAILED), "集群不认识的 key");
    let c = client(key(2), Role::Client);
    let peer = c.call(&ep, Family::Data, frame(MSG_PEER_CALL, b""), T).and_then(check_reply).map(|_| ());
    assert_eq!(peer, Err(ErrCode::EACCES), "client 调 PeerCall → EACCES（角色权限表）");
    let gone = Endpoint { node: 9, addrs: vec![("mem".into(), "nobody-listens".into())] };
    assert_eq!(c.call(&gone, Family::Data, frame(MSG_PING, b""), T).err(), Some(ErrCode::ECONNRESET));
}

/// authn 插件原样照抄 Hello 里声明的角色时，core 在 Hello 阶段的检查（`KEYMETA.role == Hello.role`）就是唯一的防线。
#[test]
fn hello_role_is_checked_by_core_even_if_the_plugin_copies_it() {
    let (_rpc, ep) = server(MemTransport::listening("mem-copy-role"), Arc::new(AcceptAll));
    let ok = |k: KeyFile, r: Role| ping(&client(k, r), &ep, b"x").map(|_| ());
    assert_eq!(ok(key(2), Role::Client), Ok(()));
    assert_eq!(ok(key(2), Role::Server), Err(ErrCode::AUTH_FAILED), "key 2 在 KEYMETA 里是 client：冒充 server 被拒");
    assert_eq!(ok(key(1), Role::Server), Err(ErrCode::AUTH_FAILED), "admin key 也冒充不了 server");
    assert_eq!(ok(key(0), Role::Admin), Err(ErrCode::AUTH_FAILED), "key 0 只能是 Server");
}
