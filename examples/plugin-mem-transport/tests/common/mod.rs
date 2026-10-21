//! mem 插件各测试文件共用的部分：两个测试用的 authn 插件（这个 crate 不依赖 wmfs-plugins 里的 shared-key）、
//! 在 mem 上起 RPC 服务端、造客户端与 Ping 帧。做法与 core/tests/common 一样：每个测试文件写 `mod common;`。
#![allow(dead_code)] // 每个测试文件只用到其中一部分

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use plugin_mem_transport::MemTransport;
use wmfs_core::api::types::KeyFile;
use wmfs_core::base::ErrCode;
use wmfs_core::base::config::NetConfig;
use wmfs_core::base::types::Role;
use wmfs_core::buf::{MemPool, PoolConfig};
use wmfs_core::hooks::manifest::{CORE_ABI_VERSION, Hook, Manifest};
use wmfs_core::hooks::pipeline::Pipeline;
use wmfs_core::hooks::registry::PluginSet;
use wmfs_core::hooks::traits::{AuthnInput, AuthnPlugin, AuthnResult, ConnIdentity, KeyLookup};
use wmfs_core::init::{InitKeys, SERVER_KEY};
use wmfs_core::net::client::{Endpoint, Family, RpcClient, check_reply};
use wmfs_core::net::server::{RpcServer, ServerId};
use wmfs_core::proto::encode_body;
use wmfs_core::proto::frame::{FrameHeader, FrameKind, InFrame, MSG_PING, OutFrame};
use wmfs_core::proto::handshake::{PingBody, ProofInput, derive_secret, server_proof, verify_auth_proof};

pub const MASTER: [u8; 32] = [0x42; 32];
pub const T: Duration = Duration::from_secs(10);

/// 测试用的最小 authn 插件：key 0 = Server，其余按 KEYMETA，校验证明。`ACCEPTS` 数它接受过几次（= 握手成功几次）。
pub struct TestAuthn;
pub static ACCEPTS: AtomicU64 = AtomicU64::new(0);
static TEST_AUTHN: Manifest = manifest("test-authn", &["plugin.test-authn.authn"]);

impl AuthnPlugin for TestAuthn {
    fn manifest(&self) -> &'static Manifest {
        &TEST_AUTHN
    }
    fn authenticate(&self, i: &AuthnInput, keys: &dyn KeyLookup) -> AuthnResult {
        let role = if i.key_id == SERVER_KEY {
            Some(Role::Server)
        } else {
            keys.meta(i.key_id).and_then(|m| Role::from_u8(m.role))
        };
        let (Some(role), Some(secret)) = (role, keys.secret(i.key_id)) else { return AuthnResult::Reject };
        let p = ProofInput {
            client_nonce: &i.client_nonce,
            server_nonce: &i.server_nonce,
            role: i.role.as_u8(),
            cluster_id: &i.cluster_id,
            key_id: i.key_id,
        };
        if !verify_auth_proof(&secret, &p, i.proof) {
            return AuthnResult::Reject;
        }
        ACCEPTS.fetch_add(1, Ordering::SeqCst);
        AuthnResult::Accept(identity(i.key_id, role))
    }
    fn server_proof(&self, i: &AuthnInput, keys: &dyn KeyLookup) -> Option<[u8; 32]> {
        keys.secret(i.key_id).map(|s| server_proof(&s, &i.server_nonce, &i.client_nonce))
    }
}

/// 一律接受、并且**原样照抄 Hello 里声明的角色**的 authn 插件：用来测 core 自己的角色检查（与插件无关），
/// 以及只关心传输的用例（fail point）。
pub struct AcceptAll;
static ACCEPT_ALL: Manifest = manifest("accept-all", &["plugin.accept-all.authn"]);

impl AuthnPlugin for AcceptAll {
    fn manifest(&self) -> &'static Manifest {
        &ACCEPT_ALL
    }
    fn authenticate(&self, i: &AuthnInput, _: &dyn KeyLookup) -> AuthnResult {
        AuthnResult::Accept(identity(i.key_id, i.role))
    }
    fn server_proof(&self, i: &AuthnInput, keys: &dyn KeyLookup) -> Option<[u8; 32]> {
        keys.secret(i.key_id).map(|s| server_proof(&s, &i.server_nonce, &i.client_nonce))
    }
}

const fn manifest(name: &'static str, fail_points: &'static [&'static str]) -> Manifest {
    Manifest {
        name,
        version: "0.1.0",
        hooks: &[Hook::Authn],
        altitude: 150,
        hot_path: true,
        config_schema: "",
        abi_version: CORE_ABI_VERSION,
        attrs: &[],
        budget_ns: 20_000,
        fail_points,
    }
}

fn identity(key_id: u32, role: Role) -> ConnIdentity {
    ConnIdentity { key_id, role, tenant: 0, uid_ranges: vec![], root_squash: false, path_caps: vec![] }
}

pub fn pool() -> MemPool {
    MemPool::new(&PoolConfig { s_mb: 1, m_slots: 4, l_slots: 1, repl_lane: false }).unwrap()
}

/// 在传输 `t`（已 `listening(名字)`）上起一个用 `authn` 认证的 RPC 服务端，返回它与连它用的 Endpoint。
pub fn server(t: MemTransport, authn: Arc<dyn AuthnPlugin>) -> (Arc<RpcServer>, Endpoint) {
    let mut set = PluginSet::new(&[]);
    set.transport(Arc::new(t));
    set.authn(authn);
    set.validate(&BTreeMap::new()).unwrap();
    let pipeline = Arc::new(Pipeline::new(set));
    let keys = Arc::new(InitKeys::new(MASTER, 0));
    let rpc =
        RpcServer::new(ServerId { cluster_id: [1; 16], node_id: 1 }, pipeline.clone(), keys, &NetConfig::default());
    let name = rpc.listen(&pipeline.transports()[0], pool()).unwrap().unwrap();
    (rpc, Endpoint { node: 1, addrs: vec![("mem".into(), name)] })
}

/// 只用 mem 传输 `t` 的客户端。
pub fn client_on(t: MemTransport, key: KeyFile, role: Role) -> Arc<RpcClient> {
    let net = NetConfig { control_scheme: "mem".into(), data_scheme: "mem".into(), ..NetConfig::default() };
    RpcClient::new_with_role(pool(), vec![Arc::new(t)], key, role, net).unwrap()
}

pub fn client(key: KeyFile, role: Role) -> Arc<RpcClient> {
    client_on(MemTransport::new(), key, role)
}

pub fn key(id: u32) -> KeyFile {
    KeyFile { key_id: id, secret: derive_secret(&MASTER, id) }
}

/// 一个请求帧（body = PingBody{payload}；msg_type 可以不是 Ping，用来测角色权限表）。
pub fn frame(msg: u16, payload: &[u8]) -> OutFrame {
    let body = encode_body(&PingBody { payload: payload.to_vec() });
    OutFrame { hdr: FrameHeader::new(FrameKind::Request, msg, 0, 0), body, bulk: vec![], recv_into: None }
}

/// 发一个 Ping（数据族），服务端回 ERROR 帧时变成它的错误码。
pub fn ping(c: &RpcClient, ep: &Endpoint, payload: &[u8]) -> Result<InFrame, ErrCode> {
    c.call(ep, Family::Data, frame(MSG_PING, payload), T).and_then(check_reply)
}
