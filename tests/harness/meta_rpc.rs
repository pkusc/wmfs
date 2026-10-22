//! 元数据服务经真实的 RPC（tcp，127.0.0.1:0）：在一个元数据引擎上起 `MetaService`（MetaCall 与 NodeCall），认证读引擎里
//! 已应用的 KEYMETA，回 NOT_LEADER 时带上给定的 `leader_hint`；以及连到它的 `MetaClient`（M4）。

use std::sync::Arc;
use std::time::Duration;

use wmfs_core::base::config::{NetConfig, PluginTable};
use wmfs_core::base::types::Role;
use wmfs_core::client::meta_client::{Caller, MetaClient, MetaClientOpts, SessionInfo};
use wmfs_core::hooks::pipeline::Pipeline;
use wmfs_core::hooks::registry::PluginSet;
use wmfs_core::meta::engine::EmbeddedEngine;
use wmfs_core::meta::service::MetaService;
use wmfs_core::net::client::Endpoint;
use wmfs_core::net::server::{RpcServer, ServerId};
use wmfs_core::proto::frame::{MSG_META_CALL, MSG_NODE_CALL};
use wmfs_plugins::shared_key::SharedKey;
use wmfs_plugins::tcp::TcpTransport;

use super::net::{CLUSTER, MASTER, client, key, pool};

/// 起好的元数据服务：RPC 服务端、它的地址、服务本身（测试看它的会话状态）。
pub struct MetaServer {
    pub rpc: Arc<RpcServer>,
    pub ep: Endpoint,
    pub svc: Arc<MetaService>,
}

/// 在 `eng` 上起元数据服务；`hint` = 回 NOT_LEADER 时填的 leader_hint（None = 不填）。
pub fn meta_server(eng: Arc<EmbeddedEngine>, hint: Option<String>) -> MetaServer {
    let cfg: PluginTable = "listen = \"127.0.0.1:0\"".parse().unwrap();
    let mut set = PluginSet::new(&[]);
    set.transport(Arc::new(TcpTransport::new(&cfg, 5000).unwrap()));
    set.authn(Arc::new(SharedKey::new()));
    let pipeline = Arc::new(Pipeline::new(set));
    let keys = Arc::new(eng.keys(MASTER));
    let rpc =
        RpcServer::new(ServerId { cluster_id: CLUSTER, node_id: 1 }, pipeline.clone(), keys, &NetConfig::default());
    let svc = MetaService::start(eng, true).unwrap();
    rpc.add_service(MSG_META_CALL, svc.clone());
    rpc.add_service(MSG_NODE_CALL, svc.clone());
    if let Some(h) = hint {
        rpc.set_leader_hint(Arc::new(move || Some(h.clone())));
    }
    let p = pool();
    let t = &pipeline.transports()[0];
    t.register_region(&p.region()).unwrap();
    let addr = rpc.listen(t, p).unwrap().unwrap();
    MetaServer { rpc, ep: Endpoint { node: 1, addrs: vec![("tcp".into(), addr)] }, svc }
}

impl MetaServer {
    /// 这个服务端在 tcp 上的地址（"127.0.0.1:端口"，当 leader_hint 用）。
    pub fn addr(&self) -> String {
        self.ep.addrs[0].1.clone()
    }
}

/// 用 key `key_id` 连上 `seeds`（GetClusterMap + SessionOpen）；`rpc_timeout` 是单次 RPC 的超时。
pub fn meta_client(seeds: &[Endpoint], key_id: u32, rpc_timeout: Duration) -> MetaClient {
    let role = if key_id == 1 { Role::Admin } else { Role::Client };
    let info = SessionInfo { client_name: "test".into(), host: "h".into(), host_id: "h".into(), mount: "/m".into() };
    let opts = MetaClientOpts { rpc_timeout, op_timeout: Duration::from_secs(20) };
    MetaClient::connect_with(client("tcp", key(key_id), role), seeds, info, opts).expect("MetaClient::connect")
}

pub fn caller(uid: u32, gid: u32) -> Caller {
    Caller { uid, gid, groups: vec![], pid: 1, job_id: 0 }
}
