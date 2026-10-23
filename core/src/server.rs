//! 节点运行时装配（design.md 1.1、1.2）：`wmfs server --data-dir D` 与 `wmfs init`（不带 --no-start）调用 [`start`]。
//!
//! M3 的服务端 = 内存池 + 元数据引擎（raft 线程 + 执行器 + 8 个 meta-read 线程，`meta/meta.redb`）+ 各传输插件监听 +
//! RPC 服务端（握手、Ping、MetaCall）+ 管理 API。认证用的 key 表读已应用的 KEYMETA（3.3.2）。数据服务（M5）等随里程碑加入。
//! M4：NodeCall（`GetClusterMap`）也交给元数据服务；回 `NOT_LEADER` 时带 `leader_hint`（[`hint_of`]）；起 cluster-mgr 线程
//! （leader 就绪时与集群图变化时把集群警告写进日志）。
//! M5：数据面——打开 node.toml 里的 target（超级块核对、清空 `p/`、`t/`）、DataCall 服务与 16 个 io 线程、服务端之间的 RPC 客户端
//! （服务端身份 key 0）（[`open_data`]），监听之后起 target 心跳、给 Uninline 与后台任务共用的数据面客户端、后台调度器
//! （[`start_background`]）。
//! 信号（design.md 0.2 第 4 条）：main 在**创建任何线程之前** `sys::block_signals(&[SIGTERM, SIGINT])`，
//! 调用 `start` 之后 `sys::wait_signals(..)`，收到后 [`Running::stop`] 再退出。

use std::path::{Path, PathBuf};
use std::sync::Arc;

use raft::StateRole;
use wmfs_api::hex::hex;
use wmfs_api::types::{Health, NodeAddrs};

use crate::base::config::NodeConfig;
use crate::base::{log, rand, sys};
use crate::bg::scheduler::{self, BgDeps};
use crate::bg::task::BgTask;
use crate::buf::MemPool;
use crate::client::data_client::DataClient;
use crate::data::node_hb::{self, HbDeps, MapHolder};
use crate::data::service::DataService;
use crate::data::{self, DataNode};
use crate::hooks::pipeline::Pipeline;
use crate::hooks::registry::PluginSet;
use crate::hooks::traits::{CODEC_REPLICA, KeyLookup};
use crate::init::{NodeFile, read_master};
use crate::meta::cluster::cluster_mgr;
use crate::meta::engine::txn::{MetaEngine, RoleInfo};
use crate::meta::engine::{self, EmbeddedEngine, EngineOpts};
use crate::meta::service::MetaService;
use crate::mgmt::{MgmtCtx, MgmtServer, RoleFn};
use crate::net::client::{Endpoint, RpcClient};
use crate::net::server::{RpcServer, ServerId};
use crate::proto::frame::{MSG_DATA_CALL, MSG_META_CALL, MSG_NODE_CALL};
use crate::proto::handshake::{SERVER_KEY, cap_key, derive_secret};
use crate::proto::peer::ClusterMap;

/// 运行中的节点。
pub struct Running {
    pub node_id: u32,
    pub addrs: Vec<(String, String)>, // (scheme, rpc 地址)
    pub mgmt_addr: String,
    pub rpc: Arc<RpcServer>,
    pub meta: Arc<EmbeddedEngine>,
    pid_file: PathBuf,
}

/// 装配并启动本节点（返回时各线程已在运行）。`plugins` 由 `cli/src/registry.rs` 按 node.toml 装好并校验过。
/// `version` 是 `wmfs --version` 的那一串（health 里报给别人）。错误是给人看的中文。
pub fn start(data_dir: &Path, cfg: &NodeConfig, plugins: PluginSet, version: &str) -> Result<Running, String> {
    let log_path = data_dir.join("logs/server.log");
    log::set_file(&log_path).map_err(|e| format!("打开日志 {} 失败：{e}", log_path.display()))?;
    let node = NodeFile::read(data_dir)?;
    let cluster_id = node.cluster_id_bytes()?;
    let master = read_master(data_dir)?;
    if plugins.authn_plugins().is_empty() {
        return Err("没有启用任何 authn 插件：服务端无法认证连接".into());
    }
    match plugins.meta_engine_manifest() {
        Some(m) if m.name == engine::MANIFEST.name => {}
        Some(m) => return Err(format!("不认识的 meta_engine 插件 {}（首发只有 embedded-kv）", m.name)),
        None => return Err("没有启用 meta_engine 插件（node.toml 的 [plugins] disable 里有 embedded-kv？）".into()),
    }
    let plugin_list = plugins.plugin_list();
    let tasks = plugins.take_bg_tasks();
    let pool = MemPool::new(&cfg.mem)
        .map_err(|e| format!("内存池创建失败（node.toml [mem]，共 {} MiB）：{e}", cfg.mem.total_bytes() >> 20))?;
    let pipeline = Arc::new(Pipeline::new(plugins));
    let opts = EngineOpts {
        node_id: node.node_id,
        plugins: plugin_list,
        pipeline: pipeline.clone(),
        cap_key: cap_key(&master),
    };
    let meta_path = data_dir.join("meta/meta.redb");
    if !meta_path.exists() {
        let hint = "这个数据目录是 M3 之前的 wmfs init 建的？开发集群请 wmfs dev down --purge 之后重新 wmfs dev up";
        return Err(format!("没有元数据库 {}（{hint}）", meta_path.display()));
    }
    let meta = EmbeddedEngine::open(&meta_path, opts)?; // 出错时的消息里已经带了元数据库的路径
    let keys: Arc<dyn KeyLookup + Send + Sync> = Arc::new(meta.keys(master));
    let rpc = RpcServer::new(ServerId { cluster_id, node_id: node.node_id }, pipeline.clone(), keys.clone(), &cfg.net);
    let svc = MetaService::start(meta.clone(), cfg.debug_ops)?;
    rpc.add_service(MSG_META_CALL, svc.clone());
    rpc.add_service(MSG_NODE_CALL, svc.clone());
    let data_side = open_data(cfg, &pipeline, &pool, &rpc, &master, cluster_id)?;
    let mut addrs = Vec::new();
    for t in pipeline.transports() {
        let (scheme, name) = (t.scheme(), t.manifest().name);
        t.register_region(&pool.region()).map_err(|e| format!("传输 {scheme} 注册内存失败：{e}"))?;
        let a = rpc.listen(t, pool.clone()).map_err(|e| {
            let hint = format!("监听地址见 node.toml 的 [plugin.{name}]，详情见日志 {}", log_path.display());
            format!("传输 {scheme} 监听失败：{e}（端口被占用？{hint}；本机的开发集群先 wmfs dev down）")
        })?;
        addrs.extend(a.map(|a| (scheme.to_string(), a)));
    }
    if !addrs.iter().any(|(s, _)| *s == cfg.net.control_scheme) {
        return Err(format!(
            "[net] control_scheme = {:?}，但没有这种传输在监听（已监听：{addrs:?}）",
            cfg.net.control_scheme
        ));
    }
    cluster_mgr::start(&meta)?;
    let mgmt = MgmtServer::bind(&cfg.mgmt.listen)?;
    let mgmt_addr = mgmt.addr().to_string();
    let me = NodeAddrs { node_id: node.node_id, addrs: addrs.clone(), mgmt_addr: mgmt_addr.clone() };
    start_background(cfg, &meta, &svc, me.clone(), data_side, tasks)?;
    let (eng, mine, scheme) = (meta.clone(), me.clone(), cfg.net.control_scheme.clone());
    rpc.set_leader_hint(Arc::new(move || {
        hint_of(leader_addrs(eng.role().leader_id, &mine, &|id| eng.node_addrs(id)), &scheme)
    }));
    let health = Health {
        ok: true,
        version: version.into(),
        cluster_id: hex(&cluster_id),
        node: me.clone(),
        meta_role: "none".into(),
        leader: None,
        audit_lag: None,
    };
    let eng = meta.clone();
    let role: RoleFn = Arc::new(move || meta_role(&eng, &me));
    // pid 文件在管理 API 开始应答之前写好：health 一通，`wmfs dev status/down` 就一定找得到这个进程
    let pid_file = data_dir.join("run/server.pid");
    sys::atomic_write(&pid_file, format!("{}\n", std::process::id()).as_bytes())
        .map_err(|e| format!("写 {} 失败：{e}", pid_file.display()))?;
    let ctx = MgmtCtx { health, role: Some(role), keys, failpoints_api: cfg.failpoints_api };
    if let Err(e) = mgmt.start(Arc::new(ctx)) {
        let _ = std::fs::remove_file(&pid_file);
        return Err(format!("管理 API 启动失败：{e}"));
    }
    let rpc_list: Vec<String> = addrs.iter().map(|(s, a)| format!("{s} {a}")).collect();
    log::info("server", format_args!("node{} started（rpc {}，http {mgmt_addr}）", node.node_id, rpc_list.join("，")));
    Ok(Running { node_id: node.node_id, addrs, mgmt_addr, rpc, meta, pid_file })
}

/// 数据面里监听之前就建好的东西（[`open_data`]）。
struct DataSide {
    node: Arc<DataNode>,  // 本节点的 target、chunk 状态表与集群图视图（DataCall 服务与 hb 线程共用）
    peer: Arc<RpcClient>, // 服务端之间用的 RPC 客户端（服务端身份 key 0）：心跳、Uninline、后台任务
}

/// 数据面的前半段（监听之前）：打开 target、起 DataCall 服务、建服务端之间的 RPC 客户端（M5）。
fn open_data(
    cfg: &NodeConfig,
    pipeline: &Arc<Pipeline>,
    pool: &MemPool,
    rpc: &Arc<RpcServer>,
    master: &[u8; 32],
    cluster_id: [u8; 16],
) -> Result<DataSide, String> {
    let (store, _) =
        pipeline.chunk_store().ok_or("没有启用 chunk_store 插件（node.toml 的 [plugins] disable 里有 local-fs？）")?;
    if pipeline.qos_call().is_none() {
        return Err("没有启用 qos 插件（数据节点要一个，例如 fifo）".into());
    }
    if pipeline.codec(CODEC_REPLICA).is_none() {
        return Err(format!("没有启用 codec 插件 replica（编号 {CODEC_REPLICA}）"));
    }
    let targets = data::open_targets(store, &cfg.targets, &cluster_id)?;
    let map = Arc::new(MapHolder::new());
    let node = DataNode::new(pipeline.clone(), targets, pool.clone(), cluster_id, cap_key(master), map);
    rpc.add_service(MSG_DATA_CALL, DataService::start(node.clone())?);
    let key = wmfs_api::types::KeyFile { key_id: SERVER_KEY, secret: derive_secret(master, SERVER_KEY) };
    let peer = RpcClient::new(pool.clone(), pipeline.transports().to_vec(), key, cfg.net.clone())
        .map_err(|e| format!("建服务端之间的 RPC 客户端失败：{e}"))?;
    Ok(DataSide { node, peer })
}

/// 数据面的后半段（监听之后，知道了本节点的地址）：target 心跳、Uninline 与后台任务共用的数据面客户端、后台调度器（M5）。
fn start_background(
    cfg: &NodeConfig,
    meta: &Arc<EmbeddedEngine>,
    svc: &Arc<MetaService>,
    me: NodeAddrs,
    side: DataSide,
    tasks: Vec<Box<dyn BgTask>>,
) -> Result<(), String> {
    let (eng, node) = (meta.clone(), me.node_id);
    let leader = Arc::new(move || {
        let a = leader_addrs(eng.role().leader_id, &me, &|id| eng.node_addrs(id))?;
        Some(Endpoint { node: a.node_id, addrs: a.addrs })
    });
    let machine_id = std::fs::read_to_string("/etc/machine-id").unwrap_or_default().trim().to_string();
    let host_id = cfg.debug.host_id.clone().unwrap_or_else(|| machine_id.clone());
    let (boot_id, pipeline) = (rand::random_u64(), side.node.pipeline.clone());
    let hb = HbDeps { node, boot_id, host_id, machine_id, data: side.node, rpc: side.peer.clone(), leader };
    node_hb::start(hb)?;
    // 服务端用的数据面客户端：集群图直接取 leader 自己已应用的（总是最新的，不用另外刷新）
    let eng = meta.clone();
    let map_of: Arc<dyn Fn() -> Arc<ClusterMap> + Send + Sync> = Arc::new(move || match eng.cluster_map() {
        Ok(m) => m,
        Err(e) => {
            log::warn(
                "server",
                format_args!("读集群图失败：{e}（这一次按空集群图处理，数据请求会以 NOT_SERVING 重试）"),
            );
            Arc::default()
        }
    });
    // 总期限 30 s（DataClient 默认 120 s）：服务端内部的 Uninline 与 gc 不必等那么久，失败交给客户端或 gc 的下一轮重试
    let (rpc_t, op_t) = (std::time::Duration::from_secs(10), std::time::Duration::from_secs(30));
    let data = Arc::new(DataClient::new(side.peer, map_of, false).with_timeouts(rpc_t, op_t));
    svc.set_data(data.clone());
    scheduler::start(tasks, BgDeps { eng: meta.clone(), data, live: svc.live(), pipeline })
}

/// health 里的元数据角色与 leader 的地址（实现注记（M3），7.1）。
fn meta_role(eng: &EmbeddedEngine, me: &NodeAddrs) -> (String, Option<NodeAddrs>) {
    let r = eng.role();
    let learner = r.role == StateRole::Follower && eng.status().learners.contains(&me.node_id);
    (role_name(&r, learner).to_string(), leader_addrs(r.leader_id, me, &|id| eng.node_addrs(id)))
}

/// leader 的地址（health 与 NOT_LEADER 的 `leader_hint` 共用）：不知道 leader（`leader_id` 为 0）→ None；是自己 → 本节点
/// 实际监听的地址；是别人 → `others(id)`（已应用的 NodeRecV1，查不到 → None。注意：`wmfs dev up --rdma` 改了 node.toml
/// 之后，KV 里的地址不会跟着变——那只影响 rdma 一项，control_scheme 的 tcp 地址不变）。
fn leader_addrs(leader_id: u32, me: &NodeAddrs, others: &dyn Fn(u32) -> Option<NodeAddrs>) -> Option<NodeAddrs> {
    match leader_id {
        0 => None,
        id if id == me.node_id => Some(me.clone()),
        id => others(id),
    }
}

/// NOT_LEADER 的 `leader_hint`（实现注记（M4），3.3.1、3.3.7）：leader 在 control_scheme 上的 rpc 地址（"127.0.0.1:19100"）。
fn hint_of(leader: Option<NodeAddrs>, scheme: &str) -> Option<String> {
    leader?.addrs.into_iter().find(|(s, _)| s == scheme).map(|(_, a)| a)
}

/// `meta_role` 的取值：`leader` = **就绪**的 leader（可以服务请求，`wmfs dev wait leader` 等的就是它）；`candidate` =
/// 正在选举，或已当选但还没就绪；`follower` / `learner`。
fn role_name(r: &RoleInfo, learner: bool) -> &'static str {
    match r.role {
        StateRole::Leader if r.ready => "leader",
        StateRole::Leader | StateRole::Candidate | StateRole::PreCandidate => "candidate",
        StateRole::Follower if learner => "learner",
        StateRole::Follower => "follower",
    }
}

impl Running {
    /// 收到 SIGTERM/SIGINT 之后：停元数据引擎（补一次 Immediate 提交，已应用的写全部落盘）、删掉 pid 文件。
    /// 实现注记（M3）：多节点时先 transfer_leader 给 matched 最大的 voter（最多等 3 s，3.4.1.9）在 M8。
    pub fn stop(&self) {
        self.meta.shutdown();
        let _ = std::fs::remove_file(&self.pid_file);
        log::info("server", format_args!("node{} 退出", self.node_id));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_role_names_follow_7_1() {
        let r = |role, ready| RoleInfo { role, term: 2, leader_id: 1, ready };
        assert_eq!(role_name(&r(StateRole::Leader, true), false), "leader");
        assert_eq!(
            role_name(&r(StateRole::Leader, false), false),
            "candidate",
            "已当选、还没就绪的 leader 不算 leader"
        );
        assert_eq!(role_name(&r(StateRole::Candidate, false), false), "candidate");
        assert_eq!(role_name(&r(StateRole::PreCandidate, false), false), "candidate");
        assert_eq!(role_name(&r(StateRole::Follower, false), false), "follower");
        assert_eq!(role_name(&r(StateRole::Follower, false), true), "learner");
    }

    #[test]
    fn leader_hint_is_the_leaders_control_scheme_address() {
        let node = |id: u32, port: u16| NodeAddrs {
            node_id: id,
            addrs: vec![("mem".into(), format!("mem-{id}")), ("tcp".into(), format!("127.0.0.1:{port}"))],
            mgmt_addr: String::new(),
        };
        let me = node(1, 19100);
        let others = |id: u32| (id == 2).then(|| node(2, 19200));
        let hint = |leader_id| hint_of(leader_addrs(leader_id, &me, &others), "tcp");
        assert_eq!(hint(0), None, "不知道 leader");
        assert_eq!(hint(1), Some("127.0.0.1:19100".into()), "leader 是自己：本节点实际监听的地址");
        assert_eq!(
            hint(2),
            Some("127.0.0.1:19200".into()),
            "leader 是别人：已应用的 NodeRecV1 里 control_scheme 的那一项"
        );
        assert_eq!(hint(3), None, "集群图里查不到这个节点");
        assert_eq!(hint_of(leader_addrs(2, &me, &others), "mem"), Some("mem-2".into()), "按 control_scheme 挑地址");
        assert_eq!(hint_of(leader_addrs(2, &me, &others), "udp"), None, "leader 没有这种传输");
        assert_eq!(leader_addrs(1, &me, &others), Some(me.clone()), "health 里的 leader 地址");
    }
}
