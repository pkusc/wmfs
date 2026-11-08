//! 节点运行时装配（design.md 1.1、1.2）：`wmfs server --data-dir D` 与 `wmfs init`（不带 --no-start）调用 [`start`]。
//!
//! M3 的服务端 = 内存池 + 元数据引擎（raft 线程 + 执行器 + 8 个 meta-read 线程，`meta/meta.redb`）+ 各传输插件监听 +
//! RPC 服务端（握手、Ping、MetaCall）+ 管理 API。认证用的 key 表读已应用的 KEYMETA（3.3.2）。数据服务（M5）等随里程碑加入。
//! M4：NodeCall（`GetClusterMap`）也交给元数据服务；回 `NOT_LEADER` 时带 `leader_hint`（[`hint_of`]）；起 cluster-mgr 线程
//! （leader 就绪时与集群图变化时把集群警告写进日志）。
//! M5：数据面——打开 node.toml 里的 target（超级块核对、清空 `p/`、`t/`）、DataCall 服务与 16 个 io 线程、服务端之间的 RPC 客户端
//! （服务端身份 key 0）（[`open_data`]），监听之后起 target 心跳、给 Uninline 与后台任务共用的数据面客户端、后台调度器
//! （[`start_background`]）。
//! M7：管理 API 拿到元数据引擎（属性、用量、声明 vs 实际、changelog……）；启动时插件 manifest 里的属性名与属性注册表对账。
//! M8：服务端之间的 RPC 客户端（key 0）在元数据引擎之前建好，交给引擎做元数据网络（raft-send、snap-recv、snap-serve），RaftMsg 与
//! SnapshotFetch 登记到 RpcServer；hb 线程每拿到一张集群图就比对插件清单（不一致 → 退出码 3，3.7.2），raft 不知道 leader 时（只做数据的
//! 节点）按认识的节点与 leader_hint 找（`node_hb.rs` 的文件头）；`--adopt-local-plugins`（[`StartOpts`]）；SIGTERM 时本节点是 leader 就先
//! `transfer_leader` 给接替的人（`membership::heir`：活着、追上了的 voter 里 matched 最大的，最多等 3 s，[`Running::stop`]）。
//! 信号（design.md 0.2 第 4 条）：main 在**创建任何线程之前** `sys::block_signals(&[SIGTERM, SIGINT])`，
//! 调用 `start` 之后 `sys::wait_signals(..)`，收到后 [`Running::stop`] 再退出。

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use raft::StateRole;
use wmfs_api::hex::hex;
use wmfs_api::types::{Health, NodeAddrs};

use crate::base::config::NodeConfig;
use crate::base::{clock, log, rand, sys};
use crate::bg::progress::TaskBoard;
use crate::bg::scheduler::{self, BgDeps, LocalGet};
use crate::bg::task::BgTask;
use crate::buf::MemPool;
use crate::client::data_client::DataClient;
use crate::data::datakey::Wrapper;
use crate::data::node_hb::{self, HbDeps, MapHolder};
use crate::data::service::DataService;
use crate::data::{self, DataNode, replica};
use crate::hooks::pipeline::Pipeline;
use crate::hooks::registry::PluginSet;
use crate::hooks::traits::{CODEC_REPLICA, KeyLookup};
use crate::init::{NodeFile, read_master};
use crate::meta::attr_registry;
use crate::meta::cluster::cluster_mgr;
use crate::meta::cluster::plugins::{self, MapCheck};
use crate::meta::engine::txn::MetaEngine;
use crate::meta::engine::{self, EmbeddedEngine, EngineOpts};
use crate::meta::raft::membership::{self, Members};
use crate::meta::schema::records::NODE_REMOVED;
use crate::meta::service::MetaService;
use crate::mgmt::{MgmtCtx, MgmtServer, RoleFn};
use crate::net::client::{Endpoint, RpcClient};
use crate::net::server::{RpcServer, ServerId};
use crate::proto::frame::{MSG_DATA_CALL, MSG_META_CALL, MSG_NODE_CALL, MSG_PEER_CALL, MSG_RAFT, MSG_SNAPSHOT_FETCH};
use crate::proto::handshake::{SERVER_KEY, cap_key, derive_secret, wrap_key};
use crate::proto::peer::ClusterMap;
use roles::{hint_of, leader_addrs, meta_role};

mod roles;

/// 运行中的节点。
pub struct Running {
    pub node_id: u32,
    pub addrs: Vec<(String, String)>, // (scheme, rpc 地址)
    pub mgmt_addr: String,
    pub rpc: Arc<RpcServer>,
    pub meta: Arc<EmbeddedEngine>,
    pid_file: PathBuf,
}

/// 启动选项（`wmfs server` 的开关）。
#[derive(Clone, Copy, Debug, Default)]
pub struct StartOpts {
    pub adopt_local_plugins: bool, // 3.7.2 第 5 条的逃生口
}

/// 装配并启动本节点（返回时各线程已在运行）。`plugins` 由 `cli/src/registry.rs` 按 node.toml 装好并校验过。
/// `version` 是 `wmfs --version` 的那一串（health 里报给别人）；`opts` 是 `wmfs server` 的开关。错误是给人看的中文。
pub fn start(
    data_dir: &Path,
    cfg: &NodeConfig,
    plugins: PluginSet,
    version: &str,
    opts: StartOpts,
) -> Result<Running, String> {
    let log_path = data_dir.join("logs/server.log");
    log::set_file(&log_path).map_err(|e| format!("打开日志 {} 失败：{e}", log_path.display()))?;
    crate::obs::trace::set_slow_ms(cfg.obs.slow_meta_ms, cfg.obs.slow_data_ms); // 慢请求日志的阈值（node.toml [obs]，M14 审查）
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
    for (name, attrs) in pipeline.consumed_attrs() {
        attr_registry::check_manifest_attrs(name, attrs)?; // 与属性注册表对账（3.8，M7）
    }
    let key = wmfs_api::types::KeyFile { key_id: SERVER_KEY, secret: derive_secret(&master, SERVER_KEY) };
    let peer = RpcClient::new(pool.clone(), pipeline.transports().to_vec(), key, cfg.net.clone())
        .map_err(|e| format!("建服务端之间的 RPC 客户端失败：{e}"))?;
    let eng_opts = EngineOpts {
        node_id: node.node_id,
        plugins: plugin_list,
        pipeline: pipeline.clone(),
        cap_key: cap_key(&master),
        rpc: Some(peer.clone()),
        snap_dir: data_dir.join("meta"),
        adopt_local_plugins: opts.adopt_local_plugins,
    };
    let meta_path = data_dir.join("meta/meta.redb");
    if !meta_path.exists() {
        let hint = "这个数据目录是 M3 之前的 wmfs init 建的？开发集群请 wmfs dev down --purge 之后重新 wmfs dev up";
        return Err(format!("没有元数据库 {}（{hint}）", meta_path.display()));
    }
    let meta = EmbeddedEngine::open(&meta_path, eng_opts)?; // 出错时的消息里已经带了元数据库的路径
    let kv_keys = Arc::new(meta.keys(master));
    let keys: Arc<dyn KeyLookup + Send + Sync> = kv_keys.clone();
    let rpc = RpcServer::new(ServerId { cluster_id, node_id: node.node_id }, pipeline.clone(), keys.clone(), &cfg.net);
    let svc = MetaService::start(meta.clone(), cfg.debug_ops, meta.target_usage())?;
    rpc.add_service(MSG_META_CALL, svc.clone());
    rpc.add_service(MSG_NODE_CALL, svc.clone());
    if let Some(net) = meta.net() {
        let n = net.clone();
        rpc.add_oneway(MSG_RAFT, Arc::new(move |f| n.deliver(f)));
        rpc.add_service(MSG_SNAPSHOT_FETCH, net);
    }
    let data_side = open_data(cfg, &pipeline, &pool, &rpc, &master, cluster_id, peer)?;
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
    cluster_mgr::start(&meta, svc.targets(), svc.live())?;
    let mgmt = MgmtServer::bind(&cfg.mgmt.listen)?;
    let mgmt_addr = mgmt.addr().to_string();
    let me = NodeAddrs { node_id: node.node_id, addrs: addrs.clone(), mgmt_addr: mgmt_addr.clone() };
    let rs = rpc.clone();
    let on_keys: Arc<dyn Fn(&ClusterMap) + Send + Sync> = Arc::new(move |m| {
        kv_keys.note_map(m); // 只做数据的节点据此认得后来建的 key（M10，`KvKeys`）
        rs.set_revoked(m.keys.iter().filter(|(_, k)| k.revoked).map(|(id, _)| *id).collect()); // 关掉已撤销 key 的连接
    });
    let board = Arc::new(TaskBoard::default());
    let wrapper =
        Arc::new(Wrapper { pipeline: pipeline.clone(), pool: pool.clone(), key: wrap_key(&master), cluster_id });
    data_side.node.keys.set_wrap(wrapper.clone()); // 数据节点解包数据密钥用（M11）
    let (boot_id, data_node) = (rand::random_u64().max(1), data_side.node.clone()); // boot_id 0 = 不核对 I5（data/chain.rs）
    let bg = Bg { tasks, board: board.clone(), run_dir: data_dir.join("run"), boot_id };
    start_background(cfg, &meta, &svc, on_keys, me.clone(), data_side, bg)?;
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
    let ctx = MgmtCtx {
        health,
        role: Some(role),
        keys,
        failpoints_api: cfg.failpoints_api,
        meta: Some(meta.clone()), // 依赖元数据的端点（M7）
        usage: Some(svc.targets()),
        polls: AtomicUsize::new(0),
        board: Some(board),
        wrapper: Some(wrapper),
        live: Some(svc.live()),
        qos: pipeline.qos_call().map(|(q, _)| q.clone()),
        data: Some(data_node),
        boot_id,
    };
    if let Err(e) = mgmt.start(Arc::new(ctx)) {
        let _ = std::fs::remove_file(&pid_file);
        return Err(format!("管理 API 启动失败：{e}"));
    }
    let rpc_list: Vec<String> = addrs.iter().map(|(s, a)| format!("{s} {a}")).collect();
    log::info("server", format_args!("node{} started（rpc {}，http {mgmt_addr}）", node.node_id, rpc_list.join("，")));
    Ok(Running { node_id: node.node_id, addrs, mgmt_addr, rpc, meta, pid_file })
}

/// 后台调度器要的本节点的东西（M11）：后台任务、进度板（管理 API 也看它）、本地任务的游标目录；本次启动的 boot_id（心跳报它，
/// `GET /api/v1/node` 也显示它，M14）。
struct Bg {
    tasks: Vec<Box<dyn BgTask>>,
    board: Arc<TaskBoard>,
    run_dir: PathBuf,
    boot_id: u64,
}

/// 数据面里监听之前就建好的东西（[`open_data`]）。
struct DataSide {
    node: Arc<DataNode>,  // 本节点的 target、chunk 状态表与集群图视图（DataCall 服务与 hb 线程共用）
    peer: Arc<RpcClient>, // 服务端之间用的 RPC 客户端（服务端身份 key 0）：心跳、Uninline、后台任务
}

/// 数据面的前半段（监听之前）：打开 target、起 DataCall 服务（M5）；服务端之间的 RPC 客户端（key 0）已经建好。
fn open_data(
    cfg: &NodeConfig,
    pipeline: &Arc<Pipeline>,
    pool: &MemPool,
    rpc: &Arc<RpcServer>,
    master: &[u8; 32],
    cluster_id: [u8; 16],
    peer: Arc<RpcClient>,
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
    let svc = DataService::start(node.clone())?;
    rpc.add_service(MSG_DATA_CALL, svc.clone());
    rpc.add_service(MSG_PEER_CALL, replica::start(&node, peer.clone(), svc)?); // 链式复制（M9）
    Ok(DataSide { node, peer })
}

/// 数据面的后半段（监听之后，知道了本节点的地址）：target 心跳、Uninline 与后台任务共用的数据面客户端、后台调度器（M5）。
/// `on_keys` = hb 线程每拉到一张新集群图时对其中的 key 表要做的事（M10：记下 key 表、关掉已撤销 key 的连接）。
fn start_background(
    cfg: &NodeConfig,
    meta: &Arc<EmbeddedEngine>,
    svc: &Arc<MetaService>,
    on_keys: Arc<dyn Fn(&ClusterMap) + Send + Sync>,
    me: NodeAddrs,
    side: DataSide,
    bg: Bg,
) -> Result<(), String> {
    let (eng, node) = (meta.clone(), me.node_id);
    let leader = Arc::new(move || {
        let a = leader_addrs(eng.role().leader_id, &me, &|id| eng.node_addrs(id))?;
        Some(Endpoint { node: a.node_id, addrs: a.addrs })
    });
    let eng = meta.clone();
    let peers = Arc::new(move || {
        let others = eng.nodes().into_iter().filter(|(id, n)| *id != node && n.state != NODE_REMOVED);
        others.map(|(id, n)| Endpoint { node: id, addrs: n.addrs }).collect()
    });
    let machine_id = std::fs::read_to_string("/etc/machine-id").unwrap_or_default().trim().to_string();
    let host_id = cfg.debug.host_id.clone().unwrap_or_else(|| machine_id.clone());
    let (boot_id, pipeline) = (bg.boot_id, side.node.pipeline.clone());
    let eng = meta.clone();
    let on_map: Arc<dyn Fn(&ClusterMap) + Send + Sync> = Arc::new(move |m| {
        on_keys(m);
        let data_only = eng.status().applied == 0; // 不在 raft 里（3.7.2 的 M9 注记）
        // 每一张集群图都比对（3.7.2 第 3 条）；逃生口的标志与 raft 线程共用（ADR-0039 第 6 条、ADR-0041 第 11 条）
        match plugins::on_map(&eng.local_plugins().plugins, &m.plugins.plugins, data_only, eng.adopting()) {
            MapCheck::AdoptDone => eng.adopt_done(),
            MapCheck::Exit(d) => plugins::exit_mismatch(&d),
            MapCheck::Keep => {}
        }
    });
    let (rpc, data_node) = (side.peer.clone(), side.node.clone());
    let hb = HbDeps { node, boot_id, host_id, machine_id, data: side.node, rpc, leader, peers, on_map };
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
    let mut data = DataClient::new(side.peer, map_of, false).with_timeouts(rpc_t, op_t);
    if let Some(c) = pipeline.codec(CODEC_REPLICA) {
        data = data.with_codec(c.clone()); // 迁移、uninline 读的时候也按 read_order 分散到各副本（M13）
    }
    let data = Arc::new(data);
    svc.set_data(data.clone());
    let eng = meta.clone();
    let local: LocalGet = Arc::new(move |k| {
        if eng.status().applied == 0 {
            return None; // 只做数据的节点：一个条目也没应用过
        }
        eng.local_get(k)
            .inspect_err(|e| log::warn("server", format_args!("读本地已应用的 KV 失败：{e}（这一次按没有记录处理）")))
            .ok()
            .flatten()
    });
    let has_targets = !data_node.targets.is_empty();
    let deps = BgDeps {
        eng: meta.clone(),
        data,
        live: svc.live(),
        pipeline,
        node: has_targets.then_some(data_node),
        local,
        run_dir: bg.run_dir,
        board: bg.board,
        node_id: node,
    };
    scheduler::start(bg.tasks, deps)
}

impl Running {
    /// 收到 SIGTERM/SIGINT 之后：本节点是 leader、有接替的人（`membership::heir`：活着、追上了的 voter 里 matched 最大的）→ 先
    /// `transfer_leader` 给它，最多等 3 s（3.4.1.9 的优雅退出）；再停元数据引擎（补一次 Immediate 提交，已应用的写全部落盘）、删掉 pid 文件。
    pub fn stop(&self) {
        let to = membership::heir(&self.meta.status(), &self.meta.nodes(), self.node_id);
        if self.meta.role().role == StateRole::Leader
            && let Some(to) = to
        {
            log::info("server", format_args!("node{} 退出前把 leader 让给节点 {to}", self.node_id));
            self.meta.transfer_leader(to);
            let deadline = clock::mono_ms() + 3000;
            while self.meta.role().role == StateRole::Leader && clock::mono_ms() < deadline {
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
        }
        self.meta.shutdown();
        let _ = std::fs::remove_file(&self.pid_file);
        log::info("server", format_args!("node{} 退出", self.node_id));
    }
}
