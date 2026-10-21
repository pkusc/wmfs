//! 节点运行时装配（design.md 1.1、1.2）：`wmfs server --data-dir D` 与 `wmfs init`（不带 --no-start）调用 [`start`]。
//!
//! M1 的服务端 = 内存池 + 各传输插件监听 + RPC 服务端（握手、Ping）+ 管理 API。元数据（M3）、数据（M5）等服务随里程碑加入。
//! 信号（design.md 0.2 第 4 条）：main 在**创建任何线程之前** `sys::block_signals(&[SIGTERM, SIGINT])`，
//! 调用 `start` 之后 `sys::wait_signals(..)`，收到后 [`Running::stop`] 再退出。

use std::path::{Path, PathBuf};
use std::sync::Arc;

use wmfs_api::hex::hex;
use wmfs_api::types::{Health, NodeAddrs};

use crate::base::config::NodeConfig;
use crate::base::{log, sys};
use crate::buf::MemPool;
use crate::hooks::pipeline::Pipeline;
use crate::hooks::registry::PluginSet;
use crate::init::{InitKeys, NodeFile, read_master};
use crate::mgmt::{MgmtCtx, MgmtServer};
use crate::net::server::{RpcServer, ServerId};

/// 运行中的节点。
pub struct Running {
    pub node_id: u32,
    pub addrs: Vec<(String, String)>, // (scheme, rpc 地址)
    pub mgmt_addr: String,
    pub rpc: Arc<RpcServer>,
    pid_file: PathBuf,
}

/// 装配并启动本节点（返回时各线程已在运行）。`plugins` 由 `cli/src/registry.rs` 按 node.toml 装好并校验过。
/// `version` 是 `wmfs --version` 的那一串（health 里报给别人）。错误是给人看的中文。
pub fn start(data_dir: &Path, cfg: &NodeConfig, plugins: PluginSet, version: &str) -> Result<Running, String> {
    let log_path = data_dir.join("logs/server.log");
    log::set_file(&log_path).map_err(|e| format!("打开日志 {} 失败：{e}", log_path.display()))?;
    let node = NodeFile::read(data_dir)?;
    let cluster_id = node.cluster_id_bytes()?;
    let keys = Arc::new(InitKeys::new(read_master(data_dir)?, node.created_ms));
    if plugins.authn_plugins().is_empty() {
        return Err("没有启用任何 authn 插件：服务端无法认证连接".into());
    }
    let pool = MemPool::new(&cfg.mem)
        .map_err(|e| format!("内存池创建失败（node.toml [mem]，共 {} MiB）：{e}", cfg.mem.total_bytes() >> 20))?;
    let pipeline = Arc::new(Pipeline::new(plugins));
    let rpc = RpcServer::new(ServerId { cluster_id, node_id: node.node_id }, pipeline.clone(), keys.clone(), &cfg.net);
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
    let mgmt = MgmtServer::bind(&cfg.mgmt.listen)?;
    let mgmt_addr = mgmt.addr().to_string();
    let health = Health {
        ok: true,
        version: version.into(),
        cluster_id: hex(&cluster_id),
        node: NodeAddrs { node_id: node.node_id, addrs: addrs.clone(), mgmt_addr: mgmt_addr.clone() },
        meta_role: "none".into(),
        leader: None,
        audit_lag: None,
    };
    // pid 文件在管理 API 开始应答之前写好：health 一通，`wmfs dev status/down` 就一定找得到这个进程
    let pid_file = data_dir.join("run/server.pid");
    sys::atomic_write(&pid_file, format!("{}\n", std::process::id()).as_bytes())
        .map_err(|e| format!("写 {} 失败：{e}", pid_file.display()))?;
    if let Err(e) = mgmt.start(Arc::new(MgmtCtx { health, keys, failpoints_api: cfg.failpoints_api })) {
        let _ = std::fs::remove_file(&pid_file);
        return Err(format!("管理 API 启动失败：{e}"));
    }
    let rpc_list: Vec<String> = addrs.iter().map(|(s, a)| format!("{s} {a}")).collect();
    log::info("server", format_args!("node{} started（rpc {}，http {mgmt_addr}）", node.node_id, rpc_list.join("，")));
    Ok(Running { node_id: node.node_id, addrs, mgmt_addr, rpc, pid_file })
}

impl Running {
    /// 收到 SIGTERM/SIGINT 之后：删掉 pid 文件（M1 没有别的需要收尾的状态；M3 起先 transfer_leader）。
    pub fn stop(&self) {
        let _ = std::fs::remove_file(&self.pid_file);
        log::info("server", format_args!("node{} 退出", self.node_id));
    }
}
