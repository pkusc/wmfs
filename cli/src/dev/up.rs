//! `wmfs dev up`（design.md 7.2，roadmap §0 第 3 条）：初始化（第一次）并起全部开发节点、等元数据成员到齐、打印集群警告；
//! `--profile nas-safe` 最后再起一个 mgr（M14，`dev/mgr.rs`）。从 `dev.rs` 拆出来（每个文件 ≤ 400 行），说明见 `dev.rs` 的文件头。

use std::net::Ipv4Addr;
use std::path::Path;
use std::time::Duration;

use wmfs_core::base::config::{NodeConfig, TargetConfig};
use wmfs_core::base::sys;
use wmfs_core::buf::PoolConfig;
use wmfs_core::client::meta_client::fetch_map;
use wmfs_core::init::{InitOpts, Profile, init_node};

use super::rdma::set_rdma;
use super::{MARKER, admin_key, cluster, log_path, mgmt_addr, node_dir, nodes, ports, running_pid, start};

pub fn up(d: &Path, want: u32, rdma: Option<Ipv4Addr>, profile: Profile) -> Result<Vec<String>, String> {
    if !(1..=9).contains(&want) {
        return Err("--nodes 只能是 1–9（第 n 个节点的端口是 19n00、19n80）".into());
    }
    std::fs::create_dir_all(d).map_err(|e| format!("建目录 {} 失败：{e}", d.display()))?;
    let marker = d.join(MARKER);
    sys::atomic_write(&marker, "wmfs dev 的开发目录（wmfs dev down --purge 只删除带这个标记的目录）\n".as_bytes())
        .map_err(|e| format!("写 {} 失败：{e}", marker.display()))?;
    let mut lines = Vec::new();
    let count = want.max(nodes(d).last().copied().unwrap_or(0)); // 已有更多节点的开发集群：全部启动
    for n in 1..=count {
        let nd = node_dir(d, n);
        if nd.join("NODE").exists() {
            check_profile(&nd, profile)?;
        } else if n > 1 {
            cluster::join(d, n, profile)?; // node1 已经在运行（上一轮起的）
        } else {
            let (rpc_port, mgmt_port) = ports(n);
            let o = InitOpts {
                data_dir: nd.clone(),
                addr: "127.0.0.1".into(),
                rpc_port,
                mgmt_port,
                mem: PoolConfig::dev_server(),
                failpoints_api: true,
                debug_ops: true, // wmfs debug kv（M3 演示）
                host_id: Some(format!("host-{n}")),
                plugins: crate::registry::init_plugin_list(&profile.disabled_plugins(), &[])?,
                targets: vec![dev_target(&nd)],
                profile,
            };
            init_node(&o)?;
        }
        if set_rdma(&nd, n, rdma)? && running_pid(d, n).is_some() {
            lines.push(format!(
                "node{n} 已在运行：node.toml 里的 RDMA 设置改了，要先 wmfs dev down、再 wmfs dev up 才生效"
            ));
        }
        lines.push(start(d, n)?);
    }
    if count > 1 {
        let m = cluster::wait_members(d, count).map_err(|e| format!("{}\n{e}", lines.join("\n")))?;
        lines.push(m);
    }
    // 取不到集群图 = 没有就绪的 leader（例如旧里程碑建的开发集群插件清单不一致）：大声失败，别让后面的命令白等
    let ws = warnings(d, 1).map_err(|e| {
        let log = log_path(d, 1).display().to_string();
        format!("{}\n节点起来了，但没能取到集群图：{e}（日志 {log}）", lines.join("\n"))
    })?;
    lines.extend(ws.iter().map(|w| format!("警告：{w}")));
    if profile == Profile::NasSafe {
        lines.extend(super::mgr::start(d)?); // 完整的 nas-safe（9.7，M14）：集群起好之后起一个 mgr，最后一行是 dashboard 网址
    }
    Ok(lines)
}

/// 开发节点的默认 target（`<节点目录>/targets/1`，容量上限 20 GiB，无标签；init 与 join 共用）。
pub fn dev_target(nd: &Path) -> TargetConfig {
    TargetConfig { path: nd.join("targets/1").display().to_string(), labels: vec![], capacity: "20G".into() }
}

/// 已有的节点是按 `want` 这个 profile 建的吗（看 node.toml 的 `[plugins] disable`：profile 的初始 KV 写进了元数据库，不能事后
/// 改写）？不是 → 报错并给出修法。
pub fn check_profile(nd: &Path, want: Profile) -> Result<(), String> {
    let p = nd.join("node.toml");
    let cfg = NodeConfig::parse(&std::fs::read_to_string(&p).map_err(|e| format!("读 {} 失败：{e}", p.display()))?)?;
    if cfg.plugins.disable == want.disabled_plugins() {
        return Ok(());
    }
    let was = if cfg.plugins.disable == Profile::Minimal.disabled_plugins() { "minimal" } else { "default" };
    Err(format!(
        "{} 是按 --profile {was} 建的开发集群，这次要的是 --profile {}：先 wmfs dev down --purge，再 wmfs dev up",
        nd.display(),
        want.name()
    ))
}

/// 集群警告（ADR-0016：`wmfs dev up` 的输出里也要看得到）：用这个节点的 admin key 取一次集群图（等 leader 最多 30 s）。
fn warnings(d: &Path, n: u32) -> Result<Vec<String>, String> {
    let l = crate::leader::connect(&[mgmt_addr(d, n)?], admin_key(d, n)?)?;
    let m = fetch_map(l.rpc, &[l.ep], Duration::from_secs(30)).map_err(|e| format!("取集群图失败：{e}"))?;
    Ok(m.warnings.clone())
}
