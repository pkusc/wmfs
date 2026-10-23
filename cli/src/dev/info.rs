//! `wmfs dev env`、`wmfs dev status`（design.md 7.2、roadmap §0 第 3 条）：打印 export 语句；各节点是否在运行、是否健康（`--json`）。

use std::path::Path;

use serde::Serialize;

use super::{health, log_path, mgmt_addr, node_dir, nodes, running_pid};

pub fn env(d: &Path) -> Result<Vec<String>, String> {
    let ns = nodes(d);
    let first = ns.first().ok_or_else(|| format!("{} 里还没有开发集群：先 wmfs dev up", d.display()))?;
    let mgmt: Result<Vec<String>, String> = ns.iter().map(|n| mgmt_addr(d, *n)).collect();
    let q = |p: &Path| format!("'{}'", p.display().to_string().replace('\'', r"'\''"));
    Ok(vec![
        format!("export WMFS_CLUSTER={}", mgmt?.join(",")),
        format!("export WMFS_KEY_FILE={}", q(&node_dir(d, *first).join("keys/admin.key"))),
        format!("export WMFS_MGR_DIR={}", q(&d.join("mgr"))),
        format!("export WMFS_DEV_DIR={}", q(d)),
    ])
}

#[derive(Serialize)]
struct NodeStatus {
    node: u32,
    running: bool,
    pid: Option<i32>,
    healthy: bool,
    rpc: Vec<(String, String)>,
    mgmt: String,
    log: String,
}

pub fn status(d: &Path, json: bool) -> Result<Vec<String>, String> {
    let mut v = Vec::new();
    for n in nodes(d) {
        let (pid, h) = (running_pid(d, n), health(d, n).ok());
        let mgmt = mgmt_addr(d, n).unwrap_or_default();
        let rpc = h.as_ref().map(|h| h.node.addrs.clone()).unwrap_or_default();
        let log = log_path(d, n).display().to_string();
        v.push(NodeStatus { node: n, running: pid.is_some(), pid, healthy: h.is_some(), rpc, mgmt, log });
    }
    if json {
        let j = serde_json::json!({"dir": d.display().to_string(), "nodes": v});
        return Ok(vec![j.to_string()]);
    }
    if v.is_empty() {
        return Ok(vec![format!("{} 里没有开发节点（wmfs dev up 起一个）", d.display())]);
    }
    let state = |s: &NodeStatus| match (s.running, s.healthy) {
        (true, true) => "运行中".to_string(),
        (true, false) => "运行中（health 没有响应）".to_string(),
        _ => "已停止".to_string(),
    };
    Ok(v.iter().map(|s| format!("node{}：{}，http {}，日志 {}", s.node, state(s), s.mgmt, s.log)).collect())
}
