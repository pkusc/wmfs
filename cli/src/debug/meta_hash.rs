//! `wmfs debug meta-hash [--cluster a,b,c] [--json]`（design.md 7.2，roadmap M8）：问每个节点的 `GET /api/v1/debug/meta-hash`（每个节点
//! 回答本节点已应用的状态：applied 与 kv 表的哈希，不跟随 leader），等到各节点的 applied 相同（最多 30 s：刚重启的节点在追日志或装快照），
//! 再比较哈希。`consistent` = 每个地址都回答了、applied 相同、哈希相同；不一致时退出码 1。实现注记（M8 审查）：先问 leader 的
//! `GET /api/v1/cluster` 谁是元数据成员（voter、learner）；不是成员的节点（只做数据，没有元数据副本，applied 停在 0）不比，列在
//! `not_members` 里。问不到 leader 就全都比。

use std::time::Duration;

use serde::Serialize;
use wmfs_core::api::types::{ClusterReply, MetaHashReply};
use wmfs_core::base::clock;
use wmfs_mgr::api::ApiClient;

use crate::attr::{ApiArgs, failed};
use crate::{cluster_addrs, load_key, out};

/// 等各节点追平的期限。
const SETTLE_MS: u64 = 30_000;

#[derive(Serialize)]
struct Report {
    consistent: bool,
    nodes: Vec<MetaHashReply>,
    unreachable: Vec<String>,
    not_members: Vec<u32>, // 不是元数据成员的节点（见文件头）：不比
}

pub fn meta_hash(api: &ApiArgs) -> i32 {
    let bad = |message| failed(api.json, &wmfs_mgr::api::ApiError { status: 0, code: "einval".into(), message });
    let cluster = match cluster_addrs(&api.cluster) {
        Ok(c) => c,
        Err(e) => return bad(e),
    };
    let key = match load_key(&api.key_file) {
        Ok(k) => k,
        Err(e) => return bad(e),
    };
    let overview = ApiClient::new(cluster.clone(), key.clone()).get::<ClusterReply>("/api/v1/cluster");
    let members = overview.ok().map(|r| [r.voters, r.learners, r.voters_outgoing].concat());
    let deadline = clock::mono_ms() + SETTLE_MS;
    let rep = loop {
        let (mut nodes, mut unreachable, mut not_members) = (Vec::new(), Vec::new(), Vec::new());
        for a in &cluster {
            match ApiClient::new(vec![a.clone()], key.clone()).get::<MetaHashReply>("/api/v1/debug/meta-hash") {
                Ok(h) if members.as_ref().is_some_and(|m| !m.contains(&h.node_id)) => not_members.push(h.node_id),
                Ok(h) => nodes.push(h),
                Err(e) => unreachable.push(format!("{a}：{e}")),
            }
        }
        let settled = unreachable.is_empty() && nodes.windows(2).all(|w| w[0].applied == w[1].applied);
        if settled || clock::mono_ms() > deadline {
            let consistent = settled && nodes.windows(2).all(|w| w[0].hash == w[1].hash);
            break Report { consistent, nodes, unreachable, not_members };
        }
        std::thread::sleep(Duration::from_millis(100));
    };
    let code = if rep.consistent { 0 } else { 1 };
    if api.json {
        let _ = out(serde_json::to_string(&rep).unwrap_or_default());
        return code;
    }
    let mut lines: Vec<String> = rep
        .nodes
        .iter()
        .map(|h| format!("node{}：applied {}，{} 个键，hash {}", h.node_id, h.applied, h.keys, h.hash))
        .collect();
    lines.extend(rep.unreachable.iter().map(|u| format!("没有回答：{u}")));
    lines.extend(rep.not_members.iter().map(|n| format!("node{n}：不是元数据成员（只做数据），不比")));
    lines.push(if rep.consistent {
        "一致".into()
    } else {
        "不一致（或有节点没追平、没回答）".into()
    });
    let _ = out(lines.join("\n"));
    code
}
