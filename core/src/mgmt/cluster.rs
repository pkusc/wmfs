//! M8 的管理端点（design.md 7.1、3.4.1.9、3.7.2）：
//! - `GET /api/v1/cluster`：集群概览——各节点（地址、状态、元数据里的实际角色 `member` 与指定的角色 `member_wanted`、leader 看到的
//!   matched、是否追上）、元数据 raft 的 leader/term/commit/applied/成员、集群警告（[`ClusterReply`]）。`wmfs node list` 用它。
//! - `POST /api/v1/nodes/<id>/{drain,remove,promote}`（admin）：只改节点记录（一个 exclusive 批次——拓扑类操作，3.4.1.4 规则 5；changelog
//!   `NodeDrain/NodeRemove/NodePromote`，`map_epoch += 1`），成员变更由 cluster-mgr 接着做（`meta/raft/membership.rs`）。drain = 不再当
//!   leader、不再被提升（M9 起还要把链从它上面迁走）；remove = 移出元数据 raft（不能移除最后一个 voter）；promote = 指定为 voter（先以
//!   learner 加入、追上之后提升）。
//! - `GET /api/v1/cluster/plugins`、`POST /api/v1/cluster/plugins {add, remove}`（admin）：集群插件清单（`meta/cluster/plugins.rs`）。
//! - `GET /api/v1/debug/meta-hash`：**每个节点都回答本节点的**（不跟随 leader）——已应用到哪、kv 表的哈希（`wmfs debug meta-hash`）。
//!
//! 前三组只有就绪的元数据 leader 回答（`api.rs` 的 leader_gate，ADR-0038 第 2 条；读写途中失去 leader 也照这条回，`failed_at`）；读是
//! 线性一致读、写是 seq 0 的执行器闭包（复用 `api.rs` 的 `read` / `write` / `write_exclusive`）。

use super::api::{failed_at, read, write, write_exclusive};
use super::auth;
use super::http::{MgmtCtx, Reply, err, json};
use crate::api::types::{ClusterReply, NodeInfo, NodeOpReply, PluginsReply, PluginsReq};
use crate::base::types::ReqCtx;
use crate::base::{ErrCode, Failed};
use crate::meta::changelog::{self, Change};
use crate::meta::cluster::plugins;
use crate::meta::engine::EmbeddedEngine;
use crate::meta::engine::txn::{EngineStatus, MetaEngine};
use crate::meta::schema::keys;
use crate::meta::schema::kv;
use crate::meta::schema::records::{
    META_LEARNER, META_NONE, META_VOTER, NODE_ACTIVE, NODE_DRAINING, NODE_REMOVED, NodeRecV1,
};

/// 这个路径归这里管吗（`meta-hash` 另走 [`meta_hash`]）。
pub fn handles(path: &str) -> bool {
    matches!(path, "/api/v1/cluster" | "/api/v1/cluster/plugins") || path.starts_with("/api/v1/nodes/")
}

/// leader_gate 之后的分派。
pub(super) fn serve(eng: &EmbeddedEngine, req: &ReqCtx, method: &str, path: &str, body: &[u8]) -> Reply {
    let r = match (method, path) {
        ("GET", "/api/v1/cluster") => overview(eng, req).map(|x| json(200, &x)),
        ("GET", "/api/v1/cluster/plugins") => {
            read(eng, req, |_v, c| Ok(PluginsReply { plugins: c.map.plugins.plugins.clone() })).map(|x| json(200, &x))
        }
        ("POST", "/api/v1/cluster/plugins") => match serde_json::from_slice::<PluginsReq>(body) {
            Ok(p) => write(eng, req, move |v, c| plugins::change(v, c, &p.add, &p.remove)).map(|x| json(200, &x)),
            Err(e) => {
                Err(Failed::new(ErrCode::EINVAL, format!("body 应为 {{\"add\": [...], \"remove\": [...]}}：{e}")))
            }
        },
        ("POST", p) if p.starts_with("/api/v1/nodes/") => node_op(eng, req, p).map(|x| json(200, &x)),
        _ => Err(Failed::new(ErrCode::EINVAL, format!("{path} 不支持 {method}"))),
    };
    r.unwrap_or_else(|f| failed_at(eng, f))
}

/// `GET /api/v1/debug/meta-hash`（见文件头）：任一已认证的 key。
pub(super) fn meta_hash(ctx: &MgmtCtx, method: &str, url: &str, auth_hdr: Option<&str>, body: &[u8]) -> Reply {
    if let Err(d) = auth::check(&*ctx.keys, method, url, auth_hdr, body, false) {
        return err(d.status, d.code, d.message);
    }
    match ctx.meta.as_ref().map(|e| e.meta_hash()) {
        Some(Ok(h)) => json(200, &h),
        Some(Err(e)) => super::api::failed(e.into()),
        None => err(503, "not_ready", "本节点没有元数据引擎".into()),
    }
}

fn overview(eng: &EmbeddedEngine, req: &ReqCtx) -> Result<ClusterReply, Failed> {
    let map = read(eng, req, |_v, c| Ok(c.map.clone()))?; // 线性一致：ReadIndex 之后的集群图
    let (st, role) = (eng.status(), eng.role());
    let nodes = map.nodes.iter().map(|(id, n)| info(*id, n, &st, role.leader_id)).collect();
    Ok(ClusterReply {
        leader: role.leader_id,
        term: role.term,
        commit: st.commit,
        applied: st.applied,
        voters: st.voters.clone(),
        learners: st.learners.clone(),
        voters_outgoing: st.voters_outgoing.clone(),
        nodes,
        warnings: map.warnings.clone(),
    })
}

fn info(id: u32, n: &NodeRecV1, st: &EngineStatus, leader: u32) -> NodeInfo {
    let member = if st.voters.contains(&id) {
        "voter"
    } else if st.learners.contains(&id) {
        "learner"
    } else {
        "none"
    };
    let wanted = match n.meta_role {
        META_VOTER => "voter",
        META_LEARNER => "learner",
        META_NONE => "none",
        _ => "unknown",
    };
    let state = match n.state {
        NODE_ACTIVE => "active",
        NODE_DRAINING => "draining",
        NODE_REMOVED => "removed",
        _ => "unknown",
    };
    NodeInfo {
        node_id: id,
        addrs: n.addrs.clone(),
        mgmt_addr: n.mgmt_addr.clone(),
        hostname: n.hostname.clone(),
        host_id: n.host_id.clone(),
        state: state.into(),
        member: member.into(),
        member_wanted: wanted.into(),
        leader: id == leader,
        matched: st.matched(id),
        caught_up: member != "none" && st.caught_up(id),
    }
}

/// `POST /api/v1/nodes/<id>/<drain|remove|promote>`（见文件头）。
fn node_op(eng: &EmbeddedEngine, req: &ReqCtx, path: &str) -> Result<NodeOpReply, Failed> {
    let bad = || Failed::new(ErrCode::EINVAL, format!("{path}：应为 /api/v1/nodes/<节点号>/<drain|remove|promote>"));
    let rest = path.strip_prefix("/api/v1/nodes/").ok_or_else(bad)?;
    let (id, op) = rest.split_once('/').ok_or_else(bad)?;
    let id: u32 = id.parse().map_err(|_| bad())?;
    let (op, message) = match op {
        "drain" => ("NodeDrain", "已标记为 drain：它不再当 leader、不再被提升为 voter（链的迁走在 M9）"),
        "remove" => ("NodeRemove", "已标记为移除：cluster-mgr 接着把它移出元数据 raft（之后它不再参与投票）"),
        "promote" => ("NodePromote", "已指定为元数据 voter：先以 learner 加入，追上之后提升"),
        _ => return Err(bad()),
    };
    let st = eng.status();
    if op == "NodeRemove" && st.voters == [id] {
        return Err(Failed::new(ErrCode::EINVAL, format!("节点 {id} 是唯一的元数据 voter：不能移除")));
    }
    let n = write_exclusive(eng, req, move |v, c| {
        let key = keys::node(id);
        let mut n: NodeRecV1 =
            kv::get(v, &key)?.ok_or_else(|| Failed::new(ErrCode::ENOENT, format!("没有节点 {id}")))?;
        if n.state == NODE_REMOVED {
            return Err(Failed::new(ErrCode::EINVAL, format!("节点 {id} 已经移除了")));
        }
        match op {
            "NodeDrain" => n.state = NODE_DRAINING,
            "NodeRemove" => n.state = NODE_REMOVED,
            "NodePromote" if n.state != NODE_ACTIVE => {
                return Err(Failed::new(ErrCode::EINVAL, format!("节点 {id} 正在 drain")));
            }
            "NodePromote" => n.meta_role = META_VOTER,
            _ => return Err(ErrCode::INTERNAL.into()), // 上面已经只留下这三种
        }
        kv::put(v, &key, &n);
        kv::bump_map_epoch(v)?;
        let extra = vec![("node".to_string(), id.to_string())];
        changelog::append(v, c, Change { op, name: n.hostname.as_bytes(), extra, ..Default::default() })?;
        Ok(n)
    })?;
    Ok(NodeOpReply { node: info(id, &n, &st, eng.role().leader_id), message: message.into() })
}
