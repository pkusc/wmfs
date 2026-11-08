//! M8 的管理端点（design.md 7.1、3.4.1.9、3.7.2）：
//! - `GET /api/v1/cluster`：集群概览——各节点（地址、状态、元数据里的实际角色 `member` 与指定的角色 `member_wanted`、leader 看到的
//!   matched、是否追上）、元数据 raft 的 leader/term/commit/applied/成员、集群警告（[`ClusterReply`]）；M14 只增 target（编号、节点、
//!   状态）、链的条数、Active 的会话数（dashboard 的集群面板）。`wmfs node list` 用它。
//! - `POST /api/v1/nodes/<id>/{drain,remove,promote}`（admin）：只改节点记录（一个 exclusive 批次——拓扑类操作，3.4.1.4 规则 5；changelog
//!   `NodeDrain/NodeRemove/NodePromote`，`map_epoch += 1`），成员变更由 cluster-mgr 接着做（`meta/raft/membership.rs`）。drain = 不再当
//!   leader、不再被提升（M9 起还要把链从它上面迁走）；remove = 移出元数据 raft（不能移除最后一个 voter）；promote = 指定为 voter（先以
//!   learner 加入、追上之后提升）。
//! - `GET /api/v1/cluster/plugins`、`POST /api/v1/cluster/plugins {add, remove}`（admin）：集群插件清单（`meta/cluster/plugins.rs`）。
//! - `GET /api/v1/debug/meta-hash`：**每个节点都回答本节点的**（不跟随 leader）——已应用到哪、kv 表的哈希（`wmfs debug meta-hash`）。
//!
//! - M9：`GET /api/v1/chains`（全部链：成员与状态、期望长度、incarnation、last_serving，`wmfs chain list/show`）、
//!   `POST /api/v1/chains/<id>/force-serving?target=`（admin，body 必须是 `{"i_accept_data_loss": true}`；`chain_mgr::force_serving`）。
//!   成员的 `state`：链记录里的 `serving`/`syncing`；leader 最近 2 s 没听到它的心跳 → `unheard`，听到的 boot_id 与集群图里的不同（刚重启、
//!   链管理器还没处理）→ `restarting`（审查后加的取值：`wmfs dev wait chains-serving` 不把它们算作 Serving）。
//!
//! - M14 收尾（ADR-0045 第 11 条，7.1 列了、roadmap 没排进里程碑的最小版）：`GET /api/v1/features`（[`FeaturesReply`]：GLOBAL 已启用的
//!   incompat 特性与没有移除的节点各自声明支持的，3.2.10）、`POST /api/v1/features/<name>/enable`（admin）：首发没有能启用的——3.2.10
//!   的预留名（都是二期）回 `FEATURE_UNSUPPORTED`"二期功能"，不认识的名字回 `EINVAL`；不写 GLOBAL、不写 changelog。
//!
//! 前三组（与链、特性）只有就绪的元数据 leader 回答（`api.rs` 的 leader_gate，ADR-0038 第 2 条；读写途中失去 leader 也照这条回，`failed_at`）；
//! 读是线性一致读、写是 seq 0 的执行器闭包（复用 `api.rs` 的 `read` / `write` / `write_exclusive`）。

use super::api::{failed_at, read, write, write_exclusive};
use super::auth;
use super::http::{MgmtCtx, Reply, err, json};
use crate::api::types::{
    ChainInfo, ChainMember, ChainsReply, ClusterReply, ClusterTarget, FeaturesReply, ForceServingReq, NodeFeatures,
    NodeInfo, NodeOpReply, PluginsReply, PluginsReq,
};
use crate::api::url::param;
use crate::base::types::ReqCtx;
use crate::base::{ErrCode, Failed, clock};
use crate::meta::changelog::{self, Change};
use crate::meta::cluster::chains::{self, SERVING};
use crate::meta::cluster::targets::TargetUsage;
use crate::meta::cluster::{chain_mgr, plugins};
use crate::meta::engine::EmbeddedEngine;
use crate::meta::engine::txn::{EngineStatus, KvScan, MetaEngine};
use crate::meta::schema::keys;
use crate::meta::schema::kv;
use crate::meta::schema::records::{
    ChainRecV1, META_LEARNER, META_NONE, META_VOTER, NODE_ACTIVE, NODE_DRAINING, NODE_REMOVED, NodeRecV1, SessionRecV1,
    TargetRecV1, decode_v1,
};
use crate::meta::sessions::SESSION_ACTIVE;
use crate::proto::peer::ClusterMap;

/// 这个路径归这里管吗（`meta-hash` 另走 [`meta_hash`]）。
pub fn handles(path: &str) -> bool {
    matches!(path, "/api/v1/cluster" | "/api/v1/cluster/plugins" | "/api/v1/chains" | "/api/v1/features")
        || path.starts_with("/api/v1/nodes/")
        || path.starts_with("/api/v1/chains/")
        || path.starts_with("/api/v1/features/")
}

/// 3.2.10 的预留名（都是二期，首发不能启用）。
const RESERVED_FEATURES: [&str; 4] =
    ["chunk.snapshots", "chunk.dyn_transform", "meta.ec-layout", "proto.pipelined-meta"];

/// leader_gate 之后的分派（`usage` = leader 内存里的 target 心跳情况，链成员的状态用）。
pub(super) fn serve(
    eng: &EmbeddedEngine,
    usage: Option<&TargetUsage>,
    req: &ReqCtx,
    (method, path, query): (&str, &str, &str),
    body: &[u8],
) -> Reply {
    let (term, now) = (eng.role().term, clock::mono_ms());
    let heard = |t: u32| usage.and_then(|u| u.heard(t, term, now));
    let r = match (method, path) {
        ("GET", "/api/v1/chains") => read(eng, req, |_v, c| Ok(chains(c.map, &heard))).map(|x| json(200, &x)),
        ("POST", p) if p.starts_with("/api/v1/chains/") => {
            force(eng, req, p, query, body, &heard).map(|x| json(200, &x))
        }
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
        ("GET", "/api/v1/features") => read(eng, req, |_v, c| Ok(features(c.map))).map(|x| json(200, &x)),
        ("POST", p) if p.starts_with("/api/v1/features/") => Err(enable(p)),
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
    let (map, sessions) = read(eng, req, |v, c| Ok((c.map.clone(), active_sessions(v)?)))?; // 线性一致：ReadIndex 之后
    let (st, role) = (eng.status(), eng.role());
    let nodes = map.nodes.iter().map(|(id, n)| info(*id, n, &st, role.leader_id)).collect();
    let state =
        |s: u8| ["unknown", "up", "offline", "draining", "removed"].get(s as usize).copied().unwrap_or("unknown");
    let target = |(id, t): &(u32, TargetRecV1)| ClusterTarget { id: *id, node: t.node, state: state(t.state).into() };
    Ok(ClusterReply {
        targets: map.targets.iter().map(target).collect(),
        chains: map.chains.len() as u64,
        sessions,
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

/// Active 的客户端会话数（M14：`ClusterReply.sessions`）。
fn active_sessions(v: &dyn KvScan) -> Result<u64, Failed> {
    let mut n = 0;
    for (_, b) in kv::scan_tag(v, keys::T_SESSION)? {
        n += u64::from(decode_v1::<SessionRecV1>(&b)?.state == SESSION_ACTIVE);
    }
    Ok(n)
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
        "drain" => {
            ("NodeDrain", "已标记为 drain：它不再当 leader、不再被提升为 voter；链管理器把它的 target 从链里迁走")
        }
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

/// `GET /api/v1/features`（见文件头）：GLOBAL 已启用的 incompat 特性，与没有移除的节点各自声明支持的。
fn features(m: &ClusterMap) -> FeaturesReply {
    let nodes = m.nodes.iter().filter(|(_, n)| n.state != NODE_REMOVED);
    let nodes = nodes.map(|(id, n)| NodeFeatures { node_id: *id, features: n.features.clone() }).collect();
    FeaturesReply { enabled: m.features.incompat.clone(), nodes }
}

/// `POST /api/v1/features/<name>/enable`（见文件头）：首发一律失败（预留名 `FEATURE_UNSUPPORTED`，别的 `EINVAL`）。
fn enable(path: &str) -> Failed {
    match path.strip_prefix("/api/v1/features/").and_then(|r| r.strip_suffix("/enable")) {
        Some(n) if RESERVED_FEATURES.contains(&n) => {
            Failed::new(ErrCode::FEATURE_UNSUPPORTED, format!("{n} 是二期功能：首发不能启用"))
        }
        Some(n) => Failed::new(
            ErrCode::EINVAL,
            format!(
                "不认识的特性 {n:?}：首发没有能启用的 incompat 特性（预留名都是二期功能：{}）",
                RESERVED_FEATURES.join("、")
            ),
        ),
        None => Failed::new(ErrCode::EINVAL, format!("{path}：应为 /api/v1/features/<名字>/enable")),
    }
}

/// leader 最近听到的 target 的 boot_id（没听到 → None）。
type Heard<'a> = &'a dyn Fn(u32) -> Option<u64>;

/// 一条链给管理 API 看的样子：`want` 见 `chains::want`（与链管理器同一口径）；成员的 `state` 见文件头。
fn chain_info(m: &ClusterMap, id: u32, c: &ChainRecV1, heard: Heard) -> ChainInfo {
    let rec = |t: u32| m.targets.iter().find(|(i, _)| *i == t).map(|(_, r)| r);
    let state = |t: u32, s: u8| match heard(t) {
        None => "unheard",
        Some(b) if rec(t).is_some_and(|r| r.boot_id != 0 && r.boot_id != b) => "restarting",
        Some(_) if s == SERVING => "serving",
        Some(_) => "syncing",
    };
    ChainInfo {
        id,
        ver: c.ver,
        incarnation: c.incarnation,
        class: m.class_name(c.class_id),
        replicas: c.replicas,
        want: chains::want(m, c) as u8,
        members: c
            .members
            .iter()
            .map(|(t, s)| ChainMember { target: *t, node: rec(*t).map_or(0, |r| r.node), state: state(*t, *s).into() })
            .collect(),
        last_serving: c.last_serving,
        removed: c.removed.iter().map(|(t, _)| *t).collect(),
    }
}

fn chains(m: &ClusterMap, heard: Heard) -> ChainsReply {
    let chains = m.chains.iter().map(|(id, c)| chain_info(m, *id, c, heard)).collect();
    ChainsReply { chains, warnings: m.warnings.clone() }
}

/// `POST /api/v1/chains/<id>/force-serving?target=`（见文件头）。
fn force(
    eng: &EmbeddedEngine,
    req: &ReqCtx,
    path: &str,
    query: &str,
    body: &[u8],
    heard: Heard,
) -> Result<ChainInfo, Failed> {
    let bad =
        || Failed::new(ErrCode::EINVAL, format!("{path}：应为 /api/v1/chains/<链号>/force-serving?target=<target 号>"));
    let id: u32 = path
        .strip_prefix("/api/v1/chains/")
        .and_then(|r| r.strip_suffix("/force-serving"))
        .ok_or_else(bad)?
        .parse()
        .map_err(|_| bad())?;
    let target: u32 = param(query, "target").and_then(|t| t.parse().ok()).ok_or_else(bad)?;
    let ok = serde_json::from_slice::<ForceServingReq>(body).is_ok_and(|r| r.i_accept_data_loss);
    if !ok {
        let msg = "force-serving 可能丢掉别的成员独有的写：body 必须是 {\"i_accept_data_loss\": true}";
        return Err(Failed::new(ErrCode::EINVAL, msg));
    }
    let rec = write_exclusive(eng, req, move |v, c| chain_mgr::force_serving(v, c, id, target))?;
    let map = read(eng, req, |_v, c| Ok(c.map.clone()))?;
    Ok(chain_info(&map, id, &rec, heard))
}
