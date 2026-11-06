//! M11 的管理端点（design.md 7.1、3.10、4.8）：后台任务、本节点的任务进度、数据加密密钥。
//!
//! - `GET /api/v1/tasks`（只由就绪的元数据 leader 回答）：每个任务一行（`TaskInfo`）。leader 任务 = leader 上调度器此刻的样子（含最近的
//!   速率；刚当上 leader、还没跑过的用库里持久化的进度）；节点任务 = 集群级记录里的控制字段（`node` 为 0；`runs` = 收到过几次 run），各节点的
//!   进度见各节点的 `GET /api/v1/node`。
//! - `POST /api/v1/tasks/<kind>/{pause,resume,run}`、`POST /api/v1/tasks/<kind>/set {bw, items}`（admin）：改集群级记录的控制字段
//!   （`bg::progress::apply_action`，一个批次），changelog `TaskControl`（`extra` = `kind`/`action`，set 另有 `bw`/`items`）；回改完的那一行；
//!   leader 任务顺手把新的限速与暂停放进本节点的进度板（调度器每秒才读一次控制字段：紧接着的 `wmfs task list` 也看得到）。不认识的任务 →
//!   404。限速是运维参数（存在 TaskRecV1 里），不是数据策略（3.10）。
//! - `GET /api/v1/node`（每个节点回答本节点的，需认证）：本节点号、本节点看不看得到任务的控制（`controlled` = 已应用过元数据：只做数据的
//!   节点为 false）与本节点上节点任务（scrub、gc-local）的进度——`wmfs task list` 与 `wmfs dev wait task-idle` 逐个节点问它。
//! - `POST /api/v1/datakeys {name, tenant}`（admin）：随机生成一把数据密钥、用 `K_wrap` 包好（`data::datakey`）；一个批次——名字没被用过、
//!   `next_datakey` 取号、写 DATAKEY 与 DATAKEY_NAME、`map_epoch += 1`（集群图里带着它）、changelog `DataKeyCreate`（`extra` =
//!   `id`/`name`/`tenant`）；回 `{id, name}`。密钥本身不出现在管理 API 里。

use super::api::{failed_at, read, write};
use super::auth;
use super::http::{MgmtCtx, Reply, err, json};
use crate::api::types::{
    DataKeyCreateReply, DataKeyCreateReq, NodeInfoReply, STATE_PAUSED, STATE_RUNNING, TaskInfo, TaskSetReq, TasksReply,
    backlog_in_dirs,
};
use crate::base::types::ReqCtx;
use crate::base::units::parse_size;
use crate::base::{ErrCode, Failed, rand};
use crate::bg::progress::{self, Action};
use crate::meta::attr_registry::datakey_name_ok;
use crate::meta::changelog::{self, Change};
use crate::meta::engine::EmbeddedEngine;
use crate::meta::engine::txn::MetaEngine;
use crate::meta::schema::keys::{self, G_NEXT_DATAKEY};
use crate::meta::schema::kv;
use crate::meta::schema::records::{DataKeyIdV1, DataKeyV1, TaskRecV1};

/// 这个路径归这里管吗（依赖元数据的那几个；`/api/v1/node` 是节点本地的，在 http.rs 里直接分派）。
pub fn handles(path: &str) -> bool {
    path == "/api/v1/tasks" || path == "/api/v1/datakeys" || path.starts_with("/api/v1/tasks/")
}

fn bad(m: String) -> Failed {
    Failed::new(ErrCode::EINVAL, m)
}

/// leader_gate 之后的分派。
pub(super) fn serve(ctx: &MgmtCtx, eng: &EmbeddedEngine, req: &ReqCtx, method: &str, path: &str, body: &[u8]) -> Reply {
    let r = match (method, path) {
        ("GET", "/api/v1/tasks") => list(ctx, eng, req).map(|x| json(200, &x)),
        ("POST", "/api/v1/datakeys") => match serde_json::from_slice::<DataKeyCreateReq>(body) {
            Ok(k) => datakey(ctx, eng, req, k).map(|x| json(200, &x)),
            Err(e) => Err(bad(format!("body 应为 {{\"name\": …, \"tenant\": …}}：{e}"))),
        },
        ("POST", p) if p.starts_with("/api/v1/tasks/") => post_action(ctx, eng, req, p, body).map(|x| json(200, &x)),
        _ => Err(bad(format!("{path} 不支持 {method}"))),
    };
    r.unwrap_or_else(|f| failed_at(eng, f))
}

/// 集群级记录 → 一行：节点任务只有控制字段（`node` 0），leader 任务带持久化的进度。`b` 是本节点进度板上的同一个任务，`default_bw` 是
/// 它自己的默认限速（没设限速时显示它）。
fn row(b: &TaskInfo, r: &TaskRecV1, default_bw: u64) -> TaskInfo {
    let bw = if r.bytes_per_sec > 0 { r.bytes_per_sec } else { default_bw };
    let mut t =
        TaskInfo { kind: b.kind.clone(), runs_on: b.runs_on.clone(), bw, items: r.items_per_sec, ..Default::default() };
    t.state = progress::state_name(r.state).into();
    if b.on_leader() {
        t.pass = r.pass;
        t.done = r.done;
        t.backlog = r.backlog;
        t.updated_ms = r.updated_ms;
        t.last_error = r.last_error.clone();
        t.progress = if r.backlog == 0 && !backlog_in_dirs(&b.kind) { 1.0 } else { 0.0 }; // 协调器没有这一轮的进度
    } else {
        t.runs = r.pass; // 节点任务的运行请求号（bg/progress.rs）
    }
    t
}

fn list(ctx: &MgmtCtx, eng: &EmbeddedEngine, req: &ReqCtx) -> Result<TasksReply, Failed> {
    let board = ctx.board.as_ref().ok_or(ErrCode::NOT_READY)?;
    let all = board.all();
    let recs = read(eng, req, |v, _c| {
        let get = |k: &str| kv::get::<TaskRecV1>(v, &keys::task(k, ""));
        all.iter().map(|b| Ok(get(&b.kind)?.unwrap_or_else(progress::new_rec))).collect::<Result<Vec<_>, Failed>>()
    })?;
    let tasks = all
        .iter()
        .zip(recs)
        .map(|(b, r)| match b.on_leader() && b.pass >= r.pass {
            true => b.clone(),                              // 调度器此刻的样子（含速率）
            false => row(b, &r, board.default_bw(&b.kind)), // 节点任务、或刚当上 leader 还没跑过：库里的
        })
        .collect();
    Ok(TasksReply { tasks })
}

/// `POST /api/v1/tasks/<kind>/<动作>`（见文件头）。
fn post_action(ctx: &MgmtCtx, eng: &EmbeddedEngine, req: &ReqCtx, path: &str, body: &[u8]) -> Result<TaskInfo, Failed> {
    let rest = path.strip_prefix("/api/v1/tasks/").unwrap_or_default();
    let (kind, action) =
        rest.split_once('/').ok_or_else(|| bad(format!("应是 /api/v1/tasks/<任务>/<动作>：{path}")))?;
    let board = ctx.board.as_ref().ok_or(ErrCode::NOT_READY)?;
    let b = board.get(kind).ok_or_else(|| Failed::new(ErrCode::ENOENT, format!("没有后台任务 {kind}")))?;
    let mut extra = vec![("kind".to_string(), kind.to_string()), ("action".into(), action.to_string())];
    let a = match action {
        "pause" => Action::Pause,
        "resume" => Action::Resume,
        "run" => Action::Run { every_node: !b.on_leader() },
        "set" => {
            let s: TaskSetReq = serde_json::from_slice(body)
                .map_err(|e| bad(format!("body 应为 {{\"bw\": \"2M\", \"items\": 1000}}：{e}")))?;
            let bps = s.bw.as_deref().map(parse_size).transpose().map_err(|e| bad(format!("bw：{e}")))?;
            extra.extend(bps.map(|x| ("bw".into(), x.to_string())));
            extra.extend(s.items.map(|x| ("items".into(), x.to_string())));
            Action::Set { bps, ips: s.items }
        }
        _ => return Err(bad(format!("不认识的动作 {action}（pause、resume、run、set）"))),
    };
    let kind = kind.to_string();
    let r = write(eng, req, move |v, c| {
        let r = progress::apply_action(v, &kind, a)?;
        changelog::append(v, c, Change { op: "TaskControl", extra: extra.clone(), ..Default::default() })?;
        Ok(r)
    })?;
    let t = row(&b, &r, board.default_bw(&b.kind));
    if b.on_leader() {
        let state = match (t.state == STATE_PAUSED, b.state == STATE_PAUSED) {
            (true, _) => STATE_PAUSED,
            (false, true) => STATE_RUNNING, // 刚恢复：调度器下一次读控制字段之前先当作在跑
            (false, false) => b.state.as_str(),
        };
        board.put(TaskInfo { bw: t.bw, items: t.items, state: state.into(), ..b.clone() }); // 见文件头
    }
    Ok(t)
}

/// `POST /api/v1/datakeys`（见文件头）。
fn datakey(
    ctx: &MgmtCtx,
    eng: &EmbeddedEngine,
    req: &ReqCtx,
    k: DataKeyCreateReq,
) -> Result<DataKeyCreateReply, Failed> {
    if !datakey_name_ok(&k.name) {
        return Err(bad(format!("数据密钥的名字应是 1–64 个字母、数字或 . _ -：{:?}", k.name)));
    }
    let w = ctx.wrapper.as_ref().ok_or(ErrCode::NOT_READY)?;
    let mut key = [0u8; 32];
    rand::fill(&mut key);
    let wrapped =
        w.seal_datakey(&k.name, &key).map_err(|e| Failed::new(e, "加密插件 aes-gcm 没有启用，建不了数据密钥"))?;
    write(eng, req, move |v, c| {
        let (name, tenant) = (k.name.clone(), k.tenant);
        if kv::get::<DataKeyIdV1>(v, &keys::datakey_name(&name))?.is_some() {
            return Err(Failed::new(ErrCode::EEXIST, format!("数据密钥 {name} 已经存在")));
        }
        let id = u32::try_from(kv::next_id(v, G_NEXT_DATAKEY)?).map_err(|_| ErrCode::ENOSPC)?;
        kv::put(v, &keys::datakey(id), &DataKeyV1 { name: name.clone(), tenant, wrapped: wrapped.clone() });
        kv::put(v, &keys::datakey_name(&name), &DataKeyIdV1 { id });
        kv::bump_map_epoch(v)?; // 集群图里带着数据密钥（3.3.6）
        let extra =
            vec![("id".into(), id.to_string()), ("name".into(), name.clone()), ("tenant".into(), tenant.to_string())];
        changelog::append(v, c, Change { op: "DataKeyCreate", extra, ..Default::default() })?;
        Ok(DataKeyCreateReply { id, name })
    })
}

/// `GET /api/v1/node`（节点本地，需认证，见文件头）。
pub(super) fn node_info(ctx: &MgmtCtx, method: &str, url: &str, auth_hdr: Option<&str>, body: &[u8]) -> Reply {
    if let Err(d) = auth::check(&*ctx.keys, method, url, auth_hdr, body, false) {
        return err(d.status, d.code, d.message);
    }
    let tasks = ctx.board.as_ref().map(|b| b.all()).unwrap_or_default();
    let tasks = tasks.into_iter().filter(|t| !t.on_leader()).collect();
    let controlled = ctx.meta.as_ref().is_some_and(|m| m.status().applied > 0); // 只做数据的节点：一个条目也没应用过
    json(200, &NodeInfoReply { node_id: ctx.health.node.node_id, controlled, tasks })
}
