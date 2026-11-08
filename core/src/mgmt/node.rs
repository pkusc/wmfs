//! 节点本地的端点（design.md 7.1：每个节点回答本节点的，不跟随 leader；需要认证，任一已认证 key）：
//! - `GET /api/v1/node`：本节点号、本节点看不看得到任务的控制（`controlled` = 已应用过元数据：只做数据的节点为 false）与本节点上节点任务
//!   （scrub、gc-local）的进度——`wmfs task list` 与 `wmfs dev wait task-idle` 逐个节点问它（M11，原在 `mgmt/tasks.rs`）；M14 加上 target、
//!   boot_id、内存池、线程数（只增，ADR-0042 第 4 条）；
//! - `GET /api/v1/trace/<id>`（本节点关于这个 trace 的 span，按跳排）与 `GET /api/v1/trace/recent?n=`（本节点最近的 n 个客户端请求，
//!   从新到旧；默认 20，最多 1000），3.11，M14。trace_id 写成 16 位以内的 hex（`obs::trace` 的文件头）。

use wmfs_api::types::{NodeInfoReply, NodeTarget, PoolClass, TraceReply, TraceSpan};
use wmfs_api::url;

use super::auth;
use super::http::{MgmtCtx, Reply, err, json};
use crate::meta::engine::txn::MetaEngine;
use crate::obs::{self, trace};

/// `GET /api/v1/node`（见文件头）。target 的已用与容量是 hb 线程每秒一次的 statfs 的最近结果（`Target::last_stat`；还没有就报 0）——
/// 管理线程不碰盘：盘挂死时 statfs 会卡住，管理 API 的 4 个线程会被占满（1.2 的线程表里 mgmt-http 只阻塞在网络上，M14 审查）。
pub(super) fn node_info(ctx: &MgmtCtx, method: &str, url: &str, auth_hdr: Option<&str>, body: &[u8]) -> Reply {
    if let Err(d) = auth::check(&*ctx.keys, method, url, auth_hdr, body, false) {
        return err(d.status, d.code, d.message);
    }
    let tasks = ctx.board.as_ref().map(|b| b.all()).unwrap_or_default();
    let tasks = tasks.into_iter().filter(|t| !t.on_leader()).collect();
    let controlled = ctx.meta.as_ref().is_some_and(|m| m.status().applied > 0); // 只做数据的节点：一个条目也没应用过
    let (mut targets, mut pool) = (Vec::new(), Vec::new());
    if let Some(d) = &ctx.data {
        for t in d.targets.values() {
            let (used, capacity) = t.last_stat().map_or((0, 0), |s| (s.used, s.capacity));
            let (path, phys_devs) = (t.path.clone(), t.phys_devs.clone());
            targets.push(NodeTarget { id: t.id, path, used, capacity, read_only: t.read_only(), phys_devs });
        }
        let class = |c: crate::buf::pool::ClassStats| PoolClass {
            class: c.name.into(),
            slot_size: c.slot_size as u64,
            slots: c.slots,
            free_normal: c.free_normal as u64,
            free_repl: c.free_repl as u64,
        };
        pool = d.pool.stats().into_iter().map(class).collect();
    }
    let reply = NodeInfoReply {
        node_id: ctx.health.node.node_id,
        controlled,
        tasks,
        boot_id: format!("{:016x}", ctx.boot_id),
        targets,
        pool,
        threads: obs::threads(),
    };
    json(200, &reply)
}

/// `GET /api/v1/trace/<id>` 与 `GET /api/v1/trace/recent?n=`（见文件头）。
pub(super) fn trace(ctx: &MgmtCtx, method: &str, url: &str, auth_hdr: Option<&str>, body: &[u8]) -> Reply {
    if let Err(d) = auth::check(&*ctx.keys, method, url, auth_hdr, body, false) {
        return err(d.status, d.code, d.message);
    }
    let node = ctx.health.node.node_id;
    let (path, query) = url.split_once('?').unwrap_or((url, ""));
    let what = path.trim_start_matches("/api/v1/trace/");
    let spans = if what == "recent" {
        match url::param(query, "n").map(|n| n.parse::<usize>()) {
            None => trace::recent(node, 20),
            Some(Ok(n)) if n <= 1000 => trace::recent(node, n),
            Some(_) => return err(400, "einval", "n 应是 0–1000 的整数".into()),
        }
    } else {
        match u64::from_str_radix(what, 16) {
            Ok(id) if what.len() <= 16 => trace::find(node, id),
            _ => return err(400, "einval", format!("trace id 应是 16 位以内的 hex：{what:?}")),
        }
    };
    json(200, &TraceReply { node_id: node, spans: spans.iter().map(span_out).collect() })
}

fn span_out(s: &trace::Span) -> TraceSpan {
    TraceSpan {
        trace_id: format!("{:016x}", s.trace_id),
        span_id: s.span_id,
        node: s.node,
        component: s.component.into(),
        op: s.op.into(),
        start_ms: s.start_ms,
        dur_us: s.dur_us,
        code: trace::code_name(s.code),
    }
}
