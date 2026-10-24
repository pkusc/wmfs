//! changelog 流与消费者游标（design.md 3.11、7.1，ADR-0005 第 5 条）：`GET /api/v1/changelog?after=&limit=&wait_ms=`、
//! `POST /api/v1/changelog/cursor {name, seq}`、`GET /api/v1/changelog/cursor?name=`。
//!
//! 长轮询：`after` 之后已经有记录就立即回；没有就等到有新记录提交（raft 线程应用了写 changelog 的批次之后唤醒，
//! `EmbeddedEngine::wait_changelog`）或 `wait_ms`（最多 30 s）到期，再读一次回复（可能是空页）。先记下"写了 changelog 的批次"
//! 计数、再读，所以读完之后才提交的记录也一定叫得醒。每个节点同时最多 2 个长轮询（1.2：至少留 2 个管理 API 线程给 CLI 与
//! dashboard），第 3 个起不等、立即回（没有新记录就是空页；客户端这时要自己隔一会儿再问，见 `wmfs changelog tail -f`）。
//! `after` 之后的一段已经被修剪 → `410 {"error":"gone","oldest": …}`（缺口显式；修剪在 M11 的 gc）。查询参数没给用默认值
//! （after 0、limit 1000、wait_ms 0），给了却不是非负整数 → `400 einval`（不静默当成默认值）。游标名只能由字母、数字和 `.` `_` `-`
//! 组成（1–64 个）。指标：`wmfs_changelog_longpolls`（挂着的长轮询数）；消费者的落后条数 `wmfs_changelog_consumer_lag{name}` 由
//! cluster-mgr 每 5 s 刷新（`meta::changelog::refresh_lag`）。实现注记（M8）：本节点在等的时候失去 leader 身份（raft 线程发布了新角色
//! 之后也唤醒等待者）→ 立即按 ADR-0038 第 2 条的状态码回（409 带 leader 的管理地址，或 503），不挂到 `wait_ms` 到期；醒来之后的那次读
//! 失败于 `NOT_LEADER` 时也一样（`api.rs` 的 `failed_at`）。

use std::sync::atomic::Ordering;
use std::time::Duration;

use wmfs_api::metrics;

use super::api::{failed, failed_at, leader_gate, read, write};
use super::http::{MgmtCtx, Reply, err, json};
use crate::api::types::{CursorReq, GoneReply};
use crate::api::url::param;
use crate::base::types::ReqCtx;
use crate::base::{ErrCode, Failed, clock};
use crate::meta::changelog::{self, PAGE_MAX, Page};
use crate::meta::engine::EmbeddedEngine;

/// 每个节点同时最多几个长轮询、每个最多挂多久（1.2、3.11）。
const MAX_POLLS: usize = 2;
const MAX_WAIT_MS: u64 = 30_000;

pub(super) fn serve(
    ctx: &MgmtCtx,
    eng: &EmbeddedEngine,
    req: &ReqCtx,
    method: &str,
    path: &str,
    query: &str,
    body: &[u8],
) -> Reply {
    match (method, path) {
        ("GET", "/api/v1/changelog") => match poll_args(query) {
            Ok((after, limit, wait_ms)) => poll(ctx, eng, req, after, limit, wait_ms),
            Err(f) => failed(f),
        },
        ("POST", "/api/v1/changelog/cursor") => {
            let Ok(c) = serde_json::from_slice::<CursorReq>(body) else {
                return err(400, "einval", "body 应为 {\"name\": \"…\", \"seq\": n}".into());
            };
            if let Err(f) = check_name(&c.name) {
                return failed(f);
            }
            match write(eng, req, move |v, x| changelog::set_cursor(v, x, &c.name, c.seq)) {
                Ok(out) => json(200, &out),
                Err(f) => failed_at(eng, f),
            }
        }
        ("GET", "/api/v1/changelog/cursor") => {
            let name = param(query, "name").unwrap_or_default();
            match read(eng, req, |v, _c| Ok(changelog::get_cursor(v, &name)?)) {
                Ok(Some(c)) => json(200, &c),
                Ok(None) => err(404, "enoent", format!("没有名为 {name:?} 的游标")),
                Err(f) => failed_at(eng, f),
            }
        }
        _ => err(400, "einval", format!("{path} 不支持 {method}")),
    }
}

/// `GET /api/v1/changelog` 的参数 (after, limit, wait_ms)：没给的用默认值，limit 与 wait_ms 按上限截短（见文件头）。
fn poll_args(query: &str) -> Result<(u64, u32, u64), Failed> {
    let num = |name: &str, dflt: u64| match param(query, name) {
        None => Ok(dflt),
        Some(v) => v.parse::<u64>().map_err(|_| Failed::new(ErrCode::EINVAL, format!("{name} 应是非负整数：{v:?}"))),
    };
    let limit = num("limit", PAGE_MAX as u64)?.min(PAGE_MAX as u64) as u32;
    Ok((num("after", 0)?, limit, num("wait_ms", 0)?.min(MAX_WAIT_MS)))
}

fn check_name(n: &str) -> Result<(), Failed> {
    let ok = (1..=64).contains(&n.len()) && n.bytes().all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b));
    if ok {
        Ok(())
    } else {
        Err(Failed::new(ErrCode::EINVAL, format!("游标名只能是 1–64 个字母、数字或 . _ -：{n:?}")))
    }
}

/// 长轮询（见文件头）。
fn poll(ctx: &MgmtCtx, eng: &EmbeddedEngine, req: &ReqCtx, after: u64, limit: u32, wait_ms: u64) -> Reply {
    // 占一个长轮询名额；不要等（wait_ms = 0），或者已经有 MAX_POLLS 个在等 → 这一个不等，立即回
    let held = if wait_ms == 0 {
        false
    } else if ctx.polls.fetch_add(1, Ordering::SeqCst) < MAX_POLLS {
        true
    } else {
        ctx.polls.fetch_sub(1, Ordering::SeqCst);
        false
    };
    let gauge = metrics::gauge("wmfs_changelog_longpolls", "正在挂着的 changelog 长轮询数", &[]);
    gauge.set(ctx.polls.load(Ordering::SeqCst) as f64);
    let deadline = clock::mono_ms() + if held { wait_ms } else { 0 };
    let out = loop {
        let seen = eng.changelog_seen();
        let page = read(eng, req, |v, _c| Ok(changelog::page(v, after, limit)?));
        let now = clock::mono_ms();
        match page {
            Ok(Page::Events(p)) if p.events.is_empty() && now < deadline => {
                eng.wait_changelog(seen, Duration::from_millis(deadline - now));
                if let Some(r) = leader_gate(eng) {
                    break r; // 等的时候本节点失去了 leader（M8）：立即回 409 / 503，不挂到超时
                }
            }
            Ok(Page::Events(p)) => break json(200, &p),
            Ok(Page::Gone { oldest }) => {
                let message = format!("seq {after} 之后的一段已经被修剪（保留期之外），最早还在的是 {oldest}");
                break json(410, &GoneReply { error: "gone".into(), message, oldest });
            }
            Err(f) => break failed_at(eng, f),
        }
    };
    if held {
        ctx.polls.fetch_sub(1, Ordering::SeqCst);
        gauge.set(ctx.polls.load(Ordering::SeqCst) as f64);
    }
    out
}
