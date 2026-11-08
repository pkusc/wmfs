//! `wmfs trace <trace_id>`、`wmfs trace --recent [N]`（design.md 3.11、7.2）：并发问集群里每个节点的 `GET /api/v1/trace/<id>`
//! （`GET /api/v1/trace/recent?n=`），合起来排成时间线。节点列表来自 `GET /api/v1/cluster`（`--cluster` 里只写了元数据节点也行；问不到
//! 就用 `--cluster` 的地址）。
//!
//! - `<trace_id>`：按跳（span_id）排——第 0 跳是第一个服务端（元数据 leader 或链头），链上每转发一跳 + 1；每一跳显示总耗时与"本节点"的
//!   耗时（减去下一跳的总耗时）。客户端自己的耗时在客户端日志里（超过阈值的请求写一行带 trace_id 的慢日志）。
//! - `--recent [N]`（默认 10）：全部节点上最近的 N 个客户端请求（第 0 跳），按开始时刻从新到旧：时刻、节点、操作、耗时、trace_id。
//!
//! `--json` 输出合起来的 span 数组（`TraceSpan`）。连不上的节点跳过（全都连不上才报错）。
//!
//! trace_id 的写法：16 位以内的 hex（`wmfs trace --recent`、慢请求日志、`GET /api/v1/trace` 都这样写）；changelog 与审计日志里的
//! `trace_id` 是十进制的 JSON 数字，写成 `d:<十进制>` 就行（实现注记（M14 审查）：例如 `wmfs trace d:1311768467463790320`）。

use wmfs_core::api::types::{TraceReply, TraceSpan};
use wmfs_core::base::log::utc_string;
use wmfs_mgr::api::{ApiClient, ApiError};
use wmfs_mgr::prom::each_node;

use crate::attr::{ApiArgs, failed, run};

#[derive(clap::Args)]
pub struct TraceArgs {
    /// trace_id：16 位以内的 hex（wmfs trace --recent、慢请求日志里的那个），或 d:<十进制>（changelog、审计日志里的那个）
    pub id: Option<String>,
    /// 列出全部节点上最近的 N 个客户端请求（不写 N 是 10）
    #[arg(long, num_args = 0..=1, default_missing_value = "10")]
    pub recent: Option<usize>,
    #[command(flatten)]
    pub api: ApiArgs,
}

/// 命令行上的 trace_id → 管理 API 要的 hex（见文件头）。
fn hex_id(id: &str) -> Result<String, String> {
    match id.strip_prefix("d:") {
        Some(d) => {
            d.parse::<u64>().map(|v| format!("{v:016x}")).map_err(|_| format!("d: 后面应是十进制的 trace_id：{id:?}"))
        }
        None => Ok(id.trim_start_matches("0x").to_string()),
    }
}

pub fn run_trace(a: TraceArgs) -> i32 {
    let target = match (&a.id, a.recent) {
        (Some(id), None) => hex_id(id).map(|h| (format!("/api/v1/trace/{h}"), false)),
        (None, Some(n)) => Ok((format!("/api/v1/trace/recent?n={n}"), true)), // 每个节点最新的 n 个，合起来再取最新的 n 个
        _ => Err("给一个 trace_id，或者 --recent [N]".to_string()),
    };
    let (path, recent) = match target {
        Ok(t) => t,
        Err(message) => return failed(a.api.json, &ApiError::new(0, "einval", message)),
    };
    let n = a.recent.unwrap_or(usize::MAX);
    let call = move |c: &ApiClient, _: &str| gather(c, &path, recent).map(|v| v.into_iter().take(n).collect());
    run(&a.api, None, call, move |s: &Vec<TraceSpan>| if recent { list(s) } else { timeline(s) })
}

/// 并发问每个节点（见文件头）；`recent` 按开始时刻从新到旧排，否则按跳排。
fn gather(c: &ApiClient, path: &str, recent: bool) -> Result<Vec<TraceSpan>, ApiError> {
    let replies: Vec<Result<TraceReply, ApiError>> =
        each_node(c, |a| c.get_at::<TraceReply>(a, path)).into_iter().map(|(_, r)| r).collect();
    // 全部节点都没回答才报错（报第一个节点的错误）；有的节点连不上就跳过
    if replies.iter().all(Result::is_err) {
        return match replies.into_iter().next() {
            Some(Err(e)) => Err(e),
            _ => Ok(vec![]), // 一个节点也没有
        };
    }
    let mut v: Vec<TraceSpan> = replies.into_iter().flatten().flat_map(|r| r.spans).collect();
    if recent {
        v.sort_by_key(|s| std::cmp::Reverse(s.start_ms));
    } else {
        v.sort_by_key(|s| (s.span_id, s.start_ms, s.node));
    }
    Ok(v)
}

fn ms(us: u64) -> String {
    format!("{:.1} ms", us as f64 / 1000.0)
}

fn timeline(spans: &[TraceSpan]) -> String {
    let Some(first) = spans.first() else {
        return "没有找到这个 trace 的 span（各节点只保留最近 65536 个；trace_id 写对了吗？）".into();
    };
    let (id, n) = (&first.trace_id, spans.len());
    let mut out = vec![format!("trace {id}：{n} 个 span（从第一个服务端开始；客户端自己的耗时在客户端日志里）")];
    for s in spans {
        // 这一跳"本节点"的耗时 = 这一跳的总耗时 − 下一跳的总耗时（链尾没有下一跳）
        let next = spans.iter().find(|n| n.span_id == s.span_id + 1);
        let own = next.map_or(String::new(), |n| format!("（本节点 {}）", ms(s.dur_us.saturating_sub(n.dur_us))));
        let at = s.start_ms.saturating_sub(first.start_ms);
        let (hop, node, total) = (s.span_id, s.node, ms(s.dur_us));
        out.push(format!("  第 {hop} 跳 +{at} ms  node{node}  {} {}  {total}{own}  {}", s.component, s.op, s.code));
    }
    out.join("\n")
}

fn list(spans: &[TraceSpan]) -> String {
    if spans.is_empty() {
        return "各节点最近都没有客户端请求".into();
    }
    let row = |s: &TraceSpan| {
        let t = utc_string(s.start_ms);
        format!("{t}  node{}  {} {}  {}  {}  trace {}", s.node, s.component, s.op, ms(s.dur_us), s.code, s.trace_id)
    };
    spans.iter().map(row).collect::<Vec<_>>().join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_timeline_shows_each_hop_and_its_own_time() {
        let s = |span_id, node, op: &str, dur_us| TraceSpan {
            trace_id: "00000000000000ff".into(),
            span_id,
            node,
            component: if span_id == 0 { "DataCall".into() } else { "PeerCall".into() },
            op: op.into(),
            start_ms: 1000 + span_id as u64,
            dur_us,
            code: "OK".into(),
        };
        let t = timeline(&[s(0, 1, "WriteChunk", 9000), s(1, 2, "ForwardWrite", 6000), s(2, 3, "ForwardWrite", 2500)]);
        let lines: Vec<&str> = t.lines().collect();
        assert!(lines[1].contains("第 0 跳 +0 ms  node1  DataCall WriteChunk  9.0 ms（本节点 3.0 ms）"), "{t}");
        assert!(lines[2].contains("第 1 跳 +1 ms  node2  PeerCall ForwardWrite  6.0 ms（本节点 3.5 ms）"), "{t}");
        assert!(lines[3].contains("第 2 跳 +2 ms  node3  PeerCall ForwardWrite  2.5 ms  OK"), "链尾没有下一跳：{t}");
        assert!(timeline(&[]).contains("没有找到"));
        assert_eq!(hex_id("d:255"), Ok("00000000000000ff".into()), "changelog 里的十进制写法");
        assert_eq!(hex_id("0x1f"), Ok("1f".into()));
        assert!(hex_id("d:x").is_err());
    }
}
