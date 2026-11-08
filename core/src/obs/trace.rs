//! span 环形缓冲与慢请求日志（design.md 3.11）。
//!
//! 每个服务端请求在回复发出（或被丢弃、自动回 EIO）时记一个 span（`net/server.rs` 的 `Responder`）：trace_id、第几跳（span_id：客户端
//! 发来的是 0，链上每转发一跳 + 1）、本节点号、消息族、操作名、开始的墙钟毫秒、耗时（微秒）、结果码。trace_id 为 0 的不记（服务端之间的
//! 心跳、raft、同步源……不属于任何客户端操作）。每个进程保留最近 65 536 个 span（全进程一个环；同一个进程里起几个节点的测试按节点号区分）。
//! 环的每一格一把小锁：写的线程用原子计数取格号（`fetch_add` 后对 65 536 取模），只锁那一格——热路径上没有全进程共用的锁（3.11"热路径
//! 无锁"，M14 审查）；查的时候一格一格地读。`GET /api/v1/trace/<id>` 回本节点的 [`find`]，`GET /api/v1/trace/recent?n=` 回 [`recent`]；
//! `wmfs trace <id>` 并发问所有节点、按跳排成时间线（`cli/src/trace.rs`）。
//!
//! 慢请求：超过阈值（元数据族 50 ms、其余 100 ms）的写一行 WARN 日志（带 trace_id）并计数 `wmfs_slow_requests_total{family}`。FUSE 客户端
//! 没有 HTTP 端点：它的请求（连同重试）超过阈值时只写进客户端自己的日志（[`ClientTimer`]，`wmfs mount --log`）。
//! 实现注记（M14 审查）：阈值可调 = node.toml 的 `[obs] slow_meta_ms` / `slow_data_ms`（部署参数，默认 50 / 100；server 启动时
//! [`set_slow_ms`]，进程级；客户端用默认值；主管已确认，ADR-0045 第 3 条）——原先用 fail point `obs.slow.*`，fail point 不是生产配置的渠道。
//! 实现注记（M14 收尾，ADR-0045 第 9 条）：慢日志那一行另列出这个请求在本节点上走过的各阶段耗时（`obs::stages`：元数据的 decode、排队、
//! authz、quota、执行、raft 提交，数据的 decode、排队、authz、磁盘、转发；没走过的不写）；跨节点的分解仍是 `wmfs trace` 的时间线。span 的
//! 结构与 trace 端点的 JSON 不变（各阶段不进环）。

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use wmfs_api::metrics;

use crate::base::{ErrCode, lock, log};
use crate::obs::stages::Stages;

/// 每个进程保留的 span 数（3.11）。
pub const RING: usize = 65_536;

/// 一个 span（见文件头）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Span {
    pub trace_id: u64,
    pub span_id: u16,
    pub node: u32,
    pub component: &'static str, // 消息族：MetaCall、DataCall、PeerCall……
    pub op: &'static str,
    pub start_ms: u64,
    pub dur_us: u64,
    pub code: ErrCode,
}

/// 结果码的写法："OK" 或错误码名。
pub fn code_name(c: ErrCode) -> String {
    if c == ErrCode(0) { "OK".into() } else { c.to_string() }
}

/// 环（见文件头）：每格一把锁，`NEXT` 是下一个要写的格号（只增，取模用）。
fn ring() -> &'static [Mutex<Option<Span>>] {
    static R: OnceLock<Vec<Mutex<Option<Span>>>> = OnceLock::new();
    R.get_or_init(|| (0..RING).map(|_| Mutex::new(None)).collect())
}
static NEXT: AtomicUsize = AtomicUsize::new(0);

/// 记一个 span（见文件头）：trace_id 为 0 的不记；超过阈值的另写一行慢日志（带上 `stages`：各阶段耗时）。
pub fn record(s: Span, stages: &Stages) {
    if s.trace_id == 0 {
        return;
    }
    if s.dur_us >= limit_ms(s.component).saturating_mul(1000) {
        slow(&s, stages);
    }
    let i = NEXT.fetch_add(1, Ordering::Relaxed) % RING;
    *lock(&ring()[i]) = Some(s);
}

/// 从新到旧逐格读出满足 `keep` 的 span，最多 `max` 个。
fn newest_first(keep: impl Fn(&Span) -> bool, max: usize) -> Vec<Span> {
    let next = NEXT.load(Ordering::Relaxed);
    let mut out = Vec::new();
    for back in 1..=next.min(RING) {
        let slot = *lock(&ring()[(next - back) % RING]);
        if let Some(s) = slot.filter(|s| keep(s)) {
            out.push(s);
            if out.len() >= max {
                break;
            }
        }
    }
    out
}

/// 节点 `node` 上属于 `trace_id` 的 span，按跳（span_id）与开始时刻排好。
pub fn find(node: u32, trace_id: u64) -> Vec<Span> {
    let mut v = newest_first(|s| s.node == node && s.trace_id == trace_id, usize::MAX);
    v.sort_by_key(|s| (s.span_id, s.start_ms));
    v
}

/// 节点 `node` 最近的 `n` 个客户端请求（span_id 0：客户端直接发来的，转发来的不算），从新到旧。
pub fn recent(node: u32, n: usize) -> Vec<Span> {
    newest_first(|s| s.node == node && s.span_id == 0, n)
}

/// 慢请求的阈值（毫秒，见文件头）：元数据族与其余。
static SLOW_META_MS: AtomicU64 = AtomicU64::new(50);
static SLOW_DATA_MS: AtomicU64 = AtomicU64::new(100);

/// 设慢请求的阈值（server 启动时按 node.toml 的 `[obs]` 调用；进程级）。
pub fn set_slow_ms(meta_ms: u64, data_ms: u64) {
    SLOW_META_MS.store(meta_ms, Ordering::Relaxed);
    SLOW_DATA_MS.store(data_ms, Ordering::Relaxed);
}

fn limit_ms(component: &str) -> u64 {
    let limit = if component == "MetaCall" { &SLOW_META_MS } else { &SLOW_DATA_MS };
    limit.load(Ordering::Relaxed)
}

fn slow_counter(family: &str) {
    let help = "超过慢请求阈值（node.toml [obs]，默认元数据 50 ms、其余 100 ms）的请求数";
    metrics::counter("wmfs_slow_requests_total", help, &[("family", family)]).inc();
}

fn slow(s: &Span, stages: &Stages) {
    slow_counter(s.component);
    let ms = s.dur_us as f64 / 1000.0;
    let t = s.trace_id;
    let st = stages.to_string(); // 一个阶段也没走过（例如解不出请求）就不写"各阶段"
    let st = if st.is_empty() { st } else { format!("；各阶段：{st}") };
    log::warn(
        "slow",
        format_args!(
            "慢请求：node{} {} {} 用了 {ms:.1} ms（{}{st}），trace {t:016x} 第 {} 跳——各节点的时间线：wmfs trace {t:016x}",
            s.node,
            s.component,
            s.op,
            code_name(s.code),
            s.span_id
        ),
    );
}

/// 客户端（FUSE 挂载、`wmfs fs`）一个请求连同重试的计时：离开作用域时超过阈值就写一行慢日志进客户端自己的日志（见文件头），带
/// trace_id，与 `wmfs trace` 的服务端时间线对得上。`op` 只在真的慢时才调用（拼操作名要分配）。
pub struct ClientTimer<'a> {
    pub component: &'static str,
    pub op: &'a dyn Fn() -> String,
    pub trace_id: u64,
    pub t0: Instant,
}

impl Drop for ClientTimer<'_> {
    fn drop(&mut self) {
        let us = self.t0.elapsed().as_micros() as u64;
        if us < limit_ms(self.component).saturating_mul(1000) {
            return;
        }
        slow_counter(self.component);
        let c = self.component;
        let t = self.trace_id;
        let op = (self.op)();
        let ms = us as f64 / 1000.0;
        log::warn(
            "slow",
            format_args!(
                "慢请求（客户端）：{c} {op} 连同重试用了 {ms:.1} ms，trace {t:016x}——服务端各节点的时间线：wmfs trace {t:016x}"
            ),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(s: Span) {
        record(s, &Stages::new(Instant::now()));
    }

    fn span(node: u32, trace_id: u64, span_id: u16, start_ms: u64) -> Span {
        let (component, op, dur_us, code) = ("DataCall", "WriteChunk", 10, ErrCode(0));
        Span { trace_id, span_id, node, component, op, start_ms, dur_us, code }
    }

    /// 只看本测试自己的节点号（同一个进程里别的测试的服务端也往这个环里记）。
    #[test]
    fn spans_are_found_per_node_and_the_ring_keeps_the_newest() {
        let (me, other) = (4_000_000_001, 4_000_000_002);
        rec(span(me, 0, 0, 1)); // trace 0：不记
        rec(span(me, 7, 2, 30));
        rec(span(me, 7, 0, 10));
        rec(span(me, 7, 1, 20));
        rec(span(other, 7, 0, 10));
        let f = find(me, 7);
        assert_eq!(f.iter().map(|s| s.span_id).collect::<Vec<_>>(), [0, 1, 2], "按跳排好，别的节点的不算");
        for i in 0..RING as u64 + 10 {
            rec(span(me, 1000 + i, 0, i));
        }
        assert!(find(me, 7).is_empty(), "环满了之后最旧的被覆盖");
        let r = recent(me, 3);
        let want = [1009 + RING as u64, 1008 + RING as u64, 1007 + RING as u64];
        assert_eq!(r.iter().map(|s| s.trace_id).collect::<Vec<_>>(), want, "最近的在前");
        assert!(recent(other, 5).iter().all(|s| s.node == other));
    }

    #[test]
    fn code_names() {
        assert_eq!(code_name(ErrCode(0)), "OK");
        assert_eq!(code_name(ErrCode::EACCES), "EACCES");
    }
}
