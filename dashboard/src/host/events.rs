//! mgr 里**唯一**的 changelog 长轮询循环（design.md 3.11、4.13）：从各 sink 游标的最小值开始拉，按 sink 分发，每个 sink 各自提交自己的
//! 游标（`POST /api/v1/changelog/cursor {name, seq}`，名字 = 插件名）——至少一次投递，事件带 seq，下游按 seq 去重。
//!
//! - 起点：sink 的游标（mgr 重启后从游标接着投）；还没有游标（第一次运行）→ 从此刻最新的 seq 开始，并马上提交一个游标（之后 mgr 停着的
//!   那段时间的记录按游标保留，3.11）。**`audit-log` 例外**：它的游标由 core 在 `audit` 第一次启用时建（3.8），宿主不建——没有就不投递，
//!   每 30 s 看一次游标出现了没有（没开过审计的集群跑过 mgr 之后，health 也不会显示"审计消费者落后 N 条"）。
//! - 先投递、投递成功之后才提交游标、才前进；失败（插件出错、fail point、游标没提交上）→ 这个 sink 指数退避（1 s 起，上限 5 min，4.13 的
//!   webhook 规则对所有 sink 都适用），不挡别的 sink：每一轮只从"现在该投的" sink 里最落后的那个位置拉。
//! - 攒批（4.13 的 webhook："每 100 条或 1 s 一批"）：`hold` 不为 0 的 sink 不到 100 条时，从第一次看到这一批起最多等 `hold` 再投。长轮询
//!   最多挂到下一个 sink 该投的时刻（攒批、退避、等游标），不超过 5 s。
//! - 读回 `410 Gone`（落后超出保留范围，那一段已被修剪）：回复里的 `oldest` 是还在的最早一条，落在缺口里的 sink 从它之前接着投；大声写一行
//!   错误日志并计数 `wmfs_mgr_changelog_gaps_total{sink}`（告警规则 `changelog_gap` 看它）——缺口是显式的，不是静默的。
//! - 服务端没挂住长轮询（每个节点同时最多 2 个）就回了空页 → 补足等待（最多 1 s）再问，不忙等（3.11 的 M7 注记）；连不上、换 leader →
//!   1 s 后再试。
//! - `wmfs_mgr_sink_stalled_seconds{sink}`：这个 sink 落后（游标 < 最新的 seq）、又多久没有前进了（不落后、或还没有游标就是 0）。告警
//!   `audit_stalled`（"审计消费者 10 分钟没有前进"）看它：只看落后条数的话，持续写入时消费者明明在前进也一直落后几条。

use std::time::{Duration, Instant};

use wmfs_api::metrics;
use wmfs_api::types::{ChangeEvent, ChangelogPage, CursorReply, CursorReq};

use super::{EventSink, Hooked};
use crate::api::ApiClient;

/// 长轮询最多挂多久（ApiClient 的单次请求超时是 10 s）；没有游标的 audit-log 多久再看一次。
const WAIT_MAX: Duration = Duration::from_secs(5);
const RECHECK: Duration = Duration::from_secs(30);

/// 一个 event_sink 插件与它的投递位置。
pub(crate) struct Sink {
    pub sink: Box<dyn EventSink>,
    pub hook: Hooked,
    hold: Duration,        // 攒批：不到 100 条时最多等这么久（webhook 1 s；别的 0 = 有就投）
    own_cursor: bool,      // 没有游标时由宿主建（audit-log 为 false，见文件头）
    active: bool,          // 有游标、在投递
    pos: u64,              // 投递到了哪个 seq（= 已提交的游标）
    backoff_ms: u64,       // 投递失败之后的退避
    next_try: Instant,     // 这个时刻之前不投（退避、攒批、等游标）
    held: Option<Instant>, // 这一批从什么时候开始攒的
    moved: Instant,        // 上一次前进（或不落后）的时刻
}

impl Sink {
    pub(crate) fn new(sink: Box<dyn EventSink>, hook: Hooked, hold: Duration, own_cursor: bool) -> Sink {
        let now = Instant::now();
        Sink {
            sink,
            hook,
            hold,
            own_cursor,
            active: false,
            pos: 0,
            backoff_ms: 0,
            next_try: now,
            held: None,
            moved: now,
        }
    }

    /// 这一轮看到最新的 seq 是 `last` 之后：更新"多久没有前进"（见文件头）。
    fn stalled(&mut self, last: u64) {
        let behind = self.active && self.pos < last;
        if !behind {
            self.moved = Instant::now();
        }
        let help = "event_sink 落后又没有前进的秒数（不落后为 0）";
        let g = metrics::gauge("wmfs_mgr_sink_stalled_seconds", help, &[("sink", self.hook.name.as_str())]);
        g.set(if behind { self.moved.elapsed().as_secs_f64().floor() } else { 0.0 });
    }

    /// 找起点（见文件头）：有游标就从游标接着投；没有 → 宿主从此刻最新的 seq 起建一个——audit-log 例外：不建，30 s 后再看。
    /// 连不上 → false（1 s 后再试）。
    fn find_start(&mut self, api: &ApiClient) -> bool {
        let name = &self.hook.name;
        let r = match cursor(api, name) {
            Ok(None) if self.own_cursor => {
                let seq = api.last_seq().map_err(|e| e.to_string());
                seq.and_then(|seq| commit(api, name, seq).map(|()| Some(seq)))
            }
            r => r,
        };
        match r {
            Ok(Some(seq)) => {
                self.pos = seq;
                self.active = true;
                self.moved = Instant::now();
            }
            Ok(None) => self.next_try = Instant::now() + RECHECK,
            Err(e) => {
                crate::log(format_args!("取 {name} 的游标失败：{e}（1 s 后再试）"));
                self.next_try = Instant::now() + Duration::from_secs(1);
                return false;
            }
        }
        true
    }

    /// 投一批（最后一条是 `last`）：插件投递成功、游标提交上了才前进；否则退避（见文件头）。
    fn deliver(&mut self, api: &ApiClient, batch: &[ChangeEvent], last: u64) {
        let sink = &mut self.sink;
        match self.hook.call(|| sink.deliver(batch)).and_then(|()| commit(api, &self.hook.name, last)) {
            Ok(()) => {
                self.pos = last;
                self.backoff_ms = 0;
                self.held = None;
                self.moved = Instant::now();
            }
            Err(e) => {
                self.backoff_ms = (self.backoff_ms * 2).clamp(1000, 300_000);
                self.next_try = Instant::now() + Duration::from_millis(self.backoff_ms);
                let (name, from, secs) = (&self.hook.name, self.pos + 1, self.backoff_ms / 1000);
                crate::log(format_args!("{name} 投递 seq {from}..={last} 失败：{e}（{secs} s 后重试）"));
            }
        }
    }
}

fn pause(ms: u64) {
    std::thread::sleep(Duration::from_millis(ms));
}

/// `name` 的游标：没有 → None。
fn cursor(api: &ApiClient, name: &str) -> Result<Option<u64>, String> {
    match api.get::<CursorReply>(&format!("/api/v1/changelog/cursor?name={name}")) {
        Ok(c) => Ok(Some(c.seq)),
        Err(e) if e.status == 404 => Ok(None),
        Err(e) => Err(e.to_string()),
    }
}

fn commit(api: &ApiClient, name: &str, seq: u64) -> Result<(), String> {
    let req = CursorReq { name: name.into(), seq };
    api.post::<_, CursorReply>("/api/v1/changelog/cursor", &req).map(|_| ()).map_err(|e| format!("提交游标失败：{e}"))
}

/// 循环（在调用线程上跑，不返回）。
pub(crate) fn run(api: &ApiClient, mut sinks: Vec<Sink>) -> ! {
    for s in &mut sinks {
        while !s.find_start(api) {
            pause(1000); // 连不上：一直重试
        }
    }
    loop {
        round(api, &mut sinks);
    }
}

fn round(api: &ApiClient, sinks: &mut [Sink]) {
    let now = Instant::now();
    for s in sinks.iter_mut().filter(|s| !s.active && s.next_try <= now) {
        s.find_start(api); // 还没有游标的（audit-log）：游标出现了没有
    }
    let due = |s: &Sink| s.active && s.next_try <= now;
    let Some(from) = sinks.iter().filter(|s| due(s)).map(|s| s.pos).min() else {
        return pause(200); // 都在退避（或在等游标）
    };
    let soonest = sinks.iter().filter(|s| s.next_try > now).map(|s| s.next_try - now).min();
    let wait = soonest.unwrap_or(WAIT_MAX).min(WAIT_MAX);
    let t0 = Instant::now();
    let path = format!("/api/v1/changelog?after={from}&limit=1000&wait_ms={}", wait.as_millis());
    let page: ChangelogPage = match api.get(&path) {
        Ok(p) => p,
        Err(e) if e.status == 410 => return gap(sinks, from, e.oldest),
        Err(e) => {
            crate::log(format_args!("拉 changelog 失败：{e}（1 s 后再试）"));
            return pause(1000);
        }
    };
    for s in sinks.iter_mut() {
        s.stalled(page.last_seq);
    }
    if page.events.is_empty() {
        // 服务端挂满了 wait 才回的空页：马上接着问；没挂住（名额满了）就回了：补足等待（最多 1 s）再问，不忙等
        if let Some(rest) = wait.min(Duration::from_secs(1)).checked_sub(t0.elapsed()) {
            std::thread::sleep(rest);
        }
        return;
    }
    for s in sinks.iter_mut().filter(|s| due(s)) {
        let batch = &page.events[page.events.partition_point(|e| e.seq <= s.pos)..];
        let Some(last) = batch.last().map(|e| e.seq) else { continue };
        if batch.len() < 100 && !s.hold.is_zero() {
            let since = *s.held.get_or_insert(now);
            if now < since + s.hold {
                s.next_try = since + s.hold; // 攒批（见文件头）：到点再投
                continue;
            }
        }
        s.deliver(api, batch, last);
    }
}

/// `from` 之后读回 410（见文件头）：`oldest` 是还在的最早一条，落在缺口里的 sink 从它之前接着投。
fn gap(sinks: &mut [Sink], from: u64, oldest: Option<u64>) {
    let Some(kept) = oldest.map(|o| o.saturating_sub(1)).filter(|k| *k > from) else {
        crate::log(format_args!("seq {from} 之后读回 410，但回复里没有可用的 oldest（1 s 后再试）"));
        return pause(1000);
    };
    for s in sinks.iter_mut().filter(|s| s.active && s.pos < kept) {
        let name = s.hook.name.as_str();
        crate::log(format_args!(
            "错误：{name} 落后太多，seq {}..={kept} 已被修剪（保留期之外），这一段不会再投递（告警 changelog_gap）",
            s.pos + 1
        ));
        let help = "event_sink 遇到的 changelog 缺口次数（410）";
        metrics::counter("wmfs_mgr_changelog_gaps_total", help, &[("sink", name)]).inc();
        s.pos = kept;
    }
}

#[cfg(test)]
mod tests;
