//! alert（mgmt_ext + event_sink，design.md 4.14）：规则（`rules.rs`；内置的在 `default_rules.toml`，mgr.toml 的 `[alert] rules` 可以补充、
//! 覆盖）、状态机 inactive → pending → firing → resolved（`for` 防抖）、动作 = 日志（mgr 的标准错误）+ 可选 webhook；当前状态在 mgr 的
//! `GET /alerts`（JSON，[`AlertsReply`]），`wmfs dev wait alert <规则> firing|resolved` 用它（[`fetch`]）。
//!
//! - 指标型规则：每 5 s（mgmt_ext 的 tick）抓一次全部节点的 `/metrics`（连同 mgr 自己的指标，每个样本带 `node` 标签）求值：条件成立 →
//!   pending，持续 `for` → firing；条件不成立 → pending 回到 inactive、firing 变 resolved（resolved 一直保持到下一次条件成立）。
//!   `rate(…)` 按序列（节点 + 标签）分别求相邻两轮的增量再相加：只算两轮都抓到的序列，变小的（节点重启）按 0——某个节点一轮没抓到、
//!   下一轮回来时，它的整个计数器值不会被当成增量（实现注记（M14 审查））。
//! - `grows(…)`（实现注记（M14 收尾，ADR-0045 第 10 条）：4.14 的"协调器积压 1 h 内持续增长"）：值 = 全部节点之和。满足比较（例如
//!   积压 > 0）→ pending，并记下这时的值；之后每满一个 `for` 比一次：比窗口开始时大 → firing（已在 firing 就继续），不大 → pending 从现在
//!   （用现值）重新计时、firing 变 resolved（这一个 `for` 里不再增长）。不满足比较（积压清零）照常立即回到 inactive / resolved。于是大目录
//!   的正常迁移（积压一直在减）、涨了又回落、持平都不告警。
//! - 事件型规则（实现注记（M14）：规格只写了"任何 PolicyViolation 事件"这类规则，状态机取最小方案）：收到匹配的 changelog 记录即 firing，
//!   之后 `for`（默认 5 min）内没有新的就 resolved。事件由宿主的 changelog 循环投递（游标 `alert`），mgr 停着的期间的事件重启后照样补上。
//!
//! 同一个插件挂在两个钩子上：[`AlertExt`] 与 [`AlertSink`] 共用锁里的 [`Alerts`]。

pub mod rules;

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use wmfs_api::metrics;
use wmfs_api::types::ChangeEvent;
use wmfs_api::units::fmt_duration;

use crate::api::ApiClient;
use crate::host::{EventSink, HttpOut, MgmtExt, Router};
use crate::prom::{self, Sample};
use rules::{Agg, Cond, Rule};

/// `GET /alerts` 里 mgr 最近收到的 changelog 记录留几条。
const RECENT_EVENTS: usize = 30;

/// 一条规则的状态（JSON 里是小写的字符串）。
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum State {
    #[default]
    Inactive,
    Pending,
    Firing,
    Resolved,
}

impl State {
    pub fn as_str(self) -> &'static str {
        match self {
            State::Inactive => "inactive",
            State::Pending => "pending",
            State::Firing => "firing",
            State::Resolved => "resolved",
        }
    }
}

/// 一条规则此刻的状态：`since_ms` = 进入这个状态的时刻（墙钟毫秒），`value` = 指标型规则最近的值（事件型：这一段收到的事件数），
/// `message` = 最近一次让它成立的原因。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
pub struct AlertInfo {
    pub name: String,
    pub rule: String,
    pub state: State,
    pub since_ms: u64,
    pub value: f64,
    pub message: String,
}

/// mgr 的 `GET /alerts`：每条规则的状态，与 mgr 最近收到的 30 条 changelog 记录 `recent`（旧的在前；dashboard 的"事件与告警"面板用它，
/// 不必再问管理 API）。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
pub struct AlertsReply {
    pub alerts: Vec<AlertInfo>,
    #[serde(default)]
    pub recent: Vec<ChangeEvent>,
}

/// 一条规则的状态机。
struct RuleState {
    info: AlertInfo,
    last_event_ms: u64,         // 事件型：最近一次匹配的事件
    prev: HashMap<String, f64>, // rate：上一轮各序列（标签原文，含 node）的值
    prev_ms: Option<u64>,       // rate：上一轮的时刻
    base: f64,                  // grows：这个窗口开始时的值
    window_ms: u64,             // grows：这个窗口开始的时刻
}

/// 全部规则的状态机。
pub struct Alerts {
    rules: Vec<Rule>,
    states: Vec<RuleState>,
    recent: VecDeque<ChangeEvent>,
}

impl Alerts {
    pub fn new(rules: Vec<Rule>) -> Alerts {
        let new_state = |r: &Rule| RuleState {
            info: AlertInfo { name: r.name.clone(), rule: r.text.clone(), ..Default::default() },
            last_event_ms: 0,
            prev: HashMap::new(),
            prev_ms: None,
            base: 0.0,
            window_ms: 0,
        };
        Alerts { states: rules.iter().map(new_state).collect(), rules, recent: VecDeque::new() }
    }

    pub fn snapshot(&self) -> AlertsReply {
        AlertsReply {
            alerts: self.states.iter().map(|st| st.info.clone()).collect(),
            recent: self.recent.iter().cloned().collect(),
        }
    }

    /// 一轮求值（见文件头）；返回这一轮变成 firing 或 resolved 的规则（下标）。
    pub fn tick(&mut self, samples: &[Sample], now: u64) -> Vec<usize> {
        let mut out = Vec::new();
        for (i, (r, st)) in self.rules.iter().zip(&mut self.states).enumerate() {
            let expr = match &r.cond {
                Cond::Metric(expr) => expr,
                Cond::Event { .. } => {
                    if st.info.state == State::Firing && now.saturating_sub(st.last_event_ms) >= r.for_ms {
                        let quiet = fmt_duration(Duration::from_millis(r.for_ms));
                        st.info.message = format!("已恢复：{quiet} 内没有新的 {}", r.text);
                        enter(st, State::Resolved, now, &mut out, i);
                    }
                    continue;
                }
            };
            let value = match expr.agg {
                Agg::Any => expr.values(samples).find(|v| expr.holds(*v)),
                Agg::Sum | Agg::Grows => Some(expr.values(samples).fold(0.0, |a, b| a + b)).filter(|v| expr.holds(*v)),
                Agg::Rate => {
                    let cur: HashMap<String, f64> =
                        expr.matching(samples).map(|s| (s.labels.clone(), s.value)).collect();
                    let rate = st.prev_ms.filter(|t| now > *t).map(|t| {
                        let grown = cur.iter().filter_map(|(k, v)| st.prev.get(k).map(|p| (v - p).max(0.0)));
                        grown.fold(0.0, |a, b| a + b) * 1000.0 / (now - t) as f64
                    });
                    st.prev = cur;
                    st.prev_ms = Some(now);
                    rate.filter(|v| expr.holds(*v))
                }
            };
            let before = st.info.state; // 进入这一轮时的状态
            match value {
                Some(v) => {
                    st.info.value = v;
                    if before == State::Inactive || before == State::Resolved {
                        enter(st, State::Pending, now, &mut out, i);
                        (st.base, st.window_ms) = (v, now); // grows：记下进入 pending 时的值
                    }
                    if expr.agg == Agg::Grows {
                        grows(st, r, v, now, &mut out, i);
                    } else if st.info.state == State::Pending && now.saturating_sub(st.info.since_ms) >= r.for_ms {
                        // 看的是刚改过的状态：for = 0 的规则同一轮里直接 pending → firing
                        st.info.message = format!("{}（现在的值 {v}）", r.text);
                        enter(st, State::Firing, now, &mut out, i);
                    }
                }
                None if before == State::Pending => enter(st, State::Inactive, now, &mut out, i),
                None if before == State::Firing => {
                    st.info.message = format!("已恢复：{} 不再成立", r.text);
                    enter(st, State::Resolved, now, &mut out, i);
                }
                None => {}
            }
        }
        out
    }

    /// 一条 changelog 记录（事件型规则，见文件头）；返回因此变成 firing 的规则。
    pub fn event(&mut self, e: &ChangeEvent, now: u64) -> Vec<usize> {
        if self.recent.len() == RECENT_EVENTS {
            self.recent.pop_front();
        }
        self.recent.push_back(e.clone());
        let mut out = Vec::new();
        for (i, (r, st)) in self.rules.iter().zip(&mut self.states).enumerate() {
            if !r.cond.matches(e) {
                continue;
            }
            let extra: Vec<String> = e.extra.iter().map(|(k, v)| format!("{k}={v}")).collect();
            st.info.message = format!("#{} {} ino={} {}（{}）", e.seq, e.op, e.ino, extra.join(" "), r.text);
            st.last_event_ms = now;
            if st.info.state != State::Firing {
                st.info.value = 0.0;
                enter(st, State::Firing, now, &mut out, i);
            }
            st.info.value += 1.0;
        }
        out
    }

    /// 变了状态的规则要通知的：(webhook, 现在的样子)。
    fn notes(&self, changed: &[usize]) -> Vec<(Option<String>, AlertInfo)> {
        changed.iter().map(|i| (self.rules[*i].webhook.clone(), self.states[*i].info.clone())).collect()
    }

    /// 更新 `wmfs_alerts_firing`（正在 firing 的规则数）。
    fn set_firing_gauge(&self) {
        let firing = self.states.iter().filter(|st| st.info.state == State::Firing).count();
        metrics::gauge("wmfs_alerts_firing", "正在 firing 的告警规则数", &[]).set(firing as f64);
    }
}

/// 进入新状态；firing 与 resolved 记进 `out`（要通知）。
fn enter(st: &mut RuleState, state: State, now: u64, out: &mut Vec<usize>, i: usize) {
    st.info.state = state;
    st.info.since_ms = now;
    if state == State::Firing || state == State::Resolved {
        out.push(i);
    }
}

/// `grows(…)` 满足比较时的一轮（见文件头）：每满一个 `for` 比一次窗口开始时的值，然后从现在（用现值）开始下一个窗口。
fn grows(st: &mut RuleState, r: &Rule, v: f64, now: u64, out: &mut Vec<usize>, i: usize) {
    if now.saturating_sub(st.window_ms) < r.for_ms {
        return;
    }
    let (from, span) = (st.base, fmt_duration(Duration::from_millis(r.for_ms)));
    (st.base, st.window_ms) = (v, now);
    match st.info.state {
        State::Pending if v > from => {
            st.info.message = format!("{}：{span} 内从 {from} 增长到 {v}", r.text);
            enter(st, State::Firing, now, out, i);
        }
        State::Firing if v <= from => {
            st.info.message = format!("已恢复：{} 在 {span} 内没有再增长（{from} → {v}）", r.text);
            enter(st, State::Resolved, now, out, i);
        }
        _ => {} // pending 没有增长：从现在重新计时；firing 还在增长：继续
    }
}

/// 动作：日志 + 可选 webhook（POST `{rule, state, message, at_ms}`，用与 webhook 插件相同的 agent；失败只写日志）。
fn notify(agent: &ureq::Agent, notes: Vec<(Option<String>, AlertInfo)>) {
    for (hook, a) in notes {
        crate::log(format_args!("告警 {} → {}：{}", a.name, a.state.as_str(), a.message));
        let Some(url) = hook else { continue };
        let body = serde_json::json!({"rule": a.name, "state": a.state, "message": a.message, "at_ms": a.since_ms});
        if let Err(e) = crate::webhook::post_json(agent, &url, body.to_string().as_bytes()) {
            crate::log(format_args!("告警 {} 的 webhook 失败：{e}", a.name));
        }
    }
}

fn lock(m: &Mutex<Alerts>) -> MutexGuard<'_, Alerts> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// 问 mgr 的 `GET /alerts`（`base` = `http://host:port`，`token` = dashboard token；不走代理）。`wmfs dev wait alert` 用它。
pub fn fetch(base: &str, token: &str) -> Result<AlertsReply, String> {
    let agent: ureq::Agent =
        ureq::Agent::config_builder().proxy(None).timeout_global(Some(Duration::from_secs(5))).build().into();
    let url = format!("{base}/alerts?token={token}");
    let mut resp = agent.get(&url).call().map_err(|e| format!("问 {base}/alerts 失败：{e}"))?;
    let text = resp.body_mut().read_to_string().map_err(|e| format!("读 {base}/alerts 的回复失败：{e}"))?;
    serde_json::from_str(&text).map_err(|e| format!("{base}/alerts 的回复不对：{e}"))
}

/// 指标型规则那一半（mgmt_ext）：注册 `GET /alerts`，每 5 s 抓指标求值。
pub struct AlertExt(Arc<Mutex<Alerts>>, ureq::Agent);
/// 事件型规则那一半（event_sink）。
pub struct AlertSink(Arc<Mutex<Alerts>>, ureq::Agent);

/// `agent`：发规则里的 webhook 用（`webhook::agent`，代理设置同 mgr.toml 的 `[webhook] proxy`）。
pub fn plugin(rules: Vec<Rule>, agent: ureq::Agent) -> (AlertExt, AlertSink) {
    let a = Arc::new(Mutex::new(Alerts::new(rules)));
    (AlertExt(a.clone(), agent.clone()), AlertSink(a, agent))
}

impl MgmtExt for AlertExt {
    fn manifest_toml(&self) -> &'static str {
        include_str!("manifest.toml")
    }

    fn start(&mut self, _api: &ApiClient, router: &mut Router) -> Result<(), String> {
        let a = self.0.clone();
        router.add("/alerts", Box::new(move |_| HttpOut::json(200, &lock(&a).snapshot())));
        Ok(())
    }

    fn tick(&mut self, api: &ApiClient) {
        let samples = prom::parse(&prom::merge(&prom::scrape(api))); // 每个样本带 node 标签（rate 按序列算）
        let notes = {
            let mut a = lock(&self.0);
            let changed = a.tick(&samples, crate::now_ms());
            a.set_firing_gauge();
            a.notes(&changed)
        };
        notify(&self.1, notes);
    }
}

impl EventSink for AlertSink {
    fn manifest_toml(&self) -> &'static str {
        include_str!("manifest.toml")
    }

    fn deliver(&mut self, batch: &[ChangeEvent]) -> Result<(), String> {
        let notes = {
            let mut a = lock(&self.0);
            let changed: Vec<usize> = batch.iter().flat_map(|e| a.event(e, crate::now_ms())).collect();
            a.set_firing_gauge();
            a.notes(&changed)
        };
        notify(&self.1, notes);
        Ok(())
    }
}

#[cfg(test)]
mod tests;
