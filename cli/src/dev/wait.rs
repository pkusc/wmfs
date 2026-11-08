//! `wmfs dev wait <条件> [--timeout 120]`（design.md 7.2）：替代演示与验收里的固定 sleep——条件满足立即返回 0，
//! 超时返回 1 并打印当前状态。每 100 ms 问一遍。
//!
//! - `leader`（M3）：有一个节点报告自己是**就绪**的元数据 leader（health 的 `meta_role = "leader"`：已当选、本任期的空条目已应用、
//!   插件清单一致，可以服务请求）。
//! - `chains-serving`（M9）：每条链都有 `want` 个成员（= min(声明副本数, 这个存储类可以用的 target 所在的 ACTIVE 节点数)）且全部
//!   Serving（问 leader 的 `GET /api/v1/chains`；leader 最近没听到心跳的、刚重启还没被处理的成员不算 Serving）。等的过程中状态变了
//!   就记一行（最多 50 行），返回时连同最后一行一起打印：演示里看得到"先 Syncing、后 Serving"。
//!
//! - `session-evicted <挂载点>`（M10）：这个开发挂载现在的会话（挂载日志里最后一个会话号）两阶段驱逐都做完了——`GET /api/v1/sessions`
//!   里它的状态是 `evicted`（只剩 EVICTED 记录）。
//!
//! - `task-idle <任务>`（M11）：这个后台任务这一轮做完了、积压为 0。leader 任务（gc、reconciler）：开始等之后又做完了**两**轮（第一轮可能
//!   在触发它的那条命令提交之前就开始了，看不到它），并且这时是 done、积压 0；节点任务（scrub、gc-local）：每个节点都照办了开始等时的运行
//!   请求（`wmfs task run` 之后）、这一轮做完、积压 0（问每个节点的 `GET /api/v1/node`）——只做数据的节点收不到运行请求（`controlled` 为
//!   false），只看它这一轮做完、积压 0；连不上的节点不当它空闲，一直等到超时，超时信息写出在等哪个节点（ADR-0042 第 12 条）。没有这个
//!   任务 → 马上失败（不等到超时）。
//!
//! - `alert <规则> firing|resolved`（M14）：`wmfs dev mgr` 起的 mgr 的 `GET /alerts` 里这条规则是这个状态（resolved = 触发过、现在已恢复）；
//!   mgr 没在运行就接着等（超时信息里写着），规则名不存在 → 马上失败。
//! - `audit-caught-up`（M14）：`audit-log` 游标追上了**开始等时**最新的 changelog seq（之后又有新记录不影响）；还没有这个游标（没有目录
//!   开过 `audit`）→ 马上失败。

use std::path::Path;
use std::time::Duration;

use wmfs_core::api::types::{ChainsReply, CursorReply, NodeInfoReply, STATE_DONE, SessionsReply, TaskInfo};
use wmfs_core::base::clock;
use wmfs_mgr::api::ApiClient;

use super::cluster::api;
use super::{admin_key, health, mgmt_addr, nodes};

pub fn wait(d: &Path, what: &[String], timeout_s: u64) -> Result<Vec<String>, String> {
    let cond = what.first().map(String::as_str).unwrap_or_default();
    // 条件的参数：num = session-evicted 的会话号、audit-caught-up 开始等时最新的 changelog seq；name = 任务名、告警规则名
    let (num, name) = match (cond, what.get(1)) {
        ("leader" | "chains-serving", None) => (0, String::new()),
        ("session-evicted", Some(mp)) if what.len() == 2 => {
            (super::mount::session_of(d, Path::new(mp))?, String::new())
        }
        ("task-idle", Some(k)) if what.len() == 2 => (0, k.clone()),
        ("alert", Some(r)) if what.len() == 3 && ["firing", "resolved"].contains(&what[2].as_str()) => (0, r.clone()),
        ("audit-caught-up", None) => (api(d)?.last_seq().map_err(|e| e.to_string())?, String::new()),
        _ => {
            let have = "leader、chains-serving、session-evicted <挂载点>、task-idle <任务>、alert <规则> firing|resolved、audit-caught-up";
            return Err(format!("不认识的条件 {what:?}：现在有 {have}"));
        }
    };
    let mut base = None; // task-idle：开始等时的样子
    let deadline = clock::mono_ms().saturating_add(timeout_s.saturating_mul(1000)); // --timeout 写得再大也不回绕
    let mut seen: Vec<String> = Vec::new();
    loop {
        let (ok, now) = match cond {
            "leader" => leader(d),
            "chains-serving" => chains(d),
            "task-idle" => idle(d, &name, &mut base)?,
            "alert" => alert(d, &name, &what[2])?,
            "audit-caught-up" => audit(d, num)?,
            _ => evicted(d, num),
        };
        if ok {
            seen.push(now);
            return Ok(seen);
        }
        if cond == "chains-serving" && seen.last() != Some(&now) && seen.len() < 50 {
            seen.push(now.clone());
        }
        if clock::mono_ms() >= deadline {
            return Err(format!("等了 {timeout_s} s，{cond} 还不成立。当前：{now}（日志：wmfs dev logs <n>）"));
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn leader(d: &Path) -> (bool, String) {
    let mut state = Vec::new();
    for n in nodes(d) {
        match health(d, n) {
            Ok(h) if h.meta_role == "leader" => return (true, format!("node{n} 是就绪的元数据 leader")),
            Ok(h) => state.push(format!("node{n}：{}", h.meta_role)),
            Err(e) => state.push(format!("node{n}：没有响应（{e}）")),
        }
    }
    (false, if state.is_empty() { format!("{} 里没有开发节点", d.display()) } else { state.join("；") })
}

/// 用全部开发节点的管理地址 GET 一个依赖元数据的端点（跟随 leader）。
fn get<T: serde::de::DeserializeOwned>(d: &Path, path: &str) -> Result<T, String> {
    api(d)?.get(path).map_err(|e| e.to_string())
}

fn chains(d: &Path) -> (bool, String) {
    let r = match get::<ChainsReply>(d, "/api/v1/chains") {
        Ok(r) => r,
        Err(e) => return (false, e),
    };
    let bad: Vec<String> = r
        .chains
        .iter()
        .filter(|c| c.members.len() < c.want as usize || c.members.iter().any(|m| m.state != "serving"))
        .map(|c| {
            let m: Vec<String> =
                c.members.iter().map(|m| format!("t{} {}", m.target, crate::chain::title(&m.state))).collect();
            format!("chain {}：期望 {} 个成员，现在 [{}]", c.id, c.want, m.join(", "))
        })
        .collect();
    let n = r.chains.len();
    if bad.is_empty() {
        (true, format!("{n} 条链全部达到期望长度且全部 Serving"))
    } else {
        (false, bad.join("；"))
    }
}

fn evicted(d: &Path, sid: u64) -> (bool, String) {
    let id = format!("{sid:x}");
    match get::<SessionsReply>(d, "/api/v1/sessions") {
        Ok(r) => match r.sessions.iter().find(|s| s.id == id) {
            Some(s) => (s.state == "evicted", format!("会话 {id}：{}", s.state.to_uppercase())),
            None => (false, format!("会话 {id} 不在会话列表里（已经正常关闭了？）")),
        },
        Err(e) => (false, e),
    }
}

/// `task-idle <kind>`（见文件头）。`base` = 开始等时的 (leader 任务的轮数, 节点任务的运行请求号)。没有这个任务 → `Err`。
fn idle(d: &Path, kind: &str, base: &mut Option<(u64, u64)>) -> Result<(bool, String), String> {
    let named: Vec<(u32, String)> = nodes(d).iter().filter_map(|n| mgmt_addr(d, *n).ok().map(|a| (*n, a))).collect();
    let addrs: Vec<String> = named.iter().map(|(_, a)| a.clone()).collect();
    let key = admin_key(d, 1)?;
    let (r, on_nodes) = match crate::task::all_tasks(&ApiClient::new(addrs.clone(), key.clone()), &addrs, &key) {
        Ok(x) => x,
        Err(e) => return Ok((false, e.to_string())), // leader 正在换：接着等
    };
    let Some(top) = r.tasks.iter().find(|t| t.kind == kind) else {
        let have: Vec<&str> = r.tasks.iter().map(|t| t.kind.as_str()).collect();
        return Err(format!("没有后台任务 {kind}（有：{}）", have.join("、")));
    };
    let (p0, r0) = *base.get_or_insert((top.pass, top.runs));
    if top.on_leader() {
        if top.pass < p0 {
            *base = Some((top.pass, r0)); // 换过 leader，轮数从库里接着数（可能更小）：从这里重新算
        }
        let ok = top.pass >= p0 + 2 && rest(top);
        return Ok((ok, format!("{kind}：{}，第 {} 轮，积压 {}", top.state, top.pass, top.backlog)));
    }
    let label = |a: &str| named.iter().find(|(_, x)| x == a).map_or(a.to_string(), |(n, _)| format!("node{n}"));
    let on_nodes: Vec<(String, Result<NodeInfoReply, String>)> =
        on_nodes.into_iter().map(|(a, n)| (label(&a), n)).collect();
    Ok(nodes_idle(kind, r0, &on_nodes))
}

/// `alert <规则> firing|resolved`（见文件头）。
fn alert(d: &Path, rule: &str, want: &str) -> Result<(bool, String), String> {
    let r = match super::mgr::alerts(d) {
        Ok(r) => r,
        Err(e) => return Ok((false, format!("mgr 没有回答（{e}；mgr 在运行吗？wmfs dev mgr）"))),
    };
    let names: Vec<&str> = r.alerts.iter().map(|a| a.name.as_str()).collect();
    let a = r
        .alerts
        .iter()
        .find(|a| a.name == rule)
        .ok_or_else(|| format!("没有告警规则 {rule}（有：{}）", names.join("、")))?;
    let state = a.state.as_str();
    Ok((state == want, format!("告警 {rule}：{state}（{}）", a.message)))
}

/// `audit-caught-up`：audit-log 游标 ≥ 开始等时最新的 seq `want`（见文件头）。
fn audit(d: &Path, want: u64) -> Result<(bool, String), String> {
    match api(d)?.get::<CursorReply>("/api/v1/changelog/cursor?name=audit-log") {
        Ok(c) if c.seq >= want => Ok((true, format!("audit-log 游标 {}：已追上 {want}", c.seq))),
        Ok(c) => Ok((false, format!("audit-log 游标 {}，要追上 {want}（mgr 在运行吗？wmfs dev mgr）", c.seq))),
        Err(e) if e.status == 404 => Err("没有 audit-log 游标：没有目录开过 audit（wmfs set <目录> audit=on）".into()),
        Err(e) => Ok((false, e.to_string())),
    }
}

/// 这一轮做完了、积压 0。
fn rest(t: &TaskInfo) -> bool {
    t.state == STATE_DONE && t.backlog == 0
}

/// 节点任务 `kind` 在每个节点（`(名字, 回复)`）上都空闲了吗：(成立吗, 每个节点的样子)。见文件头：照办了运行请求
/// `r0`（只做数据的节点不看）、这一轮做完、积压 0；连不上的节点不算空闲。
fn nodes_idle(kind: &str, r0: u64, nodes: &[(String, Result<NodeInfoReply, String>)]) -> (bool, String) {
    let mut ok = !nodes.is_empty();
    let mut s = vec![];
    for (name, n) in nodes {
        let n = match n {
            Ok(n) => n,
            Err(e) => {
                ok = false;
                s.push(format!("{name} 连不上（{e}），在等它"));
                continue;
            }
        };
        for t in n.tasks.iter().filter(|t| t.kind == kind) {
            ok &= rest(t) && (t.runs >= r0 || !n.controlled); // 只做数据的节点收不到运行请求（见文件头）
            let runs = if n.controlled {
                format!("照办到第 {} 次运行请求（要 {r0}）", t.runs)
            } else {
                "只做数据：不受控制".into()
            };
            s.push(format!("{name} {}（积压 {}，{runs}）", t.state, t.backlog));
        }
    }
    (ok, format!("{kind}：{}", if s.is_empty() { "还没有节点报上来".into() } else { s.join("，") }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use wmfs_core::api::types::{RUNS_ON_EVERY_NODE, STATE_RUNNING};

    fn node(controlled: bool, state: &str, backlog: u64, runs: u64) -> Result<NodeInfoReply, String> {
        let t = TaskInfo {
            kind: "scrub".into(),
            runs_on: RUNS_ON_EVERY_NODE.into(),
            node: 1,
            state: state.into(),
            backlog,
            runs,
            ..Default::default()
        };
        Ok(NodeInfoReply { node_id: 1, controlled, tasks: vec![t], ..Default::default() })
    }

    /// 节点任务的 task-idle：每个节点都照办了运行请求（只做数据的节点不看）、做完、积压 0；连不上的节点不算空闲，写出在等它。
    #[test]
    fn node_tasks_are_idle_only_when_every_node_answers_and_is_done() {
        let at = |name: &str, n| (name.to_string(), n);
        let both = [at("node1", node(true, STATE_DONE, 0, 2)), at("node4", node(false, STATE_DONE, 0, 0))];
        assert!(nodes_idle("scrub", 2, &both).0, "只做数据的节点收不到运行请求：不看它的运行请求号");
        assert!(!nodes_idle("scrub", 2, &[at("node1", node(true, STATE_DONE, 0, 1))]).0, "还没照办运行请求");
        assert!(!nodes_idle("scrub", 2, &[at("node4", node(false, STATE_RUNNING, 0, 0))]).0, "这一轮还没做完");
        assert!(!nodes_idle("scrub", 2, &[at("node1", node(true, STATE_DONE, 3, 2))]).0, "还有积压");
        let down = at("node3", Err("connection refused".into()));
        let (ok, why) = nodes_idle("scrub", 2, &[at("node1", node(true, STATE_DONE, 0, 2)), down]);
        assert!(!ok && why.contains("node3 连不上"), "连不上的节点不当空闲：{why}");
        assert!(!nodes_idle("scrub", 0, &[]).0, "还没有节点报上来");
    }
}
