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
//! 其余条件（`task-idle`、`alert`、`audit-caught-up`）随功能在后面的里程碑加入。

use std::path::Path;
use std::time::Duration;

use wmfs_core::api::types::{ChainsReply, SessionsReply};
use wmfs_core::base::clock;
use wmfs_mgr::api::ApiClient;

use super::{admin_key, health, mgmt_addr, nodes};

pub fn wait(d: &Path, what: &[String], timeout_s: u64) -> Result<Vec<String>, String> {
    let cond = what.first().map(String::as_str).unwrap_or_default();
    let sid = match (cond, what.get(1)) {
        ("leader" | "chains-serving", None) => 0,
        ("session-evicted", Some(mp)) if what.len() == 2 => super::mount::session_of(d, Path::new(mp))?,
        _ => {
            let have = "leader、chains-serving、session-evicted <挂载点>（task-idle 等随功能在后面的里程碑加入）";
            return Err(format!("不认识的条件 {what:?}：现在有 {have}"));
        }
    };
    let deadline = clock::mono_ms().saturating_add(timeout_s.saturating_mul(1000)); // --timeout 写得再大也不回绕
    let mut seen: Vec<String> = Vec::new();
    loop {
        let (ok, now) = match cond {
            "leader" => leader(d),
            "chains-serving" => chains(d),
            _ => evicted(d, sid),
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

fn chains(d: &Path) -> (bool, String) {
    let addrs: Vec<String> = nodes(d).iter().filter_map(|n| mgmt_addr(d, *n).ok()).collect();
    let key = match admin_key(d, 1) {
        Ok(k) => k,
        Err(e) => return (false, e),
    };
    let r = match ApiClient::new(addrs, key).get::<ChainsReply>("/api/v1/chains") {
        Ok(r) => r,
        Err(e) => return (false, e.to_string()),
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
    let addrs: Vec<String> = nodes(d).iter().filter_map(|n| mgmt_addr(d, *n).ok()).collect();
    let key = match admin_key(d, 1) {
        Ok(k) => k,
        Err(e) => return (false, e),
    };
    let id = format!("{sid:x}");
    match ApiClient::new(addrs, key).get::<SessionsReply>("/api/v1/sessions") {
        Ok(r) => match r.sessions.iter().find(|s| s.id == id) {
            Some(s) => (s.state == "evicted", format!("会话 {id}：{}", s.state.to_uppercase())),
            None => (false, format!("会话 {id} 不在会话列表里（已经正常关闭了？）")),
        },
        Err(e) => (false, e.to_string()),
    }
}
