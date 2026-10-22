//! `wmfs dev wait <条件> [--timeout 120]`（design.md 7.2）：替代演示与验收里的固定 sleep——条件满足立即返回 0，
//! 超时返回 1 并打印当前状态。每 100 ms 问一遍开发目录里每个节点的 `/api/v1/health`。
//!
//! M3 只有 `leader`：有一个节点报告自己是**就绪**的元数据 leader（health 的 `meta_role = "leader"`：已当选、本任期的空条目
//! 已应用、插件清单一致，可以服务请求）。其余条件（`chains-serving`、`task-idle`、`session-evicted`、`alert`、
//! `audit-caught-up`）随功能在后面的里程碑加入。

use std::path::Path;
use std::time::Duration;

use wmfs_core::base::clock;

use super::{health, nodes};

pub fn wait(d: &Path, what: &[String], timeout_s: u64) -> Result<Vec<String>, String> {
    let cond = what.first().map(String::as_str).unwrap_or_default();
    if cond != "leader" || what.len() > 1 {
        return Err(format!("不认识的条件 {what:?}：M3 只有 leader（chains-serving 等随功能在后面的里程碑加入）"));
    }
    let deadline = clock::mono_ms().saturating_add(timeout_s.saturating_mul(1000)); // --timeout 写得再大也不回绕
    loop {
        let mut state = Vec::new();
        for n in nodes(d) {
            match health(d, n) {
                Ok(h) if h.meta_role == "leader" => return Ok(vec![format!("node{n} 是就绪的元数据 leader")]),
                Ok(h) => state.push(format!("node{n}：{}", h.meta_role)),
                Err(e) => state.push(format!("node{n}：没有响应（{e}）")),
            }
        }
        if clock::mono_ms() >= deadline {
            let now =
                if state.is_empty() { format!("{} 里没有开发节点", d.display()) } else { state.join("；") };
            return Err(format!(
                "等了 {timeout_s} s 还没有就绪的元数据 leader。当前：{now}（日志：wmfs dev logs <n>）"
            ));
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}
