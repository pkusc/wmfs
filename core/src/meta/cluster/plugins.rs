//! 集群插件清单（design.md 3.7.2，ADR-0025 第 6 条，D17）：会影响策略的 8 个钩子上启用了哪些插件（`钩子:名字`）记在 GLOBAL `plugins`。
//!
//! - **比对**（[`diff`]、[`why`]）：每个节点拿到集群图时（`data/node_hb.rs`）、以及 raft 线程启动时、应用了改 GLOBAL `plugins` 的批次
//!   或装了快照之后（发布 `mismatch`，cluster-mgr 在任何角色上看它）比对本地清单与集群清单；不一致 → [`exit_mismatch`]：写一条列出差异与
//!   修法的错误日志，1 s 后进程退出，退出码 3（leader 先把已排队的回复发出去）——等同拒绝启动。`wmfs join` 时不一致直接拒绝加入（`join.rs`）。
//! - **修改**（[`change`]）：`wmfs cluster plugins add|remove <钩子:名字>`（admin），写 changelog `PluginListChange`、`map_epoch += 1`；
//!   之后各节点因不一致退出，用新配置重启全部节点。
//! - **逃生口**（[`reset`]）：`wmfs server --adopt-local-plugins` 的节点成为就绪 leader 时不退出，而是提交一个批次把清单改成本地的
//!   （changelog `PluginListReset`，日志大声警告），其他节点随后按新清单比对。

use std::time::Duration;

use crate::api::types::PluginsReply;
use crate::base::{ErrCode, Failed, log};
use crate::hooks::manifest::Hook;
use crate::meta::changelog::{self, Change};
use crate::meta::engine::txn::{KvTxn, OpCtx};
use crate::meta::schema::keys::{self, G_PLUGINS};
use crate::meta::schema::kv;
use crate::meta::schema::records::PluginListV1;

/// 插件清单不一致时进程的退出码（3.7.2）。
pub const EXIT_MISMATCH: i32 = 3;

/// 修法（日志与报错里都给出）。
pub const FIX: &str = "修复：让全部节点启用同一组插件后重启（node.toml 的 [plugins] disable；改集群清单用 wmfs cluster plugins add|remove）；\
     某个插件再也装不上时，在一个元数据节点上用 wmfs server --data-dir D --adopt-local-plugins 启动（3.7.2 第 5 条）；\
     开发集群多半是旧里程碑建的，wmfs dev down --purge 之后重新 wmfs dev up";

/// 本地清单与集群清单的差异（相同 → None），例如「缺少 authz:deny-uid；多出 qos:fifo」。
pub fn diff(mine: &[String], cluster: &[String]) -> Option<String> {
    let missing: Vec<&str> = cluster.iter().filter(|p| !mine.contains(p)).map(String::as_str).collect();
    let extra: Vec<&str> = mine.iter().filter(|p| !cluster.contains(p)).map(String::as_str).collect();
    let mut parts = Vec::new();
    if !missing.is_empty() {
        parts.push(format!("本节点缺少 {}（集群清单里有）", missing.join("、")));
    }
    if !extra.is_empty() {
        parts.push(format!("本节点多出 {}（集群清单里没有）", extra.join("、")));
    }
    (!parts.is_empty()).then(|| parts.join("；"))
}

/// 本地清单 `mine` 与已应用的集群清单（`committed`；没有 = 已应用的状态里没有 GLOBAL plugins）为什么不一致（一致 → 空串）。
pub fn why(mine: &PluginListV1, committed: Option<&PluginListV1>) -> String {
    match committed {
        Some(c) => diff(&mine.plugins, &c.plugins).unwrap_or_default(),
        None => "已应用的状态里没有 GLOBAL plugins（集群插件清单）".into(),
    }
}

/// 不一致：写一条错误日志（差异与修法），1 s 后以退出码 3 结束进程（3.7.2 第 3 条）。
pub fn exit_mismatch(why: &str) -> ! {
    log::error(
        "plugins",
        format_args!("插件清单与集群插件清单不一致：{why}——进程退出（退出码 {EXIT_MISMATCH}）。{FIX}"),
    );
    std::thread::sleep(Duration::from_secs(1)); // 已排队的回复先发出去
    std::process::exit(EXIT_MISMATCH)
}

/// 一项 `钩子:名字`：钩子是进清单的 8 种之一，名字是小写字母、数字与 `-`。
fn check_item(s: &str) -> Result<(), Failed> {
    let ok = s.split_once(':').is_some_and(|(h, n)| {
        Hook::ALL.iter().any(|x| x.in_cluster_list() && x.name() == h)
            && !n.is_empty()
            && n.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    });
    let hooks: Vec<&str> = Hook::ALL.iter().filter(|h| h.in_cluster_list()).map(|h| h.name()).collect();
    ok.then_some(()).ok_or_else(|| {
        let msg = format!("{s:?} 应写成 钩子:名字（钩子是 {} 之一，例如 authz:posix-mode）", hooks.join("、"));
        Failed::new(ErrCode::EINVAL, msg)
    })
}

fn current(v: &dyn KvTxn) -> Result<PluginListV1, ErrCode> {
    kv::get::<PluginListV1>(v, &keys::global(G_PLUGINS))?.ok_or(ErrCode::INTERNAL)
}

fn save(v: &mut dyn KvTxn, c: &OpCtx, op: &'static str, old: &PluginListV1, new: PluginListV1) -> Result<(), ErrCode> {
    let extra = vec![("old".into(), old.plugins.join(",")), ("new".into(), new.plugins.join(","))];
    changelog::append(v, c, Change { op, extra, ..Default::default() })?;
    kv::put(v, &keys::global(G_PLUGINS), &new);
    kv::bump_map_epoch(v)
}

/// `wmfs cluster plugins add|remove`（执行器闭包，见文件头）：加已经在的、删不在的 → `EINVAL`（不静默成功）。
pub fn change(v: &mut dyn KvTxn, c: &OpCtx, add: &[String], remove: &[String]) -> Result<PluginsReply, Failed> {
    let old = current(v)?;
    let mut list = old.plugins.clone();
    for a in add {
        check_item(a)?;
        if list.contains(a) {
            return Err(Failed::new(ErrCode::EINVAL, format!("集群插件清单里已经有 {a}")));
        }
        list.push(a.clone());
    }
    for r in remove {
        let before = list.len();
        list.retain(|x| x != r);
        if list.len() == before {
            return Err(Failed::new(ErrCode::EINVAL, format!("集群插件清单里没有 {r}")));
        }
    }
    if add.is_empty() && remove.is_empty() {
        return Err(Failed::new(ErrCode::EINVAL, "没有要加或要删的插件"));
    }
    list.sort();
    save(v, c, "PluginListChange", &old, PluginListV1 { plugins: list.clone() })?;
    Ok(PluginsReply { plugins: list })
}

/// 逃生口（见文件头）：清单还不是 `local` 才改。
pub fn reset(v: &mut dyn KvTxn, c: &OpCtx, local: &PluginListV1) -> Result<(), ErrCode> {
    let old = current(v)?;
    if old != *local {
        save(v, c, "PluginListReset", &old, local.clone())?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diff_lists_missing_and_extra_plugins() {
        let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();
        assert_eq!(diff(&s(&["authz:posix-mode"]), &s(&["authz:posix-mode"])), None);
        let d = diff(&s(&["authz:posix-mode", "qos:fifo"]), &s(&["authz:posix-mode", "authz:deny-uid"])).unwrap();
        assert!(d.contains("缺少 authz:deny-uid") && d.contains("多出 qos:fifo"), "{d}");
        assert!(check_item("authz:deny-uid").is_ok());
        for bad in ["authz", "transport:tcp", "authz:", "authz:Deny", "meta_engine:embedded-kv"] {
            assert!(check_item(bad).is_err(), "{bad}");
        }
    }
}
