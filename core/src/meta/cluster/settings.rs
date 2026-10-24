//! 集群运行参数（design.md 3.2.8 的 `ClusterSettingsV1`）：`wmfs cluster set <名>=<值>`（admin，经 `POST /api/v1/cluster/settings`）。
//! 它们是协议时序参数（持有方自停与授予方回收必须用同一个租约时长），所以存在复制状态 GLOBAL `settings` 里、经集群图下发。
//!
//! 名字 = 字段名去掉 `_ms`（`session_lease`、`target_lease`、`cap_ttl`、`repair_delay`、`rejoin_window`、`epoch_gc_grace`），值写时长
//! （`30s`、`0s`）；`bg_bytes_per_sec` 写字节数（`200M`）。改了的批次 `map_epoch += 1`（参数在集群图里：下一个批次、各节点的下一次
//! 拉图就用新值）并为每一项写一条 changelog `ClusterSet`（extra = name/old/new，显示形式）。实现注记（M7，规格没写取值范围，取最小
//! 的防呆）：时长精确到毫秒、不超过 30 天；租约至少要盖住 3 次续租——会话租约至少 10 s（客户端会话心跳每 3 s 一次，8.7）、target
//! 租约至少 3 s（数据节点的 target 心跳每 1 s 一次）：更短的租约永远续不上，M9/M10 按它回收时会不停地驱逐客户端、摘掉 target；
//! cap 有效期至少 1 s（为 0 时谁也活不过一次续期）；后台限速 > 0。

use std::collections::BTreeMap;
use std::time::Duration;

use crate::api::types::ClusterSettingsReply;
use crate::base::ErrCode;
use crate::base::units::{fmt_duration, fmt_size, parse_duration, parse_size};
use crate::meta::changelog::{self, Change};
use crate::meta::engine::txn::{KvTxn, OpCtx};
use crate::meta::schema::keys::{self, G_SETTINGS, G_SETTINGS_PREV};
use crate::meta::schema::kv;
use crate::meta::schema::records::ClusterSettingsV1;

/// 全部参数名（输出顺序）。
const NAMES: [&str; 7] =
    ["session_lease", "target_lease", "cap_ttl", "repair_delay", "rejoin_window", "epoch_gc_grace", "bg_bytes_per_sec"];
const MAX_MS: u64 = 30 * 86_400_000;

fn field<'a>(s: &'a mut ClusterSettingsV1, name: &str) -> Option<&'a mut u64> {
    Some(match name {
        "session_lease" => &mut s.session_lease_ms,
        "target_lease" => &mut s.target_lease_ms,
        "cap_ttl" => &mut s.cap_ttl_ms,
        "repair_delay" => &mut s.repair_delay_ms,
        "rejoin_window" => &mut s.rejoin_window_ms,
        "epoch_gc_grace" => &mut s.epoch_gc_grace_ms,
        "bg_bytes_per_sec" => &mut s.bg_bytes_per_sec,
        _ => return None,
    })
}

/// 时长参数的下限（毫秒，见文件头）。
fn floor_ms(name: &str) -> u64 {
    match name {
        "session_lease" => 10_000,
        "target_lease" => 3_000,
        "cap_ttl" => 1_000,
        _ => 0,
    }
}

fn fmt(name: &str, v: u64) -> String {
    if name == "bg_bytes_per_sec" { fmt_size(v) } else { fmt_duration(Duration::from_millis(v)) }
}

/// 全部参数的显示形式。
pub fn show(s: &ClusterSettingsV1) -> BTreeMap<String, String> {
    let mut s = s.clone();
    NAMES.iter().map(|n| (n.to_string(), fmt(n, field(&mut s, n).map_or(0, |x| *x)))).collect()
}

/// 解析 `{名字: 值}`（纯函数，见文件头的取值范围）；错误是给人看的原因（调用方回 `EINVAL`）。
pub fn parse(items: &BTreeMap<String, String>) -> Result<Vec<(String, u64)>, String> {
    if items.is_empty() {
        return Err(format!("没有给参数（例如 epoch_gc_grace=0s；可改的有 {}）", NAMES.join("、")));
    }
    let mut out = Vec::new();
    for (name, text) in items {
        if !NAMES.contains(&name.as_str()) {
            return Err(format!("不认识的集群参数 {name}（可改的有 {}）", NAMES.join("、")));
        }
        let v = if name == "bg_bytes_per_sec" {
            parse_size(text).ok().filter(|n| *n > 0).ok_or(format!("{name}={text}：应是大于 0 的字节数，例如 200M"))?
        } else {
            let d = parse_duration(text).map_err(|e| format!("{name}={text}：{e}"))?;
            let ms = d.as_millis() as u64;
            let floor = floor_ms(name);
            if d.subsec_micros() % 1000 != 0 || ms > MAX_MS || ms < floor {
                let why = if name.ends_with("_lease") { "；租约至少要盖住 3 次续租" } else { "" };
                return Err(format!("{name}={text}：应在 {}–30d 之间（精确到毫秒{why}）", fmt(name, floor)));
            }
            ms
        };
        out.push((name.clone(), v));
    }
    Ok(out)
}

/// 每一项取两者中较大的（ADR-0038 第 5 条：授予方判定过期用较长的租约；settings_prev 已经有时也这样合并）。
pub fn max_each(a: &ClusterSettingsV1, b: &ClusterSettingsV1) -> ClusterSettingsV1 {
    ClusterSettingsV1 {
        session_lease_ms: a.session_lease_ms.max(b.session_lease_ms),
        target_lease_ms: a.target_lease_ms.max(b.target_lease_ms),
        cap_ttl_ms: a.cap_ttl_ms.max(b.cap_ttl_ms),
        repair_delay_ms: a.repair_delay_ms.max(b.repair_delay_ms),
        rejoin_window_ms: a.rejoin_window_ms.max(b.rejoin_window_ms),
        epoch_gc_grace_ms: a.epoch_gc_grace_ms.max(b.epoch_gc_grace_ms),
        bg_bytes_per_sec: a.bg_bytes_per_sec.max(b.bg_bytes_per_sec),
    }
}

/// 执行器闭包：改参数、`map_epoch += 1`、每项一条 changelog `ClusterSet`。实现注记（M9，ADR-0038 第 5 条）：调小了某个租约时，同一个
/// 批次在 GLOBAL `settings_prev` 里记下修改之前的值（已经有就每一项取较大的）；授予方按 max(当前值, settings_prev) 判定过期，链管理器
/// 在一个旧租约周期之后删掉它（`chain_mgr.rs`）。
pub fn apply(v: &mut dyn KvTxn, c: &OpCtx, items: &[(String, u64)]) -> Result<ClusterSettingsReply, ErrCode> {
    let mut s: ClusterSettingsV1 = kv::get(v, &keys::global(G_SETTINGS))?.ok_or(ErrCode::INTERNAL)?;
    let before = s.clone();
    for (name, val) in items {
        let f = field(&mut s, name).ok_or(ErrCode::EINVAL)?;
        let old = std::mem::replace(f, *val);
        let extra =
            vec![("name".into(), name.clone()), ("old".into(), fmt(name, old)), ("new".into(), fmt(name, *val))];
        changelog::append(v, c, Change { op: "ClusterSet", extra, ..Default::default() })?;
    }
    kv::put(v, &keys::global(G_SETTINGS), &s);
    if s.target_lease_ms < before.target_lease_ms || s.session_lease_ms < before.session_lease_ms {
        let prev: Option<ClusterSettingsV1> = kv::get(v, &keys::global(G_SETTINGS_PREV))?;
        kv::put(v, &keys::global(G_SETTINGS_PREV), &prev.map_or(before.clone(), |p| max_each(&p, &before)));
    }
    kv::bump_map_epoch(v)?;
    Ok(ClusterSettingsReply { settings: show(&s) })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_values_and_ranges() {
        let m = |kv: &[(&str, &str)]| kv.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        assert_eq!(parse(&m(&[("epoch_gc_grace", "0s")])).unwrap(), vec![("epoch_gc_grace".into(), 0)]);
        assert_eq!(parse(&m(&[("bg_bytes_per_sec", "100M")])).unwrap()[0].1, 100 << 20);
        assert_eq!(parse(&m(&[("session_lease", "45s")])).unwrap()[0].1, 45_000);
        assert_eq!(parse(&m(&[("session_lease", "10s"), ("target_lease", "3s")])).unwrap().len(), 2, "正好到下限可以");
        let bad = [
            ("session_lease", "0s"),
            ("session_lease", "9s"), // 心跳每 3 s 一次：盖不住 3 次续租
            ("target_lease", "2s"),  // 心跳每 1 s 一次
            ("cap_ttl", "500ms"),
            ("epoch_gc_grace", "31d"),
            ("bg_bytes_per_sec", "0"),
        ];
        for b in bad {
            assert!(parse(&m(&[b])).is_err(), "{b:?}");
        }
        assert!(parse(&m(&[("epoch_gc_grace_ms", "0")])).unwrap_err().contains("不认识"), "名字去掉 _ms");
        assert!(parse(&BTreeMap::new()).is_err());
        let s = show(&ClusterSettingsV1::default());
        assert_eq!(
            (s["session_lease"].as_str(), s["epoch_gc_grace"].as_str(), s["bg_bytes_per_sec"].as_str()),
            ("30s", "1m", "200M")
        );
        assert_eq!(s.len(), 7);
    }
}
