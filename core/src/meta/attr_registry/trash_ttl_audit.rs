//! `trash`、`ttl`、`audit` 三个键的解析、规范化与显示（design.md 第 5 章；实现注记（M12））。从 `attr_registry.rs` 拆出来（每个文件 ≤ 400 行）。
//! （与 `hooks::policy` 的策略求值器无关：那边按这里解析出的 `AuditSpec` 判定哪些 changelog 记录要审计。）
//!
//! - `trash`：`off` / `<时长>`（0 < 时长 ≤ 365d）→ 存 `off` 或毫秒数；`ttl`：`off` / `<时长>`（> 0，按 mtime）→ 同上。
//! - `audit`：`off` / `on`，可带子键 `ops=create+unlink`（ADR-0029 的操作名，不区分大小写）、`uids=1000+1001`；也接受把过滤写成值
//!   （`audit=ops=create+unlink,uids=1000` = `audit=on,ops=…`）。存 `off`、`on` 或 `on,ops=Create+Unlink,uids=1000+1001`（操作名用表里的
//!   写法）。没写 `ops=` = 命名空间修改、setattr、xattr、写入长度与属性变更（`hooks::policy` 的 AuditFilter）；没写 `uids=` = 所有人。

use std::time::Duration;

use crate::api::ops::{op_code, op_name};
use crate::base::units::{fmt_duration, parse_duration};
use crate::meta::attrs::AuditSpec;

/// `trash` 的保留期最长 365d（第 5 章）。
const TRASH_MAX_MS: u64 = 365 * 86_400_000;
/// `ttl` 的上限只为了 mtime + ttl 不溢出。
const TTL_MAX_MS: u64 = u64::MAX / 2;

/// 规范化的 `off` / 毫秒数 → 毫秒（`off` = 0）；解不开 → None。
pub fn ms_or_off(norm: &str) -> Option<u64> {
    if norm == "off" { Some(0) } else { norm.parse().ok() }
}

/// `off` 或 (0, `max_ms`] 的时长 → 规范化的 `off` / 毫秒数；不在范围里 → `bad`。
fn duration_ms(v: &str, max_ms: u64, bad: &str) -> Result<String, String> {
    if v == "off" {
        return Ok("off".into());
    }
    let ms = parse_duration(v)?.as_millis();
    if ms == 0 || ms > max_ms as u128 {
        return Err(bad.into());
    }
    Ok(ms.to_string())
}

fn no_subkeys(subs: &[(String, String)]) -> Result<(), String> {
    match subs.first() {
        Some((k, _)) => Err(format!("不认识的子键 {k}")),
        None => Ok(()),
    }
}

pub fn parse_trash(v: &str, subs: &[(String, String)]) -> Result<String, String> {
    no_subkeys(subs)?;
    duration_ms(v, TRASH_MAX_MS, "应是 off 或 1ms–365d 的时长")
}

pub fn parse_ttl(v: &str, subs: &[(String, String)]) -> Result<String, String> {
    no_subkeys(subs)?;
    duration_ms(v, TTL_MAX_MS, "应是 off 或大于 0 的时长")
}

/// 毫秒数 → `off`（0：生效值里 0 = 没设）或 `7d` 这样的时长。
pub fn fmt_ms(ms: u64) -> String {
    if ms == 0 { "off".into() } else { fmt_duration(Duration::from_millis(ms)) }
}

/// 存储形式（`off` / 毫秒数）→ 显示形式（[`fmt_ms`]）；解不开就原样显示。
pub fn show_ms(norm: &str) -> String {
    ms_or_off(norm).map_or(norm.to_string(), fmt_ms)
}

pub fn parse_audit(v: &str, subs: &[(String, String)]) -> Result<String, String> {
    let mut subs = subs.to_vec();
    let v = match v.split_once('=') {
        Some((k, x)) if k == "ops" || k == "uids" => {
            subs.insert(0, (k.into(), x.into())); // 过滤写成了值：audit=ops=…
            "on"
        }
        _ => v,
    };
    let mut a = match v {
        "on" => AuditSpec { on: true, ..AuditSpec::default() },
        "off" if subs.is_empty() => return Ok("off".into()),
        _ => return Err("应是 off、on，或 on 加过滤 ops=create+unlink,uids=1000+1001".into()),
    };
    for (k, x) in &subs {
        let parts = x.split('+').map(str::trim);
        match k.as_str() {
            "ops" if a.ops.is_empty() => {
                a.ops = parts.map(|n| op_code(n).ok_or(format!("不认识的操作名 {n}"))).collect::<Result<_, _>>()?
            }
            "uids" if a.uids.is_empty() => {
                a.uids = parts.map(|n| n.parse().map_err(|_| format!("uid 应是数字：{n}"))).collect::<Result<_, _>>()?
            }
            _ => return Err(format!("子键 {k} 写了两次")),
        }
    }
    Ok(audit_text(&a))
}

/// 一份审计规则的规范写法（存进 DIRATTR 的形式，也是显示形式）。
pub fn audit_text(a: &AuditSpec) -> String {
    if !a.on {
        return "off".into();
    }
    let mut s = String::from("on");
    if !a.ops.is_empty() {
        let names: Vec<&str> = a.ops.iter().map(|c| op_name(*c).unwrap_or("?")).collect();
        s += &format!(",ops={}", names.join("+"));
    }
    if !a.uids.is_empty() {
        let uids: Vec<String> = a.uids.iter().map(u32::to_string).collect();
        s += &format!(",uids={}", uids.join("+"));
    }
    s
}

/// 规范写法 → 规则（DIRATTR 里存的；解不开 → None）。
pub fn audit_spec(norm: &str) -> Option<AuditSpec> {
    if norm == "off" {
        return Some(AuditSpec::default());
    }
    let mut it = norm.split(',');
    if it.next() != Some("on") {
        return None;
    }
    let mut a = AuditSpec { on: true, ..AuditSpec::default() };
    for part in it {
        match part.split_once('=')? {
            ("ops", x) => a.ops = x.split('+').map(op_code).collect::<Option<_>>()?,
            ("uids", x) => a.uids = x.split('+').map(|n| n.parse().ok()).collect::<Option<_>>()?,
            _ => return None,
        }
    }
    Some(a)
}
