//! `qos`、`tier`、`locality` 三个键的解析、规范化与显示（design.md 第 5 章、4.5、3.10；实现注记（M13））。从 `attr_registry.rs`
//! 拆出来（每个文件 ≤ 400 行）。
//!
//! - `qos`：存储类名（借用它的 QoS 包络）或 `r=<n>,w=<n>,l=<n>[,bw=<字节/秒>][,burst=<时长>]`（r、l 的单位是成本单位/秒，4.5；没写的
//!   r、l、bw、burst 为 0 = 不设，w 缺省 1；w ≥ 1；l ≠ 0 时 r ≤ l）。DIRATTR 里存类名原样（活的借用，ADR-0044 第 2 条），或
//!   `r=<n>,w=<n>,l=<n>,bw=<字节>,burst=<时长>`（五项都写，例如 `burst=30s`、`0s`）；设置时同批写 QOS 表（`18 dir`，经集群图下发给数据节点：
//!   借用的放那个类此刻的包络，`wmfs class update` 改了 [qos] 时由 `mgmt/classes.rs` 重写）。
//! - `tier`：`<from>-><to>:<条件>`（也接受 `→`）；条件 = `idle>时长`（max(atime, mtime) 早于现在减时长）、`age>时长`（btime）、
//!   `size>尺寸`，用 `&` 连接（都成立才算）。第 5 章写的是 `Nd`，这里接受任何时长单位（演示与验收用 `idle>2s` 这样的短条件）。
//!   存原样（`→` 换成 `->`）；两端的存储类存在由 `attr_ops` 在设置时查。
//! - `locality`：`prefer-client` / `any`。

use std::time::Duration;

use crate::base::clock::ts_ms;
use crate::base::types::Ts;
use crate::base::units::{fmt_duration, fmt_size, parse_duration, parse_size};
use crate::hooks::policy::FileFacts;
use crate::meta::schema::records::QosSpecV1;

/// `locality` 的取值（`EffectiveAttrs.locality`、`StorageClassV1.locality`）：0 any，1 prefer-client。
pub const LOCALITY_ANY: u8 = 0;
pub const LOCALITY_PREFER: u8 = 1;

/// `qos` 的子键（跟在 `qos=r=…` 后面）。
pub const QOS_SUBKEYS: &[&str] = &["r", "w", "l", "bw", "burst"];

/// 存储类名的写法 `[a-z0-9-]{1,32}`（第 5 章）。
pub fn class_name_ok(v: &str) -> bool {
    (1..=32).contains(&v.len()) && v.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

fn no_subkeys(subs: &[(String, String)]) -> Result<(), String> {
    subs.first().map_or(Ok(()), |(k, _)| Err(format!("不认识的子键 {k}")))
}

/// `qos=…`（见文件头）：存储类名原样（活的借用），否则规范化成五项都写的形式。
pub fn parse_qos(v: &str, subs: &[(String, String)]) -> Result<String, String> {
    let Some(first) = v.split_once('=') else {
        no_subkeys(subs)?;
        return if class_name_ok(v) { Ok(v.into()) } else { Err("应是存储类名或 r=…,w=…,l=…".into()) };
    };
    let mut kvs = vec![first];
    kvs.extend(subs.iter().map(|(a, b)| (a.as_str(), b.as_str())));
    let q = qos_fields(&kvs)?;
    if q.w == 0 || (q.l != 0 && q.r > q.l) {
        return Err("要求 w ≥ 1，且 l ≠ 0 时 r ≤ l".into());
    }
    Ok(qos_norm(&q))
}

/// 规范化的 `qos` 值是一个存储类名（借用它的包络）→ 这个名字；是 `r=…` 的数值 → None。
pub fn qos_class(norm: &str) -> Option<&str> {
    (!norm.contains('=')).then_some(norm)
}

/// `k=v` 的几项 → QoS 数值（用户写的与 DIRATTR 里存的是同一种写法：bw 是尺寸、burst 是时长）。
fn qos_fields(kvs: &[(&str, &str)]) -> Result<QosSpecV1, String> {
    let (mut q, mut seen) = (QosSpecV1 { r: 0, w: 1, l: 0, bw_max: 0, burst_ms: 0 }, vec![]);
    for &(k, x) in kvs {
        if seen.contains(&k) {
            return Err(format!("{k} 写了两次"));
        }
        seen.push(k);
        let num = |x: &str| x.parse::<u32>().map_err(|_| format!("{k} 应是成本单位/秒的整数"));
        match k {
            "r" => q.r = num(x)?,
            "w" => q.w = num(x)?,
            "l" => q.l = num(x)?,
            "bw" => q.bw_max = parse_size(x)?,
            "burst" => q.burst_ms = u32::try_from(parse_duration(x)?.as_millis()).map_err(|_| "burst 太长")?,
            _ => return Err(format!("不认识的子键 {k}（可用 r、w、l、bw、burst）")),
        }
    }
    Ok(q)
}

/// QoS 数值 → DIRATTR 里存的形式（见文件头）。
pub fn qos_norm(q: &QosSpecV1) -> String {
    format!(
        "r={},w={},l={},bw={},burst={}",
        q.r,
        q.w,
        q.l,
        q.bw_max,
        fmt_duration(Duration::from_millis(q.burst_ms as u64))
    )
}

/// DIRATTR 里存的形式 → QoS 数值（不是这个形式，例如存的是类名 → None）。
pub fn qos_spec(norm: &str) -> Option<QosSpecV1> {
    let kvs: Option<Vec<(&str, &str)>> = norm.split(',').map(|kv| kv.split_once('=')).collect();
    qos_fields(&kvs?).ok()
}

/// QoS 数值的显示形式：`r=0,w=1,l=3000`，bw、burst 不为 0 时接上 `,bw=4M,burst=30s`。
pub fn qos_text(q: &QosSpecV1) -> String {
    let mut s = format!("r={},w={},l={}", q.r, q.w, q.l);
    if q.bw_max > 0 {
        s += &format!(",bw={}", fmt_size(q.bw_max));
    }
    if q.burst_ms > 0 {
        s += &format!(",burst={}", fmt_duration(Duration::from_millis(q.burst_ms as u64)));
    }
    s
}

pub fn show_qos(norm: &str) -> String {
    qos_spec(norm).map_or(norm.to_string(), |q| qos_text(&q))
}

/// `<from>-><to>:<条件>`（也接受 `→`）→ (from, to, 条件)。
pub fn split_tier(v: &str) -> Option<(&str, &str, &str)> {
    let (from, rest) = v.split_once("->").or_else(|| v.split_once('→'))?;
    let (to, cond) = rest.split_once(':')?;
    Some((from.trim(), to.trim(), cond.trim()))
}

pub fn parse_tier(v: &str, subs: &[(String, String)]) -> Result<String, String> {
    no_subkeys(subs)?;
    let (from, to, cond) = split_tier(v).ok_or("应写成 <from>-><to>:<条件>，例如 gold->silver:idle>30d")?;
    if !class_name_ok(from) || !class_name_ok(to) || from == to {
        return Err("两端应是两个不同的存储类名".into());
    }
    parse_cond(cond)?;
    Ok(format!("{from}->{to}:{cond}"))
}

/// 条件的一项（见文件头）：时长按毫秒、尺寸按字节。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Term {
    Idle(u64),
    Age(u64),
    Size(u64),
}

/// `idle>30d&size>1M` → 各项（任何一项写错 → 给人看的原因）。
pub fn parse_cond(s: &str) -> Result<Vec<Term>, String> {
    let ms = |x: &str| parse_duration(x).map(|d| d.as_millis() as u64);
    s.split('&')
        .map(|t| match t.trim().split_once('>').map(|(k, x)| (k.trim(), x.trim())) {
            Some(("idle", x)) => ms(x).map(Term::Idle),
            Some(("age", x)) => ms(x).map(Term::Age),
            Some(("size", x)) => parse_size(x).map(Term::Size),
            _ => Err(format!("不认识的条件 {t:?}（idle>时长、age>时长、size>尺寸，用 & 连接）")),
        })
        .collect()
}

/// 条件对这个文件此刻成立吗（3.7.7 的 TierCondition；条件解不开 = 不成立）。
pub fn holds(cond: &str, f: &FileFacts, now_ms: u64) -> bool {
    let older = |t: Ts, ms: u64| ts_ms(t).saturating_add(ms) < now_ms;
    parse_cond(cond).is_ok_and(|ts| {
        ts.iter().all(|t| match *t {
            Term::Idle(ms) => older(f.atime.max(f.mtime), ms),
            Term::Age(ms) => older(f.btime, ms),
            Term::Size(n) => f.size > n,
        })
    })
}

pub fn parse_locality(v: &str, subs: &[(String, String)]) -> Result<String, String> {
    no_subkeys(subs)?;
    match v {
        "prefer-client" | "any" => Ok(v.into()),
        _ => Err("应是 prefer-client 或 any".into()),
    }
}

/// 规范化的 locality → 取值（[`LOCALITY_ANY`] / [`LOCALITY_PREFER`]）。
pub fn locality_id(norm: &str) -> u8 {
    if norm == "prefer-client" { LOCALITY_PREFER } else { LOCALITY_ANY }
}

pub fn locality_name(id: u8) -> &'static str {
    if id == LOCALITY_PREFER { "prefer-client" } else { "any" }
}
