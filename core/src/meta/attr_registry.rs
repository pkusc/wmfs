//! 属性注册表（design.md 3.8、第 5 章，ADR-0026）：每个可设置的目录属性一条 [`AttrSpec`]——种类、子键、解析与规范化、显示、
//! 生效方式、消费者。`wmfs set` 的属性字符串在**服务端**按 [`split`] 切分（CLI 只把原样的字符串发过来，所以 wmfs-mgr 不需要
//! 知道注册表）：按逗号切成片段；`k=v` 且 k 是顶层属性名 → 开始一个新属性；k 是当前属性的子键 → 归到当前属性；其他 → "未知属性 k"。
//!
//! M7 的键：class、replicas、chunk、stripe、inline、checksum、consistency（`cto` 与子键 `attr_ttl`；`strong` 在 M12 之前回
//! `EOPNOTSUPP`）、inherit。后面里程碑才实现的键（[`LATER`]）与二期功能（ec、snapshot）设置时回 `EOPNOTSUPP`；`wasm:` 开头的值回
//! `FEATURE_UNSUPPORTED`（"二期功能"，3.7.7）——都不静默接受。
//!
//! 规范化（存进 DIRATTR 的值，3.2.7）：尺寸存字节数（`1M` → `1048576`）、名字原样；显示时换回来（[`AttrSpec::show`]）。实现注记
//! （M7）：`consistency` 连同子键存成**一个**值——`cto` 或 `cto,attr_ttl=<毫秒>`（子键只能跟在主键后面，所以随主键一起继承；没写
//! attr_ttl = 默认 1 s）。消费者（3.8"必须有插件消费"）：`consumers` 里有 "core" 的键总能设置，否则要有已启用插件的 manifest 在
//! `attrs` 里声明它（[`crate::hooks::pipeline::Pipeline::consumes`]）；启动时核对每个插件 manifest 里的属性名都是认识的（[`check_manifest_attrs`]）。
//!
//! **加一个键的清单**（按字符串匹配，漏改一处编译不会报错）：① 这里的 [`REGISTRY`]（与它的 parse、show）；② `attrs.rs` 的
//! `EffectiveAttrs` 字段与 `set_typed_field`；③ `attr_ops.rs` 的 `display`（生效值的显示形式）；④ 有交叉检查时 `attr_ops::check`；
//! ⑤ "实际"可能与声明不同时 `attr_ops::status` 与 `unmet`（警告）。

use crate::base::units::{fmt_duration, fmt_size, parse_duration, parse_size};
use crate::base::{ErrCode, Failed};
use crate::data::format::{MAX_CHUNK_SIZE, MIN_CHUNK_SIZE};

/// Inherit：取最近的祖先值；Stacked：所有祖先同时生效（配额，M10）；Local：只作用于设置它的目录（inherit）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AttrKind {
    Inherit,
    Stacked,
    Local,
}

/// 生效方式（3.8、第 5 章的"生效方式"一列）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Effect {
    NewFiles,
    NewDataAndReconcile,
    Immediate,
    NewOpens,
    Reconciler,
}

impl Effect {
    pub fn name(self) -> &'static str {
        match self {
            Effect::NewFiles => "新文件",
            Effect::NewDataAndReconcile => "新文件 + 协调器",
            Effect::Immediate => "立即",
            Effect::NewOpens => "新打开的文件",
            Effect::Reconciler => "协调器",
        }
    }
}

/// 解析 + 规范化：(值, 子键) → 存进 DIRATTR 的形式；错误是给人看的原因。
pub type ParseFn = fn(&str, &[(String, String)]) -> Result<String, String>;
/// 切分时攒着的一个属性：(注册表项, 值, 子键)。
type Item = (&'static AttrSpec, String, Vec<(String, String)>);

/// 一个可设置的属性（Debug 只为测试里打印）。`parse(值, 子键)` → 规范化的存储形式（错误是给人看的中文原因，调用方映射成 `EINVAL`，0.2 第 5 条）。
#[derive(Debug)]
pub struct AttrSpec {
    pub key: &'static str,
    pub kind: AttrKind,
    pub subkeys: &'static [&'static str],
    pub takes_rest: bool, // 值里本身有逗号（acl.default，M10）
    pub parse: ParseFn,
    pub show: fn(&str) -> String, // 存储形式 → 显示形式
    pub effect: Effect,
    pub consumers: &'static [&'static str], // "core" = 本体自己；其余是插件名（报错时列出来）
}

const fn spec(
    key: &'static str,
    parse: ParseFn,
    show: fn(&str) -> String,
    effect: Effect,
    consumers: &'static [&'static str],
) -> AttrSpec {
    AttrSpec { key, kind: AttrKind::Inherit, subkeys: &[], takes_rest: false, parse, show, effect, consumers }
}

/// 全部 M7 的键（顺序就是 `wmfs get` 的显示顺序）。
pub static REGISTRY: [AttrSpec; 8] = [
    spec(
        "class",
        parse_class,
        show_as_is,
        Effect::NewDataAndReconcile,
        &["label-match", "mclock", "reconciler", "tiering"],
    ),
    spec("replicas", parse_replicas, show_as_is, Effect::NewDataAndReconcile, &["replica", "label-match"]),
    spec("chunk", parse_chunk, show_size, Effect::NewFiles, &["core"]),
    spec("stripe", parse_stripe, show_as_is, Effect::NewFiles, &["label-match"]),
    spec("inline", parse_inline, show_size, Effect::NewFiles, &["core"]),
    spec("checksum", parse_checksum, show_as_is, Effect::NewFiles, &["crc32c", "xxh3", "blake3"]),
    AttrSpec {
        subkeys: &["attr_ttl"],
        ..spec("consistency", parse_consistency, show_consistency, Effect::NewOpens, &["core"])
    },
    AttrSpec { kind: AttrKind::Local, ..spec("inherit", parse_inherit, show_as_is, Effect::Immediate, &["core"]) },
];

/// 后面的里程碑才实现的键：设置 → `EOPNOTSUPP`（`quota.<类>` 按前缀 `quota.` 匹配）。
pub const LATER: &[(&str, &str)] = &[
    ("quota", "M10"),
    ("acl.default", "M10"),
    ("compress", "M11"),
    ("encrypt", "M11"),
    ("trash", "M12"),
    ("ttl", "M12"),
    ("audit", "M12"),
    ("qos", "M13"),
    ("tier", "M13"),
    ("locality", "M13"),
];
/// 二期功能的键（第 5 章）：设置 → `EOPNOTSUPP`"二期功能"。
const PHASE2: &[&str] = &["ec", "snapshot"];

pub fn find(key: &str) -> Option<&'static AttrSpec> {
    REGISTRY.iter().find(|s| s.key == key)
}

/// 还不能设置的已知键（后面的里程碑、二期功能）→ `EOPNOTSUPP` 与原因（写明在哪个里程碑）；别的键 → None。
fn later(key: &str) -> Option<Failed> {
    let k = if key.starts_with("quota.") { "quota" } else { key };
    let why = match LATER.iter().find(|(n, _)| *n == k) {
        Some((_, m)) => format!("在 {m} 实现，现在还不能设置"),
        None if PHASE2.contains(&k) => "是二期功能".to_string(),
        None => return None,
    };
    Some(Failed::new(ErrCode::EOPNOTSUPP, format!("属性 {key} {why}")))
}

/// 一个单独给出的键（`wmfs unset`）：注册表里的 → 它的 [`AttrSpec`]；还不能设置的 → `EOPNOTSUPP`（与 set 的说法相同）；其他 →
/// `EINVAL`"未知属性"。
pub fn spec_of(key: &str) -> Result<&'static AttrSpec, Failed> {
    match find(key) {
        Some(s) => Ok(s),
        None => Err(later(key).unwrap_or_else(|| einval(format!("未知属性 {key}")))),
    }
}

/// 切分、解析好的一个属性：存进 DIRATTR 的规范化值。
#[derive(Debug)]
pub struct Parsed {
    pub spec: &'static AttrSpec,
    pub norm: String,
}

fn einval(m: impl Into<String>) -> Failed {
    Failed::new(ErrCode::EINVAL, m)
}

/// 切分并解析 `wmfs set` 的属性字符串（见文件头）。同一个键写两次、片段为空 → `EINVAL`。
pub fn split(text: &str) -> Result<Vec<Parsed>, Failed> {
    if text.trim().is_empty() {
        return Err(einval("属性字符串是空的（例如 chunk=1M,stripe=2）"));
    }
    let mut items: Vec<Item> = Vec::new();
    for frag in text.split(',').map(str::trim) {
        if frag.is_empty() {
            return Err(einval(format!("属性字符串里有空的片段：{text:?}")));
        }
        let (k, v) = frag.split_once('=').map_or((frag, None), |(k, v)| (k.trim(), Some(v.trim())));
        if let Some(e) = later(k) {
            return Err(e);
        }
        // 顶层属性名：开始一个新属性
        if let Some(s) = find(k) {
            let Some(v) = v else { return Err(einval(format!("属性 {k} 没有给值（写成 {k}=…）"))) };
            if items.iter().any(|(t, ..)| t.key == s.key) {
                return Err(einval(format!("属性 {k} 写了两次（{k}={v}）")));
            }
            items.push((s, v.to_string(), vec![]));
            continue;
        }
        // 剩下的只可能是：当前属性的子键（consistency 后面的 attr_ttl），或者值里本身带逗号的属性的续接（takes_rest，
        // M10 的 acl.default 才用）；都不是 → 未知属性
        match (items.last_mut(), v) {
            (Some((cur, _, subs)), Some(v)) if cur.subkeys.contains(&k) => subs.push((k.into(), v.into())),
            (Some((cur, val, _)), None) if cur.takes_rest => {
                val.push(',');
                val.push_str(frag);
            }
            _ => return Err(einval(format!("未知属性 {k}"))),
        }
    }
    items.into_iter().map(|(s, v, subs)| parse_one(s, &v, &subs)).collect()
}

fn parse_one(s: &'static AttrSpec, v: &str, subs: &[(String, String)]) -> Result<Parsed, Failed> {
    if v.starts_with("wasm:") {
        return Err(Failed::new(ErrCode::FEATURE_UNSUPPORTED, format!("{}={v}：wasm: 策略是二期功能", s.key)));
    }
    if s.key == "consistency" && v == "strong" {
        return Err(Failed::new(
            ErrCode::EOPNOTSUPP,
            "consistency=strong 在 M12 实现，现在只能设 cto（可带 attr_ttl）",
        ));
    }
    let shown = || {
        let rest: Vec<String> = subs.iter().map(|(k, x)| format!(",{k}={x}")).collect();
        format!("{}={v}{}", s.key, rest.concat())
    };
    let norm = (s.parse)(v, subs).map_err(|m| einval(format!("{}：{m}", shown())))?;
    Ok(Parsed { spec: s, norm })
}

/// 启动时的对账（3.8）：插件 manifest 的 `attrs` 里每个名字都得是认识的键（M7 的、后面里程碑的），否则多半是写错了。
pub fn check_manifest_attrs(plugin: &str, attrs: &[&str]) -> Result<(), String> {
    match attrs.iter().find(|a| find(a).is_none() && later(a).is_none()) {
        Some(a) => Err(format!("插件 {plugin} 的 manifest 声明消费属性 {a}，属性注册表里没有这个键")),
        None => Ok(()),
    }
}

// ---- 各键的解析（值 → 规范化形式）与显示 ----

fn no_subkeys(subs: &[(String, String)]) -> Result<(), String> {
    match subs.first() {
        Some((k, _)) => Err(format!("不认识的子键 {k}")),
        None => Ok(()),
    }
}

fn int_in(v: &str, lo: u64, hi: u64) -> Result<u64, String> {
    let n: u64 = v.parse().map_err(|_| format!("应是 {lo}–{hi} 的整数"))?;
    if (lo..=hi).contains(&n) { Ok(n) } else { Err(format!("应在 {lo}–{hi} 之间")) }
}

fn parse_class(v: &str, subs: &[(String, String)]) -> Result<String, String> {
    no_subkeys(subs)?;
    let ok =
        (1..=32).contains(&v.len()) && v.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
    if ok { Ok(v.to_string()) } else { Err("存储类名应是 1–32 个小写字母、数字或 -".into()) }
}

fn parse_replicas(v: &str, subs: &[(String, String)]) -> Result<String, String> {
    no_subkeys(subs)?;
    int_in(v, 1, 8).map(|n| n.to_string())
}

fn parse_stripe(v: &str, subs: &[(String, String)]) -> Result<String, String> {
    no_subkeys(subs)?;
    int_in(v, 1, 64).map(|n| n.to_string())
}

fn parse_chunk(v: &str, subs: &[(String, String)]) -> Result<String, String> {
    no_subkeys(subs)?;
    let n = parse_size(v)?;
    if !n.is_power_of_two() || !(MIN_CHUNK_SIZE as u64..=MAX_CHUNK_SIZE as u64).contains(&n) {
        return Err("chunk 应是 64K–16M 之间的 2 的幂".into());
    }
    Ok(n.to_string())
}

fn parse_inline(v: &str, subs: &[(String, String)]) -> Result<String, String> {
    no_subkeys(subs)?;
    let n = parse_size(v)?;
    if n > 64 << 10 { Err("inline 应在 0–64K 之间（0 = 关闭）".into()) } else { Ok(n.to_string()) }
}

fn parse_checksum(v: &str, subs: &[(String, String)]) -> Result<String, String> {
    no_subkeys(subs)?;
    match v {
        "crc32c" | "xxh3" | "blake3" => Ok(v.to_string()),
        _ => Err("校验算法应是 crc32c、xxh3 或 blake3".into()),
    }
}

/// `cto[,attr_ttl=<0–10s>]` → `cto` 或 `cto,attr_ttl=<毫秒>`（strong 在 `parse_one` 里先挡掉了）。
fn parse_consistency(v: &str, subs: &[(String, String)]) -> Result<String, String> {
    if v != "cto" {
        return Err("应是 cto 或 strong".into());
    }
    match subs {
        [] => Ok("cto".into()),
        [(_, t)] => {
            let d = parse_duration(t)?;
            if d.as_millis() > 10_000 || d.subsec_micros() % 1000 != 0 {
                return Err("attr_ttl 应在 0–10s 之间（精确到毫秒）".into());
            }
            Ok(format!("cto,attr_ttl={}", d.as_millis()))
        }
        _ => Err("attr_ttl 写了两次".into()),
    }
}

fn parse_inherit(v: &str, subs: &[(String, String)]) -> Result<String, String> {
    no_subkeys(subs)?;
    if v == "false" { Ok("false".into()) } else { Err("只能是 false（不写就是继承）".into()) }
}

fn show_as_is(s: &str) -> String {
    s.to_string()
}

fn show_size(s: &str) -> String {
    s.parse().map(fmt_size).unwrap_or_else(|_| s.to_string())
}

/// 规范化的 attr_ttl（毫秒）；没写 → None。
pub fn attr_ttl_ms(norm: &str) -> Option<u64> {
    norm.strip_prefix("cto,attr_ttl=")?.parse().ok()
}

fn show_consistency(s: &str) -> String {
    match attr_ttl_ms(s) {
        Some(ms) => format!("cto,attr_ttl={}", fmt_duration(std::time::Duration::from_millis(ms))),
        None => s.to_string(),
    }
}

#[cfg(test)]
mod tests;
