//! 告警规则的解析与求值（design.md 4.14）：TOML（格式见 `default_rules.toml` 开头的说明），表达式只支持"指标{标签} 比较符 阈值"
//! （另可写成 `sum(…)`、`rate(…)`、`grows(…)`）与"事件类型"两种。时长的写法与属性相同（`wmfs_api::units`：`500ms`、`60s`、`5m`、`1h`、`7d`）；
//! 事件型规则的操作名按 changelog 的操作码表（`wmfs_api::ops`）核对、换成规范写法，写错了 mgr 拒绝启动（实现注记（M14 审查））。

use serde::Deserialize;
use wmfs_api::ops::{op_code, op_name};
use wmfs_api::types::ChangeEvent;
use wmfs_api::units::parse_duration;

use crate::prom::Sample;

/// 指标型规则怎么把各节点的样本合成一个值。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Agg {
    Any,   // 任一样本满足比较即成立
    Sum,   // 全部样本之和
    Rate,  // 各序列每秒增量之和（计数器；只算相邻两轮都有的序列，变小了 = 节点重启过，按 0）
    Grows, // 全部样本之和，且每个 for 里都在增长才 firing（协调器积压，ADR-0045 第 10 条；状态机见 mod.rs 的文件头）
}

#[derive(Clone, Debug, PartialEq)]
pub struct Expr {
    pub agg: Agg,
    pub metric: String,
    pub labels: Vec<(String, String)>,
    pub op: String,
    pub threshold: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Cond {
    Metric(Expr),
    Event { ops: Vec<String>, kind: Option<String> },
}

#[derive(Clone, Debug, PartialEq)]
pub struct Rule {
    pub name: String,
    pub text: String, // 规则的原文（expr 或 event），`GET /alerts` 里显示
    pub cond: Cond,
    pub for_ms: u64,
    pub webhook: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRule {
    name: String,
    expr: Option<String>,
    event: Option<String>,
    kind: Option<String>,
    r#for: Option<String>,
    webhook: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawFile {
    #[serde(default)]
    rule: Vec<RawRule>,
}

/// 内置规则（`default_rules.toml`）。
pub fn defaults() -> Vec<Rule> {
    parse(include_str!("default_rules.toml")).unwrap_or_else(|e| panic!("不变量破坏：内置告警规则写错了：{e}"))
}

/// 内置规则加上规则文件里的（同名的替换内置的，别的追加）。
pub fn with_file(text: &str) -> Result<Vec<Rule>, String> {
    let mut all = defaults();
    for r in parse(text)? {
        all.retain(|x| x.name != r.name);
        all.push(r);
    }
    Ok(all)
}

pub fn parse(text: &str) -> Result<Vec<Rule>, String> {
    let f: RawFile = toml::from_str(text).map_err(|e| format!("告警规则解析失败：{e}"))?;
    f.rule.into_iter().map(rule).collect()
}

fn rule(r: RawRule) -> Result<Rule, String> {
    let what = |e: String| format!("告警规则 {}：{e}", r.name);
    let (cond, text, dflt) = match (&r.expr, &r.event) {
        (Some(x), None) => (Cond::Metric(expr(x).map_err(what)?), x.clone(), "0s"),
        (None, Some(e)) => {
            let canon = |op: &str| {
                let op = op.trim();
                op_code(op)
                    .and_then(op_name)
                    .map(str::to_string)
                    .ok_or(format!("不认识的操作名 {op:?}（changelog 的 op）"))
            };
            let ops = e.split('|').map(canon).collect::<Result<_, _>>().map_err(what)?;
            (Cond::Event { ops, kind: r.kind.clone() }, e.clone(), "5m")
        }
        _ => return Err(what("expr 与 event 要写且只写一个".into())),
    };
    if r.webhook.as_ref().is_some_and(|w| !w.starts_with("http://") && !w.starts_with("https://")) {
        return Err(what("webhook 应以 http:// 或 https:// 开头".into()));
    }
    let for_ms = parse_duration(r.r#for.as_deref().unwrap_or(dflt)).map_err(what)?.as_millis() as u64;
    Ok(Rule { name: r.name, text, cond, for_ms, webhook: r.webhook })
}

fn expr(s: &str) -> Result<Expr, String> {
    let (i, op) = [">=", "<=", "==", "!=", ">", "<"]
        .iter()
        .find_map(|op| s.find(op).map(|i| (i, *op)))
        .ok_or_else(|| format!("缺少比较符（> >= < <= == !=）：{s}"))?;
    let threshold: f64 = s[i + op.len()..].trim().parse().map_err(|_| format!("阈值不是数：{s}"))?;
    let (mut left, mut agg) = (s[..i].trim(), Agg::Any);
    for (f, a) in [("sum(", Agg::Sum), ("rate(", Agg::Rate), ("grows(", Agg::Grows)] {
        if let Some(inner) = left.strip_prefix(f).and_then(|x| x.strip_suffix(')')) {
            (left, agg) = (inner.trim(), a);
        }
    }
    let (metric, labels) = match left.split_once('{') {
        None => (left, vec![]),
        Some((m, rest)) => {
            let body = rest.strip_suffix('}').ok_or_else(|| format!("标签少了右花括号：{s}"))?;
            let pair = |p: &str| match p.split_once('=') {
                Some((k, v)) => Ok((k.trim().to_string(), v.trim().trim_matches('"').to_string())),
                None => Err(format!("标签应写成 k=\"v\"：{p}")),
            };
            (m.trim(), body.split(',').filter(|p| !p.trim().is_empty()).map(pair).collect::<Result<_, _>>()?)
        }
    };
    if metric.is_empty() || !metric.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b':') {
        return Err(format!("指标名不对：{metric:?}"));
    }
    Ok(Expr { agg, metric: metric.into(), labels, op: op.into(), threshold })
}

impl Expr {
    /// 名字与标签都对得上的样本。
    pub fn matching<'a>(&'a self, samples: &'a [Sample]) -> impl Iterator<Item = &'a Sample> + 'a {
        samples.iter().filter(|s| s.name == self.metric && self.labels.iter().all(|(k, v)| s.has(k, v)))
    }

    /// 名字与标签都对得上的样本的值。
    pub fn values<'a>(&'a self, samples: &'a [Sample]) -> impl Iterator<Item = f64> + 'a {
        self.matching(samples).map(|s| s.value)
    }

    pub fn holds(&self, v: f64) -> bool {
        let t = self.threshold;
        match self.op.as_str() {
            ">" => v > t,
            ">=" => v >= t,
            "<" => v < t,
            "<=" => v <= t,
            "==" => v == t,
            _ => v != t,
        }
    }
}

impl Cond {
    /// 事件型规则：这条 changelog 记录是它要的吗。
    pub fn matches(&self, e: &ChangeEvent) -> bool {
        match self {
            Cond::Event { ops, kind } => {
                ops.contains(&e.op) && kind.as_ref().is_none_or(|k| e.extra.iter().any(|(a, b)| a == "kind" && b == k))
            }
            Cond::Metric(_) => false,
        }
    }
}
