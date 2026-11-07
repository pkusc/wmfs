//! 属性值的显示（design.md 3.8、3.10；从 `attr_ops.rs` 拆出来，每个文件 ≤ 400 行，M13）：存储形式 → 显示形式（[`shown`]）、
//! 生效值的显示（[`effective_text`]），以及 qos 的"实际"与隔离承诺（[`qos_actual`]、[`isolation_text`]，实现注记（M13））。
//! qos 插件执不执行份额看有没有插件消费 `qos` 属性（与 3.8"没有插件消费就拒绝"同一个判据：mclock 消费，fifo 不消费）。

use std::time::Duration;

use crate::base::units::{fmt_duration, fmt_size};
use crate::data::format::{CMP_ZSTD, checksum_name, compress_name};
use crate::meta::attr_registry::find;
use crate::meta::attr_registry::qos_tier::{locality_name, qos_text};
use crate::meta::attr_registry::trash_ttl_audit::{audit_text, fmt_ms};
use crate::meta::attrs::{CONSISTENCY_STRONG, EffectiveAttrs};
use crate::meta::cluster::classes::{FIFO_PROMISE, isolation, per_node};
use crate::meta::engine::txn::OpCtx;
use crate::meta::ns::acl;
use crate::meta::quota;
use crate::meta::schema::records::{AttrMapV1, QosSpecV1};

/// 一个键的存储形式 → 显示形式：`encrypt` 存的是数据密钥的 id，按集群图显示成 `tenant:<名字>`；其余按注册表的 `show`。
pub(super) fn shown(c: &OpCtx, key: &str, norm: &str) -> String {
    match (key, norm.parse::<u32>()) {
        ("encrypt", Ok(id)) => format!("tenant:{}", datakey_name(c, id)),
        _ => find(key).map_or(norm.to_string(), |s| (s.show)(norm)),
    }
}

fn datakey_name(c: &OpCtx, id: u32) -> String {
    c.map.datakeys.iter().find(|(i, _)| *i == id).map_or(format!("#{id}"), |(_, k)| k.name.clone())
}

/// 一个键的生效值（显示形式）。加了键要在这里加一行（`attr_registry.rs` 文件头的清单）。
pub(super) fn effective_text(c: &OpCtx, e: &EffectiveAttrs, own: &AttrMapV1, key: &str) -> String {
    match key {
        "class" => c.map.class_name(e.class_id),
        "replicas" => e.replicas.to_string(),
        "chunk" => fmt_size(e.chunk as u64),
        "stripe" => e.stripe.to_string(),
        "inline" => fmt_size(e.inline_max as u64),
        "checksum" => checksum_name(e.checksum).into(),
        "compress" if e.compress == CMP_ZSTD => format!("zstd:{}", e.compress_level),
        "compress" => compress_name(e.compress).into(),
        "encrypt" if e.encrypt_key == 0 => "none".into(),
        "encrypt" => format!("tenant:{}", datakey_name(c, e.encrypt_key)),
        "consistency" if e.consistency == CONSISTENCY_STRONG => "strong".into(),
        "consistency" => format!("cto,attr_ttl={}", ttl(e)),
        "inherit" => own.entries.get(key).cloned().unwrap_or_else(|| "true".into()), // 只看目录自己的
        // Stacked：最近的那个（来源一栏是它的目录）；没有 → none
        "quota" | "quota.*" => {
            let q = e.quotas.iter().find(|(_, q)| q.class.is_some() == (key == "quota.*"));
            q.map_or("none".into(), |(_, q)| quota::decl_text(q, q.class.map(|k| c.map.class_name(k)).as_deref()))
        }
        "acl.default" => {
            e.acl_default.as_deref().and_then(|b| acl::parse(b).ok()).map_or("none".into(), |a| acl::to_text(&a))
        }
        "trash" => fmt_ms(e.trash_ms), // M12：0 显示成 off
        "ttl" => fmt_ms(e.ttl_ms),
        "audit" => audit_text(&e.audit),
        // M13：借用的类名（ADR-0044 第 2 条），或 QOS 表里的数值；没设 = none（用存储类的 QoS 包络）
        "qos" => e.qos_class.clone().unwrap_or_else(|| qos_of(c, e).map_or("none".into(), |q| qos_text(&q))),
        "tier" => e
            .tier
            .as_ref()
            .map_or("none".into(), |t| format!("{}->{}:{}", c.map.class_name(t.from), c.map.class_name(t.to), t.cond)),
        "locality" => locality_name(e.locality).into(),
        // 这里还没处理的键（不该有）：目录自己存的值（显示形式），没有就空着——不冒充成某个生效值
        _ => own.entries.get(key).map(|x| find(key).map_or(x.clone(), |s| (s.show)(x))).unwrap_or_default(),
    }
}

pub(super) fn ttl(e: &EffectiveAttrs) -> String {
    fmt_duration(Duration::from_millis(e.attr_ttl_ms as u64))
}

/// 目录 `qos` 属性的数值（QOS 表，随集群图下发；没设 → None）。
fn qos_of(c: &OpCtx, e: &EffectiveAttrs) -> Option<QosSpecV1> {
    c.map.qos.iter().find(|(d, _)| e.qos_id != 0 && *d == e.qos_id).map(|(_, q)| q.clone())
}

/// qos 的"实际"（3.10 的表）：下发到每个节点的份额——数据节点执行的同一份（`classes::per_node`：目录的 `qos` 或存储类的 QoS 包络，
/// 按这个类现在有 Up target 的节点数均分，小数保留一位）——与隔离承诺（6.3）；qos 插件不执行份额（fifo）时如实说。
pub(super) fn qos_actual(c: &OpCtx, e: &EffectiveAttrs) -> String {
    let Some(k) = c.map.classes.iter().find(|k| k.id == e.class_id) else { return "-".into() };
    if !c.pipeline.consumes("qos") {
        return format!("不执行份额（qos 插件是 fifo）；{FIFO_PROMISE}");
    }
    let p = per_node(c.map, k, e.qos_id);
    let num = |x: f64| if x.fract() == 0.0 { format!("{x:.0}") } else { format!("{x:.1}") };
    let mut s = format!("每节点 r={},w={},l={}", num(p.r), p.w, num(p.l));
    if p.bw_max > 0.0 {
        s += &format!(",bw={}", fmt_size(p.bw_max as u64));
    }
    if p.burst_ms > 0 {
        s += &format!(",burst={}", fmt_duration(Duration::from_millis(p.burst_ms as u64)));
    }
    format!("{s}（{} 个节点均分）；isolation={}", p.nodes, isolation(c.map, k).name())
}

/// `wmfs get` 的 semantics 行里的 isolation（3.1.5、6.3）：qos 插件不执行份额（fifo，minimal）→ 先进先出、什么也不承诺；mclock → 这个类
/// 由实际部署算出的隔离承诺。semantics 行的各段用半角标点分隔，所以承诺里的全角逗号换成半角的。
pub(super) fn isolation_text(c: &OpCtx, class_id: u16) -> String {
    match c.map.classes.iter().find(|k| k.id == class_id) {
        Some(k) if c.pipeline.consumes("qos") => {
            let i = isolation(c.map, k);
            format!("{}({})", i.name(), i.promise().replace('，', ","))
        }
        _ => format!("shared-device({})", FIFO_PROMISE.replace('，', ",")),
    }
}
