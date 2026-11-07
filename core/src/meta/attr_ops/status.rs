//! `wmfs status`（design.md 3.10 的"声明 vs 实际"）：每个键的声明、来源、实际、是否一致。从 `attr_ops.rs` 拆出来（每个文件 ≤ 400 行）。
//! 实现注记（M12）：`trash` 的实际 = 它的回收站里的文件数与字节数（`.wmfs-trash` 的 RSTAT，含各用户的子目录）；`audit` 的实际 = `audit-log` 游标落后最新
//! changelog 的条数（落后就算不一致：审计消费者没在运行或跟不上）；`ttl` 的实际（已经过期、还没被 ttl 任务删掉的文件数）由 `bg/status.rs`
//! 的逐文件统计填。实现注记（M13）：`qos` 的实际 = 下发到每个节点的份额与隔离承诺（`actual.new_files`）、各节点心跳报的保底欠额之和
//! （`actual.deficit`，目录设了 qos 才有；大于 0 算不一致）；`tier` 的实际（条件已成立、还没迁过去的文件数）由 `bg/status.rs` 填。
//! 实现注记（M13 收尾，ADR-0044 第 7 条）：这个目录的存储类 label 是 hdd、盘却是 nvme（按设备名）→ warnings 里如实标注。

use super::{
    SUBTREE_MAX, dir_attrs_of, dirattrs_under, domains, effective_text, qos_actual, source, unmet, usable_chains,
};
use crate::api::types::{AttrActual, StatusAttr, StatusReply};
use crate::base::units::fmt_size;
use crate::base::{Failed, clock};
use crate::meta::attr_registry::REGISTRY;
use crate::meta::changelog::{self, AUDIT_CURSOR};
use crate::meta::cluster::classes::{HDD_ON_NVME, hdd_on_nvme};
use crate::meta::engine::txn::{KvScan, OpCtx};
use crate::meta::ns::common::{Node, child};
use crate::meta::ns::layout::inline_of;
use crate::meta::ns::read_ops::{node_at, path_of};
use crate::meta::ns::trash::TRASH_DIR;
use crate::meta::{quota, rstat};

/// 回复与逐文件统计要扫的根（`path` 自己；逐文件统计由 `bg::status` 在这个读之外做、再填进来，3.10 的 M11 注记）。
pub fn status(v: &dyn KvScan, c: &OpCtx, path: &str) -> Result<(StatusReply, Node), Failed> {
    let n = node_at(v, c, path)?;
    let (dir, e, own) = dir_attrs_of(v, c, &n)?;
    let mut warnings = c.map.warnings.clone();
    // 子树里显式写着、当前满足不了的 replicas/stripe 声明；这个目录自己的在后面按全部生效值查一遍，这里跳过
    let (dirs, truncated) = dirattrs_under(v, c, dir, &["replicas", "stripe"])?;
    for (d, m) in dirs.into_iter().filter(|(d, _)| *d != dir) {
        let x = c.attrs.resolve(v, d)?;
        let (r, s) = (m.entries.contains_key("replicas"), m.entries.contains_key("stripe"));
        let who = format!("目录 {} ", path_of(v, d)?);
        warnings.extend(unmet(c.map, x.class_id, x.replicas, x.stripe, r, s, &who));
    }
    if truncated {
        warnings.push(format!(
            "设置了副本数或条带数的目录超过 {SUBTREE_MAX} 个，只检查了前 {SUBTREE_MAX} 个；其余的用 wmfs status <子目录> 查看"
        ));
    }
    warnings.extend(unmet(c.map, e.class_id, e.replicas, e.stripe, true, true, "这个目录："));
    if let Some(k) = c.map.classes.iter().find(|k| k.id == e.class_id && hdd_on_nvme(c.map, k)) {
        warnings.push(format!("存储类 {}：{HDD_ON_NVME}，性能数字没有意义", k.name)); // 6.3 正文（ADR-0044 第 7 条）
    }
    let quotas = quota::quotas_of(v, c, dir)?;
    let mut attrs = Vec::new();
    for s in REGISTRY.iter() {
        let declared = effective_text(c, &e, &own, s.key);
        let new_files = match s.key {
            "replicas" => domains(c.map, e.class_id).min(e.replicas as usize).to_string(),
            "stripe" => usable_chains(c.map, e.class_id, e.replicas).min(e.stripe as usize).to_string(),
            "inline" => fmt_size(inline_of(&e) as u64),
            "qos" => qos_actual(c, &e), // M13：下发到每个节点的份额与隔离承诺
            _ => declared.clone(),      // 每次 open/create 由 core 按生效属性执行，新文件就是声明值
        };
        let source = source(v, &e, &own, dir, s.key)?;
        let effect = s.effect.name().to_string();
        // inline 的差别是加密决定的，qos 的"实际"是份额：都不按字面比
        let mut mismatched = new_files != declared && s.key != "inline" && s.key != "qos";
        let mut actual = AttrActual { new_files, ..Default::default() };
        match s.key {
            "trash" if e.trash_ms > 0 => {
                let r = match child(v, e.trash_root, TRASH_DIR)? {
                    Some(t) => rstat::get(v, t.ino)?,
                    None => Default::default(),
                };
                (actual.files, actual.bytes) = (Some(r.files), Some(r.bytes));
            }
            "audit" => {
                actual.lag = changelog::cursor_lag(v, AUDIT_CURSOR)?;
                mismatched = e.audit.on && actual.lag.is_some_and(|n| n > 0);
            }
            "qos" if e.qos_id != 0 => {
                // 保底欠额（成本单位/秒，各节点心跳报的之和，leader 内存里）：大于 0 算不一致（3.10）
                actual.deficit = Some(c.usage.deficit(e.qos_id, clock::mono_ms()));
                mismatched = actual.deficit > Some(0);
            }
            _ => {}
        }
        if s.key == "quota" || s.key == "quota.*" {
            let mine: Vec<_> = quotas.iter().filter(|q| q.class.is_some() == (s.key == "quota.*")).cloned().collect();
            mismatched = mine.iter().any(quota::over);
            actual.quotas = (!mine.is_empty()).then_some(mine);
        }
        attrs.push(StatusAttr { key: s.key.into(), declared, source, effect, actual, mismatched });
    }
    Ok((StatusReply { path: path.into(), warnings, attrs, ..Default::default() }, n))
}
