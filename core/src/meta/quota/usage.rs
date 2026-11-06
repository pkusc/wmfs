//! 配额的显示（design.md 3.9、3.10、3.1.5）：`wmfs usage` 的配额部分、`wmfs status` 里 `quota`/`quota.<类>` 的"实际"、`wmfs get` 的
//! semantics 行。实现注记（M11）：从 quota.rs 挪出来（每个文件 ≤ 400 行），`wmfs status` 与 `wmfs usage` 用同一个 [`quotas_of`]。

use crate::api::types::{ClassUsage, QuotaUsage, UsageReply};
use crate::base::units::fmt_size;
use crate::base::{ErrCode, Failed};
use crate::meta::attrs::QuotaDecl;
use crate::meta::engine::txn::{KvRead, OpCtx};
use crate::meta::ns::read_ops::{node_at, path_of};
use crate::meta::rstat;

use super::{scopes, used_of};

/// `GET /api/v1/usage`（3.9，M7）：目录的递归用量（RSTAT，常量时间）；实现注记（M10）：另给它与它上面的每个配额（`UsageReply.quotas`，
/// 只增：用量、上限、软限的计时与宽限剩余）。
pub fn usage(v: &dyn KvRead, c: &OpCtx, path: &str) -> Result<UsageReply, Failed> {
    let n = node_at(v, c, path)?;
    if !n.is_dir() {
        return Err(Failed::new(ErrCode::ENOTDIR, format!("递归用量只对目录：{path} 不是目录")));
    }
    let r = rstat::get(v, n.ino)?;
    let per_class =
        r.per_class.iter().map(|(k, b, f)| ClassUsage { class: c.map.class_name(*k), bytes: *b, files: *f });
    let per_class = per_class.collect();
    let quotas = quotas_of(v, c, n.ino)?;
    Ok(UsageReply { path: path.into(), bytes: r.bytes, files: r.files, dirs: r.dirs, per_class, quotas })
}

/// 目录 `dir` 与它上面的每个配额：用量、上限、软限的计时起点与宽限还剩多久。
pub fn quotas_of(v: &dyn KvRead, c: &OpCtx, dir: u64) -> Result<Vec<QuotaUsage>, ErrCode> {
    let mut quotas = Vec::new();
    for s in scopes(v, c, dir, None)? {
        let (used_bytes, used_files) = used_of(s.class, &s.used);
        let left = (s.soft_since_ms > 0).then(|| (s.soft_since_ms + s.grace_ms).saturating_sub(c.req.now_ms));
        quotas.push(QuotaUsage {
            dir: path_of(v, s.dir)?,
            class: s.class.map(|k| c.map.class_name(k)),
            hard: s.hard_bytes,
            soft: s.soft_bytes,
            grace_ms: s.grace_ms,
            files: s.hard_files,
            quantum: s.quantum,
            used_bytes,
            used_files,
            soft_since_ms: s.soft_since_ms,
            grace_left_ms: left,
        });
    }
    Ok(quotas)
}

/// 这个配额此刻"实际与声明不一致"吗（3.10 的表）：超了软限（含宽限已过的）、或超了硬限（字节数或文件数；护栏范围内会短暂超出）。
pub fn over(q: &QuotaUsage) -> bool {
    (q.soft > 0 && q.used_bytes > q.soft) || q.used_bytes > q.hard || (q.files > 0 && q.used_files > q.files)
}

/// `wmfs get` 的 semantics 行里配额的那一段（3.1.5）：最近的那个配额的 quantum 与超限上界、来源；没有配额 → `quota=none`。
pub fn semantics(v: &dyn KvRead, quotas: &[(u64, QuotaDecl)]) -> Result<String, ErrCode> {
    let Some((d, q)) = quotas.first() else { return Ok("quota=none".into()) };
    let qs = fmt_size(q.quantum);
    Ok(format!("quota=quantum:{qs} overshoot<={qs}*writers (来源 {})", path_of(v, *d)?))
}
