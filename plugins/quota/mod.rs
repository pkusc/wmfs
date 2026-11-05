//! quota：配额插件（design.md 4.4、3.9，quota_admission，altitude 310）——软限 / 硬限 / 宽限期、文件数、按存储类。
//!
//! 纯函数：状态（每个 scope 的用量 RSTAT、软限的起点）由本体给出，软限的起点与事件也由本体写回（`meta/quota.rs`）；没有任何部署
//! 参数（宽限期与 quantum 的缺省值在本体的 `core/src/meta/quota.rs`：`DEFAULT_GRACE_MS` = 7 天、`DEFAULT_QUANTUM` = 64 MiB，属性
//! 注册表的 quota 项用它解析）。对本体给出的每个 scope（一个目录上的 `quota` 或 `quota.<类>`）：
//! 1. 要新建对象（`delta_files > 0`）且这个 scope 管的文件数（总量配额 = 文件 + 子目录，按类的 = 这个类的文件）加上它超过 `files=`
//!    → `Deny(EDQUOT)`；
//! 2. 有效上限 = 宽限期已过（软限的计时从 `soft_since_ms` 开始）? 软限 : 硬限；这次要增长（`delta_bytes > 0`）且用量加上它超过有效
//!    上限 → `Deny(EDQUOT)`；
//! 3. 这次增长越过了软限、而软限还没在计时（`soft_since_ms == 0`）→ `SoftWarn`（本体记下起点、写 `PolicyViolation` 事件）；
//! 4. `max_growth` = 各 scope 的 有效上限 − 用量 − 这次的增长 里最小的（不够 → 0）：本体据此给出写入额度 `quota_end`。
//!
//! 指标 `wmfs_quota_decisions_total{result}`、`wmfs_quota_denied_total{dir}`（只给设了配额的目录，基数受控）；fail point
//! `plugin.quota.quota_admission`（本体包在调用外层，触发 = 以注入的码拒绝）。

use std::sync::Arc;

use wmfs_core::api::metrics::{self, Counter};
use wmfs_core::base::ErrCode;
use wmfs_core::hooks::manifest::{CORE_ABI_VERSION, Hook, Manifest};
use wmfs_core::hooks::traits::{QuotaDecision, QuotaInput, QuotaPlugin};
use wmfs_core::meta::quota::used_of;

pub static MANIFEST: Manifest = Manifest {
    name: "quota",
    version: "0.1.0",
    hooks: &[Hook::QuotaAdmission],
    altitude: 310,
    hot_path: true,
    config_schema: "",
    abi_version: CORE_ABI_VERSION,
    attrs: &["quota", "quota.*"],
    budget_ns: 5_000, // 3.7.4：quota 5 µs
    fail_points: &["plugin.quota.quota_admission"],
};

pub struct Quota {
    allow: Arc<Counter>,
    warn: Arc<Counter>,
    deny: Arc<Counter>,
}

impl Quota {
    pub fn new() -> Quota {
        let mk = |r| metrics::counter("wmfs_quota_decisions_total", "quota 插件的判定", &[("result", r)]);
        Quota { allow: mk("allow"), warn: mk("soft_warn"), deny: mk("deny") }
    }
}

impl Default for Quota {
    fn default() -> Quota {
        Quota::new()
    }
}

impl QuotaPlugin for Quota {
    fn manifest(&self) -> &'static Manifest {
        &MANIFEST
    }

    fn admit(&self, input: &QuotaInput) -> QuotaDecision {
        let d = decide(input);
        match d {
            QuotaDecision::Allow { .. } => self.allow.inc(),
            QuotaDecision::SoftWarn { .. } => self.warn.inc(),
            QuotaDecision::Deny { dir, .. } => {
                self.deny.inc();
                let dir = dir.to_string();
                metrics::counter("wmfs_quota_denied_total", "配额拒绝的次数（按设了配额的目录）", &[("dir", &dir)])
                    .inc();
            }
        }
        d
    }
}

fn decide(i: &QuotaInput) -> QuotaDecision {
    let (mut max, mut warn) = (u64::MAX, None);
    let grow = i.delta_bytes.max(0) as u64;
    for s in i.scopes {
        let (used, files) = used_of(s.class, &s.used);
        let deny = QuotaDecision::Deny { dir: s.dir, code: ErrCode::EDQUOT };
        if i.delta_files > 0 && s.hard_files > 0 && files.saturating_add(i.delta_files as u64) > s.hard_files {
            return deny;
        }
        let grace_over =
            s.soft_bytes > 0 && s.soft_since_ms > 0 && i.now_ms >= s.soft_since_ms.saturating_add(s.grace_ms);
        let limit = if grace_over { s.soft_bytes } else { s.hard_bytes };
        let after = used.saturating_add(grow);
        if grow > 0 && after > limit {
            return deny;
        }
        if grow > 0 && s.soft_bytes > 0 && after > s.soft_bytes && s.soft_since_ms == 0 {
            warn = warn.or(Some(s.dir));
        }
        max = max.min(limit.saturating_sub(after));
    }
    match warn {
        Some(dir) => QuotaDecision::SoftWarn { dir, max_growth: max },
        None => QuotaDecision::Allow { max_growth: max },
    }
}

#[cfg(test)]
mod tests;
