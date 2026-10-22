//! label-match 放置插件（design.md 4.6、3.4.2.1，altitude 510）：create、截断到 0 与迁移时给候选链过滤、排序。
//!
//! 实现注记（M4）：基础版——候选 = `class_id` 与 `replicas` 都匹配、至少一个 Serving 成员、不在 `exclude` 里的链；排序键 =
//! Serving 成员数降序 → 使用率（`used / capacity`，capacity 为 0 = 不限，按 0 算）升序 → `hash(seed, chain_id)`（seed = 新
//! inode 号：同一个 seed 永远得到同一个顺序，不同的文件被确定性地分散到各条链）。core 取排序后的前 k = min(stripe,
//! 互不相同的链数) 条（k = 0 → `ENOSPC`）。`locality = prefer-client`（head 在客户端所在节点的链优先）与 dedicated 的放置
//! 规则在 M13（dedicated 的 target 独占已由 core 的 gen_chains 保证）。
//!
//! 指标 `wmfs_label_match_ranked_total`；fail point `plugin.label-match.placement`（由 core 包在调用外层，触发 = 这次放置失败）。

use std::cmp::Reverse;
use std::sync::Arc;

use wmfs_core::api::metrics::{self, Counter};
use wmfs_core::base::ErrCode;
use wmfs_core::hooks::manifest::{CORE_ABI_VERSION, Hook, Manifest};
use wmfs_core::hooks::traits::{ChainView, PlacementInput, PlacementPlugin};

pub static MANIFEST: Manifest = Manifest {
    name: "label-match",
    version: "0.1.0",
    hooks: &[Hook::Placement],
    altitude: 510,
    hot_path: true,
    config_schema: "",
    abi_version: CORE_ABI_VERSION,
    attrs: &["class", "replicas", "stripe", "locality"],
    budget_ns: 50_000, // 3.7.4：placement 50 µs
    fail_points: &["plugin.label-match.placement"],
};

pub struct LabelMatch {
    ranked: Arc<Counter>,
}

impl LabelMatch {
    pub fn new() -> LabelMatch {
        LabelMatch {
            ranked: metrics::counter("wmfs_label_match_ranked_total", "label-match 排过序的放置请求数", &[])
        }
    }
}

impl Default for LabelMatch {
    fn default() -> LabelMatch {
        LabelMatch::new()
    }
}

impl PlacementPlugin for LabelMatch {
    fn manifest(&self) -> &'static Manifest {
        &MANIFEST
    }

    fn rank(&self, input: &PlacementInput, candidates: &mut Vec<ChainView>) -> Result<(), ErrCode> {
        let ok = |c: &ChainView| {
            c.class_id == input.class.id
                && c.replicas == input.replicas
                && c.serving >= 1
                && !input.exclude.contains(&c.id)
        };
        candidates.retain(ok);
        candidates.sort_by_key(|c| (Reverse(c.serving), usage(c), mix(input.seed, c.id)));
        self.ranked.inc();
        Ok(())
    }
}

/// 使用率（百万分之几）；capacity 为 0（不限或未知）按 0 算。
fn usage(c: &ChainView) -> u64 {
    if c.capacity == 0 { 0 } else { (c.used as u128 * 1_000_000 / c.capacity as u128).min(u64::MAX as u128) as u64 }
}

/// `hash(seed, chain_id)`：splitmix64 的最后一步（确定性，不依赖进程与平台）。
fn mix(seed: u64, chain: u32) -> u64 {
    let mut z = seed ^ (chain as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests;
