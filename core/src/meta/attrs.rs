//! 目录属性的解析结果与解析器（design.md 3.12、3.8，ADR-0026）。
//!
//! 继承解析 `resolve(dir)`：沿 `parent` 指针从 dir 点查到根，逐级收集；Inherit 键取最近的定义；
//! 遇到 `inherit=false` 的目录，它自己的值仍生效，但不再向上取 Inherit 键；Stacked 键（配额）不受 `inherit=false` 影响。
//! 最后用内置默认值补齐（来源显示为 "(默认值)"）。leader 上 `AttrResolver` 缓存 `dir → (attr_epoch, Arc<Resolved>)`，
//! 任何属性修改和任何目录 rename 都令 `attr_epoch += 1`，缓存整体失效。
//!
//! 接口骨架（M0）：签名以这里为准，实现在 M4（内置默认值）与 M7（继承、来源、注册表）。

use std::sync::Arc;

use crate::base::ErrCode;
use crate::meta::engine::txn::KvRead;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EffectiveAttrs {
    pub class_id: u16,
    pub replicas: u8,
    pub chunk: u32,
    pub stripe: u16,
    pub inline_max: u32,
    pub checksum: u8,
    pub compress: u8,
    pub compress_level: i8,
    pub encrypt_key: u32, // 0 = 不加密
    pub consistency: u8,  // 1 cto 2 strong
    pub attr_ttl_ms: u32,
    pub trash_ms: u64, // 0 = off
    pub trash_root: u64,
    pub ttl_ms: u64,
    pub tier: Option<TierRule>,
    pub audit: AuditSpec,
    pub locality: u8,
    pub acl_default: Option<Vec<u8>>,
    pub qos_id: u64,
    pub quotas: Vec<(u64 /*目录*/, QuotaDecl)>, // Stacked：所有祖先上的 quota 与 quota.<class>
    pub sources: Vec<(&'static str, u64)>,      // 每个键的来源目录（0 = 内置默认值）
}

pub struct AttrResolver {/* dir → (attr_epoch, Arc<EffectiveAttrs>) 缓存；祖先链缓存 */}

#[allow(unused_variables)] // 骨架：函数体在 M4/M7 实现，实现后删除这一行
impl AttrResolver {
    pub fn resolve(&self, v: &dyn KvRead, dir: u64) -> Result<Arc<EffectiveAttrs>, ErrCode> {
        unimplemented!("M4/M7：AttrResolver::resolve")
    }
    /// dir → … → 根
    pub fn ancestors(&self, v: &dyn KvRead, dir: u64) -> Result<Arc<Vec<u64>>, ErrCode> {
        unimplemented!("M4：AttrResolver::ancestors")
    }
}

// ===== 被多个模块引用的小类型（3.12）=====

/// cond 已在 set 时校验过
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TierRule {
    pub from: u16,
    pub to: u16,
    pub cond: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct AuditSpec {
    pub on: bool,
    pub ops: Vec<u16>,  // 空 = 全部修改类操作
    pub uids: Vec<u32>, // 空 = 所有人
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QuotaDecl {
    pub class: Option<u16>,
    pub hard: u64,
    pub soft: u64,
    pub grace_ms: u64,
    pub files: u64,
    pub quantum: u64,
}
