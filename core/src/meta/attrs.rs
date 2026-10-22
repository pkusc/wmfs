//! 目录属性的解析结果与解析器（design.md 3.12、3.8，ADR-0026）。
//!
//! 继承解析 `resolve(dir)`：沿 `parent` 指针从 dir 点查到根，逐级收集；Inherit 键取最近的定义；
//! 遇到 `inherit=false` 的目录，它自己的值仍生效，但不再向上取 Inherit 键；Stacked 键（配额）不受 `inherit=false` 影响。
//! 最后用内置默认值补齐（来源显示为 "(默认值)"）。leader 上 `AttrResolver` 缓存 `dir → (attr_epoch, Arc<Resolved>)`，
//! 任何属性修改和任何目录 rename 都令 `attr_epoch += 1`，缓存整体失效。
//!
//! 实现注记（M4）：`resolve` 只返回内置默认值（class = default、replicas = 存储类的默认副本数、chunk 4M、stripe 1、
//! inline 64K、checksum crc32c、cto、attr_ttl 1 s，其余关闭）；DIRATTR 的继承、来源与缓存在 M7。`ancestors` 沿 parent
//! 指针点查到根（rename 判断"不得把目录移进自己的子树"用它；M7 起继承解析、M10 起 path-caps 也用，到时加缓存）。

use std::sync::Arc;

use crate::base::ErrCode;
use crate::data::format::{CK_CRC32C, CMP_NONE};
use crate::meta::engine::txn::KvRead;
use crate::meta::schema::keys::{self, ROOT_INO};
use crate::meta::schema::kv;
use crate::meta::schema::records::{ClassIdV1, InodeV1, StorageClassV1};

/// 内置默认值（3.8）。
const DEFAULT_CLASS: &str = "default";
const DEFAULT_CHUNK: u32 = 4 << 20;
const DEFAULT_INLINE: u32 = 64 << 10;
pub const DEFAULT_ATTR_TTL_MS: u32 = 1000;
/// `consistency` 的取值（`EffectiveAttrs.consistency`、`AttrOut.consistency`，3.3.4）。
pub const CONSISTENCY_CTO: u8 = 1;
pub const CONSISTENCY_STRONG: u8 = 2;
/// 目录树的深度上限（防止损坏的 parent 指针让遍历不停）。
const MAX_DEPTH: usize = 4096;

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

impl EffectiveAttrs {
    /// 内置默认值：存储类 `class_id`、副本数 = 它的默认副本数，其余见文件头。来源全部是"默认值"（`sources` 为空）。
    pub fn defaults(class_id: u16, replicas: u8) -> EffectiveAttrs {
        EffectiveAttrs {
            class_id,
            replicas,
            chunk: DEFAULT_CHUNK,
            stripe: 1,
            inline_max: DEFAULT_INLINE,
            checksum: CK_CRC32C,
            compress: CMP_NONE,
            compress_level: 0,
            encrypt_key: 0,
            consistency: CONSISTENCY_CTO,
            attr_ttl_ms: DEFAULT_ATTR_TTL_MS,
            trash_ms: 0,
            trash_root: 0,
            ttl_ms: 0,
            tier: None,
            audit: AuditSpec::default(),
            locality: 0,
            acl_default: None,
            qos_id: 0,
            quotas: vec![],
            sources: vec![],
        }
    }
}

#[derive(Default)]
pub struct AttrResolver {/* dir → (attr_epoch, Arc<EffectiveAttrs>) 缓存；祖先链缓存（M7） */}

impl AttrResolver {
    /// 目录 `dir` 的生效属性。实现注记（M4）：只有内置默认值（见文件头）；default 类读 CLASS_NAME 与 CLASS 两次点查。
    pub fn resolve(&self, v: &dyn KvRead, dir: u64) -> Result<Arc<EffectiveAttrs>, ErrCode> {
        let _ = dir; // M7 起按 dir 的祖先链继承
        let id = kv::get::<ClassIdV1>(v, &keys::class_name(DEFAULT_CLASS))?.ok_or(ErrCode::INTERNAL)?.id;
        let class = kv::get::<StorageClassV1>(v, &keys::class(id))?.ok_or(ErrCode::INTERNAL)?;
        Ok(Arc::new(EffectiveAttrs::defaults(id, class.replica)))
    }

    /// dir → … → 根（沿 parent 指针点查；根的 parent 是 0）。
    pub fn ancestors(&self, v: &dyn KvRead, dir: u64) -> Result<Arc<Vec<u64>>, ErrCode> {
        let mut out = vec![dir];
        let mut cur = dir;
        while cur != ROOT_INO {
            let i = kv::get::<InodeV1>(v, &keys::inode(cur))?.ok_or(ErrCode::ENOENT)?;
            if i.parent == 0 || out.len() > MAX_DEPTH {
                return Err(ErrCode::INTERNAL); // 走不到根：parent 指针损坏
            }
            cur = i.parent;
            out.push(cur);
        }
        Ok(Arc::new(out))
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
