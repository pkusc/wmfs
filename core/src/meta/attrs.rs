//! 目录属性的解析结果与解析器（design.md 3.12、3.8，ADR-0026）。
//!
//! 继承解析 `resolve(dir)`：沿 `parent` 指针从 dir 点查到根，逐级收集 DIRATTR；Inherit 键取最近的定义；遇到 `inherit=false` 的
//! 目录，它自己的值仍生效，但不再向上取 Inherit 键；Stacked 键（配额，M10）不受 `inherit=false` 影响。最后用内置默认值补齐
//! （来源显示为 "(默认值)"）：class = default、replicas = 存储类的默认副本数、chunk 4M、stripe 1、inline 64K、checksum crc32c、
//! cto、attr_ttl 1 s，其余关闭。每个值的来源目录记在 `sources` 里（0 = 内置默认值）。
//!
//! 缓存（3.8）：leader 上的 `AttrResolver` 按 `dir → Arc<EffectiveAttrs>` 与 `dir → 祖先链` 缓存，整体打一个 `attr_epoch` 标签：
//! 每次查都点查一次 GLOBAL `attr_epoch`，与标签不同就整个清空。**任何**属性修改与**任何**目录 rename 都令 `attr_epoch += 1`
//! （[`bump_attr_epoch`]：`attr_ops`、`ns::rename`、删掉有 DIRATTR 的目录；存储类的修改在 M13 也要这样），所以"同一个 attr_epoch 下
//! 解析结果相同"。实现注记（M7）：执行器的批次覆盖层里 `attr_epoch` 可能已经加了一、批次却没能提交（作废、丢失、fail point）——那时
//! 按覆盖层算出的缓存项就不对了，所以执行器在批次没有成功应用时调用 [`AttrResolver::clear`]；同一个操作里加了 `attr_epoch` 之后不再
//! 解析（操作失败回滚时也就不会留下错的项）。缓存的大小按"生效属性每项 1、祖先链按长度"计（[`CACHE_MAX`]），很深的目录树也撑不大内存。

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use wmfs_api::metrics::{self, Counter};

use crate::base::{ErrCode, lock};
use crate::data::format::{CK_BLAKE3, CK_CRC32C, CK_XXH3, CMP_NONE};
use crate::meta::attr_registry::{self, AttrKind, attr_ttl_ms};
use crate::meta::engine::txn::{KvRead, KvTxn};
use crate::meta::schema::keys::{self, G_ATTR_EPOCH, G_NEXT_RECONCILE, ROOT_INO};
use crate::meta::schema::kv;
use crate::meta::schema::records::{AttrMapV1, ClassIdV1, InodeV1, ReconcileItemV1, StorageClassV1};

/// 内置默认值（3.8）。
pub const DEFAULT_CLASS: &str = "default";
const DEFAULT_CHUNK: u32 = 4 << 20;
const DEFAULT_INLINE: u32 = 64 << 10;
pub const DEFAULT_ATTR_TTL_MS: u32 = 1000;
/// `consistency` 的取值（`EffectiveAttrs.consistency`、`AttrOut.consistency`，3.3.4）。
pub const CONSISTENCY_CTO: u8 = 1;
pub const CONSISTENCY_STRONG: u8 = 2;
/// 沿 parent 指针走的步数上限：只防损坏的 parent 指针让遍历不停（实现注记（M7）：规格没有目录深度的限制，所以取得远比真实的
/// 目录树深；祖先链、`attr_ops::path_of`、`rstat::check` 共用它）。
pub const MAX_DEPTH: usize = 1 << 16;
/// 缓存的大小上限（单位：生效属性每项算 1、祖先链按它的长度算）；超过就整个清空（只是缓存）。
const CACHE_MAX: usize = 1 << 17;

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

    /// 键 `key` 的来源目录（0 = 内置默认值）。
    pub fn source(&self, key: &str) -> u64 {
        self.sources.iter().find(|(k, _)| *k == key).map_or(0, |(_, d)| *d)
    }
}

/// 一个 attr_epoch 标签下的两张表（见文件头）：dir → 生效属性、dir → 祖先链；`size` 的单位见 [`CACHE_MAX`]。
#[derive(Default)]
struct Cache {
    epoch: u64,
    eff: HashMap<u64, Arc<EffectiveAttrs>>,
    anc: HashMap<u64, Arc<Vec<u64>>>,
    size: usize,
}

impl Cache {
    /// 要再放 `n` 个单位：放不下就整个清空（标签不变）。
    fn make_room(&mut self, n: usize) {
        if self.size + n > CACHE_MAX {
            *self = Cache { epoch: self.epoch, ..Cache::default() };
        }
        self.size += n;
    }
}

pub struct AttrResolver {
    cache: Mutex<Cache>,
    hits: Arc<Counter>,
    misses: Arc<Counter>,
}

impl Default for AttrResolver {
    fn default() -> AttrResolver {
        let c = |r| metrics::counter("wmfs_attr_cache_total", "目录属性解析缓存的命中与未命中", &[("result", r)]);
        AttrResolver { cache: Mutex::default(), hits: c("hit"), misses: c("miss") }
    }
}

fn attr_epoch(v: &dyn KvRead) -> Result<u64, ErrCode> {
    kv::get::<u64>(v, &keys::global(G_ATTR_EPOCH))?.ok_or(ErrCode::INTERNAL)
}

impl AttrResolver {
    /// 目录 `dir` 的生效属性（见文件头）。`dir` 不存在 → `ENOENT`。
    pub fn resolve(&self, v: &dyn KvRead, dir: u64) -> Result<Arc<EffectiveAttrs>, ErrCode> {
        let epoch = attr_epoch(v)?;
        if let Some(e) = self.cache_at(epoch).eff.get(&dir).cloned() {
            self.hits.inc();
            return Ok(e);
        }
        self.misses.inc();
        let chain = self.ancestors(v, dir)?;
        let eff = Arc::new(compute(v, &chain)?);
        let mut c = self.cache_at(epoch);
        c.make_room(1);
        c.eff.insert(dir, eff.clone());
        Ok(eff)
    }

    /// dir → … → 根（沿 parent 指针点查；根的 parent 是 0）。按 attr_epoch 缓存（目录 rename 让它加一）。
    pub fn ancestors(&self, v: &dyn KvRead, dir: u64) -> Result<Arc<Vec<u64>>, ErrCode> {
        let epoch = attr_epoch(v)?;
        if let Some(a) = self.cache_at(epoch).anc.get(&dir).cloned() {
            return Ok(a);
        }
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
        let a = Arc::new(out);
        let mut c = self.cache_at(epoch);
        c.make_room(a.len());
        c.anc.insert(dir, a.clone());
        Ok(a)
    }

    /// 清空缓存：执行器的批次没有成功应用时（见文件头）。
    pub fn clear(&self) {
        *lock(&self.cache) = Cache::default();
    }

    /// 上锁；标签不是 `epoch`（属性变过了）就先换成一个空的缓存。
    fn cache_at(&self, epoch: u64) -> MutexGuard<'_, Cache> {
        let mut c = lock(&self.cache);
        if c.epoch != epoch {
            *c = Cache { epoch, ..Cache::default() };
        }
        c
    }
}

/// `attr_epoch += 1`（见文件头：任何属性修改、任何目录 rename、删掉有 DIRATTR 的目录）。之后这个闭包里不再解析属性。
pub fn bump_attr_epoch(v: &mut dyn KvTxn) -> Result<(), ErrCode> {
    kv::next_id(v, G_ATTR_EPOCH).map(|_| ())
}

/// 写一条 `RECONCILE_Q(dir, reason)`（3.8"变更通知"③、3.1.2：1 = "新数据 + 协调器"类的属性变了，2 = 目录被移动了；协调器在
/// M11 消费）。
pub fn queue_reconcile(v: &mut dyn KvTxn, dir: u64, reason: u8) -> Result<(), ErrCode> {
    let seq = kv::next_id(v, G_NEXT_RECONCILE)?;
    kv::put(v, &keys::reconcile(seq), &ReconcileItemV1 { dir, reason });
    Ok(())
}

/// 不经缓存算一遍 `chain`（dir → … → 根）的生效属性（见文件头）。
fn compute(v: &dyn KvRead, chain: &[u64]) -> Result<EffectiveAttrs, ErrCode> {
    let mut got: Vec<(&'static str, String, u64)> = Vec::new();
    for &d in chain {
        let Some(m) = kv::get::<AttrMapV1>(v, &keys::dirattr(d))? else { continue };
        for (k, val) in &m.entries {
            let Some(s) = attr_registry::find(k).filter(|s| s.kind == AttrKind::Inherit) else { continue };
            if !got.iter().any(|(g, ..)| *g == s.key) {
                got.push((s.key, val.clone(), d));
            }
        }
        if m.entries.get("inherit").is_some_and(|x| x == "false") {
            break; // 它自己的值已经收了，不再向上取 Inherit 键
        }
    }
    let class = got.iter().find(|(k, ..)| *k == "class").map_or(DEFAULT_CLASS, |(_, v, _)| v.as_str());
    let id = kv::get::<ClassIdV1>(v, &keys::class_name(class))?.ok_or(ErrCode::INTERNAL)?.id;
    let rec = kv::get::<StorageClassV1>(v, &keys::class(id))?.ok_or(ErrCode::INTERNAL)?;
    let mut eff = EffectiveAttrs::defaults(id, rec.replica);
    for (k, val, d) in got {
        set_typed_field(&mut eff, k, &val).ok_or(ErrCode::INTERNAL)?; // DIRATTR 里存的是规范化过的值：解不开 = 状态损坏
        eff.sources.push((k, d));
    }
    Ok(eff)
}

/// 把一个规范化的值（字符串）解成 `eff` 里对应的类型化字段（class 已经在 `compute` 里处理过）。
fn set_typed_field(eff: &mut EffectiveAttrs, key: &str, v: &str) -> Option<()> {
    match key {
        "class" => {}
        "replicas" => eff.replicas = v.parse().ok()?,
        "chunk" => eff.chunk = v.parse().ok()?,
        "stripe" => eff.stripe = v.parse().ok()?,
        "inline" => eff.inline_max = v.parse().ok()?,
        "checksum" => eff.checksum = checksum_id(v)?,
        "consistency" => {
            eff.consistency = CONSISTENCY_CTO; // strong 在 M12
            eff.attr_ttl_ms = attr_ttl_ms(v).map_or(Some(DEFAULT_ATTR_TTL_MS), |ms| u32::try_from(ms).ok())?;
        }
        _ => return None,
    }
    Some(())
}

/// 校验算法名 → 编号（3.2.4）。
pub fn checksum_id(name: &str) -> Option<u8> {
    match name {
        "crc32c" => Some(CK_CRC32C),
        "xxh3" => Some(CK_XXH3),
        "blake3" => Some(CK_BLAKE3),
        _ => None,
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

#[cfg(test)]
mod tests;
