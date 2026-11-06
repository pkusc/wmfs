//! "声明 vs 实际"（design.md 3.10 的表，ADR-0027；实现注记（M11））：协调器、迁移原语与 `wmfs status` 共用的比较（[`differs`]、
//! [`keys_of`]），以及 `wmfs status` 的逐文件统计（[`scan`] / [`fill`]：在 leader 上对子树做一次只读扫描）。
//!
//! - `declared(file)` = 主父目录的生效属性（`ns::common::governing`，遍历器给出；tier 规则在 M13 套上去——到时协调器、tiering 与这里一起改）。
//! - 协调器要迁移的四个键（[`MIGRATED`]）：`class`（`inode.class_id`）、`replicas`（`layout.replicas`）、`compress`（算法与级别）、`encrypt`
//!   （数据密钥 id）；与声明不同即 [`differs`]（加密生效的目录里的 inline 文件也就不同了：它的 layout 不带加密）。另外四个（chunk、stripe、
//!   inline、checksum）标注"不迁移"。inline 比的是新文件实际的阈值（`ns::layout::inline_of`：加密生效时为 0），不是声明值。
//! - 扫描：子树 DFS（`bg::walker`，每批 256 个目录项），固定上限每秒 [`SCAN_RATE`] 个 inode（同步请求里做，不经后台任务的 RateLimiter；
//!   不把 leader 的元数据读挤满），最多 [`SCAN_MAX`] 个 inode——超过就停下、`truncated = true`，warnings 里明说统计不全。每个普通文件
//!   （孤儿不算；硬链接只在主链接那一项计，与递归计数同一个口径）逐键比较 3.10 的表（[`PER_FILE`]）：class、replicas（另数实际副本数少于
//!   声明的：它的链此刻正常服务的成员少于 `layout.replicas`；inline 文件的数据在元数据里，不算）、compress、encrypt（这四个另数写打开着的——
//!   协调器跳过它们）、chunk、stripe（另数条带数少于声明的）、inline、checksum。位于降级链上的文件数与字节数一起数出来。

use std::collections::BTreeMap;
use std::time::Duration;

use super::walker::{Walked, Walker};
use crate::api::types::StatusReply;
use crate::base::{ErrCode, clock};
use crate::data::chain;
use crate::data::format::CMP_ZSTD;
use crate::meta::attrs::EffectiveAttrs;
use crate::meta::cluster::chains::serving_count;
use crate::meta::engine::txn::MetaEngine;
use crate::meta::ns::common::Node;
use crate::meta::ns::layout::inline_of;
use crate::meta::schema::records::{F_INLINE, InodeV1, KIND_FILE, LayoutV1};
use crate::meta::sessions::Live;
use crate::proto::peer::ClusterMap;

/// 逐文件统计最多看多少个 inode、每秒最多看多少个（3.10："上限 100 万个文件"）。
pub const SCAN_MAX: u64 = 1_000_000;
pub const SCAN_RATE: u64 = 200_000;

/// 协调器会迁移的键；有逐文件统计的键（前四个之外的标注"不迁移"）。
pub const MIGRATED: [&str; 4] = ["class", "replicas", "compress", "encrypt"];
pub const PER_FILE: [&str; 8] = ["class", "replicas", "compress", "encrypt", "chunk", "stripe", "inline", "checksum"];

/// 协调器要迁移的键里有没有与声明不同的（见文件头）。
pub fn differs(i: &InodeV1, e: &EffectiveAttrs) -> bool {
    i.layout.is_some() && keys_of(i, e).iter().any(|(k, d)| *d && MIGRATED.contains(k))
}

/// [`PER_FILE`] 的每个键：(键, 实际与声明不同吗)。
pub fn keys_of(i: &InodeV1, e: &EffectiveAttrs) -> [(&'static str, bool); 8] {
    let l = i.layout.as_ref();
    let get = |f: fn(&LayoutV1) -> u64| l.map_or(0, f);
    let alg = |alg: u8, level: i8| if alg == CMP_ZSTD { (alg, level) } else { (alg, 0) }; // 只有 zstd 有级别
    let compress = l.is_some_and(|l| alg(l.compress, l.compress_level) != alg(e.compress, e.compress_level));
    [
        ("class", i.class_id != e.class_id),
        ("replicas", get(|l| l.replicas as u64) != e.replicas as u64),
        ("compress", compress),
        ("encrypt", get(|l| l.encrypt_key as u64) != e.encrypt_key as u64),
        ("chunk", get(|l| l.chunk_size as u64) != e.chunk as u64),
        ("stripe", get(|l| l.chains.len() as u64) != e.stripe as u64),
        ("inline", get(|l| l.inline_max as u64) != inline_of(e) as u64),
        ("checksum", get(|l| l.checksum as u64) != e.checksum as u64),
    ]
}

/// 逐文件统计的结果（[`scan`]）。
#[derive(Default, Debug, PartialEq, Eq)]
pub struct Counts {
    pub scanned: u64,
    pub files: u64,
    pub mismatched: BTreeMap<&'static str, u64>,
    pub open: BTreeMap<&'static str, u64>,
    pub short: BTreeMap<&'static str, u64>, // replicas：实际副本数不足；stripe：条带数不足
    pub degraded_files: u64,
    pub degraded_bytes: u64,
    pub truncated: bool,
}

/// 一个普通文件计进统计（见文件头）。
fn count(c: &mut Counts, w: &Walked, map: &ClusterMap, live: &Live) {
    let i = &w.inode;
    let Some(l) = i.layout.as_ref().filter(|_| i.kind == KIND_FILE && i.nlink > 0) else { return };
    if w.parent != 0 && (w.parent != i.parent || w.name != i.name) {
        return; // 这是它的另一个硬链接：只在主链接那一项计
    }
    c.files += 1;
    let open = live.is_write_open(w.ino);
    for (k, differ) in keys_of(i, &w.eff) {
        if differ {
            *c.mismatched.entry(k).or_default() += 1;
            if open && MIGRATED.contains(&k) {
                *c.open.entry(k).or_default() += 1;
            }
        }
    }
    if l.chains.len() < w.eff.stripe as usize {
        *c.short.entry("stripe").or_default() += 1;
    }
    let serving = |id: &u32| chain::find(map, *id).map_or(0, serving_count);
    if i.flags & F_INLINE == 0 && l.chains.iter().any(|ch| serving(ch) < l.replicas as usize) {
        *c.short.entry("replicas").or_default() += 1;
    }
    let degraded = |id: &u32| chain::find(map, *id).is_some_and(|r| serving_count(r) < r.replicas as usize);
    if i.flags & F_INLINE == 0 && l.chains.iter().any(degraded) {
        c.degraded_files += 1;
        c.degraded_bytes += i.size;
    }
}

/// 对 `root`（目录 → 它的子树；文件 → 只有它）做逐文件统计（见文件头）。
pub fn scan(eng: &dyn MetaEngine, root: &Node, map: &ClusterMap, live: &Live) -> Result<Counts, ErrCode> {
    let (walker, mut c, t0) = (Walker::new(), Counts::default(), clock::mono_ms());
    if !root.is_dir() {
        if let Some(w) = walker.one(eng, root.ino)? {
            c.scanned = 1;
            count(&mut c, &w, map, live);
        }
        return Ok(c);
    }
    let mut cursor = vec![];
    loop {
        let page = walker.subtree(eng, root.ino, &cursor)?;
        for w in &page.items {
            c.scanned += 1;
            count(&mut c, w, map, live);
        }
        if page.done {
            return Ok(c);
        }
        if c.scanned >= SCAN_MAX {
            c.truncated = true;
            return Ok(c);
        }
        cursor = page.cursor;
        let due = c.scanned * 1000 / SCAN_RATE; // 限速：到这里至少该花多少毫秒
        let spent = clock::mono_ms().saturating_sub(t0);
        if due > spent {
            std::thread::sleep(Duration::from_millis(due - spent));
        }
    }
}

/// 把逐文件统计填进 `attr_ops::status` 的回复（只增字段）：每个有逐文件统计的键的 `actual` 与是否一致；扫不全时 warnings 里明说。
pub fn fill(r: &mut StatusReply, c: &Counts) {
    for a in r.attrs.iter_mut().filter(|a| PER_FILE.contains(&a.key.as_str())) {
        let k = a.key.as_str();
        let n = |m: &BTreeMap<&'static str, u64>| m.get(k).copied().unwrap_or(0);
        let (mism, moves) = (n(&c.mismatched), MIGRATED.contains(&k));
        let short = match k {
            "replicas" => Some(n(&c.short)), // M9 起 replicas 总有 short_files
            "stripe" => c.short.get(k).copied(),
            _ => None,
        };
        a.actual.short_files = short;
        a.actual.files = Some(c.files);
        a.actual.mismatched_files = Some(mism);
        a.actual.open_files = moves.then(|| n(&c.open));
        a.actual.migrates = Some(moves);
        a.mismatched |= mism > 0 || short.is_some_and(|x| x > 0);
    }
    r.scanned = c.scanned;
    r.truncated = c.truncated;
    r.degraded_files = c.degraded_files;
    r.degraded_bytes = c.degraded_bytes;
    if c.truncated {
        r.warnings.push(format!("逐文件统计只看了前 {SCAN_MAX} 个 inode：数字不全（wmfs status <子目录> 看得更细）"));
    }
}

#[cfg(test)]
mod tests;
