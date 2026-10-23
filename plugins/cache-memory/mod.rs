//! cache-memory：客户端 cache_store 插件（design.md 4.11）——1 MiB 块的 LRU，放在挂载进程的内存里（容量默认 512 MiB，挂载参数
//! `--cache memory:SIZE`）。
//!
//! - 键是 `(ino, change_id, 块号)`（3.7.3 的 `CacheKey`）：文件一变（change_id 变了）旧条目就再也查不到，自然作废，LRU 慢慢淘汰；
//!   `invalidate_ino` 删掉这个 inode 的全部条目。
//! - 只缓存已提交的数据（读路径从数据节点读回来才放进来；写回缓冲里的数据不进来），所以不存在"缓存比数据节点新"的情况。
//! - 一个块最多 1 MiB；放进来的块让总量超过容量时，先淘汰最久没用过的。
//!
//! 指标 `wmfs_cache_memory_hits_total`、`wmfs_cache_memory_misses_total`、`wmfs_cache_memory_bytes`；fail point
//! `plugin.cache-memory.cache_store`（本体包在每次调用外层，触发 = 这次 get 当作没命中、put 不放）。

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use wmfs_core::api::metrics::{self, Counter, Gauge};
use wmfs_core::base::lock;
use wmfs_core::hooks::manifest::{CORE_ABI_VERSION, Hook, Manifest};
use wmfs_core::hooks::traits::{CacheKey, CacheStore};

pub static MANIFEST: Manifest = Manifest {
    name: "cache-memory",
    version: "0.1.0",
    hooks: &[Hook::CacheStore],
    altitude: 0,
    hot_path: true,
    config_schema: "",
    abi_version: CORE_ABI_VERSION,
    attrs: &[],
    budget_ns: 20_000, // 3.7.4：cache_store memory 20 µs（另加拷贝）
    fail_points: &["plugin.cache-memory.cache_store"],
};

/// 默认容量（4.11）。
pub const DEFAULT_CAPACITY: u64 = 512 << 20;

/// LRU：条目表 + 按"最近一次使用"的序号排的淘汰顺序。
#[derive(Default)]
struct Lru {
    items: HashMap<CacheKey, (Vec<u8>, u64)>, // 键 → (数据, 最近一次使用的序号)
    order: BTreeMap<u64, CacheKey>,           // 序号 → 键（最小的最久没用）
    tick: u64,
    bytes: u64,
}

pub struct CacheMemory {
    lru: Mutex<Lru>,
    capacity: u64,
    hits: Arc<Counter>,
    misses: Arc<Counter>,
    size: Arc<Gauge>,
}

impl CacheMemory {
    /// 容量 `capacity` 字节（0 = 什么也不缓存）。
    pub fn new(capacity: u64) -> CacheMemory {
        CacheMemory {
            lru: Mutex::default(),
            capacity,
            hits: metrics::counter("wmfs_cache_memory_hits_total", "cache-memory 命中次数", &[]),
            misses: metrics::counter("wmfs_cache_memory_misses_total", "cache-memory 没命中的次数", &[]),
            size: metrics::gauge("wmfs_cache_memory_bytes", "cache-memory 当前缓存的字节数", &[]),
        }
    }
}

impl Lru {
    fn touch(&mut self, key: &CacheKey) {
        self.tick += 1;
        if let Some((_, t)) = self.items.get_mut(key) {
            self.order.remove(t);
            *t = self.tick;
            self.order.insert(self.tick, *key);
        }
    }

    fn remove(&mut self, key: &CacheKey) {
        if let Some((d, t)) = self.items.remove(key) {
            self.order.remove(&t);
            self.bytes -= d.len() as u64;
        }
    }

    /// 放进一块（调用方已经腾出了地方），记成最近用过的。
    fn insert(&mut self, key: &CacheKey, data: &[u8]) {
        self.tick += 1;
        self.items.insert(*key, (data.to_vec(), self.tick));
        self.order.insert(self.tick, *key);
        self.bytes += data.len() as u64;
    }
}

impl CacheStore for CacheMemory {
    fn manifest(&self) -> &'static Manifest {
        &MANIFEST
    }

    fn get(&self, key: &CacheKey, into: &mut [u8]) -> Option<usize> {
        let mut l = lock(&self.lru);
        let Some((d, _)) = l.items.get(key) else {
            self.misses.inc();
            return None;
        };
        let n = d.len().min(into.len());
        into[..n].copy_from_slice(&d[..n]);
        l.touch(key);
        self.hits.inc();
        Some(n)
    }

    fn put(&self, key: &CacheKey, data: &[u8]) {
        if data.len() as u64 > self.capacity {
            return;
        }
        let mut l = lock(&self.lru);
        l.remove(key); // 同一个键放两次：新的盖旧的
        while l.bytes + data.len() as u64 > self.capacity {
            let Some((_, oldest)) = l.order.pop_first() else { break }; // 最久没用过的（先从顺序表里摘掉：循环一定往前走）
            l.remove(&oldest);
        }
        l.insert(key, data);
        self.size.set(l.bytes as f64);
    }

    fn invalidate_ino(&self, ino: u64) {
        let mut l = lock(&self.lru);
        let keys: Vec<CacheKey> = l.items.keys().filter(|k| k.ino == ino).copied().collect();
        for k in &keys {
            l.remove(k);
        }
        self.size.set(l.bytes as f64);
    }
}

#[cfg(test)]
mod tests;
