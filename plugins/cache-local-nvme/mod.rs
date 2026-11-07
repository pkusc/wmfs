//! cache-local-nvme：客户端 cache_store 插件（design.md 4.11，实现注记（M13））——把读过的块缓存在本机的一个目录里（通常在本地 NVMe 上），
//! 按总字节 LRU 淘汰（默认 50 GiB，挂载参数 `--cache local-nvme:DIR[:SIZE]`）。
//!
//! - 键是 `(ino, change_id, 块号)`：文件一变（change_id 变了）旧块就再也查不到，自然作废、按 LRU 淘汰；`invalidate_ino` 删掉这个 inode 的全部块。
//!   只缓存已提交的数据（读路径从数据节点读回来才放进来）。
//! - 与 4.11 的写法不同、取最简单的做法：**一块一个文件**（名字 `<ino>-<change_id>-<块号>` 的十六进制），淘汰 = 删这个文件；块表只在内存里，
//!   不用稀疏文件与位图（插件里不能有 unsafe，打洞要 fallocate）。
//! - **挂载时清空**（不做崩溃恢复，避免任何陈旧性推理）：只删目录里这个命名格式的文件，不碰别的——缓存目录写错时不会删掉用户的文件。
//! - **权限**（M13 审查）：缓存里是读出来的**明文**（解密在数据节点上做），包括只有属主能读的文件、加密目录里的文件——所以目录建成 0700、
//!   块文件 0600；目录已经存在时，它必须属于本进程的用户、而且组和别人都没有任何权限，否则拒绝挂载（不然本机别的用户能读到缓存，绕过挂载点上的
//!   权限检查与静态加密）。
//! - **一个目录只给一个挂载用**：目录里 `.lock` 的独占 flock（`File::try_lock`）在挂载期间一直拿着，拿不到就拒绝挂载（两个挂载用同一个目录会
//!   互相清掉、覆盖对方的块：块文件名只在一个集群里唯一）。
//! - 读写文件出错只当没命中 / 不放（本体照常从数据节点读）。
//!
//! 指标 `wmfs_cache_local_nvme_total{result}`（result = hit / miss）、`wmfs_cache_local_nvme_bytes`；fail point
//! `plugin.cache-local-nvme.cache_store`（本体包在每次调用外层，触发 = 这次 get 当作没命中、put 不放）。

use std::collections::{BTreeMap, HashMap};
use std::fs::{DirBuilder, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use wmfs_core::api::metrics::{self, Counter, Gauge};
use wmfs_core::base::{lock, sys};
use wmfs_core::hooks::manifest::{CORE_ABI_VERSION, Hook, Manifest};
use wmfs_core::hooks::traits::{CacheKey, CacheStore};

pub static MANIFEST: Manifest = Manifest {
    name: "cache-local-nvme",
    version: "0.1.0",
    hooks: &[Hook::CacheStore],
    altitude: 0,
    hot_path: true,
    config_schema: "",
    abi_version: CORE_ABI_VERSION,
    attrs: &[],
    budget_ns: 50_000_000, // 3.7.4：cache_store local-nvme 50 ms（本地盘 IO）
    fail_points: &["plugin.cache-local-nvme.cache_store"],
};

/// 默认容量（4.11）。
pub const DEFAULT_CAPACITY: u64 = 50 << 30;

#[derive(Default)]
struct Lru {
    items: HashMap<CacheKey, (u64, u64)>, // 键 → (字节数, 最近一次使用的序号)
    order: BTreeMap<u64, CacheKey>,       // 序号 → 键（最小的最久没用）
    tick: u64,
    bytes: u64,
}

impl Lru {
    /// 把 `k`（`n` 字节）标成刚用过（不在表里就加进去）。
    fn touch(&mut self, k: CacheKey, n: u64) {
        self.take(&k);
        self.tick += 1;
        self.items.insert(k, (n, self.tick));
        self.order.insert(self.tick, k);
        self.bytes += n;
    }

    /// 从表里去掉 `k`，回它的字节数（不在表里 → None）。
    fn take(&mut self, k: &CacheKey) -> Option<u64> {
        let (n, seq) = self.items.remove(k)?;
        self.order.remove(&seq);
        self.bytes -= n;
        Some(n)
    }
}

pub struct LocalNvme {
    dir: PathBuf,
    capacity: u64,
    lru: Mutex<Lru>,
    hits: Arc<Counter>,
    misses: Arc<Counter>,
    size: Arc<Gauge>,
    _lock: File, // `.lock` 的独占 flock：挂载期间一直拿着（见文件头）
}

/// 这个插件建的文件名吗（`<16 位>-<16 位>-<16 位>` 的十六进制）。
fn ours(name: &str) -> bool {
    let p: Vec<&str> = name.split('-').collect();
    p.len() == 3 && p.iter().all(|x| x.len() == 16 && x.bytes().all(|b| b.is_ascii_hexdigit()))
}

/// 新建（或截断）一个只有属主能读写（0600）的文件。
fn private_file(path: &Path) -> std::io::Result<File> {
    OpenOptions::new().write(true).create(true).truncate(true).mode(0o600).open(path)
}

impl LocalNvme {
    /// 在 `dir` 里缓存最多 `capacity` 字节；目录不在就建（0700），在就先核对属主与权限、清空这个插件留下的文件（见文件头）。
    pub fn new(dir: &Path, capacity: u64) -> Result<LocalNvme, String> {
        let show = dir.display();
        DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(dir)
            .map_err(|e| format!("建缓存目录 {show} 失败：{e}"))?;
        let md = std::fs::metadata(dir).map_err(|e| format!("读缓存目录 {show} 的属性失败：{e}"))?;
        let (me, _) = sys::euid_egid();
        if md.uid() != me || md.mode() & 0o077 != 0 {
            return Err(format!(
                "缓存目录 {show} 要属于本用户（uid {me}）、只有属主能访问（chmod 700）：缓存里是读出来的明文；现在属主 uid {}、权限 {:o}",
                md.uid(),
                md.mode() & 0o777
            ));
        }
        let lock_file = private_file(&dir.join(".lock")).map_err(|e| format!("建 {show}/.lock 失败：{e}"))?;
        lock_file
            .try_lock()
            .map_err(|e| format!("缓存目录 {show} 正被另一个挂载使用（一个目录只能给一个挂载用）：{e}"))?;
        let rd = std::fs::read_dir(dir).map_err(|e| format!("读缓存目录 {show} 失败：{e}"))?;
        for e in rd.flatten().filter(|e| e.file_name().to_str().is_some_and(ours)) {
            let _ = std::fs::remove_file(e.path());
        }
        let c =
            |r| metrics::counter("wmfs_cache_local_nvme_total", "cache-local-nvme 的命中与没命中", &[("result", r)]);
        Ok(LocalNvme {
            dir: dir.to_path_buf(),
            capacity,
            lru: Mutex::default(),
            hits: c("hit"),
            misses: c("miss"),
            size: metrics::gauge("wmfs_cache_local_nvme_bytes", "cache-local-nvme 缓存了多少字节", &[]),
            _lock: lock_file,
        })
    }

    fn path(&self, k: &CacheKey) -> PathBuf {
        self.dir.join(format!("{:016x}-{:016x}-{:016x}", k.ino, k.change_id, k.block))
    }

    /// 从块表里去掉一项、删它的文件（拿着锁调用）。
    fn drop_one(&self, l: &mut Lru, k: &CacheKey) {
        if l.take(k).is_some() {
            let _ = std::fs::remove_file(self.path(k));
        }
    }
}

impl CacheStore for LocalNvme {
    fn manifest(&self) -> &'static Manifest {
        &MANIFEST
    }

    fn get(&self, key: &CacheKey, into: &mut [u8]) -> Option<usize> {
        let n = {
            let mut l = lock(&self.lru);
            let Some(n) = l.items.get(key).map(|(n, _)| *n) else {
                self.misses.inc();
                return None;
            };
            l.touch(*key, n);
            n as usize
        };
        let data = match std::fs::read(self.path(key)) {
            Ok(b) if b.len() == n && n <= into.len() => b,
            _ => {
                self.drop_one(&mut lock(&self.lru), key); // 文件被淘汰了或读不出：当没命中
                self.misses.inc();
                return None;
            }
        };
        into[..n].copy_from_slice(&data);
        self.hits.inc();
        Some(n)
    }

    fn put(&self, key: &CacheKey, data: &[u8]) {
        let n = data.len() as u64;
        if n > self.capacity || private_file(&self.path(key)).and_then(|mut f| f.write_all(data)).is_err() {
            return;
        }
        let mut l = lock(&self.lru);
        l.touch(*key, n);
        while l.bytes > self.capacity {
            let Some(k) = l.order.values().next().copied() else { break };
            self.drop_one(&mut l, &k);
        }
        self.size.set(l.bytes as f64);
    }

    fn invalidate_ino(&self, ino: u64) {
        let mut l = lock(&self.lru);
        let keys: Vec<CacheKey> = l.items.keys().filter(|k| k.ino == ino).copied().collect();
        for k in keys {
            self.drop_one(&mut l, &k);
        }
        self.size.set(l.bytes as f64);
    }
}

#[cfg(test)]
mod tests;
