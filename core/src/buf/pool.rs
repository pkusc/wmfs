//! 预注册内存池与缓冲区所有权（design.md 3.6.1、3.6.2、3.12 RegionInfo，ADR-0022）。
//!
//! 启动时一次 `mmap(MAP_PRIVATE|MAP_ANONYMOUS|MAP_POPULATE)` 一整块内存，切成三档槽位（S/M/L）；运行期间永不归还给
//! 操作系统（整个池被丢弃时才 munmap，只发生在进程退出与测试里）。每档 25% 的槽位（向上取整、至少 1 个）只给复制流量
//! （`Lane::Repl`）：Repl 先取 Repl 槽、再取 Normal 槽；Normal 只能取 Normal 槽。没有空槽就在 Condvar 上等，最多 `wait`，
//! 超时回 `BUSY`（天然背压）。
//!
//! 规则：`Buf` 不实现 Clone——按值传递 = 所有权转移，Drop = 归还池；`freeze()` 之后只读、可共享；
//! `BufSlice` 是一个 FrozenBuf 的子区间（只发几段 extent 时不用拷贝）。`Buf` 的字段里不存裸指针
//! （只存池引用与槽位号），所以它自动是 `Send + Sync`，不需要 `unsafe impl`。注意：取到的槽位里是上一个主人留下的旧字节，
//! 调用方只能读自己写过的部分。本文件是允许写 `unsafe` 的文件之一（mmap、munmap、切片）。

use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use wmfs_api::metrics::{self, Gauge, Histogram};

use crate::base::{ErrCode, clock};

/// 三档槽位的大小：S = 128 KiB；M = 4 MiB + 128 KiB；L = 16 MiB + 128 KiB（多出的 128 KiB 放头部与块表）。
pub const S_SLOT: usize = 128 << 10;
pub const M_SLOT: usize = (4 << 20) + (128 << 10);
pub const L_SLOT: usize = (16 << 20) + (128 << 10);
const SLOT_SIZES: [usize; 3] = [S_SLOT, M_SLOT, L_SLOT];
const CLASS_NAMES: [&str; 3] = ["S", "M", "L"];

/// 启动时一次 mmap(MAP_PRIVATE|MAP_ANONYMOUS|MAP_POPULATE) 一整块内存，切成三档；永不归还给操作系统。
#[derive(Clone)]
pub struct MemPool {
    inner: Arc<PoolInner>, // Clone 只增引用计数
}

struct PoolInner {
    base: usize, // SAFETY: 只在本模块内转成指针
    len: usize,
    classes: [SizeClass; 3], // S = 128 KiB；M = 4 MiB + 128 KiB；L = 16 MiB + 128 KiB
    wait_secs: Arc<Histogram>,
}

/// 一档槽位。两张空闲表放在**同一把锁**里，`cv` 只配这一把锁（实现注记 M0，design.md 3.6.1）：
/// 如果两张表各有一把锁，`Lane::Repl`（先取 Repl 槽、再取 Normal 槽）等待时只能拿其中一把，
/// 要么丢失另一张表上的唤醒，要么嵌套持锁；std 的 Condvar 配多把锁还可能 panic。
struct SizeClass {
    slot_size: usize,
    offset: usize,
    slots: u32,
    free: Mutex<FreeSlots>,
    cv: Condvar,
    free_gauges: [Arc<Gauge>; 2], // wmfs_bufpool_free{class, lane=normal|repl}
}

/// 一档的空闲槽位号。
struct FreeSlots {
    normal: Vec<u32>,
    repl: Vec<u32>, // 只给 Lane::Repl
}

/// 复制流量专用 lane：每档 25% 的槽位（向上取整，至少 1 个）只给 Repl。
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Lane {
    Normal,
    Repl,
}

/// node.toml `[mem]`（部署参数）。
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PoolConfig {
    pub s_mb: u32,
    pub m_slots: u32,
    pub l_slots: u32,
    pub repl_lane: bool,
}

impl PoolConfig {
    /// 服务端 ≈ 1 GiB：S 256 MiB、M 124 槽 ≈ 512 MiB、L 15 槽 ≈ 242 MiB，有 Repl lane。
    pub fn server() -> Self {
        PoolConfig { s_mb: 256, m_slots: 124, l_slots: 15, repl_lane: true }
    }
    /// 客户端 ≈ 768 MiB：S 64 MiB、M 155 槽、L 4 槽，没有 Repl lane。
    pub fn client() -> Self {
        PoolConfig { s_mb: 64, m_slots: 155, l_slots: 4, repl_lane: false }
    }
    /// `wmfs dev` 下每个服务端进程 ≈ 384 MiB：S 128 MiB、M 31 槽、L 8 槽。
    pub fn dev_server() -> Self {
        PoolConfig { s_mb: 128, m_slots: 31, l_slots: 8, repl_lane: true }
    }
    /// CLI 的一次性命令（wmfs fs / ping / debug）≈ 70 MiB：S 16 MiB、M 9 槽、L 1 槽。
    pub fn small() -> Self {
        PoolConfig { s_mb: 16, m_slots: 9, l_slots: 1, repl_lane: false }
    }
    /// 整个池的字节数（`wmfs doctor` 拿 `server()` 的值与 memlock 上限比较）。
    pub fn total_bytes(&self) -> u64 {
        ((self.s_mb as u64) << 20) + self.m_slots as u64 * M_SLOT as u64 + self.l_slots as u64 * L_SLOT as u64
    }
}

/// 独占的可写缓冲：不实现 Clone。按值传递 = 所有权转移；Drop = 归还池。
pub struct Buf {
    pool: Arc<PoolInner>,
    class: u8,
    slot: u32,
    lane: Lane, // 槽位来自哪张空闲表（Repl 请求可能拿到 Normal 槽，归还时回到原来的表）
    len: u32,
}

/// 冻结后的只读缓冲：Clone 只是引用计数 +1；最后一个持有者 Drop 时归还池。
#[derive(Clone)]
pub struct FrozenBuf(Arc<Buf>);

impl MemPool {
    pub fn new(cfg: &PoolConfig) -> Result<MemPool, ErrCode> {
        let counts = [((cfg.s_mb as usize) << 20) / S_SLOT, cfg.m_slots as usize, cfg.l_slots as usize];
        if counts.contains(&0) {
            return Err(ErrCode::EINVAL);
        }
        let len: usize = counts.iter().zip(SLOT_SIZES).map(|(n, s)| n * s).sum();
        // SAFETY: 匿名私有映射，不涉及任何已有内存；参数都是合法常量；返回值在下面检查。
        let p = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_POPULATE,
                -1,
                0,
            )
        };
        if p == libc::MAP_FAILED {
            return Err(ErrCode::ENOMEM);
        }
        let mut offset = 0;
        let classes = [0, 1, 2].map(|i| {
            let n = counts[i] as u32;
            let repl = if cfg.repl_lane { n.div_ceil(4).max(1).min(n) } else { 0 };
            let c = SizeClass {
                slot_size: SLOT_SIZES[i],
                offset,
                slots: n,
                free: Mutex::new(FreeSlots { normal: (repl..n).rev().collect(), repl: (0..repl).rev().collect() }),
                cv: Condvar::new(),
                free_gauges: ["normal", "repl"].map(|l| {
                    metrics::gauge("wmfs_bufpool_free", "内存池空闲槽位数", &[("class", CLASS_NAMES[i]), ("lane", l)])
                }),
            };
            offset += counts[i] * SLOT_SIZES[i];
            c.update_gauges(&c.lock());
            c
        });
        let wait_secs = metrics::histogram(
            "wmfs_bufpool_wait_seconds",
            "从内存池取缓冲时等待的时间",
            &[],
            metrics::SECONDS_BUCKETS,
        );
        Ok(MemPool { inner: Arc::new(PoolInner { base: p as usize, len, classes, wait_secs }) })
    }

    /// 取一个能放下 `min_len` 字节的缓冲（选最小的一档；`len()` = min_len）。没有空槽就等最多 `wait`，超时回 `BUSY`；
    /// `min_len` 比最大的槽位还大 → `EINVAL`。
    pub fn get(&self, min_len: usize, lane: Lane, wait: Duration) -> Result<Buf, ErrCode> {
        let ci = SLOT_SIZES.iter().position(|s| min_len <= *s).ok_or(ErrCode::EINVAL)?;
        let c = &self.inner.classes[ci];
        let deadline = clock::mono_ms() + wait.as_millis() as u64;
        let mut waited_from = None;
        let mut fl = c.lock();
        loop {
            let got = match lane {
                Lane::Repl => {
                    fl.repl.pop().map(|s| (s, Lane::Repl)).or_else(|| fl.normal.pop().map(|s| (s, Lane::Normal)))
                }
                Lane::Normal => fl.normal.pop().map(|s| (s, Lane::Normal)),
            };
            if let Some((slot, from)) = got {
                c.update_gauges(&fl);
                drop(fl);
                if let Some(t0) = waited_from {
                    self.inner.wait_secs.observe((clock::mono_ms() - t0) as f64 / 1000.0);
                }
                let pool = self.inner.clone();
                return Ok(Buf { pool, class: ci as u8, slot, lane: from, len: min_len as u32 });
            }
            let now = clock::mono_ms();
            if now >= deadline {
                return Err(ErrCode::BUSY);
            }
            waited_from.get_or_insert(now);
            fl = c.cv.wait_timeout(fl, Duration::from_millis(deadline - now)).unwrap_or_else(|e| e.into_inner()).0;
        }
    }

    /// 给传输插件注册
    pub fn region(&self) -> RegionInfo {
        RegionInfo { base: self.inner.base as u64, len: self.inner.len as u64 }
    }

    /// 每档的槽位数与两张空闲表的长度（S、M、L 三项）：`/api/v1/node` 与测试用。
    pub fn stats(&self) -> Vec<ClassStats> {
        let c = &self.inner.classes;
        (0..3)
            .map(|i| {
                let fl = c[i].lock();
                let (free_normal, free_repl) = (fl.normal.len(), fl.repl.len());
                ClassStats {
                    name: CLASS_NAMES[i],
                    slot_size: c[i].slot_size,
                    slots: c[i].slots,
                    free_normal,
                    free_repl,
                }
            })
            .collect()
    }
}

/// 一档槽位的统计（[`MemPool::stats`]）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClassStats {
    pub name: &'static str, // "S" / "M" / "L"
    pub slot_size: usize,
    pub slots: u32,
    pub free_normal: usize,
    pub free_repl: usize, // 只给复制流量的空闲槽
}

impl SizeClass {
    fn lock(&self) -> MutexGuard<'_, FreeSlots> {
        self.free.lock().unwrap_or_else(|e| e.into_inner())
    }
    fn update_gauges(&self, fl: &FreeSlots) {
        self.free_gauges[0].set(fl.normal.len() as f64);
        self.free_gauges[1].set(fl.repl.len() as f64);
    }
}

impl Drop for PoolInner {
    fn drop(&mut self) {
        // 走到这里说明再没有任何 Buf（每个 Buf 都持有 Arc<PoolInner>），整块内存可以还给操作系统。
        // SAFETY: base/len 正是 new() 里 mmap 返回的映射；没有任何引用还指向它。
        unsafe { libc::munmap(self.base as *mut libc::c_void, self.len) };
    }
}

#[allow(clippy::len_without_is_empty)] // 按 design.md 原样
impl Buf {
    fn ptr(&self) -> *mut u8 {
        let c = &self.pool.classes[self.class as usize];
        (self.pool.base + c.offset + self.slot as usize * c.slot_size) as *mut u8
    }
    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: 槽位在映射范围内（offset + slot × slot_size + len ≤ 映射长度），映射活到最后一个 Buf 被丢弃；
        // 一个槽位同一时刻只属于一个 Buf，FrozenBuf 只给出 &[u8]，所以不会有别人同时写它。
        unsafe { std::slice::from_raw_parts(self.ptr(), self.len as usize) }
    }
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: 同 as_slice；&mut self 保证此刻没有别的引用指向这个槽位。
        unsafe { std::slice::from_raw_parts_mut(self.ptr(), self.len as usize) }
    }
    pub fn len(&self) -> usize {
        self.len as usize
    }
    /// n <= capacity
    pub fn set_len(&mut self, n: usize) {
        assert!(n <= self.capacity(), "不变量破坏：Buf::set_len({n}) 超过容量 {}", self.capacity());
        self.len = n as u32;
    }
    pub fn capacity(&self) -> usize {
        self.pool.classes[self.class as usize].slot_size
    }
    /// RDMA 用：池内绝对地址
    pub fn addr(&self) -> u64 {
        self.ptr() as u64
    }
    /// 显式转移：从此只读、可共享
    pub fn freeze(self) -> FrozenBuf {
        FrozenBuf(Arc::new(self))
    }
}

impl FrozenBuf {
    pub fn as_slice(&self) -> &[u8] {
        self.0.as_slice()
    }
    pub fn addr(&self) -> u64 {
        self.0.addr()
    }
    /// 引用计数为 1 时取回可写所有权
    pub fn try_unfreeze(self) -> Result<Buf, FrozenBuf> {
        Arc::try_unwrap(self.0).map_err(FrozenBuf)
    }
    /// 子区间视图（引用计数 +1，不拷贝）；越界 = 不变量破坏
    pub fn slice(&self, off: u32, len: u32) -> BufSlice {
        assert!(
            off as u64 + len as u64 <= self.0.len as u64,
            "不变量破坏：BufSlice 越界（{off}+{len} > {}）",
            self.0.len
        );
        BufSlice { buf: self.clone(), off, len }
    }
    pub fn whole(&self) -> BufSlice {
        self.slice(0, self.0.len)
    }
}

/// 一个 FrozenBuf 的子区间：客户端一个 chunk 的脏数据在一个 Buf 里，只发其中几段 extent 时就用几个 BufSlice
#[derive(Clone)]
pub struct BufSlice {
    pub buf: FrozenBuf,
    pub off: u32,
    pub len: u32,
}

impl BufSlice {
    pub fn as_slice(&self) -> &[u8] {
        &self.buf.as_slice()[self.off as usize..(self.off + self.len) as usize]
    }
    /// addr = buf.addr() + off
    pub fn addr(&self) -> u64 {
        self.buf.addr() + self.off as u64
    }
}

impl Drop for Buf {
    fn drop(&mut self) {
        // 槽位放回它原来的空闲表；notify_all 而不是 notify_one：等待者里可能同时有 Normal 与 Repl，
        // 只叫醒一个的话可能叫醒了用不了这张表的那个，另一个就错过了唤醒。
        let c = &self.pool.classes[self.class as usize];
        let mut fl = c.lock();
        match self.lane {
            Lane::Normal => fl.normal.push(self.slot),
            Lane::Repl => fl.repl.push(self.slot),
        }
        c.update_gauges(&fl);
        drop(fl);
        c.cv.notify_all();
    }
}

/// core/buf：给传输插件注册内存（3.12）
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RegionInfo {
    pub base: u64,
    pub len: u64,
}

#[cfg(test)]
mod tests;
