//! 预注册内存池与缓冲区所有权（design.md 3.6.1、3.6.2、3.12 RegionInfo，ADR-0022）。
//!
//! 接口骨架（M0）：类型与签名以这里为准，实现在 M1（mmap、切片、空闲表、Condvar 等待）。
//! 规则：`Buf` 不实现 Clone——按值传递 = 所有权转移，Drop = 归还池；`freeze()` 之后只读、可共享；
//! `BufSlice` 是一个 FrozenBuf 的子区间（只发几段 extent 时不用拷贝）。`Buf` 的字段里不存裸指针
//! （只存池引用与槽位号），所以它自动是 `Send + Sync`，不需要 `unsafe impl`；`as_slice` 内部的
//! `slice::from_raw_parts` 附 SAFETY 说明：一个 `Buf` 独占其槽位，`FrozenBuf` 只给出 `&[u8]`。
//! 本文件是允许写 `unsafe` 的文件之一（mmap、切片）。

use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::base::ErrCode;

/// 三档槽位的大小：S = 128 KiB；M = 4 MiB + 128 KiB；L = 16 MiB + 128 KiB（多出的 128 KiB 放头部与块表）。
pub const S_SLOT: usize = 128 << 10;
pub const M_SLOT: usize = (4 << 20) + (128 << 10);
pub const L_SLOT: usize = (16 << 20) + (128 << 10);

/// 启动时一次 mmap(MAP_PRIVATE|MAP_ANONYMOUS|MAP_POPULATE) 一整块内存，切成三档；永不归还给操作系统。
#[derive(Clone)]
pub struct MemPool {
    #[allow(dead_code)] // 骨架：M1 实现后删除
    inner: Arc<PoolInner>, // Clone 只增引用计数
}

#[allow(dead_code)] // 骨架：M1 实现后删除
struct PoolInner {
    base: usize, // SAFETY: 只在本模块内转成指针
    len: usize,
    classes: [SizeClass; 3], // S = 128 KiB；M = 4 MiB + 128 KiB；L = 16 MiB + 128 KiB
}

/// 一档槽位。两张空闲表放在**同一把锁**里，`cv` 只配这一把锁（实现注记 M0，design.md 3.6.1）：
/// 如果两张表各有一把锁，`Lane::Repl`（先取 Repl 槽、再取 Normal 槽）等待时只能拿其中一把，
/// 要么丢失另一张表上的唤醒，要么嵌套持锁；std 的 Condvar 配多把锁还可能 panic。
#[allow(dead_code)] // 骨架：M1 实现后删除
struct SizeClass {
    slot_size: usize,
    offset: usize,
    free: Mutex<FreeSlots>,
    cv: Condvar,
}

/// 一档的空闲槽位号。
#[allow(dead_code)] // 骨架：M1 实现后删除
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
#[allow(dead_code)] // 骨架：M1 实现后删除
pub struct Buf {
    pool: Arc<PoolInner>,
    class: u8,
    slot: u32,
    lane: Lane,
    len: u32,
}

/// 冻结后的只读缓冲：Clone 只是引用计数 +1；最后一个持有者 Drop 时归还池。
#[derive(Clone)]
pub struct FrozenBuf(#[allow(dead_code)] Arc<Buf>); // 骨架：M1 实现后删除 allow

#[allow(unused_variables)] // 骨架：函数体在 M1 实现，实现后删除这一行
impl MemPool {
    pub fn new(cfg: &PoolConfig) -> Result<MemPool, ErrCode> {
        unimplemented!("M1：MemPool::new")
    }
    pub fn get(&self, min_len: usize, lane: Lane, wait: Duration) -> Result<Buf, ErrCode> {
        unimplemented!("M1：MemPool::get")
    }
    /// 给传输插件注册
    pub fn region(&self) -> RegionInfo {
        unimplemented!("M1：MemPool::region")
    }
}

#[allow(unused_variables, clippy::len_without_is_empty)] // 骨架：函数体在 M1 实现，实现后删除 unused_variables
impl Buf {
    pub fn as_slice(&self) -> &[u8] {
        unimplemented!("M1：Buf::as_slice")
    }
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unimplemented!("M1：Buf::as_mut_slice")
    }
    pub fn len(&self) -> usize {
        unimplemented!("M1：Buf::len")
    }
    /// n <= capacity
    pub fn set_len(&mut self, n: usize) {
        unimplemented!("M1：Buf::set_len")
    }
    pub fn capacity(&self) -> usize {
        unimplemented!("M1：Buf::capacity")
    }
    /// RDMA 用：池内绝对地址
    pub fn addr(&self) -> u64 {
        unimplemented!("M1：Buf::addr")
    }
    /// 显式转移：从此只读、可共享
    pub fn freeze(self) -> FrozenBuf {
        unimplemented!("M1：Buf::freeze")
    }
}

#[allow(unused_variables)] // 骨架：函数体在 M1 实现，实现后删除这一行
impl FrozenBuf {
    pub fn as_slice(&self) -> &[u8] {
        unimplemented!("M1：FrozenBuf::as_slice")
    }
    pub fn addr(&self) -> u64 {
        unimplemented!("M1：FrozenBuf::addr")
    }
    /// 引用计数为 1 时取回可写所有权
    pub fn try_unfreeze(self) -> Result<Buf, FrozenBuf> {
        unimplemented!("M1：FrozenBuf::try_unfreeze")
    }
    /// 子区间视图（引用计数 +1，不拷贝）；越界 = 不变量破坏
    pub fn slice(&self, off: u32, len: u32) -> BufSlice {
        unimplemented!("M1：FrozenBuf::slice")
    }
    pub fn whole(&self) -> BufSlice {
        unimplemented!("M1：FrozenBuf::whole")
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
        unimplemented!("M1：BufSlice::as_slice")
    }
    /// addr = buf.addr() + off
    pub fn addr(&self) -> u64 {
        unimplemented!("M1：BufSlice::addr")
    }
}

impl Drop for Buf {
    fn drop(&mut self) {
        // 槽位放回对应 lane 的空闲表，notify_one（M1）
    }
}

/// core/buf：给传输插件注册内存（3.12）
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RegionInfo {
    pub base: u64,
    pub len: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preset_sizes_match_design() {
        // design.md 3.6.1：服务端 ≈ 1 GiB、客户端 ≈ 768 MiB、dev ≈ 384 MiB、CLI ≈ 70 MiB
        let mib = |c: PoolConfig| c.total_bytes() >> 20;
        assert!((1000..=1030).contains(&mib(PoolConfig::server())), "{}", mib(PoolConfig::server()));
        assert!((760..=775).contains(&mib(PoolConfig::client())));
        assert!((380..=390).contains(&mib(PoolConfig::dev_server())));
        assert!((65..=72).contains(&mib(PoolConfig::small())));
        assert!(PoolConfig::server().repl_lane && !PoolConfig::client().repl_lane);
    }
}
