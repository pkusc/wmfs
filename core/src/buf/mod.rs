//! 缓冲区与内存模型（design.md 3.6，ADR-0022）：预注册内存池、零拷贝所有权、复制专用 lane。

pub mod pool;

pub use pool::{Buf, BufSlice, ClassStats, FrozenBuf, Lane, MemPool, PoolConfig, RegionInfo};

/// 服务端（数据节点、元数据服务）从池里取一个缓冲最多等多久：取不到回 `BUSY`，客户端退避重试（3.6.1）。
/// 实现注记（M5）：新增（只增），数据面与服务端 uninline 共用这一个值。
pub const POOL_WAIT: std::time::Duration = std::time::Duration::from_millis(100);
