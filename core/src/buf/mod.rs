//! 缓冲区与内存模型（design.md 3.6，ADR-0022）：预注册内存池、零拷贝所有权、复制专用 lane。

pub mod pool;

pub use pool::{Buf, BufSlice, ClassStats, FrozenBuf, Lane, MemPool, PoolConfig, RegionInfo};
