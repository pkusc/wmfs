//! 客户端 ↔ 数据：`DataCall` / `DataOp` / `DataReply`（design.md 3.3.5，msg_type 0x0200）。
//!
//! 数据节点校验 cap（全部本地 O(1)，**在 io 线程出队后执行**，不是入队时）：MAC 正确；
//! `expires_ms + 5 min ≥ wall_ms()`；`chunk.ino == cap.ino && chunk.epoch == cap.epoch`；权限位；
//! `chunk.chain == cap.chains[chunk.idx % cap.chains.len()]`（客户端不能把数据塞进 layout 之外的链）；
//! `cap.session` 不在集群图的被驱逐列表里。这就是数据请求的 authz。

use serde::{Deserialize, Serialize};

use crate::base::types::ChunkId;

/// 0x0200 的 body：`DataCall { cap: Vec<u8>, op: DataOp }` → `DataReply`（3.3.3）。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DataCall {
    pub cap: Vec<u8>,
    pub op: DataOp,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum DataOp {
    ReadChunk { chunk: ChunkId, chain_ver: u64, off: u32, len: u32 }, // 响应 bulk = 明文
    WriteChunk { chunk: ChunkId, chain_ver: u64, extents: Vec<(u32, u32)>, sync: bool }, // (chunk 内偏移, 长度)；请求 bulk = 各段依次拼接（发送方用 BufSlice 列表，不拷贝，3.6.2）
    TruncateChunk { chunk: ChunkId, chain_ver: u64, new_len: u32 }, // 写一个新版本：new_len 之后全部清零（0 = 空版本）
    SyncChunks { chain: u32, chain_ver: u64, chunks: Vec<ChunkId> }, // 链上每个成员 fdatasync
    DeleteEpoch { chain: u32, chain_ver: u64, ino: u64, epoch: u32 }, // 需要 cap 的管理权限位（只签发给后台任务）
}

impl DataOp {
    /// 变体名：指标里的 `op` 标签（3.3.3）。
    pub fn name(&self) -> &'static str {
        match self {
            DataOp::ReadChunk { .. } => "ReadChunk",
            DataOp::WriteChunk { .. } => "WriteChunk",
            DataOp::TruncateChunk { .. } => "TruncateChunk",
            DataOp::SyncChunks { .. } => "SyncChunks",
            DataOp::DeleteEpoch { .. } => "DeleteEpoch",
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum DataReply {
    Read { len: u32, version: u64 },
    Written { version: u64 },
    Ok,
    Deleted { files: u64 },
}
