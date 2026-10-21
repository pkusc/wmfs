//! `DataClient`：FUSE 层、CLI、后台任务用；选副本、CHUNK_DIRTY 重定向、换副本、按 3.3.8 重试（design.md 3.12）。
//!
//! 接口骨架（M0）：签名以这里为准，实现在 M5。

use std::sync::Arc;

use crate::base::ErrCode;
use crate::base::types::ChunkId;
use crate::buf::{Buf, BufSlice};
use crate::meta::schema::records::LayoutV1;
use crate::net::client::RpcClient;
use crate::proto::peer::ClusterMap;

pub struct DataClient {/* RpcClient、取集群图的回调、是否 background */}

#[allow(unused_variables, clippy::too_many_arguments)] // 骨架：函数体在 M5 实现，实现后删除 unused_variables
impl DataClient {
    pub fn new(
        rpc: Arc<RpcClient>,
        map: Arc<dyn Fn() -> Arc<ClusterMap> + Send + Sync>,
        background: bool,
    ) -> DataClient {
        unimplemented!("M5：DataClient::new")
    }
    /// 返回 (缓冲, 实际长度)
    pub fn read(
        &self,
        cap: &[u8],
        layout: &LayoutV1,
        chunk: ChunkId,
        off: u32,
        len: u32,
        into: Buf,
        prefer_node: Option<u32>,
    ) -> Result<(Buf, u32 /*实际长度*/), ErrCode> {
        unimplemented!("M5：DataClient::read")
    }
    /// 返回 version
    pub fn write(
        &self,
        cap: &[u8],
        chunk: ChunkId,
        extents: Vec<(u32, u32)>,
        data: Vec<BufSlice>,
        sync: bool,
    ) -> Result<u64 /*version*/, ErrCode> {
        unimplemented!("M5：DataClient::write")
    }
    pub fn truncate(&self, cap: &[u8], chunk: ChunkId, new_len: u32) -> Result<u64, ErrCode> {
        unimplemented!("M5：DataClient::truncate")
    }
    pub fn sync(&self, cap: &[u8], chain: u32, chunks: Vec<ChunkId>) -> Result<(), ErrCode> {
        unimplemented!("M5：DataClient::sync")
    }
    /// 只给 gc（管理权限 cap）
    pub fn delete_epoch(&self, cap: &[u8], chain: u32, ino: u64, epoch: u32) -> Result<u64, ErrCode> {
        unimplemented!("M5：DataClient::delete_epoch")
    }
}
