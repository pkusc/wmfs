//! `MetaClient`：FUSE 层与 CLI 用；跟随 leader、seq、按 3.3.8 重试（design.md 3.12、3.5.2）。
//!
//! 修改型请求分配单调递增的 seq，重试必须用同一个 seq；同时在途的修改型请求 ≤ 256（DEDUP_WINDOW）。
//!
//! 接口骨架（M0）：签名以这里为准，实现在 M4。

use std::sync::Arc;

use crate::base::ErrCode;
use crate::net::client::{Endpoint, RpcClient};
use crate::proto::meta::{MetaOp, MetaReply};
use crate::proto::peer::ClusterMap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Caller {
    pub uid: u32,
    pub gid: u32,
    pub groups: Vec<u32>,
    pub pid: u32,
    pub job_id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionInfo {
    pub client_name: String,
    pub host: String,
    pub host_id: String,
    pub mount: String,
}

pub struct MetaClient {/* RpcClient、leader 提示、会话 id、seq 计数器、在途窗口 ≤ 256、集群图 */}

#[allow(unused_variables)] // 骨架：函数体在 M4 实现，实现后删除这一行
impl MetaClient {
    /// GetClusterMap + SessionOpen
    pub fn connect(rpc: Arc<RpcClient>, seeds: &[Endpoint], info: SessionInfo) -> Result<MetaClient, ErrCode> {
        unimplemented!("M4：MetaClient::connect")
    }
    /// 只读请求（seq = 0）
    pub fn read(&self, who: &Caller, op: MetaOp) -> Result<MetaReply, ErrCode> {
        unimplemented!("M4：MetaClient::read")
    }
    /// 修改型：分配 seq，重试用同一 seq
    pub fn write(&self, who: &Caller, op: MetaOp) -> Result<MetaReply, ErrCode> {
        unimplemented!("M4：MetaClient::write")
    }
    pub fn heartbeat(
        &self,
        opened: Vec<(u64, bool)>,
        closed: Vec<u64>,
        full: Option<Vec<(u64, bool)>>,
    ) -> Result<MetaReply, ErrCode> {
        unimplemented!("M4：MetaClient::heartbeat")
    }
    /// 当前集群图
    pub fn map(&self) -> Arc<ClusterMap> {
        unimplemented!("M4：MetaClient::map")
    }
    /// 心跳 map_epoch 变大、或 STALE_CHAIN/NOT_SERVING 时
    pub fn refresh_map(&self) -> Result<Arc<ClusterMap>, ErrCode> {
        unimplemented!("M4：MetaClient::refresh_map")
    }
    /// SessionClose
    pub fn close(&self) {
        unimplemented!("M4：MetaClient::close")
    }
}
