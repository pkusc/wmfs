//! `RpcClient`：到多个对端的连接管理；每个进程一个实例（design.md 3.12、3.3.3 传输选择、3.3.8 超时）。
//!
//! 控制族（MetaCall、NodeCall、RaftMsg、SnapshotFetch）永远走 `control_scheme`；数据族（DataCall、PeerCall）走
//! `data_scheme`，前提是对端在 `NodeRecV1.addrs` 里公布了这个 scheme（客户端：没有公布 → 报错，不静默降级；
//! 服务端之间：退回 control_scheme 并告警）。core 里只有 scheme 字符串比较，没有任何传输名字的特殊分支。
//!
//! 接口骨架（M0）：签名以这里为准，实现在 M1。

use std::sync::Arc;
use std::time::Duration;

use wmfs_api::types::KeyFile;

use crate::base::ErrCode;
use crate::base::config::NetConfig;
use crate::buf::MemPool;
use crate::hooks::traits::Transport;
use crate::proto::frame::{InFrame, OutFrame};

/// 消息族：按族选 scheme（3.3.3）。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Family {
    Control,
    Data,
}

/// 来自集群图 NodeRecV1.addrs；CLI 也可以手写一个
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Endpoint {
    pub node: u32,
    pub addrs: Vec<(String, String)>,
}

pub struct RpcClient {/* 连接表（每个对端每个 scheme 最多 1 条）、在途表、rpc-timer 线程 */}

#[allow(unused_variables)] // 骨架：函数体在 M1 实现，实现后删除这一行
impl RpcClient {
    pub fn new(
        pool: MemPool,
        transports: Vec<Arc<dyn Transport>>,
        key: KeyFile,
        net: NetConfig,
    ) -> Result<Arc<RpcClient>, ErrCode> {
        unimplemented!("M1：RpcClient::new")
    }
    /// 阻塞：发出请求、等回复。只允许在可以阻塞的线程里用（1.2 规则 3）。不重试（重试在 MetaClient/DataClient/retry.rs）
    pub fn call(&self, to: &Endpoint, fam: Family, f: OutFrame, timeout: Duration) -> Result<InFrame, ErrCode> {
        unimplemented!("M1：RpcClient::call")
    }
    /// 发出请求、立即返回；之后用 PendingCall::wait 等（kv bench 64 路并发等）
    pub fn start(&self, to: &Endpoint, fam: Family, f: OutFrame, timeout: Duration) -> Result<PendingCall, ErrCode> {
        unimplemented!("M1：RpcClient::start")
    }
    /// 回调式：回复、错误或超时时调用 cb，恰好一次。cb 在连接读线程或 rpc-timer 线程上执行，只允许 try_send 到某个队列，不得阻塞
    pub fn call_cb(
        &self,
        to: &Endpoint,
        fam: Family,
        f: OutFrame,
        timeout: Duration,
        cb: Box<dyn FnOnce(Result<InFrame, ErrCode>) + Send>,
    ) {
        unimplemented!("M1：RpcClient::call_cb")
    }
    /// 流式（SnapshotFetch）：timeout 是相邻两帧之间的上限
    pub fn stream(&self, to: &Endpoint, f: OutFrame, timeout: Duration) -> Result<StreamRx, ErrCode> {
        unimplemented!("M1：RpcClient::stream")
    }
}

pub struct PendingCall {/* bounded(1) 的接收端 */}

impl PendingCall {
    pub fn wait(self) -> Result<InFrame, ErrCode> {
        unimplemented!("M1：PendingCall::wait")
    }
}

pub struct StreamRx {/* … */}

impl StreamRx {
    /// None = 已收到 LAST_CHUNK
    #[allow(clippy::should_implement_trait)] // 名字按 design.md 3.12；它返回 Result，不是 Iterator
    pub fn next(&mut self) -> Result<Option<InFrame>, ErrCode> {
        unimplemented!("M8：StreamRx::next")
    }
}
// 超时：rpc-timer 线程每 100 ms 扫描在途表，过期的以 Err(ETIMEDOUT) 结束（唤醒 call/wait，或调用 cb）
// 连接断开：该连接上的在途调用全部以 Err(ECONNRESET) 结束
