//! 调用方拿到的两个句柄（design.md 3.12）：一次调用的 [`PendingCall`] 与流的 [`StreamRx`]。从 `client.rs` 拆出来（每个文件 ≤ 400 行）。

use std::time::Duration;

use crossbeam_channel::{Receiver, RecvTimeoutError};

use super::check_reply;
use crate::base::ErrCode;
use crate::proto::frame::{FLAG_LAST_CHUNK, FrameKind, InFrame};

pub struct PendingCall {
    pub(super) rx: Receiver<Result<InFrame, ErrCode>>,
}

impl PendingCall {
    pub fn wait(self) -> Result<InFrame, ErrCode> {
        self.rx.recv().unwrap_or(Err(ErrCode::ECONNRESET))
    }

    /// 最多等 `d`：还没有结果 → None（调用照常在途，可以接着等）。实现注记（M15，只增）：MetaClient 等回复时隔一会儿看一眼，别的调用是不是
    /// 已经认定这个节点卡住、换走了（`client/meta_client/peers.rs`）。
    pub fn wait_timeout(&self, d: Duration) -> Option<Result<InFrame, ErrCode>> {
        match self.rx.recv_timeout(d) {
            Ok(r) => Some(r),
            Err(RecvTimeoutError::Timeout) => None,
            Err(RecvTimeoutError::Disconnected) => Some(Err(ErrCode::ECONNRESET)),
        }
    }
}

pub struct StreamRx {
    pub(super) rx: Receiver<Result<InFrame, ErrCode>>,
    pub(super) done: bool,
}

impl StreamRx {
    /// None = 已收到 LAST_CHUNK。ERROR 帧 → 它的错误码；超时（相邻两帧之间）→ ETIMEDOUT；连接断开 → ECONNRESET。
    #[allow(clippy::should_implement_trait)] // 名字按 design.md 3.12；它返回 Result，不是 Iterator
    pub fn next(&mut self) -> Result<Option<InFrame>, ErrCode> {
        if self.done {
            return Ok(None);
        }
        let f = self.rx.recv().unwrap_or(Err(ErrCode::ECONNRESET)).and_then(check_reply);
        let more = matches!(&f, Ok(f) if f.hdr.kind == FrameKind::StreamChunk && f.hdr.flags & FLAG_LAST_CHUNK == 0);
        self.done = !more;
        f.map(Some)
    }
}
