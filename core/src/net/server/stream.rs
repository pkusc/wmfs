//! 流式回复的发送端 [`StreamTx`]（`Responder::into_stream`，design.md 3.3.3；SnapshotFetch 用，3.4.1.7）。
//! 实现注记（M8）：从 `server.rs` 拆出来（那个文件超过了 400 行）；加了 [`StreamTx::wait_room`]。

use std::sync::Arc;
use std::time::Instant;

use super::{FamilyStats, error_frame};
use crate::base::ErrCode;
use crate::buf::BufSlice;
use crate::net::conn::{self, ConnHandle};
use crate::proto::frame::{ErrorBody, FLAG_LAST_CHUNK, FrameHeader, FrameKind, OutFrame};

/// 流式回复的发送端（`Responder::into_stream`）。没发最后一帧就被丢弃时自动回 `EIO`（与 Responder 相同，3.6.3），
/// 客户端的这个流立即结束，不用等"相邻两帧之间"的超时（SnapshotFetch 是 10 min）。
pub struct StreamTx {
    conn: ConnHandle,
    hdr: FrameHeader,
    stats: Arc<FamilyStats>,
    t0: Instant,
    finished: bool, // 已经发了最后一帧、ERROR 帧，或连接已断
}

impl StreamTx {
    pub(super) fn new(conn: ConnHandle, hdr: FrameHeader, stats: Arc<FamilyStats>, t0: Instant) -> StreamTx {
        StreamTx { conn, hdr, stats, t0, finished: false }
    }

    /// 发一个 StreamChunk 帧；`last = true` 时带 LAST_CHUNK（流结束）。连接已断 → `ECONNRESET`（流到此为止）；
    /// 这一帧超过协议上限 → `FEATURE_UNSUPPORTED`（没发出去，流还在：可以改发小一点的，或用 [`StreamTx::error`] 结束）。
    pub fn send(&mut self, body: Vec<u8>, bulk: Vec<BufSlice>, last: bool) -> Result<(), ErrCode> {
        let mut hdr = self.hdr;
        (hdr.kind, hdr.flags) = (FrameKind::StreamChunk, if last { FLAG_LAST_CHUNK } else { 0 });
        let r = self.conn.send(OutFrame { hdr, body, bulk, recv_into: None });
        match r {
            Ok(()) if last => self.end(ErrCode(0)),
            Err(e) if e != ErrCode::FEATURE_UNSUPPORTED => self.end(e),
            _ => {}
        }
        r
    }

    /// 等这条连接的发送队列里最多还有 `max` 帧（流的发送方先等它，再发下一帧）；连接断了 → `ECONNRESET`。
    pub fn wait_room(&self, max: usize) -> Result<(), ErrCode> {
        while self.conn.queued() > max {
            if self.conn.is_closed() {
                return Err(ErrCode::ECONNRESET);
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        Ok(())
    }

    /// 以错误码结束这个流：发一个 ERROR 帧，客户端的 `StreamRx::next` 返回这个错误。
    pub fn error(mut self, code: ErrCode) {
        self.fail(code);
    }

    fn fail(&mut self, code: ErrCode) {
        let _ = self.conn.send(error_frame(self.hdr, ErrorBody::new(code, 0)));
        self.end(code);
    }

    fn end(&mut self, code: ErrCode) {
        self.finished = true;
        self.stats.record(code, self.t0);
    }
}

impl Drop for StreamTx {
    fn drop(&mut self) {
        if !self.finished {
            conn::counters().req_dropped.inc(); // 没发最后一帧就被丢弃 → 回 EIO（客户端的流立即结束）
            self.fail(ErrCode::EIO);
        }
    }
}
