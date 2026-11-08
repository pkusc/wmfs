//! 服务端对一个请求的回复句柄 [`Responder`]（design.md 3.12、3.3.3）：按值传递、恰好用一次；回复只是 `try_send` 进连接的发送队列
//! （队列满就断开这条连接，对端会重试），所以回复永远不会阻塞执行器或 io 线程。被 Drop 而没有回复时自动回 `EIO` 并计数
//! `wmfs_req_dropped_total`。回复超过协议上限时改回 `FEATURE_UNSUPPORTED`（只让这一个请求失败）。
//! 实现注记（M4）：回 `NOT_LEADER` 的 ERROR 帧由 core 自动填 `leader_hint`；[`Responder::drop_reply`] 什么也不回、Drop 时也不自动回 EIO
//! （fail point `meta.reply.drop` 用它"弄丢"一次回复）。实现注记（M8）：[`Responder::error_with`] 带一句给人看的原因（`ErrorBody.msg`）。
//! 实现注记（M13 收尾，ADR-0044 第 5 条）：[`Responder::on_done`] 登记"回复成功发出之后"的回调（时长从收到请求算起）。
//! 实现注记（M14，3.11）：每个回复（含被丢弃时自动回的 EIO）都记一个 span（`obs::trace`）；服务解出请求之后用 [`Responder::set_op`] 记下
//! 操作名，转发给下一跳时用 [`Responder::trace`] 带上 trace_id（span_id + 1）。
//! 实现注记（M14 收尾，ADR-0045 第 9 条）：回复句柄带着这个请求的各阶段耗时（`obs::stages`，起点同上）：服务与 io 线程走完一个阶段就
//! [`Responder::mark`]；元数据的修改型请求要进执行器（执行器拿不到回复句柄），用 [`Responder::take_stages`] 交出、随 `TxnRequest` 带过去，
//! 回来时 [`Responder::set_stages`] 交回。超过慢请求阈值的回复把它写进慢日志。从 `server.rs` 拆出来（那个文件接近 400 行）。

use std::sync::Arc;
use std::time::{Duration, Instant};

use super::HintFn;
use super::stats::FamilyStats;
use super::stream::StreamTx;
use crate::base::{ErrCode, clock};
use crate::buf::BufSlice;
use crate::hooks::traits::ConnIdentity;
use crate::net::conn::{self, ConnHandle};
use crate::obs::stages::{Stage, Stages};
use crate::obs::trace::{self, Span};
use crate::proto::encode_body;
use crate::proto::frame::{ErrorBody, FLAG_ERROR, FrameHeader, FrameKind, OutFrame};

/// 回复成功发出之后调一次，参数是从收到请求到回复发出的时长（数据节点按存储类统计延迟，ADR-0044 第 5 条）。
pub type DoneFn = Box<dyn FnOnce(Duration) + Send>;

/// 服务端对一个请求的回复句柄：按值传递，恰好用一次
pub struct Responder {
    conn: ConnHandle,
    req_id: u64,
    msg_type: u16,
    trace_id: u64,
    span_id: u16,
    done: bool,
    stats: Arc<FamilyStats>, // 指标句柄（第一次用到这个族时注册好的）
    t0: Instant,
    hint: Option<HintFn>,
    on_done: Option<DoneFn>,
    node: u32,        // 本节点号（span 用，M14）
    op: &'static str, // 操作名（服务解出请求之后 set_op；没设就是消息族名）
    stages: Stages,   // 各阶段耗时（慢日志用，ADR-0045 第 9 条）
}

impl Responder {
    pub(super) fn new(
        conn: ConnHandle,
        req: &FrameHeader,
        stats: Arc<FamilyStats>,
        hint: Option<HintFn>,
        node: u32,
    ) -> Responder {
        let (req_id, msg_type, trace_id, span_id) = (req.req_id, req.msg_type, req.trace_id, req.span_id);
        let (done, t0, on_done, op) = (false, Instant::now(), None, stats.family);
        let stages = Stages::new(t0);
        Responder { conn, req_id, msg_type, trace_id, span_id, done, stats, t0, hint, on_done, node, op, stages }
    }
    /// 走完阶段 `s`（见文件头）。
    pub fn mark(&self, s: Stage) {
        self.stages.mark(s);
    }
    /// 交出各阶段耗时（带进执行器，见文件头）；之后用 [`Responder::set_stages`] 交回。
    pub fn take_stages(&mut self) -> Stages {
        std::mem::replace(&mut self.stages, Stages::new(self.t0))
    }
    pub fn set_stages(&mut self, s: Stages) {
        self.stages = s;
    }
    /// 解出请求之后记下操作名（span 与慢日志里显示，M14）。
    pub fn set_op(&mut self, op: &'static str) {
        self.op = op;
    }
    /// 这个请求的 (trace_id, span_id)：转发给下一跳时带上（span_id + 1，3.11）。
    pub fn trace(&self) -> (u64, u16) {
        (self.trace_id, self.span_id)
    }
    /// 登记回复成功发出之后要调的 [`DoneFn`]（回 ERROR 帧、被 Drop 而没有回复时不调）。
    pub fn on_done(&mut self, f: DoneFn) {
        self.on_done = Some(f);
    }
    /// 编成 Response 帧，ConnHandle::send（try_send，不阻塞）
    pub fn reply(mut self, body: Vec<u8>, bulk: Vec<BufSlice>) {
        let f = OutFrame { hdr: self.header(), body, bulk, recv_into: None };
        self.finish(f, ErrCode(0));
    }
    /// ERROR 帧；leader_hint 由 core 自动填（`NOT_LEADER` 时问 [`RpcServer::set_leader_hint`] 登记的函数）
    pub fn error(self, code: ErrCode, extra: u64) {
        self.error_body(ErrorBody::new(code, extra));
    }
    /// ERROR 帧，带一句给人看的原因（`ErrorBody.msg`；默认是错误码名）；leader_hint 与 [`Responder::error`] 一样自动填。
    pub fn error_with(self, code: ErrCode, msg: String) {
        self.error_body(ErrorBody { msg, ..ErrorBody::new(code, 0) });
    }
    fn error_body(mut self, mut body: ErrorBody) {
        let code = ErrCode(body.code);
        body.leader_hint = self.hint.as_ref().filter(|_| code == ErrCode::NOT_LEADER).and_then(|f| f());
        let f = error_frame(self.header(), body);
        self.finish(f, code);
    }
    /// 什么也不回（Drop 时也不自动回 EIO）：对端等到超时后用同一个 seq 重试。只给 fail point `meta.reply.drop` 用。
    pub fn drop_reply(mut self) {
        self.done = true;
    }
    pub fn identity(&self) -> &ConnIdentity {
        self.conn.identity()
    }
    /// 改成流式回复（SnapshotFetch，3.4.1.7）：之后用 [`StreamTx`] 发若干 StreamChunk 帧，最后一帧带 LAST_CHUNK。
    /// 这个请求在流结束时（最后一帧、`StreamTx::error`，或 StreamTx 被丢弃）才计入指标。
    pub fn into_stream(mut self) -> StreamTx {
        self.done = true;
        StreamTx::new(self.conn.clone(), self.header(), self.stats.clone(), self.t0)
    }

    fn header(&self) -> FrameHeader {
        let mut h = FrameHeader::new(FrameKind::Response, self.msg_type, self.req_id, self.trace_id);
        h.span_id = self.span_id;
        h
    }

    fn finish(&mut self, f: OutFrame, code: ErrCode) {
        self.done = true;
        let code = match self.conn.send(f) {
            // 回复超过协议上限（body > 1 MiB，或 bulk > 16 MiB + 128 KiB）：改回一个 ERROR 帧，只让这一个请求失败（3.3.8）
            Err(ErrCode::FEATURE_UNSUPPORTED) => {
                let _ = self.conn.send(error_frame(self.header(), ErrorBody::new(ErrCode::FEATURE_UNSUPPORTED, 0)));
                ErrCode::FEATURE_UNSUPPORTED
            }
            _ => code, // 其他失败 = 连接已断，对端会重连重试
        };
        self.stats.record(code, self.t0);
        let d = self.t0.elapsed();
        trace::record(
            Span {
                trace_id: self.trace_id,
                span_id: self.span_id,
                node: self.node,
                component: self.stats.family,
                op: self.op,
                start_ms: clock::wall_ms().saturating_sub(d.as_millis() as u64),
                dur_us: d.as_micros() as u64,
                code,
            },
            &self.stages,
        );
        if let Some(f) = self.on_done.take().filter(|_| code == ErrCode(0)) {
            f(d);
        }
    }
}

impl Drop for Responder {
    fn drop(&mut self) {
        if !self.done {
            // 没回复过 → 回 EIO，计数 wmfs_req_dropped_total
            conn::counters().req_dropped.inc();
            let f = error_frame(self.header(), ErrorBody::new(ErrCode::EIO, 0));
            self.finish(f, ErrCode::EIO);
        }
    }
}

/// 一个 ERROR 帧（flags 带 ERROR，body 是 ErrorBody）；`hdr` 是这个请求的回复帧头。
pub(super) fn error_frame(mut hdr: FrameHeader, body: ErrorBody) -> OutFrame {
    (hdr.kind, hdr.flags) = (FrameKind::Response, FLAG_ERROR);
    OutFrame { hdr, body: encode_body(&body), bulk: vec![], recv_into: None }
}
