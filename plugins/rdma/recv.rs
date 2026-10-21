//! RdmaConn 收的一半（只在 core 的读线程上，`Conn::recv`）：等一个 RECV 完成 → 解析帧头 → 把 body 拷出来 → **马上**把
//! 接收槽还给网卡 → 按状态机检查 → 请求帧带 bulk 时校验描述符表、按顺序 RDMA READ 进**一个**池缓冲；回复帧的 bulk 已经被
//! 对端 WRITE 进了在途表里的 `recv_into`，只 `set_len`（design.md 3.6.4；实现注记（M2））。
//!
//! - 判死：15 s 收不到任何帧 → `ETIMEDOUT`；引导 TCP 读到 EOF（或读到不该有的字节）→ `ECONNRESET`（每秒看一眼）。
//! - 状态机（[`frame_allowed`]）：握手完成（`mark_authenticated`）之前收到声明了 bulk 或带描述符的帧 → 断开，不发 READ、
//!   不取池槽（3.3.2）；流帧与 OneWay 不走 rdma（3.3.3）→ 断开；描述符只会出现在发给 accept 侧的请求里。
//! - 读 bulk 前先校验描述符（desc.rs 的 `check_bulk`：1..=16 段、每段 > 0、Σlen == bulk_len），不对就断开——否则一个
//!   已认证的对端能让 READ 写穿相邻的池槽。池里 100 ms 取不到缓冲时不发 READ，交出 `bulk_dropped`（core 回 BUSY，对端收到
//!   这个回复才放下它的 BufSlice）。

use std::io::ErrorKind;
use std::sync::mpsc::RecvTimeoutError;
use std::time::Duration;

use wmfs_core::base::{ErrCode, clock, log};
use wmfs_core::buf::Buf;
use wmfs_core::proto::frame::{
    FLAG_BULK_REMOTE, FLAG_REPLY_REMOTE, FrameHeader, FrameKind, HEADER_LEN, InFrame, bulk_buf,
};

use super::conn::{Held, RdmaConn, Waited};
use super::desc::{Desc, check_bulk, decode_descs};
use super::lock;
use super::params::{BULK_WAIT_MS, MAX_FRAME, READ_IDLE_MS};
use super::wire::{Ev, Op};

const PROTO: ErrCode = ErrCode::PROTO_UNSUPPORTED;

fn frame(hdr: FrameHeader, body: Vec<u8>, bulk: Option<Buf>, dropped: bool) -> InFrame {
    InFrame { hdr, body, bulk, bulk_dropped: dropped }
}

/// 状态机与帧的种类：这一帧现在能收吗（不能 = 断开）。`authed` = 握手已完成；`accept_side` = 本端是接收请求的一方。
/// - 握手完成之前：不能声明 bulk，也不能带描述符（`BULK_REMOTE` / `REPLY_REMOTE`）；
/// - 请求：只有 accept 侧能收带 bulk 或描述符的（单边操作永远由接收请求的一方发起）；
/// - 回复：永远不带描述符（回复的 bulk 已被对端 WRITE 进 `recv_into`）；
/// - 流帧与 OneWay：不走 rdma（3.3.3）。
pub fn frame_allowed(h: &FrameHeader, authed: bool, accept_side: bool) -> bool {
    let remote = h.flags & (FLAG_BULK_REMOTE | FLAG_REPLY_REMOTE) != 0;
    let early = !authed && (h.bulk_len > 0 || remote);
    let kind_ok = match h.kind {
        FrameKind::Request => accept_side || (h.bulk_len == 0 && !remote),
        FrameKind::Response => !remote,
        FrameKind::OneWay | FrameKind::StreamChunk => false,
    };
    !early && kind_ok
}

impl RdmaConn {
    /// 等一个 RECV 完成。期间每秒看一眼：连接是否已关、引导 TCP 是否还在、是否已经 15 s 没有任何帧。
    pub fn wait_recv(&self) -> Result<Ev, ErrCode> {
        let rx = lock(&self.recvs);
        let deadline = clock::mono_ms() + READ_IDLE_MS;
        loop {
            let now = clock::mono_ms();
            if self.is_closed() {
                return Err(ErrCode::ECONNRESET);
            }
            if now >= deadline {
                log::info("rdma", format_args!("{}：15 s 没有收到任何帧，判对端已死", self.peer));
                return Err(ErrCode::ETIMEDOUT);
            }
            match rx.recv_timeout(Duration::from_millis((deadline - now).min(1000))) {
                Ok(ev) => return Ok(ev),
                Err(RecvTimeoutError::Timeout) if self.boot_alive() => {}
                Err(RecvTimeoutError::Timeout) => {
                    log::info("rdma", format_args!("{}：引导 TCP 连接断了，这条 RDMA 连接作废", self.peer));
                    return Err(ErrCode::ECONNRESET);
                }
                Err(RecvTimeoutError::Disconnected) => return Err(ErrCode::ECONNRESET),
            }
        }
    }

    /// 引导 TCP（非阻塞）还在吗：没有数据可读 = 还在；EOF、出错、或读到了不该有的字节 = 不在了。
    fn boot_alive(&self) -> bool {
        match self.boot.peek(&mut [0u8; 1]) {
            Err(e) => matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::Interrupted),
            Ok(_) => false,
        }
    }

    /// 一个 RECV 完成之后：解析这一帧、把 body 拷出来、马上把接收槽还给网卡，再按状态机处理。
    pub fn take_frame(&self, ev: Ev) -> Result<InFrame, ErrCode> {
        if !ev.ok {
            return Err(ErrCode::ECONNRESET); // 接收槽被 flush：QP 已出错（连接关闭或对端出错）
        }
        let (slot, n) = (ev.seq as usize, ev.len as usize);
        let data = match self.slots.get(slot) {
            Some(b) if n <= b.len() => &b.as_slice()[..n],
            _ => return Err(ErrCode::INTERNAL),
        };
        let hb: &[u8; HEADER_LEN] = data.get(..HEADER_LEN).and_then(|h| h.try_into().ok()).ok_or(PROTO)?;
        let hdr = FrameHeader::decode(hb)?;
        let end = HEADER_LEN + hdr.body_len as usize;
        if end > MAX_FRAME || end > n {
            return Err(PROTO);
        }
        let body = data[HEADER_LEN..end].to_vec();
        let (descs, reply) = decode_descs(hdr.flags, &data[end..])?;
        self.repost(slot)?; // 拷完马上还给网卡（信用控制靠这一点：对端的回复到达时这个槽一定已经重新 post 了）
        self.shared.stats.add(Op::Recv, 1);
        if !frame_allowed(&hdr, self.authed(), self.accept_side) {
            let (k, f, b, a) = (hdr.kind, hdr.flags, hdr.bulk_len, self.authed());
            log::warn(
                "rdma",
                format_args!("{}：不该有的帧（{k:?}，flags {f:#x}，bulk {b}，已认证 {a}），断开", self.peer),
            );
            return Err(PROTO);
        }
        match hdr.kind {
            FrameKind::Response => self.got_response(hdr, body),
            _ => self.got_request(hdr, body, descs, reply), // frame_allowed 已经排除了 OneWay 与 StreamChunk
        }
    }

    /// 回复帧：还一个信用；在途表里的缓冲随之放掉（对端回复了 = 已经读完我们的 bulk）。回复的 bulk 已经被对端 WRITE 进了
    /// `recv_into`，这里只 `set_len`——不能照 tcp 再调 `frame::bulk_buf`：它在 recv_into 不够大时会悄悄从池里换一个空缓冲。
    fn got_response(&self, hdr: FrameHeader, body: Vec<u8>) -> Result<InFrame, ErrCode> {
        self.credits.give();
        let held = lock(&self.inflight).remove(&hdr.req_id).unwrap_or_default();
        let n = hdr.bulk_len as usize;
        if n == 0 {
            return Ok(frame(hdr, body, None, false));
        }
        match held.into {
            Some(mut b) if b.capacity() >= n => {
                b.set_len(n);
                Ok(frame(hdr, body, Some(b), false))
            }
            _ => {
                log::warn("rdma", format_args!("{}：回复带 {n} 字节 bulk，却没有对应的 recv_into，断开", self.peer));
                Err(PROTO)
            }
        }
    }

    /// 请求帧（只在 accept 侧带描述符）：记下 REPLY_REMOTE；带 bulk 时校验描述符表、READ 进一个池缓冲。
    fn got_request(
        &self,
        hdr: FrameHeader,
        body: Vec<u8>,
        descs: Vec<Desc>,
        reply: Option<Desc>,
    ) -> Result<InFrame, ErrCode> {
        if let Some(d) = reply {
            lock(&self.replies).insert(hdr.req_id, d);
        }
        if hdr.bulk_len == 0 {
            return if descs.is_empty() { Ok(frame(hdr, body, None, false)) } else { Err(PROTO) };
        }
        if check_bulk(&descs, hdr.bulk_len).is_err() {
            log::warn("rdma", format_args!("{}：描述符表与 bulk_len {} 对不上，断开", self.peer, hdr.bulk_len));
            return Err(PROTO);
        }
        let Ok(buf) = bulk_buf(&self.pool, &hdr, None, Duration::from_millis(BULK_WAIT_MS)) else {
            return Ok(frame(hdr, body, None, true)); // 池满：不发 READ，core 回 BUSY
        };
        let buf = self.read_bulk(buf, &descs)?;
        Ok(frame(hdr, body, Some(buf), false))
    }

    /// 按描述符表依次 READ 进 `buf` 的相邻区间，等全部完成。等不到（15 s 或连接已关）→ `buf` 进墓地（网卡可能还在写它）。
    fn read_bulk(&self, buf: Buf, descs: &[Desc]) -> Result<Buf, ErrCode> {
        let seq = self.next_seq();
        let (mut off, mut posted) = (0u64, 0);
        let r = descs.iter().try_for_each(|d| {
            self.wire.post_read(seq, buf.addr() + off, d)?;
            (off, posted) = (off + d.len as u64, posted + 1);
            Ok(())
        });
        match (r, self.wait(&self.reads, seq, posted, clock::mono_ms() + READ_IDLE_MS)) {
            (Ok(()), Waited::AllOk) => {
                self.shared.stats.add(Op::Read, posted as u64);
                Ok(buf)
            }
            (r, Waited::Abandoned(e)) => {
                self.bury(Held::new(Vec::new(), Some(buf)));
                Err(r.err().unwrap_or(e))
            }
            (r, _) => {
                log::warn("rdma", format_args!("{}：RDMA READ 失败，断开", self.peer));
                Err(r.err().unwrap_or(ErrCode::ECONNRESET))
            }
        }
    }
}
