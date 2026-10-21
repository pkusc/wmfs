//! 一条 RDMA 连接（design.md 3.6.4、4.1）：一个 RC QP + 16 个接收槽 + 1 个发送暂存槽 + 引导 TCP，实现 core 的 `Conn`。
//! 收的一半（读线程）在 recv.rs。
//!
//! **发**（只在 core 的写线程上，`send`）：帧头 + body + 描述符表拷进发送暂存槽，SEND 出去并等它完成（最多
//! `[net] write_timeout_ms`）。
//! - 请求：先取一个信用（credit.rs：在途已有 16 个就一直等，直到有回复或连接关闭）；带 bulk 时附描述符表（`BULK_REMOTE`，
//!   每个长度不为 0 的 `BufSlice` 一段——长度为 0 的段 tcp 也什么都不写），期望回复带 bulk 时附 `recv_into` 的描述符
//!   （`REPLY_REMOTE`，len = 它的容量）；这些缓冲按 req_id 留在**在途表**里，回复到达或 QP 拆除之后才 Drop——core 的调用
//!   超时**不**放它们（对端可能还在读写）。
//! - 回复：带 bulk 时按请求给的 `REPLY_REMOTE` 依次 RDMA WRITE，再 SEND 回复帧（RC 保序：对端收到这一帧时数据已可见）。
//! - 只让这一个帧失败（返回 `FEATURE_UNSUPPORTED`，core 改发 ERROR 或让调用结束，不断开）：帧头 + body 超过 120 KiB（不截断）、
//!   bulk 超过 16 段（tcp 没有这个限制）、回复带 bulk 但请求没给 `REPLY_REMOTE` 或给的不够大（绝不越界 WRITE）、缓冲不在
//!   已注册的内存里、流帧与 OneWay（它们不走 rdma，3.3.3）、握手完成之前带 bulk。
//!
//! **缓冲什么时候还给池**（Rust 的所有权管不到网卡 DMA）：交给网卡的缓冲只在它的工作请求完成之后、或 QP 销毁之后才 Drop。
//! 等完成事件超时（或连接已关）的缓冲进"墓地"，与接收槽、发送暂存槽、在途表一起由本结构持有；本结构的**第一个字段是 QP**，
//! 字段按声明顺序 Drop，所以 QP 先销毁、之后网卡不再碰这些内存，然后它们才回池。`close()` 只置标志、唤醒等待者、
//! 把 QP 改到 ERR（在途的工作请求以 flush 结束）、关掉引导 TCP，不放任何缓冲。

use std::collections::HashMap;
use std::net::{Shutdown, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use wmfs_core::base::{ErrCode, clock, log};
use wmfs_core::buf::pool::S_SLOT;
use wmfs_core::buf::{Buf, BufSlice, Lane, MemPool};
use wmfs_core::hooks::traits::Conn;
use wmfs_core::proto::frame::{
    FLAG_BULK_REMOTE, FLAG_REPLY_REMOTE, FrameHeader, FrameKind, HEADER_LEN, InFrame, OutFrame,
};

use super::credit::Credits;
use super::desc::{Desc, encode_descs};
use super::params::{MAX_FRAME, MAX_SEGS, RECV_SLOTS};
use super::setup::RdmaSetup;
use super::wire::{Ev, Events, LiveGuard, Op, Wire};
use super::{Shared, lock};

/// 发出去之后对端还要来读（`BufSlice`）或来写（`recv_into`）的缓冲。
#[derive(Default)]
pub struct Held {
    _bulk: Vec<BufSlice>, // 只为持有、从不读它（与 `_live` 一样）：Drop 时才归还——对端读完了，或 QP 已销毁
    pub into: Option<Buf>,
}

impl Held {
    pub fn new(bulk: Vec<BufSlice>, into: Option<Buf>) -> Held {
        Held { _bulk: bulk, into }
    }
}

/// 等一批工作请求完成的结果。
pub enum Waited {
    AllOk,
    Failed,             // 全部完成了，但有失败的（QP 已出错）：缓冲可以还
    Abandoned(ErrCode), // 还有没完成的（超时或连接已关）：缓冲不能还，进墓地
}

/// 回复的一段 bulk：从本端 `from..from+len` RDMA WRITE 到对端的 `to`。
pub struct Write {
    pub from: u64,
    pub len: u32,
    pub to: Desc,
}

pub struct RdmaConn {
    pub wire: Box<dyn Wire>, // 第一个字段：Drop 时先销毁 QP（见文件头）
    pub recvs: Mutex<Receiver<Ev>>,
    pub reads: Mutex<Receiver<Ev>>,
    sends: Mutex<Receiver<Ev>>,
    pub slots: Vec<Buf>,                     // 接收槽（S 档，Normal lane）
    staging: Mutex<Buf>,                     // 发送暂存：帧头 + body + 描述符（S 档）
    pub inflight: Mutex<HashMap<u64, Held>>, // 本端发出的请求：req_id → 对端要读、要写的缓冲
    pub replies: Mutex<HashMap<u64, Desc>>,  // accept 侧：req_id → 对端给的 REPLY_REMOTE
    graveyard: Mutex<Vec<Held>>,             // 等不到完成事件的缓冲：留到 QP 销毁之后
    pub credits: Credits,
    pub pool: MemPool,
    pub boot: TcpStream, // 引导 TCP：一直开着，读到 EOF = 这条 RDMA 连接作废
    pub accept_side: bool,
    authed: AtomicBool, // 读写都走下面的方法（原子量的读写顺序统一在一处）
    closed: AtomicBool,
    seq: AtomicU32,
    pub peer: String,
    pub shared: Arc<Shared>,
    _live: LiveGuard, // max_qps 名额：QP 销毁之后才归还
}

impl RdmaConn {
    /// 包一个刚建好的 QP（INIT 状态）：从池里取 16 个接收槽与 1 个发送暂存槽（S 档、Normal lane——不能用 Repl，否则会吃掉
    /// 复制 lane 的保底；不等：取不到 → `BUSY`），把接收槽全部 post 上。
    pub fn new(
        wire: Box<dyn Wire>,
        ev: Events,
        pool: MemPool,
        boot: TcpStream,
        accept_side: bool,
        shared: Arc<Shared>,
        live: LiveGuard,
    ) -> Result<RdmaConn, ErrCode> {
        let take = || pool.get(S_SLOT, Lane::Normal, Duration::ZERO);
        let slots = (0..RECV_SLOTS).map(|_| take()).collect::<Result<Vec<Buf>, ErrCode>>()?;
        let staging = Mutex::new(take()?);
        let peer = format!("rdma:{}", boot.peer_addr().map(|a| a.to_string()).unwrap_or_else(|_| "?".into()));
        let c = RdmaConn {
            wire,
            recvs: Mutex::new(ev.recvs),
            reads: Mutex::new(ev.reads),
            sends: Mutex::new(ev.sends),
            slots,
            staging,
            inflight: Mutex::new(HashMap::new()),
            replies: Mutex::new(HashMap::new()),
            graveyard: Mutex::new(Vec::new()),
            credits: Credits::new(RECV_SLOTS),
            pool,
            boot,
            accept_side,
            authed: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            seq: AtomicU32::new(1),
            peer,
            shared,
            _live: live,
        };
        for i in 0..c.slots.len() {
            c.repost(i)?;
        }
        Ok(c)
    }

    /// 本端 QP 的信息（在引导 TCP 上交给对端）。
    pub fn local(&self) -> RdmaSetup {
        self.wire.local()
    }

    /// 拿到对端的信息之后：RTR → RTS。
    pub fn connect(&self, remote: &RdmaSetup) -> Result<(), ErrCode> {
        self.wire.connect(remote)
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }
    pub fn authed(&self) -> bool {
        self.authed.load(Ordering::SeqCst)
    }
    /// 一批工作请求的编号（wr_id 只放得下低 24 位）。
    pub fn next_seq(&self) -> u32 {
        self.seq.fetch_add(1, Ordering::Relaxed) & 0x00ff_ffff
    }
    /// 把接收槽 `slot` 还给网卡。
    pub fn repost(&self, slot: usize) -> Result<(), ErrCode> {
        self.wire.post_recv(slot as u32, self.slots[slot].addr(), S_SLOT as u32)
    }
    /// 网卡可能还在用的缓冲：留到 QP 销毁之后。
    pub fn bury(&self, h: Held) {
        lock(&self.graveyard).push(h);
    }

    /// 等 `n` 个属于 `seq` 的完成事件，最多等到 `deadline`（`clock::mono_ms`）；连接关闭时不再等（缓冲进墓地即可）。
    pub fn wait(&self, rx: &Mutex<Receiver<Ev>>, seq: u32, mut n: usize, deadline: u64) -> Waited {
        let rx = lock(rx);
        let mut ok = true;
        while n > 0 {
            let now = clock::mono_ms();
            if self.is_closed() {
                return Waited::Abandoned(ErrCode::ECONNRESET);
            }
            if now >= deadline {
                return Waited::Abandoned(ErrCode::ETIMEDOUT);
            }
            match rx.recv_timeout(Duration::from_millis((deadline - now).min(100))) {
                Ok(ev) if ev.seq == seq => {
                    n -= 1;
                    ok &= ev.ok;
                }
                Ok(_) | Err(RecvTimeoutError::Timeout) => {} // 别的批次晚到的完成事件：忽略
                Err(RecvTimeoutError::Disconnected) => return Waited::Abandoned(ErrCode::ECONNRESET),
            }
        }
        if ok { Waited::AllOk } else { Waited::Failed }
    }

    /// `addr..addr+len` 的描述符（rkey 来自覆盖它的 MR）；不在已注册的内存里 → `FEATURE_UNSUPPORTED`。
    fn desc(&self, addr: u64, len: u32) -> Result<Desc, ErrCode> {
        match self.wire.rkey(addr, len as u64) {
            Some(rkey) => Ok(Desc { addr, rkey, len }),
            None => self.unsupported("缓冲不在已注册的内存里（不是本进程内存池的？）"),
        }
    }

    fn unsupported<T>(&self, why: &str) -> Result<T, ErrCode> {
        log::warn("rdma", format_args!("{}：{why}——只让这一个帧失败（FEATURE_UNSUPPORTED）", self.peer));
        Err(ErrCode::FEATURE_UNSUPPORTED)
    }

    /// 请求帧：描述符表、信用、在途表，然后 SEND。
    fn send_request(
        &self,
        mut hdr: FrameHeader,
        body: &[u8],
        bulk: Vec<BufSlice>,
        into: Option<Buf>,
    ) -> Result<(), ErrCode> {
        // 长度为 0 的段不需要对端来读：不进描述符表（全是空段时不带 BULK_REMOTE，core 填的 bulk_len 也是 0）
        let nonempty = bulk.iter().filter(|s| s.len > 0);
        let descs = nonempty.map(|s| self.desc(s.addr(), s.len)).collect::<Result<Vec<Desc>, ErrCode>>()?;
        let reply = into.as_ref().map(|b| self.desc(b.addr(), b.capacity() as u32)).transpose()?;
        let keep = !descs.is_empty() || reply.is_some();
        if keep && self.accept_side {
            return self.unsupported("accept 侧的 QP 不开放远程读写，发不了带 bulk 或 recv_into 的请求");
        }
        if keep && lock(&self.inflight).contains_key(&hdr.req_id) {
            return self.unsupported("在途表里已有同一个 req_id");
        }
        if !descs.is_empty() {
            hdr.flags |= FLAG_BULK_REMOTE;
        }
        if reply.is_some() {
            hdr.flags |= FLAG_REPLY_REMOTE;
        }
        let mut tail = Vec::new();
        encode_descs(&mut tail, &descs, reply.as_ref());
        self.credits.take()?; // 在途请求已有 16 个：在这里等到有回复或连接关闭（只阻塞写线程）
        if keep {
            // 在 SEND 之前登记：回复可能比 post 返回还早到。全部段（含空段）一起留着，所有权不变
            lock(&self.inflight).insert(hdr.req_id, Held::new(bulk, into));
        }
        let deadline = clock::mono_ms() + self.shared.write_timeout_ms; // 等 SEND 完成的上限，从拿到信用之后算
        self.post_frame(&hdr, body, &tail, &[], Vec::new(), deadline)
    }

    /// 回复帧：带 bulk 时按请求给的 REPLY_REMOTE 依次 WRITE（绝不越界），再 SEND。
    fn send_response(&self, hdr: FrameHeader, body: &[u8], bulk: Vec<BufSlice>) -> Result<(), ErrCode> {
        // replies 只在 accept 侧有内容：connect 侧收到带 REPLY_REMOTE 的请求时 recv.rs 的状态机已经断开了
        let to = lock(&self.replies).remove(&hdr.req_id);
        let n: u64 = bulk.iter().map(|s| s.len as u64).sum();
        let mut writes = Vec::new();
        if n > 0 {
            let Some(to) = to else {
                return self
                    .unsupported("回复带 bulk，但请求没有给 REPLY_REMOTE（期望回复带 bulk 时必须给 recv_into）");
            };
            if n > to.len as u64 {
                return self.unsupported(&format!("回复的 bulk {n} 字节超过请求方 recv_into 的 {} 字节", to.len));
            }
            let mut off = 0u64;
            for s in bulk.iter().filter(|s| s.len > 0) {
                if self.wire.rkey(s.addr(), s.len as u64).is_none() {
                    return self.unsupported("回复的 bulk 不在已注册的内存里");
                }
                // 不会溢出：decode_descs 保证了 to.addr + to.len 不溢出，而 off + s.len ≤ n ≤ to.len
                writes.push(Write {
                    from: s.addr(),
                    len: s.len,
                    to: Desc { addr: to.addr + off, rkey: to.rkey, len: s.len },
                });
                off += s.len as u64;
            }
        }
        let deadline = clock::mono_ms() + self.shared.write_timeout_ms;
        self.post_frame(&hdr, body, &[], &writes, bulk, deadline)
    }

    /// 帧头 + body + 描述符拷进发送暂存槽，先 post 各个 WRITE 再 post SEND，等它们全部完成。`keep` 是 WRITE 的来源：
    /// 完成之后才放；等不到完成事件就进墓地。出错 = 连接作废（这里就关掉，core 也会关）。
    fn post_frame(
        &self,
        hdr: &FrameHeader,
        body: &[u8],
        tail: &[u8],
        writes: &[Write],
        keep: Vec<BufSlice>,
        deadline: u64,
    ) -> Result<(), ErrCode> {
        let mut st = lock(&self.staging);
        let (end, n) = (HEADER_LEN + body.len(), HEADER_LEN + body.len() + tail.len());
        st.set_len(n);
        let b = st.as_mut_slice();
        b[..HEADER_LEN].copy_from_slice(&hdr.encode());
        b[HEADER_LEN..end].copy_from_slice(body);
        b[end..].copy_from_slice(tail);
        let seq = self.next_seq();
        let mut posted = 0;
        let mut r =
            writes.iter().try_for_each(|w| self.wire.post_write(seq, w.from, w.len, &w.to).map(|()| posted += 1));
        if r.is_ok() {
            r = self.wire.post_send(seq, st.addr(), n as u32).map(|()| posted += 1);
        }
        let e = match (r, self.wait(&self.sends, seq, posted, deadline)) {
            (Ok(()), Waited::AllOk) => {
                self.shared.stats.add(Op::Write, writes.len() as u64);
                self.shared.stats.add(Op::Send, 1);
                return Ok(());
            }
            (r, Waited::Abandoned(e)) => {
                self.bury(Held::new(keep, None)); // 网卡可能还在读它们（暂存槽本来就由本结构持有）
                r.err().unwrap_or(e)
            }
            (r, _) => r.err().unwrap_or(ErrCode::ECONNRESET), // 都完成了、有失败的：QP 已出错，缓冲可以还
        };
        log::warn("rdma", format_args!("{}：发送失败（{e}），断开", self.peer));
        self.close();
        Err(e)
    }
}

impl Conn for RdmaConn {
    fn send(&self, f: OutFrame) -> Result<(), ErrCode> {
        if self.is_closed() {
            return Err(ErrCode::ECONNRESET);
        }
        let OutFrame { hdr, body, bulk, recv_into } = f;
        if HEADER_LEN + body.len() > MAX_FRAME {
            return self.unsupported(&format!("帧头 + body {} 字节超过 rdma 的上限 120 KiB", HEADER_LEN + body.len()));
        }
        let segs = bulk.iter().filter(|s| s.len > 0).count(); // 长度为 0 的段不占描述符
        if segs > MAX_SEGS {
            return self.unsupported(&format!("bulk 有 {segs} 段，rdma 最多 16 段"));
        }
        if !self.authed() && (segs > 0 || recv_into.is_some()) {
            return self.unsupported("握手完成之前不做单边操作");
        }
        match hdr.kind {
            FrameKind::Request => self.send_request(hdr, &body, bulk, recv_into),
            FrameKind::Response => self.send_response(hdr, &body, bulk),
            FrameKind::OneWay | FrameKind::StreamChunk => self.unsupported("流与 OneWay 不走 rdma（3.3.3）"),
        }
    }

    fn recv(&self) -> Result<InFrame, ErrCode> {
        let ev = self.wait_recv()?; // 空闲等帧的时间不计时（3.7.4）
        let t0 = Instant::now();
        let r = self.take_frame(ev);
        self.shared.recv_hook.exit(t0, r.is_ok());
        r
    }

    /// connect 侧握手完成之后才打开 QP 的远程读写（4.1 实现注记（M2））；之后才发带描述符的请求。
    fn mark_authenticated(&self) {
        if let Err(e) = self.wire.allow_remote() {
            log::error("rdma", format_args!("{}：握手完成后打开远程读写失败（{e}），断开", self.peer));
            return self.close();
        }
        self.authed.store(true, Ordering::SeqCst);
    }

    fn peer(&self) -> String {
        self.peer.clone()
    }

    fn close(&self) {
        if !self.closed.swap(true, Ordering::SeqCst) {
            self.credits.close(); // 等信用的写线程返回 ECONNRESET
            self.wire.fail(); // QP → ERR：已 post 的工作请求全部 flush，等完成事件的线程随即醒来
            let _ = self.boot.shutdown(Shutdown::Both); // 对端的读线程看到引导 TCP 的 EOF
        }
    }
}
