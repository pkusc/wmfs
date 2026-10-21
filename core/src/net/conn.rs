//! core 对传输插件 `Conn` 的包装（design.md 3.7.3、1.2 规则 2）：读线程 + 写线程 + 有界发送队列；其他代码只用它。
//!
//! 一个连接的 socket 只由它的写线程写、读线程读；别的线程发帧只是 `ConnHandle::send` = `try_send` 进该连接的
//! 有界发送队列（node.toml `[net] send_queue`，默认 1024 帧），队列满 → 关闭连接并返回 `ECONNRESET`（3.3.8），所以执行器、
//! io、repl 线程永远不会被一个不读 socket 的客户端卡住。写线程在每次 `Conn::send` 之前填好帧头的长度与 CRC，并包一层
//! transport 钩子的包装（fail point `plugin.<名字>.transport`：触发 = 这条连接作废，不论配置的是哪个错误码，3.7.4）。
//! 传输层的任何错误都按"连接断开"处理：关闭这条连接（写超时另计 `wmfs_conn_write_timeout_total`）。唯一例外是
//! `Conn::send` 返回 `FEATURE_UNSUPPORTED`（帧超过该传输的上限，例如 RDMA 的帧头 + body ≤ 120 KiB）：不断开，只让这一个
//! 请求失败——请求帧交给 [`ConnOpts::on_unsent`]（RpcClient 让这个调用以该错误码结束），回复帧改发一个同 req_id 的
//! ERROR 帧（3.3.8）。读线程收到的每一帧都由 [`recv_checked`] 校验 body 与 bulk 的 CRC。
//!
//! 保活（实现注记 M1）：**客户端**连接在**握手完成之后**，写线程空闲 5 s 就发一个 `req_id = 0` 的 Ping（回复被忽略）；
//! 握手完成之前不发——服务端会把它当成"未认证连接上的非握手帧"直接断开（3.3.2）。服务端从不向客户端发请求。
//! 传输插件负责"15 s 收不到任何帧就判死"（tcp：读超时 15 s），于是两边都能发现对端已死。

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, TrySendError, bounded};
use wmfs_api::metrics::{self, Counter};

use crate::base::{ErrCode, clock, log};
use crate::hooks::pipeline::HookCall;
use crate::hooks::traits::{Conn, ConnIdentity};
use crate::proto::encode_body;
use crate::proto::frame::{
    ErrorBody, FLAG_ERROR, FLAG_HAS_BULK, FrameHeader, FrameKind, InFrame, MAX_BODY, MAX_BULK, MSG_PING, OutFrame,
};
use crate::proto::handshake::PingBody;

/// 客户端连接空闲多久发一次保活 Ping。
pub const KEEPALIVE_MS: u64 = 5000;
/// 连接读/写线程的栈大小（1.2 规则 4）。
const STACK: usize = 256 << 10;

/// core 对一条连接的句柄（Clone 只增引用计数）；所有克隆都被丢弃后，写线程把队列里剩下的帧写完再关闭连接。
#[derive(Clone)]
pub struct ConnHandle {
    inner: Arc<Inner>,
    tx: Sender<OutFrame>,
}

struct Inner {
    id: u64,
    conn: Arc<dyn Conn>,
    peer: String,
    identity: OnceLock<ConnIdentity>, // 服务端：握手完成后才有
    authed: AtomicBool,               // 握手已完成（两端都有）：写线程从此才发保活 Ping
    closed: AtomicBool,
}

/// 请求帧超过该传输的上限、没有发出去时的通知：参数是 req_id 与错误码（只让这一个请求失败）。
pub(crate) type OnUnsent = Arc<dyn Fn(u64, ErrCode) + Send + Sync>;

/// 起一条连接时的选项。
pub(crate) struct ConnOpts {
    pub queue: usize,                // 发送队列长度（[net] send_queue）
    pub keepalive: bool,             // 客户端连接：握手完成后空闲时发保活 Ping
    pub hook: Arc<HookCall>,         // 该传输插件的 transport 钩子包装
    pub on_unsent: Option<OnUnsent>, // 客户端连接：RpcClient 让没发出去的那个调用结束
}

/// 连接层的计数器（7.3）。RpcServer / RpcClient 创建时就注册好，新进程的 /metrics 里也能看到 0
/// （分得清"没发生过"与"没有这个指标"）。
pub(crate) struct ConnCounters {
    pub write_timeout: Arc<Counter>,
    pub queue_full: Arc<Counter>,
    pub proto_errors: Arc<Counter>,
    pub req_dropped: Arc<Counter>,
}

pub(crate) fn counters() -> &'static ConnCounters {
    static C: OnceLock<ConnCounters> = OnceLock::new();
    C.get_or_init(|| ConnCounters {
        write_timeout: metrics::counter("wmfs_conn_write_timeout_total", "写超时（对端不读）而断开的连接数", &[]),
        queue_full: metrics::counter("wmfs_conn_send_queue_full_total", "发送队列满而断开的连接数", &[]),
        proto_errors: metrics::counter("wmfs_conn_proto_errors_total", "帧头、长度或 CRC 不对而断开的连接数", &[]),
        req_dropped: metrics::counter("wmfs_req_dropped_total", "没回复就被丢弃的请求数（自动回 EIO）", &[]),
    })
}

impl ConnHandle {
    /// send = try_send：队列满 → 关闭连接并返回 ECONNRESET（3.3.8）。帧超过协议上限 → `FEATURE_UNSUPPORTED`，不断开。
    pub fn send(&self, f: OutFrame) -> Result<(), ErrCode> {
        let bulk: u64 = f.bulk.iter().map(|s| s.len as u64).sum();
        if f.body.len() > MAX_BODY as usize || bulk > MAX_BULK as u64 {
            return Err(ErrCode::FEATURE_UNSUPPORTED);
        }
        if self.is_closed() {
            return Err(ErrCode::ECONNRESET);
        }
        match self.tx.try_send(f) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => {
                counters().queue_full.inc();
                log::warn("conn", format_args!("连接 {} 的发送队列满（对端太慢），断开", self.inner.peer));
                self.close();
                Err(ErrCode::ECONNRESET)
            }
            Err(TrySendError::Disconnected(_)) => Err(ErrCode::ECONNRESET),
        }
    }

    /// 立即关闭（任意线程可调用）：之后 send 返回 ECONNRESET，读线程的 recv 出错退出，写线程最多 1 s 内退出。
    pub fn close(&self) {
        if !self.inner.closed.swap(true, Ordering::SeqCst) {
            self.inner.conn.close();
        }
    }

    /// 认证后的身份。只在握手完成之后调用（例如 Responder 里）；之前调用是不变量破坏。
    pub fn identity(&self) -> &ConnIdentity {
        self.inner.identity.get().unwrap_or_else(|| panic!("不变量破坏：连接 {} 还没完成握手就取身份", self.inner.id))
    }

    pub fn id(&self) -> u64 {
        self.inner.id
    }

    pub fn peer(&self) -> &str {
        &self.inner.peer
    }

    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::SeqCst)
    }

    /// 握手完成（在读线程上调用）：记下对端身份（只有服务端有）；告诉传输插件（`Conn::mark_authenticated`：
    /// 从此才收带 bulk 的帧、单边传输才允许 READ/WRITE）；写线程从此才发保活 Ping。
    pub(crate) fn authenticated(&self, id: Option<ConnIdentity>) {
        if let Some(id) = id {
            let _ = self.inner.identity.set(id);
        }
        self.inner.conn.mark_authenticated();
        self.inner.authed.store(true, Ordering::SeqCst);
    }
}

/// 为一条新连接起写线程 `conn-w-<id>` 与读线程 `conn-<id>`，返回句柄。`reader` 在读线程上运行（先握手、再收帧）。
pub(crate) fn spawn(
    conn: Box<dyn Conn>,
    opts: ConnOpts,
    reader: Box<dyn FnOnce(ConnHandle, Arc<dyn Conn>) + Send>,
) -> Result<ConnHandle, ErrCode> {
    static NEXT_ID: AtomicU64 = AtomicU64::new(1); // 从 1 开始：0 留给"还没走任何连接"
    let conn: Arc<dyn Conn> = Arc::from(conn);
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let (tx, rx) = bounded(opts.queue.max(1));
    let inner = Arc::new(Inner {
        id,
        peer: conn.peer(),
        conn: conn.clone(),
        identity: OnceLock::new(),
        authed: AtomicBool::new(false),
        closed: AtomicBool::new(false),
    });
    let h = ConnHandle { inner: inner.clone(), tx };
    let builder = |name: String| std::thread::Builder::new().name(name).stack_size(STACK);
    if builder(format!("conn-w-{id}")).spawn(move || write_loop(inner, rx, opts)).is_err() {
        h.close();
        return Err(ErrCode::ENOMEM);
    }
    let h2 = h.clone();
    if builder(format!("conn-{id}")).spawn(move || reader(h2, conn)).is_err() {
        h.close();
        return Err(ErrCode::ENOMEM);
    }
    Ok(h)
}

/// 写线程：从队列取帧 → 填帧头 → 过 transport 钩子包装 → `Conn::send`。出错（含写超时、fail point）即关闭连接。
fn write_loop(inner: Arc<Inner>, rx: Receiver<OutFrame>, opts: ConnOpts) {
    let mut last = clock::mono_ms();
    loop {
        let f = match rx.recv_timeout(Duration::from_secs(1)) {
            Ok(f) => f,
            Err(RecvTimeoutError::Timeout) if keepalive_due(&inner, &opts, last) => keepalive(),
            Err(RecvTimeoutError::Timeout) if !inner.closed.load(Ordering::SeqCst) => continue,
            Err(_) => break, // 已关闭，或所有句柄都被丢弃（队列里的帧已写完）
        };
        if inner.closed.load(Ordering::SeqCst) {
            break;
        }
        let hdr = f.hdr;
        let r = match send_one(&inner, f, &opts.hook) {
            Err(ErrCode::FEATURE_UNSUPPORTED) => unsent(&inner, &opts, &hdr), // 帧超过该传输的上限：只让这一个请求失败
            r => r,
        };
        if let Err(e) = r {
            if e == ErrCode::ETIMEDOUT {
                counters().write_timeout.inc();
            }
            log::warn("conn", format_args!("连接 {} 写失败：{e}，断开", inner.peer));
            break;
        }
        last = clock::mono_ms();
    }
    inner.closed.store(true, Ordering::SeqCst);
    inner.conn.close();
}

/// 该发保活 Ping 了吗：客户端连接、握手已完成、写线程空闲满 5 s。
fn keepalive_due(inner: &Inner, opts: &ConnOpts, last: u64) -> bool {
    opts.keepalive && inner.authed.load(Ordering::SeqCst) && clock::mono_ms() - last >= KEEPALIVE_MS
}

/// 填好帧头的长度、CRC 与 HAS_BULK（3.3.1："发送前由 core 填"），过 transport 钩子的包装，再交给传输层。
fn send_one(inner: &Inner, mut f: OutFrame, hook: &HookCall) -> Result<(), ErrCode> {
    f.hdr.body_len = f.body.len() as u32;
    f.hdr.body_crc32c = crc32c::crc32c(&f.body);
    let (mut n, mut crc) = (0u32, 0u32);
    for s in &f.bulk {
        n += s.len;
        crc = crc32c::crc32c_append(crc, s.as_slice());
    }
    (f.hdr.bulk_len, f.hdr.bulk_crc32c) = (n, crc);
    f.hdr.flags = if n > 0 { f.hdr.flags | FLAG_HAS_BULK } else { f.hdr.flags & !FLAG_HAS_BULK };
    // fail point 触发 = 这条连接作废（3.7.4）：不论配置的是哪个错误码，一律按连接错误处理（不走"只让一个请求失败"）
    let t0 = hook.enter().map_err(|e| {
        log::warn("conn", format_args!("连接 {} 的 transport fail point 触发（{e}）：连接作废", inner.peer));
        ErrCode::ECONNRESET
    })?;
    let r = inner.conn.send(f);
    hook.exit(t0, r.is_ok());
    r
}

/// `Conn::send` 返回 `FEATURE_UNSUPPORTED`（帧超过该传输的上限）：不断开，只让这一个请求失败（3.3.8、3.6.4）。
/// 请求帧 → `on_unsent`；回复帧与流帧 → 改发一个同 req_id、同 msg_type 的 ERROR 帧（很小，一定发得出去），对端的这个
/// 调用立即结束；OneWay（RaftMsg）只丢掉这一帧（raft 会重发）。返回的错误才是连接错误。
fn unsent(inner: &Inner, opts: &ConnOpts, hdr: &FrameHeader) -> Result<(), ErrCode> {
    let msg = hdr.msg_type;
    log::warn("conn", format_args!("连接 {} 上一个 {msg:#06x} 帧超过该传输的上限：只让这一个请求失败", inner.peer));
    match hdr.kind {
        FrameKind::Request => {
            if let Some(cb) = &opts.on_unsent {
                cb(hdr.req_id, ErrCode::FEATURE_UNSUPPORTED);
            }
            Ok(())
        }
        FrameKind::Response | FrameKind::StreamChunk => {
            let mut h = FrameHeader::new(FrameKind::Response, msg, hdr.req_id, hdr.trace_id);
            (h.flags, h.span_id) = (FLAG_ERROR, hdr.span_id);
            let body = encode_body(&ErrorBody::new(ErrCode::FEATURE_UNSUPPORTED, 0));
            send_one(inner, OutFrame { hdr: h, body, bulk: vec![], recv_into: None }, &opts.hook)
        }
        FrameKind::OneWay => Ok(()),
    }
}

fn keepalive() -> OutFrame {
    let hdr = FrameHeader::new(FrameKind::Request, MSG_PING, 0, 0);
    OutFrame { hdr, body: encode_body(&PingBody { payload: vec![] }), bulk: vec![], recv_into: None }
}

/// 读一帧并校验 body 与 bulk 的长度和 CRC（帧头本身由传输插件用 `FrameHeader::decode` 校验）。
/// 任何不符 → `PROTO_UNSUPPORTED`（调用方关闭连接；计数 `wmfs_conn_proto_errors_total`）。
pub(crate) fn recv_checked(conn: &dyn Conn) -> Result<InFrame, ErrCode> {
    let r = conn.recv().and_then(|f| {
        let h = &f.hdr;
        let bulk_ok = f
            .bulk
            .as_ref()
            .is_none_or(|b| b.len() == h.bulk_len as usize && crc32c::crc32c(b.as_slice()) == h.bulk_crc32c);
        let body_ok = f.body.len() == h.body_len as usize && crc32c::crc32c(&f.body) == h.body_crc32c;
        if body_ok && bulk_ok { Ok(f) } else { Err(ErrCode::PROTO_UNSUPPORTED) }
    });
    if r.as_ref().err() == Some(&ErrCode::PROTO_UNSUPPORTED) {
        counters().proto_errors.inc();
    }
    r
}
