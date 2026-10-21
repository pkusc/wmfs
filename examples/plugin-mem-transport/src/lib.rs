//! plugin-mem-transport：进程内存里的传输插件（design.md 2.1、9.3）。
//!
//! 它是一个独立 crate，只依赖 `wmfs-core` 的公开 API（`hooks::traits::{Transport, Conn}`、帧、内存池），
//! 用来证明"新增一个传输插件 = 新 crate + `cli/src/registry.rs` 一行 + 一个 feature，core 一行不改"；
//! 也给单进程的组件测试用（不占端口）。
//!
//! 实现：`listen` 把名字登记进进程内的表；`connect(名字)` 造一对互相连通的 `MemConn`，把服务端那一头交给 `on_conn`。
//! 帧经有界通道传过去，bulk 以 `BufSlice` 的形式跟着走——**接收方**在自己的 `recv` 里把它拷进自己的池缓冲
//! （按 core 的 `frame::bulk_buf` 规则：回复帧用请求方给的 `recv_into`），然后才放掉发送方的缓冲：和 RDMA
//! "接收方 READ 完、发送方才 Drop" 的语义一样。握手完成之前只收握手帧（不带 bulk）。
//! [`MemTransport::with_frame_limit`] 可以给帧头 + body 设一个上限（模拟 RDMA 的 120 KiB；测试"帧超过该传输的上限只让
//! 这一个请求失败"用）。指标 `wmfs_mem_frames_total{dir}`；fail point `plugin.mem.transport` 由 core 包在写线程的 `send` 外层；
//! 收帧（拷贝 bulk）的计时由这里做（3.7.4）。

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender, TrySendError, sync_channel};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant};

use wmfs_core::api::metrics::{self, Counter};
use wmfs_core::base::ErrCode;
use wmfs_core::buf::{Buf, BufSlice, MemPool, RegionInfo};
use wmfs_core::hooks::manifest::{CORE_ABI_VERSION, Hook, Manifest};
use wmfs_core::hooks::pipeline::HookCall;
use wmfs_core::hooks::traits::{Conn, Transport};
use wmfs_core::proto::frame::{FrameHeader, FrameKind, HEADER_LEN, InFrame, OutFrame, bulk_buf};

pub static MANIFEST: Manifest = Manifest {
    name: "mem",
    version: "0.1.0",
    hooks: &[Hook::Transport],
    altitude: 0,
    hot_path: true,
    config_schema: "", // 没有部署参数：监听名字只在代码里给（MemTransport::listening），它只用于单进程测试
    abi_version: CORE_ABI_VERSION,
    attrs: &[],
    budget_ns: 100_000_000,
    fail_points: &["plugin.mem.transport"],
};

/// 每个方向最多攒多少帧没被读（相当于 socket 缓冲）。
const QUEUE: usize = 256;

type OnConn = Arc<dyn Fn(Box<dyn Conn>) + Send + Sync>;
/// 一个在监听的名字：服务端的传输（它的帧上限、指标）、内存池、新连接回调。
type Listener = (MemTransport, MemPool, OnConn);

/// 进程内的监听表：名字 → 监听者。
fn listeners() -> MutexGuard<'static, HashMap<String, Listener>> {
    static L: OnceLock<Mutex<HashMap<String, Listener>>> = OnceLock::new();
    L.get_or_init(Default::default).lock().unwrap_or_else(|e| e.into_inner())
}

#[derive(Clone)]
pub struct MemTransport {
    listen: Option<String>,
    frame_limit: usize, // 帧头 + body 的上限（默认不限）
    frames_in: Arc<Counter>,
    frames_out: Arc<Counter>,
    recv_hook: Arc<HookCall>, // 收帧一侧的计时（发帧一侧由 core 包）
}

impl MemTransport {
    /// 不监听（只连别人，例如客户端）。
    pub fn new() -> MemTransport {
        MemTransport::with_listen(None)
    }
    /// 在进程内的名字 `name` 上监听（连接地址就是这个名字）。
    pub fn listening(name: &str) -> MemTransport {
        MemTransport::with_listen(Some(name.to_string()))
    }
    /// 帧头 + body 超过 `max` 字节的帧 `send` 返回 `FEATURE_UNSUPPORTED`（不截断；模拟 RDMA 的上限，3.6.4）。
    pub fn with_frame_limit(mut self, max: usize) -> MemTransport {
        self.frame_limit = max;
        self
    }
    fn with_listen(listen: Option<String>) -> MemTransport {
        let frames = |d| metrics::counter("wmfs_mem_frames_total", "mem 传输收发的帧数", &[("dir", d)]);
        let recv_hook = Arc::new(HookCall::new(&MANIFEST, Hook::Transport));
        MemTransport { listen, frame_limit: usize::MAX, frames_in: frames("in"), frames_out: frames("out"), recv_hook }
    }
    /// 这个传输这一头的连接。
    fn end(&self, tx: SyncSender<Wire>, rx: Receiver<Wire>, pool: MemPool, peer: String) -> MemConn {
        let (closed, authed) = (AtomicBool::new(false), AtomicBool::new(false));
        let (tx, rx, recv_into) = (Mutex::new(Some(tx)), Mutex::new(rx), Mutex::new(HashMap::new()));
        MemConn { tx, rx, closed, authed, pool, peer, recv_into, t: self.clone() }
    }
}

impl Default for MemTransport {
    fn default() -> MemTransport {
        MemTransport::new()
    }
}

impl Transport for MemTransport {
    fn manifest(&self) -> &'static Manifest {
        &MANIFEST
    }
    fn scheme(&self) -> &'static str {
        "mem"
    }
    fn register_region(&self, _region: &RegionInfo) -> Result<(), ErrCode> {
        Ok(()) // 内存里搬运不需要注册
    }
    fn listen(&self, pool: MemPool, on_conn: OnConn) -> Result<Option<String>, ErrCode> {
        let Some(name) = self.listen.clone() else { return Ok(None) };
        let mut l = listeners();
        if l.contains_key(&name) {
            return Err(ErrCode::EBUSY); // 名字已被占用（相当于端口被占用）
        }
        l.insert(name.clone(), (self.clone(), pool, on_conn));
        Ok(Some(name))
    }
    fn connect(&self, addr: &str, pool: MemPool, _timeout: Duration) -> Result<Box<dyn Conn>, ErrCode> {
        let (server, server_pool, on_conn) = listeners().get(addr).cloned().ok_or(ErrCode::ECONNRESET)?; // 没人监听 = 连接被拒
        let (c2s_tx, c2s_rx) = sync_channel(QUEUE);
        let (s2c_tx, s2c_rx) = sync_channel(QUEUE);
        on_conn(Box::new(server.end(s2c_tx, c2s_rx, server_pool, "mem-client".into())));
        Ok(Box::new(self.end(c2s_tx, s2c_rx, pool, format!("mem:{addr}"))))
    }
}

/// 线上的一帧：帧头与 body 原样，bulk 仍是发送方池里的 `BufSlice`（接收方拷走之后才放掉）。
struct Wire {
    hdr: FrameHeader,
    body: Vec<u8>,
    bulk: Vec<BufSlice>,
}

struct MemConn {
    tx: Mutex<Option<SyncSender<Wire>>>, // close() 时拿走并丢弃：对端读完剩下的帧后看到"断开"
    rx: Mutex<Receiver<Wire>>,
    closed: AtomicBool,
    authed: AtomicBool, // 握手完成之前只收握手帧（不带 bulk）
    pool: MemPool,
    peer: String,
    recv_into: Mutex<HashMap<u64, Buf>>,
    t: MemTransport, // 帧上限、指标句柄
}

impl Conn for MemConn {
    fn send(&self, f: OutFrame) -> Result<(), ErrCode> {
        FrameHeader::decode(&f.hdr.encode())?; // 与 tcp 一样遵守帧的长度上限
        if HEADER_LEN + f.body.len() > self.t.frame_limit {
            return Err(ErrCode::FEATURE_UNSUPPORTED); // 超过这个传输的上限：不截断，core 只让这一个请求失败
        }
        if let Some(b) = f.recv_into {
            lock(&self.recv_into).insert(f.hdr.req_id, b);
        }
        let mut w = Wire { hdr: f.hdr, body: f.body, bulk: f.bulk };
        loop {
            let r = match lock(&self.tx).as_ref() {
                Some(tx) => tx.try_send(w),
                None => return Err(ErrCode::ECONNRESET),
            };
            match r {
                Ok(()) => break,
                Err(TrySendError::Full(back)) if !self.closed.load(Ordering::SeqCst) => {
                    w = back; // 对端还没读：像写满的 socket 一样等一会儿（只阻塞写线程）
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(_) => return Err(ErrCode::ECONNRESET),
            }
        }
        self.t.frames_out.inc();
        Ok(())
    }

    fn recv(&self) -> Result<InFrame, ErrCode> {
        let rx = lock(&self.rx);
        let w = loop {
            if self.closed.load(Ordering::SeqCst) {
                return Err(ErrCode::ECONNRESET);
            }
            match rx.recv_timeout(Duration::from_millis(100)) {
                Ok(w) => break w,
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => return Err(ErrCode::ECONNRESET),
            }
        };
        if !w.bulk.is_empty() && !self.authed.load(Ordering::SeqCst) {
            return Err(ErrCode::PROTO_UNSUPPORTED); // 未认证的连接只允许握手帧（不带 bulk）：直接断开（3.3.2）
        }
        self.t.frames_in.inc();
        let t0 = Instant::now(); // 3.7.4：收帧的计时只算"帧到了之后"（这里是把 bulk 拷进本端的池）
        let f = self.land(w);
        self.t.recv_hook.exit(t0, true);
        Ok(f)
    }

    fn mark_authenticated(&self) {
        self.authed.store(true, Ordering::SeqCst);
    }

    fn peer(&self) -> String {
        self.peer.clone()
    }

    fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        lock(&self.tx).take();
    }
}

impl MemConn {
    /// 把 bulk 拷进本端的池缓冲（core 的 `bulk_buf`：回复帧优先用请求方给的 recv_into）；100 ms 取不到缓冲 → bulk_dropped。
    fn land(&self, w: Wire) -> InFrame {
        let hdr = w.hdr;
        let mine = if hdr.kind == FrameKind::Response { lock(&self.recv_into).remove(&hdr.req_id) } else { None };
        // 按实际带来的字节数取缓冲（帧头里的 bulk_len 由 core 的 recv_checked 核对，对不上就断开）
        let got = FrameHeader { bulk_len: w.bulk.iter().map(|s| s.len).sum(), ..hdr };
        let (mut bulk, mut dropped) = (None, false);
        if got.bulk_len > 0 {
            match bulk_buf(&self.pool, &got, mine, Duration::from_millis(100)) {
                Ok(mut b) => {
                    let mut at = 0;
                    for s in &w.bulk {
                        b.as_mut_slice()[at..at + s.len as usize].copy_from_slice(s.as_slice());
                        at += s.len as usize;
                    }
                    bulk = Some(b);
                }
                Err(_) => dropped = true,
            }
        }
        InFrame { hdr, body: w.body, bulk, bulk_dropped: dropped } // 这里 w.bulk 被丢弃：发送方的缓冲回池
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}
