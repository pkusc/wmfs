//! tcp 传输插件（design.md 4.1、3.6.4）：同一种帧跑在 TCP 上；插件只负责"能收发帧"，握手、CRC 校验、重连都在 core。
//!
//! - `listen`：按自己的部署配置 `[plugin.tcp] listen` 绑定，起一个 `accept-tcp` 线程，返回实际绑定的地址；
//! - 每条连接：`TCP_NODELAY`、`SO_KEEPALIVE`（core 的 `sys::set_keepalive`，插件里没有 unsafe）、写超时
//!   （`[net] write_timeout_ms`，默认 5 s，按整帧计：一帧 5 s 内没写完即 `ETIMEDOUT`，core 断开连接并计数）、读超时 15 s
//!   （15 s 收不到任何帧判死；客户端连接空闲 5 s 会发保活 Ping，见 core `net/conn.rs`）；
//! - `send`（只在 core 的写线程上调用，所以不需要锁）：依次写帧头、body、各段 bulk（`BufSlice` 逐个写，不拼接）；
//! - `recv`（只在读线程上调用）：读 48 字节帧头并 `FrameHeader::decode`（magic、CRC、长度上限）→ 读 body → 有 bulk 时
//!   按 core 的 `frame::bulk_buf` 找缓冲（回复帧读进请求方给的 `recv_into`，请求帧按 msg_type 选 lane 从池里取）后读进去；
//!   池里 100 ms 取不到 → 把 bulk 读出丢弃，交出 `bulk_dropped = true`（core 回 BUSY）。body 与 bulk 的 CRC 由 core 校验。
//!   握手完成（`mark_authenticated`）之前只收握手帧：帧头声明了 bulk 就直接断开，不取池槽（3.3.2）。
//! - 计时（3.7.4）：发的一半由 core 包在写线程的 `send` 外层；收的一半由这里计——读到帧头之后开始，读完 body + bulk 结束。
//! - `register_region` 空操作（TCP 不需要注册内存）。
//!
//! 指标 `wmfs_tcp_bytes_total{dir}`；fail point `plugin.tcp.transport` 由 core 包在写线程的 `send` 外层（触发 = 连接作废）。

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use wmfs_core::api::metrics::{self, Counter};
use wmfs_core::base::config::PluginTable;
use wmfs_core::base::{ErrCode, clock, log, sys};
use wmfs_core::buf::{Buf, MemPool, RegionInfo};
use wmfs_core::hooks::manifest::{CORE_ABI_VERSION, Hook, Manifest};
use wmfs_core::hooks::pipeline::HookCall;
use wmfs_core::hooks::traits::{Conn, Transport};
use wmfs_core::proto::frame::{
    FLAG_BULK_REMOTE, FLAG_REPLY_REMOTE, FrameHeader, FrameKind, HEADER_LEN, InFrame, OutFrame, bulk_buf,
};

pub static MANIFEST: Manifest = Manifest {
    name: "tcp",
    version: "0.1.0",
    hooks: &[Hook::Transport],
    altitude: 0,
    hot_path: true,
    config_schema: r#"listen = { type = "addr", default = "0.0.0.0:9300", doc = "rpc 监听地址（wmfs init 写成 --addr:--rpc-port）" }"#,
    abi_version: CORE_ABI_VERSION,
    attrs: &[],
    budget_ns: 100_000_000, // 100 ms：一次 send，或收到帧头之后读完 body + bulk（3.7.4）
    fail_points: &["plugin.tcp.transport"],
};

/// 15 s 收不到任何字节就判对端已死。
const READ_IDLE: Duration = Duration::from_secs(15);
/// 读 bulk 时从池里取缓冲最多等多久（3.6.1）。
const BULK_WAIT: Duration = Duration::from_millis(100);

#[derive(Clone)]
pub struct TcpTransport {
    listen: String,
    write_timeout: Duration,
    bytes_in: Arc<Counter>,
    bytes_out: Arc<Counter>,
    recv_hook: Arc<HookCall>, // 收帧一侧的计时（发帧一侧由 core 包）
}

impl TcpTransport {
    /// `cfg` = node.toml 的 `[plugin.tcp]`（键已由 PluginSet::validate 按 config_schema 检查）；
    /// `write_timeout_ms` = `[net] write_timeout_ms`（默认 5000）。
    pub fn new(cfg: &PluginTable, write_timeout_ms: u32) -> Result<TcpTransport, String> {
        let listen = match cfg.get("listen") {
            None => "0.0.0.0:9300".to_string(),
            Some(v) => v.as_str().ok_or("[plugin.tcp] listen 应是字符串，例如 \"127.0.0.1:9300\"")?.to_string(),
        };
        if write_timeout_ms == 0 {
            return Err("[net] write_timeout_ms 必须大于 0".into());
        }
        let bytes = |d| metrics::counter("wmfs_tcp_bytes_total", "tcp 传输收发的字节数", &[("dir", d)]);
        Ok(TcpTransport {
            listen,
            write_timeout: Duration::from_millis(write_timeout_ms as u64),
            bytes_in: bytes("in"),
            bytes_out: bytes("out"),
            recv_hook: Arc::new(HookCall::new(&MANIFEST, Hook::Transport)),
        })
    }

    fn wrap(&self, s: TcpStream, pool: MemPool) -> Result<TcpConn, ErrCode> {
        s.set_nodelay(true)?;
        sys::set_keepalive(&s)?;
        s.set_write_timeout(Some(self.write_timeout))?;
        s.set_read_timeout(Some(READ_IDLE))?;
        let peer = s.peer_addr().map(|a| a.to_string()).unwrap_or_else(|_| "?".into());
        let w = s.try_clone()?;
        let (authed, recv_into) = (AtomicBool::new(false), Mutex::new(HashMap::new()));
        Ok(TcpConn { r: s, w, pool, peer, authed, recv_into, t: self.clone() })
    }
}

impl Transport for TcpTransport {
    fn manifest(&self) -> &'static Manifest {
        &MANIFEST
    }
    fn scheme(&self) -> &'static str {
        "tcp"
    }
    fn register_region(&self, _region: &RegionInfo) -> Result<(), ErrCode> {
        Ok(())
    }
    fn listen(
        &self,
        pool: MemPool,
        on_conn: Arc<dyn Fn(Box<dyn Conn>) + Send + Sync>,
    ) -> Result<Option<String>, ErrCode> {
        let l = TcpListener::bind(&self.listen).map_err(|e| {
            log::error("tcp", format_args!("监听 {} 失败：{e}", self.listen));
            ErrCode::from(e)
        })?;
        let addr = l.local_addr()?.to_string();
        let me = self.clone();
        let accept = move || {
            for s in l.incoming() {
                match s.map_err(ErrCode::from).and_then(|s| me.wrap(s, pool.clone())) {
                    Ok(c) => on_conn(Box::new(c)),
                    Err(e) => {
                        log::warn("tcp", format_args!("接受连接失败：{e}"));
                        std::thread::sleep(Duration::from_millis(100)); // 例如文件描述符用完：别空转
                    }
                }
            }
        };
        std::thread::Builder::new().name("accept-tcp".into()).spawn(accept).map_err(|_| ErrCode::ENOMEM)?;
        Ok(Some(addr))
    }
    fn connect(&self, addr: &str, pool: MemPool, timeout: Duration) -> Result<Box<dyn Conn>, ErrCode> {
        let sa = addr.to_socket_addrs().ok().and_then(|mut a| a.next()).ok_or(ErrCode::EINVAL)?; // 地址写错：不必重试
        let s = TcpStream::connect_timeout(&sa, timeout.max(Duration::from_millis(1))).map_err(io_err)?;
        Ok(Box::new(self.wrap(s, pool)?))
    }
}

struct TcpConn {
    r: TcpStream, // 读线程用
    w: TcpStream, // 写线程用（同一个 socket 的另一个 fd）
    pool: MemPool,
    peer: String,
    authed: AtomicBool,                  // 握手完成之前只收握手帧（握手帧不带 bulk）
    recv_into: Mutex<HashMap<u64, Buf>>, // req_id → 请求方为回复 bulk 预先分配的缓冲
    t: TcpTransport,                     // 写超时、指标句柄
}

impl Conn for TcpConn {
    fn send(&self, f: OutFrame) -> Result<(), ErrCode> {
        if let Some(b) = f.recv_into {
            self.recv_into.lock().unwrap_or_else(|e| e.into_inner()).insert(f.hdr.req_id, b);
        }
        let deadline = clock::mono_ms() + self.t.write_timeout.as_millis() as u64; // 整帧的写超时，不是每次 write 的
        self.write_by(&f.hdr.encode(), deadline)?;
        self.write_by(&f.body, deadline)?;
        for s in &f.bulk {
            self.write_by(s.as_slice(), deadline)?;
        }
        self.t.bytes_out.add((HEADER_LEN + f.body.len()) as u64 + f.hdr.bulk_len as u64);
        Ok(())
    }

    fn recv(&self) -> Result<InFrame, ErrCode> {
        let mut hb = [0u8; HEADER_LEN];
        (&self.r).read_exact(&mut hb).map_err(io_err)?; // 空闲等帧头的时间不计时（3.7.4）
        let t0 = Instant::now();
        let r = self.read_frame(&hb);
        self.t.recv_hook.exit(t0, r.is_ok());
        r
    }

    fn mark_authenticated(&self) {
        self.authed.store(true, Ordering::SeqCst);
    }

    fn peer(&self) -> String {
        self.peer.clone()
    }

    fn close(&self) {
        let _ = self.w.shutdown(Shutdown::Both); // 阻塞在读或写上的线程会立刻返回错误
    }
}

impl TcpConn {
    /// 帧头之后的部分：校验帧头 → 读 body → 读 bulk（缓冲按 core 的 `bulk_buf` 规则找；找不到就读出丢弃）。
    fn read_frame(&self, hb: &[u8; HEADER_LEN]) -> Result<InFrame, ErrCode> {
        let hdr = FrameHeader::decode(hb)?;
        if hdr.flags & (FLAG_BULK_REMOTE | FLAG_REPLY_REMOTE) != 0 {
            return Err(ErrCode::PROTO_UNSUPPORTED); // tcp 不支持单边描述符
        }
        let n = hdr.bulk_len as usize;
        if n > 0 && !self.authed.load(Ordering::SeqCst) {
            return Err(ErrCode::PROTO_UNSUPPORTED); // 未认证的连接只允许握手帧（不带 bulk）：直接断开，不取池槽（3.3.2）
        }
        let mut r = &self.r;
        let mut body = vec![0u8; hdr.body_len as usize];
        r.read_exact(&mut body).map_err(io_err)?;
        let mine = match hdr.kind {
            FrameKind::Response => self.recv_into.lock().unwrap_or_else(|e| e.into_inner()).remove(&hdr.req_id),
            _ => None,
        };
        let (mut bulk, mut dropped) = (None, false);
        if n > 0 {
            match bulk_buf(&self.pool, &hdr, mine, BULK_WAIT) {
                Ok(mut b) => {
                    r.read_exact(b.as_mut_slice()).map_err(io_err)?;
                    bulk = Some(b);
                }
                Err(_) => {
                    // 内存池满：把这段 bulk 读出来丢掉（保持字节流同步），由 core 回 BUSY，发送方稍后重发
                    let got = std::io::copy(&mut r.take(n as u64), &mut std::io::sink()).map_err(io_err)?;
                    if got != n as u64 {
                        return Err(ErrCode::ECONNRESET);
                    }
                    dropped = true;
                }
            }
        }
        self.t.bytes_in.add((HEADER_LEN + body.len() + n) as u64);
        Ok(InFrame { hdr, body, bulk, bulk_dropped: dropped })
    }

    /// 在 `deadline_ms` 之前写完 `b`。每次 write 之前把 socket 的写超时（SO_SNDTIMEO）设成剩下的时间，所以一帧最多等
    /// `[net] write_timeout_ms`——否则每次阻塞的 write 都能再等满一个写超时，对端每读一点点就又续上 5 s。
    fn write_by(&self, mut b: &[u8], deadline_ms: u64) -> Result<(), ErrCode> {
        while !b.is_empty() {
            let left = deadline_ms.saturating_sub(clock::mono_ms());
            if left == 0 {
                return Err(ErrCode::ETIMEDOUT);
            }
            self.w.set_write_timeout(Some(Duration::from_millis(left)))?;
            match (&self.w).write(b) {
                Ok(0) => return Err(ErrCode::ECONNRESET),
                Ok(n) => b = &b[n..],
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => return Err(io_err(e)),
            }
        }
        Ok(())
    }
}

/// io 错误 → 错误码：读/写超时（Linux 上是 EAGAIN）一律 `ETIMEDOUT`；对端关闭 → `ECONNRESET`。
fn io_err(e: std::io::Error) -> ErrCode {
    use std::io::ErrorKind::*;
    match e.kind() {
        WouldBlock | TimedOut => ErrCode::ETIMEDOUT,
        UnexpectedEof => ErrCode::ECONNRESET,
        _ => ErrCode::from(e),
    }
}

#[cfg(test)]
mod tests;
