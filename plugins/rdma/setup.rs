//! 建连（design.md 3.6.4"建连"行、4.1）：插件自己的引导 TCP 端口（`[plugin.rdma] listen`）上只用插件私有消息 0x0600
//! `RdmaSetup{qpn, psn, gid, mtu}` 交换 QP 信息 → QP 状态迁移 → 包成 `Conn` 交给 core 跑 3.3.2 握手。
//!
//! - **RdmaSetup 帧**（插件两端私有，core 不看；3.6.4 实现注记（M2）②，金样在 setup.golden）：标准 48 字节帧头
//!   （msg_type 0x0600，connect 侧发 Request、accept 侧回 Response，req_id = 1）+ postcard 编码的 `{qpn, psn, gid, mtu}`，
//!   **不含 rkey**（rkey 只随每个请求的描述符给出）。用标准帧头，所以 `FrameHeader::decode` 照样校验 magic、版本、CRC 与长度。
//! - connect 侧：先在本机预检（UDP 套接字 connect 一下拿到本端地址——不发包；sysfs 里没有它对应的 RoCE v2 GID 就立刻
//!   `EINVAL`，RpcClient 不重试；GID 在、只是端口链路不是 ACTIVE 时按连接断开，重连重试）→ TCP 连引导端口 → 建 QP（INIT）、
//!   post 接收槽 → 发 RdmaSetup（Request）→ 读对端的（Response）→ RTR → RTS。
//! - accept 侧：accept 线程只做 accept；每条引导连接起一个 `rdma-setup` 线程，交换期间 socket 读写超时 5 s（一个连上来却
//!   不说话的对端卡不住别的建连，最多占用一个名额 5 s）；读 RdmaSetup → 建 QP、post 接收槽 → RTR → RTS → 回自己的
//!   RdmaSetup（所以对端拿到回复时这边已经能收了）。已有 max_qps 个 QP（含建连中的）时新的引导连接直接关掉。
//! - 失败时 QP 与它的接收槽立刻释放（RdmaConn 被丢弃：先销毁 QP、再还槽）。交换完之后引导 TCP 一直开着（非阻塞，读线程
//!   每秒看一眼）：读到 EOF = 这条 RDMA 连接作废。

use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener, TcpStream, UdpSocket};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use wmfs_core::base::{ErrCode, log};
use wmfs_core::buf::MemPool;
use wmfs_core::hooks::traits::Conn;
use wmfs_core::proto::frame::{FrameHeader, FrameKind, HEADER_LEN, MSG_PLUGIN_FIRST};
use wmfs_core::proto::{decode_body, encode_body};

use super::Shared;
use super::conn::RdmaConn;
use super::params::{RECV_SLOTS, SETUP_TIMEOUT_MS};
use super::wire::LiveGuard;

type OnConn = Arc<dyn Fn(Box<dyn Conn>) + Send + Sync>;

/// 引导 TCP 上交换的 QP 信息（插件私有消息 0x0600，4.1）。`mtu` 是 ibv_mtu 的值（1 = 256 … 5 = 4096）。
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub struct RdmaSetup {
    pub qpn: u32,
    pub psn: u32,
    pub gid: [u8; 16],
    pub mtu: u8,
}

/// RdmaSetup 的 body 最多这么长（postcard：两个 varint u32 + 16 字节 + 1 字节）。
pub const SETUP_MAX_BODY: u32 = 64;

/// 一个完整的 RdmaSetup 帧（帧头 + body）。connect 侧发 Request，accept 侧回 Response。
pub fn setup_frame(kind: FrameKind, m: &RdmaSetup) -> Vec<u8> {
    let body = encode_body(m);
    let mut h = FrameHeader::new(kind, MSG_PLUGIN_FIRST, 1, 0);
    (h.body_len, h.body_crc32c) = (body.len() as u32, crc32c::crc32c(&body));
    let mut out = h.encode().to_vec();
    out.extend_from_slice(&body);
    out
}

/// 校验 RdmaSetup 帧的帧头（magic、版本、CRC、长度、类型、kind、不带 bulk）。
pub fn setup_header(hb: &[u8; HEADER_LEN], want: FrameKind) -> Result<FrameHeader, ErrCode> {
    let h = FrameHeader::decode(hb)?;
    if h.msg_type != MSG_PLUGIN_FIRST || h.kind != want || h.bulk_len != 0 || h.body_len > SETUP_MAX_BODY {
        return Err(ErrCode::PROTO_UNSUPPORTED);
    }
    Ok(h)
}

/// 校验 body 的 CRC 并解码；mtu 必须是 1..=5。
pub fn setup_body(h: &FrameHeader, body: &[u8]) -> Result<RdmaSetup, ErrCode> {
    if crc32c::crc32c(body) != h.body_crc32c {
        return Err(ErrCode::PROTO_UNSUPPORTED);
    }
    let m: RdmaSetup = decode_body(body).map_err(|_| ErrCode::PROTO_UNSUPPORTED)?;
    if !(1..=5).contains(&m.mtu) {
        return Err(ErrCode::PROTO_UNSUPPORTED);
    }
    Ok(m)
}

/// io 错误 → 错误码：读写超时一律 `ETIMEDOUT`；对端关闭 → `ECONNRESET`。
fn io_err(e: std::io::Error) -> ErrCode {
    use std::io::ErrorKind::*;
    match e.kind() {
        WouldBlock | TimedOut => ErrCode::ETIMEDOUT,
        UnexpectedEof => ErrCode::ECONNRESET,
        _ => ErrCode::from(e),
    }
}

/// 一个 socket 的本端 IPv4。
fn local_v4(a: std::io::Result<SocketAddr>) -> Result<Ipv4Addr, ErrCode> {
    match a.map_err(io_err)? {
        SocketAddr::V4(a) => Ok(*a.ip()),
        SocketAddr::V6(_) => Err(ErrCode::EINVAL),
    }
}

/// 去 `to` 的路由用哪个本端地址（UDP 的 connect 只让内核选路由，不发任何包）。
fn route_to(to: SocketAddrV4) -> Result<Ipv4Addr, ErrCode> {
    let u = UdpSocket::bind("0.0.0.0:0").map_err(io_err)?;
    u.connect(to).map_err(io_err)?;
    local_v4(u.local_addr())
}

/// 读一个 RdmaSetup 帧（帧头 + body），必须是 `want` 这种 kind。
fn read_setup(s: &TcpStream, want: FrameKind) -> Result<RdmaSetup, ErrCode> {
    let mut hb = [0u8; HEADER_LEN];
    (&*s).read_exact(&mut hb).map_err(io_err)?;
    let h = setup_header(&hb, want)?;
    let mut body = vec![0u8; h.body_len as usize];
    (&*s).read_exact(&mut body).map_err(io_err)?;
    setup_body(&h, &body)
}

/// connect 侧建连（见文件头）。本机没有能用于这个地址的设备 → `EINVAL`；其余失败按"连接断开"（`ECONNRESET` 等）。
pub fn dial(shared: &Arc<Shared>, to: SocketAddrV4, pool: MemPool, timeout: Duration) -> Result<RdmaConn, ErrCode> {
    let local = route_to(to)?;
    if let Err(e) = shared.provider.route(local) {
        log::warn("rdma", format_args!("连不了 rdma://{to}：{}", e.why));
        // 本机根本没有这个地址的设备 → EINVAL（配置错了，RpcClient 不重试）；只是端口链路暂时不是 ACTIVE → 重连重试
        return Err(if e.link_down { ErrCode::ECONNRESET } else { ErrCode::EINVAL });
    }
    let live = shared.live.try_take(shared.cfg.max_qps).ok_or_else(|| {
        log::warn(
            "rdma",
            format_args!("连不了 rdma://{to}：本进程已有 {} 个 RDMA 连接（max_qps）", shared.live.count()),
        );
        ErrCode::BUSY
    })?;
    // 建 TCP、写、读这几步各自最多等这么久（不是三步合计）
    let step_timeout = timeout.clamp(Duration::from_millis(1), Duration::from_millis(SETUP_TIMEOUT_MS));
    let s = TcpStream::connect_timeout(&SocketAddr::V4(to), step_timeout).map_err(io_err)?;
    s.set_read_timeout(Some(step_timeout)).map_err(io_err)?;
    s.set_write_timeout(Some(step_timeout)).map_err(io_err)?;
    s.set_nodelay(true).map_err(io_err)?;
    let local = local_v4(s.local_addr())?;
    let (wire, ev) = shared.provider.create_qp(local, false).map_err(|m| {
        log::warn("rdma", format_args!("连不了 rdma://{to}：{m}"));
        ErrCode::ECONNRESET
    })?;
    let c = RdmaConn::new(wire, ev, pool, s, false, shared.clone(), live)?;
    (&c.boot).write_all(&setup_frame(FrameKind::Request, &c.local())).map_err(io_err)?;
    let remote = read_setup(&c.boot, FrameKind::Response)?;
    c.connect(&remote)?;
    c.boot.set_nonblocking(true).map_err(io_err)?;
    Ok(c)
}

/// accept 侧：起 `accept-rdma` 线程，它只做 accept；每条引导连接交给一个 `rdma-setup` 线程建连。
pub fn serve(shared: Arc<Shared>, l: TcpListener, pool: MemPool, on_conn: OnConn) -> Result<(), ErrCode> {
    let accept = move || {
        for s in l.incoming() {
            let s = match s {
                Ok(s) => s,
                Err(e) => {
                    log::warn("rdma", format_args!("接受引导连接失败：{e}"));
                    std::thread::sleep(Duration::from_millis(100)); // 例如文件描述符用完：别空转
                    continue;
                }
            };
            let Some(live) = shared.live.try_take(shared.cfg.max_qps) else {
                let who = s.peer_addr().map(|a| a.to_string()).unwrap_or_default();
                log::warn(
                    "rdma",
                    format_args!("已有 {} 个 RDMA 连接（max_qps）：关掉来自 {who} 的引导连接", shared.live.count()),
                );
                continue; // s 在这里被丢弃 = 关掉
            };
            let (shared2, pool2, on2) = (shared.clone(), pool.clone(), on_conn.clone());
            let setup = move || match answer(&shared2, s, pool2, live) {
                Ok(c) => on2(Box::new(c)),
                Err(e) => log::info("rdma", format_args!("RDMA 建连失败：{e}")),
            };
            if let Err(e) = std::thread::Builder::new().name("rdma-setup".into()).spawn(setup) {
                log::warn("rdma", format_args!("起建连线程失败：{e}"));
            }
        }
    };
    std::thread::Builder::new().name("accept-rdma".into()).spawn(accept).map_err(|_| ErrCode::ENOMEM)?;
    Ok(())
}

/// accept 侧的一条引导连接（在 `rdma-setup` 线程上）。错误是给日志看的中文。
fn answer(shared: &Arc<Shared>, s: TcpStream, pool: MemPool, live: LiveGuard) -> Result<RdmaConn, String> {
    let peer = s.peer_addr().map(|a| a.to_string()).unwrap_or_else(|_| "?".into());
    let to = Some(Duration::from_millis(SETUP_TIMEOUT_MS));
    let cfg = s.set_read_timeout(to).and_then(|()| s.set_write_timeout(to)).and_then(|()| s.set_nodelay(true));
    cfg.map_err(|e| format!("{peer}：设置引导连接失败：{e}"))?;
    let remote =
        read_setup(&s, FrameKind::Request).map_err(|e| format!("{peer}：没有在 5 s 内发来正确的 RdmaSetup（{e}）"))?;
    let local = local_v4(s.local_addr()).map_err(|e| format!("{peer}：引导连接不是 IPv4（{e}）"))?;
    let (wire, ev) = shared.provider.create_qp(local, true).map_err(|m| format!("{peer}：{m}"))?;
    let c = RdmaConn::new(wire, ev, pool, s, true, shared.clone(), live).map_err(|e| match e {
        ErrCode::BUSY => format!("{peer}：取不到 {} 个 S 档槽位（接收槽 + 发送暂存）", RECV_SLOTS + 1),
        e => format!("{peer}：post 接收槽失败（{e}）"),
    })?;
    c.connect(&remote).map_err(|e| format!("{peer}：QP 进 RTR/RTS 失败（{e}）"))?;
    (&c.boot)
        .write_all(&setup_frame(FrameKind::Response, &c.local()))
        .and_then(|()| c.boot.set_nonblocking(true))
        .map_err(|e| format!("{peer}：回 RdmaSetup 失败：{e}"))?;
    Ok(c)
}
