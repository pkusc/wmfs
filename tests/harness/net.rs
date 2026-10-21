//! RPC 组件测试的公共部分：在 tcp（127.0.0.1:0）或 mem 上起一个带 shared-key 认证的 RPC 服务端，造客户端与 Ping 帧；
//! 另有用裸 TCP 手工收发帧的工具：手工完成握手的"假客户端"（慢客户端、坏 CRC 等用例：它握手之后想怎么发就怎么发），
//! 以及"假服务端"一侧的握手（测客户端：server_proof 不对、Challenge 回得很慢）。

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

use plugin_mem_transport::MemTransport;
use wmfs_core::api::types::KeyFile;
use wmfs_core::base::ErrCode;
use wmfs_core::base::config::{NetConfig, PluginTable};
use wmfs_core::base::types::Role;
use wmfs_core::buf::{Buf, FrozenBuf, Lane, MemPool, PoolConfig};
use wmfs_core::hooks::pipeline::Pipeline;
use wmfs_core::hooks::registry::PluginSet;
use wmfs_core::hooks::traits::Transport;
use wmfs_core::init::InitKeys;
use wmfs_core::net::client::{Endpoint, RpcClient};
use wmfs_core::net::server::{RpcServer, ServerId};
use wmfs_core::proto::frame::{
    ErrorBody, FLAG_ERROR, FrameHeader, FrameKind, HEADER_LEN, MSG_AUTH, MSG_CHALLENGE, MSG_HELLO, MSG_PING,
    MSG_WELCOME, OutFrame,
};
use wmfs_core::proto::handshake::{
    Auth, Challenge, Hello, PingBody, ProofInput, Welcome, auth_proof, derive_secret, server_proof,
};
use wmfs_core::proto::{decode_body, encode_body};
use wmfs_plugins::shared_key::SharedKey;
use wmfs_plugins::tcp::TcpTransport;

pub const MASTER: [u8; 32] = [0x5e; 32];
pub const CLUSTER: [u8; 16] = [0xc1; 16];
/// 测试里 RPC 的单次超时。
pub const T: Duration = Duration::from_secs(10);

/// 测试用的小池（≈ 34 MiB）：S 1 MiB、M 4 槽、L 1 槽。
pub fn pool() -> MemPool {
    MemPool::new(&PoolConfig { s_mb: 1, m_slots: 4, l_slots: 1, repl_lane: true }).unwrap()
}

/// 一个跑在某个传输上的测试服务端。
pub struct TestServer {
    pub rpc: Arc<RpcServer>,
    pub ep: Endpoint,
    pub addr: String,
    pub scheme: &'static str,
    pub pool: MemPool,
}

/// 在 tcp 的 127.0.0.1:0 上起服务端（`[net]` 取默认值：写超时 5 s、发送队列 1024 帧）。
pub fn tcp() -> TestServer {
    tcp_with(NetConfig::default())
}

/// 同 [`tcp`]，`[net]` 用给定的值（例如很小的发送队列）。
pub fn tcp_with(net: NetConfig) -> TestServer {
    let cfg: PluginTable = "listen = \"127.0.0.1:0\"".parse().unwrap();
    start(Arc::new(TcpTransport::new(&cfg, net.write_timeout_ms).unwrap()), &net)
}

/// 在进程内的名字 `name` 上起 mem 服务端。
pub fn mem(name: &str) -> TestServer {
    start(Arc::new(MemTransport::listening(name)), &NetConfig::default())
}

fn start(t: Arc<dyn Transport>, net: &NetConfig) -> TestServer {
    let scheme = t.scheme();
    let mut set = PluginSet::new(&[]);
    set.transport(t);
    set.authn(Arc::new(SharedKey::new()));
    set.validate(&BTreeMap::new()).unwrap();
    let pipeline = Arc::new(Pipeline::new(set));
    let keys = Arc::new(InitKeys::new(MASTER, 0));
    let rpc = RpcServer::new(ServerId { cluster_id: CLUSTER, node_id: 1 }, pipeline.clone(), keys, net);
    let pool = pool();
    let addr = rpc.listen(&pipeline.transports()[0], pool.clone()).unwrap().unwrap();
    let ep = Endpoint { node: 1, addrs: vec![(scheme.to_string(), addr.clone())] };
    TestServer { rpc, ep, addr, scheme, pool }
}

/// key `id` 的正确密钥（init 的约定：1 = admin、2 = client；0 = 服务端之间）。
pub fn key(id: u32) -> KeyFile {
    KeyFile { key_id: id, secret: derive_secret(&MASTER, id) }
}

impl TestServer {
    /// 连这个服务端的客户端（控制与数据都走它的 scheme）。
    pub fn client(&self, key: KeyFile, role: Role) -> Arc<RpcClient> {
        client(self.scheme, key, role)
    }
}

/// 一个只用 `scheme`（"tcp" 或 "mem"）这一种传输的客户端。
pub fn client(scheme: &str, key: KeyFile, role: Role) -> Arc<RpcClient> {
    RpcClient::new_with_role(pool(), vec![transport(scheme)], key, role, net(scheme)).unwrap()
}

/// 客户端用的传输（不监听）："tcp" 或 "mem"。
pub fn transport(scheme: &str) -> Arc<dyn Transport> {
    if scheme == "tcp" {
        Arc::new(TcpTransport::new(&PluginTable::new(), NetConfig::default().write_timeout_ms).unwrap())
    } else {
        Arc::new(MemTransport::new())
    }
}

pub fn net(scheme: &str) -> NetConfig {
    NetConfig { control_scheme: scheme.into(), data_scheme: scheme.into(), ..NetConfig::default() }
}

/// 一个请求帧（body = PingBody{payload}；msg_type 可以不是 Ping，用来测角色权限表）。
pub fn frame(msg: u16, payload: &[u8]) -> OutFrame {
    let body = encode_body(&PingBody { payload: payload.to_vec() });
    OutFrame { hdr: FrameHeader::new(FrameKind::Request, msg, 0, 0), body, bulk: vec![], recv_into: None }
}

/// 带 bulk 的 Ping：发 `data` 整段，回复读进 `into`。
pub fn bulk_ping(data: &FrozenBuf, into: Buf) -> OutFrame {
    OutFrame { bulk: vec![data.whole()], recv_into: Some(into), ..frame(MSG_PING, b"bulk") }
}

/// 从池里取一个填满确定内容的缓冲并冻结。
pub fn pattern(p: &MemPool, n: usize) -> FrozenBuf {
    let mut b = p.get(n, Lane::Normal, T).unwrap();
    b.as_mut_slice().iter_mut().enumerate().for_each(|(i, x)| *x = (i * 7 % 251) as u8);
    b.freeze()
}

// ---- 裸 TCP 的"假客户端"：自己编帧、自己握手（用来模拟不读 socket 的坏客户端）----

/// 把一帧（帧头长度与 CRC 填好）写进裸 socket。
pub fn raw_send(s: &mut TcpStream, kind: FrameKind, msg: u16, req_id: u64, body: &[u8]) -> std::io::Result<()> {
    let mut h = FrameHeader::new(kind, msg, req_id, 0);
    h.body_len = body.len() as u32;
    h.body_crc32c = crc32c::crc32c(body);
    raw_write(s, &h, body, &[])
}

/// 把帧头（长度与 CRC 由调用方填，可以故意填错）、body、bulk 原样写进裸 socket。
pub fn raw_write(s: &mut TcpStream, h: &FrameHeader, body: &[u8], bulk: &[u8]) -> std::io::Result<()> {
    s.write_all(&h.encode())?;
    s.write_all(body)?;
    s.write_all(bulk)
}

/// 从裸 socket 读一帧（只支持没有 bulk 的帧）。
pub fn raw_recv(s: &mut TcpStream) -> Result<(FrameHeader, Vec<u8>), ErrCode> {
    let mut hb = [0u8; HEADER_LEN];
    s.read_exact(&mut hb).map_err(|_| ErrCode::ECONNRESET)?;
    let h = FrameHeader::decode(&hb)?;
    let mut body = vec![0u8; h.body_len as usize];
    s.read_exact(&mut body).map_err(|_| ErrCode::ECONNRESET)?;
    Ok((h, body))
}

/// 一个各字段都合法的 Hello（测试按需改掉其中一项）。
pub fn hello(key: &KeyFile, role: Role) -> Hello {
    Hello {
        proto_min: 1,
        proto_max: 1,
        features: 0,
        required: 0,
        role: role.as_u8(),
        cluster_id: [0; 16],
        key_id: key.key_id,
        client_nonce: [0x11; 16],
        software: "raw-test".into(),
    }
}

/// 发 Hello、读回 Challenge；服务端回 ERROR 时返回它的错误码。
pub fn raw_hello(s: &mut TcpStream, h: &Hello) -> Result<Challenge, ErrCode> {
    raw_send(s, FrameKind::Request, MSG_HELLO, 1, &encode_body(h)).map_err(|_| ErrCode::ECONNRESET)?;
    let (hdr, body) = raw_recv(s)?;
    if hdr.flags & FLAG_ERROR != 0 {
        return Err(ErrCode(decode_body::<ErrorBody>(&body)?.code));
    }
    decode_body(&body)
}

/// 用裸 socket 完成 3.3.2 握手（客户端一侧），之后这条连接就只由测试自己读写。
pub fn raw_handshake(s: &mut TcpStream, key: &KeyFile, role: Role) -> Result<(), ErrCode> {
    let h = hello(key, role);
    let ch = raw_hello(s, &h)?;
    let p = ProofInput {
        client_nonce: &h.client_nonce,
        server_nonce: &ch.server_nonce,
        role: h.role,
        cluster_id: &h.cluster_id,
        key_id: key.key_id,
    };
    let auth = Auth { proof: auth_proof(&key.secret, &p) };
    raw_send(s, FrameKind::Request, MSG_AUTH, 2, &encode_body(&auth)).map_err(|_| ErrCode::ECONNRESET)?;
    let (hdr, body) = raw_recv(s)?;
    if hdr.flags & FLAG_ERROR != 0 {
        return Err(ErrCode(decode_body::<ErrorBody>(&body)?.code));
    }
    decode_body::<Welcome>(&body).map(|_| ())
}

/// "假服务端"读到客户端的 Hello（`hello_req` 是它的 req_id）之后的握手：回 Challenge → 读 Auth → 回 Welcome。
/// `good_proof = false` 时 Welcome 里的 server_proof 故意是错的（客户端应该拒绝这个冒充的服务端）。
pub fn fake_server_finish(
    s: &mut TcpStream,
    key: &KeyFile,
    hello: &Hello,
    hello_req: u64,
    good_proof: bool,
) -> Result<(), ErrCode> {
    let server_nonce = [0x22; 16];
    let ch = Challenge { proto: 1, features: 0, cluster_id: CLUSTER, node_id: 1, server_nonce };
    raw_send(s, FrameKind::Response, MSG_CHALLENGE, hello_req, &encode_body(&ch)).map_err(|_| ErrCode::ECONNRESET)?;
    let (h, _auth) = raw_recv(s)?;
    let proof = if good_proof { server_proof(&key.secret, &server_nonce, &hello.client_nonce) } else { [0; 32] };
    let w = Welcome { key_id: hello.key_id, role: hello.role, tenant: 0, server_proof: proof };
    raw_send(s, FrameKind::Response, MSG_WELCOME, h.req_id, &encode_body(&w)).map_err(|_| ErrCode::ECONNRESET)
}
