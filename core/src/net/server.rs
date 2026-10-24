//! RPC 服务端（design.md 1.2、3.3.3、3.12）：accept、每连接读/写线程、握手、角色权限表、分发，以及回复句柄 `Responder`。
//!
//! 每个传输插件 `listen` 交出的连接都由 core 起读线程 `conn-<id>` 与写线程 `conn-w-<id>`（`net/conn.rs`）。读线程先跑
//! 3.3.2 握手，之后循环收帧 → 核对角色权限（请求与 OneWay 都查：不符的请求回 `EACCES`，不符的 OneWay 丢弃，都计数
//! `wmfs_rpc_denied_total`）→ 按 msg_type 分发：Ping 由 core 自己原样回显；其他族交给注册的 [`Service`]（M1 还没有；
//! 没有服务的族 → `FEATURE_UNSUPPORTED`，不断开）。读线程**自己从不执行请求**：服务只把请求连同 `Responder` 交给别的线程。
//!
//! `Responder` 按值传递、恰好用一次；回复只是 `try_send` 进连接的发送队列（队列满就断开这条连接，对端会重试），
//! 所以回复永远不会阻塞执行器或 io 线程。被 Drop 而没有回复时自动回 `EIO` 并计数 `wmfs_req_dropped_total`；
//! 流式回复的 [`StreamTx`] 没发最后一帧就被 Drop 时也一样。回复超过协议上限时改回 `FEATURE_UNSUPPORTED`（只让这一个请求失败）。
//! 实现注记（M4）：回 `NOT_LEADER` 的 ERROR 帧由 core 自动填 `leader_hint`（[`RpcServer::set_leader_hint`] 登记的函数给出
//! leader 在 control_scheme 上的 rpc 地址，例如 "127.0.0.1:19100"；不知道 leader 时为 None）；[`Responder::drop_reply`]
//! 什么也不回、Drop 时也不自动回 EIO（fail point `meta.reply.drop` 用它"弄丢"一次回复）。
//! 实现注记（M8）：OneWay 帧（RaftMsg）交给 [`RpcServer::add_oneway`] 登记的处理函数（在连接读线程上调用，不得阻塞；bulk 被丢弃的
//! OneWay 直接丢掉，raft 会重发）；[`Responder::error_with`] 带一句给人看的原因（`ErrorBody.msg`，例如 join 被拒时的插件差异；`NOT_LEADER` 时
//! 照样自动填 leader_hint——join 发给 follower 也能跟到 leader）；
//! [`StreamTx::wait_room`] 让流的发送方（snap-serve）等发送队列腾出地方，不会把整个快照一下子塞进队列。

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::Instant;

use wmfs_api::metrics::{self, Counter, Histogram};

use crate::base::config::NetConfig;
use crate::base::types::Role;
use crate::base::{ErrCode, log};
use crate::buf::{BufSlice, MemPool};
use crate::hooks::manifest::Hook;
use crate::hooks::pipeline::{HookCall, Pipeline};
use crate::hooks::traits::{Conn, ConnIdentity, KeyLookup, Transport};
use crate::net::conn::{self, ConnHandle, ConnOpts, recv_checked};
use crate::net::handshake::{self, ServerSide};
use crate::proto::frame::{
    ErrorBody, FLAG_ERROR, FrameHeader, FrameKind, InFrame, MSG_DATA_CALL, MSG_META_CALL, MSG_NODE_CALL, MSG_PEER_CALL,
    MSG_PING, MSG_RAFT, MSG_SNAPSHOT_FETCH, OutFrame, msg_name,
};
use crate::proto::handshake::PingBody;
use crate::proto::peer::NodeOp;
use crate::proto::{decode_body, encode_body};

mod stream;
pub use stream::StreamTx;

/// 一个消息族的服务（M3 起：元数据、数据、节点……）。在连接读线程上调用，**不得阻塞**：只把请求交给别的线程。
pub trait Service: Send + Sync {
    fn call(&self, f: InFrame, r: Responder);
}

/// 本节点在握手里报出的身份。
#[derive(Clone, Copy, Debug)]
pub struct ServerId {
    pub cluster_id: [u8; 16],
    pub node_id: u32,
}

/// 给出 leader 的 rpc 地址（填进 NOT_LEADER 的 `leader_hint`；不知道 → None）。
pub type HintFn = Arc<dyn Fn() -> Option<String> + Send + Sync>;

/// 一种 OneWay 帧的处理函数（M8：RaftMsg）：在连接读线程上调用，不得阻塞（只 try_send 进某个队列）。
pub type OneWayFn = Arc<dyn Fn(InFrame) + Send + Sync>;

/// 各自计数的消息族（3.3.3）；其余的 msg_type（不认识的、认证之后又发来的握手消息）都记在 "Unknown" 名下。
const FAMILIES: [u16; 7] =
    [MSG_PING, MSG_META_CALL, MSG_DATA_CALL, MSG_PEER_CALL, MSG_NODE_CALL, MSG_RAFT, MSG_SNAPSHOT_FETCH];

/// RPC 服务端：一个节点一个，可以在多个传输上监听。
pub struct RpcServer {
    id: ServerId,
    pipeline: Arc<Pipeline>,
    keys: Arc<dyn KeyLookup + Send + Sync>,
    queue: usize,
    services: RwLock<HashMap<u16, Arc<dyn Service>>>,
    oneway: RwLock<HashMap<u16, OneWayFn>>,
    stats: [OnceLock<Arc<FamilyStats>>; FAMILIES.len() + 1], // FAMILIES 各一个 + Unknown；第一次用到时注册，之后不加锁
    hint: OnceLock<HintFn>,
}

impl RpcServer {
    pub fn new(
        id: ServerId,
        pipeline: Arc<Pipeline>,
        keys: Arc<dyn KeyLookup + Send + Sync>,
        net: &NetConfig,
    ) -> Arc<RpcServer> {
        conn::counters(); // 连接层的计数器现在就注册（新节点的 /metrics 里也有，值为 0）
        Arc::new(RpcServer {
            id,
            pipeline,
            keys,
            queue: net.send_queue as usize,
            services: RwLock::new(HashMap::new()),
            oneway: RwLock::new(HashMap::new()),
            stats: Default::default(),
            hint: OnceLock::new(),
        })
    }

    /// 登记"leader 在哪"的函数：之后回 `NOT_LEADER` 的 ERROR 帧都带上它给的 `leader_hint`（只登记一次，再登记被忽略）。
    pub fn set_leader_hint(&self, f: HintFn) {
        let _ = self.hint.set(f);
    }

    /// 注册一个消息族的服务（例如 `MSG_META_CALL`）。
    pub fn add_service(&self, msg_type: u16, s: Arc<dyn Service>) {
        self.services.write().unwrap_or_else(|e| e.into_inner()).insert(msg_type, s);
    }

    /// 登记一种 OneWay 帧的处理函数（例如 `MSG_RAFT`）。
    pub fn add_oneway(&self, msg_type: u16, f: OneWayFn) {
        self.oneway.write().unwrap_or_else(|e| e.into_inner()).insert(msg_type, f);
    }

    /// 在一个传输上开始监听；返回要写进 `NodeRecV1.addrs` 的地址（None = 这个传输在本节点不监听）。
    pub fn listen(self: &Arc<Self>, t: &Arc<dyn Transport>, pool: MemPool) -> Result<Option<String>, ErrCode> {
        let hook = Arc::new(HookCall::new(t.manifest(), Hook::Transport));
        let me = self.clone();
        t.listen(pool, Arc::new(move |c| me.accept(c, hook.clone())))
    }

    /// 传输插件的 accept 线程调用：起这条连接的读/写线程后立刻返回。
    fn accept(self: &Arc<Self>, c: Box<dyn Conn>, hook: Arc<HookCall>) {
        let me = self.clone();
        // 服务端不发请求：回复帧超过传输上限时写线程自己改回 ERROR 帧，用不到 on_unsent
        let opts = ConnOpts { queue: self.queue, keepalive: false, hook, on_unsent: None };
        if let Err(e) = conn::spawn(c, opts, Box::new(move |h, c| me.serve(h, c))) {
            log::warn("rpc", format_args!("起连接线程失败：{e}"));
        }
    }

    /// 读线程：握手 → 收帧分发，直到连接断开。
    fn serve(&self, h: ConnHandle, c: Arc<dyn Conn>) {
        let side = ServerSide {
            cluster_id: self.id.cluster_id,
            node_id: self.id.node_id,
            pipeline: &self.pipeline,
            keys: &*self.keys,
        };
        let id = match handshake::server(&h, &*c, &side) {
            Ok(id) => id,
            Err(e @ (ErrCode::AUTH_FAILED | ErrCode::PROTO_UNSUPPORTED)) => {
                log::info("rpc", format_args!("拒绝来自 {} 的连接：{e}", h.peer()));
                return; // ERROR 帧（如果该回）已在队列里；丢掉句柄后写线程写完它再关闭连接
            }
            Err(e) => {
                log::debug("rpc", format_args!("来自 {} 的连接在握手时断开：{e}", h.peer()));
                return;
            }
        };
        log::debug("rpc", format_args!("{} 已认证：key {}，角色 {}", h.peer(), id.key_id, id.role.name()));
        loop {
            match recv_checked(&*c) {
                Ok(f) => self.dispatch(&h, &id, f),
                Err(e) => {
                    log::debug("rpc", format_args!("连接 {} 断开：{e}", h.peer()));
                    break;
                }
            }
        }
        h.close();
    }

    fn dispatch(&self, h: &ConnHandle, id: &ConnIdentity, f: InFrame) {
        let hdr = f.hdr;
        if matches!(hdr.kind, FrameKind::Response | FrameKind::StreamChunk) {
            return; // 服务端从不向客户端发请求，所以不该收到回复与流帧：忽略
        }
        // 角色权限先查（请求与 OneWay 都查）：OneWay 没法回 EACCES，不符就只计数、丢弃
        let family = msg_name(hdr.msg_type).unwrap_or("Unknown");
        let hint = self.hint.get().cloned();
        if !allowed(id.role, hdr.msg_type, &f.body) {
            denied(id.role, family);
            if hdr.kind == FrameKind::Request {
                Responder::new(h.clone(), &hdr, self.stats(hdr.msg_type), hint).error(ErrCode::EACCES, 0);
            }
            return;
        }
        if hdr.kind == FrameKind::OneWay {
            let handler = self.oneway.read().unwrap_or_else(|e| e.into_inner()).get(&hdr.msg_type).cloned();
            if let Some(on_frame) = handler.filter(|_| !f.bulk_dropped) {
                on_frame(f); // bulk 被丢弃的、没有处理函数的：丢掉（raft 会重发）
            }
            return;
        }
        let r = Responder::new(h.clone(), &hdr, self.stats(hdr.msg_type), hint);
        if f.bulk_dropped {
            return r.error(ErrCode::BUSY, 0); // 内存池 100 ms 内没有空槽：bulk 已读出丢弃，让对端退避重试
        }
        if hdr.msg_type == MSG_PING {
            return ping(f, r);
        }
        let svc = self.services.read().unwrap_or_else(|e| e.into_inner()).get(&hdr.msg_type).cloned();
        match svc {
            Some(s) => s.call(f, r),
            None => r.error(ErrCode::FEATURE_UNSUPPORTED, 0), // 不认识的 msg_type，或本节点没有这个服务：不断开
        }
    }

    /// 这个 msg_type 的指标句柄：第一次用到时注册（只这一次加注册表的锁），之后只是读 OnceLock。
    fn stats(&self, msg_type: u16) -> Arc<FamilyStats> {
        let i = FAMILIES.iter().position(|t| *t == msg_type).unwrap_or(FAMILIES.len());
        let family = if i < FAMILIES.len() { msg_name(msg_type).unwrap_or("Unknown") } else { "Unknown" };
        self.stats[i].get_or_init(|| Arc::new(FamilyStats::new(family))).clone()
    }
}

/// 角色权限表（3.3.3）：Ping、MetaCall、DataCall 谁都可以；NodeCall 非 Server 只能 GetClusterMap；
/// PeerCall、RaftMsg、SnapshotFetch 只有 Server（key 0）。
fn allowed(role: Role, msg_type: u16, body: &[u8]) -> bool {
    match msg_type {
        MSG_PEER_CALL | MSG_RAFT | MSG_SNAPSHOT_FETCH => role == Role::Server,
        MSG_NODE_CALL => {
            role == Role::Server || matches!(decode_body::<NodeOp>(body), Ok(NodeOp::GetClusterMap { .. }))
        }
        _ => true,
    }
}

fn denied(role: Role, family: &'static str) {
    let l = [("role", role.name()), ("family", family)];
    metrics::counter("wmfs_rpc_denied_total", "角色权限表拒绝的请求数", &l).inc();
}

/// Ping：`PingBody` + 可选 bulk，原样回显（bulk 用收到的那个池缓冲，零拷贝）。
fn ping(f: InFrame, r: Responder) {
    if decode_body::<PingBody>(&f.body).is_err() {
        return r.error(ErrCode::FEATURE_UNSUPPORTED, 0);
    }
    let bulk = f.bulk.map(|b| vec![b.freeze().whole()]).unwrap_or_default();
    r.reply(f.body, bulk);
}

/// 一个消息族的服务端指标：`wmfs_rpc_requests_total{family,op,code}` 与 `wmfs_rpc_latency_seconds{family,op}`
/// （3.11：注册时加锁，热路径只做原子加）。code = OK 的计数器与直方图在创建时注册；某个错误码第一次出现时才注册它的计数器，
/// 之后从本族自己的小表里取（这把锁只在错误回复上用）。实现注记（M1）：`op` 暂时等于 family，各服务解出具体操作后再细分。
struct FamilyStats {
    family: &'static str,
    ok: Arc<Counter>,
    latency: Arc<Histogram>,
    errors: Mutex<Vec<(ErrCode, Arc<Counter>)>>,
}

impl FamilyStats {
    fn new(family: &'static str) -> FamilyStats {
        let l = [("family", family), ("op", family)];
        let latency =
            metrics::histogram("wmfs_rpc_latency_seconds", "服务端处理请求的耗时", &l, metrics::SECONDS_BUCKETS);
        FamilyStats { family, ok: requests(family, "OK"), latency, errors: Mutex::new(Vec::new()) }
    }

    /// 记一个处理完的请求（`code` = 0 表示成功），耗时从收到请求算起。
    fn record(&self, code: ErrCode, t0: Instant) {
        self.latency.observe(t0.elapsed().as_secs_f64());
        if code == ErrCode(0) {
            return self.ok.inc();
        }
        let mut errs = self.errors.lock().unwrap_or_else(|e| e.into_inner());
        match errs.iter().find(|(c, _)| *c == code) {
            Some((_, h)) => h.inc(),
            None => {
                let h = requests(self.family, &code.to_string());
                h.inc();
                errs.push((code, h));
            }
        }
    }
}

fn requests(family: &'static str, code: &str) -> Arc<Counter> {
    let l = [("family", family), ("op", family), ("code", code)];
    metrics::counter("wmfs_rpc_requests_total", "服务端处理的请求数", &l)
}

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
}

impl Responder {
    fn new(conn: ConnHandle, req: &FrameHeader, stats: Arc<FamilyStats>, hint: Option<HintFn>) -> Responder {
        let (req_id, msg_type, trace_id, span_id) = (req.req_id, req.msg_type, req.trace_id, req.span_id);
        Responder { conn, req_id, msg_type, trace_id, span_id, done: false, stats, t0: Instant::now(), hint }
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
fn error_frame(mut hdr: FrameHeader, body: ErrorBody) -> OutFrame {
    (hdr.kind, hdr.flags) = (FrameKind::Response, FLAG_ERROR);
    OutFrame { hdr, body: encode_body(&body), bulk: vec![], recv_into: None }
}
