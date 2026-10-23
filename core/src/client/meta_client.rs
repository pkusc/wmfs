//! `MetaClient`：FUSE 层与 CLI 用；跟随 leader、seq、按 3.3.8 重试（design.md 3.12、3.5.2、3.3.7）。
//!
//! - **会话**：`connect` = GetClusterMap + SessionOpen（session id 由客户端随机生成）；`close` = SessionClose。
//! - **seq**：每个修改型请求分配单调递增的 seq，**重试必须用同一个 seq**（服务端的去重表保证恰好执行一次，3.5.2）。
//!   只读请求 seq = 0。在途窗口**按 seq 计**（[`Window`]）：新 seq 必须小于"最小的在途 seq + 256"（`DEDUP_WINDOW`），
//!   到了就等。服务端写入 seq 的结果时点删 seq − 256 的去重记录（3.4.1.4 规则 4），所以只数个数不够：一个回复丢了、
//!   正在等 10 s 超时的请求，会被同一会话里别的线程的 256 个新请求挤掉它的去重记录，重试时就会重新执行、回出假的
//!   `EEXIST`。按 seq 计时，所有在途请求的 seq 都 > 新 seq − 256，它们的记录一定还在。
//! - **跟随 leader**：回 `NOT_LEADER` 且带 `leader_hint`（leader 在 control_scheme 上的 rpc 地址）→ 换到它立即重试
//!   （连续跟随最多 [`MAX_HINT_HOPS`] 次，之后照常退避）；没有 hint → 退避后换下一个已知节点（`connect` 给的种子与
//!   集群图里的节点）。实现注记（M4）：core 里没有 HTTP 客户端，"按 health 找 leader"由调用方（CLI）在 `connect` 之前做，
//!   给出种子；之后没有 hint 时在认识的节点之间轮换（不是 leader 的节点回 `NOT_LEADER`，换下一个）。
//! - **重试**（3.3.8）：可重试的错误（1000–1007、1019、`ECONNRESET`、`ETIMEDOUT`）按指数退避重试，10 ms 起、×2、上限 1 s，
//!   直到总期限（[`MetaClientOpts::op_timeout`]：CLI 30 s，挂载参数 `--op-timeout` 默认 120 s）。每次重试写一行 INFO 日志
//!   （`wmfs fs --verbose` 能看到）。超时之后的重试成功时，回复里的 ctime 早于重试的时刻 = 第一次其实已经执行、这是去重表
//!   返回的原结果，日志写"超时重试 seq=… → 去重命中"（判断用两边的墙钟，跨机器时要求时钟误差远小于一个 RPC 超时，
//!   只影响这一行提示；协议里没有"去重命中"的标志，3.3.4）。

use std::collections::BTreeSet;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use crate::base::{ErrCode, clock, lock, log, rand, sys};
use crate::net::client::{Endpoint, Family, RpcClient};
use crate::net::retry::{BACKOFF_MAX_MS, BACKOFF_START_MS, retryable};
use crate::proto::frame::{FrameHeader, FrameKind, InFrame, MSG_META_CALL, MSG_NODE_CALL, OutFrame};
use crate::proto::meta::{DEDUP_WINDOW, MetaCall, MetaOp, MetaReply, ReqHead};
use crate::proto::peer::{ClusterMap, NodeOp, NodeReply};
use crate::proto::{decode_body, encode_body};

/// 连续跟随 `leader_hint` 的次数上限（之后照常退避，防止两个节点互相指来指去）。
const MAX_HINT_HOPS: u32 = 3;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Caller {
    pub uid: u32,
    pub gid: u32,
    pub groups: Vec<u32>,
    pub pid: u32,
    pub job_id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionInfo {
    pub client_name: String,
    pub host: String,
    pub host_id: String,
    pub mount: String,
}

/// 超时参数（3.3.8）：单次 RPC 10 s；总期限默认 120 s（挂载参数 `--op-timeout`），CLI 用更短的。
#[derive(Clone, Copy, Debug)]
pub struct MetaClientOpts {
    pub rpc_timeout: Duration,
    pub op_timeout: Duration,
}

impl Default for MetaClientOpts {
    fn default() -> MetaClientOpts {
        MetaClientOpts { rpc_timeout: Duration::from_secs(10), op_timeout: Duration::from_secs(120) }
    }
}

/// 认识的节点：当前认为的 leader、种子与集群图里的节点（轮流试）。
struct Peers {
    leader: Endpoint,
    known: Vec<Endpoint>,
    next: usize,
}

/// 在途的修改型请求：下一个要分配的 seq，与还没结束的请求的 seq。
struct Inflight {
    next: u64,
    seqs: BTreeSet<u64>,
}

/// 按 seq 计的在途窗口（见文件头）：分配 seq 时，新 seq 必须 < 最小的在途 seq + `DEDUP_WINDOW`，否则等。
struct Window {
    m: Mutex<Inflight>,
    cv: Condvar,
}

/// 占着一个在途的 seq；Drop 时（请求成功或最终失败）把它从在途集合里去掉，并唤醒等待者。
struct Slot<'a> {
    w: &'a Window,
    seq: u64,
}

impl Window {
    fn new() -> Window {
        Window { m: Mutex::new(Inflight { next: 1, seqs: BTreeSet::new() }), cv: Condvar::new() }
    }

    fn acquire(&self) -> Slot<'_> {
        let mut g = lock(&self.m);
        while g.seqs.first().is_some_and(|min| g.next >= min + DEDUP_WINDOW) {
            g = self.cv.wait(g).unwrap_or_else(|e| e.into_inner());
        }
        let seq = g.next;
        g.next += 1;
        g.seqs.insert(seq);
        Slot { w: self, seq }
    }
}

impl Drop for Slot<'_> {
    fn drop(&mut self) {
        lock(&self.w.m).seqs.remove(&self.seq);
        self.w.cv.notify_all(); // 最小的在途 seq 可能变大了：等待者都重新看一遍
    }
}

pub struct MetaClient {
    rpc: Arc<RpcClient>,
    peers: Mutex<Peers>,
    session: u64,
    window: Window,
    map: Mutex<Arc<ClusterMap>>,
    opts: MetaClientOpts,
    me: Caller, // 会话操作（打开、心跳、关闭）请求头里的身份：本进程的有效 uid/gid（服务端不检查也不用它，记为 nobody）
}

impl MetaClient {
    /// GetClusterMap + SessionOpen（超时参数取默认值）。
    pub fn connect(rpc: Arc<RpcClient>, seeds: &[Endpoint], info: SessionInfo) -> Result<MetaClient, ErrCode> {
        MetaClient::connect_with(rpc, seeds, info, MetaClientOpts::default())
    }

    /// 同 [`MetaClient::connect`]，超时参数由调用方给。
    pub fn connect_with(
        rpc: Arc<RpcClient>,
        seeds: &[Endpoint],
        info: SessionInfo,
        opts: MetaClientOpts,
    ) -> Result<MetaClient, ErrCode> {
        let c = MetaClient::bare(rpc, seeds, opts)?;
        c.refresh_map()?;
        let SessionInfo { client_name, host, host_id, mount } = info;
        let version = env!("CARGO_PKG_VERSION").to_string();
        match c.call(&c.me, 0, &MetaOp::SessionOpen { client_name, host, host_id, mount, version })? {
            MetaReply::Session { .. } => Ok(c),
            other => Err(unexpected("SessionOpen", &other)),
        }
    }

    /// 没有会话的客户端（只用来取集群图）。
    fn bare(rpc: Arc<RpcClient>, seeds: &[Endpoint], opts: MetaClientOpts) -> Result<MetaClient, ErrCode> {
        let first = seeds.first().ok_or(ErrCode::EINVAL)?.clone();
        let (uid, gid) = sys::euid_egid();
        let me = Caller { uid, gid, groups: vec![], pid: std::process::id(), job_id: 0 };
        let mut session = 0;
        while session == 0 {
            session = rand::random_u64();
        }
        let peers = Peers { leader: first, known: seeds.to_vec(), next: 0 };
        let map = Mutex::new(Arc::new(ClusterMap::default()));
        Ok(MetaClient { rpc, peers: Mutex::new(peers), session, window: Window::new(), map, opts, me })
    }

    /// 本客户端的会话 id。
    pub fn session(&self) -> u64 {
        self.session
    }

    /// 一个操作连同重试的总期限（`--op-timeout`）。实现注记（M6）：新增（只增），写回缓冲等槽位的背压用同一个期限。
    pub fn op_timeout(&self) -> Duration {
        self.opts.op_timeout
    }

    /// 只读请求（seq = 0）
    pub fn read(&self, who: &Caller, op: MetaOp) -> Result<MetaReply, ErrCode> {
        self.call(who, 0, &op)
    }

    /// 修改型：分配 seq（按 seq 计的在途窗口，见文件头），重试用同一 seq
    pub fn write(&self, who: &Caller, op: MetaOp) -> Result<MetaReply, ErrCode> {
        let slot = self.window.acquire();
        self.call(who, slot.seq, &op)
    }

    pub fn heartbeat(
        &self,
        opened: Vec<(u64, bool)>,
        closed: Vec<u64>,
        full: Option<Vec<(u64, bool)>>,
    ) -> Result<MetaReply, ErrCode> {
        self.call(&self.me, 0, &MetaOp::SessionHeartbeat { opened, closed, full })
    }

    /// 当前集群图
    pub fn map(&self) -> Arc<ClusterMap> {
        lock(&self.map).clone()
    }

    /// 心跳 map_epoch 变大、或 STALE_CHAIN/NOT_SERVING 时
    pub fn refresh_map(&self) -> Result<Arc<ClusterMap>, ErrCode> {
        let body = encode_body(&NodeOp::GetClusterMap { since_epoch: 0 });
        let (f, _) = self.exchange(MSG_NODE_CALL, body, "GetClusterMap", 0)?;
        match decode_body::<NodeReply>(&f.body)? {
            NodeReply::Map(m) => {
                let m: Arc<ClusterMap> = Arc::new(*m);
                *lock(&self.map) = m.clone();
                let mut p = lock(&self.peers);
                for (id, n) in &m.nodes {
                    if !p.known.iter().any(|e| e.addrs == n.addrs) {
                        p.known.push(Endpoint { node: *id, addrs: n.addrs.clone() });
                    }
                }
                Ok(m)
            }
            other => Err(unexpected("GetClusterMap", &other)),
        }
    }

    /// SessionClose（失败只写日志：会话最终会因租约到期被驱逐，M10）。
    pub fn close(&self) {
        if let Err(e) = self.call(&self.me, 0, &MetaOp::SessionClose) {
            log::warn("meta-client", format_args!("关闭会话 {:x} 失败：{e}", self.session));
        }
    }

    fn call(&self, who: &Caller, seq: u64, op: &MetaOp) -> Result<MetaReply, ErrCode> {
        let head = ReqHead {
            session: self.session,
            seq,
            uid: who.uid,
            gid: who.gid,
            groups: who.groups.clone(),
            pid: who.pid,
            job_id: who.job_id,
        };
        let body = encode_body(&MetaCall { head, op: op.clone() });
        let (f, retried_at) = self.exchange(MSG_META_CALL, body, op.name(), seq)?;
        let reply: MetaReply = decode_body(&f.body)?;
        if let Some(sent_ms) = retried_at {
            log_timeout_retry(seq, sent_ms, &reply);
        }
        Ok(reply)
    }

    /// 发一个控制族请求，跟随 leader、按 3.3.8 重试（见文件头）；返回成功的回复帧，以及"超时之后重试"时最后一次发送的墙钟时刻。
    /// 不用 `net::retry::retry`：这里的重试日志是 INFO 级（`wmfs fs --verbose` 才看得到，retry 写 WARN），带 hint 的
    /// `NOT_LEADER` 不退避、立即换到 leader，没有 hint 时还要换下一个认识的节点——retry 只会原地退避。
    fn exchange(&self, msg_type: u16, body: Vec<u8>, what: &str, seq: u64) -> Result<(InFrame, Option<u64>), ErrCode> {
        let deadline = clock::mono_ms().saturating_add(self.opts.op_timeout.as_millis() as u64);
        let trace_id = rand::random_u64(); // 同一个操作的重试用同一个 trace_id
        let (mut backoff, mut hops, mut attempt) = (BACKOFF_START_MS, 0, 0);
        let mut timed_out = false;
        loop {
            let ep = lock(&self.peers).leader.clone();
            let hdr = FrameHeader::new(FrameKind::Request, msg_type, 0, trace_id);
            let f = OutFrame { hdr, body: body.clone(), bulk: vec![], recv_into: None };
            let sent = clock::wall_ms();
            let (code, hint) = match self.rpc.call(&ep, Family::Control, f, self.opts.rpc_timeout) {
                Ok(f) => match f.error() {
                    None => return Ok((f, timed_out.then_some(sent))), // sent = 这一次（重试）发出的时刻
                    Some(e) => (ErrCode(e.code), e.leader_hint),
                },
                Err(e) => (e, None),
            };
            if code == ErrCode::NOT_LEADER
                && let Some(h) = hint.filter(|_| hops < MAX_HINT_HOPS)
            {
                log::info("meta-client", format_args!("{what}：NOT_LEADER，跟随 leader 提示换到 {h}"));
                let scheme = self.rpc.net().control_scheme.clone();
                lock(&self.peers).leader = Endpoint { node: 0, addrs: vec![(scheme, h)] };
                hops += 1;
                continue;
            }
            if !retryable(code) || clock::mono_ms().saturating_add(backoff) >= deadline {
                return Err(code);
            }
            attempt += 1;
            if code == ErrCode::ETIMEDOUT && seq != 0 {
                let secs = self.opts.rpc_timeout.as_secs_f64();
                log::info("meta-client", format_args!("{what} 超时（{secs:.0} s 没有回复），用同一个 seq={seq} 重试"));
                timed_out = true;
            } else {
                log::info("meta-client", format_args!("{what}：{code}，退避重试（第 {attempt} 次，{backoff} ms 后）"));
            }
            if code == ErrCode::NOT_LEADER || code == ErrCode::ECONNRESET {
                self.rotate(); // 没有 hint：换下一个认识的节点试试
            }
            std::thread::sleep(Duration::from_millis(backoff));
            backoff = (backoff * 2).min(BACKOFF_MAX_MS);
            hops = 0;
        }
    }

    fn rotate(&self) {
        let mut p = lock(&self.peers);
        if !p.known.is_empty() {
            p.next = (p.next + 1) % p.known.len();
            p.leader = p.known[p.next].clone();
        }
    }
}

/// 超时之后的重试成功了：回复里的 ctime 早于这次重试发出的时刻（`sent_ms`）= 第一次其实已经执行，这是去重表返回的
/// 原结果（见文件头）；否则只说成功。
fn log_timeout_retry(seq: u64, sent_ms: u64, reply: &MetaReply) {
    let ctime = match reply {
        MetaReply::Entry { attr } => Some(attr.ctime),
        MetaReply::Opened(o) => Some(o.attr.ctime),
        _ => None,
    };
    if ctime.is_some_and(|t| clock::ts_ms(t) < sent_ms) {
        let why = "返回的是第一次执行的结果：ctime 早于重试的时刻；没有 File exists 这类假错误";
        log::info("meta-client", format_args!("超时重试 seq={seq} → 去重命中（{why}）"));
    } else {
        let why = "同一个 seq：第一次若已执行，服务端由去重表返回原结果";
        log::info("meta-client", format_args!("超时重试 seq={seq} → 成功（{why}）"));
    }
}

/// 不开会话、只取一次集群图（`wmfs dev up` 打印集群警告用）：跟随 leader、按 3.3.8 重试到 `total`。
pub fn fetch_map(rpc: Arc<RpcClient>, seeds: &[Endpoint], total: Duration) -> Result<Arc<ClusterMap>, ErrCode> {
    MetaClient::bare(rpc, seeds, MetaClientOpts { op_timeout: total, ..MetaClientOpts::default() })?.refresh_map()
}

fn unexpected(what: &str, r: &impl std::fmt::Debug) -> ErrCode {
    log::error("meta-client", format_args!("{what} 的回复不对：{r:?}"));
    ErrCode::INTERNAL
}
