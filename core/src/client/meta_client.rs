//! `MetaClient`：FUSE 层与 CLI 用；跟随 leader、seq、按 3.3.8 重试（design.md 3.12、3.5.2、3.3.7）。
//!
//! - **会话**：`connect` = GetClusterMap + SessionOpen（session id 由客户端随机生成）；`close` = SessionClose。心跳、0.8 × lease
//!   自停（除会话操作外的请求先过 [`MetaClient::gate`]）与被驱逐之后换新会话在 `lease.rs`（M10）：任何请求回 `SESSION_EVICTED`
//!   都触发换新会话，这个请求本身照样失败（修改不知道执行了没有，不自动重做）。经已打开的句柄的请求、写回的一批数据只在打开（写进）
//!   它时的会话里发（[`MetaClient::read_in`]、[`MetaClient::write_in`]，M10 审查）：被驱逐之后它们一律 `SESSION_EVICTED`，不会
//!   换到新会话里去做。
//! - **seq**：每个修改型请求分配单调递增的 seq，**重试必须用同一个 seq**（服务端的去重表保证恰好执行一次，3.5.2）。
//!   只读请求 seq = 0。在途窗口**按 seq 计**（[`Window`]）：新 seq 必须小于"最小的在途 seq + 256"（`DEDUP_WINDOW`），
//!   到了就等。服务端写入 seq 的结果时点删 seq − 256 的去重记录（3.4.1.4 规则 4），所以只数个数不够：一个回复丢了、
//!   正在等 10 s 超时的请求，会被同一会话里别的线程的 256 个新请求挤掉它的去重记录，重试时就会重新执行、回出假的
//!   `EEXIST`。按 seq 计时，所有在途请求的 seq 都 > 新 seq − 256，它们的记录一定还在。
//! - **跟随 leader**：回 `NOT_LEADER` 且带 `leader_hint`（leader 在 control_scheme 上的 rpc 地址）→ 换到它立即重试
//!   （连续跟随最多 [`MAX_HINT_HOPS`] 次，之后照常退避）；没有 hint → 退避后换下一个已知节点（`connect` 给的种子与
//!   集群图里的节点）。实现注记（M4）：core 里没有 HTTP 客户端，"按 health 找 leader"由调用方（CLI）在 `connect` 之前做，
//!   给出种子；之后没有 hint 时在认识的节点之间轮换（不是 leader 的节点回 `NOT_LEADER`，换下一个）。实现注记（M9 审查）：`ETIMEDOUT`
//!   也换下一个——被冻住（SIGSTOP）的 leader 既不回复也不断开，以前要等它恢复或连接保活断开才换，客户端停顿远超"leader 切换 1–3 s"。
//! - **重试**（3.3.8）：可重试的错误（1000–1007、1019、`ECONNRESET`、`ETIMEDOUT`）按指数退避重试，10 ms 起、×2、上限 1 s，
//!   直到总期限（[`MetaClientOpts::op_timeout`]：CLI 30 s，挂载参数 `--op-timeout` 默认 120 s）。每次重试写一行 INFO 日志
//!   （`wmfs fs --verbose` 能看到）。超时之后的重试成功时，回复里的 ctime 早于重试的时刻 = 第一次其实已经执行、这是去重表
//!   返回的原结果，日志写"超时重试 seq=… → 去重命中"（判断用两边的墙钟，跨机器时要求时钟误差远小于一个 RPC 超时，
//!   只影响这一行提示；协议里没有"去重命中"的标志，3.3.4）。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use super::lease::{Lease, OpenDelta, OpenFiles};
use crate::base::{ErrCode, clock, lock, log, rand, sys};
use crate::net::client::{Endpoint, Family, RpcClient};
use crate::net::retry::{BACKOFF_MAX_MS, BACKOFF_START_MS, retryable};
use crate::obs::trace::ClientTimer;
use crate::proto::frame::{FrameHeader, FrameKind, InFrame, MSG_META_CALL, MSG_NODE_CALL, OutFrame, msg_name};
use crate::proto::meta::{MetaCall, MetaOp, MetaReply, ReqHead};
use crate::proto::peer::{ClusterMap, NodeOp, NodeReply};
use crate::proto::{decode_body, encode_body};

mod window;
use window::Window;

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

#[derive(Clone, Debug, PartialEq, Eq, Default)]
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

pub struct MetaClient {
    rpc: Arc<RpcClient>,
    peers: Mutex<Peers>,
    session: AtomicU64, // 被驱逐之后换成新会话（M10，`lease.rs`）
    window: Window,
    map: Mutex<Arc<ClusterMap>>,
    opts: MetaClientOpts,
    me: Caller, // 会话操作（打开、心跳、关闭）请求头里的身份：本进程的有效 uid/gid（服务端不检查也不用它，记为 nobody）
    info: SessionInfo, // SessionOpen 带的客户端信息（换新会话时再用）
    pub(super) lease: Lease,
    pub(super) delta: Mutex<OpenDelta>,
    pub(super) files: OnceLock<Weak<dyn OpenFiles>>,
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
        let c = MetaClient::bare(rpc, seeds, opts, info)?;
        c.refresh_map()?;
        let sent = clock::mono_ms();
        let lease_ms = c.open_session(c.session())?;
        c.lease.renewed(sent, lease_ms);
        Ok(c)
    }

    /// 用 id `sid` 发 SessionOpen，返回会话租约（ms）。
    pub(super) fn open_session(&self, sid: u64) -> Result<u64, ErrCode> {
        let SessionInfo { client_name, host, host_id, mount } = self.info.clone();
        let version = env!("CARGO_PKG_VERSION").to_string();
        match self.call_in(sid, &self.me, 0, &MetaOp::SessionOpen { client_name, host, host_id, mount, version })? {
            MetaReply::Session { lease_ms, .. } => Ok(lease_ms),
            other => Err(unexpected("SessionOpen", &other)),
        }
    }

    /// 没有会话的客户端（只用来取集群图）。
    fn bare(
        rpc: Arc<RpcClient>,
        seeds: &[Endpoint],
        opts: MetaClientOpts,
        info: SessionInfo,
    ) -> Result<MetaClient, ErrCode> {
        let first = seeds.first().ok_or(ErrCode::EINVAL)?.clone();
        let (uid, gid) = sys::euid_egid();
        Ok(MetaClient {
            rpc,
            peers: Mutex::new(Peers { leader: first, known: seeds.to_vec(), next: 0 }),
            session: AtomicU64::new(new_session_id()),
            window: Window::new(),
            map: Mutex::new(Arc::new(ClusterMap::default())),
            opts,
            me: Caller { uid, gid, groups: vec![], pid: std::process::id(), job_id: 0 },
            info,
            lease: Lease::default(),
            delta: Mutex::default(),
            files: OnceLock::new(),
        })
    }

    /// 本客户端现在的会话 id（被驱逐之后是新会话的）。
    pub fn session(&self) -> u64 {
        self.session.load(Ordering::SeqCst)
    }

    pub(super) fn set_session(&self, sid: u64) {
        self.session.store(sid, Ordering::SeqCst);
    }

    /// 一个操作连同重试的总期限（`--op-timeout`）。实现注记（M6）：新增（只增），写回缓冲等槽位的背压用同一个期限。
    pub fn op_timeout(&self) -> Duration {
        self.opts.op_timeout
    }

    /// 只读请求（seq = 0）
    pub fn read(&self, who: &Caller, op: MetaOp) -> Result<MetaReply, ErrCode> {
        self.call(who, 0, &op, None)
    }

    /// 修改型：分配 seq（按 seq 计的在途窗口，见文件头），重试用同一 seq
    pub fn write(&self, who: &Caller, op: MetaOp) -> Result<MetaReply, ErrCode> {
        let slot = self.window.acquire();
        self.call(who, slot.seq, &op, None)
    }

    /// 同 [`read`](Self::read)，但只在会话 `sid` 里发（M10 审查，3.4.4）：经已打开的句柄的请求、写回的一批数据属于打开（写进）它时
    /// 的会话。那个会话已被驱逐、换了新会话 → `SESSION_EVICTED`，不发；请求头里带的就是 `sid`，所以核对之后才换的会话也挡得住
    /// （服务端回 `SESSION_EVICTED`）——"写回缓冲里未提交的数据丢失"，不会在新会话里被提交。
    pub fn read_in(&self, sid: u64, who: &Caller, op: MetaOp) -> Result<MetaReply, ErrCode> {
        self.call(who, 0, &op, Some(sid))
    }

    /// 同 [`write`](Self::write)，只在会话 `sid` 里发（见 [`read_in`](Self::read_in)）。
    pub fn write_in(&self, sid: u64, who: &Caller, op: MetaOp) -> Result<MetaReply, ErrCode> {
        let slot = self.window.acquire();
        self.call(who, slot.seq, &op, Some(sid))
    }

    pub fn heartbeat(
        &self,
        opened: Vec<(u64, bool)>,
        closed: Vec<u64>,
        full: Option<Vec<(u64, bool)>>,
    ) -> Result<MetaReply, ErrCode> {
        self.call(&self.me, 0, &MetaOp::SessionHeartbeat { opened, closed, full }, None)
    }

    /// 当前集群图
    pub fn map(&self) -> Arc<ClusterMap> {
        lock(&self.map).clone()
    }

    /// 心跳 map_epoch 变大、或 STALE_CHAIN/NOT_SERVING 时；返回换上之后手上的图（只装不旧于它的：ADR-0041 第 17 条）
    pub fn refresh_map(&self) -> Result<Arc<ClusterMap>, ErrCode> {
        let body = encode_body(&NodeOp::GetClusterMap { since_epoch: 0 });
        let (f, _) = self.exchange(MSG_NODE_CALL, body, "GetClusterMap", 0, 0)?;
        match decode_body::<NodeReply>(&f.body)? {
            NodeReply::Map(m) => {
                let mut cur = lock(&self.map);
                *cur = std::cmp::max_by_key(cur.clone(), Arc::new(*m), |x| x.map_epoch); // 一样新时取刚拉到的
                let m = cur.clone();
                drop(cur);
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
        self.lease.closed.store(true, Ordering::SeqCst); // 之后不再心跳、不再换新会话（M10）
        if let Err(e) = self.call(&self.me, 0, &MetaOp::SessionClose, None) {
            log::warn("meta-client", format_args!("关闭会话 {:x} 失败：{e}", self.session()));
        }
    }

    /// 发一个请求：会话操作以外的先过自停的闸门（M10）；`pin` = 只在这个会话里发（[`read_in`](Self::read_in)），它已经换掉了
    /// → `SESSION_EVICTED`；回 `SESSION_EVICTED` → 换新会话（`lease.rs`），这个请求照样失败。
    fn call(&self, who: &Caller, seq: u64, op: &MetaOp, pin: Option<u64>) -> Result<MetaReply, ErrCode> {
        let session_op =
            matches!(op, MetaOp::SessionOpen { .. } | MetaOp::SessionHeartbeat { .. } | MetaOp::SessionClose);
        if !session_op {
            self.gate()?;
        }
        let sid = match pin {
            Some(p) if p != self.session() => return Err(ErrCode::SESSION_EVICTED), // 换会话只因为旧的被驱逐了（`lease.rs`）
            Some(p) => p,
            None => self.session(),
        };
        let r = self.call_in(sid, who, seq, op);
        if r.as_ref().err() == Some(&ErrCode::SESSION_EVICTED) && !matches!(op, MetaOp::SessionOpen { .. }) {
            self.reopen(sid);
        }
        r
    }

    fn call_in(&self, sid: u64, who: &Caller, seq: u64, op: &MetaOp) -> Result<MetaReply, ErrCode> {
        let head = ReqHead {
            session: sid,
            seq,
            uid: who.uid,
            gid: who.gid,
            groups: who.groups.clone(),
            pid: who.pid,
            job_id: who.job_id,
        };
        let body = encode_body(&MetaCall { head, op: op.clone() });
        let (f, retried_at) = self.exchange(MSG_META_CALL, body, op.name(), seq, op.feature())?;
        let reply: MetaReply = decode_body(&f.body)?;
        if let Some(sent_ms) = retried_at {
            log_timeout_retry(seq, sent_ms, &reply);
        }
        Ok(reply)
    }

    /// 发一个控制族请求，跟随 leader、按 3.3.8 重试（见文件头）；返回成功的回复帧，以及"超时之后重试"时最后一次发送的墙钟时刻。
    /// 不用 `net::retry::retry`：这里的重试日志是 INFO 级（`wmfs fs --verbose` 才看得到，retry 写 WARN），带 hint 的
    /// `NOT_LEADER` 不退避、立即换到 leader，没有 hint 时还要换下一个认识的节点——retry 只会原地退避。`need`：对端要在握手里声明的
    /// feature 位（`MetaOp::feature`），没声明 → `EOPNOTSUPP`、不发（ADR-0043 第 3 条）。
    fn exchange(
        &self,
        msg_type: u16,
        body: Vec<u8>,
        what: &str,
        seq: u64,
        need: u64,
    ) -> Result<(InFrame, Option<u64>), ErrCode> {
        let deadline = clock::mono_ms().saturating_add(self.opts.op_timeout.as_millis() as u64);
        // 同一个操作的重试用同一个 trace_id；会话心跳不属于哪个文件操作，用 0（服务端不记 span，`wmfs trace --recent` 不被心跳占满，3.11）
        let trace_id = if what == "SessionHeartbeat" { 0 } else { rand::random_u64() };
        let (component, op) = (msg_name(msg_type).unwrap_or("MetaCall"), || what.to_string());
        let _slow = ClientTimer { component, op: &op, trace_id, t0: std::time::Instant::now() }; // 慢了写客户端日志（3.11）
        let (mut backoff, mut hops, mut attempt) = (BACKOFF_START_MS, 0, 0);
        let mut timed_out = false;
        loop {
            let ep = lock(&self.peers).leader.clone();
            let hdr = FrameHeader::new(FrameKind::Request, msg_type, 0, trace_id);
            let f = OutFrame { hdr, body: body.clone(), bulk: vec![], recv_into: None };
            let sent = clock::wall_ms();
            let have = if need == 0 { Ok(0) } else { self.rpc.features(&ep, Family::Control, self.opts.rpc_timeout) };
            let r = match have {
                Ok(h) if h & need != need => return Err(ErrCode::EOPNOTSUPP), // 对端没在握手里声明：不发（见上）
                Ok(_) => self.rpc.call(&ep, Family::Control, f, self.opts.rpc_timeout),
                Err(e) => Err(e), // 建连失败等：照常按下面的规则重试
            };
            let (code, hint) = match r {
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
            if matches!(code, ErrCode::NOT_LEADER | ErrCode::ECONNRESET | ErrCode::ETIMEDOUT) {
                self.rotate(); // 没有 hint、连不上或卡住（例如被冻住的 leader）：换下一个认识的节点试试
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

/// 新的会话号：随机、不是 0（连接时与被驱逐之后换新会话时）。
pub(super) fn new_session_id() -> u64 {
    rand::random_u64().max(1)
}

/// 不开会话、只取一次集群图（`wmfs dev up` 打印集群警告用）：跟随 leader、按 3.3.8 重试到 `total`。
pub fn fetch_map(rpc: Arc<RpcClient>, seeds: &[Endpoint], total: Duration) -> Result<Arc<ClusterMap>, ErrCode> {
    let opts = MetaClientOpts { op_timeout: total, ..MetaClientOpts::default() };
    MetaClient::bare(rpc, seeds, opts, SessionInfo::default())?.refresh_map()
}

fn unexpected(what: &str, r: &impl std::fmt::Debug) -> ErrCode {
    log::error("meta-client", format_args!("{what} 的回复不对：{r:?}"));
    ErrCode::INTERNAL
}
