//! `RpcClient`：到多个对端的连接管理；每个进程一个实例（design.md 3.12、3.3.3 传输选择、3.3.8 超时）。
//!
//! 连接表里每个 `(scheme, 地址)` 最多 1 条连接（请求复用），第一次用到时建立：传输插件 `connect` → core 起读/写线程 →
//! 读线程做 3.3.2 握手 → 之后按 `req_id` 把回复交给等待者。在途表记着每个请求的期限：`rpc-timer` 线程每 100 ms 扫一遍，
//! 过期的以 `ETIMEDOUT` 结束；连接断开时它上面的在途调用全部以 `ECONNRESET` 结束（调用方重连后重试，见 `retry.rs`）。
//! 建连失败、握手中连接出错也一律报 `ECONNRESET`（3.3.8："传输层的任何错误都按连接断开处理"），原因写进日志；
//! 被对端拒绝的握手原样报 `AUTH_FAILED` / `PROTO_UNSUPPORTED`。服务端回的 ERROR 帧不算本地错误：`call` 照常返回这一帧，
//! 调用方用 [`check_reply`]（或 `InFrame::error`，需要 leader_hint、extra 时）取出错误码。
//!
//! 控制族（MetaCall、NodeCall、RaftMsg、SnapshotFetch）永远走 `control_scheme`；数据族（DataCall、PeerCall）走
//! `data_scheme`，前提是对端在 `NodeRecV1.addrs` 里公布了这个 scheme（客户端：没有公布 → 报错，不静默降级；
//! 服务端之间：退回 control_scheme 并计数 `wmfs_transport_fallback_total`）。core 里只有 scheme 字符串比较。
//!
//! 实现注记（M1）：① Hello 里要声明本端角色，而密钥文件里没有角色：[`RpcClient::new`] 按 init 的约定推断
//! （key 0 = server、key 1 = admin、其余 = client），别的 key 用 [`RpcClient::new_with_role`] 明确给出；
//! ② `call_cb` 在还没有连接时不在调用线程上建连（实现注记（M9）：repl/io 线程从不等网络，1.2 规则 3）：调用先登记在途表、到期照样
//! 由 rpc-timer 结束，建连与发送交给一个短命的 `rpc-dial` 线程——实现注记（M3，ADR-0031 第 11 条）：
//! 建连按 `(scheme, 地址)` 单飞，多个线程同时对同一对端发第一次调用时只有一个线程建连，其余等它的结果（成功就共享这条连接，
//! 失败就都拿到同一个错误，再各自按 3.3.8 重试）；③ 一次调用的任何本地失败
//! （选路失败、建连或握手失败、入队失败、请求帧超过传输上限）都先记进在途表，由 rpc-timer 在 100 ms 内结束——
//! 于是回调只在连接读线程或 rpc-timer 线程上执行（3.12），从不在调用方线程上；④ 期限 = 现在 + timeout + 1 ms
//! （`mono_ms` 只精确到毫秒，不加 1 的话调用可能比 timeout 早不到 1 ms 结束）。

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, Weak};
use std::time::Duration;

use crossbeam_channel::{Receiver, Sender, bounded};
use wmfs_api::metrics::{self, Counter, Gauge};
use wmfs_api::types::KeyFile;

use crate::base::config::NetConfig;
use crate::base::types::Role;
use crate::base::{ErrCode, clock, log};
use crate::buf::MemPool;
use crate::hooks::manifest::Hook;
use crate::hooks::pipeline::HookCall;
use crate::hooks::traits::Transport;
use crate::net::conn::{self, ConnHandle};
use crate::proto::frame::{FLAG_LAST_CHUNK, FrameKind, InFrame, OutFrame};
use crate::proto::handshake::{ADMIN_KEY, SERVER_KEY};

mod link;

/// 消息族：按族选 scheme（3.3.3）。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Family {
    Control,
    Data,
}

/// 来自集群图 NodeRecV1.addrs；CLI 也可以手写一个
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Endpoint {
    pub node: u32,
    pub addrs: Vec<(String, String)>,
}

type Callback = Box<dyn FnOnce(Result<InFrame, ErrCode>) + Send>;

/// 流收到的帧最多攒几帧（每帧 ≤ 1 MiB）：接收方处理得慢时连接的读线程在这里等，TCP 的背压一直传到发送方（实现注记（M8 审查）：
/// 快照下载不会把整个 kv 表攒在内存里）。代价是同一条连接上别的回复也跟着慢下来（最多这么多帧的处理时间）。
const STREAM_WINDOW: usize = 16;

/// 在途请求的等待者：一次性回调（call/start/call_cb）或流（stream）。
enum Waiter {
    Once(Callback),
    Stream(Sender<Result<InFrame, ErrCode>>),
}

impl Waiter {
    /// 以 `r` 结束（超时、连接断开……）。流的通道满了就不等（这里可能在 rpc-timer 线程上）：发送端随之丢掉，接收方读完已有的帧之后
    /// 看到通道断开，按连接断开（`ECONNRESET`）结束。
    fn finish(self, r: Result<InFrame, ErrCode>) {
        match self {
            Waiter::Once(cb) => cb(r),
            Waiter::Stream(tx) => {
                let _ = tx.try_send(r);
            }
        }
    }
}

struct Pending {
    conn: u64,       // 走的哪条连接（连接断开时只结束它上面的调用）；0 = 还没发出去
    deadline: u64,   // mono_ms：到期由 rpc-timer 以 `fail` 结束
    timeout_ms: u64, // 流：相邻两帧之间的上限
    fail: ErrCode,   // 到期时的结果：一般是 ETIMEDOUT；"马上失败"的调用是它自己的错误码（deadline = 0）
    w: Waiter,
}

pub struct RpcClient {
    /// 指向自己的弱引用：读线程、写线程的回调与 rpc-timer 靠它找回 RpcClient；RpcClient 被丢弃后它们自然退出。
    me: Weak<RpcClient>,
    pool: MemPool,
    transports: Vec<(Arc<dyn Transport>, Arc<HookCall>)>,
    key: KeyFile,
    role: Role,
    net: NetConfig,
    conns: Mutex<HashMap<(String, String), ConnHandle>>,
    dialing: Mutex<HashMap<(String, String), Arc<link::Dial>>>, // 正在建连的 (scheme, 地址)：同一对端只让一个线程建连
    inflight: Mutex<HashMap<u64, Pending>>,
    next_req: AtomicU64,
    inflight_gauge: Arc<Gauge>,
    fallback: OnceLock<Arc<Counter>>, // wmfs_transport_fallback_total{from,to}：第一次退回时注册，之后不加锁
}

/// init 的约定：key 0 = server（服务端之间）、key 1 = admin、其余 = client。
pub fn default_role(key_id: u32) -> Role {
    match key_id {
        SERVER_KEY => Role::Server,
        ADMIN_KEY => Role::Admin,
        _ => Role::Client,
    }
}

/// 服务端回的是 ERROR 帧时把它变成错误码；否则原样返回这一帧。
pub fn check_reply(f: InFrame) -> Result<InFrame, ErrCode> {
    match f.error() {
        Some(e) => Err(ErrCode(e.code)),
        None => Ok(f),
    }
}

impl RpcClient {
    /// 角色按 [`default_role`] 推断。
    pub fn new(
        pool: MemPool,
        transports: Vec<Arc<dyn Transport>>,
        key: KeyFile,
        net: NetConfig,
    ) -> Result<Arc<RpcClient>, ErrCode> {
        let role = default_role(key.key_id);
        RpcClient::new_with_role(pool, transports, key, role, net)
    }

    /// 明确给出 Hello 里声明的角色（必须等于这把 key 在 KEYMETA 里的角色，否则握手被拒）。
    pub fn new_with_role(
        pool: MemPool,
        transports: Vec<Arc<dyn Transport>>,
        key: KeyFile,
        role: Role,
        net: NetConfig,
    ) -> Result<Arc<RpcClient>, ErrCode> {
        for t in &transports {
            t.register_region(&pool.region())?;
        }
        let transports = transports
            .into_iter()
            .map(|t| (t.clone(), Arc::new(HookCall::new(t.manifest(), Hook::Transport))))
            .collect();
        conn::counters(); // 连接层的计数器现在就注册（/metrics 里也有，值为 0）
        let rpc = Arc::new_cyclic(|me| RpcClient {
            me: me.clone(),
            pool,
            transports,
            key,
            role,
            net,
            conns: Mutex::new(HashMap::new()),
            dialing: Mutex::new(HashMap::new()),
            inflight: Mutex::new(HashMap::new()),
            next_req: AtomicU64::new(1),
            inflight_gauge: metrics::gauge("wmfs_rpc_inflight", "客户端在途请求数", &[]),
            fallback: OnceLock::new(),
        });
        let me = rpc.me.clone();
        std::thread::Builder::new()
            .name("rpc-timer".into())
            .spawn(move || link::timer(me))
            .map_err(|_| ErrCode::ENOMEM)?;
        Ok(rpc)
    }

    /// 构造时给的传输选择（MetaClient 按 control_scheme 解释 NOT_LEADER 的 leader_hint）。
    pub fn net(&self) -> &NetConfig {
        &self.net
    }

    /// 这个客户端的内存池（实现注记（M5）：DataClient 重试读的时候从这里取回复用的缓冲）。
    pub fn pool(&self) -> &MemPool {
        &self.pool
    }

    /// 阻塞：发出请求、等回复。只允许在可以阻塞的线程里用（1.2 规则 3）。不重试（重试在 MetaClient/DataClient/retry.rs）
    pub fn call(&self, to: &Endpoint, fam: Family, f: OutFrame, timeout: Duration) -> Result<InFrame, ErrCode> {
        self.start(to, fam, f, timeout)?.wait()
    }

    /// 发出请求、立即返回；之后用 PendingCall::wait 等（kv bench 64 路并发等）
    pub fn start(&self, to: &Endpoint, fam: Family, f: OutFrame, timeout: Duration) -> Result<PendingCall, ErrCode> {
        let (tx, rx) = bounded(1);
        let cb = move |r| {
            let _ = tx.try_send(r);
        };
        self.submit(to, fam, f, timeout, Waiter::Once(Box::new(cb)), false);
        Ok(PendingCall { rx })
    }

    /// 回调式：回复、错误或超时时调用 cb，恰好一次。cb 在连接读线程或 rpc-timer 线程上执行，只允许 try_send 到某个队列，不得阻塞
    pub fn call_cb(
        &self,
        to: &Endpoint,
        fam: Family,
        f: OutFrame,
        timeout: Duration,
        cb: Box<dyn FnOnce(Result<InFrame, ErrCode>) + Send>,
    ) {
        self.submit(to, fam, f, timeout, Waiter::Once(cb), true); // 没有连接时在后台建连（见文件头 ②）
    }

    /// 流式（SnapshotFetch，永远走 control_scheme）：timeout 是相邻两帧之间的上限；收到的帧最多攒 [`STREAM_WINDOW`] 帧（背压）
    pub fn stream(&self, to: &Endpoint, f: OutFrame, timeout: Duration) -> Result<StreamRx, ErrCode> {
        let (tx, rx) = bounded(STREAM_WINDOW);
        self.submit(to, Family::Control, f, timeout, Waiter::Stream(tx), false);
        Ok(StreamRx { rx, done: false })
    }

    /// OneWay 帧（RaftMsg，永远走 control_scheme）：取（必要时新建，最多等 `timeout`）到对端的连接，放进它的发送队列就返回。
    /// 没有回复，所以不进在途表；失败（建连失败、队列满……）原样返回，调用方丢掉这一帧（raft 会重发）。
    pub fn send_oneway(&self, to: &Endpoint, f: OutFrame, timeout: Duration) -> Result<(), ErrCode> {
        let deadline = clock::mono_ms() + timeout.as_millis() as u64 + 1; // +1：见文件头的实现注记 ④
        let (s, a) = self.route(to, Family::Control)?;
        self.connection(&s, &a, deadline)?.send(f)
    }

    /// 登记在途、选连接、交给写线程。本地的任何失败也记进在途表、由 rpc-timer（100 ms 内）以该错误码结束：
    /// 于是回调恰好一次，而且只在连接读线程或 rpc-timer 线程上执行（3.12），不在调用方线程上。`background` = 还没有连接时
    /// 在 `rpc-dial` 线程上建连（`call_cb`，见文件头 ②）。
    fn submit(&self, to: &Endpoint, fam: Family, mut f: OutFrame, timeout: Duration, w: Waiter, background: bool) {
        let timeout_ms = timeout.as_millis() as u64;
        let deadline = clock::mono_ms() + timeout_ms + 1; // +1：见文件头的实现注记 ④
        let id = self.next_req.fetch_add(1, Ordering::Relaxed);
        let route = self.route(to, fam);
        let live = route.as_ref().ok().and_then(|(s, a)| self.live_conn(&(s.clone(), a.clone())));
        let conn = live.as_ref().map_or(0, |h| h.id());
        self.track().insert(id, Pending { conn, deadline, timeout_ms, fail: ErrCode::ETIMEDOUT, w });
        self.update_gauge();
        f.hdr.req_id = id;
        match (route, live) {
            (Err(e), _) => self.fail_soon(id, e),
            (Ok(_), Some(h)) => {
                if let Err(e) = h.send(f) {
                    self.fail_soon(id, e);
                }
            }
            (Ok((s, a)), None) if background => {
                let me = self.me.clone();
                let dial = move || {
                    if let Some(rpc) = me.upgrade() {
                        rpc.dial_and_send(id, &s, &a, f, deadline);
                    }
                };
                if std::thread::Builder::new().name("rpc-dial".into()).spawn(dial).is_err() {
                    self.fail_soon(id, ErrCode::ECONNRESET);
                }
            }
            (Ok((s, a)), None) => self.dial_and_send(id, &s, &a, f, deadline),
        }
    }

    /// 建连（同一对端单飞）再发出在途调用 `id`；它这期间已经到期结束了就不发。
    fn dial_and_send(&self, id: u64, scheme: &str, addr: &str, f: OutFrame, deadline: u64) {
        match self.connection(scheme, addr, deadline) {
            Ok(h) => {
                match self.track().get_mut(&id) {
                    Some(p) => p.conn = h.id(),
                    None => return, // 已经超时结束了
                }
                if let Err(e) = h.send(f) {
                    self.fail_soon(id, e);
                }
            }
            Err(e) => self.fail_soon(id, e),
        }
    }

    /// 让在途调用 `id` 由 rpc-timer 在 100 ms 内以 `code` 结束（已经结束了就什么也不做）。
    fn fail_soon(&self, id: u64, code: ErrCode) {
        if let Some(p) = self.track().get_mut(&id) {
            (p.deadline, p.fail) = (0, code);
        }
    }

    /// 按族选 (scheme, 地址)（3.3.3）。
    fn route(&self, to: &Endpoint, fam: Family) -> Result<(String, String), ErrCode> {
        let find = |s: &str| to.addrs.iter().find(|(x, _)| x == s).cloned();
        let want = if fam == Family::Control { &self.net.control_scheme } else { &self.net.data_scheme };
        if let Some(a) = find(want) {
            return Ok(a);
        }
        if fam == Family::Data
            && self.role == Role::Server
            && let Some(a) = find(&self.net.control_scheme)
        {
            let counter = || {
                let (to_s, n) = (self.net.control_scheme.as_str(), to.node);
                log::warn(
                    "rpc",
                    format_args!(
                        "节点 {n} 没有公布 {want} 地址：服务端之间的数据复制退回 {to_s}（只在第一次时写这一行）"
                    ),
                );
                let l = [("from", want.as_str()), ("to", to_s)];
                metrics::counter("wmfs_transport_fallback_total", "服务端之间数据族退回 control_scheme 的次数", &l)
            };
            self.fallback.get_or_init(counter).inc();
            return Ok(a);
        }
        log::warn("rpc", format_args!("节点 {} 没有公布 {want} 地址（不静默降级）：{:?}", to.node, to.addrs));
        Err(ErrCode::FEATURE_UNSUPPORTED)
    }

    fn conns(&self) -> MutexGuard<'_, HashMap<(String, String), ConnHandle>> {
        self.conns.lock().unwrap_or_else(|e| e.into_inner())
    }
    fn dialing(&self) -> MutexGuard<'_, HashMap<(String, String), Arc<link::Dial>>> {
        self.dialing.lock().unwrap_or_else(|e| e.into_inner())
    }
    fn track(&self) -> MutexGuard<'_, HashMap<u64, Pending>> {
        self.inflight.lock().unwrap_or_else(|e| e.into_inner())
    }
    fn untrack(&self, id: u64) -> Option<Pending> {
        let p = self.track().remove(&id);
        self.update_gauge();
        p
    }
    /// 从在途表里取出所有满足 `pred` 的调用（连接断开、到期时用）。
    fn take_pending(&self, pred: impl Fn(&Pending) -> bool) -> Vec<Pending> {
        let taken: Vec<Pending> = self.track().extract_if(|_, p| pred(p)).map(|(_, p)| p).collect();
        if !taken.is_empty() {
            self.update_gauge();
        }
        taken
    }
    fn update_gauge(&self) {
        let n = self.track().len();
        self.inflight_gauge.set(n as f64);
    }
}

impl Drop for RpcClient {
    fn drop(&mut self) {
        for h in self.conns().values() {
            h.close();
        }
        // 还没结束的调用（例如刚记下、rpc-timer 还没来得及处理的失败）：以 ECONNRESET 结束，回调仍然恰好一次
        let left = std::mem::take(self.inflight.get_mut().unwrap_or_else(|e| e.into_inner()));
        for p in left.into_values() {
            p.w.finish(Err(ErrCode::ECONNRESET));
        }
    }
}

pub struct PendingCall {
    rx: Receiver<Result<InFrame, ErrCode>>,
}

impl PendingCall {
    pub fn wait(self) -> Result<InFrame, ErrCode> {
        self.rx.recv().unwrap_or(Err(ErrCode::ECONNRESET))
    }
}

pub struct StreamRx {
    rx: Receiver<Result<InFrame, ErrCode>>,
    done: bool,
}

impl StreamRx {
    /// None = 已收到 LAST_CHUNK。ERROR 帧 → 它的错误码；超时（相邻两帧之间）→ ETIMEDOUT；连接断开 → ECONNRESET。
    #[allow(clippy::should_implement_trait)] // 名字按 design.md 3.12；它返回 Result，不是 Iterator
    pub fn next(&mut self) -> Result<Option<InFrame>, ErrCode> {
        if self.done {
            return Ok(None);
        }
        let f = self.rx.recv().unwrap_or(Err(ErrCode::ECONNRESET)).and_then(check_reply);
        let more = matches!(&f, Ok(f) if f.hdr.kind == FrameKind::StreamChunk && f.hdr.flags & FLAG_LAST_CHUNK == 0);
        self.done = !more;
        f.map(Some)
    }
}
