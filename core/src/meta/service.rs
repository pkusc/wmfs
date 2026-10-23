//! `MetaCall`（0x0100）与 `NodeCall`（0x0400）消息族的服务端（design.md 3.3.3、3.3.4、3.3.6、1.2）。
//!
//! [`Service::call`] 在连接读线程上执行，**自己从不执行请求**：修改型请求连同回复回调交给执行器（`MetaEngine::txn`，
//! 投递后立即返回），只读请求交给 8 个 `meta-read` 线程（它们在读闸门上等 ReadIndex，再开 redb 读事务）。
//! 回复经 `Responder`（只 try_send 进连接的发送队列，不碰 socket）。指标 `wmfs_meta_ops_total{op,code}`。
//!
//! 实现注记（M4）：
//! - 命名空间操作见 `meta/ns`；会话：SessionOpen / SessionClose 一律按 seq 0 执行（不进去重表，它们本身幂等，`meta/sessions.rs`），
//!   SessionHeartbeat 走只读路径并更新 leader 内存里的会话状态。其余修改型请求必须带会话与 seq（seq 0 → `EINVAL`：
//!   没有 seq 就没有去重，也绕过了会话检查）。还没实现的操作回 `FEATURE_UNSUPPORTED`（不断开）。
//! - 请求头里的 uid：client 角色的 key 只在它的 uid 范围内被相信（不在范围内 → `EACCES`），`root_squash` 把 uid/gid 0
//!   映射为 65534；admin 与 server 可以声明任意 uid（`wmfs fs --as-uid`，3.3.3）——[`req_ctx`]。会话操作（打开、心跳、
//!   关闭）不代表任何用户、也不碰 inode：不按 uid 范围检查，身份一律记为 nobody（changelog 里的 uid/gid 是 65534）——
//!   否则一把 uid 范围不含挂载进程自己的 uid 的 key 连会话都开不了。会话绑定打开它的 key（`sessions::usable`）。
//! - NodeCall 只有 `GetClusterMap`（线性一致读之后取集群图快照；client 与 admin 拿到的不含 keys、datakeys）；心跳（M5）、
//!   Join（M8）等回 `FEATURE_UNSUPPORTED`。
//! - fail point `meta.reply.drop`（3.11）：带 seq 的修改型请求执行完、批次提交之后，吞掉这一次回复（Responder 什么也不回）——
//!   客户端等一个 RPC 超时（10 s）后用同一个 seq 重试，去重表返回原结果（演示"重试不会冒出假错误"）。只读请求与会话的
//!   打开/关闭不受它影响（它们的重试本来就幂等，演示的是去重）。去重命中计入 `wmfs_meta_dedup_hits_total`。
//!
//! 实现注记（M5）：NodeCall 加了 target 心跳（`meta/cluster/targets.rs`）；MetaCall 的 `StatFs`（要心跳报上来的容量，同一个文件）与
//! `Uninline`（ADR-0034：按只读请求的方式处理、seq 0，在 meta-read 线程上经数据面写 chunk，`meta/uninline.rs`）也在 meta-read 线程
//! 上做；`Uninline` 要的数据面客户端由服务端装配时 [`MetaService::set_data`] 交进来（没有 → `NOT_READY`）。同一个 inode 同时最多
//! 一个 Uninline、全部加起来最多 `READ_THREADS / 2` = 4 个（`uninline::InFlight`，超出 → `BUSY`）：它们在 meta-read 线程上同步等
//! 数据面，至少要留一半线程给心跳、集群图与普通读——数据节点正是经它们拉到新集群图，Uninline 才写得成。

use std::sync::{Arc, Mutex, OnceLock};

use crossbeam_channel::{Sender, TrySendError, bounded};
use wmfs_api::failpoint::fail_point_hit;
use wmfs_api::metrics::{self, Counter};

use crate::base::types::{Principal, ReqCtx, Role};
use crate::base::{ErrCode, clock, lock, log};
use crate::client::data_client::DataClient;
use crate::hooks::traits::ConnIdentity;
use crate::meta::cluster::map::filtered;
use crate::meta::cluster::targets::{self, TargetUsage};
use crate::meta::engine::txn::{MetaEngine, TxnOp, TxnRequest};
use crate::meta::ns;
use crate::meta::schema::keys;
use crate::meta::sessions::{self, Live};
use crate::meta::uninline::{self, InFlight};
use crate::net::server::{Responder, Service};
use crate::proto::frame::{InFrame, MSG_NODE_CALL};
use crate::proto::meta::{MetaCall, MetaOp, MetaReply, ReqHead};
use crate::proto::peer::{NodeHeartbeat, NodeOp, NodeReply};
use crate::proto::{decode_body, encode_body};

/// meta-read 线程数与它们的输入队列长度（1.2）。
const READ_THREADS: usize = 8;
const READ_QUEUE: usize = 1024;
/// root_squash 之后的 uid/gid（nobody，4.2）；会话操作的身份也记为它。
pub const NOBODY: u32 = 65534;

/// 交给 meta-read 线程的一个只读请求。
struct ReadJob {
    ctx: ReqCtx,
    op: ReadOp,
    r: Responder,
}

/// meta-read 线程上执行的只读操作。
enum ReadOp {
    Meta(MetaOp),
    Map { since: u64, full: bool }, // NodeOp::GetClusterMap；full = 带 keys、datakeys（只给 server 角色）
    Heartbeat(NodeHeartbeat),       // NodeOp::Heartbeat（M5）
    StatFs,                         // MetaOp::StatFs（M5）
    Uninline(u64),                  // MetaOp::Uninline（M5，ADR-0034）
}

/// meta-read 线程与服务共用的状态。
struct Shared {
    eng: Arc<dyn MetaEngine>,
    live: Arc<Live>,
    usage: TargetUsage,
    data: OnceLock<Arc<DataClient>>, // Uninline 用的数据面客户端（服务端装配时交进来）
    uninlining: InFlight,            // 正在做的 Uninline（见文件头）
}

/// 修改型请求成功之后，要不要更新 leader 内存里的会话状态（[`Live`]）。
#[derive(Clone, Copy)]
enum AfterOk {
    Nothing,
    SessionOpened,
    SessionClosed,
}

pub struct MetaService {
    eng: Arc<dyn MetaEngine>, // meta_engine 钩子（首发是 embedded-kv）：只用它的 txn / read / role
    reads: Sender<ReadJob>,
    debug_ops: bool,
    stats: Arc<OpStats>,
    shared: Arc<Shared>,
}

impl MetaService {
    /// 建服务并起 8 个 meta-read 线程（随进程一直运行）。`debug_ops` = node.toml 的同名开关。
    pub fn start(eng: Arc<dyn MetaEngine>, debug_ops: bool) -> Result<Arc<MetaService>, String> {
        let (tx, rx) = bounded::<ReadJob>(READ_QUEUE);
        let stats = Arc::new(OpStats::default());
        let live = Arc::new(Live::default());
        let uninlining = InFlight::new(READ_THREADS / 2);
        let shared = Arc::new(Shared {
            eng: eng.clone(),
            live,
            usage: TargetUsage::default(),
            data: OnceLock::new(),
            uninlining,
        });
        for i in 0..READ_THREADS {
            let (rx, stats, shared) = (rx.clone(), stats.clone(), shared.clone());
            std::thread::Builder::new()
                .name(format!("meta-read-{i}"))
                .spawn(move || {
                    while let Ok(j) = rx.recv() {
                        let name = j.op.name();
                        stats.reply_raw(j.r, name, read_job(&shared, &j.ctx, j.op));
                    }
                })
                .map_err(|e| format!("起 meta-read 线程失败：{e}"))?;
        }
        Ok(Arc::new(MetaService { eng, reads: tx, debug_ops, stats, shared }))
    }

    /// leader 内存里的会话状态（后台调度器的孤儿 GC 长期持有它；测试也看它）。
    pub fn live(&self) -> Arc<Live> {
        self.shared.live.clone()
    }

    /// 交进 `Uninline` 用的数据面客户端（服务端装配时，只第一次有效；与后台任务共用一个）。
    pub fn set_data(&self, data: Arc<DataClient>) {
        let _ = self.shared.data.set(data);
    }

    /// 调试操作只给 admin key，且服务端打开了 `debug_ops`（3.3.4）。
    fn debug_allowed(&self, r: &Responder) -> Result<(), ErrCode> {
        if r.identity().role == Role::Admin && self.debug_ops { Ok(()) } else { Err(ErrCode::EACCES) }
    }

    fn read(&self, ctx: ReqCtx, op: ReadOp, r: Responder) {
        let name = op.name();
        if let Err(TrySendError::Full(j) | TrySendError::Disconnected(j)) = self.reads.try_send(ReadJob { ctx, op, r })
        {
            self.stats.reply(j.r, name, Err(ErrCode::BUSY)); // meta-read 线程都忙：让客户端退避重试
        }
    }

    fn txn(&self, ctx: ReqCtx, name: &'static str, op: TxnOp, after: AfterOk, r: Responder) {
        let (stats, live, eng) = (self.stats.clone(), self.shared.live.clone(), self.eng.clone());
        let (session, seq) = (ctx.session, ctx.seq);
        let done = Box::new(move |res: Result<MetaReply, ErrCode>| {
            if res.is_ok() {
                match after {
                    AfterOk::Nothing => {}
                    AfterOk::SessionOpened => live.touch(session, eng.role().term, clock::mono_ms()),
                    AfterOk::SessionClosed => live.forget(session),
                }
            }
            if seq != 0 && fail_point_hit("meta.reply.drop") {
                log::warn(
                    "meta",
                    format_args!("fail point meta.reply.drop：吞掉 {name} 的回复（session {session:x}，seq {seq}）"),
                );
                return r.drop_reply();
            }
            stats.reply(r, name, res)
        });
        self.eng.txn(TxnRequest { ctx, op, done, exclusive: false });
    }

    fn node_call(&self, f: InFrame, r: Responder) {
        let op = match decode_body::<NodeOp>(&f.body) {
            Ok(op) => op,
            Err(e) => return r.error(e, 0),
        };
        match op {
            NodeOp::GetClusterMap { since_epoch } => {
                let id = r.identity();
                let full = id.role == Role::Server;
                let ctx = base_ctx(&ReqHead::default(), id, f.hdr.trace_id, NOBODY, NOBODY, vec![]);
                self.read(ctx, ReadOp::Map { since: since_epoch, full }, r);
            }
            NodeOp::Heartbeat(hb) => {
                let ctx = base_ctx(&ReqHead::default(), r.identity(), f.hdr.trace_id, 0, 0, vec![]);
                self.read(ctx, ReadOp::Heartbeat(hb), r); // 角色表已保证只有 Server 能发（3.3.3）
            }
            other => self.stats.reply(r, other.name(), Err(ErrCode::FEATURE_UNSUPPORTED)), // Join M8、SyncDone M9……
        }
    }
}

impl ReadOp {
    fn name(&self) -> &'static str {
        match self {
            ReadOp::Meta(op) => op.name(),
            ReadOp::Map { .. } => "GetClusterMap",
            ReadOp::Heartbeat(_) => "Heartbeat",
            ReadOp::StatFs => "StatFs",
            ReadOp::Uninline(_) => "Uninline",
        }
    }
}

impl Service for MetaService {
    fn call(&self, f: InFrame, r: Responder) {
        if f.hdr.msg_type == MSG_NODE_CALL {
            return self.node_call(f, r);
        }
        let call = match decode_body::<MetaCall>(&f.body) {
            Ok(c) => c,
            Err(e) => return r.error(e, 0), // 不认识的变体 → FEATURE_UNSUPPORTED，不断开（3.3.2）
        };
        let name = call.op.name();
        let after = match call.op {
            MetaOp::SessionOpen { .. } => AfterOk::SessionOpened,
            MetaOp::SessionClose => AfterOk::SessionClosed,
            _ => AfterOk::Nothing,
        };
        let session_op =
            matches!(call.op, MetaOp::SessionOpen { .. } | MetaOp::SessionHeartbeat { .. } | MetaOp::SessionClose);
        let mut ctx = if session_op {
            base_ctx(&call.head, r.identity(), f.hdr.trace_id, NOBODY, NOBODY, vec![]) // 见文件头：不代表任何用户
        } else {
            match req_ctx(&call.head, r.identity(), f.hdr.trace_id) {
                Ok(c) => c,
                Err(e) => return self.stats.reply(r, name, Err(e)),
            }
        };
        match call.op {
            MetaOp::DebugPut { key, value } => {
                if let Err(e) = self.debug_allowed(&r) {
                    return self.stats.reply(r, name, Err(e));
                }
                ctx.seq = 0; // 调试写不进去重表（M3 起就是这样）
                let op: TxnOp = Box::new(move |v, _c| {
                    v.put(&keys::debug(&key), value);
                    Ok(MetaReply::Ok)
                });
                self.txn(ctx, name, op, AfterOk::Nothing, r);
            }
            op @ MetaOp::DebugGet { .. } => match self.debug_allowed(&r) {
                Ok(()) => self.read(ctx, ReadOp::Meta(op), r),
                Err(e) => self.stats.reply(r, name, Err(e)),
            },
            op @ MetaOp::SessionHeartbeat { .. } => self.read(ctx, ReadOp::Meta(op), r),
            MetaOp::StatFs => self.read(ctx, ReadOp::StatFs, r),
            MetaOp::Uninline { ino } => {
                ctx.seq = 0; // 幂等，按只读请求的方式处理（ADR-0034 第 2 条）
                self.read(ctx, ReadOp::Uninline(ino), r);
            }
            op if ns::is_read(&op) => self.read(ctx, ReadOp::Meta(op), r),
            op => {
                let Some(op) = ns::txn_op(op) else {
                    return self.stats.reply(r, name, Err(ErrCode::FEATURE_UNSUPPORTED)); // 后面的里程碑实现
                };
                if session_op {
                    ctx.seq = 0;
                } else if ctx.seq == 0 || ctx.session == 0 {
                    return self.stats.reply(r, name, Err(ErrCode::EINVAL)); // 修改型请求必须带会话与 seq
                }
                self.txn(ctx, name, op, after, r);
            }
        }
    }
}

/// meta-read 线程上执行一个只读请求：线性一致读（先过读闸门）。返回编码好的回复。
fn read_job(sh: &Shared, ctx: &ReqCtx, op: ReadOp) -> Result<Vec<u8>, ErrCode> {
    let (eng, live) = (&*sh.eng, &*sh.live);
    match op {
        ReadOp::Heartbeat(hb) => targets::heartbeat(eng, ctx, &sh.usage, hb),
        ReadOp::StatFs => targets::statfs(eng, ctx, &sh.usage).map(|r| encode_body(&r)),
        ReadOp::Uninline(ino) => {
            let data = sh.data.get().ok_or(ErrCode::NOT_READY)?;
            sh.uninlining.begin(ino)?; // 这个 inode 已经在做、或已经有 4 个在做 → BUSY（见文件头）
            let r = uninline::run(eng, data, ctx, ino);
            sh.uninlining.end(ino);
            r.map(|r| encode_body(&r))
        }
        ReadOp::Meta(MetaOp::DebugGet { key }) => {
            let r = eng.read(ctx, &mut |v, _c| Ok(MetaReply::DebugValue { value: v.get(&keys::debug(&key))? }))?;
            Ok(encode_body(&r))
        }
        ReadOp::Meta(MetaOp::SessionHeartbeat { opened, closed, full }) => {
            let (mut lease_ms, mut map_epoch) = (0, 0);
            // 这个闭包只用来在 ReadIndex 之后的视图上检查会话、取租约时长与 map_epoch，它的返回值丢弃
            eng.read(ctx, &mut |v, c| {
                sessions::usable(v, ctx.session, ctx.who.conn.key_id)?;
                (lease_ms, map_epoch) = (c.settings.session_lease_ms, c.map.map_epoch);
                Ok(MetaReply::Ok)
            })?;
            // ReadIndex 确认了自己仍是 leader、会话还活着，才续租（R*，3.3.4）
            let need = live.heartbeat(ctx.session, eng.role().term, clock::mono_ms(), opened, closed, full);
            Ok(encode_body(&MetaReply::Heartbeat { lease_ms, map_epoch, need_full_open_set: need }))
        }
        ReadOp::Meta(op) => eng.read(ctx, &mut |v, c| ns::read(v, c, &op)).map(|r| encode_body(&r)),
        ReadOp::Map { since, full } => {
            let mut out = None;
            // 这个闭包只用来在 ReadIndex 之后的视图上取集群图快照，它的返回值丢弃
            eng.read(ctx, &mut |_v, c| {
                out = Some(if full { c.map.clone() } else { filtered(c.map) });
                Ok(MetaReply::Ok)
            })?;
            let m = out.ok_or(ErrCode::INTERNAL)?;
            let reply = if since < m.map_epoch { NodeReply::Map(Box::new(m)) } else { NodeReply::Ok };
            Ok(encode_body(&reply))
        }
    }
}

/// 请求上下文（见文件头的实现注记）：身份来自连接（握手时认证），uid/gid 来自请求头。`now` 是 leader 的墙钟，写进写集。
fn req_ctx(h: &ReqHead, id: &ConnIdentity, trace_id: u64) -> Result<ReqCtx, ErrCode> {
    let (mut uid, mut gid, mut groups) = (h.uid, h.gid, h.groups.clone());
    if id.role == Role::Client {
        if !id.uid_ranges.iter().any(|(lo, hi)| (*lo..=*hi).contains(&uid)) {
            return Err(ErrCode::EACCES); // 这把 key 不许声明这个 uid
        }
        if id.root_squash {
            uid = if uid == 0 { NOBODY } else { uid };
            gid = if gid == 0 { NOBODY } else { gid };
            groups.retain(|g| *g != 0);
        }
    }
    Ok(base_ctx(h, id, trace_id, uid, gid, groups))
}

fn base_ctx(h: &ReqHead, id: &ConnIdentity, trace_id: u64, uid: u32, gid: u32, groups: Vec<u32>) -> ReqCtx {
    let conn = Arc::new(id.clone());
    let who = Principal { uid, gid, groups, tenant: id.tenant, job_id: h.job_id, conn };
    let (session, seq, pid) = (h.session, h.seq, h.pid);
    ReqCtx { who, session, seq, pid, trace_id, now: clock::wall_ts(), now_ms: clock::wall_ms(), internal: false }
}

/// `wmfs_meta_ops_total{op,code}`：每个 (操作, 结果码) 的计数器第一次出现时注册，之后从这张小表里取。
#[derive(Default)]
struct OpStats {
    counters: Mutex<Vec<(&'static str, ErrCode, Arc<Counter>)>>,
}

impl OpStats {
    fn reply(&self, r: Responder, op: &'static str, res: Result<MetaReply, ErrCode>) {
        self.reply_raw(r, op, res.map(|reply| encode_body(&reply)));
    }

    fn reply_raw(&self, r: Responder, op: &'static str, res: Result<Vec<u8>, ErrCode>) {
        let code = res.as_ref().err().copied().unwrap_or(ErrCode(0));
        self.counter(op, code).inc();
        match res {
            Ok(body) => r.reply(body, vec![]),
            Err(c) => r.error(c, 0),
        }
    }

    fn counter(&self, op: &'static str, code: ErrCode) -> Arc<Counter> {
        let mut v = lock(&self.counters);
        if let Some((_, _, c)) = v.iter().find(|(o, c, _)| *o == op && *c == code) {
            return c.clone();
        }
        let name = if code == ErrCode(0) { "OK".to_string() } else { code.to_string() };
        let c = metrics::counter("wmfs_meta_ops_total", "元数据服务处理的请求数", &[("op", op), ("code", &name)]);
        v.push((op, code, c.clone()));
        c
    }
}
