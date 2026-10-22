//! `MetaCall`（0x0100）消息族的服务端（design.md 3.3.3、3.3.4、1.2）。
//!
//! [`Service::call`] 在连接读线程上执行，**自己从不执行请求**：修改型请求连同回复回调交给执行器（`MetaEngine::txn`，
//! 投递后立即返回），只读请求交给 8 个 `meta-read` 线程（它们在读闸门上等 ReadIndex，再开 redb 读事务）。
//! 回复经 `Responder`（只 try_send 进连接的发送队列，不碰 socket）。指标 `wmfs_meta_ops_total{op,code}`。
//!
//! M3 只有调试键空间 `0x2F`：`DebugPut`（写，经 raft）与 `DebugGet`（线性一致读），只给 admin key，且服务端 node.toml 里
//! 要有 `debug_ops = true`（`wmfs dev` 默认打开），否则 `EACCES`。其余操作 M4 起实现，现在回 `FEATURE_UNSUPPORTED`。

use std::sync::{Arc, Mutex};

use crossbeam_channel::{Sender, TrySendError, bounded};
use wmfs_api::metrics::{self, Counter};

use crate::base::types::{Principal, ReqCtx, Role};
use crate::base::{ErrCode, clock};
use crate::hooks::traits::ConnIdentity;
use crate::meta::engine::txn::{MetaEngine, TxnOp, TxnRequest};
use crate::meta::schema::keys;
use crate::net::server::{Responder, Service};
use crate::proto::frame::InFrame;
use crate::proto::meta::{MetaCall, MetaOp, MetaReply, ReqHead};
use crate::proto::{decode_body, encode_body};

/// meta-read 线程数与它们的输入队列长度（1.2）。
const READ_THREADS: usize = 8;
const READ_QUEUE: usize = 1024;

/// 交给 meta-read 线程的一个只读请求。
struct ReadJob {
    ctx: ReqCtx,
    op: MetaOp,
    r: Responder,
}

pub struct MetaService {
    eng: Arc<dyn MetaEngine>, // meta_engine 钩子（首发是 embedded-kv）：只用它的 txn / read
    reads: Sender<ReadJob>,
    debug_ops: bool,
    stats: Arc<OpStats>,
}

impl MetaService {
    /// 建服务并起 8 个 meta-read 线程（随进程一直运行）。`debug_ops` = node.toml 的同名开关。
    pub fn start(eng: Arc<dyn MetaEngine>, debug_ops: bool) -> Result<Arc<MetaService>, String> {
        let (tx, rx) = bounded::<ReadJob>(READ_QUEUE);
        let stats = Arc::new(OpStats::default());
        for i in 0..READ_THREADS {
            let (rx, eng, stats) = (rx.clone(), eng.clone(), stats.clone());
            std::thread::Builder::new()
                .name(format!("meta-read-{i}"))
                .spawn(move || {
                    while let Ok(j) = rx.recv() {
                        let name = j.op.name();
                        stats.reply(j.r, name, read_op(&*eng, &j.ctx, j.op));
                    }
                })
                .map_err(|e| format!("起 meta-read 线程失败：{e}"))?;
        }
        Ok(Arc::new(MetaService { eng, reads: tx, debug_ops, stats }))
    }

    /// 调试操作只给 admin key，且服务端打开了 `debug_ops`（3.3.4）。
    fn debug_allowed(&self, r: &Responder) -> Result<(), ErrCode> {
        if r.identity().role == Role::Admin && self.debug_ops { Ok(()) } else { Err(ErrCode::EACCES) }
    }
}

impl Service for MetaService {
    fn call(&self, f: InFrame, r: Responder) {
        let call = match decode_body::<MetaCall>(&f.body) {
            Ok(c) => c,
            Err(e) => return r.error(e, 0), // 不认识的变体 → FEATURE_UNSUPPORTED，不断开（3.3.2）
        };
        let name = call.op.name();
        let ctx = req_ctx(&call.head, r.identity(), f.hdr.trace_id);
        match call.op {
            MetaOp::DebugPut { key, value } => {
                if let Err(e) = self.debug_allowed(&r) {
                    return self.stats.reply(r, name, Err(e));
                }
                let op: TxnOp = Box::new(move |v, _c| {
                    v.put(&keys::debug(&key), value);
                    Ok(MetaReply::Ok)
                });
                let stats = self.stats.clone();
                let done = Box::new(move |res| stats.reply(r, name, res));
                self.eng.txn(TxnRequest { ctx, op, done, exclusive: false });
            }
            op @ MetaOp::DebugGet { .. } => {
                if let Err(e) = self.debug_allowed(&r) {
                    return self.stats.reply(r, name, Err(e));
                }
                if let Err(TrySendError::Full(j) | TrySendError::Disconnected(j)) =
                    self.reads.try_send(ReadJob { ctx, op, r })
                {
                    self.stats.reply(j.r, name, Err(ErrCode::BUSY)); // meta-read 线程都忙：让客户端退避重试
                }
            }
            _ => self.stats.reply(r, name, Err(ErrCode::FEATURE_UNSUPPORTED)), // M4 起实现
        }
    }
}

/// meta-read 线程上执行一个只读请求：线性一致读（先过读闸门）。
fn read_op(eng: &dyn MetaEngine, ctx: &ReqCtx, op: MetaOp) -> Result<MetaReply, ErrCode> {
    match op {
        MetaOp::DebugGet { key } => {
            eng.read(ctx, &mut |v, _c| Ok(MetaReply::DebugValue { value: v.get(&keys::debug(&key))? }))
        }
        _ => Err(ErrCode::FEATURE_UNSUPPORTED),
    }
}

/// 请求上下文：身份来自连接（握手时认证），uid/gid 来自请求头。实现注记（M3）：按 key 的 uid 范围校验、root_squash 映射
/// 在 M4（命名空间操作开始用它们）。`now` 是 leader 的墙钟，写进写集（follower 不再取时间）。
fn req_ctx(h: &ReqHead, id: &ConnIdentity, trace_id: u64) -> ReqCtx {
    let conn = Arc::new(id.clone());
    let who = Principal { uid: h.uid, gid: h.gid, groups: h.groups.clone(), tenant: id.tenant, job_id: h.job_id, conn };
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
        let code = res.as_ref().err().copied().unwrap_or(ErrCode(0));
        self.counter(op, code).inc();
        match res {
            Ok(reply) => r.reply(encode_body(&reply), vec![]),
            Err(c) => r.error(c, 0),
        }
    }

    fn counter(&self, op: &'static str, code: ErrCode) -> Arc<Counter> {
        let mut v = self.counters.lock().unwrap_or_else(|e| e.into_inner());
        if let Some((_, _, c)) = v.iter().find(|(o, c, _)| *o == op && *c == code) {
            return c.clone();
        }
        let name = if code == ErrCode(0) { "OK".to_string() } else { code.to_string() };
        let c = metrics::counter("wmfs_meta_ops_total", "元数据服务处理的请求数", &[("op", op), ("code", &name)]);
        v.push((op, code, c.clone()));
        c
    }
}
