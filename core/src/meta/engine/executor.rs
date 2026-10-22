//! 单飞批次串行执行器 `meta-exec`（design.md 3.4.1.4，ADR-0019）。只在 leader 上干活。
//!
//! 一个批次在 raft 里提交的几毫秒内（本机 redb Immediate 提交约 2.6 ms），新到的请求排队，下一批一次带走——负载越高
//! 批越大。**同一时刻 raft 里最多只有一个批次在途**：构建下一批时上一批已经应用，执行器直接读"已提交状态"。
//! 边取边执行（`fill`）：每执行完一个请求就看一次结束条件——独占操作 / 刚生成了新链 / 256 个 / 写集 ≥ 768 KiB——
//! 到了就不再从队列取，没取的请求留给下一批。回复在批次有了确定结果之后才发出（规则 2，`settle`）：`Applied{ok: true}` →
//! 各自的结果；批次作废或没进日志 → `RETRY`（客户端用同一个 seq 重试）；结果不确定 → `NOT_LEADER`（去重表兜底）。
//! 每个批次开始时从批次的读事务里取一份集群图快照（`OpCtx.map`，按 map_epoch 缓存）：placement 读它，所以生成了新链的
//! 操作之后本批结束（规则 5），下一批的快照里就有新链。

use std::sync::Arc;
use std::time::Instant;

use crossbeam_channel::Receiver;
use raft::StateRole;
use wmfs_api::failpoint::fail_point;

use super::dedup;
use super::engine::{EngineShared, OpEnv};
use super::txn::{TxnOp, TxnRequest};
use super::view::{BatchView, ReadView};
use crate::base::ErrCode;
use crate::base::types::ReqCtx;
use crate::meta::raft::driver::ProposeOutcome;
use crate::meta::raft::{or_abort, storage};
use crate::meta::schema::records::WriteBatchV1;
use crate::meta::sessions;
use crate::proto::meta::{DEDUP_WINDOW, MetaReply};
use crate::proto::peer::ClusterMap;

/// 批次上限（3.4.1.4）：≤ 256 个操作；写集编码字节数一旦 ≥ 768 KiB 就结束本批。
const MAX_BATCH_OPS: usize = 256;
const MAX_BATCH_BYTES: usize = 768 << 10;

type Done = Box<dyn FnOnce(Result<MetaReply, ErrCode>) + Send>;

/// 一个已经执行、等批次结果的请求。
struct Executed {
    done: Done,
    result: Result<MetaReply, ErrCode>,
}

/// 执行器线程本体：没有请求就一直睡（没有定时工作要做）；请求通道断开就退出。
pub(crate) fn run(rx: Receiver<TxnRequest>, shared: Arc<EngineShared>) {
    while let Ok(first) = rx.recv() {
        let role = shared.published.role();
        if !role.ready {
            // 非 leader、新 leader 尚未应用本 term 的空条目、或插件清单不一致（3.4.1.4 规则 3）
            let code = if role.role == StateRole::Leader { ErrCode::NOT_READY } else { ErrCode::NOT_LEADER };
            (first.done)(Err(code));
            while let Ok(r) = rx.try_recv() {
                (r.done)(Err(code));
            }
            continue;
        }
        let rtx = or_abort(shared.db.begin_read(), "begin_read"); // redb 出错 = 写日志并 abort（0.2 第 5 条）
        let base_version = or_abort(storage::read_applied(&rtx), "读 applied").state_version;
        let map = or_abort(ReadView::new(&rtx).and_then(|v| shared.map_for(&v)), "读集群图");
        let mut view = BatchView::new(&rtx);
        let (results, trace_ids) = fill(&mut view, first, &rx, &shared.env, &map);
        if view.is_empty() {
            // 只可能全是"已提交的事实"：去重命中、会话已驱逐、内部 CAS 前提不成立
            results.into_iter().for_each(|e| (e.done)(e.result));
            continue;
        }
        if let Err(c) = fail_point("meta.exec.before_propose") {
            results.into_iter().for_each(|e| (e.done)(Err(ErrCode(c))));
            continue;
        }
        let (n, bytes) = (results.len(), view.encoded_len());
        let wb = WriteBatchV1 { expect_version: base_version, leader_term: role.term, ops: view.into_ops(), trace_ids };
        drop(rtx);
        let t0 = Instant::now();
        let outcome = shared.propose_and_wait(&wb); // 阻塞到 raft 线程给出 Applied/Lost/Dropped（raft 线程保证必有其一）
        shared.observe_batch(n, bytes, t0.elapsed());
        for e in results {
            (e.done)(settle(outcome, e.result));
        }
    }
}

/// 批次的结局 → 每个请求的回复（3.4.1.4 规则 2、3.4.1.5）：批次已应用 → 各自执行时的结果（含 `EEXIST` 这类业务错误）；
/// 批次作废（expect_version 不符）或没进日志 → `RETRY`（一个 op 也没有应用，客户端用同一个 seq 重试）；
/// 结果不确定 → `NOT_LEADER`（重试时去重表兜底）。
fn settle(outcome: ProposeOutcome, result: Result<MetaReply, ErrCode>) -> Result<MetaReply, ErrCode> {
    match outcome {
        ProposeOutcome::Applied { ok: true } => result,
        ProposeOutcome::Applied { ok: false } | ProposeOutcome::Dropped => Err(ErrCode::RETRY),
        ProposeOutcome::Lost => Err(ErrCode::NOT_LEADER),
    }
}

/// 边取边执行：从 `first` 开始，每执行完一个请求看一次结束条件，到了就不再从 `rx` 取。返回各请求的结果与 trace_id。
fn fill(
    view: &mut BatchView,
    first: TxnRequest,
    rx: &Receiver<TxnRequest>,
    env: &OpEnv,
    map: &ClusterMap,
) -> (Vec<Executed>, Vec<u64>) {
    let (mut out, mut traces) = (Vec::new(), Vec::new());
    let mut next = Some(first);
    while let Some(req) = next.take() {
        let end_here = req.exclusive; // 集群拓扑类内部操作：必须是本批最后一个（规则 5）
        if req.ctx.trace_id != 0 {
            traces.push(req.ctx.trace_id);
        }
        let result = run_one(view, env, map, &req.ctx, req.op);
        out.push(Executed { done: req.done, result });
        if end_here || view.topology_changed() || out.len() >= MAX_BATCH_OPS || view.encoded_len() >= MAX_BATCH_BYTES {
            break;
        }
        next = rx.try_recv().ok();
    }
    (out, traces)
}

/// 执行一个请求（3.4.1.4 run_one）：会话已驱逐 → `SESSION_EVICTED`；会话是别的 key 打开的 → `EACCES`（实现注记（M4），
/// `sessions::usable`）；去重命中 → 原结果；否则在操作层里执行闭包，失败就整体丢弃它的写入；最终结果（成功或 POSIX errno）
/// 写进去重表，并点删 seq − 256 的旧记录（规则 4）。
fn run_one(view: &mut BatchView, env: &OpEnv, map: &ClusterMap, ctx: &ReqCtx, op: TxnOp) -> Result<MetaReply, ErrCode> {
    if ctx.seq != 0 {
        sessions::usable(view, ctx.session, ctx.who.conn.key_id)?; // 驱逐是单调的事实；会话不能借给别的 key
        if let Some(r) = dedup::get(view, ctx.session, ctx.seq) {
            return r; // 重试：返回原结果
        }
    }
    view.begin_op();
    let res = op(view, &env.op_ctx(ctx, map)); // 内部调用 authz / quota / placement 钩子
    if res.is_err() {
        view.rollback_op();
    }
    if ctx.seq != 0 && dedup::is_final(&res) {
        dedup::put(view, ctx.session, ctx.seq, &res); // 错误也记录：重试得到同样的答案，且批次写集必然非空
        if ctx.seq > DEDUP_WINDOW {
            dedup::delete(view, ctx.session, ctx.seq - DEDUP_WINDOW); // 点删除，无需扫描
        }
    }
    // 与伪代码"成功 commit_op、失败 rollback_op"等价：失败时操作层已经清空，这里并入批次的只有去重记录
    // （3.4.1.4 规则 2：错误也写去重表）
    view.commit_op();
    // changelog（3.4.1.4 的 changelog::record）：成功的修改由各操作在自己的闭包末尾写（meta/changelog.rs 的实现注记）；
    // 被拒绝的修改只在 audited 时写——audit 属性在 M12，到时在这里补上
    res
}

#[cfg(test)]
mod tests;
