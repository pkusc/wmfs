//! PeerCall（0x0300）服务与 repl 线程（design.md 1.2、3.4.2.4、3.4.2.9）。
//!
//! 连接读线程上的 [`PeerService`] 只分派：转发（ForwardWrite / ForwardSync / ForwardDeleteEpoch）进 repl 队列（满了回 `BUSY`）；同步源
//! 的 ListChunks / FetchChunk 进数据服务的 QoS 队列（io 线程、后台低优先级，3.4.2.8）。4 个 `repl-<i>` 线程处理转发到达、转发的回复
//! （确认 → 提交并逆向确认；NeedFull → 完整重发；错误或超时 → 回复 upstream RETRY、留给 chain-timer），**从不等网络**。
//!
//! 后继处理 ForwardWrite（3.4.2.4 的状态表）：先比 `chain_ver`（请求的更新 → 通知 hb 线程立即拉集群图；不一致一律 `STALE_CHAIN`）、
//! `from_target` 必须是本地视图里的前驱（I4）、自己在链上、I5（`FENCED`）；然后占住 chunk：
//! - `pending.version == V` → 重新驱动的重复：用本请求替换 upstream；自己是尾 → 提交并确认；否则完整重发给当前后继——**先看这一格**
//!   （审查后）：3.4.2.6 规则 2 在本地提交了、还在等后面成员的 pending 也走这里，后面的成员都确认了才回（I3、附录 C 的 C-9），不能
//!   因为"committed ≥ V"马上确认；
//! - `committed ≥ V` → 重复：回 `Ack{committed}`（I6：同版本即同内容）；
//! - 增量且 `committed == base_version`、没有 pending，或完整转发 → 写 pending（完整转发先放弃旧 pending、整份替换本地）；自己是尾 →
//!   提交并确认；否则把同一份转发（增量或完整，bulk 零拷贝）交给后继并登记；
//! - 其他（缺基础版本、或有更旧的 pending，增量）→ 自己是 Syncing 回 `NeedFull`；Serving 回 `INTERNAL`（不变量破坏，写错误日志；
//!   head 的重新驱动用完整转发修好它）。
//!
//! Syncing 成员在本地提交了转发来的版本（自己是尾、重复、收到后继的确认），就把 chunk 加进本轮同步的 touched 集合（3.4.2.8；回
//! NeedFull 的不算）。后继落后——回 `STALE_CHAIN` 且 extra（它的 chain_ver）比这个 pending 发出时的旧，或回 `NOT_READY`/`FENCED`（它还没
//! 拿到集群图、自我隔离着：多半刚起来还没被确认）——它很快就能收：chain-timer 约 0.5 s 后就重发、upstream 留着；本节点的视图更旧、
//! 或本节点自己也自我隔离着（多半与 leader 分区）→ 照第 8 步回 RETRY，3 s 一次（换上新图时规则 4 会重发）。
//! fail point `chain.ack.drop`：丢掉一个转发的回复（pending 留着，chain-timer 3 s 后重新驱动，后继回重复的 `Ack{committed}`）。

use std::sync::Arc;

use crossbeam_channel::{TrySendError, bounded};
use wmfs_api::failpoint::fail_point_hit;

use super::format::BlockEntry;
use super::packed;
use super::service::DataService;
use super::state::{PendingWrite, Upstream};
use super::write::{self, WriteJob};
use super::{DataNode, chain, forward, read, redrive, sync};
use crate::base::types::ChunkId;
use crate::base::{ErrCode, clock, log};
use crate::buf::{Buf, FrozenBuf};
use crate::net::client::{RpcClient, check_reply};
use crate::net::server::{Responder, Service};
use crate::proto::frame::InFrame;
use crate::proto::peer::{ForwardWrite, PeerOp, PeerReply};
use crate::proto::{decode_body, encode_body};

const REPL_THREADS: usize = 4;
const REPL_QUEUE: usize = 4096;

/// repl 队列里的一项。
pub enum ReplJob {
    Peer { op: PeerOp, bulk: Option<Buf>, r: Responder }, // 前驱发来的转发
    Reply { chunk: ChunkId, version: u64, succ: u32, res: Result<InFrame, ErrCode> }, // 我们发出的转发的回复
    Drain(Box<WriteJob>), // 链变化时解决了 pending 的 chunk 上排着的写：换上新视图之后按它做（redrive.rs 的 `switch`）
}

pub struct PeerService {
    node: Arc<DataNode>,
    data: Arc<DataService>,
}

/// 起 repl 线程、chain-timer 与同步线程，交出 PeerCall 服务（服务端装配时；`rpc` = 服务端之间的 RPC 客户端，key 0）。
pub fn start(node: &Arc<DataNode>, rpc: Arc<RpcClient>, data: Arc<DataService>) -> Result<Arc<PeerService>, String> {
    let (tx, rx) = bounded(REPL_QUEUE);
    let _ = (node.peer.set(rpc), node.repl.set(tx));
    for i in 0..REPL_THREADS {
        let (node, rx) = (node.clone(), rx.clone());
        std::thread::Builder::new()
            .name(format!("repl-{i}"))
            .spawn(move || {
                while let Ok(j) = rx.recv() {
                    handle(&node, j);
                }
            })
            .map_err(|e| format!("起 repl 线程失败：{e}"))?;
    }
    redrive::start(node)?;
    sync::start(node)?;
    Ok(Arc::new(PeerService { node: node.clone(), data }))
}

impl Service for PeerService {
    fn call(&self, f: InFrame, r: Responder) {
        let op: PeerOp = match decode_body(&f.body) {
            Ok(op) => op,
            Err(e) => return r.error(e, 0),
        };
        if matches!(op, PeerOp::ListChunks { .. } | PeerOp::FetchChunk { .. }) {
            return self.data.peer(op, r); // 同步源：io 线程、后台低优先级
        }
        let Some(q) = self.node.repl.get() else { return r.error(ErrCode::NOT_SERVING, 0) };
        if let Err(TrySendError::Full(j) | TrySendError::Disconnected(j)) =
            q.try_send(ReplJob::Peer { op, bulk: f.bulk, r })
            && let ReplJob::Peer { r, .. } = j
        {
            r.error(ErrCode::BUSY, 0); // repl 队列满：前驱保留 pending，chain-timer 稍后重发
        }
    }
}

fn handle(node: &Arc<DataNode>, j: ReplJob) {
    match j {
        ReplJob::Peer { op: PeerOp::ForwardWrite(fw), bulk, r } => {
            match check(node, fw.chunk.chain, fw.chain_ver, fw.from_target) {
                Err(chain::Fail(e, x)) => r.error(e, x),
                Ok(here) if node.chunks.claim(&fw.chunk) => apply(node, fw, bulk, here, Upstream::Peer(r)),
                Ok(_) => r.error(ErrCode::BUSY, 0), // 别的线程正在写这个 chunk（同步安装、上一个转发）：前驱稍后重发
            }
        }
        ReplJob::Peer { op, r, .. } => propagate(node, op, r),
        ReplJob::Reply { chunk, version, succ, res } => on_reply(node, &chunk, version, succ, res),
        ReplJob::Drain(j) => write::drain(node, Some(j)),
    }
}

/// 本节点在链上的样子。
struct Here {
    target: u32,
    chain_ver: u64,
    succ: Option<u32>,
    syncing: bool,
}

/// 转发的前置检查（见文件头）。
fn check(node: &DataNode, chain_id: u32, ver: u64, from: u32) -> Result<Here, chain::Fail> {
    let map = node.map.get().ok_or(ErrCode::NOT_READY)?;
    let c = chain::current(node, &map, chain_id, ver)?;
    let (target, pos) = chain::local(node, c).ok_or(ErrCode::NOT_SERVING)?;
    if chain::predecessor(c, pos) != Some(from) {
        return Err(chain::Fail(ErrCode::STALE_CHAIN, c.ver)); // I4：只接受当前前驱的转发
    }
    chain::check_i5(node, &map, target)?;
    Ok(Here { target, chain_ver: c.ver, succ: chain::successor(c, pos), syncing: !chain::serving_at(c, pos) })
}

/// 放下 chunk 再回复（前驱收到回复就可能马上发下一个，3.4.2.3）。
fn done(node: &Arc<DataNode>, id: &ChunkId, reply: impl FnOnce()) {
    let next = node.chunks.finish(id);
    reply();
    super::write::drain(node, next);
}

/// 3.4.2.4 的状态表（见文件头；调用者已占住 chunk）。
fn apply(node: &Arc<DataNode>, fw: ForwardWrite, bulk: Option<Buf>, here: Here, up: Upstream) {
    let id = fw.chunk;
    let (committed, base) = match local_state(node, here.target, &id) {
        Ok(x) => x,
        Err(e) => return done(node, &id, || up.fail(e)),
    };
    match node.chunks.take_held(&id) {
        Some(mut p) if p.version == fw.version => {
            if let Some(prev) = p.upstream.replace(up) {
                prev.fail(ErrCode::RETRY); // 前驱重新驱动了：以新的这个请求为准
            }
            match here.succ {
                None => write::drain(node, forward::settle(node, &id, p)),
                Some(s) => {
                    node.chunks.set_pending(&id, p);
                    forward::resend_full(node, &id, s);
                }
            }
        }
        old => match committed.filter(|c| *c >= fw.version) {
            Some(c) => {
                if let Some(p) = old {
                    node.chunks.put_held(&id, p); // 更新的 pending 留着
                }
                sync::touch(node, &id, here.target);
                done(node, &id, || up.ok(c)) // 重复：同版本即同内容（I6）
            }
            None => accept(node, fw, bulk.map(|b| b.freeze()), here, up, Local { committed, base, old }),
        },
    }
}

/// 本地的样子：已提交的版本（None = 被标为 Corrupt、头坏了）、它的块表、更旧的 pending。
struct Local {
    committed: Option<u64>,
    base: Vec<BlockEntry>,
    old: Option<PendingWrite>,
}

/// 状态表的后两格：能用这个转发 → 写 pending（完整转发先放弃更旧的 pending）、提交或交给后继；不能 → NeedFull / INTERNAL。
fn accept(node: &Arc<DataNode>, mut fw: ForwardWrite, data: Option<FrozenBuf>, here: Here, up: Upstream, l: Local) {
    let id = fw.chunk;
    let fits = l.committed == Some(fw.base_version) && l.old.is_none();
    if !fw.full && !fits {
        if let Some(p) = l.old {
            node.chunks.put_held(&id, p); // 放回去：这个转发不能用
        }
        if here.syncing {
            return done(node, &id, || up.responder().reply(encode_body(&PeerReply::NeedFull), vec![]));
        }
        let (want, have) = (fw.base_version, l.committed);
        log::error("data", format_args!("不变量破坏：Serving 成员缺 {id:?} 的基础版本 {want}（本地 {have:?}）"));
        return done(node, &id, || up.fail(ErrCode::INTERNAL));
    }
    if let Some(p) = l.old.and_then(|p| p.upstream) {
        p.fail(ErrCode::RETRY); // 完整转发：放弃更旧的 pending，整份替换本地
    }
    let bytes = data.as_ref().map_or(&[][..], |d| d.as_slice());
    let clean = match write_pending(node, here.target, &fw, &l.base, bytes) {
        Ok(clean) => clean,
        Err(e) => return done(node, &id, || up.fail(e)),
    };
    let p = PendingWrite {
        version: fw.version,
        target: here.target,
        upstream: Some(up),
        successor: here.succ,
        chain_ver: here.chain_ver,
        last_sent: clock::mono_ms(),
        sync: fw.sync,
        clean,
        committed: false,
    };
    match here.succ {
        None => write::drain(node, forward::settle(node, &id, p)), // 自己是尾：提交并确认
        Some(s) => {
            node.chunks.set_pending(&id, p);
            fw.from_target = here.target;
            forward::send(node, fw, data.map(|d| vec![d.whole()]).unwrap_or_default(), s);
        }
    }
}

/// 本地已提交的版本号与块表（不存在 → (0, 空)）；被标为 Corrupt 或头坏了 → (None, 空)：只能接受完整转发。
fn local_state(node: &DataNode, t: u32, id: &ChunkId) -> Result<(Option<u64>, Vec<BlockEntry>), ErrCode> {
    let tg = node.targets.get(&t).ok_or(ErrCode::NOT_SERVING)?;
    if node.chunks.is_corrupt(id) {
        return Ok((None, vec![]));
    }
    match read::open(node, &*tg.store, id) {
        Ok(Some(c)) => Ok((Some(c.header.chunk_version), c.table)),
        Ok(None) => Ok((Some(0), vec![])),
        Err(ErrCode::CORRUPT) => {
            node.report_corrupt(tg, id, ErrCode::CORRUPT);
            Ok((None, vec![]))
        }
        Err(e) => Err(e),
    }
}

/// 把转发写成本地的 pending 文件（完整：packed；增量：基础块表 + 改动条目）；返回"没有从旧版本复制任何块"。
fn write_pending(
    node: &DataNode,
    t: u32,
    fw: &ForwardWrite,
    base: &[BlockEntry],
    bulk: &[u8],
) -> Result<bool, ErrCode> {
    let tg = node.targets.get(&t).ok_or(ErrCode::NOT_SERVING)?;
    let id = &fw.chunk;
    let (h, head, mut writes, copies) = if fw.full {
        let (h, w) = packed::unpack(bulk)?;
        (h, vec![], w, vec![])
    } else {
        let a = packed::incremental(base, &fw.header, &fw.blocks, bulk)?;
        (a.header, a.head, a.blocks, a.copies)
    };
    let who = (h.ino, h.epoch, h.chunk_idx, h.chain_id, h.chunk_version);
    if who != (id.ino, id.epoch, id.idx, id.chain, fw.version) || h.cluster_id != node.cluster_id {
        return Err(ErrCode::EINVAL);
    }
    if !head.is_empty() {
        writes.insert(0, (0, head.as_slice()));
    }
    if let Err(e) = node.store(|| tg.store.write_pending(id, &writes, &copies)) {
        let _ = node.store(|| tg.store.abort_pending(id));
        return Err(e);
    }
    tg.write_bytes.add(writes.iter().map(|(_, d)| d.len() as u64).sum());
    Ok(copies.is_empty())
}

/// 我们发出的转发有了结果（3.4.2.3 第 6–8 步）。
fn on_reply(node: &Arc<DataNode>, id: &ChunkId, version: u64, succ: u32, res: Result<InFrame, ErrCode>) {
    if fail_point_hit("chain.ack.drop") {
        return log::warn(
            "data",
            format_args!("fail point chain.ack.drop：丢掉 {id:?} v{version} 的确认（等 chain-timer 重新驱动）"),
        );
    }
    let extra = res.as_ref().ok().and_then(|f| f.error()).map_or(0, |e| e.extra); // STALE_CHAIN：后继的 chain_ver
    let rep = res.and_then(check_reply).and_then(|f| decode_body::<PeerReply>(&f.body));
    if matches!(rep, Ok(PeerReply::Ack { .. })) {
        forward::note_ok(node, id.chain, succ);
    }
    // 只认当前后继的回复：后继换过（规则 4）之后旧后继迟到的确认不代表新后继有了这个版本（I3）；pending 已经没了（重复的确认：
    // 重新驱动过）也什么都不做
    if node.chunks.with_pending(id, Some(version), |p| p.successor == Some(succ)) != Some(true) {
        return;
    }
    match rep {
        Ok(PeerReply::Ack { version: v }) if v >= version => {
            if let Some(p) = node.chunks.take_pending(id, Some(version)) {
                write::drain(node, forward::settle(node, id, p));
            }
        }
        Ok(PeerReply::NeedFull) => {
            node.craq.need_full.inc();
            forward::resend_full(node, id, succ);
        }
        Err(e @ (ErrCode::STALE_CHAIN | ErrCode::FENCED | ErrCode::NOT_READY)) => {
            if e == ErrCode::STALE_CHAIN {
                node.map.poke(); // 链刚变：两边先后拿到新集群图（见文件头）
            }
            let (map, soon) = (node.map.get(), clock::mono_ms().saturating_sub(redrive::REDRIVE_MS - redrive::TICK_MS));
            let fast = node.chunks.with_pending(id, Some(version), |p| {
                let here_ok = map.as_ref().is_some_and(|m| chain::check_i5(node, m, p.target).is_ok());
                let behind = here_ok && (e != ErrCode::STALE_CHAIN || extra < p.chain_ver); // 后继落后（见文件头）
                if behind {
                    p.last_sent = p.last_sent.min(soon);
                }
                behind
            });
            if fast == Some(false) {
                retry_upstream(node, id, version);
            }
        }
        other => {
            let e = other.err().unwrap_or(ErrCode::INTERNAL);
            if matches!(e, ErrCode::ECONNRESET | ErrCode::ETIMEDOUT) {
                forward::note_fail(node, id.chain, succ);
            }
            log::debug(
                "data",
                format_args!("{id:?} v{version} 转发给 target {succ} 失败：{e}（保留 pending，等前滚）"),
            );
            retry_upstream(node, id, version);
        }
    }
}

/// 不回滚、保留 pending：结果未知，upstream 回 RETRY（写者用同样的内容重试，3.4.2.3 第 8 步）。
fn retry_upstream(node: &DataNode, id: &ChunkId, version: u64) {
    if let Some(Some(up)) = node.chunks.with_pending(id, Some(version), |p| p.upstream.take()) {
        up.fail(ErrCode::RETRY);
    }
}

/// 后继收到 ForwardSync / ForwardDeleteEpoch（3.4.2.9）：本地做完，再交给自己的后继；尾直接回复。
fn propagate(node: &Arc<DataNode>, mut op: PeerOp, r: Responder) {
    let (chain_id, ver, from) = match &op {
        PeerOp::ForwardSync { chain, chain_ver, from_target, .. } => (*chain, *chain_ver, *from_target),
        PeerOp::ForwardDeleteEpoch { chain, chain_ver, from_target, .. } => (*chain, *chain_ver, *from_target),
        _ => return r.error(ErrCode::INTERNAL, 0),
    };
    let (t, succ) = match check(node, chain_id, ver, from) {
        Ok(h) => (h.target, h.succ),
        Err(chain::Fail(e, x)) => return r.error(e, x),
    };
    let Some(tg) = node.targets.get(&t) else { return r.error(ErrCode::NOT_SERVING, 0) };
    let local = match &mut op {
        PeerOp::ForwardSync { chunks, from_target, .. } => {
            *from_target = t;
            node.store(|| tg.store.sync(chunks)).map(|()| PeerReply::Ok)
        }
        PeerOp::ForwardDeleteEpoch { ino, epoch, from_target, .. } => {
            *from_target = t;
            drop_epoch(node, chain_id, *ino, *epoch);
            node.store(|| tg.store.delete_epoch(chain_id, *ino, *epoch)).map(|files| PeerReply::Deleted { files })
        }
        _ => Err(ErrCode::INTERNAL),
    };
    match (local, succ) {
        (Err(e), _) => r.error(e, 0),
        (Ok(rep), None) => r.reply(encode_body(&rep), vec![]),
        (Ok(rep), Some(s)) => forward::relay(node, op, s, r, encode_body(&rep)),
    }
}

/// DeleteEpoch：放弃这个 (ino, epoch) 在这条链上的 pending（3.4.2.9）。
pub fn drop_epoch(node: &Arc<DataNode>, chain_id: u32, ino: u64, epoch: u32) {
    for id in node.chunks.pendings(|c, _| (c.chain, c.ino, c.epoch) == (chain_id, ino, epoch)) {
        if let Some(p) = node.chunks.take_pending(&id, None) {
            write::drain(node, forward::abandon(node, &id, p));
        }
    }
}
