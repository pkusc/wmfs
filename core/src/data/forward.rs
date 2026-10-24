//! 向后继发转发与 pending 的结局（design.md 3.4.2.3 第 5–8 步、3.4.2.4、3.4.2.6、3.4.2.9 的同步与删除传播）。
//!
//! 转发不占线程（1.2 规则 3、D9）：`RpcClient::call_cb` 发出、pending 登记在 chunk 状态表里、立即返回；回调（连接读线程或 rpc-timer
//! 上）只把结果 try_send 进 repl 队列，由 repl 线程处理（replica.rs）。单次转发的期限 3 s：到期、连接断开或后继回错误时**不回滚**、
//! 保留 pending，回复 upstream RETRY 并置 None，由 chain-timer 前滚（redrive.rs）。完整重发（NeedFull、重新驱动、后继变了）从
//! pending 文件（规则 2 之后是已提交文件）重新打包，缓冲取自复制 lane（3.6.1）；pending 文件是按路径一段段读的，打包完再核对一次
//! 还是同一个没变的 pending 才发（这期间它被确认、提交，又为下一个版本建了 pending 文件的话，读到的是两个版本拼起来的）。
//! [`settle`] 与 [`abandon`] 交回排在后面的写，由调用者接着做（链变化时要等换上新视图之后，redrive.rs）。
//!
//! fail point（3.11）：`chain.forward.drop`（这一次不发、也不回：等 chain-timer 3 s 后重新驱动）、`chain.forward.delay`（发之前
//! sleep）、`chain.head.crash_after_pending`（head 写完并登记 pending、转发之前 abort：重启后以 Syncing 回链，存活的成员前滚）。

use std::sync::Arc;
use std::time::Duration;

use wmfs_api::failpoint::{fail_point, fail_point_hit};

use super::packed::{self, ReadAt};
use super::replica::ReplJob;
use super::state::{PendingWrite, Upstream};
use super::write::{self, Prepared, WriteJob};
use super::{DataNode, chain, read, sync};
use crate::base::types::ChunkId;
use crate::base::{ErrCode, clock, lock, log};
use crate::buf::BufSlice;
use crate::net::client::{Family, check_reply};
use crate::net::server::Responder;
use crate::proto::encode_body;
use crate::proto::frame::{FrameHeader, FrameKind, InFrame, MSG_PEER_CALL, OutFrame};
use crate::proto::peer::{ForwardWrite, PeerOp};

/// 单次转发的期限（3.4.2.3 第 8 步的"3 s 无确认"）；同步与删除的传播一路到尾要 fsync，给 10 s。
pub const FWD_TIMEOUT: Duration = Duration::from_secs(3);
const RELAY_TIMEOUT: Duration = Duration::from_secs(10);

pub fn peer_frame(op: &PeerOp, bulk: Vec<BufSlice>) -> OutFrame {
    let hdr = FrameHeader::new(FrameKind::Request, MSG_PEER_CALL, 0, 0);
    OutFrame { hdr, body: encode_body(op), bulk, recv_into: None }
}

/// head 写完 pending 之后（write.rs）：登记 pending（客户端的回复句柄交给它），第一次转发（增量）给后继。
pub fn first_send(node: &Arc<DataNode>, job: Box<WriteJob>, p: Prepared, succ: u32, chain_ver: u64) {
    let id = job.chunk;
    let (header, blocks) = p.nv.forward_parts();
    let fw = ForwardWrite {
        chunk: id,
        chain_ver,
        from_target: job.target,
        base_version: p.base,
        version: p.version,
        full: false,
        header,
        blocks,
        sync: p.sync,
    };
    let pw = PendingWrite {
        version: p.version,
        target: job.target,
        upstream: job.reply.map(Upstream::Client),
        successor: Some(succ),
        chain_ver,
        last_sent: clock::mono_ms(),
        sync: p.sync,
        clean: p.clean,
        committed: false,
    };
    node.chunks.set_pending(&id, pw);
    if fail_point_hit("chain.head.crash_after_pending") {
        log::error(
            "data",
            format_args!("fail point chain.head.crash_after_pending：{id:?} v{} 写好 pending 后 abort", p.version),
        );
        std::process::abort();
    }
    send(node, fw, p.nv.into_bulk(), succ);
}

/// 发一次 ForwardWrite；结果（确认、NeedFull、错误、超时）由 repl 线程处理（replica.rs 的 `on_reply`）。
pub fn send(node: &DataNode, fw: ForwardWrite, bulk: Vec<BufSlice>, succ: u32) {
    let (chunk, version) = (fw.chunk, fw.version);
    let q = node.repl.get().cloned();
    let done = move |res: Result<InFrame, ErrCode>| {
        if let Some(q) = &q {
            let _ = q.try_send(ReplJob::Reply { chunk, version, succ, res }); // 队列满：丢掉，chain-timer 兜底
        }
    };
    if fail_point_hit("chain.forward.drop") {
        log::warn("data", format_args!("fail point chain.forward.drop：{chunk:?} v{version} 这次不发"));
        return;
    }
    let _ = fail_point("chain.forward.delay");
    let ep = node.map.get().and_then(|m| chain::endpoint(&m, succ));
    let (Some(rpc), Some(ep)) = (node.peer.get(), ep) else { return done(Err(ErrCode::NOT_SERVING)) };
    rpc.call_cb(&ep, Family::Data, peer_frame(&PeerOp::ForwardWrite(fw), bulk), FWD_TIMEOUT, Box::new(done));
}

/// 把 pending（规则 2 之后是已在本地提交的）版本完整地重发给 `succ`（NeedFull、重新驱动、后继变了）。失败只写日志：pending 留着，
/// chain-timer 3 s 后再重新驱动。
pub fn resend_full(node: &DataNode, id: &ChunkId, succ: u32) {
    match pack_and_send(node, id, succ) {
        Ok(()) | Err(ErrCode::ENOENT) => {} // pending 这期间已经确认、提交了
        Err(e) => log::warn("data", format_args!("{id:?} 完整重发给 target {succ} 失败：{e}（chain-timer 稍后重试）")),
    }
}

fn pack_and_send(node: &DataNode, id: &ChunkId, succ: u32) -> Result<(), ErrCode> {
    let map = node.map.get().ok_or(ErrCode::NOT_READY)?;
    let ver = chain::find(&map, id.chain).ok_or(ErrCode::STALE_CHAIN)?.ver;
    let now = clock::mono_ms();
    let sent = node.chunks.with_pending(id, None, |p| {
        (p.successor, p.chain_ver, p.last_sent) = (Some(succ), ver, now);
        (p.version, p.target, p.sync, p.committed)
    });
    let (version, t, sync, committed) = sent.ok_or(ErrCode::ENOENT)?;
    let store = &node.targets.get(&t).ok_or(ErrCode::NOT_SERVING)?.store;
    let (buf, h) = if committed {
        let mut rd = node.store(|| store.open_committed(id))?.ok_or(ErrCode::ENOENT)?;
        packed::pack(&node.pool, &mut |off, b| node.store(|| rd.read_at(off, b)))?
    } else {
        let r: ReadAt = &mut |off, b| node.store(|| store.read_pending_at(id, off, b));
        packed::pack(&node.pool, r)?
    };
    let same = node.chunks.with_pending(id, Some(version), |p| p.committed == committed) == Some(true);
    if h.chunk_version != version || !same {
        return Err(ErrCode::ENOENT); // 这期间 pending 已经被确认、提交了（见文件头）
    }
    let header = buf.as_slice()[..super::format::CHUNK_HEADER_LEN].to_vec();
    let fw = ForwardWrite {
        chunk: *id,
        chain_ver: ver,
        from_target: t,
        base_version: 0,
        version,
        full: true,
        header,
        blocks: vec![],
        sync,
    };
    send(node, fw, vec![buf.freeze().whole()], succ);
    Ok(())
}

/// 提交 pending 并回复 upstream（确认到了、自己成了尾）；返回排在后面的写（调用者接着做：`write::drain`）。调用者已 `take_pending`
/// （占着 busy）。先放下 chunk 再回复：前驱收到确认就可能马上转发下一个版本，不能撞上还没放下的 busy。Syncing 成员在本地提交了
/// 转发来的版本 → 记进本轮同步的 touched（3.4.2.8 注记 ②）。
/// 本地提交失败：pending 不删、版本号不让出——后继（或已被移出链的成员）可能已经提交了这个版本，重新分配同一个版本号会让同版本不同
/// 内容（I6、附录 C 的 C-8）；upstream 回 RETRY，pending 留着由 chain-timer 重新驱动、再试提交（rename 已成功、之后 fsync 目录失败
/// 的那一次：已提交的就是这个版本，算提交了）。
#[must_use]
pub fn settle(node: &Arc<DataNode>, id: &ChunkId, mut p: PendingWrite) -> Option<Box<WriteJob>> {
    if !p.committed
        && let Err(e) = write::commit(node, id, p.target, p.sync, p.clean)
        && node.targets.get(&p.target).and_then(|t| read::committed_version(node, t, id)) != Some(p.version)
    {
        log::warn(
            "data",
            format_args!("{id:?} v{} 本地提交失败：{e}（pending 留着，chain-timer 稍后重试）", p.version),
        );
        if let Some(up) = p.upstream.take() {
            up.fail(ErrCode::RETRY);
        }
        node.chunks.set_pending(id, p);
        return None;
    }
    sync::touch(node, id, p.target);
    let next = node.chunks.finish(id);
    if let Some(up) = p.upstream {
        up.ok(p.version);
    }
    next
}

/// 放弃 pending（自己被移出链或降为 Syncing、DeleteEpoch）：删 pending 文件、回复 upstream RETRY；返回排在后面的写（同 [`settle`]）。
#[must_use]
pub fn abandon(node: &Arc<DataNode>, id: &ChunkId, p: PendingWrite) -> Option<Box<WriteJob>> {
    if !p.committed
        && let Some(t) = node.targets.get(&p.target)
    {
        let _ = node.store(|| t.store.abort_pending(id));
    }
    let next = node.chunks.finish(id);
    if let Some(up) = p.upstream {
        up.fail(ErrCode::RETRY);
    }
    next
}

/// 把 ForwardSync / ForwardDeleteEpoch 交给后继；整条链的回复到了，就用 `ok` 回复 `up`（失败原样回错误码）。回调里只是 try_send 回复。
pub fn relay(node: &DataNode, op: PeerOp, succ: u32, up: Responder, ok: Vec<u8>) {
    let ep = node.map.get().and_then(|m| chain::endpoint(&m, succ));
    let (Some(rpc), Some(ep)) = (node.peer.get(), ep) else { return up.error(ErrCode::NOT_SERVING, 0) };
    let cb = move |res: Result<InFrame, ErrCode>| match res.and_then(check_reply) {
        Ok(_) => up.reply(ok, vec![]),
        Err(e) => up.error(e, 0),
    };
    rpc.call_cb(&ep, Family::Data, peer_frame(&op, vec![]), RELAY_TIMEOUT, Box::new(cb));
}

/// 转发到后继失败 / 成功（"持续 10 s 无法把转发送达后继"由心跳上报，3.4.2.10）。
pub fn note_fail(node: &DataNode, chain: u32, succ: u32) {
    lock(&node.fwd_fail).entry((chain, succ)).or_insert_with(clock::wall_ms);
}
pub fn note_ok(node: &DataNode, chain: u32, succ: u32) {
    lock(&node.fwd_fail).remove(&(chain, succ));
}
