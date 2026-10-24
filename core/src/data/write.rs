//! head 上的写与截断（design.md 3.4.2.3、3.4.2.9、3.2.5 R1/R5）。
//!
//! 步骤（io 线程；排在后面的写由解决了上一个 pending 的线程接着做）：同一 chunk 的写排队串行（state.rs）→ 还是这条链的 head 吗
//! （排队期间链可能变了，不是就 `STALE_CHAIN`）→ 打开已提交版本（头坏了 → 标 Corrupt）→ 版本号 `V = max(committed + 1,
//! incarnation << 40)`（3.4.2.2；永不重置）→ 构建新版本（build.rs）→ `write_pending`（R1）→
//! - 没有后继（链长 1，或自己是尾）：`commit_pending`（rename；O_SYNC 时先 fdatasync）→ 回复 `Written{V}` → 做队里的下一个；
//! - 有后继：登记 pending（回复句柄交给它）、增量转发（forward.rs，`call_cb` 不占线程）后立即返回；确认由 repl 线程处理（第 6 步）。
//!
//! 还没转发给任何人时失败就删掉 pending 文件（已提交版本不受影响，这个版本号以后可以重新分配）；转发过的 pending 提交失败时留着重试
//! （forward.rs 的 `settle`：后继可能已经提交了这个版本）。截断一个从未写过的 chunk 什么也不做（回 version 0）：它读起来本来就是 0，链上别的成员
//! 也不会有它（head 上没有提交过、也没有 pending 的版本，别的 Serving 成员不可能提交过，I2）。新版本没有从旧版本原样复制任何块
//! （`copies` 为空）才清除 Corrupt 标记（3.2.5 R5）。target 的已用到了容量上限时写回 `ENOSPC`；截断照常（只会变小）。

use std::sync::Arc;

use super::build::{self, ChunkEdit, NewVersion};
use super::{DataNode, chain, forward, read};
use crate::base::types::ChunkId;
use crate::base::{ErrCode, log};
use crate::buf::Buf;
use crate::net::server::Responder;
use crate::proto::cap::CapV1;
use crate::proto::data::DataReply;
use crate::proto::encode_body;

/// 一个写类请求：写一些区间，或截断。
pub enum JobKind {
    Write { extents: Vec<(u32, u32)>, sync: bool },
    Truncate { new_len: u32 },
}

/// 排队等着写某个 chunk 的请求（已校验过 cap、链版本、本节点是 head）。
pub struct WriteJob {
    pub chunk: ChunkId,
    pub target: u32, // 本节点上存这个 chunk 的 target
    pub cap: CapV1,
    pub kind: JobKind,
    pub bulk: Option<Buf>,        // 写的数据（各 extent 依次拼接）
    pub reply: Option<Responder>, // 测试里直接调用时可以没有
}

/// 写好了 pending 文件的新版本。
pub struct Prepared {
    pub version: u64,
    pub base: u64, // 构建时的已提交版本（增量转发的 base_version；0 = chunk 原本不存在）
    pub sync: bool,
    pub clean: bool, // 新版本没有从旧版本原样复制任何块：提交之后可以清除 Corrupt 标记
    pub nv: NewVersion,
}

/// 在 io 线程上提交一个写：轮到它就做，做完接着做同一 chunk 排在后面的写（见文件头）。
pub fn submit(node: &Arc<DataNode>, job: Box<WriteJob>) {
    match node.chunks.begin(job) {
        Ok(j) => drain(node, j),
        Err(j) => respond(j, Err(ErrCode::BUSY)), // 这个 chunk 的等待队列满了
    }
}

/// 接着做已经轮到的写（`next` 已占住 chunk）；遇到转发出去的就停：pending 占着 chunk，确认它的线程再接着做。
pub fn drain(node: &Arc<DataNode>, mut next: Option<Box<WriteJob>>) {
    while let Some(job) = next {
        let id = job.chunk;
        if !step(node, job) {
            return;
        }
        next = node.chunks.finish(&id);
    }
}

/// 做一个写；返回 false = 已转发给后继（回复交给确认它的线程）。
fn step(node: &Arc<DataNode>, job: Box<WriteJob>) -> bool {
    match head_view(node, &job) {
        Err(e) => respond(job, Err(e)),
        Ok(None) => {
            let r = run(node, &job);
            respond(job, r);
        }
        Ok(Some((s, ver))) => match prepare(node, &job) {
            Ok(Some(p)) => {
                forward::first_send(node, job, p, s, ver);
                return false;
            }
            Ok(None) => respond(job, Ok(0)), // 截断一个从未写过的 chunk
            Err(e) => respond(job, Err(e)),
        },
    }
    true
}

/// 本节点在本地视图里还是这条链的 head 吗：是 → 后继（有的话）与链版本。组件测试里没有集群图视图之外的东西也照样可用。
fn head_view(node: &DataNode, job: &WriteJob) -> Result<Option<(u32, u64)>, ErrCode> {
    let map = node.map.get().ok_or(ErrCode::NOT_READY)?;
    let c = chain::find(&map, job.chunk.chain).ok_or(ErrCode::STALE_CHAIN)?;
    match chain::local(node, c) {
        Some((t, pos)) if t == job.target && pos == 0 && chain::serving_at(c, 0) => {
            Ok(chain::successor(c, pos).map(|s| (s, c.ver)))
        }
        _ => Err(ErrCode::STALE_CHAIN), // 排队期间链变了：客户端拉新集群图后重试
    }
}

fn respond(job: Box<WriteJob>, r: Result<u64, ErrCode>) {
    let Some(rr) = job.reply else { return };
    match r {
        Ok(version) => rr.reply(encode_body(&DataReply::Written { version }), vec![]),
        Err(e) => rr.error(e, 0),
    }
}

/// 没有后继时（链长 1，或自己是尾）的整个写：构建、写 pending、提交；返回新版本号。调用方已经占住这个 chunk。提交失败 → 删掉
/// pending 文件（它没有转发给任何人）。
pub fn run(node: &DataNode, job: &WriteJob) -> Result<u64, ErrCode> {
    let Some(p) = prepare(node, job)? else { return Ok(0) };
    let r = commit(node, &job.chunk, job.target, p.sync, p.clean);
    r.map(|()| p.version).inspect_err(|e| discard(node, job.target, &job.chunk, "提交", *e))
}

/// 删掉还没转发给任何人的 pending 文件并写日志（已提交版本不受影响）。
fn discard(node: &DataNode, target: u32, id: &ChunkId, what: &str, e: ErrCode) {
    if let Some(t) = node.targets.get(&target) {
        let _ = node.store(|| t.store.abort_pending(id));
    }
    log::warn("data", format_args!("{what} chunk {id:?} 失败：{e}（pending 已删除，已提交版本不受影响）"));
}

/// 构建新版本并写好 pending 文件（第 3–4 步）；截断一个从未写过的 chunk → None。
pub fn prepare(node: &DataNode, job: &WriteJob) -> Result<Option<Prepared>, ErrCode> {
    let id = &job.chunk;
    let t = node.targets.get(&job.target).ok_or(ErrCode::NOT_SERVING)?;
    if t.read_only {
        return Err(ErrCode::EROFS);
    }
    let (mut old, corrupt) = match read::open(node, &*t.store, id) {
        Ok(o) => (o, node.chunks.is_corrupt(id)),
        Err(ErrCode::CORRUPT) => {
            node.report_corrupt(t, id, ErrCode::CORRUPT);
            (None, true)
        }
        Err(e) => return Err(e),
    };
    let (edit, sync) = match &job.kind {
        JobKind::Truncate { .. } if old.is_none() && !corrupt => return Ok(None),
        JobKind::Truncate { new_len } => (ChunkEdit::Truncate { new_len: *new_len }, false),
        JobKind::Write { extents, sync } => {
            let st = node.store(|| t.stat())?;
            if st.capacity > 0 && st.used >= st.capacity {
                return Err(ErrCode::ENOSPC);
            }
            let data = job.bulk.as_ref().map_or(&[][..], |b| b.as_slice());
            (ChunkEdit::Write { extents, data }, *sync)
        }
    };
    let committed = old.as_ref().map_or(0, |o| o.header.chunk_version);
    let version = (committed + 1).max((incarnation(node, id.chain) as u64) << 40);
    let nv = match build::build(node, &job.cap, id, old.as_mut(), corrupt, version, edit) {
        Err(ErrCode::CHECKSUM_MISMATCH) => {
            node.report_corrupt(t, id, ErrCode::CHECKSUM_MISMATCH);
            return Err(ErrCode::EIO); // 要保留的旧块坏了：修复在 M11（scrub），现在诚实报错（R5）
        }
        r => r?,
    };
    drop(old); // 放下已提交版本的读句柄
    let writes = nv.writes();
    if let Err(e) = node.store(|| t.store.write_pending(id, &writes, &nv.copies)) {
        discard(node, job.target, id, "写 pending", e);
        return Err(e);
    }
    t.write_bytes.add(nv.bytes());
    Ok(Some(Prepared { version, base: committed, sync, clean: nv.copies.is_empty(), nv }))
}

/// 提交本地的 pending 版本（rename；`sync` 时先 fdatasync）；`clean` = 新版本没有从旧版本原样复制任何块（可以清除 Corrupt 标记）。
/// 失败时 pending 文件留着，由调用者决定：没转发过的删掉（[`run`]），转发过的留着重试（forward.rs 的 `settle`）。
pub fn commit(node: &DataNode, id: &ChunkId, target: u32, sync: bool, clean: bool) -> Result<(), ErrCode> {
    let t = node.targets.get(&target).ok_or(ErrCode::NOT_SERVING)?;
    node.store(|| t.store.commit_pending(id, sync))?;
    if clean {
        node.chunks.clear_corrupt(id); // 新版本里全是新编码的数据（见文件头）
    }
    Ok(())
}

/// 链的 incarnation（本节点的集群图视图里；没有就按 1）。
fn incarnation(node: &DataNode, chain: u32) -> u32 {
    let map = node.map.get();
    map.as_ref().and_then(|m| chain::find(m, chain)).map_or(1, |c| c.incarnation).max(1)
}
