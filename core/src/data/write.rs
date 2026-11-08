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
//!
//! 本地副本用不了时（3.2.5 R5，实现注记（M11））：头读不出（不知道旧版本号，也不知道旧块在哪）、或要保留的旧块坏了（部分写、
//! `new_len > 0` 的截断）——链上还有别的 Serving 成员、这个 chunk 也没被判定丢失 → 交给 repair 线程从它们修复（`repair::want_repair`）、
//! 回可重试的 `BUSY`，修好之后客户端的重试照常做；没有（单副本、其余成员都在 Syncing、或修复判定丢失）→ 要保留旧块的写回 `EIO`。
//! 不保留旧块的写（整 chunk 覆盖、截断到 0）在头读得出时照常做；头读不出时（ADR-0042 第 6 条）：链上只有自己（没有后继）→ 照常做，
//! 版本号按 `max(1, incarnation << 40)`（没有别的副本会与它冲突）；有后继却修不了 → `EIO`，不猜版本号——猜小了，后继按"已提交 ≥ V"
//! 回重复的确认而不落盘，确认过的写就丢了。

use std::sync::Arc;

use super::build::{self, ChunkEdit, NewVersion};
use super::{DataNode, chain, forward, read, repair};
use crate::base::types::ChunkId;
use crate::base::{ErrCode, log};
use crate::buf::Buf;
use crate::meta::cluster::chains::SERVING;
use crate::net::server::Responder;
use crate::obs::stages::Stage;
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
    mark(&job, Stage::Queue); // 等这个 chunk 上前一个写的时间（慢日志的各阶段耗时，ADR-0045 第 9 条）
    match head_view(node, &job) {
        Err(e) => respond(job, Err(e)),
        Ok(None) => {
            let r = run(node, &job);
            mark(&job, Stage::Disk);
            respond(job, r);
        }
        Ok(Some((s, ver))) => match prepare(node, &job) {
            Ok(Some(p)) => {
                mark(&job, Stage::Disk);
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

/// 在写者的回复句柄上记一个阶段（测试里直接调用时没有回复句柄）。
fn mark(job: &WriteJob, s: Stage) {
    if let Some(r) = &job.reply {
        r.mark(s);
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
    if t.broken() {
        return Err(ErrCode::NOT_SERVING); // 运行中坏了的盘：可重试，等链管理器把它移出写路径（M11）
    }
    if t.read_only() {
        return Err(ErrCode::EROFS);
    }
    let (mut old, unknown) = match read::open(node, &*t.store, id) {
        Ok(o) => (o, false),
        Err(ErrCode::CORRUPT) => {
            node.report_corrupt(t, id, ErrCode::CORRUPT);
            (None, true)
        }
        Err(e) => return Err(e),
    };
    // 头读不出（见文件头）：别的 Serving 成员那里修得好 → 先修；修不了而链上还有后继 → EIO；链上只有自己 → 照常写
    if unknown {
        match repair_first(node, job) {
            ErrCode::BUSY => return Err(ErrCode::BUSY),
            e if has_successor(node, job) => return Err(e),
            _ => {}
        }
    }
    let corrupt = unknown || node.chunks.is_corrupt(id);
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
    let committed = old.as_ref().map_or(0, |o| o.header.chunk_version); // 0 = chunk 原本不存在（或头读不出、链上只有自己）
    let version = version_for(committed, incarnation(node, id.chain));
    let nv = match build::build(node, &job.cap, id, old.as_mut(), corrupt, version, edit) {
        Err(ErrCode::CHECKSUM_MISMATCH) => {
            node.report_corrupt(t, id, ErrCode::CHECKSUM_MISMATCH);
            return Err(repair_first(node, job)); // 要保留的旧块坏了（见文件头）
        }
        Err(ErrCode::EIO) if corrupt => return Err(repair_first(node, job)), // R5：Corrupt 的 chunk 上要保留旧块
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

/// 本节点的视图里这条链上还有别的成员吗（head 后面的都是后继：Serving 或 Syncing）。
fn has_successor(node: &DataNode, job: &WriteJob) -> bool {
    let map = node.map.get();
    let c = map.as_ref().and_then(|m| chain::find(m, job.chunk.chain));
    c.is_some_and(|c| c.members.iter().any(|(m, _)| *m != job.target))
}

/// 本地这份用不了时（见文件头）：链上还有别的 Serving 成员、这个 chunk 没被判定丢失 → 交给 repair 线程，`BUSY`；否则 `EIO`。
fn repair_first(node: &DataNode, job: &WriteJob) -> ErrCode {
    let map = node.map.get();
    let c = map.as_ref().and_then(|m| chain::find(m, job.chunk.chain));
    let others = c.is_some_and(|c| c.members.iter().any(|(m, s)| *s == SERVING && *m != job.target));
    if others && !node.chunks.is_lost(&job.chunk) && repair::want_repair(node, job.target, &job.chunk) {
        ErrCode::BUSY
    } else {
        ErrCode::EIO
    }
}

/// 新版本号（3.4.2.2）：`max(已提交 + 1, incarnation << 40)`（`committed` = 0：chunk 原本不存在，或头读不出、链上只有自己）。
fn version_for(committed: u64, incarnation: u32) -> u64 {
    (committed + 1).max((incarnation as u64) << 40)
}

/// 提交本地的 pending 版本（rename；`sync` 时先 fdatasync）；`clean` = 新版本没有从旧版本原样复制任何块（可以清除 Corrupt 标记）。
/// 失败时 pending 文件留着，由调用者决定：没转发过的删掉（[`run`]），转发过的留着重试（forward.rs 的 `settle`）。
pub fn commit(node: &DataNode, id: &ChunkId, target: u32, sync: bool, clean: bool) -> Result<(), ErrCode> {
    let t = node.targets.get(&target).ok_or(ErrCode::NOT_SERVING)?;
    let r = node.store(|| t.store.commit_pending(id, sync));
    t.note_commit(r.is_ok()); // 连续失败 → 只读（M11，ADR-0040 第 10 条）
    r?;
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
