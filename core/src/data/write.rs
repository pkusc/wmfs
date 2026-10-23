//! head 上的写与截断（design.md 3.4.2.3、3.4.2.9、3.2.5 R1/R5）。实现注记（M5）：单节点——链长 1，没有后继，写完 pending 就本地
//! 提交（第 5 步"无后继"一支）；转发、逆向确认、前滚在 M9。
//!
//! 步骤（都在 io 线程上）：同一 chunk 的写排队串行（state.rs）→ 打开已提交版本（头坏了 → 标 Corrupt）→ 版本号
//! `V = max(committed + 1, incarnation << 40)`（3.4.2.2；永不重置）→ 构建新版本（build.rs）→ `write_pending`（R1：写头、块表、
//! 改动块，用 `copy_file_range` 复制未改动块）→ `commit_pending`（rename；O_SYNC 时先 fdatasync）→ 回复 `Written{V}` → 做队里的下一个。
//! 失败时删掉 pending 文件（已提交版本不受影响）。截断一个从未写过的 chunk 什么也不做（回 version 0）：它读起来本来就是 0。
//! 新版本没有从旧版本原样复制任何块（`copies` 为空：全是新编码、校验过的数据）才清除 Corrupt 标记——原样复制过来的旧块没有校验过，
//! 可能正是读路径刚发现的坏块（3.2.5 R5）。
//! target 的已用到了容量上限（node.toml 的 `capacity`，防止测试写满系统盘）时写回 `ENOSPC`；截断照常（只会变小）。

use super::build::{self, ChunkEdit};
use super::{DataNode, read};
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

/// 在 io 线程上提交一个写：轮到它就做，做完接着做同一 chunk 排在后面的写（见文件头）。
pub fn submit(node: &DataNode, job: Box<WriteJob>) {
    let mut next = match node.chunks.begin(job) {
        Ok(j) => j,
        Err(j) => return respond(j, Err(ErrCode::BUSY)), // 这个 chunk 的等待队列满了
    };
    while let Some(job) = next {
        let r = run(node, &job);
        next = node.chunks.finish(&job.chunk);
        respond(job, r);
    }
}

fn respond(job: Box<WriteJob>, r: Result<u64, ErrCode>) {
    let Some(rr) = job.reply else { return };
    match r {
        Ok(version) => rr.reply(encode_body(&DataReply::Written { version }), vec![]),
        Err(e) => rr.error(e, 0),
    }
}

/// 写一个新版本（调用方已经独占这个 chunk）；返回新版本号。
pub fn run(node: &DataNode, job: &WriteJob) -> Result<u64, ErrCode> {
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
        JobKind::Truncate { .. } if old.is_none() && !corrupt => return Ok(0),
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
            return Err(ErrCode::EIO); // 要保留的旧块坏了：单副本修不了（R5）
        }
        r => r?,
    };
    drop(old); // 放下已提交版本的读句柄
    let writes = nv.writes();
    let done = node
        .store(|| t.store.write_pending(id, &writes, &nv.copies))
        .and_then(|()| node.store(|| t.store.commit_pending(id, sync)));
    if let Err(e) = done {
        let _ = node.store(|| t.store.abort_pending(id));
        log::warn("data", format_args!("写 chunk {id:?} 失败：{e}（pending 已删除，已提交版本不受影响）"));
        return Err(e);
    }
    if nv.copies.is_empty() {
        node.chunks.clear_corrupt(id); // 新版本里全是新编码的数据（见文件头）
    }
    t.write_bytes.add(nv.bytes());
    Ok(version)
}

/// 链的 incarnation（本节点的集群图视图里；没有就按 1）。
fn incarnation(node: &DataNode, chain: u32) -> u32 {
    let map = node.map.get();
    let rec = map.as_ref().and_then(|m| m.chains.iter().find(|(id, _)| *id == chain).map(|(_, c)| c.incarnation));
    rec.unwrap_or(1).max(1)
}
