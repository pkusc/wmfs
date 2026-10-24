//! chunk 状态表（design.md 3.4.2.2、3.2.5 R5）：64 路分片的 `Mutex<HashMap<ChunkId, Entry>>`，**只保护内存状态，不跨网络、不跨
//! 磁盘 IO 持有**（3.4.2.3 第 2 步）。
//!
//! - `busy`：有一个线程正在对这个 chunk 做磁盘上的事（构建并写 pending、提交、同步安装），很短。
//! - `pending`：已写好 pending 文件、等后继确认的版本（I1：最多一个，`version > committed`）；它占着这个 chunk 直到被确认（提交）或
//!   放弃。head 上 `busy` 或有 `pending` 时，后到的客户端写进等待队列（≤ 8 个，满了 `BUSY`）；pending 解决之后由解决它的线程
//!   （repl 线程、hb 线程或 chain-timer）接着做队首（[`ChunkTable::finish`]）。
//! - Corrupt 标记**只在内存**：读到头 CRC 或块校验和不符时标上；之后读一律 `CORRUPT`；成功提交一个没有从旧版本原样复制任何块的
//!   新版本之后清除（write.rs）。重启后由下一次读重新发现。
//! - 条目在"不忙、没有 pending、没有排队、没有 Corrupt 标记"时删除。
//!
//! 实现注记（M9）：pending 的 `upstream` 是回复句柄（head 上是客户端，后继上是前驱）；重新驱动时可被替换，回复过 RETRY 后置 None。
//! "脏"（[`ChunkTable::has_dirty`]）= 有**还没在本地提交**的 pending；3.4.2.6 规则 2 在本地提交了、还在等后面成员确认的 pending
//! 不算脏，但照样占着 chunk（[`ChunkTable::pendings`] 里有它）。

use std::collections::{HashMap, VecDeque};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::{Mutex, MutexGuard};

use crate::base::types::ChunkId;
use crate::base::{ErrCode, lock};
use crate::data::write::WriteJob;
use crate::net::server::Responder;
use crate::proto::data::DataReply;
use crate::proto::encode_body;
use crate::proto::peer::PeerReply;

const SHARDS: usize = 64;
/// 每个 chunk 的等待队列上限（3.4.2.3 第 2 步）。
pub const MAX_WAITERS: usize = 8;

/// pending 版本等着回复的那一方：head 上是客户端（成功回 `Written`），后继上是前驱（成功回 `Ack`）。
pub enum Upstream {
    Client(Responder),
    Peer(Responder),
}

impl Upstream {
    pub fn ok(self, version: u64) {
        match self {
            Upstream::Client(r) => r.reply(encode_body(&DataReply::Written { version }), vec![]),
            Upstream::Peer(r) => r.reply(encode_body(&PeerReply::Ack { version }), vec![]),
        }
    }
    pub fn fail(self, e: ErrCode) {
        self.responder().error(e, 0);
    }
    pub fn responder(self) -> Responder {
        match self {
            Upstream::Client(r) | Upstream::Peer(r) => r,
        }
    }
}

/// 已写好 pending 文件、等后继确认的版本（3.4.2.2 的 PendingWrite）。
pub struct PendingWrite {
    pub version: u64,
    pub target: u32, // 本节点上存这个 chunk 的 target
    pub upstream: Option<Upstream>,
    pub successor: Option<u32>, // 转发给了哪个 target（None = 自己是尾、还没转发）
    pub chain_ver: u64,         // 在哪个链版本下转发的
    pub last_sent: u64,         // mono_ms：chain-timer 据此重新驱动（3 s 未确认）
    pub sync: bool,             // O_SYNC：提交前 fdatasync
    pub clean: bool,            // 新版本没有从旧版本原样复制任何块：提交之后可以清除 Corrupt 标记
    pub committed: bool,        // 已在本地提交（3.4.2.6 规则 2：新的 last serving），还要推给后面的成员
}

#[derive(Default)]
struct Entry {
    busy: bool,
    pending: Option<PendingWrite>,
    waiters: VecDeque<Box<WriteJob>>,
    corrupt: bool,
}

impl Entry {
    fn idle(&self) -> bool {
        !self.busy && self.pending.is_none() && self.waiters.is_empty() && !self.corrupt
    }
}

pub struct ChunkTable {
    shards: Vec<Mutex<HashMap<ChunkId, Entry>>>,
}

impl Default for ChunkTable {
    fn default() -> ChunkTable {
        ChunkTable::new()
    }
}

impl ChunkTable {
    pub fn new() -> ChunkTable {
        ChunkTable { shards: (0..SHARDS).map(|_| Mutex::new(HashMap::new())).collect() }
    }

    fn shard(&self, id: &ChunkId) -> MutexGuard<'_, HashMap<ChunkId, Entry>> {
        let mut h = DefaultHasher::new();
        id.hash(&mut h);
        lock(&self.shards[h.finish() as usize % SHARDS])
    }

    /// 要写 `job.chunk`：没有别的写在做、也没有 pending → `Ok(Some(job))`，调用者现在就做；有 → 进等待队列，`Ok(None)`；
    /// 队列满 → `Err(job)`（调用者回 `BUSY`）。
    pub fn begin(&self, job: Box<WriteJob>) -> Result<Option<Box<WriteJob>>, Box<WriteJob>> {
        let mut m = self.shard(&job.chunk);
        let e = m.entry(job.chunk).or_default();
        if !e.busy && e.pending.is_none() {
            e.busy = true;
            return Ok(Some(job));
        }
        if e.waiters.len() >= MAX_WAITERS {
            return Err(job);
        }
        e.waiters.push_back(job);
        Ok(None)
    }

    /// 一次磁盘上的事做完（或 pending 解决了）：放下 `busy`；还有排队的写就把队首交给调用者接着做（`busy` 留给它）。
    pub fn finish(&self, id: &ChunkId) -> Option<Box<WriteJob>> {
        let mut m = self.shard(id);
        let e = m.get_mut(id)?;
        e.busy = false;
        let next = if e.pending.is_none() { e.waiters.pop_front() } else { None };
        e.busy = next.is_some();
        if e.idle() {
            m.remove(id);
        }
        next
    }

    /// 占住一个不忙的 chunk（后继处理转发、同步安装、链变化时的处理）：忙 → false。之后一定要 [`ChunkTable::finish`]。
    pub fn claim(&self, id: &ChunkId) -> bool {
        let mut m = self.shard(id);
        let e = m.entry(*id).or_default();
        !std::mem::replace(&mut e.busy, true)
    }

    /// 登记 pending（调用者正占着 `busy`：登记之后放下 `busy`，pending 继续占着这个 chunk）。
    pub fn set_pending(&self, id: &ChunkId, p: PendingWrite) {
        let mut m = self.shard(id);
        let e = m.entry(*id).or_default();
        (e.pending, e.busy) = (Some(p), false);
    }

    /// 把 [`take_held`](Self::take_held) 取出的 pending 放回去，`busy` 仍归调用者（之后照常 [`ChunkTable::finish`]）。
    pub fn put_held(&self, id: &ChunkId, p: PendingWrite) {
        self.shard(id).entry(*id).or_default().pending = Some(p);
    }

    /// 取出 pending（`version` 给了就只取这个版本的），同时占住 `busy`：调用者提交或放弃它之后 [`ChunkTable::finish`]。
    /// chunk 正忙（别的线程在做磁盘上的事）→ None。
    pub fn take_pending(&self, id: &ChunkId, version: Option<u64>) -> Option<PendingWrite> {
        let mut m = self.shard(id);
        let e = m.get_mut(id)?;
        if e.busy || !e.pending.as_ref().is_some_and(|p| version.is_none_or(|v| p.version == v)) {
            return None;
        }
        e.busy = true;
        e.pending.take()
    }

    /// 调用者正占着 `busy`（后继处理转发时）：取出 pending（有的话）。
    pub fn take_held(&self, id: &ChunkId) -> Option<PendingWrite> {
        self.shard(id).get_mut(id)?.pending.take()
    }

    /// 改一下 pending（`version` 给了就只改这个版本的）；返回 `f` 的结果（没有这样的 pending → None）。
    pub fn with_pending<T>(
        &self,
        id: &ChunkId,
        version: Option<u64>,
        f: impl FnOnce(&mut PendingWrite) -> T,
    ) -> Option<T> {
        let mut m = self.shard(id);
        let p = m.get_mut(id)?.pending.as_mut().filter(|p| version.is_none_or(|v| p.version == v))?;
        Some(f(p))
    }

    /// 这个 chunk 脏吗：有没有还没在本地提交的版本（3.4.2.5：脏，且本成员不是 last serving → `CHUNK_DIRTY`）。
    pub fn has_dirty(&self, id: &ChunkId) -> bool {
        self.shard(id).get(id).is_some_and(|e| e.pending.as_ref().is_some_and(|p| !p.committed))
    }

    /// 有 pending（含已在本地提交、还在等确认的）的全部 chunk（`pred` 过滤）：链变化时的处理与 chain-timer 用（pending 很少，扫一遍
    /// 64 个分片）。
    pub fn pendings(&self, pred: impl Fn(&ChunkId, &PendingWrite) -> bool) -> Vec<ChunkId> {
        let mut out = Vec::new();
        for s in &self.shards {
            out.extend(
                lock(s).iter().filter(|(id, e)| e.pending.as_ref().is_some_and(|p| pred(id, p))).map(|(id, _)| *id),
            );
        }
        out
    }

    pub fn mark_corrupt(&self, id: &ChunkId) {
        self.shard(id).entry(*id).or_default().corrupt = true;
    }

    /// 提交了一个不含任何旧块的新版本之后（write.rs、后继收到完整转发、同步安装）：去掉 Corrupt 标记。
    pub fn clear_corrupt(&self, id: &ChunkId) {
        if let Some(e) = self.shard(id).get_mut(id) {
            e.corrupt = false;
        }
    }

    pub fn is_corrupt(&self, id: &ChunkId) -> bool {
        self.shard(id).get(id).is_some_and(|e| e.corrupt)
    }
}

#[cfg(test)]
mod tests;
