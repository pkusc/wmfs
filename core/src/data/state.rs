//! chunk 状态表（design.md 3.4.2.2 的单节点部分、3.2.5 R5）：64 路分片的 `Mutex<HashMap<ChunkId, Entry>>`，
//! **只保护内存状态，不跨磁盘 IO 持有**（3.4.2.3 第 2 步）。
//!
//! - 同一 chunk 的写（含截断）串行：一个 io 线程在做（"有 pending"）时，后到的写进这个 chunk 的等待队列（≤ 8 个，满了回 `BUSY`）；
//!   做完的那个 io 线程接着做队首（[`ChunkTable::finish`]）。所以任一时刻一个 chunk 最多一个 pending 版本（I1）。
//! - Corrupt 标记**只在内存**：读到头 CRC 或块校验和不符时标上；之后读一律 `CORRUPT`；成功提交一个没有从旧版本原样复制任何块的
//!   新版本之后清除（[`ChunkTable::clear_corrupt`]，write.rs；Corrupt 的 chunk 上只有不保留旧块的写能成功，见 build.rs）。
//!   重启后由下一次读重新发现。
//! - 条目在"没有在做的写、没有排队、也没有 Corrupt 标记"时删除。

use std::collections::{HashMap, VecDeque};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::{Mutex, MutexGuard};

use crate::base::lock;
use crate::base::types::ChunkId;
use crate::data::write::WriteJob;

const SHARDS: usize = 64;
/// 每个 chunk 的等待队列上限（3.4.2.3 第 2 步）。
pub const MAX_WAITERS: usize = 8;

#[derive(Default)]
struct Entry {
    busy: bool,                       // 有一个 io 线程正在写它（pending 版本还没提交）
    waiters: VecDeque<Box<WriteJob>>, // 排队的写
    corrupt: bool,
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

    /// 要写 `job.chunk`：没有别的写在做 → `Ok(Some(job))`，调用者现在就做；有 → 进等待队列，`Ok(None)`；
    /// 队列满 → `Err(job)`（调用者回 `BUSY`）。
    pub fn begin(&self, job: Box<WriteJob>) -> Result<Option<Box<WriteJob>>, Box<WriteJob>> {
        let mut m = self.shard(&job.chunk);
        let e = m.entry(job.chunk).or_default();
        if !e.busy {
            e.busy = true;
            return Ok(Some(job));
        }
        if e.waiters.len() >= MAX_WAITERS {
            return Err(job);
        }
        e.waiters.push_back(job);
        Ok(None)
    }

    /// 一次写做完：返回排在队首的下一个写（调用者接着做），没有就 None。
    pub fn finish(&self, id: &ChunkId) -> Option<Box<WriteJob>> {
        let mut m = self.shard(id);
        let e = m.get_mut(id)?;
        let next = e.waiters.pop_front();
        e.busy = next.is_some();
        if !e.busy && !e.corrupt {
            m.remove(id);
        }
        next
    }

    pub fn mark_corrupt(&self, id: &ChunkId) {
        self.shard(id).entry(*id).or_default().corrupt = true;
    }

    /// 提交了一个不含任何旧块的新版本之后（write.rs）：去掉 Corrupt 标记。
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
mod tests {
    use super::*;
    use crate::data::write::JobKind;
    use crate::proto::cap::CapV1;

    fn chunk(idx: u32) -> ChunkId {
        ChunkId { chain: 1, ino: 18, epoch: 1, idx }
    }

    fn job(idx: u32) -> Box<WriteJob> {
        let cap = CapV1 {
            ino: 18,
            epoch: 1,
            rights: 0,
            session: 0,
            tenant: 0,
            job_id: 0,
            chains: vec![1],
            class_id: 0,
            qos_id: 0,
            chunk_size: 0,
            block_size: 0,
            checksum: 0,
            compress: 0,
            compress_level: 0,
            encrypt: 0,
            encrypt_key: 0,
            expires_ms: 0,
        };
        let kind = JobKind::Truncate { new_len: idx };
        Box::new(WriteJob { chunk: chunk(idx % 2), target: 1, cap, kind, bulk: None, reply: None })
    }

    #[test]
    fn writes_to_one_chunk_run_one_at_a_time_and_the_queue_is_bounded() {
        let t = ChunkTable::new();
        assert!(matches!(t.begin(job(0)), Ok(Some(_))), "没有别的写在做：现在就做");
        for i in 1..=MAX_WAITERS as u32 {
            assert!(matches!(t.begin(job(2 * i)), Ok(None)), "第 {i} 个：进等待队列");
        }
        assert!(t.begin(job(100)).is_err(), "等待队列满了：调用者回 BUSY");
        assert!(matches!(t.begin(job(1)), Ok(Some(_))), "别的 chunk 不受影响");
        for i in 1..=MAX_WAITERS as u32 {
            let next = t.finish(&chunk(0)).expect("做完一个，接着做队首");
            assert!(matches!(next.kind, JobKind::Truncate { new_len } if new_len == 2 * i), "按到达的顺序");
        }
        assert!(t.finish(&chunk(0)).is_none(), "队列空了");
        assert!(matches!(t.begin(job(0)), Ok(Some(_))), "之后又可以直接做");
    }

    #[test]
    fn corrupt_marks_stay_until_cleared() {
        let t = ChunkTable::new();
        t.mark_corrupt(&chunk(0));
        assert!(t.is_corrupt(&chunk(0)) && !t.is_corrupt(&chunk(1)));
        assert!(matches!(t.begin(job(0)), Ok(Some(_))));
        assert!(t.finish(&chunk(0)).is_none());
        assert!(t.is_corrupt(&chunk(0)), "一次写做完不会自动清除标记");
        t.clear_corrupt(&chunk(0));
        assert!(!t.is_corrupt(&chunk(0)));
    }
}
