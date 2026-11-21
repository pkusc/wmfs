//! 数据写去重表（design.md 3.4.2.3，ADR-0046）：记录每个 chunk 最近提交的 `(session, write_id) → version`。
//!
//! 客户端对同一个 `WriteChunk` / `TruncateChunk` 的重试带相同的 `write_id`（`DataClient` 每次调用生成一次、重试沿用），
//! 经 `ForwardWrite` 沿链复制到每个成员；任一成员本地提交该版本时登记。回复丢失或 head 宕机换主之后，客户端重试到旧 head
//! 或新 head，命中去重表直接回原版本号，不再作为新版本覆盖别的客户端之后完成的写，保证 `semantics.md` 2.3 "单个 chunk 内的读写线性一致"。
//!
//! 只保存在内存：按单调时钟保留 [`DEDUP_TTL_MS`]（5 分钟，大于默认 `--op-timeout` 120 s）且总条数不超过 [`DEDUP_MAX`]
//! （65 536 条），写入和查询都是 O(1)。`write_id == 0` 表示未启用去重（测试直接构造请求）。

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

use crate::base::types::ChunkId;
use crate::base::{clock, lock};

/// 去重记录保留时长：5 分钟（覆盖默认 `--op-timeout` 120 s 及其单调时钟偏差）。
const DEDUP_TTL_MS: u64 = 5 * 60 * 1000;
/// 每个数据节点最多保留的去重记录条数（约 4 MiB 上限）。
const DEDUP_MAX: usize = 65_536;

type Key = (ChunkId, u64, u64);

#[derive(Default)]
struct Inner {
    map: HashMap<Key, u64>,
    order: VecDeque<(u64, Key)>, // (登记时刻 mono_ms, key)，队首最旧
}

pub struct WriteDedup {
    ttl_ms: u64,
    max: usize,
    inner: Mutex<Inner>,
}

impl Default for WriteDedup {
    fn default() -> WriteDedup {
        WriteDedup::with_limits(DEDUP_TTL_MS, DEDUP_MAX)
    }
}

impl WriteDedup {
    pub fn with_limits(ttl_ms: u64, max: usize) -> WriteDedup {
        WriteDedup { ttl_ms, max: max.max(1), inner: Mutex::default() }
    }

    /// 查 `(chunk, dedup)` 是否已提交过；`dedup.1 == 0` 不查。
    pub fn get(&self, chunk: ChunkId, dedup: (u64, u64)) -> Option<u64> {
        if dedup.1 == 0 {
            return None;
        }
        lock(&self.inner).map.get(&(chunk, dedup.0, dedup.1)).copied()
    }

    /// 记下 `(chunk, dedup)` 已提交为 `version`；`dedup.1 == 0` 不记。先驱逐过期与超限的最旧条目。
    pub fn record(&self, chunk: ChunkId, dedup: (u64, u64), version: u64) {
        if dedup.1 == 0 {
            return;
        }
        let now = clock::mono_ms();
        let key = (chunk, dedup.0, dedup.1);
        let mut g = lock(&self.inner);
        while g.order.front().is_some_and(|(at, _)| now.saturating_sub(*at) >= self.ttl_ms) {
            if let Some((_, old)) = g.order.pop_front() {
                g.map.remove(&old);
            }
        }
        if g.map.contains_key(&key) {
            return;
        }
        while g.order.len() >= self.max {
            if let Some((_, old)) = g.order.pop_front() {
                g.map.remove(&old);
            }
        }
        g.map.insert(key, version);
        g.order.push_back((now, key));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cid(idx: u32) -> ChunkId {
        ChunkId { chain: 1, ino: 18, epoch: 1, idx }
    }

    #[test]
    fn dedup_remembers_committed_writes_and_evicts_oldest() {
        let d = WriteDedup::with_limits(60_000, 2);
        d.record(cid(0), (1, 0), 9);
        assert_eq!(d.get(cid(0), (1, 0)), None, "write_id = 0 不进表");
        d.record(cid(0), (1, 10), 1);
        d.record(cid(0), (1, 10), 2); // 重复登记保留原版本号
        assert_eq!(d.get(cid(0), (1, 10)), Some(1));
        assert_eq!(d.get(cid(0), (2, 10)), None, "不同会话不冲突");
        assert_eq!(d.get(cid(1), (1, 10)), None, "不同 chunk 不冲突");
        d.record(cid(0), (1, 11), 2);
        d.record(cid(0), (1, 12), 3); // 超过容量 2：挤掉最旧的 10
        assert_eq!(d.get(cid(0), (1, 10)), None);
        assert_eq!(d.get(cid(0), (1, 11)), Some(2));
        assert_eq!(d.get(cid(0), (1, 12)), Some(3));
    }

    #[test]
    fn dedup_evicts_entries_older_than_ttl() {
        let d = WriteDedup::with_limits(0, 8);
        d.record(cid(0), (1, 1), 1);
        d.record(cid(0), (1, 2), 2); // ttl = 0：插入新条目前先驱逐已过期的
        assert_eq!(d.get(cid(0), (1, 1)), None);
        assert_eq!(d.get(cid(0), (1, 2)), Some(2));
    }
}
