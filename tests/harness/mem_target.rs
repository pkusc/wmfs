//! 测试用的内存版 TargetStore（design.md 9.3 的 `MemTargetStore`，实现 TargetStore 这个插件边界）：已提交版本的内容、rename、
//! 目录项都先进"易失层"，`commit_pending(sync = true)`、`install`、`sync` 才让它们落盘；[`MemTargetStore::crash`] 丢掉易失层——
//! 用来验证本体按 R1–R6 调用了 fdatasync 与目录 fsync（`kill -9` 不丢页缓存，真实磁盘上测不出来）。`p/` 里的 pending 文件掉电后
//! 还在（真实磁盘上没写完的 pending 文件也可能留下一部分），启动清理（`clear_temp`）必须把它们扔掉。
//! 另有制造损坏的工具：把已提交的文件截短（"rename 已持久、数据只落了一部分"，XFS 上掉电可能出现，3.2.5）、翻转任意比特，以及
//! 在 `p/` 里放一个写了一半的 pending 文件。删除（R4）不落盘：掉电后被删的版本可能"复活"。

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use wmfs_core::base::types::ChunkId;
use wmfs_core::base::{ErrCode, lock};
use wmfs_core::hooks::traits::{ChunkReader, StoreStat, TargetStore};

#[derive(Default)]
struct State {
    committed: BTreeMap<ChunkId, Arc<Vec<u8>>>, // 看得见的已提交版本（包括还没落盘的）
    durable: BTreeMap<ChunkId, Arc<Vec<u8>>>,   // 已落盘的已提交版本：掉电之后剩下的就是它
    pending: BTreeMap<ChunkId, Vec<u8>>,
}

#[derive(Clone, Default)]
pub struct MemTargetStore {
    st: Arc<Mutex<State>>,
}

/// 按 (偏移, 字节) 写进 `v`（不够长就补 0）。
fn put_at(v: &mut Vec<u8>, off: u64, d: &[u8]) {
    let end = off as usize + d.len();
    if v.len() < end {
        v.resize(end, 0);
    }
    v[off as usize..end].copy_from_slice(d);
}

impl MemTargetStore {
    pub fn new() -> MemTargetStore {
        MemTargetStore::default()
    }

    /// 掉电：已提交版本只留下已落盘的；`p/` 里的 pending 文件原样留下（等启动清理）。返回"重启之后"的存储。
    pub fn crash(&self) -> MemTargetStore {
        let st = lock(&self.st);
        let (durable, pending) = (st.durable.clone(), st.pending.clone());
        let after = State { committed: durable.clone(), durable, pending };
        MemTargetStore { st: Arc::new(Mutex::new(after)) }
    }

    /// 掉电，而且 `id` 的版本"rename 已持久、数据只落了前 `keep` 字节"（XFS 上可能出现，3.2.5）。
    pub fn crash_torn(&self, id: &ChunkId, keep: usize) -> MemTargetStore {
        let s = self.crash();
        let cur = lock(&self.st).committed.get(id).cloned();
        if let Some(cur) = cur {
            let torn = Arc::new(cur[..keep.min(cur.len())].to_vec());
            let mut st = lock(&s.st);
            st.committed.insert(*id, torn.clone());
            st.durable.insert(*id, torn);
        }
        s
    }

    /// 翻转 `id` 已提交版本的第 `bit` 个比特（易失层与已落盘的一起改，相当于盘坏了）。
    pub fn flip_bit(&self, id: &ChunkId, bit: usize) {
        let mut guard = lock(&self.st);
        let st = &mut *guard;
        for m in [&mut st.committed, &mut st.durable] {
            if let Some(v) = m.get_mut(id) {
                let mut d = (**v).clone();
                d[bit / 8] ^= 1 << (bit % 8);
                *v = Arc::new(d);
            }
        }
    }

    /// `id` 已提交版本的字节数（没有 → None）。
    pub fn committed_len(&self, id: &ChunkId) -> Option<usize> {
        lock(&self.st).committed.get(id).map(|v| v.len())
    }

    /// 已提交版本的原始字节（测试比对用）。
    pub fn committed_bytes(&self, id: &ChunkId) -> Option<Vec<u8>> {
        lock(&self.st).committed.get(id).map(|v| (**v).clone())
    }

    /// 在 `p/` 里放一个 pending 文件（例如写了一半的）。
    pub fn put_pending(&self, id: &ChunkId, bytes: Vec<u8>) {
        lock(&self.st).pending.insert(*id, bytes);
    }

    pub fn pending_count(&self) -> usize {
        lock(&self.st).pending.len()
    }
}

impl TargetStore for MemTargetStore {
    fn read_at(&self, id: &ChunkId, off: u64, buf: &mut [u8]) -> Result<Option<usize>, ErrCode> {
        Ok(self.open_committed(id)?.map(|mut r| r.read_at(off, buf).unwrap_or(0)))
    }

    fn open_committed(&self, id: &ChunkId) -> Result<Option<Box<dyn ChunkReader>>, ErrCode> {
        Ok(lock(&self.st).committed.get(id).map(|d| Box::new(Reader(d.clone())) as Box<dyn ChunkReader>))
    }

    fn write_pending(&self, id: &ChunkId, writes: &[(u64, &[u8])], copies: &[(u64, u64)]) -> Result<(), ErrCode> {
        let mut st = lock(&self.st);
        let mut v = Vec::new();
        if !copies.is_empty() {
            let src = st.committed.get(id).ok_or(ErrCode::EIO)?;
            for (off, len) in copies {
                let (a, z) = (*off as usize, (*off + *len) as usize);
                put_at(&mut v, *off, src.get(a..z).ok_or(ErrCode::CORRUPT)?);
            }
        }
        for (off, d) in writes {
            put_at(&mut v, *off, d);
        }
        st.pending.insert(*id, v);
        Ok(())
    }

    fn read_pending_at(&self, id: &ChunkId, off: u64, buf: &mut [u8]) -> Result<usize, ErrCode> {
        let st = lock(&self.st);
        let p = st.pending.get(id).ok_or(ErrCode::ENOENT)?;
        Ok(Reader(Arc::new(p.clone())).read_at(off, buf).unwrap_or(0))
    }

    fn commit_pending(&self, id: &ChunkId, sync: bool) -> Result<(), ErrCode> {
        let mut st = lock(&self.st);
        let v = Arc::new(st.pending.remove(id).ok_or(ErrCode::ENOENT)?);
        st.committed.insert(*id, v.clone());
        if sync {
            st.durable.insert(*id, v);
        }
        Ok(())
    }

    fn abort_pending(&self, id: &ChunkId) -> Result<(), ErrCode> {
        lock(&self.st).pending.remove(id);
        Ok(())
    }

    fn install(&self, id: &ChunkId, writes: &[(u64, &[u8])]) -> Result<(), ErrCode> {
        let mut v = Vec::new();
        for (off, d) in writes {
            put_at(&mut v, *off, d);
        }
        let mut st = lock(&self.st);
        let v = Arc::new(v);
        st.committed.insert(*id, v.clone());
        st.durable.insert(*id, v);
        Ok(())
    }

    fn sync(&self, ids: &[ChunkId]) -> Result<(), ErrCode> {
        let mut st = lock(&self.st);
        for id in ids {
            if let Some(v) = st.committed.get(id).cloned() {
                st.durable.insert(*id, v);
            }
        }
        Ok(())
    }

    fn delete_epoch(&self, chain: u32, ino: u64, epoch: u32) -> Result<u64, ErrCode> {
        let mut st = lock(&self.st);
        let n = st.committed.len();
        st.committed.retain(|c, _| (c.chain, c.ino, c.epoch) != (chain, ino, epoch));
        Ok((n - st.committed.len()) as u64)
    }

    fn delete_chunk(&self, id: &ChunkId) -> Result<(), ErrCode> {
        lock(&self.st).committed.remove(id);
        Ok(())
    }

    fn list_chain(&self, chain: u32, after: Option<&ChunkId>, limit: usize) -> Result<Vec<ChunkId>, ErrCode> {
        let st = lock(&self.st);
        let it = st.committed.keys().filter(|c| c.chain == chain && after.is_none_or(|a| *c > a));
        Ok(it.take(limit).copied().collect())
    }

    #[allow(clippy::type_complexity)]
    fn list_epochs(&self, after: Option<(u32, u64, u32)>, limit: usize) -> Result<Vec<(u32, u64, u32, u64)>, ErrCode> {
        let st = lock(&self.st);
        let mut v: Vec<(u32, u64, u32, u64)> = st.committed.keys().map(|c| (c.chain, c.ino, c.epoch, 0)).collect();
        v.dedup();
        Ok(v.into_iter().filter(|e| after.is_none_or(|a| (e.0, e.1, e.2) > a)).take(limit).collect())
    }

    fn delete_chain_dir(&self, chain: u32) -> Result<(), ErrCode> {
        lock(&self.st).committed.retain(|c, _| c.chain != chain);
        Ok(())
    }

    fn clear_temp(&self) -> Result<(), ErrCode> {
        lock(&self.st).pending.clear();
        Ok(())
    }

    fn statfs(&self) -> Result<StoreStat, ErrCode> {
        let used = lock(&self.st).committed.values().map(|v| v.len() as u64).sum();
        Ok(StoreStat { capacity: 0, used })
    }
}

/// 一个已提交版本的读句柄：打开时的那份内容（之后的提交不影响它）。
struct Reader(Arc<Vec<u8>>);

impl ChunkReader for Reader {
    fn read_at(&mut self, off: u64, buf: &mut [u8]) -> Result<usize, ErrCode> {
        let d = &self.0;
        if off as usize >= d.len() {
            return Ok(0);
        }
        let n = buf.len().min(d.len() - off as usize);
        buf[..n].copy_from_slice(&d[off as usize..off as usize + n]);
        Ok(n)
    }

    fn len(&self) -> u64 {
        self.0.len() as u64
    }
}
