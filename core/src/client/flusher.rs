//! 写回用到的请求与后台写回（design.md 8.4、1.2）。文件的前半是写回路径发的几种请求，后半是后台线程做的事：
//! - **请求**：`WriteChunk`、fsync 的 `SyncChunks`（按链分组）、`CommitWrite`（带 inline 补丁时也更新本地的 inline 副本）、
//!   `Uninline`（ADR-0034）。数据请求都经 [`FileClient::with_cap`]（`BAD_CAP` → 续一次 cap 再试，再失败 → `EACCES`，8.7）。
//! - **后台**：4 个写线程（`flush-w-<i>`）各写一个写满了的 chunk 并为它提交一次（服务端取 max，提交可以乱序）；`flusher` 线程每秒
//!   一轮：脏数据超过 5 s 的、或挂载的写回槽位用了 3/4 以上时全部，写出去（正被别的线程提交、截断的 inode 这一轮跳过，不等它）；
//!   进程退出前全部写出。彻底失败时保留数据并记下错误，下一次 write/flush/fsync/close 报告（`writeback.rs` 的文件头）。

use std::sync::Arc;

use crossbeam_channel::Receiver;

use super::data_client::chunk_id;
use super::files::{FileClient, expect_open_out};
use super::inode::Inode;
use super::meta_client::Caller;
use super::session::{every, spawn};
use super::writeback::Flying;
use crate::base::{ErrCode, clock, lock, log};
use crate::buf::BufSlice;
use crate::meta::schema::records::F_INLINE;
use crate::proto::meta::{MetaOp, MetaReply};

/// 后台写线程数（1.2：flusher 1 + 4 个写线程）。
const WRITERS: usize = 4;
/// flusher 每隔多久看一轮（1.2 的 `wmfs mount` 线程表）。
const FLUSHER_EVERY_MS: u64 = 1000;
/// 脏数据多老就由 flusher 写出去（8.4）。
const MAX_AGE_MS: u64 = 5000;

/// 起后台写回线程（`FileClient::start` 调用）：`WRITERS` 个写线程从 `rx` 取写满了的 chunk，flusher 每秒一轮。只持有 Weak。
pub(super) fn start_threads(fc: &Arc<FileClient>, rx: Receiver<(u64, u32)>) {
    for i in 0..WRITERS {
        let (w, rx) = (Arc::downgrade(fc), rx.clone());
        spawn(&format!("flush-w-{i}"), move || {
            while let Ok((ino, idx)) = rx.recv() {
                match w.upgrade() {
                    Some(fc) => fc.write_job(ino, idx),
                    None => return,
                }
            }
        });
    }
    let w = Arc::downgrade(fc);
    spawn("flusher", move || every(&w, FLUSHER_EVERY_MS, |fc| fc.flusher_round()));
}

impl FileClient {
    /// 一个 chunk 的 WriteChunk（`sync = false`：落盘一律另发 SyncChunks，`writeback.rs` 的文件头），记进"还没 SyncChunks"。
    pub(super) fn write_chunk(
        &self,
        inode: &Inode,
        who: &Caller,
        idx: u32,
        extents: Vec<(u32, u32)>,
        slices: Vec<BufSlice>,
    ) -> Result<(), ErrCode> {
        let l = lock(&inode.st).layout.clone().ok_or(ErrCode::EINVAL)?;
        let chunk = chunk_id(&l, inode.ino, idx)?;
        self.with_cap(inode, who, true, |cap| self.data.write(cap, chunk, extents.clone(), slices.clone(), false))?;
        lock(&inode.st).unsynced.insert(idx);
        Ok(())
    }

    /// 对写过（或截断过）、还没落盘的 chunk 发 `SyncChunks`（按链分组）。
    pub(super) fn sync_chunks(&self, inode: &Inode, who: &Caller) -> Result<(), ErrCode> {
        let (l, idxs) = {
            let st = lock(&inode.st);
            (st.layout.clone().ok_or(ErrCode::EINVAL)?, st.unsynced.iter().copied().collect::<Vec<u32>>())
        };
        if idxs.is_empty() {
            return Ok(());
        }
        let all: Vec<_> = idxs.iter().filter_map(|i| chunk_id(&l, inode.ino, *i).ok()).collect();
        self.with_cap(inode, who, true, |cap| {
            for chain in &l.chains {
                let ids: Vec<_> = all.iter().filter(|c| c.chain == *chain).copied().collect();
                if !ids.is_empty() {
                    self.data.sync(cap, *chain, ids)?;
                }
            }
            Ok(())
        })?;
        let mut st = lock(&inode.st);
        for i in &idxs {
            st.unsynced.remove(i);
        }
        Ok(())
    }

    /// `CommitWrite`：提交长度（带 inline 补丁时也提交内容），用回复更新属性与本地的 inline 副本。
    pub(super) fn commit(
        &self,
        inode: &Inode,
        who: &Caller,
        new_size: u64,
        patches: Vec<(u64, Vec<u8>)>,
        close: bool,
    ) -> Result<(), ErrCode> {
        let epoch = lock(&inode.st).layout.as_ref().ok_or(ErrCode::EINVAL)?.epoch;
        let op = MetaOp::CommitWrite {
            ino: inode.ino,
            epoch,
            new_size,
            mtime: Default::default(), // 服务端用 leader 的时钟（放松 ⑫）
            inline_patches: patches.clone(),
            uninline_expect_change_id: None,
            want_quota: false,
            close,
        };
        let MetaReply::Committed { attr, .. } = self.meta.write(who, op)? else { return Err(ErrCode::INTERNAL) };
        let mut st = lock(&inode.st);
        st.patch_inline(&attr, &patches);
        st.mine(&attr);
        Ok(())
    }

    /// `Uninline`（ADR-0034）：请服务端把 inline 内容搬进 chunk，用回复更新 layout 与 cap（这是自己的修改）。
    pub(super) fn uninline(&self, inode: &Inode, who: &Caller) -> Result<(), ErrCode> {
        let o = expect_open_out(self.meta.read(who, MetaOp::Uninline { ino: inode.ino }))?;
        let mut st = lock(&inode.st);
        st.install(&o);
        st.mine(&o.attr);
        Ok(())
    }

    /// 后台写线程：写一个写满了的 chunk 并提交（见文件头）。
    fn write_job(&self, ino: u64, idx: u32) {
        let Some(inode) = self.inode(ino) else { return };
        let mut st = lock(&inode.st);
        let Some(cs) = st.layout.as_ref().map(|l| l.chunk_size) else { return };
        let full = st.dirty.chunks.get(&idx).is_some_and(|c| c.extents == [(0, cs)]);
        if !full || st.attr.flags & F_INLINE != 0 {
            return; // 已经被 flush 拿走了，或者（不会发生）还是 inline
        }
        let Some(c) = st.dirty.chunks.remove(&idx) else { return };
        let f = Flying { idx, data: c.buf.freeze(), extents: c.extents };
        st.dirty.flying.push(f.clone());
        st.dirty.busy += 1;
        let who = st.writer.clone().unwrap_or_else(|| self.process_caller());
        drop(st);
        let r = self.write_out(&inode, &who, std::slice::from_ref(&f), false, false);
        if let Err(e) = r {
            log::warn("client", format_args!("ino {ino} chunk {idx} 写回失败（保留数据，下次报告）：{e}"));
            lock(&inode.st).write_err = Some(e);
        }
        self.finish_flying(&inode, vec![f], r.is_ok());
        lock(&inode.st).dirty.busy -= 1;
        inode.cv.notify_all();
    }

    /// flusher 线程的一轮：脏数据超过 5 s 的、内存吃紧时全部，写出去；失败记成这个 inode 的写错误。别的线程正拿着 io 权（提交、
    /// 截断、同步写，要等网络）的 inode 这一轮跳过，下一轮再看。
    fn flusher_round(&self) {
        let now = clock::mono_ms();
        let pressed = self.slots.used() * 4 >= self.slots.max * 3;
        for inode in self.all_inodes() {
            let due = {
                let st = lock(&inode.st);
                !st.dirty.chunks.is_empty() && (pressed || now.saturating_sub(st.dirty.since_ms) >= MAX_AGE_MS)
            };
            if !due {
                continue;
            }
            let Some(io) = inode.try_io() else { continue };
            let r = self.flush_held(&inode, false, false);
            drop(io);
            if let Err(e) = r {
                log::warn("client", format_args!("ino {} 写回失败（保留数据，下次报告）：{e}", inode.ino));
                lock(&inode.st).write_err = Some(e);
            }
        }
    }

    /// 进程退出（SIGTERM、卸载）之前：把全部脏数据写出去。返回失败的 inode 数。
    pub fn flush_all(&self) -> usize {
        let mut bad = 0;
        for inode in self.all_inodes() {
            if !lock(&inode.st).dirty.is_empty() && self.flush_inode(&inode, false, false).is_err() {
                bad += 1;
            }
        }
        bad
    }

    /// 丢掉一个 inode 的全部脏数据（截断时丢 size 之后的另有 `discard_beyond`），返回丢了几个 chunk。
    pub(super) fn drop_dirty(&self, inode: &Inode) -> usize {
        let n = std::mem::take(&mut lock(&inode.st).dirty.chunks).len();
        self.slots.release(n);
        n
    }
}

/// inline 文件的一批脏数据 → CommitWrite 的补丁（文件内偏移，内容）。
pub(super) fn inline_patches(fly: &[Flying], cs: u64) -> Vec<(u64, Vec<u8>)> {
    let mut v = Vec::new();
    for f in fly {
        let d = f.data.as_slice();
        for (o, n) in &f.extents {
            v.push((f.idx as u64 * cs + *o as u64, d[*o as usize..(o + n) as usize].to_vec()));
        }
    }
    v
}
