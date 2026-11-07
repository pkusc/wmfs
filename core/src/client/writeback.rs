//! 写回缓冲（design.md 8.4）：`write()` 把数据拷进该 chunk 的缓冲（内存池 Buf，按 chunk 大小选 S/M/L 档）并合并 extent，
//! 马上返回；之后由下面几种时机一次写出去：某个 chunk 写满了 → 交给 4 个后台写线程（同一个 chunk 同一时刻只有一个在途）；
//! 脏数据超过 5 s、内存吃紧 → flusher 线程；写句柄的每次 close（FUSE flush）、fsync、截断之前、进程退出。
//!
//! - **按槽位计**（3.6.1，计数器在 `slots.rs`）：一个挂载的写回缓冲最多占 M+L 档槽位的 60%，一个 inode 最多 16 个槽位；超过了
//!   `write()` 先把自己这个 inode 写出去（或等别人腾出槽位）——背压，剩下的槽位留给读路径。等别人腾槽位最多等 `--op-timeout`，
//!   超过 → `EIO`（3.3.8；例如槽位被一个一直写失败的 inode 的脏数据占着）。
//! - **写出去**：每个脏 chunk 一个 `WriteChunk`（bulk = 同一个冻结 Buf 的几段切片，不拷贝）→ 要落盘时 `SyncChunks` →
//!   `CommitWrite{new_size = 写到的最大偏移, close}`；**CommitWrite 成功之后**才放掉这些数据（之前它们留在"在途"表里，读自己的写
//!   照样看得见）。fail point `client.flush.before_commit` 埋在写完数据、提交之前（3.11）。
//! - **inline**：文件还是 inline、写完也不超过 `inline_max` → 一个带 inline 补丁的 CommitWrite；会超过 → 先 `Uninline`（ADR-0034，
//!   服务端把旧内容搬进 chunk；在 `write()` 越过阈值时就做，所以后台写线程只见到 chunk 文件），再按 chunk 写。
//! - `STALE_LAYOUT` → 重新 Open 取新 layout，把仍拿在手里的数据按新 layout 重写，再提交（C4，最多 3 次）。
//! - **失败**：可重试的错误由 DataClient/MetaClient 在 `--op-timeout` 内重试；彻底失败时**保留脏数据并记下错误**，下一次
//!   write/flush/fsync/close 报告这个错误（不会悄悄丢弃）。只读句柄的 close 不写出、也不报告写错误（那是写者的错误，留给写者）。
//! - **会话被驱逐**（M10 审查，3.4.4）：每块脏数据记着写进它的句柄的会话，写出去时的请求只在这个会话里发（`MetaClient::read_in`）；
//!   会话换过了 → `SESSION_EVICTED`，这批数据丢掉（不放回脏区、不记成 inode 的写错误——旧句柄自己报 `EIO`），不会在新会话里写出、
//!   提交。经旧句柄的 write 不再写进脏区（核对会话与写进去在同一把锁里）。ADR-0041 第 9 条：新 chunk 也是先核对、再插入——以前
//!   先插入一个属于旧会话的空 chunk 再回错，新会话随后写进它的数据被当成旧会话的整批丢掉，close 却成功；往已有的 chunk 里写时它
//!   属于别的会话（不该有）就当作作废的，清空了按这个会话重写。
//! - **落盘只有一种做法**（`O_SYNC`/`O_DSYNC` 与 fsync）：`WriteChunk` 一律 `sync = false`，写过（或截断过）的 chunk 记进"还没
//!   SyncChunks"的集合，要落盘时对它们发 `SyncChunks`（链上每个成员 fdatasync），再提交长度。`O_SYNC` 打开的句柄每次 write 都这样
//!   做完才返回，`O_DIRECT` 的同步写（`strong.rs`）也一样。实现注记（M6）：8.4 原文写的是"WriteChunk 带 sync: O_SYNC?"，可写回
//!   缓冲里的数据要等 close 才发出去，所以统一成"写完再 SyncChunks"。

use std::collections::BTreeMap;

use wmfs_api::failpoint::fail_point;

use super::files::{FileClient, Handle, Reopen};
use super::flusher::inline_patches;
use super::inode::Inode;
use super::meta_client::Caller;
use super::{CLIENT_POOL_WAIT, STALE_RETRIES};
use crate::base::{ErrCode, clock, lock, log};
use crate::buf::{Buf, FrozenBuf, Lane};

/// 每个 inode 最多占几个写回槽位。
const PER_INODE: usize = 16;

/// 一个 chunk 的脏数据：chunk 大小的缓冲（数据放在 chunk 内的偏移处）+ 写过的区间（升序、不重叠）。
pub struct DirtyChunk {
    pub buf: Buf,
    pub extents: Vec<(u32, u32)>,
    pub session: u64, // 写进它的句柄的会话（见文件头）
}

/// 正在写出去、还没提交的数据（读自己的写时也要看它）。拷一份只是多一个引用（`FrozenBuf` 共享同一块内存）。
#[derive(Clone)]
pub struct Flying {
    pub idx: u32,
    pub data: FrozenBuf,
    pub extents: Vec<(u32, u32)>,
    pub session: u64, // 这些数据所属的会话：请求只在它里面发；它被驱逐了 → 丢掉，不放回脏区（见文件头）
}

/// 一个 inode 的写回缓冲。
#[derive(Default)]
pub struct DirtyFile {
    pub chunks: BTreeMap<u32, DirtyChunk>,
    pub flying: Vec<Flying>,
    pub busy: u32,     // 后台写线程手上正在写的 chunk 数（flush 要等它们写完）
    pub since_ms: u64, // 最早一笔还没写出去的写（单调时钟；0 = 没有）
}

impl DirtyFile {
    pub fn is_empty(&self) -> bool {
        self.chunks.is_empty() && self.flying.is_empty()
    }

    fn slots(&self) -> usize {
        self.chunks.len() + self.flying.len()
    }

    /// 本地写到的末尾（文件内偏移，没有脏数据 → 0）。
    pub fn local_end(&self, cs: u32) -> u64 {
        let end = |idx: u32, e: &[(u32, u32)]| e.last().map_or(0, |(o, n)| idx as u64 * cs as u64 + (*o + *n) as u64);
        let a = self.chunks.iter().map(|(i, c)| end(*i, &c.extents)).max().unwrap_or(0);
        a.max(self.flying.iter().map(|f| end(f.idx, &f.extents)).max().unwrap_or(0))
    }

    /// 把本地还没提交的数据盖到 `out`（文件内 `[off, off + out.len())`）上：先在途、后脏区（后写的盖先写的）；`mask` 标出哪些字节
    /// 有本地数据。
    pub fn overlay(&self, cs: u32, off: u64, out: &mut [u8], mask: &mut [bool]) {
        for f in &self.flying {
            copy_extents(f.idx, cs, &f.extents, f.data.as_slice(), off, out, mask);
        }
        for (i, c) in &self.chunks {
            copy_extents(*i, cs, &c.extents, c.buf.as_slice(), off, out, mask);
        }
    }
}

fn copy_extents(idx: u32, cs: u32, ext: &[(u32, u32)], data: &[u8], off: u64, out: &mut [u8], mask: &mut [bool]) {
    let base = idx as u64 * cs as u64;
    for (o, n) in ext {
        let (s, e) = ((base + *o as u64).max(off), (base + (*o + *n) as u64).min(off + out.len() as u64));
        if s < e {
            let r = (s - off) as usize..(e - off) as usize;
            out[r.clone()].copy_from_slice(&data[(s - base) as usize..(e - base) as usize]);
            mask[r].fill(true);
        }
    }
}

/// 把区间 [o, o + n) 并进升序、不重叠的区间表（相邻的也合并）。
pub fn merge_extent(v: &mut Vec<(u32, u32)>, o: u32, n: u32) {
    let (mut s, mut e) = (o, o + n);
    v.retain(|(a, l)| {
        let keep = a + l < s || *a > e;
        if !keep {
            (s, e) = (s.min(*a), e.max(a + l));
        }
        keep
    });
    let at = v.partition_point(|(a, _)| *a < s);
    v.insert(at, (s, e - s));
}

impl FileClient {
    /// 写回缓冲里的一次 write（见文件头）。返回写进去的字节数；之前记下的写错误在这里报告。
    pub fn write(&self, who: &Caller, h: &Handle, off: u64, data: &[u8]) -> Result<u32, ErrCode> {
        let inode = self.inode(h.ino).ok_or(ErrCode::EBADF)?;
        let end = off.checked_add(data.len() as u64).ok_or(ErrCode::EFBIG)?;
        if h.direct {
            if !h.strong_append() {
                self.ensure_quota(&inode, who, h.session, off, end)?; // strong 的追加：AppendReserve 自己精确检查配额（M12）
            }
            return self.write_direct(&inode, who, h, off, data);
        }
        self.uninline_if_past(&inode, who, h.session, end)?;
        self.ensure_quota(&inode, who, h.session, off, end)?; // 越过配额额度之前先要额度（M10，3.9）
        let mut pos = 0usize;
        while pos < data.len() {
            pos += self.write_piece(&inode, who, h.session, off + pos as u64, &data[pos..])?;
        }
        if h.sync {
            self.flush_inode(&inode, false, true)?; // O_SYNC / O_DSYNC：write 返回时数据已在全部副本落盘、长度已提交
        }
        Ok(data.len() as u32)
    }

    /// inline 文件这次会写过 `inline_max` → 先请服务端把它转成 chunk（ADR-0034）。
    fn uninline_if_past(&self, inode: &Inode, who: &Caller, sid: u64, end: u64) -> Result<(), ErrCode> {
        let past = lock(&inode.st).layout_view().is_ok_and(|(inline, imax, _)| inline && end > imax);
        if past { self.uninline(inode, who, sid) } else { Ok(()) }
    }

    /// 经会话 `sid` 里打开的句柄，写进一个 chunk 里的那一段（不跨 chunk），返回写了多少字节。
    fn write_piece(&self, inode: &Inode, who: &Caller, sid: u64, off: u64, data: &[u8]) -> Result<usize, ErrCode> {
        let deadline = clock::mono_ms().saturating_add(self.meta.op_timeout().as_millis() as u64);
        loop {
            let mut st = lock(&inode.st);
            if let Some(e) = st.write_err.take() {
                return Err(e);
            }
            let cs = st.layout.as_ref().ok_or(ErrCode::EINVAL)?.chunk_size;
            let idx = u32::try_from(off / cs as u64).map_err(|_| ErrCode::EFBIG)?;
            let at = (off % cs as u64) as u32;
            let n = (cs - at).min(data.len() as u32);
            if !st.dirty.chunks.contains_key(&idx) {
                // 要一个新槽位：这个 inode 占满了 → 先把它写出去；整个挂载占满了 → 等别人腾（flusher 也在写）
                if st.dirty.slots() >= PER_INODE || !self.slots.try_take() {
                    let mine = !st.dirty.is_empty();
                    drop(st);
                    if mine {
                        self.flush_inode(inode, false, false)?;
                    } else if clock::mono_ms() >= deadline {
                        log::warn("client", format_args!("ino {}：等写回槽位超过 --op-timeout，回 EIO", inode.ino));
                        return Err(ErrCode::EIO);
                    } else {
                        self.slots.wait(200);
                    }
                    continue;
                }
                drop(st);
                let buf = match self.data.pool().get(cs as usize, Lane::Normal, CLIENT_POOL_WAIT) {
                    Ok(b) => b,
                    Err(e) => {
                        self.slots.release(1);
                        return Err(e);
                    }
                };
                st = lock(&inode.st);
                if sid == self.meta.session() && !st.dirty.chunks.contains_key(&idx) {
                    st.dirty.chunks.insert(idx, DirtyChunk { buf, extents: vec![], session: sid });
                } else {
                    drop(buf); // 别的线程先建好了这个 chunk；或会话换了：先核对、再插入，不留下旧会话的空 chunk
                    self.slots.release(1);
                }
            }
            if sid != self.meta.session() {
                return Err(ErrCode::SESSION_EVICTED); // 句柄属于已被驱逐的会话：不写进脏区（见文件头）
            }
            let c = st.dirty.chunks.get_mut(&idx).ok_or(ErrCode::INTERNAL)?;
            if c.session != sid {
                (c.extents, c.session) = (Vec::new(), sid); // 旧会话留下的 chunk（不该有）：作废，按这个会话重写
            }
            c.buf.as_mut_slice()[at as usize..(at + n) as usize].copy_from_slice(&data[..n as usize]);
            merge_extent(&mut c.extents, at, n);
            let full = c.extents == [(0, cs)];
            if st.dirty.since_ms == 0 {
                st.dirty.since_ms = clock::mono_ms();
            }
            st.writer = Some(who.clone());
            drop(st);
            if full {
                let _ = self.jobs.send((inode.ino, idx)); // 写满了：交给后台写线程
            }
            return Ok(n as usize);
        }
    }

    /// FUSE flush（每次 close）：写句柄 → 把这个 inode 的脏数据写出去并提交（cto），之前记下的写错误在这里报告；只读句柄什么
    /// 也不做（不替写者写出、也不取走写者的错误）。
    pub fn flush(&self, fh: u64) -> Result<(), ErrCode> {
        let h = self.handle(fh)?;
        if h.write { self.flush_handle(&h, true, false) } else { Ok(()) }
    }

    /// fsync：写出去、链上每个成员 fdatasync（`SyncChunks`）、提交长度（3.1.3 第 6 条）。
    pub fn fsync(&self, fh: u64) -> Result<(), ErrCode> {
        self.flush_handle(&self.handle(fh)?, false, true)
    }

    fn flush_handle(&self, h: &Handle, close: bool, sync: bool) -> Result<(), ErrCode> {
        match self.inode(h.ino) {
            Some(i) => self.flush_inode(&i, close, sync),
            None => Ok(()),
        }
    }

    /// 拿着 io 权把这个 inode 的脏数据写出去（见 [`flush_held`](Self::flush_held)）。
    pub(super) fn flush_inode(&self, inode: &Inode, close: bool, sync: bool) -> Result<(), ErrCode> {
        let _io = inode.io();
        self.flush_held(inode, close, sync)
    }

    /// 把这个 inode 的脏数据写出去并提交（`sync`：再让它们落盘）；调用方已经拿着 io 权。之前记下的写错误在这里报告。
    pub(super) fn flush_held(&self, inode: &Inode, close: bool, sync: bool) -> Result<(), ErrCode> {
        let mut st = inode.wait_idle(); // 等后台写线程手上的 chunk（它们的错误也在这里报告）
        if let Some(e) = st.write_err.take() {
            return Err(e);
        }
        let taken: Vec<Flying> = std::mem::take(&mut st.dirty.chunks)
            .into_iter()
            .map(|(idx, c)| Flying { idx, data: c.buf.freeze(), extents: c.extents, session: c.session })
            .collect();
        let need_sync = sync && !st.unsynced.is_empty();
        if taken.is_empty() && !need_sync {
            return Ok(());
        }
        st.dirty.since_ms = 0;
        st.dirty.flying.extend(taken.iter().cloned());
        let who = st.writer.clone().unwrap_or_else(|| self.process_caller());
        drop(st);
        let r = self.write_out(inode, &who, &taken, close, sync);
        self.finish_flying(inode, taken, r.is_ok());
        r
    }

    /// 写出去的一批数据有了结果：从在途表里摘掉；成功 → 放掉槽位；失败 → 放回脏区（与这期间的新写合并，新写盖旧写）——
    /// 除非它属于已被驱逐的会话（M10，见文件头）。
    pub(super) fn finish_flying(&self, inode: &Inode, done: Vec<Flying>, ok: bool) {
        let n = done.len();
        let mut st = lock(&inode.st);
        st.dirty.flying.retain(|f| !done.iter().any(|d| d.data.addr() == f.data.addr()));
        if ok || done.iter().any(|f| f.session != self.meta.session()) {
            drop(st); // 成功；或属于已被驱逐的会话：没提交的写丢失（3.4.4），不放回脏区
            self.slots.release(n);
            return;
        }
        let mut freed = 0;
        for f in done {
            let Some(mut buf) = self.unfreeze(f.data) else {
                log::error("client", format_args!("ino {} chunk {}：取不到缓冲，丢掉写失败的数据", inode.ino, f.idx));
                freed += 1;
                continue;
            };
            let mut ext = f.extents;
            if let Some(newer) = st.dirty.chunks.remove(&f.idx) {
                for (o, l) in &newer.extents {
                    let r = *o as usize..(*o + *l) as usize;
                    buf.as_mut_slice()[r.clone()].copy_from_slice(&newer.buf.as_slice()[r]);
                    merge_extent(&mut ext, *o, *l);
                }
                freed += 1;
            }
            st.dirty.chunks.insert(f.idx, DirtyChunk { buf, extents: ext, session: f.session });
        }
        if st.dirty.since_ms == 0 {
            st.dirty.since_ms = clock::mono_ms();
        }
        drop(st);
        self.slots.release(freed);
    }

    /// 冻结的缓冲变回可写的：一般直接拿回（在途表与 RPC 都已放手）；万一还有人拿着（例如超时的请求帧还在发送队列里）就拷一份。
    fn unfreeze(&self, f: FrozenBuf) -> Option<Buf> {
        match f.try_unfreeze() {
            Ok(b) => Some(b),
            Err(f) => {
                let mut b = self.data.pool().get(f.as_slice().len(), Lane::Normal, CLIENT_POOL_WAIT).ok()?;
                b.as_mut_slice().copy_from_slice(f.as_slice());
                Some(b)
            }
        }
    }

    /// 把一批数据写进链并提交（见文件头；`STALE_LAYOUT` 时按新 layout 重写，最多 3 次），请求只在这批数据所属的会话里发。没有
    /// 数据 = 只是 fsync：让写过的 chunk 落盘就行，长度早已提交（inline 文件的内容已在元数据里持久），不再发 CommitWrite（它会改
    /// mtime 与 change_id）。
    pub(super) fn write_out(
        &self,
        inode: &Inode,
        who: &Caller,
        fly: &[Flying],
        close: bool,
        sync: bool,
    ) -> Result<(), ErrCode> {
        let sid = self.batch_session(fly);
        for _ in 0..STALE_RETRIES {
            let (inline, imax, cs) = lock(&inode.st).layout_view()?;
            let end = fly.iter().map(|f| f.idx as u64 * cs + f.extents.last().map_or(0, |(o, n)| (o + n) as u64));
            let end = end.max().unwrap_or(0);
            let r = if fly.is_empty() {
                if sync { self.sync_chunks(inode, who, sid) } else { Ok(()) }
            } else if inline && end <= imax {
                self.commit(inode, who, sid, end, inline_patches(fly, cs), close, false)
            } else {
                if inline {
                    self.uninline(inode, who, sid)?;
                }
                self.write_flying(inode, who, fly, sync, end, close)
            };
            match r {
                Err(ErrCode::STALE_LAYOUT) => self.stale(inode, who, sid, Reopen::Handle)?,
                r => return r,
            }
        }
        Err(ErrCode::ESTALE)
    }

    /// 一批数据所属的会话（同一批都一样：会话换过之后脏区里只有新会话的，见 `OpenFiles::evicted`）；只是 fsync（没有数据）→
    /// 现在的会话（换会话时"还没 SyncChunks"的记录一并清掉了）。
    fn batch_session(&self, fly: &[Flying]) -> u64 {
        fly.first().map_or_else(|| self.meta.session(), |f| f.session)
    }

    /// 按 chunk 写出去 →（要落盘时）SyncChunks → 提交 `new_size = end`。
    fn write_flying(
        &self,
        inode: &Inode,
        who: &Caller,
        fly: &[Flying],
        sync: bool,
        end: u64,
        close: bool,
    ) -> Result<(), ErrCode> {
        let sid = self.batch_session(fly);
        for f in fly {
            let slices = f.extents.iter().map(|(o, n)| f.data.slice(*o, *n)).collect();
            self.write_chunk(inode, who, sid, f.idx, f.extents.clone(), slices)?;
        }
        if sync {
            self.sync_chunks(inode, who, sid)?;
        }
        fail_point("client.flush.before_commit").map_err(ErrCode)?;
        self.commit(inode, who, sid, end, vec![], close, false)
    }
}

#[cfg(test)]
mod tests;
