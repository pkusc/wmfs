//! 同步写路径与截断（design.md 8.3、8.5、8.4 末尾、3.4.2.9）。
//!
//! - **同步写**（strong，以及 cto 下用 `O_DIRECT` 打开的文件，M6 先给后者用）：每次 write 先同步 `WriteChunk` 写链（`O_SYNC` 时再
//!   `SyncChunks`，与写回路径同一种做法，`writeback.rs` 的文件头），再同步 `CommitWrite` 才返回；`STALE_LAYOUT` → 重新 Open，用这次
//!   write 回调里仍拿着的数据按新 layout 重写（最多 3 次）。inline 文件写完不超过 `inline_max` → 一个带 inline 补丁的 CommitWrite；
//!   会超过 → 先 `Uninline`。同一个 inode 的同步写一个一个来（io 权），写之前先把这个 inode 写回缓冲里的数据写出去（保持先后顺序）。
//! - **截断**（`setattr(size)`，[`FileClient::set_size`]）：
//!   1. 按路径（没有写句柄：`truncate(2)`）先 `Open(O_WRONLY)`——照常按当前权限检查（ADR-0036 第 3 条；`EACCES` 原样返回，这时
//!      什么也没动），也拿到最新的长度、layout 与写 cap。经写句柄（ftruncate）用手上的 layout 与写 cap（不够了 `RenewCap`：属主
//!      不看权限位）。
//!   2. 丢弃脏区里 size 之后的数据、把其余的写出去。
//!   3. s > 0 且不是 inline → 对 `[⌊s/chunk⌋, ⌈旧长度/chunk⌉)` 的 chunk 发 `TruncateChunk`（[`truncate_chunks`]，`wmfs fs truncate`
//!      也用它）。旧长度 = max(线性一致的最新长度, 本挂载的长度, 丢弃之前本地写到的末尾)：别的挂载可能已经把文件写长了（按路径时用
//!      第 1 步 Open 回来的长度，经句柄时先 `GetAttr`），只按本挂载记着的长度算会漏掉后面的 chunk，之后扩长旧数据就会冒出来。
//!      截断过的 chunk 记进"还没 SyncChunks"的集合：之后的 fsync 让截断也落盘（3.1.3 第 6 条）。
//!   4. 经写句柄发 `TruncateHandle`（ADR-0036：属主不看权限位），按路径发 `SetAttr{size}`。`epoch_seen`：看到的是 inline（跳过了
//!      TruncateChunk）→ 0，否则 = 看到的 epoch（ADR-0035 第 9 条）；`STALE_LAYOUT` → 重新 Open、回到第 3 步（最多 3 次）。回复是
//!      `Opened(OpenOut)`，用它更新 InodeState（C4：截断到 0 之后的写直接进新 epoch）。
//!
//!   实现注记（M6）：内核对 `open(O_TRUNC)` 发的 SETATTR 不带 fh（6.17 的 `fuse_do_setattr` 遇到 ATTR_OPEN 把 file 置空），与按路径的
//!   truncate 分不出来，所以它发的是 `SetAttr`——刚做完的 Open 已经按当前权限检查过写权限，结果相同（ADR-0036 第 3 条的偏离，
//!   design.md 8.1 的注记）。

use super::data_client::{DataClient, chunk_id};
use super::files::{FileClient, Handle, Reopen, expect_open_out};
use super::inode::Inode;
use super::meta_client::Caller;
use super::{CLIENT_POOL_WAIT, STALE_RETRIES};
use crate::base::{ErrCode, lock};
use crate::buf::Lane;
use crate::meta::schema::records::LayoutV1;
use crate::proto::meta::{AttrOut, MetaOp, MetaReply, O_WRONLY, OpenOut, SetAttrIn};

impl FileClient {
    /// 同步写（见文件头）。
    pub(super) fn write_direct(
        &self,
        inode: &Inode,
        who: &Caller,
        h: &Handle,
        off: u64,
        data: &[u8],
    ) -> Result<u32, ErrCode> {
        let _io = inode.io();
        self.flush_held(inode, false, false)?;
        let end = off + data.len() as u64;
        for _ in 0..STALE_RETRIES {
            let (inline, imax, cs) = lock(&inode.st).layout_view()?;
            let r = if inline && end <= imax {
                self.commit(inode, who, end, vec![(off, data.to_vec())], false)
            } else {
                if inline {
                    self.uninline(inode, who)?;
                }
                self.write_through(inode, who, off, data, cs, h.sync)
            };
            match r {
                Err(ErrCode::STALE_LAYOUT) => self.stale(inode, who, Reopen::Handle)?,
                r => return r.map(|()| data.len() as u32),
            }
        }
        Err(ErrCode::ESTALE)
    }

    /// 按 chunk 把 `data` 写进链（每段拷进一个池缓冲）→（`O_SYNC` 时）SyncChunks → 提交长度。
    fn write_through(
        &self,
        inode: &Inode,
        who: &Caller,
        off: u64,
        data: &[u8],
        cs: u64,
        sync: bool,
    ) -> Result<(), ErrCode> {
        let mut pos = 0usize;
        while pos < data.len() {
            let at = off + pos as u64;
            let idx = u32::try_from(at / cs).map_err(|_| ErrCode::EFBIG)?;
            let n = ((cs - at % cs) as usize).min(data.len() - pos);
            let mut b = self.data.pool().get(n, Lane::Normal, CLIENT_POOL_WAIT)?;
            b.as_mut_slice().copy_from_slice(&data[pos..pos + n]);
            let b = b.freeze();
            self.write_chunk(inode, who, idx, vec![((at % cs) as u32, n as u32)], vec![b.whole()])?;
            pos += n;
        }
        if sync {
            self.sync_chunks(inode, who)?;
        }
        self.commit(inode, who, off + data.len() as u64, vec![], false)
    }

    /// `setattr(size)`（见文件头）。`fh` = 带着的句柄（是这个文件的写句柄时经句柄截断）。返回新属性。
    pub fn set_size(&self, who: &Caller, ino: u64, size: u64, fh: Option<u64>) -> Result<AttrOut, ErrCode> {
        let by_handle = fh.and_then(|f| self.handle(f).ok()).is_some_and(|h| h.write && h.ino == ino);
        let fresh = if by_handle {
            None
        } else {
            Some(expect_open_out(self.meta.read(who, MetaOp::Open { ino, flags: O_WRONLY }))?) // 第 1 步
        };
        let inode = match &fresh {
            Some(o) => {
                let i = self.inode_entry(o);
                lock(&i.st).install(o);
                i
            }
            None => self.inode(ino).ok_or(ErrCode::EBADF)?,
        };
        let _io = inode.io();
        let local_end = lock(&inode.st).local_end(); // 丢弃之前本地写到的末尾：这些 chunk 可能已经写进了链
        self.discard_beyond(&inode, size);
        self.flush_held(&inode, false, false)?;
        let mut known = fresh.map(|o| o.attr.size); // 线性一致的最新长度；None = 还不知道（经句柄：用到时 GetAttr）
        for _ in 0..STALE_RETRIES {
            match self.truncate_once(&inode, who, size, by_handle, known, local_end) {
                Ok(o) => {
                    let mut st = lock(&inode.st);
                    st.install(&o);
                    st.mine(&o.attr);
                    return Ok(o.attr);
                }
                Err(ErrCode::STALE_LAYOUT) => {
                    self.stale(&inode, who, if by_handle { Reopen::Handle } else { Reopen::Path })?;
                    known = Some(lock(&inode.st).attr.size); // 重新 Open 回来的长度就是最新的
                }
                Err(e) => return Err(e),
            }
        }
        Err(ErrCode::ESTALE)
    }

    /// 截断的第 3、4 步（见文件头）按手上的 layout 做一遍；layout 变了 → `STALE_LAYOUT`（调用方重新 Open 再来）。
    fn truncate_once(
        &self,
        inode: &Inode,
        who: &Caller,
        size: u64,
        by_handle: bool,
        known: Option<u64>,
        local_end: u64,
    ) -> Result<OpenOut, ErrCode> {
        let ino = inode.ino;
        let (inline, l, cached) = {
            let st = lock(&inode.st);
            (st.layout_view()?.0, st.layout.clone().ok_or(ErrCode::EINVAL)?, st.attr.size)
        };
        if size > 0 && !inline {
            let latest = match known {
                Some(s) => s,
                None => self.size_now(who, ino)?,
            };
            let old = latest.max(cached).max(local_end);
            let done = self.with_cap(inode, who, true, |cap| truncate_chunks(&self.data, cap, &l, ino, old, size))?;
            lock(&inode.st).unsynced.extend(done);
        }
        let epoch_seen = if inline { 0 } else { l.epoch };
        let op = if by_handle {
            MetaOp::TruncateHandle { ino, size, epoch_seen }
        } else {
            let set =
                SetAttrIn { mode: None, uid: None, gid: None, size: Some(size), atime: None, mtime: None, epoch_seen };
            MetaOp::SetAttr { ino, set }
        };
        expect_open_out(self.meta.write(who, op))
    }

    /// 线性一致的最新长度（GetAttr）。
    fn size_now(&self, who: &Caller, ino: u64) -> Result<u64, ErrCode> {
        match self.meta.read(who, MetaOp::GetAttr { ino })? {
            MetaReply::Entry { attr } => Ok(attr.size),
            _ => Err(ErrCode::INTERNAL),
        }
    }

    /// 截断之前：丢掉脏区里 `size` 之后的数据（起点不在 size 之前的 chunk 整个放掉槽位，跨 size 的 chunk 剪短区间）。
    fn discard_beyond(&self, inode: &Inode, size: u64) {
        let mut st = inode.wait_idle();
        let Some(cs) = st.layout.as_ref().map(|l| l.chunk_size as u64) else { return };
        let before = st.dirty.chunks.len();
        st.dirty.chunks.retain(|idx, _| (*idx as u64) * cs < size);
        let freed = before - st.dirty.chunks.len();
        if let Some((idx, c)) = st.dirty.chunks.iter_mut().next_back() {
            let keep = (size - *idx as u64 * cs).min(cs) as u32; // 这个 chunk 里还留着的长度
            c.extents.retain_mut(|(o, n)| {
                *n = (*n).min(keep.saturating_sub(*o));
                *n > 0
            });
            if c.extents.is_empty() {
                let i = *idx;
                st.dirty.chunks.remove(&i);
                drop(st);
                self.slots.release(freed + 1);
                return;
            }
        }
        drop(st);
        self.slots.release(freed);
    }

    /// `fallocate`（只支持 mode 0：扩展长度，3.1.1）：经句柄（ADR-0036）。返回新属性。
    pub fn fallocate(&self, who: &Caller, ino: u64, off: u64, len: u64, mode: u32) -> Result<AttrOut, ErrCode> {
        let MetaReply::Entry { attr } = self.meta.write(who, MetaOp::Fallocate { ino, offset: off, len, mode })? else {
            return Err(ErrCode::INTERNAL);
        };
        if let Some(i) = self.inode(ino) {
            let mut st = lock(&i.st);
            st.patch_inline(&attr, &[]); // inline 内容不变（新增的区域读 0）
            st.mine(&attr);
        }
        Ok(attr)
    }
}

/// 截断的数据面一步（3.4.2.9）：`[⌊new/chunk⌋, ⌈old/chunk⌉)` 的每个 chunk 截到它在 `new` 之内的长度（整个在 `new` 之后的截到 0），
/// 返回发过 TruncateChunk 的 chunk 号。截到 0 与 inline 文件不用它（服务端换 epoch；inline 文件没有 chunk）。FUSE 的截断与
/// `wmfs fs truncate` 共用。
pub fn truncate_chunks(
    data: &DataClient,
    cap: &[u8],
    l: &LayoutV1,
    ino: u64,
    old: u64,
    new: u64,
) -> Result<Vec<u32>, ErrCode> {
    let cs = l.chunk_size as u64;
    let mut done = Vec::new();
    for idx in new / cs..old.div_ceil(cs) {
        let idx = u32::try_from(idx).map_err(|_| ErrCode::EFBIG)?;
        let keep = new.saturating_sub(idx as u64 * cs).min(cs) as u32;
        data.truncate(cap, chunk_id(l, ino, idx)?, keep)?;
        done.push(idx);
    }
    Ok(done)
}
