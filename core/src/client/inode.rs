//! 一个 inode 在本挂载里的状态（design.md 8.2、8.3、3.1.4）：所有句柄共用一份 [`InodeState`]（属性、layout、cap、inline 副本、
//! 写回缓冲、记下的写错误）；`Open`、`Create`、改长度的 SetAttr/TruncateHandle、`Uninline`、重新 Open 的回复都直接更新它（C4）。
//!
//! - **只前进**：4 个后台写线程各自 `CommitWrite`，回复到达的顺序可能与服务端执行的顺序不同；属性只在回复的 `change_id` 不小于
//!   手上的时才换上（change_id 每次修改 + 1、从不变小）。否则读会按变短的长度截断（已提交的数据读成 0），截断也会按旧长度算范围。
//! - **C2 的"见过的 change_id"**：open 时记下服务端的值（调用方据此 `inval_inode` 或回 `FOPEN_KEEP_CACHE`）；自己的修改
//!   （CommitWrite、改长度、Uninline、Fallocate）回来的 change_id 恰好是见过的 + 1（只有我们改过）才前进（[`InodeState::mine`]），
//!   中间夹着别人的修改就不前进，下一次 open 照样失效。`STALE_LAYOUT` 之后的重新 Open 只换上 layout、cap 与属性
//!   （[`InodeState::install`]），**不**前进：那正是别人改过的时候。
//! - **inline 副本**带着它对应的 change_id（`inline_cid`）：回复带内容时才换上；它与属性的 change_id 对不上（只写打开、截断者
//!   不能读、别人改过）就当作不知道内容，读的时候重新取（`read.rs`）。自己的提交恰好 + 1 时把补丁打在副本上（[`InodeState::patch_inline`]）。
//! - cap 按权限位分两份——`rcap`（带读）与 `wcap`（带写），各自保留最新的一张（实现注记（M6）：8.2 写的是"cap 只有一份"；同一个
//!   inode 先写打开、再只读打开时，只读打开回来的 cap 没有写权限，只存一份会让写回用错 cap）。layout 的 epoch 变了：两张 cap 与
//!   "写过、还没 SyncChunks 的 chunk"一起作废（旧 epoch 的 chunk 已经不属于这个文件，fsync 不用再同步它们）。
//! - **io 权**：同一个 inode 的提交、截断、同步写一个一个来（[`Inode::io`]）。实现注记（M6）：用 InodeState 里的一个标记加条件变量，
//!   不用 `Mutex<()>`——这些事都要等网络，1.2 规则 2 不许一边持锁一边等网络；等的人在条件变量上等，flusher 线程用 [`Inode::try_io`]
//!   跳过正忙的 inode（不会因为一个 inode 的请求卡住，全部 inode 的写回都停下）。

use std::collections::BTreeSet;
use std::sync::{Condvar, Mutex, MutexGuard};

use super::meta_client::Caller;
use super::writeback::DirtyFile;
use crate::base::{ErrCode, clock, lock};
use crate::meta::schema::records::{F_INLINE, LayoutV1};
use crate::proto::cap::{self, RIGHT_READ, RIGHT_WRITE};
use crate::proto::meta::{AttrOut, OpenOut};

/// 一张 cap 与它该续期的墙钟时刻（用到 80%，8.3）。
#[derive(Clone, Default)]
pub struct Cap {
    pub bytes: Vec<u8>,
    pub renew_ms: u64,
}

impl Cap {
    pub fn new(bytes: Vec<u8>) -> Cap {
        let now = clock::wall_ms();
        let renew_ms = cap::peek(&bytes).map_or(0, |c| now + c.expires_ms.saturating_sub(now) * 4 / 5);
        Cap { bytes, renew_ms }
    }
}

/// 一个 inode 在本挂载里的状态（8.2）。
pub struct InodeState {
    pub attr: AttrOut,
    pub seen_change_id: u64, // 本挂载的内核缓存对应的 change_id（C2，见文件头）
    pub layout: Option<LayoutV1>,
    pub rcap: Cap,
    pub wcap: Cap,
    pub inline: Option<Vec<u8>>, // inline 文件的完整内容（`inline_cid` 那个版本的）
    pub inline_cid: u64,         // `inline` 对应的 change_id
    pub dirty: DirtyFile,
    pub write_err: Option<ErrCode>, // 写回彻底失败的错误：下一次 write/flush/fsync/close 报告（类似内核的 errseq）
    pub writer: Option<Caller>,     // 最近一次写的人（后台写回以他的身份提交）
    pub readers: u32,
    pub writers: u32,
    pub unsynced: BTreeSet<u32>, // 写过或截断过、还没 SyncChunks 的 chunk（fsync 用）
    io_busy: bool,               // 有线程拿着 io 权（[`Inode::io`]）
}

impl InodeState {
    fn new(attr: AttrOut) -> InodeState {
        InodeState {
            attr,
            seen_change_id: 0,
            layout: None,
            rcap: Cap::default(),
            wcap: Cap::default(),
            inline: None,
            inline_cid: 0,
            dirty: DirtyFile::default(),
            write_err: None,
            writer: None,
            readers: 0,
            writers: 0,
            unsynced: BTreeSet::new(),
            io_busy: false,
        }
    }

    /// 换上一个 OpenOut 里的 layout、cap、属性与 inline 内容（不动"见过的 change_id"，见文件头）。比手上的还旧的回复（并发的
    /// 请求晚到了）整个不要。
    pub fn install(&mut self, o: &OpenOut) {
        if o.attr.change_id < self.attr.change_id {
            return;
        }
        let old_epoch = self.layout.as_ref().map(|l| l.epoch);
        if o.layout.is_some() {
            self.layout = o.layout.clone();
        }
        if old_epoch.is_some() && old_epoch != self.layout.as_ref().map(|l| l.epoch) {
            self.rcap = Cap::default();
            self.wcap = Cap::default();
            self.unsynced.clear();
        }
        if let Ok(c) = cap::peek(&o.cap) {
            if c.rights & RIGHT_READ != 0 {
                self.rcap = Cap::new(o.cap.clone());
            }
            if c.rights & RIGHT_WRITE != 0 {
                self.wcap = Cap::new(o.cap.clone());
            }
        }
        if let Some(d) = &o.inline {
            self.inline = Some(d.clone());
            self.inline_cid = o.attr.change_id;
        }
        self.attr = o.attr.clone();
    }

    /// 自己的一次修改回来的属性：change_id 恰好是见过的 + 1 才前进"见过的"（C2）；属性只前进。
    pub fn mine(&mut self, a: &AttrOut) {
        if a.change_id == self.seen_change_id + 1 {
            self.seen_change_id = a.change_id;
        }
        if a.change_id >= self.attr.change_id {
            self.attr = a.clone();
        }
    }

    /// 自己的 CommitWrite / Fallocate 回来了：inline 副本到这次之前没有别人改过（`inline_cid` 恰好 + 1）就把补丁打上去、跟上新
    /// 版本；否则副本留在旧版本，读时对不上就重新取。补丁越过副本的末尾时中间补 0（与服务端相同）。
    pub fn patch_inline(&mut self, a: &AttrOut, patches: &[(u64, Vec<u8>)]) {
        if a.change_id != self.inline_cid + 1 {
            return;
        }
        let Some(d) = self.inline.as_mut() else { return };
        for (o, b) in patches {
            let e = *o as usize + b.len();
            if d.len() < e {
                d.resize(e, 0);
            }
            d[*o as usize..e].copy_from_slice(b);
        }
        self.inline_cid = a.change_id;
    }

    /// 当前属性那个版本的 inline 内容（不知道 → None）。
    pub fn inline_now(&self) -> Option<&Vec<u8>> {
        self.inline.as_ref().filter(|_| self.inline_cid == self.attr.change_id)
    }

    /// 写与截断要看的 layout：(还是 inline, inline_max, chunk_size)。没有 layout（不是普通文件）→ `EINVAL`。
    pub fn layout_view(&self) -> Result<(bool, u64, u64), ErrCode> {
        let l = self.layout.as_ref().ok_or(ErrCode::EINVAL)?;
        Ok((self.attr.flags & F_INLINE != 0, l.inline_max as u64, l.chunk_size as u64))
    }

    /// 本地写到的末尾（没有 layout 或没有脏数据 → 0）。
    pub fn local_end(&self) -> u64 {
        self.layout.as_ref().map_or(0, |l| self.dirty.local_end(l.chunk_size))
    }

    pub fn clone_cap(&self, write: bool) -> Cap {
        if write { self.wcap.clone() } else { self.rcap.clone() }
    }
}

/// 回给内核的长度：max(元数据长度, 本地写到的末尾)（本挂载还有没提交的写时，否则内核会把文件看短）。
pub fn fix_size(st: &InodeState, attr: &mut AttrOut) {
    attr.size = attr.size.max(st.local_end());
}

/// 一个 inode：状态（短暂持锁，不跨网络）+ 条件变量（等后台写线程写完手上的 chunk、等 io 权）。
pub struct Inode {
    pub ino: u64,
    pub st: Mutex<InodeState>,
    pub cv: Condvar,
}

/// 拿着一个 inode 的 io 权（见文件头）；丢掉时放掉并叫醒等它的人。拿着它的时候不要拿着 `st` 的锁丢掉它。
pub struct IoGuard<'a> {
    inode: &'a Inode,
}

impl Drop for IoGuard<'_> {
    fn drop(&mut self) {
        lock(&self.inode.st).io_busy = false;
        self.inode.cv.notify_all();
    }
}

impl Inode {
    pub fn new(attr: AttrOut) -> Inode {
        Inode { ino: attr.ino, st: Mutex::new(InodeState::new(attr)), cv: Condvar::new() }
    }

    /// 拿 io 权：别人拿着就在条件变量上等（等的时候不持锁）。
    pub fn io(&self) -> IoGuard<'_> {
        let mut st = lock(&self.st);
        while st.io_busy {
            st = self.cv.wait(st).unwrap_or_else(|e| e.into_inner());
        }
        st.io_busy = true;
        IoGuard { inode: self }
    }

    /// 同 [`Inode::io`]，但别人拿着就不等（flusher 线程用：这一轮跳过它）。
    pub fn try_io(&self) -> Option<IoGuard<'_>> {
        let mut st = lock(&self.st);
        if st.io_busy {
            return None;
        }
        st.io_busy = true;
        Some(IoGuard { inode: self })
    }

    /// 等后台写线程把这个 inode 手上的 chunk 写完（它们的结果都已落定），返回状态的锁。
    pub fn wait_idle(&self) -> MutexGuard<'_, InodeState> {
        let st = lock(&self.st);
        self.cv.wait_while(st, |s| s.dirty.busy > 0).unwrap_or_else(|e| e.into_inner())
    }
}

#[cfg(test)]
mod tests;
