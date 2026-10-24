//! 客户端的 inode 表、文件句柄表与目录句柄（design.md 8.2、8.3、3.1.4）：FUSE 适配层（`cli/src/fuse/`）只经 [`FileClient`] 读写
//! 文件、列目录；将来的内核客户端适配或 LD_PRELOAD 库也能复用它（不依赖 fuser）。一个 inode 的状态在 `inode.rs`。
//!
//! - **C2**：open 总是向 leader 取最新属性；`change_id` 与本挂载上次见到的不同 → 调用方先 `inval_inode` 再回复、不带
//!   `FOPEN_KEEP_CACHE`，这里同时丢掉 cache_store 里这个 inode 的旧块（键里带 change_id，不丢也读不错，只是白占内存）；相同 → 带。
//! - **打开集合**：每 3 s 的会话心跳上报增量（新打开 / 已关闭，带是否写打开；`session.rs`）。
//! - consistency = strong 或 `O_DIRECT` 打开 → `direct`：FUSE 回 `FOPEN_DIRECT_IO`，写走 `strong.rs` 的同步路径（8.3）。
//! - relatime：读打开的回复带 `atime_due` 时把这一项放进队列，`atime` 线程每 5 s 批量发 `SetAtime`（尽力而为）。
//! - **cap**：用到 80% 时 `RenewCap`（经已打开的句柄，ADR-0036：属主不看权限位）；续回来的 cap 的 epoch 与手上的 layout 不同 =
//!   layout 已经变了 → `STALE_LAYOUT`（调用方重新 Open）。数据节点回 `BAD_CAP` 时续一次再试，再失败 → `EACCES`（8.7，
//!   [`FileClient::with_cap`]：读、写、截断、SyncChunks 都经它）。
//! - **重新取 layout**（`STALE_LAYOUT` 之后，C4）：只换上 layout、cap 与属性，不前进"见过的 change_id"（`inode.rs`）；写句柄用
//!   `ReopenHandle`（M7，ADR-0037 第 4 条），读句柄也用 `ReopenHandle{write: false}`（M8 第一步，ADR-0038 第 7 条），按路径截断用 `Open`。
//! - **目录句柄** = opendir 时按名字分页读出的完整快照（readdir 的 offset = 快照下标；POSIX 允许 readdir 反映某个快照），带属性
//!   （readdirplus 用）；前面补上 `.` 与 `..`。实现注记（M7，ADR-0037 第 13 条）：`..` 的 inode 号是父目录（不改协议：客户端在
//!   lookup、mkdir、readdirplus 与自己的 rename 时记下目录的父目录，[`FileClient::note_parent`]）；根目录的 `..` 是它自己；不知道时
//!   （极少）沿用目录自己的并写 debug 日志。M6 的 `..` 一律是目录自己。

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crossbeam_channel::{Sender, unbounded};

use super::data_client::DataClient;
use super::inode::{Cap, Inode, fix_size};
use super::meta_client::{Caller, MetaClient};
use super::session::{AtimeItem, OpenDelta};
use super::writeback::Slots;
use crate::base::{ErrCode, clock, lock, log};
use crate::hooks::manifest::Hook;
use crate::hooks::pipeline::HookCall;
use crate::hooks::traits::CacheStore;
use crate::meta::attrs::CONSISTENCY_STRONG;
use crate::meta::schema::keys::ROOT_INO;
use crate::meta::schema::records::KIND_DIR;
use crate::proto::cap;
use crate::proto::meta::{AttrOut, DirEntryOut, MetaOp, MetaReply, O_ACCMODE, O_RDONLY, O_RDWR, O_WRONLY, OpenOut};

/// 一个打开的文件句柄。
#[derive(Clone, Copy, Debug)]
pub struct Handle {
    pub ino: u64,
    pub read: bool,
    pub write: bool,
    pub direct: bool, // FOPEN_DIRECT_IO：strong 或 O_DIRECT（8.3）
    pub sync: bool,   // O_SYNC / O_DSYNC：每次 write 写到全部副本并落盘、提交长度才返回
}

/// open / create 的结果。
pub struct Opened {
    pub fh: u64,
    pub attr: AttrOut,
    pub keep_cache: bool, // C2：change_id 没变 → FOPEN_KEEP_CACHE；变了 → 调用方先 inval_inode
    pub direct: bool,
}

/// 客户端的文件层：MetaClient + DataClient + cache_store + inode/句柄表 + 写回。
pub struct FileClient {
    pub meta: Arc<MetaClient>,
    pub data: DataClient,
    pub(super) cache: Option<(Arc<dyn CacheStore>, HookCall)>,
    pub(super) inodes: Mutex<HashMap<u64, Arc<Inode>>>,
    handles: Mutex<HashMap<u64, Handle>>,
    dirs: Mutex<HashMap<u64, Arc<Vec<DirEntryOut>>>>, // 目录句柄 → 快照
    parents: Mutex<HashMap<u64, u64>>,                // 目录 → 它的父目录（快照里 `..` 的 inode 号，见文件头）
    next_fh: AtomicU64,                               // 文件句柄与目录句柄共用这一个计数（不会撞号）
    pub(super) delta: Mutex<OpenDelta>,
    pub(super) slots: Slots,
    pub(super) jobs: Sender<(u64, u32)>, // 写满了的 chunk：交给后台写线程
    pub(super) atime: Mutex<Vec<AtimeItem>>,
}

impl FileClient {
    /// 建好文件层并起后台线程：4 个写线程与 flusher（`flusher.rs`）、session-hb 与 atime（`session.rs`）。
    /// 后台线程只持有 Weak：FileClient 被丢弃后各自退出。
    pub fn start(meta: Arc<MetaClient>, data: DataClient, cache: Option<Arc<dyn CacheStore>>) -> Arc<FileClient> {
        let (tx, rx) = unbounded::<(u64, u32)>();
        let cache = cache.map(|c| {
            let h = HookCall::new(c.manifest(), Hook::CacheStore);
            (c, h)
        });
        let slots = Slots::for_pool(data.pool());
        let fc = Arc::new(FileClient {
            meta,
            data,
            cache,
            inodes: Mutex::default(),
            handles: Mutex::default(),
            dirs: Mutex::default(),
            parents: Mutex::default(),
            next_fh: AtomicU64::new(1),
            delta: Mutex::default(),
            slots,
            jobs: tx,
            atime: Mutex::default(),
        });
        super::flusher::start_threads(&fc, rx);
        super::session::start_threads(&fc);
        fc
    }

    /// 打开一个已有的文件（只读请求，3.3.4）；`flags` 是内核给的打开标志。
    pub fn open(&self, who: &Caller, ino: u64, flags: u32) -> Result<Opened, ErrCode> {
        let o = expect_open_out(self.meta.read(who, MetaOp::Open { ino, flags: flags & O_ACCMODE }))?;
        Ok(self.register(who, o, flags, false))
    }

    /// 建文件并打开（`flags` 里有 O_EXCL 时名字已存在 → `EEXIST`）。
    pub fn create(
        &self,
        who: &Caller,
        parent: u64,
        name: &[u8],
        mode: u32,
        umask: u32,
        flags: u32,
    ) -> Result<Opened, ErrCode> {
        let excl = flags & libc::O_EXCL as u32 != 0;
        let op = MetaOp::Create { parent, name: name.to_vec(), mode, umask, excl };
        let o = expect_open_out(self.meta.write(who, op))?;
        Ok(self.register(who, o, flags, true))
    }

    fn register(&self, who: &Caller, o: OpenOut, flags: u32, created: bool) -> Opened {
        let ino = o.attr.ino;
        let inode = self.inode_entry(&o);
        let acc = flags & O_ACCMODE;
        let (read, write) = (acc != O_WRONLY, acc != O_RDONLY);
        let direct = flags & libc::O_DIRECT as u32 != 0 || o.attr.consistency == CONSISTENCY_STRONG;
        let sync = flags & (libc::O_SYNC | libc::O_DSYNC) as u32 != 0;
        let mut st = lock(&inode.st);
        let keep_cache = !created && st.seen_change_id == o.attr.change_id;
        st.seen_change_id = o.attr.change_id;
        st.install(&o);
        let first = st.readers + st.writers == 0;
        st.readers += read as u32;
        st.writers += write as u32;
        // 打开集合的增量：第一次打开 → 报"打开了"（带是否写打开）；已经开着、来了第一个写者 → 报"写打开"
        let report = if first {
            Some(st.writers > 0)
        } else if write && st.writers == 1 {
            Some(true)
        } else {
            None
        };
        let mut attr = st.attr.clone();
        fix_size(&st, &mut attr);
        drop(st);
        if let Some(w) = report {
            self.note(ino, Some(w)); // 不拿着 inode 的锁（心跳线程是先 delta 后 inode）
        }
        if !keep_cache && !created {
            self.drop_cached_blocks(ino);
        }
        if o.atime_due {
            lock(&self.atime).push(AtimeItem { ino, at: clock::wall_ts(), who: who.clone() });
        }
        let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
        lock(&self.handles).insert(fh, Handle { ino, read, write, direct, sync });
        Opened { fh, attr, keep_cache, direct }
    }

    /// 这个 inode 在表里的项（没有就按回复里的属性建一个）。
    pub(super) fn inode_entry(&self, o: &OpenOut) -> Arc<Inode> {
        lock(&self.inodes).entry(o.attr.ino).or_insert_with(|| Arc::new(Inode::new(o.attr.clone()))).clone()
    }

    /// 全部 inode（flusher 与退出前的写回用）。
    pub(super) fn all_inodes(&self) -> Vec<Arc<Inode>> {
        lock(&self.inodes).values().cloned().collect()
    }

    /// 本进程自己的身份（有效 uid/gid）：后台写回找不到写者时用它。
    pub(super) fn process_caller(&self) -> Caller {
        let (uid, gid) = crate::base::sys::euid_egid();
        Caller { uid, gid, groups: vec![], pid: std::process::id(), job_id: 0 }
    }

    /// 句柄 `fh` 的信息（不存在 → `EBADF`）。
    pub fn handle(&self, fh: u64) -> Result<Handle, ErrCode> {
        lock(&self.handles).get(&fh).copied().ok_or(ErrCode::EBADF)
    }

    /// inode 在本挂载里的状态（没打开过 → None）。
    pub fn inode(&self, ino: u64) -> Option<Arc<Inode>> {
        lock(&self.inodes).get(&ino).cloned()
    }

    /// 关掉句柄（FUSE release）。最后一个写者关掉时还有没写出去的数据（之前的 flush 失败了，错误已经在 close 时报告）：
    /// 再试一次，仍失败就丢弃并写一行错误日志（不再占着内存无限重试）。
    pub fn release(&self, fh: u64) {
        let Some(h) = lock(&self.handles).remove(&fh) else { return };
        let Some(inode) = self.inode(h.ino) else { return };
        let mut st = lock(&inode.st);
        st.readers -= h.read as u32;
        st.writers -= h.write as u32;
        let (open, writers, dirty) = (st.readers + st.writers > 0, st.writers, !st.dirty.is_empty());
        drop(st);
        if !open {
            self.note(h.ino, None);
        } else if h.write && writers == 0 {
            self.note(h.ino, Some(false));
        }
        if h.write && writers == 0 && dirty && self.flush_inode(&inode, false, false).is_err() {
            let n = self.drop_dirty(&inode);
            log::error(
                "client",
                format_args!("ino {}：写回一直失败（已在 close 时报告），丢弃 {n} 个 chunk 的脏数据", h.ino),
            );
        }
    }

    /// 内核忘掉了这个 inode（FUSE forget）：没有句柄、也没有脏数据时丢掉它的状态（连同"见过的 change_id"）与它在 cache_store 里的块。
    pub fn forget(&self, ino: u64) {
        lock(&self.parents).remove(&ino);
        let mut m = lock(&self.inodes);
        let idle = m.get(&ino).is_some_and(|i| {
            let st = lock(&i.st);
            st.readers + st.writers == 0 && st.dirty.is_empty()
        });
        if idle {
            m.remove(&ino);
            drop(m);
            self.drop_cached_blocks(ino);
        }
    }

    /// 丢掉 cache_store 里这个 inode 的全部块（经 3.7.4 的调用包装；出错只是少丢一些，不影响正确性）。
    fn drop_cached_blocks(&self, ino: u64) {
        if let Some((c, hc)) = &self.cache {
            let _ = hc.call(|| {
                c.invalidate_ino(ino);
                Ok(())
            });
        }
    }

    /// 回给内核的属性：本挂载还有没提交的写时，长度取 max(元数据长度, 本地写到的末尾)（否则内核会把文件看短）。
    pub fn fix_attr(&self, attr: &mut AttrOut) {
        if let Some(i) = self.inode(attr.ino) {
            fix_size(&lock(&i.st), attr);
        }
    }

    /// 重新取最新的 layout、cap 与属性（C4：`STALE_LAYOUT` 之后），只换上、不前进"见过的 change_id"。`how`：经写句柄
    /// （写回、同步写、ftruncate）→ `ReopenHandle{write}`（ADR-0037 第 4 条：属主不看权限位，所以属主经句柄写一个 0444 文件时
    /// 碰上 layout 变化也照常重写进新 epoch；实现注记（M7）：以前只能 Open，回 `ESTALE`）；读 → `ReopenHandle{write: false}`
    /// （M8 第一步，ADR-0038 第 7 条：属主去掉自己的读权限之后读句柄照常可用；对其他人与 `Open` 只读的规则相同）；按路径截断
    /// （没有写句柄）→ `Open`，照常按当前权限：先试读写打开（顺便拿到读权限与 inline 内容），没有读权限再试只写。被授权拒绝
    /// → 原样回 `EACCES`（8.7 的 M7 注记：与 ADR-0036 的"其他用户之后可能 EACCES"一致；M6 把它映射成 `ESTALE`）。
    pub(super) fn reopen(&self, inode: &Inode, who: &Caller, how: Reopen) -> Result<(), ErrCode> {
        let ino = inode.ino;
        let r = match how {
            Reopen::Handle => self.meta.read(who, MetaOp::ReopenHandle { ino, write: true }),
            Reopen::Read => self.meta.read(who, MetaOp::ReopenHandle { ino, write: false }),
            Reopen::Path => match self.meta.read(who, MetaOp::Open { ino, flags: O_RDWR }) {
                Err(ErrCode::EACCES) => self.meta.read(who, MetaOp::Open { ino, flags: O_WRONLY }),
                r => r,
            },
        };
        let o = expect_open_out(r)?;
        lock(&inode.st).install(&o);
        Ok(())
    }

    /// `STALE_LAYOUT` 之后（C4）：写一行日志，再 [`reopen`](Self::reopen)。
    pub(super) fn stale(&self, inode: &Inode, who: &Caller, how: Reopen) -> Result<(), ErrCode> {
        let what = if how == Reopen::Read { "读" } else { "重写" };
        log::info("client", format_args!("ino {}：layout 变了（STALE_LAYOUT），重新取 layout 再{what}", inode.ino));
        self.reopen(inode, who, how)
    }

    /// 带写（或读）权限的 cap；用到 80% 了先 `RenewCap`。
    pub(super) fn cap_for(&self, inode: &Inode, who: &Caller, write: bool) -> Result<Vec<u8>, ErrCode> {
        let c = lock(&inode.st).clone_cap(write);
        if !c.bytes.is_empty() && clock::wall_ms() < c.renew_ms {
            return Ok(c.bytes);
        }
        self.renew(inode, who, write)
    }

    /// `RenewCap`（经已打开的句柄，ADR-0036），换上新 cap。续回来的 cap 的 epoch 与手上的 layout 不同 → layout 已经变了：不换，
    /// 回 `STALE_LAYOUT`（调用方重新 Open 取新 layout）。
    pub(super) fn renew(&self, inode: &Inode, who: &Caller, write: bool) -> Result<Vec<u8>, ErrCode> {
        let MetaReply::Cap { cap: b } = self.meta.read(who, MetaOp::RenewCap { ino: inode.ino, write })? else {
            return Err(ErrCode::INTERNAL);
        };
        let epoch = cap::peek(&b)?.epoch;
        let mut st = lock(&inode.st);
        if st.layout.as_ref().map(|l| l.epoch) != Some(epoch) {
            return Err(ErrCode::STALE_LAYOUT);
        }
        if write {
            st.wcap = Cap::new(b.clone());
        } else {
            st.rcap = Cap::new(b.clone());
        }
        Ok(b)
    }

    /// 用这个 inode 的 cap 发一个数据请求 `f`：数据节点回 `BAD_CAP`（过期、被吊销……）→ 续一次 cap 再试，再失败 → `EACCES`
    /// （3.3.7、8.7）。读、写、截断、SyncChunks 都经它。
    pub(super) fn with_cap<T>(
        &self,
        inode: &Inode,
        who: &Caller,
        write: bool,
        mut f: impl FnMut(&[u8]) -> Result<T, ErrCode>,
    ) -> Result<T, ErrCode> {
        let cap = self.cap_for(inode, who, write)?;
        match f(&cap) {
            Err(ErrCode::BAD_CAP) => {
                let cap = self.renew(inode, who, write)?;
                f(&cap).map_err(|e| if e == ErrCode::BAD_CAP { ErrCode::EACCES } else { e })
            }
            r => r,
        }
    }

    /// 记下目录 `dir` 的父目录（lookup、mkdir、readdirplus 与自己的 rename 时；只对目录记，内核 forget 时一起丢）。
    pub fn note_parent(&self, dir: u64, parent: u64) {
        lock(&self.parents).insert(dir, parent);
    }

    /// opendir：读出完整的目录快照（按名字分页，每页 1024 条、带属性），前面补 `.` 与 `..`；返回目录句柄。`..` 是父目录
    /// （根目录的是它自己；不知道时——极少，例如别的客户端刚移动了它——沿用目录自己的，写一行 debug 日志）；快照里的子目录顺便
    /// 记下父目录。实现注记（M7，ADR-0037 第 13 条）：M6 的 `..` 一律是目录自己。
    pub fn open_dir(&self, who: &Caller, ino: u64) -> Result<u64, ErrCode> {
        let MetaReply::Entry { attr: me } = self.meta.read(who, MetaOp::GetAttr { ino })? else {
            return Err(ErrCode::INTERNAL);
        };
        let up = if ino == ROOT_INO { Some(ROOT_INO) } else { lock(&self.parents).get(&ino).copied() };
        let up = up.unwrap_or_else(|| {
            log::debug("client", format_args!("目录 {ino} 的父目录不知道：快照里的 .. 先用它自己"));
            ino
        });
        // fuser 的 readdirplus 用属性里的 ino 填 d_ino（内核对 . 与 .. 不用别的属性），所以 .. 的属性换上父目录的号
        let dot = |name: &[u8], ino| {
            let attr = Some(AttrOut { ino, ..me.clone() });
            DirEntryOut { name: name.to_vec(), ino, kind: KIND_DIR, attr }
        };
        let mut all = vec![dot(b".", ino), dot(b"..", up)];
        let mut after = Vec::new();
        loop {
            let op = MetaOp::ReadDir { ino, after: after.clone(), limit: 1024, plus: true };
            let MetaReply::Dir { entries, eof } = self.meta.read(who, op)? else { return Err(ErrCode::INTERNAL) };
            after = entries.last().map(|e| e.name.clone()).unwrap_or_default();
            all.extend(entries);
            if eof || after.is_empty() {
                break;
            }
        }
        let mut parents = lock(&self.parents);
        for e in all[2..].iter().filter(|e| e.kind == KIND_DIR) {
            parents.insert(e.ino, ino);
        }
        drop(parents);
        let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
        lock(&self.dirs).insert(fh, Arc::new(all));
        Ok(fh)
    }

    /// 目录句柄的快照（不存在 → None）。
    pub fn dir(&self, fh: u64) -> Option<Arc<Vec<DirEntryOut>>> {
        lock(&self.dirs).get(&fh).cloned()
    }

    /// releasedir。
    pub fn close_dir(&self, fh: u64) {
        lock(&self.dirs).remove(&fh);
    }
}

/// layout 变了之后怎么重新取它（[`FileClient::reopen`]）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Reopen {
    Handle, // 经写句柄：ReopenHandle（属主不看权限位）
    Read,   // 读句柄：ReopenHandle{write: false}（属主不看权限位，M8 第一步）
    Path,   // 按路径截断：Open，按当前权限
}

/// 回复应当是 `Opened(OpenOut)`（Open、Create、改长度的 SetAttr/TruncateHandle、Uninline）。
pub(super) fn expect_open_out(r: Result<MetaReply, ErrCode>) -> Result<OpenOut, ErrCode> {
    match r? {
        MetaReply::Opened(o) => Ok(o),
        _ => Err(ErrCode::INTERNAL),
    }
}
