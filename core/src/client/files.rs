//! 客户端的 inode 表、文件句柄表与目录句柄（design.md 8.2、8.3、3.1.4）：FUSE 适配层（`cli/src/fuse/`）只经 [`FileClient`] 读写
//! 文件、列目录；将来的内核客户端适配或 LD_PRELOAD 库也能复用它（不依赖 fuser）。一个 inode 的状态在 `inode.rs`。
//!
//! - **C2**：open 总是向 leader 取最新属性；`change_id` 与本挂载上次见到的不同 → 调用方先 `inval_inode` 再回复、不带
//!   `FOPEN_KEEP_CACHE`，这里同时丢掉 cache_store 里这个 inode 的旧块（键里带 change_id，不丢也读不错，只是白占内存）；相同 → 带。
//! - **打开集合**：每 3 s 的会话心跳上报增量（新打开 / 已关闭，带是否写打开；`session.rs`）。
//! - consistency = strong 或 `O_DIRECT` 打开 → `direct`：FUSE 回 `FOPEN_DIRECT_IO`，写走 `strong.rs` 的同步路径（8.3）。实现注记（M12）：
//!   strong 文件以写方式打开、而它还是 inline → 先 `Uninline`（8.5）；句柄记下 strong 与 O_APPEND（读前 GetAttr、追加用 AppendReserve）。
//! - relatime：读打开的回复带 `atime_due` 时把这一项放进队列，`atime` 线程每 5 s 批量发 `SetAtime`（尽力而为）。
//! - **cap**：用到 80% 时 `RenewCap`（经已打开的句柄，ADR-0036：属主不看权限位）；续回来的 cap 的 epoch 与手上的 layout 不同 =
//!   layout 已经变了 → `STALE_LAYOUT`（调用方重新 Open）。数据节点回 `BAD_CAP` 时续一次再试，再失败 → `EACCES`（8.7，
//!   [`FileClient::with_cap`]：读、写、截断、SyncChunks 都经它）。
//! - **重新取 layout**（`STALE_LAYOUT` 之后，C4）：只换上 layout、cap 与属性，不前进"见过的 change_id"（`inode.rs`）；写句柄用
//!   `ReopenHandle`（M7，ADR-0037 第 4 条），读句柄也用 `ReopenHandle{write: false}`（M8 第一步，ADR-0038 第 7 条），按路径截断用 `Open`。
//! - **目录句柄** = opendir 时的完整快照（`dirs.rs`）。
//! - **会话被驱逐**（M10，3.4.4）：句柄（文件与目录的）记下打开时的会话号；不是现在的 = 被驱逐的会话里打开的 → 此后一律 `EIO`
//!   （[`FileClient::handle`]；关闭照常）。句柄上的操作与数据请求先过 MetaClient 的自停闸门（0.8 × lease，`lease.rs`），经句柄的
//!   元数据请求只在它的会话里发（`MetaClient::read_in`/`write_in`，M10 审查：在闸门上等到换了新会话的请求不会在新会话里做）。

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use crossbeam_channel::{Sender, unbounded};

use super::data_client::DataClient;
use super::dirs::DirHandle;
use super::inode::{Cap, Inode, fix_size};
use super::locks::Locks;
use super::meta_client::{Caller, MetaClient};
use super::session::AtimeItem;
use super::slots::Slots;
use crate::base::{ErrCode, clock, lock, log};
use crate::hooks::manifest::Hook;
use crate::hooks::pipeline::HookCall;
use crate::hooks::traits::CacheStore;
use crate::meta::attrs::CONSISTENCY_STRONG;
use crate::meta::schema::records::F_INLINE;
use crate::proto::cap;
use crate::proto::meta::{AttrOut, MetaOp, MetaReply, O_ACCMODE, O_RDONLY, O_RDWR, O_WRONLY, OpenOut};

/// 一个打开的文件句柄。
#[derive(Clone, Copy, Debug)]
pub struct Handle {
    pub ino: u64,
    pub read: bool,
    pub write: bool,
    pub direct: bool, // FOPEN_DIRECT_IO：strong 或 O_DIRECT（8.3）
    pub sync: bool,   // O_SYNC / O_DSYNC：每次 write 写到全部副本并落盘、提交长度才返回
    pub session: u64, // 打开时的会话号（M10：不是现在的 = 被驱逐的会话里打开的；经它的请求只在这个会话里发）
    pub strong: bool, // M12：consistency=strong（读前 GetAttr，O_APPEND 用 AppendReserve，8.5）
    pub append: bool, // M12：O_APPEND（打开时的；FUSE 的 write 按每次 WRITE 带的标志换掉，[`Handle::with_write_flags`]）
}

impl Handle {
    /// strong 的 `O_APPEND`：偏移由 `AppendReserve` 分（`strong.rs`）。
    pub fn strong_append(&self) -> bool {
        self.strong && self.append
    }

    /// 按这次 write 带的打开标志算 `append`（内核每次 WRITE 给的是此刻的 f_flags：fcntl F_SETFL 改过之后也对，8.5）。
    pub fn with_write_flags(mut self, flags: u32) -> Handle {
        self.append = flags & libc::O_APPEND as u32 != 0;
        self
    }
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
    pub(super) dirs: Mutex<HashMap<u64, DirHandle>>, // 目录句柄（`dirs.rs`）
    pub(super) parents: Mutex<HashMap<u64, u64>>,    // 目录 → 它的父目录（快照里 `..` 的 inode 号，`dirs.rs`）
    pub(super) next_fh: AtomicU64,                   // 文件句柄与目录句柄共用这一个计数（不会撞号）
    pub(super) evict_hook: OnceLock<Box<dyn Fn(Vec<u64>) + Send + Sync>>, // 会话被驱逐之后（M10，`session.rs`）
    pub(super) slots: Slots,
    pub(super) jobs: Sender<(u64, u32)>, // 写满了的 chunk：交给后台写线程
    pub(super) atime: Mutex<Vec<AtimeItem>>,
    pub(super) locks: Mutex<Locks>, // M12：拿过的锁与等着的 F_SETLKW（`locks.rs`）
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
            evict_hook: OnceLock::new(),
            slots,
            jobs: tx,
            atime: Mutex::default(),
            locks: Mutex::default(),
        });
        super::flusher::start_threads(&fc, rx);
        super::session::start_threads(&fc);
        fc
    }

    /// 打开一个已有的文件（只读请求，3.3.4）；`flags` 是内核给的打开标志。
    pub fn open(&self, who: &Caller, ino: u64, flags: u32) -> Result<Opened, ErrCode> {
        self.meta.gate()?; // 先过自停闸门（可能就在这里发现被驱逐、换了新会话），再记下会话号，只在这个会话里打开
        let sid = self.meta.session();
        let o = expect_open_out(self.meta.read_in(sid, who, MetaOp::Open { ino, flags: flags & O_ACCMODE }))?;
        let o = self.strong_uninline(who, sid, o, flags)?;
        Ok(self.register(who, o, flags, false, sid))
    }

    /// strong 文件以写方式打开、而它还是 inline → 先请服务端转出（8.5：strong 的写路径与 AppendReserve 不处理 inline 文件；M12），
    /// 换上 Uninline 的回复（新 layout 与写 cap，能读时也带读权限）。
    fn strong_uninline(&self, who: &Caller, sid: u64, o: OpenOut, flags: u32) -> Result<OpenOut, ErrCode> {
        let write = flags & O_ACCMODE != O_RDONLY;
        if !write || o.attr.consistency != CONSISTENCY_STRONG || o.attr.flags & F_INLINE == 0 {
            return Ok(o);
        }
        expect_open_out(self.meta.read_in(sid, who, MetaOp::Uninline { ino: o.attr.ino }))
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
        self.meta.gate()?;
        let sid = self.meta.session();
        let o = expect_open_out(self.meta.write_in(sid, who, op))?;
        let o = self.strong_uninline(who, sid, o, flags)?; // 打开的是已有的 inline 文件时
        Ok(self.register(who, o, flags, true, sid))
    }

    /// open / create 的回复 → 句柄（`sid` = 打开它的会话）。
    fn register(&self, who: &Caller, o: OpenOut, flags: u32, created: bool, sid: u64) -> Opened {
        let ino = o.attr.ino;
        let inode = self.inode_entry(&o);
        let acc = flags & O_ACCMODE;
        let (read, write) = (acc != O_WRONLY, acc != O_RDONLY);
        let strong = o.attr.consistency == CONSISTENCY_STRONG;
        let direct = flags & libc::O_DIRECT as u32 != 0 || strong;
        let sync = flags & (libc::O_SYNC | libc::O_DSYNC) as u32 != 0;
        let append = flags & libc::O_APPEND as u32 != 0;
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
            self.meta.note(ino, Some(w)); // 不拿着 inode 的锁（心跳线程是先 delta 后 inode）
        }
        if !keep_cache && !created {
            self.drop_cached_blocks(ino);
        }
        if o.atime_due {
            lock(&self.atime).push(AtimeItem { ino, at: clock::wall_ts(), who: who.clone() });
        }
        let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
        lock(&self.handles).insert(fh, Handle { ino, read, write, direct, sync, session: sid, strong, append });
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

    /// 句柄 `fh` 的信息（不存在 → `EBADF`）：先过自停闸门（0.8 × lease 之后先等一次续租结果，8.7）；被驱逐的会话里打开的 → `EIO`
    /// （即使数据在本地缓存里，3.4.4）。
    pub fn handle(&self, fh: u64) -> Result<Handle, ErrCode> {
        self.meta.gate()?;
        let h = lock(&self.handles).get(&fh).copied().ok_or(ErrCode::EBADF)?;
        if h.session == self.meta.session() { Ok(h) } else { Err(ErrCode::EIO) }
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
            self.meta.note(h.ino, None);
        } else if h.write && writers == 0 {
            self.meta.note(h.ino, Some(false));
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
    /// → 原样回 `EACCES`（8.7 的 M7 注记：与 ADR-0036 的"其他用户之后可能 EACCES"一致；M6 把它映射成 `ESTALE`）。只在会话 `sid`
    /// 里发（句柄或这批数据所属的会话，M10 审查）。
    pub(super) fn reopen(&self, inode: &Inode, who: &Caller, sid: u64, how: Reopen) -> Result<(), ErrCode> {
        let ino = inode.ino;
        let r = match how {
            Reopen::Handle => self.meta.read_in(sid, who, MetaOp::ReopenHandle { ino, write: true }),
            Reopen::Read => self.meta.read_in(sid, who, MetaOp::ReopenHandle { ino, write: false }),
            Reopen::Path => match self.meta.read_in(sid, who, MetaOp::Open { ino, flags: O_RDWR }) {
                Err(ErrCode::EACCES) => self.meta.read_in(sid, who, MetaOp::Open { ino, flags: O_WRONLY }),
                r => r,
            },
        };
        let o = expect_open_out(r)?;
        lock(&inode.st).install(&o);
        Ok(())
    }

    /// `STALE_LAYOUT` 之后（C4）：写一行日志，再 [`reopen`](Self::reopen)。
    pub(super) fn stale(&self, inode: &Inode, who: &Caller, sid: u64, how: Reopen) -> Result<(), ErrCode> {
        let what = if how == Reopen::Read { "读" } else { "重写" };
        log::info("client", format_args!("ino {}：layout 变了（STALE_LAYOUT），重新取 layout 再{what}", inode.ino));
        self.reopen(inode, who, sid, how)
    }

    /// 带写（或读）权限的 cap；用到 80% 了先 `RenewCap`。
    fn cap_for(&self, inode: &Inode, who: &Caller, sid: u64, write: bool) -> Result<Vec<u8>, ErrCode> {
        let c = lock(&inode.st).clone_cap(write);
        if !c.bytes.is_empty() && clock::wall_ms() < c.renew_ms {
            return Ok(c.bytes);
        }
        self.renew(inode, who, sid, write)
    }

    /// `RenewCap`（经已打开的句柄，ADR-0036；只在会话 `sid` 里发），换上新 cap。续回来的 cap 的 epoch 与手上的 layout 不同 →
    /// layout 已经变了：不换，回 `STALE_LAYOUT`（调用方重新 Open 取新 layout）。
    fn renew(&self, inode: &Inode, who: &Caller, sid: u64, write: bool) -> Result<Vec<u8>, ErrCode> {
        let MetaReply::Cap { cap: b } = self.meta.read_in(sid, who, MetaOp::RenewCap { ino: inode.ino, write })? else {
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
    /// （3.3.7、8.7）。读、写、截断、SyncChunks 都经它。`sid` = 这个请求所属的会话（句柄的、这批数据的）：它已被驱逐 →
    /// `SESSION_EVICTED`，不发（M10 审查）。发之前还核对 cap 里的会话号（ADR-0041 第 9 条）：不是 `sid` 的（核对 `sid` 之后换了会话、
    /// 新会话装上了新 cap；或旧会话的 Open 回复在换会话之后才装上）→ 先在 `sid` 里续一张——`sid` 已被驱逐就续不到
    /// （`SESSION_EVICTED`），旧会话的数据不会拿新会话的 cap 写出去。数据节点回 `SESSION_EVICTED`（它先知道了驱逐）→ 也续一次：
    /// 续不到，元数据服务的回复顺便触发换新会话。
    pub(super) fn with_cap<T>(
        &self,
        inode: &Inode,
        who: &Caller,
        sid: u64,
        write: bool,
        mut f: impl FnMut(&[u8]) -> Result<T, ErrCode>,
    ) -> Result<T, ErrCode> {
        self.meta.gate()?; // 自停：0.8 × lease 之后的数据请求先等续租（3.4.4）
        if sid != self.meta.session() {
            return Err(ErrCode::SESSION_EVICTED);
        }
        let mut cap = self.cap_for(inode, who, sid, write)?;
        if cap::peek(&cap)?.session != sid {
            cap = self.renew(inode, who, sid, write)?; // 只用这个会话自己的 cap（见上）
        }
        match f(&cap) {
            Err(ErrCode::BAD_CAP | ErrCode::SESSION_EVICTED) => {
                let cap = self.renew(inode, who, sid, write)?;
                f(&cap).map_err(|e| if e == ErrCode::BAD_CAP { ErrCode::EACCES } else { e })
            }
            r => r,
        }
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
