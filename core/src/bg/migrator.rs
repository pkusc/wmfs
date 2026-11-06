//! 迁移原语 `Migrator::migrate(ino, declared)`（design.md 3.10 的六步，ADR-0027；实现注记（M11））：协调器、以后的 tiering、rebalance
//! 共用。把一个已有文件换到 `declared`（它的生效属性）描述的布局：新 epoch、按声明的存储类、副本数选的新链、按声明的压缩与加密编码。
//!
//! 1. 线性一致地读出 inode（layout L、change_id c、size s）。与声明没有差别（`status::differs`）→ `Same`；不是有链的普通文件、孤儿 →
//!    `Same`（孤儿就要被删了）。本进程没在迁移它、它却留着 `pending_epoch`（换 leader 或重启之前没做完的迁移；协调器碰到这样的非 inline
//!    文件，声明一致了也调 `migrate`，ADR-0042 第 10 条）而声明已经一致了 → 放弃那个 epoch（[`drop_stale`]：记 GC 记录、清字段；
//!    不然它一直被当成活的，写了一半的数据永不回收）；还不一致的由第 2 步一并处理。leader 内存的打开集合里有人写打开着 →
//!    `SkippedOpen`，并记下它第一次被跳过的时刻（换 leader 后重新计时）：被跳过超过 24 h（测试用 fail point `bg.migrate.open_alert_ms`
//!    的 `return(<毫秒>)` 缩短）→ 写一次 `PolicyViolation`（`kind=migration_blocked_by_open`）。被跳过的个数是指标
//!    `wmfs_migration_skipped_open_files`（协调器每一轮开头把已经不再写打开着的清掉，[`Migrator::prune`]；失去 leader 时清零）。
//! 2. **意图**（一个内部批次，CAS：c 与 L.epoch 都没变）：别人留下的 `pending_epoch` 记为垃圾；`max_epoch += 1`、
//!    `pending_epoch = 新 epoch`；同一个闭包里按声明生成 L′（`ns::layout::new_layout`，`size_hint = s`；必要时生成新链）。
//!    **不迁移**的参数沿用 L 的：chunk 大小、条带数、校验算法（第 5 章）；inline 阈值用声明的（经 `ns::layout::inline_of`：加密或
//!    strong 时为 0），不沿用 L 的——不然加密之后再改回不加密，文件永远是 `inline_max = 0`（ADR-0042 第 11 条）。顺便签两张后台用的
//!    cap（管理权限位、session 0）：读 L、写 L′。只与这一个文件有关的错（候选 target 都满了的 `ENOSPC`、存储类没了、`EOVERFLOW`）→
//!    写日志、回 `Retry`（协调器把这一项留着、计入积压），不挡住遍历；集群层面可重试的错（失去 leader 等）才让这一步失败。
//! 3. inline 文件：声明仍允许 inline → 只换元数据，直接到第 5 步；不允许（加密生效）→ 把 INLINE 内容按 L′ 写进 chunk 0
//!    （`meta::uninline::write_inline`，与服务端 uninline 共用），第 5 步同时清掉 F_INLINE 与 INLINE。
//! 4. 其余文件逐 chunk：按 L 读整个 chunk（没写过的 chunk 跳过，新布局里它也是洞）→ 按 L′ 写（新 head 按新的变换编码）→ 对 L′ 的每条
//!    链 `SyncChunks`（换布局之前必须持久）。两张 cap 的有效期过了一半，就在一个只读闭包里核对 c、L.epoch、`pending_epoch` 都还是这次的、
//!    重签两张再接着拷（[`renew`]）：要拷很久的大文件不会因为 cap 过期而永远迁不完。fail point `bg.migrate.before_switch`（数据都写好、
//!    切换之前）。
//! 5. **切换**（CAS：c、L.epoch、`pending_epoch` 都还是这次的，切换之前再看一眼没有写打开）：layout = L′、class_id、清
//!    `pending_epoch`、旧 epoch 写 GC 记录（kind 3 `GC_MOVED`：除了宽限期，还要等没人打开着这个文件才删——拿着旧 layout
//!    的读者照样读得到，ADR-0042 第 5 条）、per_class 计数搬到新类、changelog `SwitchLayout`。内容没变，所以 change_id 与 ctime
//!    不变（客户端的页缓存照样有效；拿着旧 layout 的读写按 C4/C5 刷新）。
//! 6. CAS 失败（期间有写入、截断、uninline）或第 3/4 步失败 → 新 epoch 记为垃圾（GC 记录带 L′ 的链）、还是自己的就清掉
//!    `pending_epoch`，回 `Retry`（协调器下一轮重做）。已经切换成这个 epoch 了（切换的回复丢了）就什么也不做。正在写的客户端随后的
//!    `CommitWrite` 得到 `STALE_LAYOUT`，把缓冲重写到新布局——数据不会丢。

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use wmfs_api::failpoint::{fail_point, fail_point_ms};
use wmfs_api::metrics::{self, Counter, Gauge};

use super::status::differs;
use super::task::TaskCtx;
use crate::base::types::{ChunkId, ReqCtx};
use crate::base::{ErrCode, clock, lock, log};
use crate::buf::{Lane, POOL_WAIT};
use crate::client::data_client::{DataClient, chunk_id};
use crate::meta::attrs::EffectiveAttrs;
use crate::meta::changelog::{self, Change};
use crate::meta::engine::txn::{KvRead, MetaEngine, OpCtx, TxnOp, txn_blocking, txn_value};
use crate::meta::ns::common::{Node, get, save, sign_cap};
use crate::meta::ns::file_ops::{gc_epoch, gc_record};
use crate::meta::ns::layout::new_layout;
use crate::meta::rstat::{self, Delta};
use crate::meta::schema::keys::{self, GC_MOVED};
use crate::meta::schema::records::{F_INLINE, KIND_FILE, LayoutV1};
use crate::meta::sessions::Live;
use crate::meta::uninline::write_inline;
use crate::net::retry::retryable;
use crate::proto::cap::{RIGHT_ADMIN, RIGHT_READ, RIGHT_WRITE};
use crate::proto::meta::MetaReply;

/// 跳过多久之后告警（3.10："某个文件被跳过超过 24 h"）。
const OPEN_ALERT_MS: u64 = 24 * 3600 * 1000;

/// 一次迁移的结果。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Migrated {
    Done { bytes: u64 },
    Same,
    SkippedOpen,
    Retry,
}

/// 第 2 步之后：新 epoch、新布局、新的存储类与两张 cap（过了 `renew_at`（墙钟毫秒）就重签，见文件头第 4 步）。
struct Intent {
    epoch: u32,
    new: LayoutV1,
    class: u16,
    caps: Caps,
}

struct Caps {
    read: Vec<u8>,
    write: Vec<u8>,
    renew_at: u64,
}

/// 本任期里因写打开被跳过的文件（换 leader 后重新计时）；`shown` 是已经加进指标的个数（[`Migrator::show`]）。
#[derive(Default)]
struct Skipped {
    term: u64,
    files: HashMap<u64, SkipInfo>,
    shown: usize,
}

#[derive(Clone, Copy)]
struct SkipInfo {
    since_ms: u64, // 第一次被跳过的时刻（mono_ms）
    alerted: bool, // 告警过了吗
}

pub struct Migrator {
    skipped: Mutex<Skipped>,
    skipped_gauge: Arc<Gauge>,
    done: Arc<Counter>,
    retried: Arc<Counter>,
}

impl Default for Migrator {
    fn default() -> Migrator {
        Migrator::new()
    }
}

/// 让这一步失败的错（集群层面可重试的：失去 leader 等）；其余的只与这一个文件有关（见文件头第 2 步）。
fn stop(e: ErrCode) -> bool {
    retryable(e)
}

impl Migrator {
    pub fn new() -> Migrator {
        let c = |r| metrics::counter("wmfs_migrations_total", "迁移原语换布局的次数", &[("result", r)]);
        Migrator {
            skipped: Mutex::default(),
            skipped_gauge: metrics::gauge("wmfs_migration_skipped_open_files", "因写打开被迁移跳过的文件数", &[]),
            done: c("done"),
            retried: c("retry"),
        }
    }

    /// 把 `ino` 迁到 `declared` 描述的布局（见文件头）。
    pub fn migrate(&self, ctx: &TaskCtx, ino: u64, declared: &EffectiveAttrs) -> Result<Migrated, ErrCode> {
        let mut seen = None;
        ctx.meta.read(&ReqCtx::internal(), &mut |v, _c| {
            let n = get(v, ino)?;
            let content =
                if n.rec.flags & F_INLINE != 0 { v.get(&keys::inline(ino))?.unwrap_or_default() } else { vec![] };
            seen = Some((n, content));
            Ok(MetaReply::Ok)
        })?;
        let (n, content) = seen.ok_or(ErrCode::INTERNAL)?;
        let movable = n.rec.kind == KIND_FILE && n.rec.nlink > 0 && n.rec.layout.is_some();
        if !movable || !differs(&n.rec, declared) {
            self.forget(ino);
            let stale = movable && n.rec.pending_epoch != 0 && n.rec.flags & F_INLINE == 0;
            if stale
                && let Err(e) = drop_stale(ctx.meta, &n)
                && stop(e)
            {
                return Err(e);
            }
            return Ok(Migrated::Same);
        }
        if ctx.live.is_write_open(ino) {
            self.note_skip(ctx.meta, ino);
            return Ok(Migrated::SkippedOpen);
        }
        let mut it = match intent(ctx.meta, &n, declared) {
            Ok(it) => it,
            Err(ErrCode::STALE_LAYOUT) => return Ok(Migrated::Retry), // 读完之后有人改了它
            Err(e) if stop(e) => return Err(e),
            Err(e) => {
                log::warn("bg", format_args!("迁移 inode {ino} 现在做不了：{e}（这一项留着，稍后再试）"));
                return Ok(Migrated::Retry);
            }
        };
        let r = copy(ctx.meta, ctx.data, &n, &content, &mut it).and_then(|bytes| {
            fail_point("bg.migrate.before_switch").map_err(ErrCode)?;
            if ctx.live.is_write_open(ino) {
                return Err(ErrCode::STALE_LAYOUT); // 期间有人写打开了：这一次作废（第 5 步的"仍没有写打开"）
            }
            switch(ctx.meta, &n, &it).map(|()| bytes)
        });
        match r {
            Ok(bytes) => {
                self.forget(ino);
                self.done.inc();
                Ok(Migrated::Done { bytes })
            }
            Err(e) => {
                log::info("bg", format_args!("迁移 inode {ino} 没成：{e}（新 epoch {} 记为垃圾，稍后重做）", it.epoch));
                self.retried.inc();
                abandon(ctx.meta, ino, &it)?;
                Ok(Migrated::Retry)
            }
        }
    }

    /// 已经不再写打开着的（关了、删了）跳过记录清掉（协调器每一轮开头调用，见文件头第 1 步）。
    pub fn prune(&self, live: &Live) {
        let mut s = lock(&self.skipped);
        s.files.retain(|ino, _| live.is_write_open(*ino));
        self.show(&mut s);
    }

    /// 不再是 leader：跳过记录全部清掉（指标归 0；新 leader 自己重新计）。
    pub fn forget_all(&self) {
        let mut s = lock(&self.skipped);
        (s.term, s.files) = (0, HashMap::new());
        self.show(&mut s);
    }

    fn forget(&self, ino: u64) {
        let mut s = lock(&self.skipped);
        if s.files.remove(&ino).is_some() {
            self.show(&mut s);
        }
    }

    /// 把跳过的个数报进指标。指标是进程级的：一个进程里跑着几个节点时（测试），各自 `set` 会互相盖掉（谁最后写算谁的），所以
    /// 只加减自己的变化量，指标是本进程各节点之和（生产上一个进程一个节点：就是它自己的个数）。
    fn show(&self, s: &mut Skipped) {
        self.skipped_gauge.add(s.files.len() as f64 - s.shown as f64);
        s.shown = s.files.len();
    }

    /// 记下"因写打开被跳过"（见文件头第 1 步）；到了告警的时候写一次 `PolicyViolation`。
    fn note_skip(&self, eng: &dyn MetaEngine, ino: u64) {
        let (term, now) = (eng.role().term, clock::mono_ms());
        let alert = fail_point_ms("bg.migrate.open_alert_ms", OPEN_ALERT_MS);
        let mut s = lock(&self.skipped);
        if s.term != term {
            (s.term, s.files) = (term, HashMap::new()); // 换了 leader：重新计时
        }
        let e = s.files.entry(ino).or_insert(SkipInfo { since_ms: now, alerted: false });
        let (since, due) = (e.since_ms, !e.alerted && now.saturating_sub(e.since_ms) >= alert);
        e.alerted |= due;
        self.show(&mut s);
        drop(s);
        if due {
            let extra = vec![
                ("kind".into(), "migration_blocked_by_open".into()),
                ("skipped_ms".into(), now.saturating_sub(since).to_string()),
            ];
            let op: TxnOp = Box::new(move |v, c| {
                changelog::append(v, c, Change { op: "PolicyViolation", ino, extra, ..Default::default() })?;
                Ok(MetaReply::Ok)
            });
            if let Err(e) = txn_blocking(eng, ReqCtx::internal(), op) {
                log::warn("bg", format_args!("inode {ino} 一直写打开、迁移被跳过：告警事件没写成（{e}）"));
            }
        }
    }
}

/// 第 1 步读到的样子还在吗（CAS）：change_id 与 epoch 都没变；返回 (inode, 它的 layout)。
fn unchanged(v: &dyn KvRead, n: &Node) -> Result<(Node, LayoutV1), ErrCode> {
    let cur = get(v, n.ino)?;
    let l = cur.rec.layout.clone().ok_or(ErrCode::STALE_LAYOUT)?;
    let old = n.rec.layout.as_ref().map_or(0, |l| l.epoch);
    if cur.rec.change_id != n.rec.change_id || l.epoch != old {
        return Err(ErrCode::STALE_LAYOUT);
    }
    Ok((cur, l))
}

/// 两张后台用的 cap（管理权限位、session 0）：读当前的 L、写 L′（见文件头第 2、4 步）。
fn caps(c: &OpCtx, cur: &Node, new: &LayoutV1, class: u16) -> Caps {
    let read = sign_cap(c, cur, RIGHT_READ | RIGHT_ADMIN, 0);
    let mut to = cur.clone();
    (to.rec.layout, to.rec.class_id) = (Some(new.clone()), class);
    let write = sign_cap(c, &to, RIGHT_READ | RIGHT_WRITE | RIGHT_ADMIN, 0);
    Caps { read, write, renew_at: c.req.now_ms.saturating_add(c.settings.cap_ttl_ms / 2) }
}

/// 第 2 步（见文件头）。
fn intent(eng: &dyn MetaEngine, n: &Node, declared: &EffectiveAttrs) -> Result<Intent, ErrCode> {
    let (n, mut eff) = (n.clone(), declared.clone());
    txn_value(eng, ReqCtx::internal(), move |v, c| {
        let (mut cur, l) = unchanged(v, &n)?;
        if cur.rec.pending_epoch != 0 {
            gc_epoch(v, c, cur.ino, cur.rec.pending_epoch, &l.chains); // 别人留下的意图（不知道它的链：真有数据的由数据节点的本地孤儿扫描清理）
        }
        let epoch = cur.rec.max_epoch.checked_add(1).ok_or(ErrCode::EOVERFLOW)?;
        // 不迁移的参数（第 5 章）沿用 L 的；inline 阈值用声明的（见文件头第 2 步）
        eff.chunk = l.chunk_size;
        eff.stripe = l.chains.len() as u16;
        eff.checksum = l.checksum;
        let new = new_layout(v, c, &eff, cur.ino, epoch, cur.rec.size)?;
        (cur.rec.max_epoch, cur.rec.pending_epoch) = (epoch, epoch);
        save(v, &cur);
        let caps = caps(c, &cur, &new, eff.class_id);
        Ok(Intent { epoch, new, class: eff.class_id, caps })
    })
}

/// 第 1 步：放弃别人留下的 `pending_epoch`（见文件头；CAS：c、L.epoch、`pending_epoch` 都没变）。GC 记录（kind 2）带的是**当前**布局的链：
/// 那个意图的 L′ 用了哪些链没有记下来（3.10 第 2 步只记 epoch），leader 的 gc 删不到 L′ 上写了一半的数据——它由数据节点的本地孤儿扫描
/// （gc-local）在这条记录删掉、目录又过了 min_age 之后清理（ADR-0042 第 10 条）。
fn drop_stale(eng: &dyn MetaEngine, n: &Node) -> Result<(), ErrCode> {
    let n = n.clone();
    txn_value(eng, ReqCtx::internal(), move |v, c| {
        let (mut cur, l) = unchanged(v, &n)?;
        if cur.rec.pending_epoch == n.rec.pending_epoch {
            gc_epoch(v, c, cur.ino, cur.rec.pending_epoch, &l.chains);
            cur.rec.pending_epoch = 0;
            save(v, &cur);
        }
        Ok(())
    })
}

/// 第 4 步里重签两张 cap（见文件头）。
fn renew(eng: &dyn MetaEngine, n: &Node, it: &mut Intent) -> Result<(), ErrCode> {
    let mut out = None;
    // 这个闭包只用来在 ReadIndex 之后的视图上核对并签名，它的返回值丢弃
    eng.read(&ReqCtx::internal(), &mut |v, c| {
        let (cur, _) = unchanged(v, n)?;
        if cur.rec.pending_epoch != it.epoch {
            return Err(ErrCode::STALE_LAYOUT); // 别人换过意图
        }
        out = Some(caps(c, &cur, &it.new, it.class));
        Ok(MetaReply::Ok)
    })?;
    it.caps = out.ok_or(ErrCode::INTERNAL)?;
    Ok(())
}

/// 第 3、4 步（见文件头）；返回搬了多少字节。
fn copy(eng: &dyn MetaEngine, data: &DataClient, n: &Node, content: &[u8], it: &mut Intent) -> Result<u64, ErrCode> {
    let old = n.rec.layout.as_ref().ok_or(ErrCode::INTERNAL)?;
    if n.rec.flags & F_INLINE != 0 {
        if it.new.inline_max > 0 {
            return Ok(0); // 仍可以 inline：只换元数据
        }
        write_inline(data, &it.caps.write, &it.new, n.ino, content)?;
        return Ok(content.len() as u64);
    }
    let (cs, mut bytes) = (old.chunk_size, 0);
    let mut written: BTreeMap<u32, Vec<ChunkId>> = BTreeMap::new();
    for idx in 0..n.rec.size.div_ceil(cs as u64) {
        let idx = u32::try_from(idx).map_err(|_| ErrCode::EFBIG)?;
        if clock::wall_ms() >= it.caps.renew_at {
            renew(eng, n, it)?;
        }
        let buf = data.pool().get(cs as usize, Lane::Normal, POOL_WAIT)?;
        let (b, len) = match data.read(&it.caps.read, old, chunk_id(old, n.ino, idx)?, 0, cs, buf, None) {
            Err(ErrCode::CHUNK_NOT_FOUND) => continue, // 从没写过的 chunk：新布局里它也是洞
            r => r?,
        };
        if len == 0 {
            continue;
        }
        let to = chunk_id(&it.new, n.ino, idx)?;
        data.write(&it.caps.write, to, vec![(0, len)], vec![b.freeze().slice(0, len)], false)?;
        written.entry(to.chain).or_default().push(to);
        bytes += len as u64;
    }
    for (chain, chunks) in written {
        data.sync(&it.caps.write, chain, chunks)?; // 换布局之前必须持久
    }
    Ok(bytes)
}

/// 第 5 步（见文件头）。
fn switch(eng: &dyn MetaEngine, n: &Node, it: &Intent) -> Result<(), ErrCode> {
    let (n, new, epoch, class) = (n.clone(), it.new.clone(), it.epoch, it.class);
    txn_value(eng, ReqCtx::internal(), move |v, c| {
        let (mut cur, l) = unchanged(v, &n)?;
        if cur.rec.pending_epoch != epoch {
            return Err(ErrCode::STALE_LAYOUT); // 别人换过意图
        }
        let before = Delta::of(v, &cur)?;
        gc_record(v, c, GC_MOVED, cur.ino, l.epoch, &l.chains); // 等没人打开着它才删（见文件头第 5 步）
        if cur.rec.flags & F_INLINE != 0 && new.inline_max == 0 {
            cur.rec.flags &= !F_INLINE;
            v.delete(&keys::inline(cur.ino));
        }
        (cur.rec.layout, cur.rec.class_id, cur.rec.pending_epoch) = (Some(new.clone()), class, 0);
        rstat::changed(v, c, &before, &cur)?; // 换了存储类：per_class 搬过去（总量不变）
        save(v, &cur);
        let extra = vec![
            ("old_epoch".into(), l.epoch.to_string()),
            ("new_epoch".into(), epoch.to_string()),
            ("replicas".into(), new.replicas.to_string()),
            ("compress".into(), new.compress.to_string()),
            ("encrypt_key".into(), new.encrypt_key.to_string()),
        ];
        changelog::append(v, c, Change { op: "SwitchLayout", ino: cur.ino, extra, ..Default::default() })
    })
}

/// 第 6 步：新 epoch 记为垃圾（带 L′ 的链）、还是自己的就清掉 `pending_epoch`；已经切换成它了（回复丢了）就什么也不做。
fn abandon(eng: &dyn MetaEngine, ino: u64, it: &Intent) -> Result<(), ErrCode> {
    let (epoch, chains) = (it.epoch, it.new.chains.clone());
    txn_value(eng, ReqCtx::internal(), move |v, c| {
        let Ok(mut cur) = get(v, ino) else {
            gc_epoch(v, c, ino, epoch, &chains); // 文件已经删了：这个 epoch 下我们写的数据也要删
            return Ok(());
        };
        if cur.rec.layout.as_ref().is_some_and(|l| l.epoch == epoch) {
            return Ok(()); // 切换其实成功了
        }
        gc_epoch(v, c, ino, epoch, &chains);
        if cur.rec.pending_epoch == epoch {
            cur.rec.pending_epoch = 0;
            save(v, &cur);
        }
        Ok(())
    })
}
