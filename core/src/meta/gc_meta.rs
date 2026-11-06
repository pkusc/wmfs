//! GC 记录（键空间 0x0D，design.md 3.2.7）的元数据一侧：给 gc 任务（`plugins/gc`）分页列出记录、为每条记录要删的 (epoch, 链)
//! 签管理 cap、处理完之后的点删除批次（3.5.6、3.4.2.9、4.12 gc 的 leader 部分）。
//!
//! 扫描在只读路径上做（线性一致读之后的一个读事务）；删除是带前提的内部批次（CAS：GC 记录还是同一条、inode 还是孤儿），重复执行
//! 无害（3.4.1.4 规则 6），所以 gc 的一步可以放心重放（3.10 的幂等要求）。
//! - kind 2（废弃 epoch）与 kind 3（迁移换下来的旧 epoch，M11）：对记录里的每条链 `DeleteEpoch`，然后删记录（两种各等什么由 gc 任务判断）。
//! - kind 1（孤儿 inode）：对 inode **当前** layout 的每条链 `DeleteEpoch`（孤儿被截断到 0 过的话 epoch 已经换了，旧 epoch 另有
//!   kind 2 记录），然后一个批次删 inode、XATTR、INLINE、LOCKS 与这条记录。"能不能删"（3.5.6 的三个条件）由 gc 任务判断——
//!   leader 内存里的打开集合在 `sessions::Live`。实现注记（M5）：迁移中的 `pending_epoch`（M11）不在 inode 里记链，到时由迁移原语
//!   自己的 GC 记录（kind 2）负责。
//! - 实现注记（M10）：EVICTED 记录（0x1B）保留 24 h 之后删掉（[`expire_evicted`]，gc 每一遍扫完时；同批 `map_epoch += 1`：它在集群图里）。
//! - 实现注记（M11）：**去重记录的清理**（[`dead_dedup`]，ADR-0041 第 9 条）——会话记录已经不在了（关闭了、两阶段驱逐做完了）的会话，它的
//!   DEDUP 记录再也不会被查到（之后带这个会话号的请求一律 `SESSION_EVICTED`），分批点删；**changelog 修剪**（[`trim_changelog`]，3.11）——
//!   保留全部消费者游标的最小值之后的记录，但最多 7 天 / 1000 万条：最旧的一页里被所有游标消费过的（有游标时才算这一条：没有消费者时只按
//!   7 天 / 1000 万条修剪，`wmfs changelog tail` 仍看得到最近的历史）、超过 7 天的、比最新的旧 1000 万条以上的，点删；之后读它们得到显式的
//!   `410 Gone`。

use crate::base::ErrCode;
use crate::base::types::ReqCtx;
use crate::meta::changelog::last_seq;
use crate::meta::engine::txn::{KvScan, MetaEngine, OpCtx, TxnOp, txn_blocking};
use crate::meta::evict::EVICTED_KEEP_MS;
use crate::meta::schema::keys::{
    self, GC_EPOCH, GC_MOVED, GC_ORPHAN, T_CHANGELOG, T_CURSOR, T_DEDUP, T_EVICTED, T_GC, T_SESSION,
};
use crate::meta::schema::kv;
use crate::meta::schema::records::{
    ChangeRecV1, CursorV1, EvictedV1, F_ORPHAN, GcRecV1, InodeV1, SessionRecV1, decode_v1,
};
use crate::meta::sessions::SESSION_ACTIVE;
use crate::proto::cap::{self, CapV1, RIGHT_ADMIN};
use crate::proto::meta::MetaReply;

/// 一条 GC 记录与它要删的东西。
pub struct GcItem {
    pub key: Vec<u8>, // 记录的 key（gc 任务的游标就是它）
    pub kind: u8,
    pub ino: u64,
    pub epoch: u32,
    pub rec: GcRecV1,
    pub deletes: Vec<Delete>,
}

/// 对一条链的一个 (ino, epoch) 发 `DeleteEpoch`，带一张管理 cap（只签发给后台任务，3.3.5）。
pub struct Delete {
    pub epoch: u32,
    pub chain: u32,
    pub cap: Vec<u8>,
}

/// 一页 GC 记录，以及全部存活（Active）会话——孤儿 GC 的条件 ③ 要它（3.5.6）。
pub struct GcPage {
    pub items: Vec<GcItem>,
    pub more: bool,
    pub sessions: Vec<u64>,
}

/// 从 `after`（不含）之后列出最多 `limit` 条 GC 记录（线性一致读）。
pub fn scan(eng: &dyn MetaEngine, ctx: &ReqCtx, after: &[u8], limit: usize) -> Result<GcPage, ErrCode> {
    let mut page = None;
    // 这个闭包只用来在 ReadIndex 之后的视图上取出这一页，它的返回值丢弃
    eng.read(ctx, &mut |v, c| {
        page = Some(scan_view(v, c, after, limit)?);
        Ok(MetaReply::Ok)
    })?;
    page.ok_or(ErrCode::INTERNAL)
}

fn scan_view(v: &dyn KvScan, c: &OpCtx, after: &[u8], limit: usize) -> Result<GcPage, ErrCode> {
    let (lo, hi) = keys::tag_range(T_GC);
    let start = if after.is_empty() { lo } else { keys::after(after) }; // 游标 after 本身已经处理过
    let rows = v.scan(&start, &hi, limit + 1)?;
    let more = rows.len() > limit;
    let mut items = Vec::new();
    for (key, b) in rows.into_iter().take(limit) {
        let (kind, ino, epoch) = keys::parse_gc(&key).ok_or(ErrCode::INTERNAL)?;
        let rec: GcRecV1 = decode_v1(&b)?;
        let deletes = match kind {
            GC_EPOCH | GC_MOVED => deletes(c, ino, epoch, &rec.chains),
            GC_ORPHAN => match kv::get::<InodeV1>(v, &keys::inode(ino))?.and_then(|i| i.layout) {
                Some(l) => deletes(c, ino, l.epoch, &l.chains),
                None => vec![],
            },
            _ => vec![],
        };
        items.push(GcItem { key, kind, ino, epoch, rec, deletes });
    }
    let active = |b: &Vec<u8>| decode_v1::<SessionRecV1>(b).is_ok_and(|s| s.state == SESSION_ACTIVE);
    let sessions = kv::scan_tag(v, T_SESSION)?.iter().filter(|(_, b)| active(b)).map(|(k, _)| kv::key_id(k)).collect();
    Ok(GcPage { items, more, sessions })
}

fn deletes(c: &OpCtx, ino: u64, epoch: u32, chains: &[u32]) -> Vec<Delete> {
    let cap = CapV1 {
        ino,
        epoch,
        rights: RIGHT_ADMIN,
        session: 0,
        tenant: 0,
        job_id: 0,
        chains: chains.to_vec(),
        class_id: 0,
        qos_id: 0,
        chunk_size: 0,
        block_size: 0,
        checksum: 0,
        compress: 0,
        compress_level: 0,
        encrypt: 0,
        encrypt_key: 0,
        expires_ms: c.req.now_ms.saturating_add(c.settings.cap_ttl_ms),
    };
    let cap = cap::sign(c.cap_key, &cap);
    let mut uniq = chains.to_vec();
    uniq.sort();
    uniq.dedup();
    uniq.into_iter().map(|chain| Delete { epoch, chain, cap: cap.clone() }).collect()
}

/// 删掉一条处理完的 GC 记录（还是同一条——`created_ms` 没变——才删）。
pub fn drop_record(eng: &dyn MetaEngine, item: &GcItem) -> Result<(), ErrCode> {
    let (key, created) = (item.key.clone(), item.rec.created_ms);
    let op: TxnOp = Box::new(move |v, _c| {
        if kv::get::<GcRecV1>(v, &key)?.is_some_and(|r| r.created_ms == created) {
            v.delete(&key);
        }
        Ok(MetaReply::Ok)
    });
    txn_blocking(eng, ReqCtx::internal(), op).map(|_| ())
}

/// 孤儿的最后一步（3.5.6）：inode 还是孤儿（F_ORPHAN、nlink 0）就删 inode、XATTR、INLINE、LOCKS，再删 GC 记录。
pub fn finish_orphan(eng: &dyn MetaEngine, item: &GcItem) -> Result<(), ErrCode> {
    let (ino, key) = (item.ino, item.key.clone());
    let op: TxnOp = Box::new(move |v, _c| {
        if let Some(i) = kv::get::<InodeV1>(v, &keys::inode(ino))? {
            if i.flags & F_ORPHAN == 0 || i.nlink != 0 {
                return Ok(MetaReply::Ok); // 不再是孤儿（不应发生）：什么也不删，记录留着人看
            }
            for k in [keys::inode(ino), keys::xattr(ino), keys::inline(ino), keys::locks(ino)] {
                v.delete(&k);
            }
        }
        v.delete(&key);
        Ok(MetaReply::Ok)
    });
    txn_blocking(eng, ReqCtx::internal(), op).map(|_| ())
}

/// 删掉驱逐已超过 24 h 的 EVICTED 记录（3.4.4 第 4 步）：先线性一致地读出它们，再一个批次点删（还是同一条才删），同批
/// `map_epoch += 1`。返回删了几条。
pub fn expire_evicted(eng: &dyn MetaEngine, now_ms: u64) -> Result<usize, ErrCode> {
    let mut old = Vec::new();
    eng.read(&ReqCtx::internal(), &mut |v, _c| {
        let rows = kv::scan_tag(v, T_EVICTED)?;
        let due = |b: &Vec<u8>| decode_v1::<EvictedV1>(b).is_ok_and(|e| e.evicted_ms + EVICTED_KEEP_MS < now_ms);
        old = rows.into_iter().filter(|(_, b)| due(b)).collect();
        Ok(MetaReply::Ok)
    })?;
    if old.is_empty() {
        return Ok(0);
    }
    let n = old.len();
    let op: TxnOp = Box::new(move |v, _c| {
        for (k, b) in &old {
            if v.get(k)?.as_ref() == Some(b) {
                v.delete(k);
            }
        }
        kv::bump_map_epoch(v)?;
        Ok(MetaReply::Ok)
    });
    txn_blocking(eng, ReqCtx::internal(), op).map(|_| n)
}

/// changelog 的保留上限（3.11）。
const KEEP_MS: u64 = 7 * 24 * 3600 * 1000;
const KEEP_RECORDS: u64 = 10_000_000;

/// [`dead_dedup`] 的一页：要删的键、这一页最后一个键、后面还有没有。
pub struct DedupPage {
    pub dead: Vec<Vec<u8>>,
    pub last: Vec<u8>,
    pub more: bool,
}

/// `after`（不含；空 = 从头）之后最多 `limit` 条 DEDUP 里，会话记录已经不在了的那些（见文件头）。
pub fn dead_dedup(eng: &dyn MetaEngine, after: &[u8], limit: usize) -> Result<DedupPage, ErrCode> {
    let mut out = None;
    eng.read(&ReqCtx::internal(), &mut |v, _c| {
        let (lo, hi) = keys::tag_range(T_DEDUP);
        let start = if after.is_empty() { lo } else { keys::after(after) };
        let rows = v.scan(&start, &hi, limit)?;
        let (mut dead, mut known): (Vec<Vec<u8>>, Vec<(u64, bool)>) = (vec![], vec![]);
        for (k, _) in &rows {
            let s = kv::key_id(k); // 0C session seq：会话号紧跟标签
            let gone = match known.iter().find(|(x, _)| *x == s) {
                Some((_, g)) => *g,
                None => {
                    let g = v.get(&keys::session(s))?.is_none();
                    known.push((s, g));
                    g
                }
            };
            if gone {
                dead.push(k.clone());
            }
        }
        let last = rows.last().map(|(k, _)| k.clone()).unwrap_or_default();
        out = Some(DedupPage { dead, last, more: rows.len() == limit });
        Ok(MetaReply::Ok)
    })?;
    out.ok_or(ErrCode::INTERNAL)
}

/// 点删一批键（一个内部批次；删过的再删无害）。
pub fn delete_keys(eng: &dyn MetaEngine, keys: Vec<Vec<u8>>) -> Result<(), ErrCode> {
    if keys.is_empty() {
        return Ok(());
    }
    let op: TxnOp = Box::new(move |v, _c| {
        for k in &keys {
            v.delete(k);
        }
        Ok(MetaReply::Ok)
    });
    txn_blocking(eng, ReqCtx::internal(), op).map(|_| ())
}

/// changelog 修剪（见文件头）：最旧的最多 `limit` 条里该删的那一段（从最旧的起、到第一条要留的为止）；回 (删了几条, 还有没有要删的)。
pub fn trim_changelog(eng: &dyn MetaEngine, now_ms: u64, limit: usize) -> Result<(usize, bool), ErrCode> {
    let mut cut = vec![];
    eng.read(&ReqCtx::internal(), &mut |v, _c| {
        let last = last_seq(v)?;
        let cursors = kv::scan_tag(v, T_CURSOR)?;
        let mut min_cursor = None;
        for (_, b) in &cursors {
            let c: CursorV1 = decode_v1(b)?;
            min_cursor = Some(min_cursor.map_or(c.seq, |m: u64| m.min(c.seq)));
        }
        let (lo, hi) = keys::tag_range(T_CHANGELOG);
        cut.clear();
        for (k, b) in v.scan(&lo, &hi, limit)? {
            let (seq, r): (u64, ChangeRecV1) = (kv::key_id(&k), decode_v1(&b)?);
            let consumed = min_cursor.is_some_and(|m| seq <= m);
            let too_many = seq.saturating_add(KEEP_RECORDS) <= last;
            let too_old = r.time_ms.saturating_add(KEEP_MS) < now_ms;
            if !consumed && !too_many && !too_old {
                break; // 第一条要留的：之后的更新，都留着
            }
            cut.push(k);
        }
        Ok(MetaReply::Ok)
    })?;
    let (n, more) = (cut.len(), cut.len() == limit);
    delete_keys(eng, cut)?;
    Ok((n, more))
}
