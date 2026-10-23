//! GC 记录（键空间 0x0D，design.md 3.2.7）的元数据一侧：给 gc 任务（`plugins/gc`）分页列出记录、为每条记录要删的 (epoch, 链)
//! 签管理 cap、处理完之后的点删除批次（3.5.6、3.4.2.9、4.12 gc 的 leader 部分）。
//!
//! 扫描在只读路径上做（线性一致读之后的一个读事务）；删除是带前提的内部批次（CAS：GC 记录还是同一条、inode 还是孤儿），重复执行
//! 无害（3.4.1.4 规则 6），所以 gc 的一步可以放心重放（3.10 的幂等要求）。
//! - kind 2（废弃 epoch）：对记录里的每条链 `DeleteEpoch`，然后删记录。
//! - kind 1（孤儿 inode）：对 inode **当前** layout 的每条链 `DeleteEpoch`（孤儿被截断到 0 过的话 epoch 已经换了，旧 epoch 另有
//!   kind 2 记录），然后一个批次删 inode、XATTR、INLINE、LOCKS 与这条记录。"能不能删"（3.5.6 的三个条件）由 gc 任务判断——
//!   leader 内存里的打开集合在 `sessions::Live`。实现注记（M5）：迁移中的 `pending_epoch`（M11）不在 inode 里记链，到时由迁移原语
//!   自己的 GC 记录（kind 2）负责。

use crate::base::ErrCode;
use crate::base::types::ReqCtx;
use crate::meta::engine::txn::{KvScan, MetaEngine, OpCtx, TxnOp, txn_blocking};
use crate::meta::schema::keys::{self, GC_EPOCH, GC_ORPHAN, T_GC, T_SESSION};
use crate::meta::schema::kv;
use crate::meta::schema::records::{F_ORPHAN, GcRecV1, InodeV1, SessionRecV1, decode_v1};
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
    // after 后面补一个 0 字节 = 严格大于 after 的第一个 key（游标 after 本身已经处理过）
    let start = if after.is_empty() { lo } else { [after, &[0]].concat() };
    let rows = v.scan(&start, &hi, limit + 1)?;
    let more = rows.len() > limit;
    let mut items = Vec::new();
    for (key, b) in rows.into_iter().take(limit) {
        let (kind, ino, epoch) = keys::parse_gc(&key).ok_or(ErrCode::INTERNAL)?;
        let rec: GcRecV1 = decode_v1(&b)?;
        let deletes = match kind {
            GC_EPOCH => deletes(c, ino, epoch, &rec.chains),
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
