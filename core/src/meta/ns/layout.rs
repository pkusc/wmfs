//! 新文件（与截断到 0 的文件）的 layout（design.md 3.4.2.1、4.6、3.5.1 的 setattr(size = 0) 一行）：按生效属性选链、建 layout。
//! 实现注记（M7）：从 `common.rs` 搬过来（每个文件 ≤ 400 行），`common` 原样转出这两个函数。新文件的 chunk、stripe、inline、checksum、
//! class、replicas 从这里开始真正按目录属性生效（M7 之前 `resolve` 只有内置默认值）。

use super::common::{BLOCK_SIZE, Node, governing};
use crate::base::ErrCode;
use crate::data::format::{ENC_AES256GCM, ENC_NONE};
use crate::hooks::traits::{CODEC_REPLICA, PlacementInput};
use crate::meta::attrs::{CONSISTENCY_STRONG, EffectiveAttrs};
use crate::meta::cluster::chains::{gen_chains, view_of};
use crate::meta::engine::txn::{KvTxn, OpCtx};
use crate::meta::schema::records::{F_INLINE, LayoutV1};

/// 按生效属性给新文件（或截断到 0 的文件）建 layout（3.4.2.1、4.6）：候选 = 集群图快照里同 (class, replicas) 的链；
/// 一条都没有 → `gen_chains` 生成（写了 CHAIN，本批在这个操作之后结束）；placement 插件依次过滤、排序；取前
/// k = min(stripe, 互不相同的链数) 条，k = 0 → `ENOSPC`（拒绝，而不是静默放到别处）。encrypt 或 strong 生效时不 inline。
pub fn new_layout(
    v: &mut dyn KvTxn,
    c: &OpCtx,
    eff: &EffectiveAttrs,
    ino: u64,
    epoch: u32,
    size_hint: u64,
) -> Result<LayoutV1, ErrCode> {
    let class = c.map.classes.iter().find(|k| k.id == eff.class_id).ok_or(ErrCode::ENOSPC)?;
    let replicas = eff.replicas;
    let same = c.map.chains.iter().filter(|(_, r)| r.class_id == class.id && r.replicas == replicas);
    let mut cands: Vec<_> = same.map(|(id, r)| view_of(c.map, *id, r)).collect();
    if cands.is_empty() {
        cands = gen_chains(v, c, class, replicas)?;
    }
    let stripe = eff.stripe;
    let input = PlacementInput { eff, class, replicas, stripe, client_node: None, seed: ino, exclude: &[], size_hint };
    let ranked = c.pipeline.place(&input, cands)?;
    let k = (stripe as usize).min(ranked.len());
    if k == 0 {
        return Err(ErrCode::ENOSPC);
    }
    let no_inline = eff.encrypt_key != 0 || eff.consistency == CONSISTENCY_STRONG;
    Ok(LayoutV1 {
        epoch,
        chunk_size: eff.chunk,
        block_size: BLOCK_SIZE.min(eff.chunk),
        chains: ranked[..k].iter().map(|c| c.id).collect(),
        replicas,
        codec: CODEC_REPLICA,
        checksum: eff.checksum,
        compress: eff.compress,
        compress_level: eff.compress_level,
        encrypt: if eff.encrypt_key != 0 { ENC_AES256GCM } else { ENC_NONE },
        encrypt_key: eff.encrypt_key,
        inline_max: if no_inline { 0 } else { eff.inline_max },
    })
}

/// 给普通文件 `n` 装上新 layout（epoch = max_epoch + 1）、存储类与 F_INLINE（inline_max > 0 才 inline）。
pub fn set_new_layout(v: &mut dyn KvTxn, c: &OpCtx, n: &mut Node) -> Result<(), ErrCode> {
    let eff = governing(v, c, n)?; // 主父目录的生效属性（孤儿的主父目录可能已经删掉了：用根目录的）
    let epoch = n.rec.max_epoch.checked_add(1).ok_or(ErrCode::EOVERFLOW)?;
    let l = new_layout(v, c, &eff, n.ino, epoch, 0)?;
    n.rec.flags = if l.inline_max > 0 { n.rec.flags | F_INLINE } else { n.rec.flags & !F_INLINE };
    (n.rec.max_epoch, n.rec.class_id, n.rec.layout) = (epoch, eff.class_id, Some(l));
    Ok(())
}
