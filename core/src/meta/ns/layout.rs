//! 新文件（与截断到 0 的文件）的 layout（design.md 3.4.2.1、4.6、3.5.1 的 setattr(size = 0) 一行）：按生效属性选链、建 layout。
//! 实现注记（M7）：从 `common.rs` 搬过来（每个文件 ≤ 400 行），`common` 原样转出这两个函数。新文件的 chunk、stripe、inline、checksum、
//! class、replicas 从这里开始真正按目录属性生效（M7 之前 `resolve` 只有内置默认值）。

use std::sync::Arc;

use super::common::{BLOCK_SIZE, Node, governing};
use crate::base::ErrCode;
use crate::data::format::{ENC_AES256GCM, ENC_NONE};
use crate::hooks::policy::{PolicyInput, PolicyOutput, PolicyPoint};
use crate::hooks::traits::{CODEC_REPLICA, PlacementInput};
use crate::meta::attr_registry::qos_tier::LOCALITY_PREFER;
use crate::meta::attrs::{CONSISTENCY_STRONG, EffectiveAttrs};
use crate::meta::cluster::chains::{gen_chains, owned, view_of};
use crate::meta::engine::txn::{KvRead, KvTxn, OpCtx};
use crate::meta::schema::records::{F_INLINE, LayoutV1, NODE_ACTIVE, SessionRecV1};
use crate::meta::schema::{keys, kv};

/// 选链的提示（4.6、理念文档 4.1 的"大小提示"）：`size` = create 与截断到 0 时为 0、迁移时为文件大小；`client_node` = 客户端所在的
/// 节点（locality = prefer-client 时由会话的 host_id 认出，[`client_node`]）；`exclude` = 不要的链（rebalance 把文件从最满的链上挪走）。
#[derive(Default)]
pub struct Hint<'a> {
    pub size: u64,
    pub client_node: Option<u32>,
    pub exclude: &'a [u32],
}

/// 按生效属性给新文件（或截断到 0 的文件）建 layout（3.4.2.1、4.6）：候选 = 集群图快照里同 (class, replicas) 的链；
/// 一条都没有 → `gen_chains` 生成（写了 CHAIN，本批在这个操作之后结束）；placement 插件依次过滤、排序；取前
/// k = min(stripe, 互不相同的链数) 条，k = 0 → `ENOSPC`（拒绝，而不是静默放到别处）。encrypt 或 strong 生效时不 inline。
/// 实现注记（M13）：成员已经不归这个类用的链（后来建的 dedicated 类占走了它的 target，6.1）不当候选；链的已用与容量来自 target 心跳。
pub fn new_layout(
    v: &mut dyn KvTxn,
    c: &OpCtx,
    eff: &EffectiveAttrs,
    ino: u64,
    epoch: u32,
    hint: &Hint,
) -> Result<LayoutV1, ErrCode> {
    let class = c.map.classes.iter().find(|k| k.id == eff.class_id).ok_or(ErrCode::ENOSPC)?;
    let replicas = eff.replicas;
    let owned_by_class = |t: &u32| c.map.targets.iter().any(|(i, x)| i == t && owned(c.map, class, x));
    let same = c.map.chains.iter().filter(|(_, r)| r.class_id == class.id && r.replicas == replicas);
    let mut cands: Vec<_> = same
        .filter(|(_, r)| r.members.iter().all(|(t, _)| owned_by_class(t)))
        .map(|(id, r)| view_of(c.map, c.usage, *id, r))
        .collect();
    if cands.is_empty() {
        cands = gen_chains(v, c, class, replicas)?;
    }
    let stripe = eff.stripe;
    let input = PlacementInput {
        eff,
        class,
        replicas,
        stripe,
        client_node: hint.client_node,
        seed: ino,
        exclude: hint.exclude,
        size_hint: hint.size,
    };
    let ranked = c.pipeline.place(&input, cands)?;
    let k = (stripe as usize).min(ranked.len());
    if k == 0 {
        return Err(ErrCode::ENOSPC);
    }
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
        inline_max: inline_of(eff),
    })
}

/// 新文件实际的 inline 阈值（实现注记（M11）：`wmfs status` 也按它比较）：`encrypt` 生效或 `consistency=strong` 时为 0（内容不能留在
/// 元数据里），否则就是声明的 `inline`。
pub fn inline_of(eff: &EffectiveAttrs) -> u32 {
    if eff.encrypt_key != 0 || eff.consistency == CONSISTENCY_STRONG { 0 } else { eff.inline_max }
}

/// 给普通文件 `n` 装上新 layout（epoch = max_epoch + 1）、存储类与 F_INLINE（inline_max > 0 才 inline）。
pub fn set_new_layout(v: &mut dyn KvTxn, c: &OpCtx, n: &mut Node) -> Result<(), ErrCode> {
    let mut eff = governing(v, c, n)?; // 主父目录的生效属性（孤儿的主父目录可能已经删掉了：用根目录的）
    // 策略求值点 CreatePlacement（3.7.7，M13 接上）：内置求值器回"默认"= 目录的存储类；二期的策略可以在这里换一个类
    let input = PolicyInput { point: PolicyPoint::CreatePlacement, attrs: &eff, file: None, event: None, kv: None };
    if let PolicyOutput::Class(id) = c.pipeline.policy().eval(&input) {
        eff = Arc::new(EffectiveAttrs { class_id: id, ..(*eff).clone() });
    }
    let epoch = n.rec.max_epoch.checked_add(1).ok_or(ErrCode::EOVERFLOW)?;
    let client_node = if eff.locality == LOCALITY_PREFER { client_node(v, c)? } else { None };
    let l = new_layout(v, c, &eff, n.ino, epoch, &Hint { client_node, ..Hint::default() })?;
    n.rec.flags = if l.inline_max > 0 { n.rec.flags | F_INLINE } else { n.rec.flags & !F_INLINE };
    (n.rec.max_epoch, n.rec.class_id, n.rec.layout) = (epoch, eff.class_id, Some(l));
    Ok(())
}

/// 发请求的客户端在哪个节点上（locality，4.6）：会话的 host_id 与某个 Active 节点的 host_id 相同（`wmfs dev mount --node n` 冒充第 n 个
/// 节点所在的"主机"）；没有会话（内部操作）、没报 host_id、或不在任何节点上 → None。
fn client_node(v: &dyn KvRead, c: &OpCtx) -> Result<Option<u32>, ErrCode> {
    let Some(s) = kv::get::<SessionRecV1>(v, &keys::session(c.req.session))?.filter(|s| !s.host_id.is_empty()) else {
        return Ok(None);
    };
    Ok(c.map.nodes.iter().find(|(_, n)| n.host_id == s.host_id && n.state == NODE_ACTIVE).map(|(id, _)| *id))
}
