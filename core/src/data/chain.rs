//! 本节点眼里的链（design.md 3.4.2.1、3.4.2.2 的 I4/I5、3.4.4）：在本地集群图视图里找本节点在某条链上的成员、它的前驱与后继、
//! last serving（L）、某个 target 在哪个节点上；请求里的链版本与本地视图一致吗（[`current`]）；以及 I5——成员只有在"target 租约
//! 有效 + 集群图里本 target 的 `boot_id` 是本进程的 + 本地集群图不比最近一次心跳回复里的 `map_epoch` 旧"时才服务读写（[`check_i5`]）。
//!
//! 租约（[`Lease`]）：hb 线程每次心跳成功，按**发送时刻** + 0.8 × `target_lease` 续期（持有方先自停、授予方 8 s 后才回收，3.4.4）。
//! 还没有 hb 线程的组件测试不核对租约与 boot_id（`Lease` 里的 boot 是 0）。

use std::sync::atomic::{AtomicU64, Ordering};

use super::DataNode;
use crate::base::types::ChunkId;
use crate::base::{ErrCode, clock, lock, log};
use crate::meta::cluster::chains::SERVING;
use crate::meta::schema::records::ChainRecV1;
use crate::net::client::Endpoint;
use crate::proto::peer::ClusterMap;

/// 一个失败的回复：错误码与 ErrorBody.extra（`STALE_CHAIN` 带本地的 chain_ver，`CHUNK_DIRTY` 带 last serving 的 target）。
pub struct Fail(pub ErrCode, pub u64);

impl From<ErrCode> for Fail {
    fn from(e: ErrCode) -> Fail {
        Fail(e, 0)
    }
}

/// 本节点的 target 租约（见文件头）。
#[derive(Default)]
pub struct Lease {
    boot: AtomicU64,  // 本进程的 boot_id；0 = 没有 hb 线程（组件测试）：不核对租约与 boot_id
    until: AtomicU64, // mono_ms：过了就自我隔离（FENCED）
    epoch: AtomicU64, // 最近一次心跳回复里的 map_epoch：本地集群图比它旧就先不服务（I5）
}

impl Lease {
    /// hb 线程起来时：从此核对租约与集群图里的 boot_id（`boot` 不是 0；第一次心跳成功之前一律 FENCED）。
    pub fn enforce(&self, boot: u64) {
        self.boot.store(boot, Ordering::Relaxed);
    }
    /// 一次心跳成功（`sent_ms` = 发送时刻，`lease_ms` = 回复里的租约，`epoch` = 回复里的 map_epoch）。
    pub fn renew(&self, sent_ms: u64, lease_ms: u64, epoch: u64) {
        self.until.store(sent_ms + lease_ms * 4 / 5, Ordering::Relaxed);
        self.epoch.store(epoch, Ordering::Relaxed);
    }
    pub fn valid(&self, local_epoch: u64) -> bool {
        let r = Ordering::Relaxed;
        self.boot.load(r) == 0 || (clock::mono_ms() < self.until.load(r) && local_epoch >= self.epoch.load(r))
    }
}

/// I5（见文件头）：target `t` 现在能服务读写吗；不能 → `FENCED`（租约过期、刚重启还没被确认、或集群图旧了：自我隔离）。
pub fn check_i5(node: &DataNode, map: &ClusterMap, t: u32) -> Result<(), Fail> {
    let boot = node.lease.boot.load(Ordering::Relaxed);
    let boot_ok = boot == 0 || map.targets.iter().any(|(id, r)| *id == t && r.boot_id == boot);
    if boot_ok && node.lease.valid(map.map_epoch) { Ok(()) } else { Err(ErrCode::FENCED.into()) }
}

/// 请求带的链版本与本地视图一致的那条链（3.4.2.3 第 1 步、3.4.2.4）：本地还不认识这条链（多半是刚生成的）→ 拉新集群图、
/// `STALE_CHAIN(0)`；版本不同（请求的更新就拉新图）→ `STALE_CHAIN(本地的 chain_ver)`。
pub fn current<'m>(node: &DataNode, map: &'m ClusterMap, chain_id: u32, ver: u64) -> Result<&'m ChainRecV1, Fail> {
    let Some(c) = find(map, chain_id) else {
        node.map.poke();
        return Err(Fail(ErrCode::STALE_CHAIN, 0));
    };
    if c.ver != ver {
        if ver > c.ver {
            node.map.poke();
        }
        return Err(Fail(ErrCode::STALE_CHAIN, c.ver));
    }
    Ok(c)
}

pub fn find(map: &ClusterMap, chain: u32) -> Option<&ChainRecV1> {
    map.chains.iter().find(|(id, _)| *id == chain).map(|(_, c)| c)
}

/// 本节点在链 `c` 上的成员：(target, 在 members 里的位置)。链上的成员在不同节点上，所以最多一个。
pub fn local(node: &DataNode, c: &ChainRecV1) -> Option<(u32, usize)> {
    c.members.iter().position(|(t, _)| node.targets.contains_key(t)).map(|i| (c.members[i].0, i))
}

pub fn serving_at(c: &ChainRecV1, pos: usize) -> bool {
    c.members.get(pos).is_some_and(|(_, s)| *s == SERVING)
}

/// 后继（链上的下一个成员，可能是 Syncing）；`pos` 是尾 → None。
pub fn successor(c: &ChainRecV1, pos: usize) -> Option<u32> {
    c.members.get(pos + 1).map(|(t, _)| *t)
}

pub fn predecessor(c: &ChainRecV1, pos: usize) -> Option<u32> {
    pos.checked_sub(1).map(|p| c.members[p].0)
}

/// last serving（L）：最后一个 Serving 成员（脏读的仲裁者、同步源）。
pub fn last_serving(c: &ChainRecV1) -> Option<u32> {
    c.members.iter().rev().find(|(_, s)| *s == SERVING).map(|(t, _)| *t)
}

/// 换上新图 `new` 时（在 3.4.2.6 规则 2、3 之前，`redrive::switch`），本节点的成员在这条链上新成为 last serving → 记下"一个 target 租约
/// （新图的）之后"；在那之前读回可重试的 `BUSY`（[`read_busy`]；写与转发照常）。换图那一刻各节点的视图不一致，旧图里的 L 也许还在旧图上
/// 服务读——等一个租约，还在服务的成员要么已换图，要么已按 I5 自我隔离。用整个租约、不用 0.8 倍：比的是两个持有方各自的时钟，与租约
/// 协议同样留余量。
/// - 旧图里它是 Syncing（被提升，ADR-0040 第 11 条）→ 整条链的读都 `BUSY`：它在物理链尾、先提交了 V，旧的 L 还只有 V 的 pending、会回
///   旧版本。
/// - 旧图里它是排在 L 前面的 Serving（L 被降级或移出、force-serving，ADR-0041 第 14 条）→ 只对被占着的 chunk（有 pending——包括规则 2
///   在本地提交、还在等确认的——或正忙）回 `BUSY`：干净的 chunk 上，它已提交的版本不旧于下游任何成员的（I2）。
///
/// 要在规则 2 之前登记：规则 2 在换图途中就把在途的 pending 本地提交，晚登记的话，换图途中按旧图的读会读到旧 L 还没有的版本。
pub fn note_promotions(node: &DataNode, old: Option<&ClusterMap>, new: &ClusterMap) {
    let now = clock::mono_ms();
    let mut busy = lock(&node.promoted);
    busy.retain(|_, (until, _)| *until > now);
    for (id, c) in &new.chains {
        // 新图里本节点的成员是 last serving……
        let Some((t, p)) = local(node, c) else { continue };
        if !serving_at(c, p) || last_serving(c) != Some(t) {
            continue;
        }
        // ……而在旧图里它不是（还是 Syncing，或排在 L 前面）
        let Some(o) = old.and_then(|m| find(m, *id)) else { continue };
        let Some((_, op)) = local(node, o) else { continue };
        if last_serving(o) == Some(t) {
            continue;
        }
        let (ms, all) = (new.settings.target_lease_ms, !serving_at(o, op));
        let which = if all { "全部 chunk（从 Syncing 被提升）" } else { "有 pending 或正忙的 chunk" };
        log::info("data", format_args!("链 {id}：target {t} 成为 last serving，{ms} ms 内读{which}回 BUSY"));
        let w = busy.entry(*id).or_insert((0, false));
        *w = (w.0.max(now + ms), w.1 || all); // 还没过去的"整条链"不能缩成"只挡被占着的 chunk"
    }
}

/// 读这个 chunk 要回 `BUSY` 吗：这条链刚让本节点的成员成为 last serving、还在一个租约之内，并且是整条链都挡，或这个 chunk 被占着
/// （[`note_promotions`]）。
pub fn read_busy(node: &DataNode, id: &ChunkId) -> bool {
    let w = lock(&node.promoted).get(&id.chain).copied();
    w.is_some_and(|(until, all)| clock::mono_ms() < until && (all || node.chunks.held(id)))
}

/// target `t` 所在节点的地址（集群图里找不到 → None）。
pub fn endpoint(map: &ClusterMap, t: u32) -> Option<Endpoint> {
    let node = map.targets.iter().find(|(id, _)| *id == t)?.1.node;
    let addrs = map.nodes.iter().find(|(id, _)| *id == node)?.1.addrs.clone();
    Some(Endpoint { node, addrs })
}

#[cfg(test)]
mod tests {
    use super::Lease;
    use crate::base::clock;

    /// I5 的租约（3.4.4）：hb 线程起来之前不核对；起来之后第一次心跳成功之前、过了 0.8 × 租约、本地集群图比心跳回复里的旧，都不服务。
    #[test]
    fn the_target_lease_fences_itself_first() {
        let l = Lease::default();
        assert!(l.valid(0), "没有 hb 线程（组件测试）：不核对");
        l.enforce(7);
        assert!(!l.valid(9), "第一次心跳成功之前一律 FENCED");
        let now = clock::mono_ms();
        l.renew(now, 8000, 5);
        assert!(l.valid(5) && !l.valid(4), "续上了；本地集群图比回复里的 map_epoch 旧 → 先拉新图");
        l.renew(clock::mono_ms(), 100, 5);
        std::thread::sleep(std::time::Duration::from_millis(90));
        assert!(!l.valid(5), "从发送时刻算 0.8 × 租约就自停（授予方满一个租约才回收）");
    }
}
