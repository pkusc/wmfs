//! 链的生成（design.md 3.4.2.1）与给 placement 看的 [`ChainView`]。
//!
//! [`gen_chains`] 在 create 或截断到 0 的执行器闭包里、批次开始时的集群图快照里找不到 (class, replicas) 的链时调用：
//! 只写 CHAIN 记录、`next_chain` 与 `map_epoch += 1`，返回新链（它们不在快照里；调用方 `new_layout` 只在快照里一条这种链都
//! 没有时调用它，直接用返回值作 placement 的候选）。写了 CHAIN 键 = 拓扑变了，执行器执行完这个操作就结束本批（3.4.1.4
//! 规则 5）。之后由链管理器维护（M9）。
//!
//! 候选 target（[`candidates`]）= 状态 Up、不是只读（3.2.3：只读的 target 不进入任何链的写路径）、标签匹配存储类的 `label`
//! （`"*"` 匹配任何 target）；`dedicated` 的类独占它的 target，别的类不得使用；按 target id 排序。链数 = 候选数（每个 target 当一次 head）；第 i 条链从候选 i 开始按环形顺序取，
//! 跳过同一节点（故障域 = 节点）的 target，直到 replicas 个或候选用完（[`plan`]）——故障域不够时链以较短长度创建
//! （期望长度仍是 replicas，`wmfs status` 与集群警告显示差距，ADR-0016）。新链 `ver = 1`、`incarnation = 1`、成员全部 Serving。

use std::collections::BTreeSet;

use crate::base::ErrCode;
use crate::hooks::traits::ChainView;
use crate::meta::engine::txn::{KvTxn, OpCtx};
use crate::meta::schema::keys::{self, G_NEXT_CHAIN};
use crate::meta::schema::kv;
use crate::meta::schema::records::{ChainRecV1, NODE_ACTIVE, StorageClassV1, TargetRecV1};
use crate::proto::peer::ClusterMap;

/// 3.2.8 的状态码：`TargetRecV1.state` 1 Up、2 Offline；链成员的状态 1 Serving、2 Syncing。
pub const TARGET_UP: u8 = 1;
pub const TARGET_OFFLINE: u8 = 2;
pub const SERVING: u8 = 1;
pub const SYNCING: u8 = 2;

fn label_ok(labels: &[String], label: &str) -> bool {
    label == "*" || labels.iter().any(|l| l == label)
}

/// 存储类 `class` 可以用 target `t` 吗（不看它此刻是否 Up）：不是只读、标签匹配、不被另一个 dedicated 类占用。
fn fits(m: &ClusterMap, class: &StorageClassV1, t: &TargetRecV1) -> bool {
    let taken = m.classes.iter().any(|k| k.dedicated && k.id != class.id && label_ok(&t.labels, &k.label));
    !t.read_only && label_ok(&t.labels, &class.label) && !taken
}

/// 存储类 `class` 能用的 target：Up 且 [`fits`]，(target, node)，按 target id 升序。
pub fn candidates(m: &ClusterMap, class: &StorageClassV1) -> Vec<(u32, u32)> {
    let mut v: Vec<(u32, u32)> = m
        .targets
        .iter()
        .filter(|(_, t)| t.state == TARGET_UP && fits(m, class, t))
        .map(|(id, t)| (*id, t.node))
        .collect();
    v.sort();
    v
}

/// 一条链的期望长度（管理 API、`wmfs dev wait chains-serving`）：min(声明副本数, 这个存储类可以用的 target 所在的 ACTIVE 节点数)。
/// 不看 target 此刻在不在线（节点暂时宕了，期望不变）；drain 中的节点不算（与链管理器的 `want` 同一口径，3.4.2.10 的注记 ②）。
pub fn want(m: &ClusterMap, c: &ChainRecV1) -> usize {
    let Some(class) = m.classes.iter().find(|k| k.id == c.class_id) else { return 0 };
    let active = |n: u32| m.nodes.iter().any(|(id, r)| *id == n && r.state == NODE_ACTIVE);
    let nodes: BTreeSet<u32> =
        m.targets.iter().filter(|(_, t)| fits(m, class, t) && active(t.node)).map(|(_, t)| t.node).collect();
    (c.replicas as usize).min(nodes.len())
}

/// 链上 Serving 成员的个数。
pub fn serving_count(c: &ChainRecV1) -> usize {
    c.members.iter().filter(|(_, s)| *s == SERVING).count()
}

/// 每条新链的成员（纯函数）：第 i 条从候选 i 开始按环形顺序取，跳过已经用过的节点，直到 `replicas` 个或候选用完。
pub fn plan(cands: &[(u32, u32)], replicas: u8) -> Vec<Vec<(u32, u32)>> {
    let n = cands.len();
    (0..n)
        .map(|i| {
            let mut chain: Vec<(u32, u32)> = Vec::new();
            for t in (0..n).map(|j| cands[(i + j) % n]) {
                if chain.len() < replicas as usize && !chain.iter().any(|(_, node)| *node == t.1) {
                    chain.push(t);
                }
            }
            chain
        })
        .collect()
}

/// 为 (class, replicas) 生成新链（见文件头）；一个候选 target 都没有时返回空（调用方回 `ENOSPC`）。
pub fn gen_chains(
    v: &mut dyn KvTxn,
    c: &OpCtx,
    class: &StorageClassV1,
    replicas: u8,
) -> Result<Vec<ChainView>, ErrCode> {
    let mut out = Vec::new();
    for members in plan(&candidates(c.map, class), replicas) {
        let id = u32::try_from(kv::next_id(v, G_NEXT_CHAIN)?).map_err(|_| ErrCode::ENOSPC)?;
        let rec = ChainRecV1 {
            ver: 1,
            incarnation: 1,
            class_id: class.id,
            replicas,
            members: members.iter().map(|(t, _)| (*t, SERVING)).collect(),
            last_serving: None,
            removed: vec![],
        };
        kv::put(v, &keys::chain(id), &rec);
        out.push(view_of(c.map, id, &rec));
    }
    if !out.is_empty() {
        kv::bump_map_epoch(v)?; // 改变了链的批次在同一批次里 map_epoch += 1（3.3.6）
    }
    Ok(out)
}

/// 一条链给 placement 看的样子：成员 (target, node)、Serving 成员数、容量（有上限的成员里最小的那个；成员全都不限或未知
/// 才是 0 = 不限）。实现注记（M4）：`used` 来自 target 心跳（M5），现在一律是 0。
pub fn view_of(m: &ClusterMap, id: u32, r: &ChainRecV1) -> ChainView {
    let target = |t: u32| m.targets.iter().find(|(i, _)| *i == t).map(|(_, x)| x);
    let caps = r.members.iter().filter_map(|(t, _)| target(*t)).map(|x| x.capacity);
    ChainView {
        id,
        class_id: r.class_id,
        replicas: r.replicas,
        serving: serving_count(r) as u8,
        members: r.members.iter().map(|(t, _)| (*t, target(*t).map_or(0, |x| x.node))).collect(),
        used: 0,
        capacity: caps.filter(|c| *c > 0).min().unwrap_or(0),
    }
}

#[cfg(test)]
mod tests;
