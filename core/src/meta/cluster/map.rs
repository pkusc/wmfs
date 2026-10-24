//! 集群图 `ClusterMap`（design.md 3.3.6）：由已提交状态构建、按 `map_epoch` 缓存，顺带计算集群警告（中文，不进 KV）。
//!
//! 任何改变节点、target、链、存储类、key、数据密钥、QoS 表、被驱逐列表、集群参数或插件清单的批次都在同一批次里
//! `map_epoch += 1`，所以"map_epoch 没变 = 集群图没变"：[`MapCache::get`] 每次只点查一次 GLOBAL `map_epoch`，变了才重建
//! （于是 raft 线程应用批次时不用另外通知谁让缓存失效）。执行器每个批次开始时、只读请求在 ReadIndex 之后各取一份快照，
//! 都从它们自己的读事务里取，所以快照与它们看到的 KV 一致（3.5.1 的 `OpCtx.map`）。
//!
//! 实现注记（M4）：警告只有"短链"一类（ADR-0016）——存储类声明的副本数大于它可用的故障域数、链的成员少于声明的副本数；
//! 故障域重叠、2 voter 无高可用等随 M8/M9 加入，目录属性上的 `replicas`（例如 init 写的 /home replicas=3）随 M7 加入。
//! 警告变化时（含 leader 第一次建图）写 WARN 日志（3.1.5：server 日志里一定看得到）。
//! 实现注记（M8）：加了三条——元数据节点不少于 2 个而 voter 不到 3 个（"元数据没有高可用"，3.4.1.9）、几个节点的 machine_id 相同
//! （"故障域重叠"，1.1）、有多个成员的链（数据的链式复制在 M9：在那之前文件数据只在 head 上，实际 1 副本——不能让 `durability` 的
//! "实际 3"看起来像真的）。

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use serde::de::DeserializeOwned;

use super::chains::candidates;
use crate::base::{ErrCode, lock, log};
use crate::meta::engine::txn::KvScan;
use crate::meta::schema::keys::{
    self, G_CLUSTER, G_FEATURES, G_MAP_EPOCH, G_PLUGINS, G_SETTINGS, T_CHAIN, T_CLASS, T_DATAKEY, T_EVICTED, T_KEYMETA,
    T_NODE, T_QOS, T_TARGET,
};
use crate::meta::schema::kv;
use crate::meta::schema::records::{ClusterInfoV1, META_NONE, META_VOTER, NODE_REMOVED, decode_v1};
use crate::proto::peer::ClusterMap;

/// 由已提交状态构建一张集群图（每张表扫一遍；小集群，几十 KB）。GLOBAL 里缺的项用默认值（启动时已核对过 cluster 与 settings）。
pub fn build(v: &dyn KvScan, leader: Option<u32>) -> Result<ClusterMap, ErrCode> {
    let cluster: Option<ClusterInfoV1> = kv::get(v, &keys::global(G_CLUSTER))?;
    let mut m = ClusterMap {
        map_epoch: kv::get(v, &keys::global(G_MAP_EPOCH))?.unwrap_or(0),
        cluster_id: cluster.map_or([0; 16], |c| c.cluster_id),
        leader,
        settings: kv::get(v, &keys::global(G_SETTINGS))?.unwrap_or_default(),
        features: kv::get(v, &keys::global(G_FEATURES))?.unwrap_or_default(),
        nodes: with_u32_ids(rows(v, T_NODE)?),
        targets: with_u32_ids(rows(v, T_TARGET)?),
        chains: with_u32_ids(rows(v, T_CHAIN)?),
        classes: rows(v, T_CLASS)?.into_iter().map(|(_, c)| c).collect(),
        keys: with_u32_ids(rows(v, T_KEYMETA)?),
        datakeys: with_u32_ids(rows(v, T_DATAKEY)?),
        qos: rows(v, T_QOS)?,
        evicted: rows(v, T_EVICTED)?,
        plugins: kv::get(v, &keys::global(G_PLUGINS))?.unwrap_or_default(),
        warnings: vec![],
    };
    m.warnings = warnings(&m);
    Ok(m)
}

/// 把行的 id 换成 u32（节点、target、链、key、数据密钥的 id 都是 u32）。
fn with_u32_ids<T>(r: Vec<(u64, T)>) -> Vec<(u32, T)> {
    r.into_iter().map(|(i, x)| (i as u32, x)).collect()
}

/// 一个以整数 id 为键的键空间：(id, 记录)。
fn rows<T: DeserializeOwned>(v: &dyn KvScan, tag: u8) -> Result<Vec<(u64, T)>, ErrCode> {
    kv::scan_tag(v, tag)?.iter().map(|(k, b)| Ok((kv::key_id(k), decode_v1(b)?))).collect()
}

/// 集群警告（见文件头：短链、没有可用的 target、元数据没有高可用、故障域重叠、M9 之前只有 1 副本）。
pub fn warnings(m: &ClusterMap) -> Vec<String> {
    let mut w = Vec::new();
    for k in &m.classes {
        let fd = candidates(m, k).iter().map(|(_, node)| *node).collect::<BTreeSet<u32>>().len();
        if fd == 0 {
            w.push(format!("{} 类声明 {} 副本，没有可用的 target：新文件无法创建（ENOSPC）", k.name, k.replica));
        } else if k.replica as usize > fd {
            let r = k.replica;
            w.push(format!(
                "{} 类声明 {r} 副本，只有 {fd} 个故障域：新文件实际为 {fd} 副本（加入节点后自动补齐）",
                k.name
            ));
        }
    }
    let live: Vec<_> = m.nodes.iter().map(|(_, n)| n).filter(|n| n.state != NODE_REMOVED).collect();
    let voters = live.iter().filter(|n| n.meta_role == META_VOTER).count();
    let meta = live.iter().filter(|n| n.meta_role != META_NONE).count(); // learner 与 voter
    if meta >= 2 && voters < 3 {
        w.push(format!(
            "元数据只有 {voters} 个 voter（共 {meta} 个元数据节点）：没有高可用，坏了 voter 的多数元数据就不可用；推荐 1 个或 ≥ 3 个元数据节点"
        ));
    }
    let mut machines = BTreeMap::new();
    for n in live.iter().filter(|n| !n.machine_id.is_empty()) {
        *machines.entry(n.machine_id.as_str()).or_insert(0) += 1;
    }
    if let Some(k) = machines.values().copied().filter(|k| *k > 1).max() {
        w.push(format!("故障域重叠：{k} 个节点在同一台机器上（machine_id 相同），这不是真正的容错"));
    }
    if m.chains.iter().any(|(_, c)| c.members.len() > 1) {
        w.push(
            "数据的链式复制在 M9 实现：现在有多个成员的链只有 head（第一个成员）存数据，文件数据实际只有 1 副本".into(),
        );
    }
    let short: Vec<_> = m.chains.iter().filter(|(_, c)| c.members.len() < c.replicas as usize).collect();
    if let Some((id, c)) = short.first() {
        w.push(format!(
            "有 {} 条链少于声明副本数（例如 chain {id}：声明 {} 副本、实际 {} 个成员；加入节点后自动补齐）",
            short.len(),
            c.replicas,
            c.members.len()
        ));
    }
    w
}

/// client 与 admin 角色拿到的集群图：不含 keys、datakeys（3.3.3 的角色表）。
pub fn filtered(m: &ClusterMap) -> ClusterMap {
    ClusterMap { keys: vec![], datakeys: vec![], ..m.clone() }
}

/// 按 (map_epoch, leader) 缓存的集群图，并记着上一次写进日志的警告。
#[derive(Default)]
pub struct MapCache {
    cur: Mutex<Option<Arc<ClusterMap>>>,
    logged: Mutex<Vec<String>>,
}

impl MapCache {
    /// 与视图 `v` 一致的集群图：`v` 里的 map_epoch 与 leader 都和缓存的一样就用缓存，否则从 `v` 重建（警告变了写日志）。
    pub fn get(&self, v: &dyn KvScan, leader: Option<u32>) -> Result<Arc<ClusterMap>, ErrCode> {
        let epoch: u64 = kv::get(v, &keys::global(G_MAP_EPOCH))?.unwrap_or(0);
        let mut cur = lock(&self.cur);
        if let Some(m) = cur.as_ref().filter(|m| m.map_epoch == epoch && m.leader == leader) {
            return Ok(m.clone());
        }
        let m = Arc::new(build(v, leader)?);
        self.note_warnings(&m.warnings);
        *cur = Some(m.clone());
        Ok(m)
    }

    fn note_warnings(&self, now: &[String]) {
        let mut logged = lock(&self.logged);
        if *logged == now {
            return;
        }
        for w in now {
            log::warn("cluster", format_args!("集群警告：{w}"));
        }
        if now.is_empty() {
            log::info("cluster", "集群警告已全部消除");
        }
        *logged = now.to_vec();
    }
}
