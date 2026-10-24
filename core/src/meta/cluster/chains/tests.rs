//! 链生成的单元测试（design.md 9.1 的"placement / 链生成"）：确定性（同样的 target 同样的链）、故障域约束（同一条链的成员
//! 在不同节点上，节点不够时链短一些）、dedicated 的类独占它的 target、只用 Up 的 target、gen_chains 写 CHAIN、next_chain 与
//! map_epoch、链的容量取有上限的成员里最小的；带种子的随机测试（xorshift，失败时消息里有种子）检查"每条链里节点各不相同、
//! 长度 = min(replicas, 节点数)"。

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use super::*;
use crate::base::rand::XorShift64;
use crate::base::types::{Principal, ReqCtx, Role, Ts};
use crate::hooks::pipeline::Pipeline;
use crate::hooks::registry::PluginSet;
use crate::hooks::traits::ConnIdentity;
use crate::meta::attrs::AttrResolver;
use crate::meta::engine::txn::KvRead;
use crate::meta::schema::keys::G_MAP_EPOCH;
use crate::meta::schema::records::{ChainRecV1, ClusterSettingsV1, TargetRecV1, decode_v1, encode_v1};

fn target(node: u32, labels: &[&str], state: u8) -> TargetRecV1 {
    TargetRecV1 {
        node,
        path: format!("/t/{node}"),
        labels: labels.iter().map(|l| l.to_string()).collect(),
        phys_devs: vec![],
        capacity: 0,
        boot_id: 0,
        state,
        last_up_ms: 0,
        read_only: false,
    }
}

fn class(id: u16, label: &str, dedicated: bool) -> StorageClassV1 {
    StorageClassV1 {
        id,
        name: format!("c{id}"),
        label: label.into(),
        dedicated,
        replica: 3,
        iops_min: 0,
        iops_max: 0,
        bw_min: 0,
        bw_max: 0,
        burst_ms: 0,
        weight: 1,
        latency_target_us: 0,
        priority: 1,
        failure_domain: 1,
        locality: 0,
        tier_to: None,
        tier_when: None,
    }
}

fn map(targets: Vec<(u32, TargetRecV1)>, classes: Vec<StorageClassV1>) -> ClusterMap {
    ClusterMap { targets, classes, ..ClusterMap::default() }
}

#[test]
fn ring_plan_is_deterministic_and_respects_failure_domains() {
    let one_per_node = [(1, 1), (2, 2), (3, 3)];
    assert_eq!(
        plan(&one_per_node, 3),
        vec![vec![(1, 1), (2, 2), (3, 3)], vec![(2, 2), (3, 3), (1, 1)], vec![(3, 3), (1, 1), (2, 2)]],
        "每个 target 当一次 head，按环形顺序取"
    );
    let shared = [(1, 1), (2, 1), (3, 2), (4, 3)]; // target 1、2 在同一个节点上
    let p = plan(&shared, 3);
    assert_eq!(p[0], vec![(1, 1), (3, 2), (4, 3)], "跳过与已选成员同节点的 target");
    assert_eq!(p[1], vec![(2, 1), (3, 2), (4, 3)]);
    assert_eq!(plan(&[(1, 1), (2, 1)], 3), vec![vec![(1, 1)], vec![(2, 1)]], "只有 1 个故障域：链长 1（ADR-0016）");
    assert!(plan(&[], 3).is_empty());
}

#[test]
fn random_target_sets_never_put_two_members_on_one_node() {
    for seed in 1..=200u64 {
        let mut r = XorShift64::new(seed);
        let n = 1 + r.below(12) as u32;
        let cands: Vec<(u32, u32)> = (1..=n).map(|t| (t, 1 + r.below(5) as u32)).collect();
        let replicas = 1 + r.below(4) as u8;
        let nodes = cands.iter().map(|(_, n)| *n).collect::<BTreeSet<_>>().len();
        let p = plan(&cands, replicas);
        assert_eq!(p.len(), cands.len(), "种子 {seed}：链数 = 候选数");
        for (i, c) in p.iter().enumerate() {
            let distinct = c.iter().map(|(_, n)| *n).collect::<BTreeSet<_>>().len();
            assert_eq!(distinct, c.len(), "种子 {seed}：链 {i} 的成员在不同节点上：{c:?}");
            assert_eq!(c.len(), (replicas as usize).min(nodes), "种子 {seed}：链长 = min(replicas, 节点数)");
            assert_eq!(c[0], cands[i], "种子 {seed}：第 {i} 条链的 head 是候选 {i}");
        }
    }
}

#[test]
fn candidates_match_labels_skip_offline_and_read_only_and_honour_dedicated() {
    let m = map(
        vec![
            (3, target(2, &["nvme"], TARGET_UP)),
            (1, target(1, &[], TARGET_UP)),
            (2, target(1, &["hdd"], TARGET_UP)),
            (4, target(3, &[], 2)), // Offline
            (5, TargetRecV1 { read_only: true, ..target(4, &[], TARGET_UP) }),
        ],
        vec![class(1, "*", false), class(2, "nvme", true), class(3, "hdd", false)],
    );
    let (default, gold, hdd) = (&m.classes[0], &m.classes[1], &m.classes[2]);
    assert_eq!(
        candidates(&m, default),
        vec![(1, 1), (2, 1)],
        "按 id 排序；nvme 被 dedicated 类独占；Offline、只读的不要"
    );
    assert_eq!(candidates(&m, gold), vec![(3, 2)], "dedicated 类自己用它的 target");
    assert_eq!(candidates(&m, hdd), vec![(2, 1)], "标签匹配");
}

/// 一个直接改 BTreeMap 的视图（gen_chains 只做点查与写）。
struct View(BTreeMap<Vec<u8>, Vec<u8>>);
impl KvRead for View {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ErrCode> {
        Ok(self.0.get(key).cloned())
    }
}
impl KvTxn for View {
    fn put(&mut self, key: &[u8], value: Vec<u8>) {
        self.0.insert(key.to_vec(), value);
    }
    fn delete(&mut self, key: &[u8]) {
        self.0.remove(key);
    }
}

#[test]
fn gen_chains_writes_chain_records_and_bumps_map_epoch() {
    let m = map(vec![(1, target(1, &[], TARGET_UP)), (2, target(1, &[], TARGET_UP))], vec![class(1, "*", false)]);
    let mut v = View(BTreeMap::new());
    kv::put(&mut v, &keys::global(G_NEXT_CHAIN), &7u32);
    kv::put(&mut v, &keys::global(G_MAP_EPOCH), &3u64);
    let conn = Arc::new(ConnIdentity {
        key_id: 1,
        role: Role::Admin,
        tenant: 0,
        uid_ranges: vec![],
        root_squash: false,
        path_caps: vec![],
    });
    let who = Principal { uid: 0, gid: 0, groups: vec![], tenant: 0, job_id: 0, conn };
    let req = ReqCtx { who, session: 0, seq: 0, pid: 0, trace_id: 0, now: Ts::default(), now_ms: 0, internal: true };
    let (pipeline, attrs, settings) =
        (Pipeline::new(PluginSet::new(&[])), AttrResolver::default(), ClusterSettingsV1::default());
    let c = OpCtx { req: &req, pipeline: &pipeline, attrs: &attrs, map: &m, settings: &settings, cap_key: &[0; 32] };
    let views = gen_chains(&mut v, &c, &m.classes[0], 3).unwrap();
    assert_eq!(
        views.iter().map(|c| (c.id, c.serving, c.members.clone())).collect::<Vec<_>>(),
        vec![(7, 1, vec![(1, 1)]), (8, 1, vec![(2, 1)])]
    );
    let rec: ChainRecV1 = decode_v1(&v.0[&keys::chain(7)]).unwrap();
    assert_eq!((rec.ver, rec.incarnation, rec.replicas, rec.members), (1, 1, 3, vec![(1, SERVING)]), "期望长度仍是 3");
    assert_eq!(v.0[&keys::global(G_NEXT_CHAIN)], encode_v1(&9u32));
    assert_eq!(v.0[&keys::global(G_MAP_EPOCH)], encode_v1(&4u64), "改变了链：map_epoch + 1");
    let none = map(vec![], vec![class(1, "*", false)]);
    let c = OpCtx { map: &none, ..c };
    assert!(gen_chains(&mut v, &c, &none.classes[0], 3).unwrap().is_empty(), "没有 target：不生成链");
    assert_eq!(v.0[&keys::global(G_MAP_EPOCH)], encode_v1(&4u64), "什么也没生成时 map_epoch 不变");
}

#[test]
fn chain_capacity_is_the_smallest_limited_member() {
    let cap = |c| TargetRecV1 { capacity: c, ..target(1, &[], TARGET_UP) };
    let m = map(vec![(1, cap(0)), (2, cap(100 << 30)), (3, cap(0))], vec![class(1, "*", false)]);
    let chain = |ts: &[u32]| ChainRecV1 {
        ver: 1,
        incarnation: 1,
        class_id: 1,
        replicas: 3,
        members: ts.iter().map(|t| (*t, SERVING)).collect(),
        last_serving: None,
        removed: vec![],
    };
    assert_eq!(view_of(&m, 1, &chain(&[1, 2])).capacity, 100 << 30, "一个成员不限、另一个 100 GiB：按 100 GiB");
    assert_eq!(view_of(&m, 2, &chain(&[1, 3])).capacity, 0, "成员全都不限：0 = 不限");
}

#[test]
fn warnings_cover_short_chains_and_missing_failure_domains() {
    let mut m = map(vec![(1, target(1, &[], TARGET_UP)), (2, target(1, &[], TARGET_UP))], vec![class(1, "*", false)]);
    let short = ChainRecV1 {
        ver: 1,
        incarnation: 1,
        class_id: 1,
        replicas: 3,
        members: vec![(1, SERVING)],
        last_serving: None,
        removed: vec![],
    };
    m.chains = vec![(1, short.clone()), (2, ChainRecV1 { members: vec![(2, SERVING)], ..short })];
    let w = crate::meta::cluster::map::warnings(&m);
    assert_eq!(w.len(), 2, "{w:?}");
    assert!(w[0].starts_with("有 2 条链少于声明副本数"), "链降级排在最前面（wmfs status 的第一行，M9）：{w:?}");
    assert!(w[1].contains("c1 类声明 3 副本，只有 1 个故障域"), "{w:?}");
    let full = map((1..=3).map(|n| (n, target(n, &[], TARGET_UP))).collect(), vec![class(1, "*", false)]);
    assert!(crate::meta::cluster::map::warnings(&full).is_empty(), "3 个故障域：没有警告");
}

/// M9：降级按 Serving 成员数算（Syncing 不算副本）；没有 Serving 成员的链单独一条"等待最后的副本"；一个节点公布了别的节点没有的
/// 数据传输 → "n→m 的数据复制不能走 X"（3.3.3）。
#[test]
fn warnings_cover_degraded_and_unavailable_chains_and_transport_fallback() {
    use crate::meta::schema::records::{NODE_ACTIVE, NodeRecV1};
    let mut m = map((1..=3).map(|n| (n, target(n, &[], TARGET_UP))).collect(), vec![class(1, "*", false)]);
    let c = |members: Vec<(u32, u8)>, last| ChainRecV1 {
        ver: 1,
        incarnation: 1,
        class_id: 1,
        replicas: 3,
        members,
        last_serving: last,
        removed: vec![],
    };
    m.chains = vec![(1, c(vec![(1, SERVING), (2, SERVING), (3, SYNCING)], None)), (2, c(vec![(3, SYNCING)], Some(2)))];
    let node = |addrs: &[&str]| NodeRecV1 {
        addrs: addrs.iter().map(|s| (s.to_string(), "a".to_string())).collect(),
        mgmt_addr: String::new(),
        host_id: String::new(),
        machine_id: String::new(),
        hostname: String::new(),
        meta_role: 0,
        features: vec![],
        joined_ms: 0,
        state: NODE_ACTIVE,
    };
    m.nodes = vec![(1, node(&["tcp", "fast"])), (2, node(&["tcp"]))];
    let w = crate::meta::cluster::map::warnings(&m);
    assert!(
        w.iter().any(|x| x.starts_with("有 2 条链少于声明副本数") && x.contains("Serving 2 个、Syncing 1 个")),
        "{w:?}"
    );
    assert!(w.iter().any(|x| x.contains("链 2 等待最后的副本 target 2") && x.contains("force-serving")), "{w:?}");
    assert!(w.iter().any(|x| x.contains("节点 1→2 的数据复制不能走 fast")), "{w:?}");
    assert!(!w.iter().any(|x| x.contains("节点 2→1")), "{w:?}");
}
