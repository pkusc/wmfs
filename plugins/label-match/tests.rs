//! label-match 的单元测试（design.md 9.1 的"placement / 链生成"里插件的部分）：manifest、按 class/replicas/Serving/exclude
//! 过滤、按 Serving 成员数与使用率排序、同一个 seed 同一个结果、不同的 seed 把文件分散到各条链。

use wmfs_core::hooks::manifest::check_manifest_toml;
use wmfs_core::meta::attrs::EffectiveAttrs;
use wmfs_core::meta::schema::records::StorageClassV1;

use super::*;

fn class(id: u16) -> StorageClassV1 {
    StorageClassV1 {
        id,
        name: "c".into(),
        label: "*".into(),
        dedicated: false,
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

fn chain(id: u32, class_id: u16, replicas: u8, serving: u8, used: u64, capacity: u64) -> ChainView {
    ChainView { id, class_id, replicas, serving, members: vec![(id, id)], used, capacity }
}

fn rank(cands: &[ChainView], seed: u64, exclude: &[u32]) -> Vec<u32> {
    let eff = EffectiveAttrs::defaults(1, 3);
    let c = class(1);
    let input =
        PlacementInput { eff: &eff, class: &c, replicas: 3, stripe: 1, client_node: None, seed, exclude, size_hint: 0 };
    let mut v = cands.to_vec();
    LabelMatch::new().rank(&input, &mut v).unwrap();
    v.iter().map(|c| c.id).collect()
}

#[test]
fn manifest_toml_matches_code() {
    check_manifest_toml(&MANIFEST, include_str!("manifest.toml")).unwrap();
}

#[test]
fn filters_by_class_replicas_serving_and_exclude() {
    let cands = [
        chain(1, 1, 3, 1, 0, 0),
        chain(2, 2, 3, 1, 0, 0), // 别的存储类
        chain(3, 1, 2, 1, 0, 0), // 副本数不同
        chain(4, 1, 3, 0, 0, 0), // 没有 Serving 成员（不可用）
        chain(5, 1, 3, 1, 0, 0),
    ];
    let mut got = rank(&cands, 7, &[]);
    got.sort();
    assert_eq!(got, vec![1, 5]);
    assert_eq!(rank(&cands, 7, &[5]), vec![1], "exclude 里的链不要");
}

#[test]
fn more_serving_members_then_lower_usage_first() {
    let cands = [chain(1, 1, 3, 1, 0, 100), chain(2, 1, 3, 3, 90, 100), chain(3, 1, 3, 3, 10, 100)];
    assert_eq!(rank(&cands, 1, &[]), vec![3, 2, 1], "Serving 多的优先，再按使用率");
    for seed in 0..50 {
        assert_eq!(rank(&cands, seed, &[]), vec![3, 2, 1], "前两个排序键分得出先后时与 seed 无关");
    }
}

#[test]
fn same_seed_same_order_and_seeds_spread_files_over_chains() {
    let cands: Vec<ChainView> = (1..=4).map(|i| chain(i, 1, 3, 3, 0, 0)).collect();
    for seed in [0, 1, 42, u64::MAX] {
        assert_eq!(rank(&cands, seed, &[]), rank(&cands, seed, &[]), "确定性：seed {seed}");
    }
    let mut first = [0usize; 5];
    for seed in 16..216 {
        first[rank(&cands, seed, &[])[0] as usize] += 1;
    }
    assert!(first[1..].iter().all(|n| *n >= 20), "200 个文件应大致均匀地分到 4 条链上：{first:?}");
}
