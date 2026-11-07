//! rebalance 的单元测试：manifest；"差距最大的那一组、最满的链、要搬多少"的纯函数（只比同存储类同副本数的、不看没有容量或没有 Serving 成员的）。

use wmfs_core::hooks::manifest::check_manifest_toml;

use super::*;

fn chain(id: u32, class_id: u16, replicas: u8, used: u64, capacity: u64) -> ChainView {
    ChainView { id, class_id, replicas, serving: replicas, members: vec![], used, capacity }
}

#[test]
fn manifest_toml_matches_code() {
    check_manifest_toml(&MANIFEST, include_str!("manifest.toml")).unwrap();
}

#[test]
fn picks_the_widest_gap_within_a_class() {
    let v = [
        chain(1, 1, 2, 80, 100),
        chain(2, 1, 2, 30, 100),
        chain(3, 1, 2, 50, 100),
        chain(4, 2, 2, 90, 100), // 别的类：只有一条，不比
        chain(5, 1, 3, 0, 100),  // 副本数不同：另一组，也只有一条
        chain(6, 1, 2, 0, 0),    // 容量未知：不比
    ];
    let (gap, full, bytes) = pick(&v).unwrap();
    assert!((gap - 0.5).abs() < 1e-9);
    assert_eq!((full, bytes), (1, 25), "最满的是 1，搬两者已用之差的一半");
    assert!(pick(&[chain(1, 1, 2, 10, 100)]).is_none(), "只有一条链：没得比");
    let even = [chain(1, 1, 2, 50, 100), chain(2, 1, 2, 45, 100)];
    assert!(pick(&even).unwrap().0 < START, "5% 的差距不到开始的门限");
}
