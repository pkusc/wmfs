//! gc 的单元测试：manifest 与任务的基本属性。删 epoch 与孤儿的行为在 testkit 里经完整的节点测（`data_gc`）。

use wmfs_core::hooks::manifest::check_manifest_toml;

use super::*;

#[test]
fn manifest_toml_matches_code() {
    check_manifest_toml(&MANIFEST, include_str!("manifest.toml")).unwrap();
}

#[test]
fn runs_on_the_leader_every_second() {
    let g = Gc::new();
    assert_eq!((g.kind(), g.runs_on(), g.interval()), ("gc", RunOn::MetaLeader, Duration::from_secs(1)));
}
