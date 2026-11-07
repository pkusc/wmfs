//! tiering 的单元测试：manifest 与代码一致、任务的基本信息（迁移本身由 `tests/it/classes.rs` 在完整的节点上测）。

use wmfs_core::hooks::manifest::check_manifest_toml;

use super::*;

#[test]
fn manifest_toml_matches_code() {
    check_manifest_toml(&MANIFEST, include_str!("manifest.toml")).unwrap();
}

#[test]
fn runs_on_the_leader_every_day() {
    let t = Tiering::new();
    assert_eq!((t.kind(), t.runs_on(), t.interval()), ("tiering", RunOn::MetaLeader, Duration::from_secs(86_400)));
}
