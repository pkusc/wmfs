//! trash-expire 的单元测试：manifest、任务的基本属性、TRASH_INDEX 键的解析。真的过期删除在 testkit 里经完整的节点测（`meta_trash`）。

use wmfs_core::hooks::manifest::check_manifest_toml;

use super::*;

#[test]
fn manifest_toml_matches_code() {
    check_manifest_toml(&MANIFEST, include_str!("manifest.toml")).unwrap();
}

#[test]
fn runs_on_the_leader_and_parses_index_keys() {
    let t = TrashExpire::new();
    assert_eq!((t.kind(), t.runs_on(), t.interval()), ("trash-expire", RunOn::MetaLeader, Duration::from_secs(10)));
    assert_eq!(parse_trash_index(&keys::trash_index(1 << 40, 77)), Some((1 << 40, 77)));
    assert_eq!(parse_trash_index(&[0x0F, 1, 2]), None);
    assert_eq!(parse_trash_index(&keys::gc(1, 77, 0)), None, "别的键空间：不认");
}
