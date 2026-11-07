//! ttl 的单元测试：manifest、任务的基本属性、游标（认不出来的游标从头开始）。真的按 mtime 删除在 testkit 里经完整的节点测（`meta_trash`）。

use wmfs_core::hooks::manifest::check_manifest_toml;

use super::*;

#[test]
fn manifest_toml_matches_code() {
    check_manifest_toml(&MANIFEST, include_str!("manifest.toml")).unwrap();
}

#[test]
fn runs_on_the_leader_every_minute_and_cursors_round_trip() {
    let t = Ttl::new();
    assert_eq!((t.kind(), t.runs_on(), t.interval()), ("ttl", RunOn::MetaLeader, Duration::from_secs(60)));
    let c: (u64, Vec<u8>) = (1 << 40, vec![1, 2, 3]);
    assert_eq!(decode_body::<(u64, Vec<u8>)>(&encode_body(&c)), Ok(c));
    assert_eq!(decode_body::<(u64, Vec<u8>)>(&[]).unwrap_or_default(), (0, vec![]), "空游标 = 一轮的开头");
}
