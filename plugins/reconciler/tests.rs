//! reconciler 的单元测试：manifest、任务的基本属性、游标的编码（认不出来的游标从头开始）。迁移、断点续跑、跳过写打开的文件、
//! RStatDrift 的修正在 testkit 里经完整的节点测（`bg_reconcile`）。

use wmfs_core::hooks::manifest::check_manifest_toml;

use super::*;

#[test]
fn manifest_toml_matches_code() {
    check_manifest_toml(&MANIFEST, include_str!("manifest.toml")).unwrap();
}

#[test]
fn runs_on_the_leader_every_second() {
    let r = Reconciler::new();
    assert_eq!((r.kind(), r.runs_on(), r.interval()), ("reconciler", RunOn::MetaLeader, Duration::from_secs(1)));
}

#[test]
fn cursors_round_trip_and_garbage_starts_over() {
    for c in [Cursor::Walk { seq: 1 << 40, skipped: true, dfs: vec![9, 8, 7] }, Cursor::Full { after: 77 }] {
        assert_eq!(decode_body::<Cursor>(&encode_body(&c)), Ok(c));
    }
    for junk in [&[][..], &[7, 0][..]] {
        assert_eq!(decode_body::<Cursor>(junk).unwrap_or_default(), Cursor::Start, "{junk:?}：认不出来就从头开始");
    }
}
