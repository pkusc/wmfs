//! scrub 的单元测试：manifest、任务的基本属性（每个数据节点、24 h 一轮、默认 50 MiB/s）、游标的编码。检查、修复与上报在 testkit 里经
//! 真的数据节点测（`scrub_repair`）。

use wmfs_core::hooks::manifest::check_manifest_toml;

use super::*;

#[test]
fn manifest_toml_matches_code() {
    check_manifest_toml(&MANIFEST, include_str!("manifest.toml")).unwrap();
}

#[test]
fn runs_on_every_data_node_daily_at_50_mib_per_second() {
    let s = Scrub::new();
    assert_eq!((s.kind(), s.runs_on(), s.interval()), ("scrub", RunOn::EveryDataNode, Duration::from_secs(86400)));
    assert_eq!(s.default_rate(), 50 << 20);
}

#[test]
fn cursors_round_trip_and_garbage_starts_the_pass_over() {
    let id = ChunkId { chain: 3, ino: 1 << 40, epoch: 2, idx: 9 };
    let c = Cursor { target: 7, chain: 3, phase: 1, after: Some(id) };
    assert_eq!(decode_body::<Cursor>(&encode_body(&c)), Ok(c));
    for junk in [&[][..], &[9, 9, 9][..]] {
        assert_eq!(decode_body::<Cursor>(junk).unwrap_or_default(), Cursor::default(), "{junk:?}：这一轮的开头");
    }
}
