//! gc 的单元测试：manifest、任务的基本属性、本地孤儿扫描的游标。删 epoch 与孤儿、清理元数据、本地孤儿扫描的行为在 testkit 里经完整的
//! 节点测（`data_gc`、`gc_full`）。

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
    let l = GcLocal::new();
    assert_eq!((l.kind(), l.runs_on(), l.interval()), ("gc-local", RunOn::EveryDataNode, Duration::from_secs(600)));
    assert!(std::ptr::eq(l.manifest(), g.manifest()), "同一个插件的两个任务");
}

#[test]
fn local_cursors_round_trip_and_garbage_starts_over() {
    use wmfs_core::proto::{decode_body, encode_body};
    let c = local::Cursor { target: 3, after: Some((7, 1u64 << 40, 9)) };
    assert_eq!(decode_body::<local::Cursor>(&encode_body(&c)), Ok(c));
    assert_eq!(
        decode_body::<local::Cursor>(&[]).unwrap_or_default(),
        local::Cursor::default(),
        "空游标 = 从第一个 target 开始"
    );
    let junk = decode_body::<local::Cursor>(&[0xff; 3]).unwrap_or_default();
    assert_eq!(junk, local::Cursor::default(), "解不开的游标也从第一个 target 开始");
}

/// 旧链目录留不留（`keep_chain_dir`）：是成员、是 last serving、在 rejoin_window 之内被移出的留着；移出超过窗口的、早就不是成员的删掉；
/// 集群图里没有的链（列表比集群图新：刚生成的）留着。
#[test]
fn old_chain_directories_are_kept_unless_the_target_can_never_return() {
    use wmfs_core::meta::schema::records::ChainRecV1;
    let c = |members: Vec<(u32, u8)>, last_serving, removed| ChainRecV1 {
        ver: 1,
        incarnation: 1,
        class_id: 1,
        replicas: 2,
        members,
        last_serving,
        removed,
    };
    let (t, window, now) = (5, 1000, 10_000);
    assert!(local::keep_chain_dir(Some(&c(vec![(5, 2)], None, vec![])), t, window, now), "是成员（Syncing 也算）");
    assert!(local::keep_chain_dir(Some(&c(vec![], Some(5), vec![])), t, window, now), "链在等它回来（last_serving）");
    assert!(local::keep_chain_dir(Some(&c(vec![(1, 1)], None, vec![(5, 9500)])), t, window, now), "刚被移出：窗口之内");
    assert!(!local::keep_chain_dir(Some(&c(vec![(1, 1)], None, vec![(5, 8000)])), t, window, now), "移出超过窗口");
    assert!(!local::keep_chain_dir(Some(&c(vec![(1, 1)], None, vec![])), t, window, now), "早就不是成员");
    assert!(local::keep_chain_dir(None, t, window, now), "集群图里没有这条链：不删");
}
