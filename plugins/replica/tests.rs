//! replica 的单元测试：manifest、每个成员存整块、读顺序（同节点优先、其余按 chunk 轮转、不丢不重）。

use wmfs_core::hooks::manifest::check_manifest_toml;

use super::*;

#[test]
fn manifest_toml_matches_code() {
    check_manifest_toml(&MANIFEST, include_str!("manifest.toml")).unwrap();
}

#[test]
fn whole_shards_and_read_order() {
    let r = Replica::new();
    assert_eq!(r.codec_id(), 1);
    assert_eq!(r.shards(3), vec![ShardSpec::Whole; 3]);
    let serving = [(1, 10), (2, 20), (3, 30)];
    let c = |idx| ChunkId { chain: 1, ino: 16, epoch: 1, idx };
    let mut firsts = std::collections::BTreeSet::new();
    for idx in 0..6 {
        let mut o = r.read_order(&serving, None, &c(idx));
        firsts.insert(o[0]);
        o.sort();
        assert_eq!(o, vec![1, 2, 3], "每个副本恰好出现一次");
    }
    assert_eq!(firsts.len(), 3, "不同的 chunk 先问不同的副本（分散读）");
    for idx in 0..6 {
        assert_eq!(r.read_order(&serving, Some(20), &c(idx))[0], 2, "同节点的副本优先");
    }
    assert!(r.read_order(&[], None, &c(0)).is_empty());
}
