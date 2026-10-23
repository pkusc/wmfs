//! cache-memory 的单元测试：manifest、命中与没命中、键里带 change_id（文件变了旧条目查不到）、LRU 淘汰最久没用的、
//! 超过容量的块不放、invalidate_ino、指标。

use super::*;
use wmfs_core::hooks::manifest::check_manifest_toml;

fn k(ino: u64, change_id: u64, block: u64) -> CacheKey {
    CacheKey { ino, change_id, block }
}

#[test]
fn manifest_toml_matches_code() {
    check_manifest_toml(&MANIFEST, include_str!("manifest.toml")).unwrap();
}

#[test]
fn hits_misses_and_change_id_in_the_key() {
    let c = CacheMemory::new(1 << 20);
    let (h0, m0) = (c.hits.get(), c.misses.get());
    let mut buf = [0u8; 8];
    assert_eq!(c.get(&k(1, 1, 0), &mut buf), None);
    c.put(&k(1, 1, 0), b"hello");
    assert_eq!(c.get(&k(1, 1, 0), &mut buf), Some(5));
    assert_eq!(&buf[..5], b"hello");
    assert_eq!(c.get(&k(1, 2, 0), &mut buf), None, "文件变了（change_id 不同）：旧条目查不到");
    assert_eq!(c.get(&k(1, 1, 1), &mut buf), None, "别的块");
    assert!(c.hits.get() > h0 && c.misses.get() >= m0 + 3);
}

#[test]
fn lru_evicts_the_least_recently_used_and_skips_oversized_blocks() {
    let c = CacheMemory::new(10);
    c.put(&k(1, 1, 0), b"aaaa");
    c.put(&k(1, 1, 1), b"bbbb");
    let mut buf = [0u8; 4];
    assert!(c.get(&k(1, 1, 0), &mut buf).is_some()); // 0 号块刚用过：1 号块成了最久没用的
    c.put(&k(1, 1, 2), b"cccc");
    assert!(c.get(&k(1, 1, 1), &mut buf).is_none(), "淘汰最久没用的");
    assert!(c.get(&k(1, 1, 0), &mut buf).is_some() && c.get(&k(1, 1, 2), &mut buf).is_some());
    c.put(&k(1, 1, 3), &[7u8; 11]);
    assert!(c.get(&k(1, 1, 3), &mut [0u8; 11]).is_none(), "比容量还大的块不放");
    c.put(&k(1, 1, 0), b"zz");
    assert_eq!(c.get(&k(1, 1, 0), &mut buf), Some(2), "同一个键放两次：新的盖旧的");
    assert_eq!(lock(&c.lru).bytes, 6);
}

#[test]
fn invalidate_ino_drops_all_blocks_of_that_file() {
    let c = CacheMemory::new(1 << 20);
    c.put(&k(1, 1, 0), b"x");
    c.put(&k(1, 2, 5), b"y");
    c.put(&k(2, 1, 0), b"z");
    c.invalidate_ino(1);
    let mut b = [0u8; 1];
    assert!(c.get(&k(1, 1, 0), &mut b).is_none() && c.get(&k(1, 2, 5), &mut b).is_none());
    assert_eq!(c.get(&k(2, 1, 0), &mut b), Some(1), "别的文件不受影响");
    assert_eq!(lock(&c.lru).bytes, 1);
}
