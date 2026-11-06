//! xxh3 的单元测试：manifest、往返、校验和写在前 8 字节、篡改任意一个字节都被发现（4.8 的 `transform.xxh3.corrupt` 用例）。

use wmfs_core::base::types::ChunkId;
use wmfs_core::buf::{Lane, PoolConfig};
use wmfs_core::hooks::manifest::check_manifest_toml;

use super::*;

#[test]
fn manifest_toml_matches_code() {
    check_manifest_toml(&MANIFEST, include_str!("manifest.toml")).unwrap();
}

#[test]
fn round_trip_and_any_flipped_byte_is_caught() {
    let pool = MemPool::new(&PoolConfig { s_mb: 1, m_slots: 1, l_slots: 1, repl_lane: false }).unwrap();
    let chunk = ChunkId { chain: 1, ino: 16, epoch: 1, idx: 0 };
    let ctx = TransformCtx { chunk: &chunk, block_idx: 0, level: 0, key_id: 0, key: None, aad: b"" };
    let p = Xxh3::new();
    assert_eq!((p.kind(), p.alg_id()), (TransformKind::Checksum, 2));
    let want: Vec<u8> = (0..1000).map(|i| (i * 7) as u8).collect();
    let fresh = || {
        let mut b = pool.get(1000, Lane::Normal, std::time::Duration::ZERO).unwrap();
        b.as_mut_slice().copy_from_slice(&want);
        b
    };
    let mut meta = BlockMeta { plain_len: 1000, ..Default::default() };
    let b = p.encode(&ctx, fresh(), &mut meta, &pool).unwrap();
    assert_eq!(&meta.checksum[..8], &xxhash_rust::xxh3::xxh3_64(&want).to_le_bytes(), "小端写在前 8 字节");
    assert!(meta.checksum[8..].iter().all(|x| *x == 0));
    assert_eq!(p.decode(&ctx, b, &meta, &pool).unwrap().as_slice(), &want[..]);
    for i in 0..1000usize {
        let mut b = fresh();
        b.as_mut_slice()[i] ^= 0x01;
        let r = p.decode(&ctx, b, &meta, &pool);
        assert_eq!(r.as_ref().err(), Some(&ErrCode::CHECKSUM_MISMATCH), "篡改第 {i} 个字节必须被发现");
    }
}
