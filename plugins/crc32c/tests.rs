//! crc32c 的单元测试：manifest、往返、篡改输出的任意一个字节都能被发现（4.8 的 `transform.<名>.corrupt` 用例：1000 个字节逐个翻转）、
//! 校验和写在前 4 字节。

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
    let p = Crc32c::new();
    assert_eq!((p.kind(), p.alg_id()), (TransformKind::Checksum, 1));
    let mut b = pool.get(1000, Lane::Normal, std::time::Duration::ZERO).unwrap();
    for (i, x) in b.as_mut_slice().iter_mut().enumerate() {
        *x = i as u8;
    }
    let mut meta = BlockMeta { plain_len: 1000, ..Default::default() };
    let b = p.encode(&ctx, b, &mut meta, &pool).unwrap();
    let want: Vec<u8> = (0..1000).map(|i| i as u8).collect();
    assert_eq!(&meta.checksum[..4], &crc32c::crc32c(&want).to_le_bytes(), "小端写在前 4 字节");
    assert!(meta.checksum[4..].iter().all(|x| *x == 0));
    let mut b = p.decode(&ctx, b, &meta, &pool).unwrap();
    assert_eq!(b.as_slice(), &want[..], "原样返回同一个缓冲");
    for i in 0..1000usize {
        b.as_mut_slice()[i] ^= 0x10;
        let r = p.decode(&ctx, b, &meta, &pool);
        assert_eq!(r.as_ref().err(), Some(&ErrCode::CHECKSUM_MISMATCH), "篡改第 {i} 个字节必须被发现");
        b = pool.get(1000, Lane::Normal, std::time::Duration::ZERO).unwrap();
        b.as_mut_slice().copy_from_slice(&want);
    }
}
