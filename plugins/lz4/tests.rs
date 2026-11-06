//! lz4 的单元测试：manifest、压得下去的块往返、压不下去的块存原文（不置 COMPRESSED）、篡改压缩后的任意一个字节——要么解不开，
//! 要么解出来的明文不同（之后被校验和查出来），绝不会悄悄解出原来的明文以外的东西还当作对（4.8 的 `transform.lz4.corrupt` 用例）。

use wmfs_core::base::rand::XorShift64;
use wmfs_core::base::types::ChunkId;
use wmfs_core::buf::PoolConfig;
use wmfs_core::hooks::manifest::check_manifest_toml;

use super::*;

#[test]
fn manifest_toml_matches_code() {
    check_manifest_toml(&MANIFEST, include_str!("manifest.toml")).unwrap();
}

fn buf(pool: &MemPool, data: &[u8]) -> Buf {
    let mut b = pool.get(data.len(), Lane::Normal, POOL_WAIT).unwrap();
    b.as_mut_slice().copy_from_slice(data);
    b
}

#[test]
fn compressible_blocks_round_trip_and_random_ones_are_stored() {
    let pool = MemPool::new(&PoolConfig { s_mb: 2, m_slots: 1, l_slots: 1, repl_lane: false }).unwrap();
    let chunk = ChunkId { chain: 1, ino: 16, epoch: 1, idx: 0 };
    let ctx = TransformCtx { chunk: &chunk, block_idx: 0, level: 0, key_id: 0, key: None, aad: b"" };
    let p = Lz4::new();
    assert_eq!((p.kind(), p.alg_id()), (TransformKind::Compress, 1));
    let text: Vec<u8> = b"WMFS-PLAINTEXT-MARKER 7\n".iter().copied().cycle().take(65536).collect();
    let mut meta = BlockMeta { plain_len: 65536, ..Default::default() };
    let z = p.encode(&ctx, buf(&pool, &text), &mut meta, &pool).unwrap();
    assert!(meta.flags & BLK_COMPRESSED != 0 && z.len() < 4096, "重复的文本压得很小：{}", z.len());
    let packed = z.as_slice().to_vec();
    assert_eq!(p.decode(&ctx, z, &meta, &pool).unwrap().as_slice(), &text[..]);
    for i in 0..packed.len() {
        let mut bad = packed.clone();
        bad[i] ^= 0x40;
        match p.decode(&ctx, buf(&pool, &bad), &meta, &pool) {
            Err(e) => assert_eq!(e, ErrCode::CHECKSUM_MISMATCH),
            Ok(b) => assert_ne!(b.as_slice(), &text[..], "篡改第 {i} 个字节后解出了原文：篡改没被发现"),
        }
    }
    let mut noise = vec![0u8; 65536];
    XorShift64::new(5).fill(&mut noise);
    let mut meta = BlockMeta { plain_len: 65536, ..Default::default() };
    let s = p.encode(&ctx, buf(&pool, &noise), &mut meta, &pool).unwrap();
    assert_eq!((meta.flags & BLK_COMPRESSED, s.as_slice()), (0, &noise[..]), "压不下去：原样返回，不置 COMPRESSED");
}
