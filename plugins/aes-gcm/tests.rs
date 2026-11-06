//! aes-gcm 的单元测试：manifest、往返、原地加密（同一个缓冲、密文不是明文）、每次的 nonce 都不同、篡改密文的任意一个字节或 tag、
//! 换了 AAD（块被挪到别处）、换了密钥都解不开（4.8 的 `transform.aes-gcm.corrupt` 用例）；没有密钥 → NOT_READY。

use wmfs_core::base::types::ChunkId;
use wmfs_core::buf::{Lane, POOL_WAIT, PoolConfig};
use wmfs_core::hooks::manifest::check_manifest_toml;

use super::*;

#[test]
fn manifest_toml_matches_code() {
    check_manifest_toml(&MANIFEST, include_str!("manifest.toml")).unwrap();
}

#[test]
fn round_trip_and_any_tampering_is_caught() {
    let pool = MemPool::new(&PoolConfig { s_mb: 1, m_slots: 1, l_slots: 1, repl_lane: false }).unwrap();
    let chunk = ChunkId { chain: 1, ino: 16, epoch: 1, idx: 0 };
    let (key, other) = ([7u8; 32], [8u8; 32]);
    let ctx = TransformCtx { chunk: &chunk, block_idx: 0, level: 0, key_id: 3, key: Some(&key), aad: b"aad-1" };
    let p = AesGcm::new();
    assert_eq!((p.kind(), p.alg_id()), (TransformKind::Encrypt, 1));
    let want: Vec<u8> = b"WMFS-PLAINTEXT-MARKER 1\n".iter().copied().cycle().take(4000).collect();
    let fresh = |d: &[u8]| {
        let mut b = pool.get(d.len(), Lane::Normal, POOL_WAIT).unwrap();
        b.as_mut_slice().copy_from_slice(d);
        b
    };
    let mut meta = BlockMeta { plain_len: 4000, ..Default::default() };
    let c = p.encode(&ctx, fresh(&want), &mut meta, &pool).unwrap();
    assert!(meta.flags & BLK_ENCRYPTED != 0 && c.len() == 4000 && c.as_slice() != &want[..], "原地加密，长度不变");
    let ct = c.as_slice().to_vec();
    let mut meta2 = BlockMeta { plain_len: 4000, ..Default::default() };
    p.encode(&ctx, fresh(&want), &mut meta2, &pool).unwrap();
    assert_ne!(meta.nonce, meta2.nonce, "每次加密都用新的随机 nonce");
    assert_eq!(p.decode(&ctx, c, &meta, &pool).unwrap().as_slice(), &want[..]);
    for i in (0..ct.len()).step_by(37) {
        let mut bad = ct.clone();
        bad[i] ^= 1;
        assert_eq!(
            p.decode(&ctx, fresh(&bad), &meta, &pool).err(),
            Some(ErrCode::CHECKSUM_MISMATCH),
            "密文第 {i} 字节"
        );
    }
    let mut m = BlockMeta { ..meta };
    m.tag[5] ^= 1;
    assert_eq!(p.decode(&ctx, fresh(&ct), &m, &pool).err(), Some(ErrCode::CHECKSUM_MISMATCH), "tag 被改");
    let moved = TransformCtx { aad: b"aad-2", ..ctx };
    assert_eq!(p.decode(&moved, fresh(&ct), &meta, &pool).err(), Some(ErrCode::CHECKSUM_MISMATCH), "块被挪到别处");
    let wrong = TransformCtx { key: Some(&other), ..ctx };
    assert_eq!(p.decode(&wrong, fresh(&ct), &meta, &pool).err(), Some(ErrCode::CHECKSUM_MISMATCH), "换了密钥");
    let none = TransformCtx { key: None, ..ctx };
    assert_eq!(p.decode(&none, fresh(&ct), &meta, &pool).err(), Some(ErrCode::NOT_READY), "还没拿到密钥");
}
