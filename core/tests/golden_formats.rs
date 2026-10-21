//! 金样：chunk 文件固定头与块表（design.md 3.2.4）、块的 AES-GCM 附加数据、target 超级块（3.2.3）。

mod common;

use common::Golden;
use wmfs_core::data::format::*;
use wmfs_core::data::superblock::TargetSuperblock;

fn header(chunk_size: u32, logical_len: u32) -> ChunkHeader {
    let (block_size, nblocks, header_len_4k) = layout_for(chunk_size).expect("合法的 chunk_size");
    ChunkHeader {
        header_len_4k,
        features_compat: 0x10,    // compat、ro_compat 取互不相同的值：两个偏移写反会被发现
        features_ro_compat: 0x20, // （decode 不拒绝不认识的 compat/ro_compat 位）
        features_incompat: INCOMPAT_BLOCK_COMPRESS | INCOMPAT_BLOCK_ENCRYPT,
        ino: 0x1_0000_0010,
        epoch: 2,
        chunk_idx: 5,
        chain_id: 7,
        block_size,
        chunk_version: (2u64 << 40) + 3,
        chunk_size,
        logical_len,
        nblocks,
        checksum_alg: CK_BLAKE3,
        compress_alg: CMP_ZSTD,
        compress_level: 5,
        encrypt_alg: ENC_AES256GCM,
        encrypt_key_id: 9,
        snap_epoch: 6, // 首发写 0；样本用非 0 值钉住它的偏移
        cluster_id: [0xC1; 16],
        write_time_ms: 1_792_454_400_000,
    }
}

fn entry(i: u8) -> BlockEntry {
    let mut e = BlockEntry {
        stored_len: 1000 + i as u32,
        plain_len: 65536,
        flags: BLK_COMPRESSED | BLK_ENCRYPTED,
        ..Default::default()
    };
    e.nonce = [i; 12];
    e.gcm_tag = [0x80 | i; 16];
    e.checksum[..4].copy_from_slice(&(0xC0DE_0000u32 + i as u32).to_le_bytes());
    e
}

#[test]
fn golden_chunk_header() {
    let mut g = Golden::new("chunk");
    // 4 MiB chunk：64 个块，头 + 块表占 2 个 4 KiB；第 0、1 块有数据，其余是洞
    let h = header(4 << 20, 70_000);
    let mut entries = vec![BlockEntry::default(); h.nblocks as usize];
    entries[0] = entry(0);
    entries[1] = entry(1);
    let table = encode_table(&entries);
    let fixed = h.encode(&table);
    assert_eq!(ChunkHeader::decode(&fixed, &table), Ok(h));
    g.add("header.4M", fixed.to_vec());
    g.add("table.4M", table);
    // 64 KiB chunk：1 个块，头 + 块表占 1 个 4 KiB
    let h = header(64 << 10, 10);
    let table = encode_table(&[entry(7)]);
    g.add("header.64K", h.encode(&table).to_vec());
    g.add("BlockEntry", entry(7).encode().to_vec());
    g.add("block_aad", block_aad(&[0xC1; 16], 0x1_0000_0010, 2, 5, 1));
    g.check();
}

#[test]
fn golden_target_superblock() {
    let mut g = Golden::new("target_superblock");
    let sb = TargetSuperblock {
        features_compat: 0x11,
        features_ro_compat: 0x22,
        features_incompat: 0, // 只能是 0：首发没有已知的 incompat 位，非 0 会被 decode 拒绝
        cluster_id: [0xC1; 16],
        target_id: 2,
        node_id: 3,
        created_ms: 1_792_454_400_000,
    };
    let b = sb.encode();
    assert_eq!(TargetSuperblock::decode(&b), Ok(sb));
    g.add("TARGET", b);
    g.check();
}
