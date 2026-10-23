//! chunk 文件格式 v1（design.md 3.2.4，ADR-0021）的字节布局与编解码（target 超级块在 superblock.rs）。
//!
//! 一个 chunk 版本 = 一个文件 = 固定头（256 字节）+ 块表（nblocks × 72 字节）+ 数据区。数据区从
//! `header_len_4k × 4096` 开始（4 KiB 对齐），第 i 块固定在 `header_len_4k × 4096 + i × block_size`，只写 `stored_len`
//! 字节（压缩省下的空间与洞靠稀疏文件真正省下来）。`block_size = min(65536, chunk_size)`；`chunk_size` 取
//! 64 KiB..=16 MiB 的 2 的幂。全部整数小端。字节布局固定，扩展只能用保留字段 + feature 位（3.2.11）。
//!
//! 固定头：0 magic `WMFSCHK1` | 8 format_version | 10 header_len_4k | 12 features_compat | 16 features_ro_compat |
//! 20 features_incompat | 24 ino | 32 epoch | 36 chunk_idx | 40 chain_id | 44 block_size | 48 chunk_version |
//! 56 chunk_size | 60 logical_len | 64 nblocks | 68 checksum_alg | 69 compress_alg | 70 compress_level(i8) |
//! 71 encrypt_alg | 72 encrypt_key_id | 76 snap_epoch | 80 cluster_id(16) | 96 write_time_ms | 104..252 保留 0 |
//! 252 header_crc = crc32c(固定头 0..252 ‖ 整个块表)。
//! 块表条目：0 stored_len | 4 plain_len | 8 flags | 9..12 保留 | 12 nonce(12) | 24 gcm_tag(16) | 40 checksum(32)。

use crate::base::ErrCode;

pub const CHUNK_MAGIC: [u8; 8] = *b"WMFSCHK1";
pub const CHUNK_FORMAT_VERSION: u16 = 1;
pub const CHUNK_HEADER_LEN: usize = 256;
pub const BLOCK_ENTRY_LEN: usize = 72;
pub const MAX_BLOCK_SIZE: u32 = 65536;
pub const MIN_CHUNK_SIZE: u32 = 64 << 10;
pub const MAX_CHUNK_SIZE: u32 = 16 << 20;
pub const MAX_NBLOCKS: u32 = MAX_CHUNK_SIZE / MAX_BLOCK_SIZE; // 256

// features_incompat：不认识的位 → 拒绝读（FEATURE_UNSUPPORTED）；ro_compat 不认识 → 只读；compat → 忽略
pub const INCOMPAT_BLOCK_COMPRESS: u32 = 1 << 0; // 有块被压缩
pub const INCOMPAT_BLOCK_ENCRYPT: u32 = 1 << 1; // 有块被加密
// bit2 保留给二期的动态库压缩算法（DYN_TRANSFORM，首发不实现、不写）
pub const INCOMPAT_KNOWN: u32 = INCOMPAT_BLOCK_COMPRESS | INCOMPAT_BLOCK_ENCRYPT;
pub const RO_COMPAT_KNOWN: u32 = 0;

// 块表条目的 flags
pub const BLK_COMPRESSED: u8 = 1 << 0; // 本块真的压缩了
pub const BLK_ENCRYPTED: u8 = 1 << 1;

// 算法编号（写进 chunk 头，也出现在 LayoutV1 与 CapV1 里）
pub const CK_CRC32C: u8 = 1;
pub const CK_XXH3: u8 = 2;
pub const CK_BLAKE3: u8 = 3;
pub const CMP_NONE: u8 = 0;
pub const CMP_LZ4: u8 = 1;
pub const CMP_ZSTD: u8 = 2; // 128..=255 保留给二期动态库算法
pub const ENC_NONE: u8 = 0;
pub const ENC_AES256GCM: u8 = 1;

/// 算法编号的名字（`wmfs debug chunk-header` 等给人看的输出用；不认识的编号 → "?"）。实现注记（M5）：新增（只增）。
pub fn checksum_name(alg: u8) -> &'static str {
    match alg {
        CK_CRC32C => "crc32c",
        CK_XXH3 => "xxh3",
        CK_BLAKE3 => "blake3",
        _ => "?",
    }
}
pub fn compress_name(alg: u8) -> &'static str {
    match alg {
        CMP_NONE => "none",
        CMP_LZ4 => "lz4",
        CMP_ZSTD => "zstd",
        _ => "?",
    }
}
pub fn encrypt_name(alg: u8) -> &'static str {
    match alg {
        ENC_NONE => "none",
        ENC_AES256GCM => "aes-256-gcm",
        _ => "?",
    }
}

/// chunk 文件的固定头（magic 与 format_version 由 encode/decode 处理）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChunkHeader {
    pub header_len_4k: u16, // 头 + 块表占多少个 4 KiB；数据区从 header_len_4k × 4096 开始
    pub features_compat: u32,
    pub features_ro_compat: u32,
    pub features_incompat: u32,
    pub ino: u64, // 所属 inode
    pub epoch: u32,
    pub chunk_idx: u32,
    pub chain_id: u32,
    pub block_size: u32,
    pub chunk_version: u64,  // chunk 版本（3.4.2.2）
    pub chunk_size: u32,     // 逻辑最大长度
    pub logical_len: u32,    // 本 chunk 当前数据长度（≤ chunk_size；截断会缩小它）
    pub nblocks: u32,        // = chunk_size / block_size
    pub checksum_alg: u8,    // 1 crc32c、2 xxh3-64、3 blake3-256
    pub compress_alg: u8,    // 0 none、1 lz4、2 zstd
    pub compress_level: i8,  // zstd 级别
    pub encrypt_alg: u8,     // 0 none、1 aes-256-gcm
    pub encrypt_key_id: u32, // 0 = 无
    pub snap_epoch: u32,     // 二期快照预留，首发写 0
    pub cluster_id: [u8; 16],
    pub write_time_ms: u64, // 由 head 填写并随转发传给后继，保证各副本文件逐字节相同
}

/// 由 chunk_size 算出 (block_size, nblocks, header_len_4k)；chunk_size 不合法（不是 64K..=16M 的 2 的幂）→ EINVAL。
pub fn layout_for(chunk_size: u32) -> Result<(u32, u32, u16), ErrCode> {
    if !chunk_size.is_power_of_two() || !(MIN_CHUNK_SIZE..=MAX_CHUNK_SIZE).contains(&chunk_size) {
        return Err(ErrCode::EINVAL);
    }
    let block_size = chunk_size.min(MAX_BLOCK_SIZE);
    let nblocks = chunk_size / block_size;
    let bytes = CHUNK_HEADER_LEN + nblocks as usize * BLOCK_ENTRY_LEN;
    Ok((block_size, nblocks, bytes.div_ceil(4096) as u16))
}

impl ChunkHeader {
    /// 数据区起点（字节偏移）。
    pub fn data_offset(&self) -> u64 {
        self.header_len_4k as u64 * 4096
    }

    /// 第 i 块数据在文件里的偏移。
    pub fn block_offset(&self, i: u32) -> u64 {
        self.data_offset() + i as u64 * self.block_size as u64
    }

    /// 编码固定头；`table` 是已编码的整个块表（nblocks × 72 字节），参与 header_crc。
    pub fn encode(&self, table: &[u8]) -> [u8; CHUNK_HEADER_LEN] {
        let mut b = [0u8; CHUNK_HEADER_LEN];
        b[0..8].copy_from_slice(&CHUNK_MAGIC);
        b[8..10].copy_from_slice(&CHUNK_FORMAT_VERSION.to_le_bytes());
        b[10..12].copy_from_slice(&self.header_len_4k.to_le_bytes());
        b[12..16].copy_from_slice(&self.features_compat.to_le_bytes());
        b[16..20].copy_from_slice(&self.features_ro_compat.to_le_bytes());
        b[20..24].copy_from_slice(&self.features_incompat.to_le_bytes());
        b[24..32].copy_from_slice(&self.ino.to_le_bytes());
        b[32..36].copy_from_slice(&self.epoch.to_le_bytes());
        b[36..40].copy_from_slice(&self.chunk_idx.to_le_bytes());
        b[40..44].copy_from_slice(&self.chain_id.to_le_bytes());
        b[44..48].copy_from_slice(&self.block_size.to_le_bytes());
        b[48..56].copy_from_slice(&self.chunk_version.to_le_bytes());
        b[56..60].copy_from_slice(&self.chunk_size.to_le_bytes());
        b[60..64].copy_from_slice(&self.logical_len.to_le_bytes());
        b[64..68].copy_from_slice(&self.nblocks.to_le_bytes());
        b[68] = self.checksum_alg;
        b[69] = self.compress_alg;
        b[70] = self.compress_level as u8;
        b[71] = self.encrypt_alg;
        b[72..76].copy_from_slice(&self.encrypt_key_id.to_le_bytes());
        b[76..80].copy_from_slice(&self.snap_epoch.to_le_bytes());
        b[80..96].copy_from_slice(&self.cluster_id);
        b[96..104].copy_from_slice(&self.write_time_ms.to_le_bytes());
        let crc = crc32c::crc32c_append(crc32c::crc32c(&b[0..252]), table);
        b[252..256].copy_from_slice(&crc.to_le_bytes());
        b
    }

    /// 解码并校验固定头与块表的 CRC、自洽性：magic/CRC/布局不对 → `CORRUPT`（3.2.5 R5）；
    /// 版本或 incompat 位不认识 → `FEATURE_UNSUPPORTED`。ro_compat 由调用方判断（不认识 → 只读服务）。
    pub fn decode(fixed: &[u8; CHUNK_HEADER_LEN], table: &[u8]) -> Result<ChunkHeader, ErrCode> {
        let u16_at = |o: usize| u16::from_le_bytes([fixed[o], fixed[o + 1]]);
        let u32_at = |o: usize| u32::from_le_bytes([fixed[o], fixed[o + 1], fixed[o + 2], fixed[o + 3]]);
        let u64_at = |o: usize| {
            let b = fixed;
            u64::from_le_bytes([b[o], b[o + 1], b[o + 2], b[o + 3], b[o + 4], b[o + 5], b[o + 6], b[o + 7]])
        };
        if fixed[0..8] != CHUNK_MAGIC {
            return Err(ErrCode::CORRUPT);
        }
        if crc32c::crc32c_append(crc32c::crc32c(&fixed[0..252]), table) != u32_at(252) {
            return Err(ErrCode::CORRUPT);
        }
        if u16_at(8) != CHUNK_FORMAT_VERSION || u32_at(20) & !INCOMPAT_KNOWN != 0 {
            return Err(ErrCode::FEATURE_UNSUPPORTED);
        }
        let mut cluster_id = [0u8; 16];
        cluster_id.copy_from_slice(&fixed[80..96]);
        let h = ChunkHeader {
            header_len_4k: u16_at(10),
            features_compat: u32_at(12),
            features_ro_compat: u32_at(16),
            features_incompat: u32_at(20),
            ino: u64_at(24),
            epoch: u32_at(32),
            chunk_idx: u32_at(36),
            chain_id: u32_at(40),
            block_size: u32_at(44),
            chunk_version: u64_at(48),
            chunk_size: u32_at(56),
            logical_len: u32_at(60),
            nblocks: u32_at(64),
            checksum_alg: fixed[68],
            compress_alg: fixed[69],
            compress_level: fixed[70] as i8,
            encrypt_alg: fixed[71],
            encrypt_key_id: u32_at(72),
            snap_epoch: u32_at(76),
            cluster_id,
            write_time_ms: u64_at(96),
        };
        let consistent = layout_for(h.chunk_size) == Ok((h.block_size, h.nblocks, h.header_len_4k))
            && table.len() == h.nblocks as usize * BLOCK_ENTRY_LEN
            && h.logical_len <= h.chunk_size;
        if !consistent {
            return Err(ErrCode::CORRUPT);
        }
        Ok(h)
    }
}

/// 读完 256 字节固定头之后，先用它得知块表有多长（nblocks × 72；nblocks 异常大时按 0 处理，decode 会报 CORRUPT）。
pub fn table_len_of(fixed: &[u8; CHUNK_HEADER_LEN]) -> usize {
    let nblocks = u32::from_le_bytes([fixed[64], fixed[65], fixed[66], fixed[67]]);
    if nblocks <= MAX_NBLOCKS { nblocks as usize * BLOCK_ENTRY_LEN } else { 0 }
}

/// 块表条目（72 字节）。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct BlockEntry {
    pub stored_len: u32,    // 数据区里实际存的字节数；0 = 洞（从未写过或被截断，读为 0）
    pub plain_len: u32,     // 该块明文有效长度（≤ block_size）
    pub flags: u8,          // bit0 COMPRESSED（本块真的压缩了）、bit1 ENCRYPTED
    pub nonce: [u8; 12],    // AES-GCM nonce：每次加密一个块时用 rand::fill 生成 12 个随机字节，绝不从版本号推导
    pub gcm_tag: [u8; 16],  // 未加密为 0
    pub checksum: [u8; 32], // 明文的校验和：crc32c 用前 4 字节（小端），xxh3 用前 8 字节，blake3 用全部 32 字节
}

impl BlockEntry {
    pub fn encode(&self) -> [u8; BLOCK_ENTRY_LEN] {
        let mut b = [0u8; BLOCK_ENTRY_LEN];
        b[0..4].copy_from_slice(&self.stored_len.to_le_bytes());
        b[4..8].copy_from_slice(&self.plain_len.to_le_bytes());
        b[8] = self.flags;
        b[12..24].copy_from_slice(&self.nonce);
        b[24..40].copy_from_slice(&self.gcm_tag);
        b[40..72].copy_from_slice(&self.checksum);
        b
    }

    pub fn decode(b: &[u8; BLOCK_ENTRY_LEN]) -> BlockEntry {
        let mut e = BlockEntry {
            stored_len: u32::from_le_bytes([b[0], b[1], b[2], b[3]]),
            plain_len: u32::from_le_bytes([b[4], b[5], b[6], b[7]]),
            flags: b[8],
            ..Default::default()
        };
        e.nonce.copy_from_slice(&b[12..24]);
        e.gcm_tag.copy_from_slice(&b[24..40]);
        e.checksum.copy_from_slice(&b[40..72]);
        e
    }
}

/// 把块表编码成连续字节（固定头之后紧跟它）。
pub fn encode_table(entries: &[BlockEntry]) -> Vec<u8> {
    entries.iter().flat_map(|e| e.encode()).collect()
}

/// 解码块表；长度不是 72 的整数倍 → CORRUPT。
pub fn decode_table(table: &[u8]) -> Result<Vec<BlockEntry>, ErrCode> {
    if !table.len().is_multiple_of(BLOCK_ENTRY_LEN) {
        return Err(ErrCode::CORRUPT);
    }
    Ok(table.as_chunks::<BLOCK_ENTRY_LEN>().0.iter().map(BlockEntry::decode).collect())
}

/// AES-GCM 的附加数据：`b"WMFS-BLK1" ‖ cluster_id ‖ ino ‖ epoch ‖ chunk_idx ‖ block_idx`（大端整数）。
/// 不含版本号：未改动的块被原样复制进新版本后仍然可解；防止块被挪到别的文件、位置或集群。
pub fn block_aad(cluster_id: &[u8; 16], ino: u64, epoch: u32, chunk_idx: u32, block_idx: u32) -> Vec<u8> {
    let mut v = Vec::with_capacity(9 + 16 + 8 + 12);
    v.extend_from_slice(b"WMFS-BLK1");
    v.extend_from_slice(cluster_id);
    v.extend_from_slice(&ino.to_be_bytes());
    v.extend_from_slice(&epoch.to_be_bytes());
    v.extend_from_slice(&chunk_idx.to_be_bytes());
    v.extend_from_slice(&block_idx.to_be_bytes());
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> (ChunkHeader, Vec<u8>) {
        let (block_size, nblocks, header_len_4k) = layout_for(4 << 20).unwrap();
        let h = ChunkHeader {
            header_len_4k,
            features_compat: 0,
            features_ro_compat: 0,
            features_incompat: INCOMPAT_BLOCK_COMPRESS,
            ino: 16,
            epoch: 1,
            chunk_idx: 3,
            chain_id: 7,
            block_size,
            chunk_version: 1 << 40,
            chunk_size: 4 << 20,
            logical_len: 100_000,
            nblocks,
            checksum_alg: CK_CRC32C,
            compress_alg: CMP_ZSTD,
            compress_level: -3,
            encrypt_alg: ENC_NONE,
            encrypt_key_id: 0,
            snap_epoch: 0,
            cluster_id: [0xC1; 16],
            write_time_ms: 1_792_454_400_000,
        };
        let mut entries = vec![BlockEntry::default(); nblocks as usize];
        entries[0] = BlockEntry { stored_len: 4000, plain_len: 65536, flags: BLK_COMPRESSED, ..Default::default() };
        entries[1].checksum[..4].copy_from_slice(&0x1234_5678u32.to_le_bytes());
        (h, encode_table(&entries))
    }

    /// 改了字节之后重算 header_crc（用来构造"CRC 正确但内容不对"的头）。
    fn reseal(b: &mut [u8; CHUNK_HEADER_LEN], table: &[u8]) {
        let crc = crc32c::crc32c_append(crc32c::crc32c(&b[0..252]), table);
        b[252..256].copy_from_slice(&crc.to_le_bytes());
    }

    #[test]
    fn layout_rules() {
        assert_eq!(layout_for(4 << 20), Ok((65536, 64, 2)));
        assert_eq!(layout_for(64 << 10), Ok((65536, 1, 1)));
        assert_eq!(layout_for(16 << 20), Ok((65536, 256, 5)));
        for bad in [3 << 20, 32 << 10, 32 << 20, 0] {
            assert_eq!(layout_for(bad), Err(ErrCode::EINVAL));
        }
    }

    #[test]
    fn header_round_trip_and_offsets() {
        let (h, table) = sample();
        let b = h.encode(&table);
        assert_eq!(&b[0..8], b"WMFSCHK1");
        assert_eq!(&b[8..10], &[1, 0]);
        assert_eq!(&b[48..56], &(1u64 << 40).to_le_bytes(), "chunk_version 在偏移 48");
        assert_eq!(b[70], (-3i8) as u8);
        assert!(b[104..252].iter().all(|x| *x == 0), "保留字段全 0");
        assert_eq!(table_len_of(&b), 64 * 72);
        assert_eq!(ChunkHeader::decode(&b, &table), Ok(h));
        assert_eq!(h.data_offset(), 8192);
        assert_eq!(h.block_offset(2), 8192 + 2 * 65536);
        assert_eq!(decode_table(&table).unwrap()[0].stored_len, 4000);
    }

    #[test]
    fn header_rejections() {
        let (h, table) = sample();
        let good = h.encode(&table);
        let mut t2 = table.clone();
        t2[100] ^= 1;
        assert_eq!(ChunkHeader::decode(&good, &t2), Err(ErrCode::CORRUPT), "header_crc 覆盖整个块表");
        let mut b = good;
        b[0] = b'X';
        assert_eq!(ChunkHeader::decode(&b, &table), Err(ErrCode::CORRUPT));
        let mut b = good;
        b[20] |= 1 << 2; // bit2 = 二期 DYN_TRANSFORM，首发不认识
        reseal(&mut b, &table);
        assert_eq!(ChunkHeader::decode(&b, &table), Err(ErrCode::FEATURE_UNSUPPORTED));
        let mut b = good;
        b[44..48].copy_from_slice(&4096u32.to_le_bytes()); // block_size 与 chunk_size 不自洽
        reseal(&mut b, &table);
        assert_eq!(ChunkHeader::decode(&b, &table), Err(ErrCode::CORRUPT));
        assert_eq!(ChunkHeader::decode(&good, &table[..72]), Err(ErrCode::CORRUPT), "块表长度不对");
    }

    #[test]
    fn block_entry_and_aad() {
        let mut e = BlockEntry { stored_len: 1, plain_len: 2, flags: BLK_ENCRYPTED, ..Default::default() };
        e.nonce = [9; 12];
        e.gcm_tag = [8; 16];
        e.checksum = [7; 32];
        let b = e.encode();
        assert_eq!((b[8], b[12], b[24], b[40], &b[9..12]), (BLK_ENCRYPTED, 9, 8, 7, &[0u8, 0, 0][..]));
        assert_eq!(BlockEntry::decode(&b), e);
        let aad = block_aad(&[1; 16], 0x10, 2, 3, 4);
        assert_eq!(&aad[..9], b"WMFS-BLK1");
        assert_eq!(&aad[25..33], &0x10u64.to_be_bytes(), "AAD 里的整数是大端");
        assert_eq!(aad.len(), 9 + 16 + 8 + 4 + 4 + 4);
        assert!(decode_table(&[0u8; 71]).is_err());
    }

    #[test]
    fn random_headers_never_panic() {
        let mut r = crate::base::rand::XorShift64::new(11);
        let (h, table) = sample();
        let good = h.encode(&table);
        for _ in 0..5000 {
            let mut b = good;
            let i = r.below(256) as usize;
            b[i] ^= 1 << r.below(8);
            assert!(ChunkHeader::decode(&b, &table).is_err(), "翻转任意一个比特都必须被发现（偏移 {i}）");
            let mut junk = [0u8; CHUNK_HEADER_LEN];
            r.fill(&mut junk);
            let _ = ChunkHeader::decode(&junk, &table);
            let _ = table_len_of(&junk);
        }
    }
}
