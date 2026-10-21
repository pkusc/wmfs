//! target 超级块 `TARGET`（4096 字节，design.md 3.2.3）的字节布局与编解码。全部整数小端。
//!
//! 0 magic `WMFSTGT1` | 8 format_version = 1 | 10 保留 0 | 12 features_compat | 16 features_ro_compat |
//! 20 features_incompat | 24 cluster_id(16) | 40 target_id | 44 node_id | 48 created_ms | 56..4092 保留 0 |
//! 4092 crc32c(0..4092)。
//! 启动时：cluster_id 不符 → 拒绝启动（防止插错盘）；不认识的 incompat 位 → 拒绝；不认识的 ro_compat 位 →
//! 只读服务（target 不进入任何链的写路径，上报 `read_only = true`）。

use crate::base::ErrCode;

pub const TARGET_MAGIC: [u8; 8] = *b"WMFSTGT1";
pub const TARGET_FORMAT_VERSION: u16 = 1;
pub const TARGET_SB_LEN: usize = 4096;
/// 首发认识的 incompat 位：一个也没有（任何一位置 1 → 拒绝打开这个 target）。
pub const TARGET_INCOMPAT_KNOWN: u32 = 0;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TargetSuperblock {
    pub features_compat: u32,
    pub features_ro_compat: u32,
    pub features_incompat: u32,
    pub cluster_id: [u8; 16],
    pub target_id: u32,
    pub node_id: u32,
    pub created_ms: u64,
}

impl TargetSuperblock {
    pub fn encode(&self) -> Vec<u8> {
        let mut b = vec![0u8; TARGET_SB_LEN];
        b[0..8].copy_from_slice(&TARGET_MAGIC);
        b[8..10].copy_from_slice(&TARGET_FORMAT_VERSION.to_le_bytes());
        b[12..16].copy_from_slice(&self.features_compat.to_le_bytes());
        b[16..20].copy_from_slice(&self.features_ro_compat.to_le_bytes());
        b[20..24].copy_from_slice(&self.features_incompat.to_le_bytes());
        b[24..40].copy_from_slice(&self.cluster_id);
        b[40..44].copy_from_slice(&self.target_id.to_le_bytes());
        b[44..48].copy_from_slice(&self.node_id.to_le_bytes());
        b[48..56].copy_from_slice(&self.created_ms.to_le_bytes());
        let crc = crc32c::crc32c(&b[0..4092]);
        b[4092..4096].copy_from_slice(&crc.to_le_bytes());
        b
    }

    /// magic、长度或 CRC 不对 → CORRUPT；版本或 incompat 位不认识 → FEATURE_UNSUPPORTED。cluster_id 由调用方比对。
    pub fn decode(b: &[u8]) -> Result<TargetSuperblock, ErrCode> {
        if b.len() != TARGET_SB_LEN || b[0..8] != TARGET_MAGIC {
            return Err(ErrCode::CORRUPT);
        }
        let u32_at = |o: usize| u32::from_le_bytes([b[o], b[o + 1], b[o + 2], b[o + 3]]);
        if crc32c::crc32c(&b[0..4092]) != u32_at(4092) {
            return Err(ErrCode::CORRUPT);
        }
        if u16::from_le_bytes([b[8], b[9]]) != TARGET_FORMAT_VERSION || u32_at(20) & !TARGET_INCOMPAT_KNOWN != 0 {
            return Err(ErrCode::FEATURE_UNSUPPORTED);
        }
        let mut cluster_id = [0u8; 16];
        cluster_id.copy_from_slice(&b[24..40]);
        Ok(TargetSuperblock {
            features_compat: u32_at(12),
            features_ro_compat: u32_at(16),
            features_incompat: u32_at(20),
            cluster_id,
            target_id: u32_at(40),
            node_id: u32_at(44),
            created_ms: u64::from_le_bytes([b[48], b[49], b[50], b[51], b[52], b[53], b[54], b[55]]),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn target_superblock() {
        let sb = TargetSuperblock {
            features_compat: 0,
            features_ro_compat: 1,
            features_incompat: 0,
            cluster_id: [0xAB; 16],
            target_id: 2,
            node_id: 1,
            created_ms: 42,
        };
        let b = sb.encode();
        assert_eq!(b.len(), 4096);
        assert_eq!(&b[0..8], b"WMFSTGT1");
        assert_eq!(TargetSuperblock::decode(&b), Ok(sb));
        let mut bad = b.clone();
        bad[100] = 1;
        assert_eq!(TargetSuperblock::decode(&bad), Err(ErrCode::CORRUPT), "CRC 覆盖保留区");
        let mut inc = b.clone();
        inc[20] = 1;
        let crc = crc32c::crc32c(&inc[0..4092]);
        inc[4092..].copy_from_slice(&crc.to_le_bytes());
        assert_eq!(TargetSuperblock::decode(&inc), Err(ErrCode::FEATURE_UNSUPPORTED));
        assert_eq!(TargetSuperblock::decode(&b[..100]), Err(ErrCode::CORRUPT));
    }

    #[test]
    fn target_superblock_field_offsets() {
        // 每个字段用互不相同的值，逐个按 design.md 3.2.3 的偏移核对（incompat 只能是 0，所以单独看偏移 20 是 0）
        let sb = TargetSuperblock {
            features_compat: 0x1111_0001,
            features_ro_compat: 0x2222_0002,
            features_incompat: 0,
            cluster_id: [0xAB; 16],
            target_id: 0x3333_0003,
            node_id: 0x4444_0004,
            created_ms: 0x5555_6666_7777_8888,
        };
        let b = sb.encode();
        assert_eq!(&b[8..10], &1u16.to_le_bytes(), "format_version 在偏移 8");
        assert_eq!(&b[10..12], &[0, 0], "偏移 10 保留");
        assert_eq!(&b[12..16], &0x1111_0001u32.to_le_bytes(), "features_compat 在偏移 12");
        assert_eq!(&b[16..20], &0x2222_0002u32.to_le_bytes(), "features_ro_compat 在偏移 16");
        assert_eq!(&b[20..24], &[0, 0, 0, 0], "features_incompat 在偏移 20");
        assert_eq!(&b[24..40], &[0xAB; 16], "cluster_id 在偏移 24");
        assert_eq!(&b[40..44], &0x3333_0003u32.to_le_bytes(), "target_id 在偏移 40");
        assert_eq!(&b[44..48], &0x4444_0004u32.to_le_bytes(), "node_id 在偏移 44");
        assert_eq!(&b[48..56], &0x5555_6666_7777_8888u64.to_le_bytes(), "created_ms 在偏移 48");
        assert!(b[56..4092].iter().all(|x| *x == 0), "保留区全 0");
        assert_eq!(&b[4092..], &crc32c::crc32c(&b[0..4092]).to_le_bytes(), "crc32c 在偏移 4092");
        assert_eq!(TargetSuperblock::decode(&b), Ok(sb));
    }
}
