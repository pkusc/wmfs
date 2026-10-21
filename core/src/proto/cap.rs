//! 能力令牌 cap（design.md 3.3.5，ADR-0023）：元数据在 Open/Create 时签发、数据节点只验签的授权决定。
//!
//! 线上：`postcard(CapV1) ‖ mac[16]`，`mac = HMAC-SHA256(K_cap, "WMFS-CAP-1" ‖ postcard 字节)[0..16]`。
//! 变换参数在 cap 里，客户端无法绕过 `encrypt` 策略。签发与校验在 M5 实现。

use serde::{Deserialize, Serialize};

/// cap 末尾 MAC 的字节数。
pub const CAP_MAC_LEN: usize = 16;
/// `CapV1.rights` 的位。
pub const RIGHT_READ: u8 = 1 << 0;
pub const RIGHT_WRITE: u8 = 1 << 1;
pub const RIGHT_ADMIN: u8 = 1 << 2;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CapV1 {
    pub ino: u64,
    pub epoch: u32,
    pub rights: u8, // bit0 读，bit1 写，bit2 管理
    pub session: u64,
    pub tenant: u32,
    pub job_id: u64,
    pub chains: Vec<u32>, // = layout.chains
    pub class_id: u16,
    pub qos_id: u64, // qos_id = qos 属性来源目录的 ino（0 = 用存储类的 QoS 包络）
    pub chunk_size: u32,
    pub block_size: u32,
    pub checksum: u8,
    pub compress: u8,
    pub compress_level: i8,
    pub encrypt: u8,
    pub encrypt_key: u32,
    pub expires_ms: u64, // 墙钟；签发时 now + cap_ttl（默认 10 min）
}
