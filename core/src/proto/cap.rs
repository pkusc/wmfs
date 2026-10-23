//! 能力令牌 cap（design.md 3.3.5，ADR-0023）：元数据在 Open/Create/改长度的 SetAttr 时签发、数据节点只验签的授权决定。
//!
//! 线上：`postcard(CapV1) ‖ mac[16]`，`mac = HMAC-SHA256(K_cap, "WMFS-CAP-1" ‖ postcard 字节)[0..16]`。
//! 变换参数在 cap 里，客户端无法绕过 `encrypt` 策略。数据节点的校验（全部本地 O(1)，**在 io 线程出队后执行**）：
//! MAC 正确（[`verify`]）；`expires_ms + 5 min ≥ wall_ms()`；`chunk.ino == cap.ino && chunk.epoch == cap.epoch`；权限位；
//! `chunk.chain == cap.chains[chunk.idx % cap.chains.len()]`（[`CapV1::allows`]）；`cap.session` 不在集群图的被驱逐列表里
//! （数据服务查集群图）。
//!
//! 实现注记（M5）：K_cap 由 `cluster.key` 派生（`handshake::cap_key`），元数据 leader 签、数据节点验，节点之间不传秘密。
//! 规格没写的两点取最小方案：① 后台任务的请求走 QoS 的低优先级队列——cap 带**管理权限位**就算后台请求（管理 cap 只签发给
//! 后台任务，3.3.5 的 DeleteEpoch 一行）；② ADR-0034 的服务端 uninline 用 leader 自己签的写 cap，`session = 0`（不属于任何会话）。

use serde::{Deserialize, Serialize};

use crate::base::ErrCode;
use crate::base::types::ChunkId;
use crate::proto::handshake::{hmac_sha256, hmac_verify_prefix};
use crate::proto::{decode_body, encode_body};

/// cap 末尾 MAC 的字节数。
pub const CAP_MAC_LEN: usize = 16;
/// `CapV1.rights` 的位。
pub const RIGHT_READ: u8 = 1 << 0;
pub const RIGHT_WRITE: u8 = 1 << 1;
pub const RIGHT_ADMIN: u8 = 1 << 2;
/// 过期判断对墙钟误差的容忍：`expires_ms + 5 min ≥ wall_ms()` 才算没过期（3.3.5；要求节点间 NTP，放松 ⑫）。
pub const CAP_SKEW_MS: u64 = 5 * 60 * 1000;
/// MAC 的域分隔串（与 3.3.5 原文一致）。
const CAP_DOMAIN: &[u8] = b"WMFS-CAP-1";

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

/// 签名：`postcard(cap) ‖ HMAC-SHA256(K_cap, "WMFS-CAP-1" ‖ postcard)[0..16]`。
pub fn sign(k_cap: &[u8; 32], cap: &CapV1) -> Vec<u8> {
    let mut b = encode_body(cap);
    let mac = hmac_sha256(k_cap, &[CAP_DOMAIN, &b]);
    b.extend_from_slice(&mac[..CAP_MAC_LEN]);
    b
}

/// 验签并解出内容（MAC 常数时间比较）：太短、MAC 不对、内容解不开 → `BAD_CAP`。
pub fn verify(k_cap: &[u8; 32], b: &[u8]) -> Result<CapV1, ErrCode> {
    let body = b.len().checked_sub(CAP_MAC_LEN).map(|n| &b[..n]).ok_or(ErrCode::BAD_CAP)?;
    if !hmac_verify_prefix(k_cap, &[CAP_DOMAIN, body], &b[body.len()..]) {
        return Err(ErrCode::BAD_CAP);
    }
    decode_body(body).map_err(|_| ErrCode::BAD_CAP)
}

/// 只解出内容、**不验 MAC**：数据节点在入 QoS 队列时用它取队列键（租户、qos_id、存储类）；出队执行前一定再 [`verify`]。
pub fn peek(b: &[u8]) -> Result<CapV1, ErrCode> {
    let body = b.len().checked_sub(CAP_MAC_LEN).map(|n| &b[..n]).ok_or(ErrCode::BAD_CAP)?;
    decode_body(body).map_err(|_| ErrCode::BAD_CAP)
}

impl CapV1 {
    /// 没过期（墙钟，容差 5 min）且带 `right` 里的某一位；否则 `BAD_CAP`。
    pub fn check_time_and_right(&self, right: u8, now_ms: u64) -> Result<(), ErrCode> {
        if self.expires_ms.saturating_add(CAP_SKEW_MS) < now_ms || self.rights & right == 0 {
            return Err(ErrCode::BAD_CAP);
        }
        Ok(())
    }

    /// 这个 cap 允不允许对 `chunk` 做 `right`（3.3.5 的本地校验，被驱逐会话除外）：没过期、chunk 属于这个文件的这个 epoch、
    /// 权限位、chunk 所在的链正是 layout 规定的那条（客户端不能把数据塞进 layout 之外的链）。不允许 → `BAD_CAP`。
    pub fn allows(&self, chunk: &ChunkId, right: u8, now_ms: u64) -> Result<(), ErrCode> {
        self.check_time_and_right(right, now_ms)?;
        let k = self.chains.len();
        let chain_ok = k > 0 && self.chains[chunk.idx as usize % k] == chunk.chain;
        if chunk.ino != self.ino || chunk.epoch != self.epoch || !chain_ok {
            return Err(ErrCode::BAD_CAP);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cap() -> CapV1 {
        CapV1 {
            ino: 18,
            epoch: 2,
            rights: RIGHT_READ | RIGHT_WRITE,
            session: 7,
            tenant: 0,
            job_id: 0,
            chains: vec![3, 4],
            class_id: 1,
            qos_id: 0,
            chunk_size: 4 << 20,
            block_size: 65536,
            checksum: 1,
            compress: 0,
            compress_level: 0,
            encrypt: 0,
            encrypt_key: 0,
            expires_ms: 1_000_000,
        }
    }

    fn chunk(chain: u32, idx: u32) -> ChunkId {
        ChunkId { chain, ino: 18, epoch: 2, idx }
    }

    #[test]
    fn sign_verify_and_tamper() {
        let k = [9u8; 32];
        let b = sign(&k, &cap());
        assert_eq!(verify(&k, &b), Ok(cap()));
        assert_eq!(peek(&b), Ok(cap()));
        assert_eq!(verify(&[8u8; 32], &b), Err(ErrCode::BAD_CAP), "别的集群的 K_cap");
        for i in 0..b.len() {
            let mut t = b.clone();
            t[i] ^= 0x40;
            assert_eq!(verify(&k, &t), Err(ErrCode::BAD_CAP), "改动第 {i} 个字节必须被发现");
        }
        assert_eq!(verify(&k, &b[..b.len() - 1]), Err(ErrCode::BAD_CAP), "截短");
        assert_eq!(verify(&k, &[]), Err(ErrCode::BAD_CAP));
    }

    #[test]
    fn allows_checks_time_rights_and_chain() {
        let c = cap();
        assert_eq!(c.allows(&chunk(3, 0), RIGHT_READ, 1_000_000), Ok(()));
        assert_eq!(c.allows(&chunk(4, 1), RIGHT_WRITE, 1_000_000 + CAP_SKEW_MS), Ok(()), "容差 5 分钟以内照常");
        assert_eq!(c.allows(&chunk(3, 0), RIGHT_READ, 1_000_001 + CAP_SKEW_MS), Err(ErrCode::BAD_CAP), "过期");
        assert_eq!(c.allows(&chunk(3, 0), RIGHT_ADMIN, 0), Err(ErrCode::BAD_CAP), "没有管理权限位");
        assert_eq!(c.allows(&chunk(4, 0), RIGHT_READ, 0), Err(ErrCode::BAD_CAP), "chunk 0 应在 chains[0] = 3");
        assert_eq!(c.allows(&chunk(5, 2), RIGHT_READ, 0), Err(ErrCode::BAD_CAP), "layout 之外的链");
        let other = ChunkId { ino: 19, ..chunk(3, 0) };
        assert_eq!(c.allows(&other, RIGHT_READ, 0), Err(ErrCode::BAD_CAP), "别的文件");
        let old = ChunkId { epoch: 1, ..chunk(3, 0) };
        assert_eq!(c.allows(&old, RIGHT_READ, 0), Err(ErrCode::BAD_CAP), "旧 epoch");
        let none = CapV1 { chains: vec![], ..cap() };
        assert_eq!(none.allows(&chunk(3, 0), RIGHT_READ, 0), Err(ErrCode::BAD_CAP), "没有链的 cap 什么也不允许");
    }
}
