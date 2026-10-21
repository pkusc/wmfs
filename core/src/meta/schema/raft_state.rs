//! `meta.redb` 的 `raft_state` 表里由 wmfs 自己定义格式的三个值（design.md 3.2.6，ADR-0020）：
//!
//! - `"format"` = `[1] ‖ postcard(MetaFormatV1)`：元数据格式版本与已启用的 incompat 特性
//!   （不认识的 incompat → 节点拒绝启动元数据角色，3.2.10）；
//! - `"snap_meta"` = 16 字节：压缩点的 index、term（u64 小端）；
//! - `"applied"` = 24 字节：applied_index、applied_term、state_version（u64 小端，3.4.1.5）。
//!
//! `"hard_state"`、`"conf_state"` 是 raft-rs 自己的 protobuf，不在这里。读写这张表的 RedbStorage 在 M3。
//! 这些都是持久化格式：字节布局永不改动（3.2.11），金样在 `tests/golden/records.golden`。

use serde::{Deserialize, Serialize};

use super::records::{decode_v1, encode_v1};
use crate::base::ErrCode;

/// `raft_state` 表的 key。
pub const KEY_HARD_STATE: &str = "hard_state";
pub const KEY_CONF_STATE: &str = "conf_state";
pub const KEY_SNAP_META: &str = "snap_meta";
pub const KEY_APPLIED: &str = "applied";
pub const KEY_FORMAT: &str = "format";

/// `"format"` 的内容；init 写入 `{ version: 1, incompat: [] }`。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct MetaFormatV1 {
    pub version: u16, // 与 ClusterInfoV1.format 同类型（实现注记 M0：3.2.6 没写类型）
    pub incompat: Vec<String>,
}

impl MetaFormatV1 {
    pub fn encode(&self) -> Vec<u8> {
        encode_v1(self)
    }
    /// 版本字节不是 1、内容解不开或有多余字节 → `INTERNAL`（与其他记录一样）。
    pub fn decode(b: &[u8]) -> Result<MetaFormatV1, ErrCode> {
        decode_v1(b)
    }
}

/// `"snap_meta"`：日志压缩点（或快照点）的 index 与 term。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnapMeta {
    pub index: u64,
    pub term: u64,
}

impl SnapMeta {
    pub fn encode(&self) -> [u8; 16] {
        let mut b = [0u8; 16];
        b[0..8].copy_from_slice(&self.index.to_le_bytes());
        b[8..16].copy_from_slice(&self.term.to_le_bytes());
        b
    }
    /// 长度不是 16 → `INTERNAL`（持久化状态损坏）。
    pub fn decode(b: &[u8]) -> Result<SnapMeta, ErrCode> {
        if b.len() != 16 {
            return Err(ErrCode::INTERNAL);
        }
        Ok(SnapMeta { index: le_u64(b, 0), term: le_u64(b, 8) })
    }
}

/// `"applied"`：已应用到的条目，以及最近一个改变了 KV 的批次条目的 index（`state_version`，3.4.1.5）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Applied {
    pub index: u64,
    pub term: u64,
    pub state_version: u64,
}

impl Applied {
    pub fn encode(&self) -> [u8; 24] {
        let mut b = [0u8; 24];
        b[0..8].copy_from_slice(&self.index.to_le_bytes());
        b[8..16].copy_from_slice(&self.term.to_le_bytes());
        b[16..24].copy_from_slice(&self.state_version.to_le_bytes());
        b
    }
    /// 长度不是 24 → `INTERNAL`（持久化状态损坏）。
    pub fn decode(b: &[u8]) -> Result<Applied, ErrCode> {
        if b.len() != 24 {
            return Err(ErrCode::INTERNAL);
        }
        Ok(Applied { index: le_u64(b, 0), term: le_u64(b, 8), state_version: le_u64(b, 16) })
    }
}

/// 从偏移 `o` 读一个小端 u64（调用方已检查过长度）。
fn le_u64(b: &[u8], o: usize) -> u64 {
    u64::from_le_bytes([b[o], b[o + 1], b[o + 2], b[o + 3], b[o + 4], b[o + 5], b[o + 6], b[o + 7]])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_layouts_round_trip() {
        let s = SnapMeta { index: 0x0102_0304_0506_0708, term: 9 };
        let b = s.encode();
        assert_eq!(&b[0..8], &[8, 7, 6, 5, 4, 3, 2, 1], "index 在前、小端");
        assert_eq!(SnapMeta::decode(&b), Ok(s));
        let a = Applied { index: 1001, term: 4, state_version: 999 };
        let b = a.encode();
        assert_eq!(&b[16..24], &999u64.to_le_bytes(), "state_version 在偏移 16");
        assert_eq!(Applied::decode(&b), Ok(a));
        assert_eq!(SnapMeta::decode(&b), Err(ErrCode::INTERNAL), "长度不对");
        assert_eq!(Applied::decode(&[]), Err(ErrCode::INTERNAL));
    }

    #[test]
    fn format_record() {
        let f = MetaFormatV1 { version: 1, incompat: vec![] };
        let b = f.encode();
        assert_eq!(b, vec![1, 1, 0], "[1] ‖ postcard{{version: 1, incompat: []}}");
        assert_eq!(MetaFormatV1::decode(&b), Ok(f));
        assert_eq!(MetaFormatV1::decode(&[2, 1, 0]), Err(ErrCode::INTERNAL), "版本字节不是 1");
    }
}
