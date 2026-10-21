//! rdma 插件两端私有的描述符表（core 不看；design.md 3.6.4 实现注记（M2）①③，金样在 setup.golden）。
//! 一次 SEND 的内容 = 48 字节帧头 + body + 描述符（小端，与帧头一致）：
//! - 带 `BULK_REMOTE`（请求带 bulk）：`n: u8`（1..=16）+ n × `{addr: u64, rkey: u32, len: u32}`（每段 16 字节，一段对应一个
//!   长度不为 0 的 `BufSlice`，接收方按顺序 READ 进自己的一个池缓冲）；
//! - 带 `REPLY_REMOTE`（期望回复带 bulk）：恰好 1 × 16 字节（不带 n），描述请求方的 `recv_into`（len = 它的容量）；
//! - 两个都有时 `BULK_REMOTE` 的表在前。最多 1 + 256 + 16 = 273 字节。
//!
//! 描述符来自对端，不可信：解码时就拒绝 `addr + len` 溢出的（之后算 READ 的范围、WRITE 的偏移都不会溢出）；
//! 发 READ 之前再按帧头的 bulk_len 校验整张表（`check_bulk`）。

use wmfs_core::base::ErrCode;
use wmfs_core::proto::frame::{FLAG_BULK_REMOTE, FLAG_REPLY_REMOTE};

use super::params::MAX_SEGS;

/// 一段对端内存：地址、rkey、长度。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Desc {
    pub addr: u64,
    pub rkey: u32,
    pub len: u32,
}

/// 一个描述符的字节数。
pub const DESC_LEN: usize = 16;

/// 把描述符表追加到 `out`（帧头 + body 之后）。`bulk` 为空 = 不带 BULK_REMOTE 的表。
pub fn encode_descs(out: &mut Vec<u8>, bulk: &[Desc], reply: Option<&Desc>) {
    if !bulk.is_empty() {
        out.push(bulk.len() as u8);
    }
    for d in bulk.iter().chain(reply) {
        out.extend_from_slice(&d.addr.to_le_bytes());
        out.extend_from_slice(&d.rkey.to_le_bytes());
        out.extend_from_slice(&d.len.to_le_bytes());
    }
}

/// 按帧头的 flags 解出 body 之后的描述符；`tail` 必须正好是这么长（多一个字节都不行），每个描述符的 `addr + len` 都不能
/// 溢出。不对 → `PROTO_UNSUPPORTED`（断开）。
pub fn decode_descs(flags: u16, tail: &[u8]) -> Result<(Vec<Desc>, Option<Desc>), ErrCode> {
    let one = |at: usize| -> Result<Desc, ErrCode> {
        let b = tail.get(at..at + DESC_LEN).ok_or(ErrCode::PROTO_UNSUPPORTED)?;
        let le64 = |x: &[u8]| u64::from_le_bytes([x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7]]);
        let le32 = |x: &[u8]| u32::from_le_bytes([x[0], x[1], x[2], x[3]]);
        let d = Desc { addr: le64(&b[0..8]), rkey: le32(&b[8..12]), len: le32(&b[12..16]) };
        match d.addr.checked_add(d.len as u64) {
            Some(_) => Ok(d),
            None => Err(ErrCode::PROTO_UNSUPPORTED),
        }
    };
    let (mut bulk, mut at) = (Vec::new(), 0);
    if flags & FLAG_BULK_REMOTE != 0 {
        let n = *tail.first().ok_or(ErrCode::PROTO_UNSUPPORTED)? as usize;
        if !(1..=MAX_SEGS).contains(&n) {
            return Err(ErrCode::PROTO_UNSUPPORTED);
        }
        at = 1;
        for _ in 0..n {
            bulk.push(one(at)?);
            at += DESC_LEN;
        }
    }
    let mut reply = None;
    if flags & FLAG_REPLY_REMOTE != 0 {
        reply = Some(one(at)?);
        at += DESC_LEN;
    }
    if at != tail.len() {
        return Err(ErrCode::PROTO_UNSUPPORTED);
    }
    Ok((bulk, reply))
}

/// 接收方在发起 READ 之前必须先校验描述符表（design.md 3.6.4 实现注记（M2））：1..=16 段、每段 len > 0、
/// Σlen == 帧头的 bulk_len。否则一个已认证的对端可以声明很小的 bulk_len（只分到一个小槽），却给出很长的描述符——
/// MR 覆盖整个池，网卡不会拦，READ 会写穿到相邻的池槽。不对 → `PROTO_UNSUPPORTED`（断开）。
pub fn check_bulk(d: &[Desc], bulk_len: u32) -> Result<(), ErrCode> {
    let sum: u64 = d.iter().map(|x| x.len as u64).sum();
    let ok = (1..=MAX_SEGS).contains(&d.len()) && d.iter().all(|x| x.len > 0) && sum == bulk_len as u64;
    if ok { Ok(()) } else { Err(ErrCode::PROTO_UNSUPPORTED) }
}
