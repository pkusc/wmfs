//! 帧头（48 字节，小端，design.md 3.3.1）、msg_type 表（3.3.3）、`ErrorBody`，
//! 以及传输插件与 core 之间交接的 `OutFrame` / `InFrame`（3.6.4）。
//!
//! | 偏移 | 长度 | 字段 | 说明 |
//! |---|---|---|---|
//! | 0 | 4 | magic | `b"WMF1"` |
//! | 4 | 1 | proto_major | = 1；不等即断开 |
//! | 5 | 1 | kind | 0 Request，1 Response，2 OneWay，3 StreamChunk |
//! | 6 | 2 | msg_type | 3.3.3 |
//! | 8 | 8 | req_id | 连接内唯一；响应原样带回；OneWay 为 0 |
//! | 16 | 8 | trace_id | 客户端每个 FUSE 操作生成一个，全链路透传；0 = 接收方生成 |
//! | 24 | 2 | flags | bit0 HAS_BULK，bit1 BULK_REMOTE，bit2 REPLY_REMOTE，bit3 ERROR，bit4 LAST_CHUNK |
//! | 26 | 2 | span_id | 每一跳 +1（慢请求时间线排序用） |
//! | 28 | 4 | body_len | ≤ 1 MiB（某个传输可以更小：RDMA 的帧头 + body ≤ 120 KiB） |
//! | 32 | 4 | bulk_len | ≤ 16 MiB + 128 KiB |
//! | 36 | 4 | body_crc32c | crc32c(body) |
//! | 40 | 4 | bulk_crc32c | crc32c(bulk)；无 bulk 为 0；单边传输由接收方在 READ 完成后校验 |
//! | 44 | 4 | header_crc32c | crc32c(帧头 0..44) |
//!
//! 超限或 CRC 错误 → 关闭连接（`PROTO_UNSUPPORTED` 计数）。

use serde::{Deserialize, Serialize};

use crate::base::ErrCode;
use crate::buf::{Buf, BufSlice};

pub const MAGIC: [u8; 4] = *b"WMF1";
pub const PROTO_MAJOR: u8 = 1;
pub const HEADER_LEN: usize = 48;
pub const MAX_BODY: u32 = 1 << 20; // body_len ≤ 1 MiB
pub const MAX_BULK: u32 = (16 << 20) + (128 << 10); // bulk_len ≤ 16 MiB + 128 KiB

// ---- flags ----
pub const FLAG_HAS_BULK: u16 = 1 << 0;
pub const FLAG_BULK_REMOTE: u16 = 1 << 1; // bulk 是对端内存描述符表（bit1、bit2 只由支持单边操作的传输插件设置与解析，core 不看）
pub const FLAG_REPLY_REMOTE: u16 = 1 << 2; // 请求附带"响应 bulk 写到这里"的描述符
pub const FLAG_ERROR: u16 = 1 << 3; // body 是 ErrorBody
pub const FLAG_LAST_CHUNK: u16 = 1 << 4; // 流结束

// ---- msg_type（3.3.3；一个 msg_type 对应一个固定的 body 结构）----
pub const MSG_HELLO: u16 = 0x0001;
pub const MSG_CHALLENGE: u16 = 0x0002;
pub const MSG_AUTH: u16 = 0x0003;
pub const MSG_WELCOME: u16 = 0x0004;
pub const MSG_PING: u16 = 0x0005; // PingBody + 可选 bulk，原样回显（任何传输）
pub const MSG_META_CALL: u16 = 0x0100; // MetaCall → MetaReply（控制族）
pub const MSG_DATA_CALL: u16 = 0x0200; // DataCall → DataReply（数据族）
pub const MSG_PEER_CALL: u16 = 0x0300; // PeerOp → PeerReply（数据节点之间；数据族）
pub const MSG_NODE_CALL: u16 = 0x0400; // NodeOp → NodeReply（节点 ↔ 元数据 leader；控制族）
pub const MSG_RAFT: u16 = 0x0500; // OneWay；body 为空，bulk = protobuf 编码的 eraftpb::Message（控制族）
pub const MSG_SNAPSHOT_FETCH: u16 = 0x0501; // Request → StreamChunk…（3.4.1.7；控制族）
pub const MSG_PLUGIN_FIRST: u16 = 0x0600; // 0x0600–0x06FF：传输插件私有，只出现在插件自己的引导连接上，
pub const MSG_PLUGIN_LAST: u16 = 0x06FF; // core 的分发器从不处理（例如 RDMA 插件用 0x0600 交换 QP 信息）

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameKind {
    Request = 0,
    Response = 1,
    OneWay = 2,
    StreamChunk = 3,
}

impl FrameKind {
    pub fn from_u8(v: u8) -> Option<FrameKind> {
        match v {
            0 => Some(FrameKind::Request),
            1 => Some(FrameKind::Response),
            2 => Some(FrameKind::OneWay),
            3 => Some(FrameKind::StreamChunk),
            _ => None,
        }
    }
    pub fn as_u8(self) -> u8 {
        self as u8
    }
}

/// 帧头的各字段（magic 与 header_crc32c 由 encode/decode 处理）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrameHeader {
    pub proto_major: u8, // = 1
    pub kind: FrameKind,
    pub msg_type: u16,
    pub req_id: u64,
    pub trace_id: u64,
    pub flags: u16,
    pub span_id: u16,
    pub body_len: u32,
    pub bulk_len: u32,
    pub body_crc32c: u32,
    pub bulk_crc32c: u32,
}

impl FrameHeader {
    /// 新帧头：proto_major = 1，长度、CRC、flags、span_id 都是 0（发送前由 core 填）。
    pub fn new(kind: FrameKind, msg_type: u16, req_id: u64, trace_id: u64) -> FrameHeader {
        FrameHeader {
            proto_major: PROTO_MAJOR,
            kind,
            msg_type,
            req_id,
            trace_id,
            flags: 0,
            span_id: 0,
            body_len: 0,
            bulk_len: 0,
            body_crc32c: 0,
            bulk_crc32c: 0,
        }
    }

    /// 编成 48 字节（小端），最后 4 字节是 crc32c(0..44)。
    pub fn encode(&self) -> [u8; HEADER_LEN] {
        let mut b = [0u8; HEADER_LEN];
        b[0..4].copy_from_slice(&MAGIC);
        b[4] = self.proto_major;
        b[5] = self.kind.as_u8();
        b[6..8].copy_from_slice(&self.msg_type.to_le_bytes());
        b[8..16].copy_from_slice(&self.req_id.to_le_bytes());
        b[16..24].copy_from_slice(&self.trace_id.to_le_bytes());
        b[24..26].copy_from_slice(&self.flags.to_le_bytes());
        b[26..28].copy_from_slice(&self.span_id.to_le_bytes());
        b[28..32].copy_from_slice(&self.body_len.to_le_bytes());
        b[32..36].copy_from_slice(&self.bulk_len.to_le_bytes());
        b[36..40].copy_from_slice(&self.body_crc32c.to_le_bytes());
        b[40..44].copy_from_slice(&self.bulk_crc32c.to_le_bytes());
        let crc = crc32c::crc32c(&b[0..44]);
        b[44..48].copy_from_slice(&crc.to_le_bytes());
        b
    }

    /// 解码并校验：magic、header CRC、proto_major、kind、长度上限。任何一项不符 → `PROTO_UNSUPPORTED`
    /// （调用方关闭连接并计数）。
    pub fn decode(b: &[u8; HEADER_LEN]) -> Result<FrameHeader, ErrCode> {
        let u16_at = |o: usize| u16::from_le_bytes([b[o], b[o + 1]]);
        let u32_at = |o: usize| u32::from_le_bytes([b[o], b[o + 1], b[o + 2], b[o + 3]]);
        let u64_at =
            |o: usize| u64::from_le_bytes([b[o], b[o + 1], b[o + 2], b[o + 3], b[o + 4], b[o + 5], b[o + 6], b[o + 7]]);
        if b[0..4] != MAGIC || crc32c::crc32c(&b[0..44]) != u32_at(44) || b[4] != PROTO_MAJOR {
            return Err(ErrCode::PROTO_UNSUPPORTED);
        }
        let kind = FrameKind::from_u8(b[5]).ok_or(ErrCode::PROTO_UNSUPPORTED)?;
        let h = FrameHeader {
            proto_major: b[4],
            kind,
            msg_type: u16_at(6),
            req_id: u64_at(8),
            trace_id: u64_at(16),
            flags: u16_at(24),
            span_id: u16_at(26),
            body_len: u32_at(28),
            bulk_len: u32_at(32),
            body_crc32c: u32_at(36),
            bulk_crc32c: u32_at(40),
        };
        if h.body_len > MAX_BODY || h.bulk_len > MAX_BULK {
            return Err(ErrCode::PROTO_UNSUPPORTED);
        }
        Ok(h)
    }
}

/// ERROR 帧（flags bit3）的 body。
/// `extra` 用于 `CHUNK_DIRTY` 携带 last serving 的 target id、`STALE_CHAIN` 携带当前 chain_ver。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ErrorBody {
    pub code: u16,
    pub msg: String,
    pub leader_hint: Option<String>,
    pub retry_after_ms: u32,
    pub extra: u64,
}

// ---- 3.6.4：core 只看到这两个 ----
pub struct OutFrame {
    pub hdr: FrameHeader,
    pub body: Vec<u8>,
    pub bulk: Vec<BufSlice>,
    pub recv_into: Option<Buf>,
}

pub struct InFrame {
    pub hdr: FrameHeader,
    pub body: Vec<u8>,
    pub bulk: Option<Buf>,
    pub bulk_dropped: bool, // bulk_dropped：取不到缓冲，bulk 已读出丢弃；请求 → core 回 BUSY，OneWay（RaftMsg）→ 丢弃（raft 会重发）
}
// bulk 的各段在线上按顺序拼成一段（TCP 依次 write_all；RDMA 依次 READ 进一个 Buf），接收方看到的永远是一个 Buf。
// recv_into 是请求方为"期望的响应 bulk"预先分配的缓冲：TCP 把响应 bulk read_exact 进去，RDMA 让对端 WRITE 进去。

#[cfg(test)]
mod tests {
    use super::*;
    use crate::base::rand::XorShift64;

    fn sample() -> FrameHeader {
        let mut h = FrameHeader::new(FrameKind::Request, MSG_META_CALL, 0x0102_0304_0506_0708, 0x1122_3344_5566_7788);
        h.flags = FLAG_HAS_BULK | FLAG_LAST_CHUNK;
        h.span_id = 3;
        h.body_len = 100;
        h.bulk_len = 4 << 20;
        h.body_crc32c = 0xdead_beef;
        h.bulk_crc32c = 0xcafe_f00d;
        h
    }

    #[test]
    fn layout_matches_table() {
        let b = sample().encode();
        assert_eq!(&b[0..4], b"WMF1");
        assert_eq!(b[4], 1);
        assert_eq!(b[5], 0);
        assert_eq!(&b[6..8], &[0x00, 0x01], "msg_type 0x0100 小端");
        assert_eq!(&b[8..16], &[8, 7, 6, 5, 4, 3, 2, 1]);
        assert_eq!(&b[24..26], &[0x11, 0x00]);
        assert_eq!(&b[26..28], &[3, 0]);
        assert_eq!(&b[28..32], &100u32.to_le_bytes());
        assert_eq!(&b[44..48], &crc32c::crc32c(&b[0..44]).to_le_bytes());
        assert_eq!(FrameHeader::decode(&b), Ok(sample()));
    }

    #[test]
    fn rejects_bad_headers() {
        let good = sample().encode();
        let mut b = good;
        b[0] = b'X';
        assert_eq!(FrameHeader::decode(&b), Err(ErrCode::PROTO_UNSUPPORTED), "magic");
        let mut b = good;
        b[20] ^= 1;
        assert_eq!(FrameHeader::decode(&b), Err(ErrCode::PROTO_UNSUPPORTED), "header CRC");
        let mut h = sample();
        h.proto_major = 2;
        assert_eq!(FrameHeader::decode(&h.encode()), Err(ErrCode::PROTO_UNSUPPORTED), "proto_major");
        let mut h = sample();
        h.body_len = MAX_BODY + 1;
        assert_eq!(FrameHeader::decode(&h.encode()), Err(ErrCode::PROTO_UNSUPPORTED), "body 超限");
        let mut h = sample();
        h.bulk_len = MAX_BULK + 1;
        assert_eq!(FrameHeader::decode(&h.encode()), Err(ErrCode::PROTO_UNSUPPORTED), "bulk 超限");
        let mut b = good;
        b[5] = 9;
        let crc = crc32c::crc32c(&b[0..44]);
        b[44..48].copy_from_slice(&crc.to_le_bytes());
        assert_eq!(FrameHeader::decode(&b), Err(ErrCode::PROTO_UNSUPPORTED), "未知 kind");
    }

    #[test]
    fn random_bytes_never_panic() {
        let mut r = XorShift64::new(7);
        for _ in 0..10_000 {
            let mut b = [0u8; HEADER_LEN];
            r.fill(&mut b);
            if r.below(2) == 0 {
                b[0..4].copy_from_slice(&MAGIC); // 一半的样本带正确 magic，走得更深
            }
            let _ = FrameHeader::decode(&b);
        }
    }
}
