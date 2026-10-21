//! 线上协议（design.md 3.3，ADR-0018）：TCP 与 RDMA 跑同一种帧。
//!
//! 帧 = 48 字节帧头 + body（postcard 编码的消息，永远是小结构）+ bulk（原始字节：TCP 紧跟在 body 后；
//! 支持单边操作的传输用描述符，由"接收请求的一方"发起单边操作，3.6.4）。大块字节一律走 bulk。
//! 帧头编解码在 core（[`frame`]），传输插件只搬字节。
//!
//! 接口骨架（M0）：本模块的全部消息结构以源码为唯一真源，`tests/golden/` 有每种消息的金样。
//! 演进规则（3.2.11）：同一 msg_type 的 body 结构永不改变；枚举只能在末尾加变体，
//! 并且只能在对端于握手中声明支持（feature 位）后发送；已有变体的字段永不增删改。

pub mod cap;
pub mod data;
pub mod features;
pub mod frame;
pub mod handshake;
pub mod meta;
pub mod peer;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::base::ErrCode;

/// 消息体编码（postcard：不自描述，字段顺序就是格式；enum 存的是从 0 开始的变体序号）。
pub fn encode_body<T: Serialize>(m: &T) -> Vec<u8> {
    postcard::to_stdvec(m).unwrap_or_else(|e| panic!("不变量破坏：消息编码失败：{e}"))
}

/// 消息体解码：解不开（含不认识的枚举变体）或有多余的尾部字节 → `FEATURE_UNSUPPORTED`（3.3.2：不断开连接）。
pub fn decode_body<T: DeserializeOwned>(b: &[u8]) -> Result<T, ErrCode> {
    match postcard::take_from_bytes::<T>(b) {
        Ok((v, [])) => Ok(v),
        _ => Err(ErrCode::FEATURE_UNSUPPORTED),
    }
}
