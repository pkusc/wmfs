//! 管理 API 的 HMAC 签名格式（design.md 7.1，ADR-0005）：服务端（core/mgmt）与客户端（wmfs-mgr 的 ApiClient）共用，
//! 两边不会各拼各的字符串。wmfs-api 不依赖任何密码学库，所以这里只负责**拼要签名的文本**与**请求头的格式**；
//! HMAC-SHA256 与 SHA-256 由两边各自用 `hmac`/`sha2` 计算（两边都有同一组固定输入的金样测试）。
//!
//! ```text
//! Authorization: WMFS-HMAC key=<id>,ts=<unix_ms>,sig=<hex>
//! sig = HMAC-SHA256(secret(key), METHOD ‖ "\n" ‖ path_and_query ‖ "\n" ‖ ts ‖ "\n" ‖ hex(sha256(body)))
//! ```
//! `|now − ts| ≤ 300 s`；hex 一律小写；`ts` 是十进制毫秒。服务端不记录见过的签名：5 分钟内截获的写请求可以被重放
//! （首发只在可信网络中使用，ADR-0009）。

use crate::hex::unhex;

/// 请求头里的方案名。
pub const SCHEME: &str = "WMFS-HMAC";
/// 时间戳允许的偏差（毫秒）。
pub const MAX_SKEW_MS: u64 = 300_000;

/// 要签名的文本：`METHOD\npath_and_query\nts\nhex(sha256(body))`。
pub fn sign_input(method: &str, path_and_query: &str, ts_ms: u64, body_sha256_hex: &str) -> String {
    format!("{method}\n{path_and_query}\n{ts_ms}\n{body_sha256_hex}")
}

/// `Authorization` 头的值：`WMFS-HMAC key=<id>,ts=<unix_ms>,sig=<hex>`。
pub fn auth_header(key_id: u32, ts_ms: u64, sig_hex: &str) -> String {
    format!("{SCHEME} key={key_id},ts={ts_ms},sig={sig_hex}")
}

/// 解析后的 `Authorization` 头。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AuthHeader {
    pub key_id: u32,
    pub ts_ms: u64,
    pub sig: [u8; 32],
}

/// 解析 `Authorization` 头。来自网络、不可信：任何输入都只返回错误，不 panic。
pub fn parse_auth_header(v: &str) -> Result<AuthHeader, String> {
    let rest = v.trim().strip_prefix(SCHEME).ok_or("Authorization 头应以 WMFS-HMAC 开头")?;
    let (mut key_id, mut ts_ms, mut sig) = (None, None, None);
    for kv in rest.trim().split(',') {
        match kv.trim().split_once('=') {
            Some(("key", x)) => key_id = x.parse::<u32>().ok(),
            Some(("ts", x)) => ts_ms = x.parse::<u64>().ok(),
            Some(("sig", x)) => {
                let mut b = [0u8; 32];
                sig = unhex(x, &mut b).then_some(b);
            }
            _ => return Err(format!("Authorization 头里有不认识的项：{kv}")),
        }
    }
    match (key_id, ts_ms, sig) {
        (Some(key_id), Some(ts_ms), Some(sig)) => Ok(AuthHeader { key_id, ts_ms, sig }),
        _ => Err("Authorization 头应为 WMFS-HMAC key=<十进制>,ts=<十进制毫秒>,sig=<64 位小写 hex>".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hex::hex;

    #[test]
    fn sign_input_layout() {
        let s = sign_input("POST", "/api/v1/failpoints?x=1", 1_792_454_400_123, "ab");
        assert_eq!(s, "POST\n/api/v1/failpoints?x=1\n1792454400123\nab");
    }

    #[test]
    fn header_round_trip_and_garbage() {
        let sig = [0x5a; 32];
        let h = auth_header(7, 123, &hex(&sig));
        assert_eq!(h, format!("WMFS-HMAC key=7,ts=123,sig={}", "5a".repeat(32)));
        assert_eq!(parse_auth_header(&h), Ok(AuthHeader { key_id: 7, ts_ms: 123, sig }));
        for bad in [
            "",
            "Basic abc",
            "WMFS-HMAC key=7,ts=123",
            "WMFS-HMAC key=x,ts=1,sig=00",
            "WMFS-HMAC key=1,ts=1,sig=zz",
            "WMFS-HMAC key=1,ts=1,sig=ab,extra=1",
            &format!("WMFS-HMAC key=1,ts=1,sig={}é", "a".repeat(62)),
        ] {
            assert!(parse_auth_header(bad).is_err(), "应拒绝：{bad}");
        }
    }
}
