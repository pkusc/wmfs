//! 管理 API 的签名校验（design.md 7.1）：`sig = HMAC-SHA256(secret(key), METHOD ‖ "\n" ‖ path_and_query ‖ "\n" ‖ ts ‖ "\n" ‖
//! hex(sha256(body)))`，`|now − ts| ≤ 300 s`；写需要 admin 角色，读需要任一已认证 key。要签名的文本由
//! `wmfs_api::auth::sign_input` 拼（与 ApiClient 同一个函数）。服务端不记录见过的签名：5 分钟内截获的写请求可以被重放
//! （首发只在可信网络中使用，ADR-0009）。

use sha2::{Digest, Sha256};
use wmfs_api::auth::{MAX_SKEW_MS, parse_auth_header, sign_input};
use wmfs_api::hex::hex;

use crate::base::clock;
use crate::base::types::Role;
use crate::hooks::traits::KeyLookup;
use crate::proto::handshake::hmac_verify;

/// 校验失败：HTTP 状态码、错误码（小写，`{"error": …}`）、中文说明。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Denied {
    pub status: u16,
    pub code: &'static str,
    pub message: String,
}

/// 通过了签名校验的调用者（实现注记（M7）：以前只返回角色；依赖元数据的端点还要 key 号与租户，写进 changelog 的身份）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Caller {
    pub key_id: u32,
    pub role: Role,
    pub tenant: u32,
}

/// 校验一个请求的签名；成功返回调用者。`need_admin` = 写操作。
pub fn check(
    keys: &dyn KeyLookup,
    method: &str,
    path_and_query: &str,
    header: Option<&str>,
    body: &[u8],
    need_admin: bool,
) -> Result<Caller, Denied> {
    let deny = |message: String| Denied { status: 401, code: "auth_failed", message };
    let h = header.ok_or_else(|| deny("缺少 Authorization 头（WMFS-HMAC key=…,ts=…,sig=…）".into()))?;
    let a = parse_auth_header(h).map_err(deny)?;
    if clock::wall_ms().abs_diff(a.ts_ms) > MAX_SKEW_MS {
        return Err(deny("时间戳与服务端相差超过 300 秒（请检查两台机器的时钟）".into()));
    }
    let k = a.key_id;
    let meta = keys.meta(k).filter(|m| !m.revoked).ok_or_else(|| deny(format!("key {k} 不存在或已撤销")))?;
    let secret = keys.secret(k).ok_or_else(|| deny(format!("key {k} 不存在")))?;
    let input = sign_input(method, path_and_query, a.ts_ms, &hex(&Sha256::digest(body)));
    if !hmac_verify(&secret, &[input.as_bytes()], &a.sig) {
        return Err(deny("签名不对（密钥错误，或请求在路上被改过）".into()));
    }
    let role = Role::from_u8(meta.role).ok_or_else(|| deny(format!("key {k} 的角色无效")))?;
    if need_admin && role != Role::Admin {
        return Err(Denied { status: 403, code: "eacces", message: "写操作需要 admin key".into() });
    }
    Ok(Caller { key_id: k, role, tenant: meta.tenant })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::schema::records::KeyMetaV1;
    use crate::proto::handshake::hmac_sha256;
    use wmfs_api::auth::auth_header;

    struct Keys;
    impl KeyLookup for Keys {
        fn secret(&self, key_id: u32) -> Option<[u8; 32]> {
            (key_id <= 3).then_some([key_id as u8; 32])
        }
        fn meta(&self, key_id: u32) -> Option<KeyMetaV1> {
            let role = match key_id {
                1 => 1,
                2 | 3 => 2,
                _ => return None,
            };
            let (name, tenant, revoked) = ("k".to_string(), 0, key_id == 3);
            Some(KeyMetaV1 {
                name,
                role,
                tenant,
                uid_ranges: vec![],
                root_squash: false,
                path_caps: vec![],
                revoked,
                created_ms: 0,
            })
        }
    }

    fn signed(key: u32, method: &str, pq: &str, ts: u64, body: &[u8]) -> String {
        let input = sign_input(method, pq, ts, &hex(&Sha256::digest(body)));
        auth_header(key, ts, &hex(&hmac_sha256(&[key as u8; 32], &[input.as_bytes()])))
    }

    #[test]
    fn good_signatures_pass_and_roles_are_enforced() {
        let now = clock::wall_ms();
        let pq = "/api/v1/failpoints";
        let h = signed(1, "POST", pq, now, b"{}");
        assert_eq!(check(&Keys, "POST", pq, Some(&h), b"{}", true).map(|c| (c.key_id, c.role)), Ok((1, Role::Admin)));
        let h = signed(2, "GET", pq, now, b"");
        let r = check(&Keys, "GET", pq, Some(&h), b"", false).map(|c| c.role);
        assert_eq!(r, Ok(Role::Client), "读只要任一已认证 key");
        let h = signed(2, "POST", pq, now, b"{}");
        assert_eq!(check(&Keys, "POST", pq, Some(&h), b"{}", true).unwrap_err().status, 403, "写需要 admin");
    }

    #[test]
    fn bad_requests_are_rejected() {
        let now = clock::wall_ms();
        let pq = "/api/v1/failpoints";
        let reject = |h: Option<&str>, body: &[u8]| check(&Keys, "POST", pq, h, body, true).unwrap_err().status;
        assert_eq!(reject(None, b""), 401, "没有签名");
        assert_eq!(reject(Some(&signed(1, "POST", pq, now, b"{}")), b"{ }"), 401, "body 被改过");
        assert_eq!(reject(Some(&signed(1, "POST", "/other", now, b"")), b""), 401, "路径不同");
        assert_eq!(reject(Some(&signed(1, "POST", pq, now - 301_000, b"")), b""), 401, "超过 300 s");
        assert_eq!(reject(Some(&signed(3, "POST", pq, now, b"")), b""), 401, "已撤销的 key");
        assert_eq!(reject(Some(&signed(9, "POST", pq, now, b"")), b""), 401, "不认识的 key");
        assert_eq!(reject(Some("WMFS-HMAC key=1"), b""), 401);
    }
}
