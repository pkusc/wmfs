//! ApiClient 的测试：签名的金样（与 core 的 tests/golden_net.rs 是同一组输入与期望值）、连不上换地址、409 跟随 leader、
//! 错误回复变成 ApiError。测试用的 HTTP 服务端是 tiny_http（wmfs-mgr 不依赖 core）。

use std::sync::Arc;

use wmfs_api::auth::{AuthHeader, parse_auth_header};
use wmfs_api::types::FailpointList;

use super::*;

/// 金样：固定输入的签名（core 端的同一个值在 core/tests/golden_net.rs）。改了签名格式 = 管理 API 不兼容，需要 ADR。
const SIG_GOLDEN: &str = "d4713d8b4cce54afac1da24bed20a6871f5a7ea00570cb2926ca58c1c09fc163";

#[test]
fn signature_golden() {
    let text = sign_input("POST", "/api/v1/failpoints", 1_792_454_400_123, &hex(&Sha256::digest(b"{}")));
    assert_eq!(hex(&sign(&[0x11; 32], &text)), SIG_GOLDEN);
}

/// 起一个 tiny_http 服务端：`leader` 为 Some 时对每个请求回 409 指向它；否则校验签名后回空的 fail point 列表。
fn server(leader: Option<String>, secret: [u8; 32]) -> String {
    let s = Arc::new(tiny_http::Server::http("127.0.0.1:0").unwrap());
    let addr = s.server_addr().to_ip().unwrap().to_string();
    std::thread::spawn(move || {
        while let Ok(mut req) = s.recv() {
            let mut body = Vec::new();
            req.as_reader().read_to_end(&mut body).unwrap();
            let auth = req.headers().iter().find(|h| h.field.equiv("Authorization")).map(|h| h.value.to_string());
            let (status, json) = match (&leader, auth.as_deref().map(parse_auth_header)) {
                (Some(l), _) => (409, format!(r#"{{"error":"not_leader","leader":"{l}"}}"#)),
                (None, Some(Ok(AuthHeader { key_id: 1, ts_ms, sig }))) => {
                    let text = sign_input(req.method().as_str(), req.url(), ts_ms, &hex(&Sha256::digest(&body)));
                    if sign(&secret, &text) == sig {
                        (200, r#"{"failpoints":[]}"#.to_string())
                    } else {
                        (401, r#"{"error":"auth_failed","message":"签名不对"}"#.to_string())
                    }
                }
                _ => (401, r#"{"error":"auth_failed","message":"缺少签名"}"#.to_string()),
            };
            req.respond(tiny_http::Response::from_string(json).with_status_code(status)).unwrap();
        }
    });
    addr
}

#[test]
fn skips_dead_addresses_and_follows_the_leader() {
    let secret = [0x22; 32];
    let leader = server(None, secret);
    let follower = server(Some(format!("http://{leader}")), secret);
    // 127.0.0.1:1 没人监听：换下一个；follower 回 409：跟随到 leader；leader 校验签名
    let c = ApiClient::new(vec!["127.0.0.1:1".into(), follower], KeyFile { key_id: 1, secret });
    let l: FailpointList = c.get("/api/v1/failpoints").unwrap();
    assert!(l.failpoints.is_empty());
    let l: FailpointList = c.post("/api/v1/failpoints", &serde_json::json!({"name": "a", "action": "off"})).unwrap();
    assert!(l.failpoints.is_empty());
    let l: FailpointList = c.delete("/api/v1/failpoints?name=a").unwrap();
    assert!(l.failpoints.is_empty());
}

#[test]
fn wrong_key_and_unreachable_become_api_errors() {
    let addr = server(None, [0x33; 32]);
    let bad = ApiClient::new(vec![addr], KeyFile { key_id: 1, secret: [0x44; 32] });
    let e = bad.get::<FailpointList>("/api/v1/failpoints").unwrap_err();
    assert_eq!((e.status, e.code.as_str()), (401, "auth_failed"));
    let none = ApiClient::new(vec!["127.0.0.1:1".into()], KeyFile { key_id: 1, secret: [0; 32] });
    assert_eq!(none.get::<FailpointList>("/x").unwrap_err().status, 0, "连不上：status = 0");
    assert_eq!(base_url("http://h:1/"), "http://h:1");
    assert_eq!(base_url("h:1"), "http://h:1");
}
