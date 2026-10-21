//! 带 HMAC 签名的管理 API 客户端（design.md 3.12、7.1），CLI 与 mgr 共用。
//!
//! - ureq 的 agent 必须 `proxy(None)`（本机设置了 http_proxy，不这样访问 127.0.0.1 会走代理失败）；
//! - 地址列表（`WMFS_CLUSTER` 逗号分隔拆开）依次尝试：连不上就换下一个；遇到 409 `{"error":"not_leader","leader":"http://…"}`
//!   就跟随 leader（最多 3 跳，不做服务端代理）；
//! - 每个请求都签名：`Authorization: WMFS-HMAC key=<id>,ts=<unix_ms>,sig=<hex>`，要签名的文本由
//!   `wmfs_api::auth::sign_input` 拼（与服务端同一个函数）；`/metrics` 与 `/api/v1/health` 服务端不检查签名，带着也无妨。
//! - 非 2xx 的回复变成 [`ApiError`]（= 服务端的 `{"error","message"}` 加 HTTP 状态码）；连不上是 `status = 0`。

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hmac::{Hmac, KeyInit, Mac};
use serde_json::Value;
use sha2::{Digest, Sha256};
use wmfs_api::auth::{auth_header, sign_input};
use wmfs_api::hex::hex;
use wmfs_api::types::KeyFile;

pub struct ApiClient {
    agent: ureq::Agent,
    addrs: Vec<String>,
    key: KeyFile,
}

impl ApiClient {
    /// addrs：逗号分隔的 WMFS_CLUSTER 拆开（`host:port` 或 `http://host:port`）
    pub fn new(addrs: Vec<String>, key: KeyFile) -> ApiClient {
        let cfg = ureq::Agent::config_builder()
            .proxy(None)
            .http_status_as_error(false)
            .timeout_global(Some(Duration::from_secs(10)))
            .build();
        ApiClient { agent: cfg.into(), addrs: addrs.iter().map(|a| base_url(a)).collect(), key }
    }

    /// 依次尝试地址；409 跟随 leader
    pub fn get<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T, ApiError> {
        self.request("GET", path, Vec::new())
    }

    pub fn post<B: serde::Serialize, T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T, ApiError> {
        let b =
            serde_json::to_vec(body).map_err(|e| ApiError::new(0, "einval", format!("请求无法编码为 JSON：{e}")))?;
        self.request("POST", path, b)
    }

    pub fn delete<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T, ApiError> {
        self.request("DELETE", path, Vec::new())
    }

    fn request<T: serde::de::DeserializeOwned>(&self, method: &str, path: &str, body: Vec<u8>) -> Result<T, ApiError> {
        let mut last = ApiError::new(
            0,
            "unreachable",
            "没有可用的管理地址（WMFS_CLUSTER 为空？先 eval \"$(wmfs dev env)\"）".into(),
        );
        for addr in &self.addrs {
            let mut base = addr.clone();
            for _hop in 0..3 {
                let (status, v) = match self.once(method, &base, path, &body) {
                    Ok(r) => r,
                    Err(e) => {
                        last = e; // 连不上：换下一个地址
                        break;
                    }
                };
                if status == 409
                    && let Some(l) = v.get("leader").and_then(Value::as_str)
                {
                    base = base_url(l); // 不是 leader：跟随它给的地址
                    continue;
                }
                if !(200..300).contains(&status) {
                    return Err(ApiError::from_reply(status, &v));
                }
                return serde_json::from_value(v)
                    .map_err(|e| ApiError::new(status, "bad_reply", format!("回复的 JSON 结构不对：{e}")));
            }
        }
        Err(last)
    }

    /// 发一个签好名的请求，返回 (状态码, JSON)；回复不是 JSON 时（例如 /metrics）返回 JSON 字符串。
    fn once(&self, method: &str, base: &str, path: &str, body: &[u8]) -> Result<(u16, Value), ApiError> {
        // 签名时间戳用标准库的墙钟（design.md 0.2 第 6 条的例外，见那里的实现注记）：wmfs-mgr 不依赖 core，
        // 用不了 core::base::clock，所以 clock.wall.skew_ms 这个 fail point 对 CLI 与 mgr 的签名时间不起作用
        let ts = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0);
        let sig = hex(&sign(&self.key.secret, &sign_input(method, path, ts, &hex(&Sha256::digest(body)))));
        let (url, auth) = (format!("{base}{path}"), auth_header(self.key.key_id, ts, &sig));
        let r = match method {
            "GET" => self.agent.get(&url).header("Authorization", &auth).call(),
            "DELETE" => self.agent.delete(&url).header("Authorization", &auth).call(),
            _ => self
                .agent
                .post(&url)
                .header("Authorization", &auth)
                .header("Content-Type", "application/json")
                .send(body),
        };
        let mut resp = r.map_err(|e| ApiError::new(0, "unreachable", format!("连不上 {url}：{e}")))?;
        let status = resp.status().as_u16();
        let text = resp
            .body_mut()
            .read_to_string()
            .map_err(|e| ApiError::new(status, "bad_reply", format!("读回复失败：{e}")))?;
        Ok((status, serde_json::from_str(&text).unwrap_or(Value::String(text))))
    }
}

/// HMAC-SHA256(secret, text)：与服务端（core/mgmt/auth.rs）算法相同；两边有同一组固定输入的金样测试。
fn sign(secret: &[u8; 32], text: &str) -> [u8; 32] {
    let mut m = <Hmac<Sha256> as KeyInit>::new_from_slice(secret)
        .unwrap_or_else(|_| panic!("不变量破坏：HMAC 接受任意长度的密钥"));
    m.update(text.as_bytes());
    m.finalize().into_bytes().into()
}

/// `127.0.0.1:19180` → `http://127.0.0.1:19180`（已带 http:// 的原样，去掉末尾的 /）。
fn base_url(a: &str) -> String {
    let a = a.trim().trim_end_matches('/');
    if a.starts_with("http://") || a.starts_with("https://") { a.to_string() } else { format!("http://{a}") }
}

/// = 服务端的 `{"error","message"}`，另加 HTTP 状态码（连不上时为 0）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApiError {
    pub status: u16,
    pub code: String,
    pub message: String,
}

impl ApiError {
    fn new(status: u16, code: &str, message: String) -> ApiError {
        ApiError { status, code: code.into(), message }
    }
    fn from_reply(status: u16, v: &Value) -> ApiError {
        let s = |k: &str| v.get(k).and_then(Value::as_str).map(str::to_string);
        let code = s("error").unwrap_or_else(|| format!("http_{status}"));
        ApiError { status, code, message: s("message").unwrap_or_else(|| v.to_string()) }
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}（{}，HTTP {}）", self.message, self.code, self.status)
    }
}

#[cfg(test)]
mod tests;
