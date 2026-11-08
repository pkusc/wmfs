//! 带 HMAC 签名的管理 API 客户端（design.md 3.12、7.1），CLI 与 mgr 共用。
//!
//! - ureq 的 agent 必须 `proxy(None)`（本机设置了 http_proxy，不这样访问 127.0.0.1 会走代理失败）；
//! - 地址列表（`WMFS_CLUSTER` 逗号分隔拆开）依次尝试：连不上就换下一个；遇到 409 `{"error":"not_leader","leader":"http://…"}`
//!   就跟随 leader（最多 3 跳，不做服务端代理）；实现注记（M8）：回 503 `not_leader`（这个节点还不知道 leader，例如刚重启）也换下一个
//!   地址，全都不行才把最后一个错误交给调用方；
//! - 每个请求都签名：`Authorization: WMFS-HMAC key=<id>,ts=<unix_ms>,sig=<hex>`，要签名的文本由
//!   `wmfs_api::auth::sign_input` 拼（与服务端同一个函数）；`/metrics` 与 `/api/v1/health` 服务端不检查签名，带着也无妨。
//! - 非 2xx 的回复变成 [`ApiError`]（= 服务端的 `{"error","message"}` 加 HTTP 状态码；410 另带 `oldest`）；连不上是 `status = 0`。
//!
//! 实现注记（M14）：① 下一次从上一次回答了的那个地址问起（列表里的第一个节点被冻住时，不必每个请求都先白等一个超时）；② 只问一个节点的
//! [`ApiClient::get_at`]（节点本地的端点：`/metrics`、`/api/v1/node`、`/api/v1/qos`、`/api/v1/trace/…`）用超时 3 s 的 agent——一个冻住的
//! 节点不把 mgr 的 `/metrics`、dashboard 的逐节点代理拖到 10 s；③ [`ApiClient::node_addrs`]：集群里全部节点的管理地址（`GET /api/v1/cluster`），
//! 之后 join 的节点、`--cluster` 里没写的只做数据的节点也问得到。

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use hmac::{Hmac, KeyInit, Mac};
use serde_json::Value;
use sha2::{Digest, Sha256};
use wmfs_api::auth::{auth_header, sign_input};
use wmfs_api::hex::hex;
use wmfs_api::types::{ChangelogPage, ClusterReply, KeyFile};

#[derive(Clone)]
pub struct ApiClient {
    agent: ureq::Agent,
    node_agent: ureq::Agent, // 只问一个节点的请求：超时短（见文件头 ②）
    addrs: Vec<String>,
    key: KeyFile,
    prefer: Arc<AtomicUsize>, // 上一次回答了的地址的下标（见文件头 ①）
}

/// 一个 ureq agent：不走代理、非 2xx 也交回回复体、整个请求最多 `secs` 秒。
fn agent(secs: u64) -> ureq::Agent {
    let cfg = ureq::Agent::config_builder().proxy(None).http_status_as_error(false);
    cfg.timeout_global(Some(Duration::from_secs(secs))).build().into()
}

impl ApiClient {
    /// addrs：逗号分隔的 WMFS_CLUSTER 拆开（`host:port` 或 `http://host:port`）
    pub fn new(addrs: Vec<String>, key: KeyFile) -> ApiClient {
        let addrs = addrs.iter().map(|a| base_url(a)).collect();
        ApiClient { agent: agent(10), node_agent: agent(3), addrs, key, prefer: Arc::default() }
    }

    /// 同一个客户端，只是跟随 leader 的请求最多等 `secs` 秒（M14：exporter 每 5 min 一次的 `GET /api/v1/status?path=/` 要扫整棵树）。
    pub fn with_timeout(mut self, secs: u64) -> ApiClient {
        self.agent = agent(secs);
        self
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

    /// body 原样发出（M13：存储类的 TOML，`POST /api/v1/classes`、`PUT /api/v1/classes/<名字>`）；`method` 是 POST 或 PUT。
    pub fn send_raw<T: serde::de::DeserializeOwned>(
        &self,
        method: &str,
        path: &str,
        body: &[u8],
    ) -> Result<T, ApiError> {
        self.request(method, path, body.to_vec())
    }

    /// 构造时给的管理地址（`http://host:port`）。
    pub fn addrs(&self) -> &[String] {
        &self.addrs
    }

    /// 集群里没有移除的节点的管理地址（见文件头 ③）；问不到集群概览就用构造时给的地址。
    pub fn node_addrs(&self) -> Vec<String> {
        match self.get::<ClusterReply>("/api/v1/cluster") {
            Ok(r) => r.nodes.iter().filter(|n| n.state != "removed").map(|n| base_url(&n.mgmt_addr)).collect(),
            Err(_) => self.addrs.clone(),
        }
    }

    /// 此刻最新的 changelog seq（`after` 很大：不取事件，只要 `last_seq`）。
    pub fn last_seq(&self) -> Result<u64, ApiError> {
        self.get::<ChangelogPage>(&format!("/api/v1/changelog?after={}&limit=1", u64::MAX - 1)).map(|p| p.last_seq)
    }

    /// 只问 `addr` 这一个节点（节点本地的端点，见文件头 ②）；不跟随 leader、不换地址。回复不是 JSON 时（`/metrics`）`T = String`
    /// 拿到原文。
    pub fn get_at<T: serde::de::DeserializeOwned>(&self, addr: &str, path: &str) -> Result<T, ApiError> {
        let (status, v) = self.once(&self.node_agent, "GET", addr, path, &[])?;
        decode(status, v)
    }

    fn request<T: serde::de::DeserializeOwned>(&self, method: &str, path: &str, body: Vec<u8>) -> Result<T, ApiError> {
        let mut last = ApiError::new(
            0,
            "unreachable",
            "没有可用的管理地址（WMFS_CLUSTER 为空？先 eval \"$(wmfs dev env)\"）".into(),
        );
        let first = self.prefer.load(Ordering::Relaxed);
        for i in (0..self.addrs.len()).map(|k| (first + k) % self.addrs.len()) {
            let mut base = self.addrs[i].clone();
            for _hop in 0..3 {
                let (status, v) = match self.once(&self.agent, method, &base, path, &body) {
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
                if status == 503 && v.get("error").and_then(Value::as_str) == Some("not_leader") {
                    last = ApiError::from_reply(status, &v); // 这个节点还不知道 leader：问下一个地址
                    break;
                }
                self.prefer.store(i, Ordering::Relaxed);
                return decode(status, v);
            }
        }
        Err(last)
    }

    /// 用 `agent` 发一个签好名的请求，返回 (状态码, JSON)；回复不是 JSON 时（例如 /metrics）返回 JSON 字符串。
    fn once(
        &self,
        agent: &ureq::Agent,
        method: &str,
        base: &str,
        path: &str,
        body: &[u8],
    ) -> Result<(u16, Value), ApiError> {
        // 签名时间戳用标准库的墙钟（design.md 0.2 第 6 条的例外，见那里的实现注记）：wmfs-mgr 不依赖 core，
        // 用不了 core::base::clock，所以 clock.wall.skew_ms 这个 fail point 对 CLI 与 mgr 的签名时间不起作用
        let ts = crate::now_ms();
        let sig = hex(&sign(&self.key.secret, &sign_input(method, path, ts, &hex(&Sha256::digest(body)))));
        let url = format!("{base}{path}");
        let auth = auth_header(self.key.key_id, ts, &sig);
        let r = match method {
            "GET" => agent.get(&url).header("Authorization", &auth).call(),
            "DELETE" => agent.delete(&url).header("Authorization", &auth).call(),
            "PUT" => agent.put(&url).header("Authorization", &auth).send(body),
            _ => agent.post(&url).header("Authorization", &auth).header("Content-Type", "application/json").send(body),
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

/// 一个回复：非 2xx → [`ApiError`]；否则把 JSON 解成 `T`。
fn decode<T: serde::de::DeserializeOwned>(status: u16, v: Value) -> Result<T, ApiError> {
    if !(200..300).contains(&status) {
        return Err(ApiError::from_reply(status, &v));
    }
    serde_json::from_value(v).map_err(|e| ApiError::new(status, "bad_reply", format!("回复的 JSON 结构不对：{e}")))
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

/// = 服务端的 `{"error","message"}`，另加 HTTP 状态码（连不上时为 0）。`oldest`：changelog 的 410 回复里最早还在的 seq（3.11；
/// 别的错误为 None），读到 410 的消费者从它之前接着读，不用再找（实现注记（M14））。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApiError {
    pub status: u16,
    pub code: String,
    pub message: String,
    pub oldest: Option<u64>,
}

impl ApiError {
    pub fn new(status: u16, code: &str, message: String) -> ApiError {
        ApiError { status, code: code.into(), message, oldest: None }
    }
    fn from_reply(status: u16, v: &Value) -> ApiError {
        let s = |k: &str| v.get(k).and_then(Value::as_str).map(str::to_string);
        let code = s("error").unwrap_or_else(|| format!("http_{status}"));
        let message = s("message").unwrap_or_else(|| v.to_string());
        ApiError { status, code, message, oldest: v.get("oldest").and_then(Value::as_u64) }
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}（{}，HTTP {}）", self.message, self.code, self.status)
    }
}

#[cfg(test)]
mod tests;
