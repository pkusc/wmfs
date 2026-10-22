//! 管理 API 的 HTTP 服务（design.md 7.1）：tiny_http 监听、4 个处理线程 `mgmt-http-<i>`、路由与端点。
//!
//! M1 的端点：`GET /metrics`（免认证）、`GET /api/v1/health`（免认证，带本节点地址；M3 起带元数据角色与 leader 地址）、
//! `/api/v1/failpoints`（仅 node.toml 里 `failpoints_api = true` 时存在：`GET` 列出（任一已认证 key）、`POST {name, action}` 设置、
//! `DELETE ?name=` 清除（写需要 admin；清除没设置过的名字回 404））。实现注记（M1）：规格只写了 POST 与 DELETE，
//! `wmfs failpoint list` 另需要 `GET /api/v1/failpoints`；三个都回本节点当前的全部 fail point（`FailpointList`）。
//!
//! 请求体（实现注记 M1）：先看请求头里声明的长度——超过 1 MiB 的请求既不读、也不回、也不交还给 tiny_http，整个"忘掉"
//! （这条连接一直挂着）：tiny_http 丢弃没读完的 body 时会按剩余长度**一次分配内存**，`Content-Length` 写成几十 TB
//! 就会让整个进程因分配失败而退出（读到一半对端断开也一样）。不超过 1 MiB 的照常读完再处理。剩余风险：声明了 body
//! 却不发的连接会一直占着一个处理线程（tiny_http 没有读超时，丢弃请求时也要把 body 读完）；首发只在可信网络中使用（ADR-0009）。

use std::io::Read;
use std::sync::Arc;

use wmfs_api::failpoint;
use wmfs_api::metrics;
use wmfs_api::types::{ErrorReply, Failpoint, FailpointList, Health, NodeAddrs};

use crate::base::{ErrCode, log};
use crate::hooks::traits::KeyLookup;
use crate::mgmt::auth;

/// 请求体上限（字节）。
const MAX_BODY: usize = 1 << 20;
const THREADS: usize = 4;

/// health 里随时变化的部分：本节点的元数据角色与 leader 的地址（M3 起由元数据引擎给出）。
pub type RoleFn = Arc<dyn Fn() -> (String, Option<NodeAddrs>) + Send + Sync>;

/// 各端点要用的本节点信息。
pub struct MgmtCtx {
    pub health: Health, // 不变的部分（版本、cluster_id、本节点地址）
    pub role: Option<RoleFn>,
    pub keys: Arc<dyn KeyLookup + Send + Sync>,
    pub failpoints_api: bool,
}

/// 已经绑定好地址、还没开始处理请求的管理 API（先绑定，才知道实际地址写进 health）。
pub struct MgmtServer {
    server: Arc<tiny_http::Server>,
    addr: String,
}

/// 一个回复。
struct Reply {
    status: u16,
    content_type: &'static str,
    body: Vec<u8>,
}

impl MgmtServer {
    pub fn bind(listen: &str) -> Result<MgmtServer, String> {
        let server = tiny_http::Server::http(listen).map_err(|e| format!("管理 API 无法监听 {listen}：{e}"))?;
        let addr = server.server_addr().to_ip().map(|a| a.to_string()).unwrap_or_else(|| listen.to_string());
        Ok(MgmtServer { server: Arc::new(server), addr })
    }

    /// 实际监听的地址（例如 "127.0.0.1:19180"）。
    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// 起 4 个处理线程（进程退出前一直运行）。
    pub fn start(self, ctx: Arc<MgmtCtx>) -> Result<(), ErrCode> {
        for i in 0..THREADS {
            let (s, ctx) = (self.server.clone(), ctx.clone());
            let t = std::thread::Builder::new().name(format!("mgmt-http-{i}"));
            t.spawn(move || {
                while let Ok(req) = s.recv() {
                    handle(&ctx, req);
                }
            })
            .map_err(|_| ErrCode::ENOMEM)?;
        }
        Ok(())
    }
}

fn handle(ctx: &MgmtCtx, mut req: tiny_http::Request) {
    let method = req.method().as_str().to_string();
    let url = req.url().to_string();
    if req.body_length().is_some_and(|n| n > MAX_BODY) {
        // 见文件头"请求体"：不读、不回、不交还 tiny_http（否则它丢弃 body 时按声明的长度一次分配内存）
        log::warn("mgmt", format_args!("{method} {url} 声明的 body 超过 {MAX_BODY} 字节：不处理，连接挂起"));
        count(413);
        std::mem::forget(req);
        return;
    }
    let auth = req.headers().iter().find(|h| h.field.equiv("Authorization")).map(|h| h.value.as_str().to_string());
    let mut body = Vec::new();
    let read = req.as_reader().take(MAX_BODY as u64 + 1).read_to_end(&mut body); // 分块传输没有声明长度：也只读到上限
    let out = match read {
        Ok(_) if body.len() <= MAX_BODY => route(ctx, &method, &url, auth.as_deref(), &body),
        Ok(_) => err(413, "e2big", format!("请求体超过 {MAX_BODY} 字节")),
        Err(e) => err(400, "einval", format!("请求体读不完整：{e}")),
    };
    count(out.status);
    let ctype = tiny_http::Header::from_bytes("Content-Type", out.content_type)
        .unwrap_or_else(|_| panic!("不变量破坏：固定的 HTTP 头"));
    let resp = tiny_http::Response::from_data(out.body).with_status_code(out.status).with_header(ctype);
    if let Err(e) = req.respond(resp) {
        log::debug("mgmt", format_args!("回复 {method} {url} 失败：{e}"));
    }
}

fn count(status: u16) {
    metrics::counter("wmfs_mgmt_requests_total", "管理 API 处理的请求数", &[("status", &status.to_string())]).inc();
}

fn route(ctx: &MgmtCtx, method: &str, url: &str, auth: Option<&str>, body: &[u8]) -> Reply {
    let path = url.split_once('?').map_or(url, |(p, _)| p);
    match (method, path) {
        ("GET", "/metrics") => {
            let mut s = String::new();
            metrics::render_prometheus(&mut s);
            Reply { status: 200, content_type: "text/plain; version=0.0.4", body: s.into_bytes() }
        }
        ("GET", "/api/v1/health") => {
            let mut h = ctx.health.clone();
            if let Some(f) = &ctx.role {
                (h.meta_role, h.leader) = f();
            }
            json(200, &h)
        }
        (_, "/api/v1/failpoints") => failpoints(ctx, method, url, auth, body),
        _ => err(404, "enoent", format!("没有这个接口：{method} {path}")),
    }
}

fn failpoints(ctx: &MgmtCtx, method: &str, url: &str, auth: Option<&str>, body: &[u8]) -> Reply {
    if !ctx.failpoints_api {
        let msg = "本节点没有开启 fail point 接口（node.toml 里写 failpoints_api = true；wmfs dev 默认开启）";
        return err(404, "enoent", msg.into());
    }
    if let Err(d) = auth::check(&*ctx.keys, method, url, auth, body, method != "GET") {
        return err(d.status, d.code, d.message);
    }
    match method {
        "GET" => {}
        "POST" => {
            let Ok(fp) = serde_json::from_slice::<Failpoint>(body) else {
                return err(400, "einval", "body 应为 {\"name\": \"…\", \"action\": \"…\"}".into());
            };
            if let Err(e) = failpoint::set(&fp.name, &fp.action) {
                return err(400, "einval", e);
            }
            log::warn("mgmt", format_args!("经管理 API 设置 fail point：{} = {}", fp.name, fp.action));
        }
        "DELETE" => {
            let query = url.split_once('?').map_or("", |(_, q)| q);
            let Some(name) = query.split('&').find_map(|kv| kv.strip_prefix("name=")) else {
                return err(400, "einval", "缺少参数 ?name=<fail point 名字>".into());
            };
            if !failpoint::list().iter().any(|(n, _)| n == name) {
                return err(404, "enoent", format!("没有设置过 fail point {name:?}（名字拼错了？）"));
            }
            failpoint::clear(name);
            log::warn("mgmt", format_args!("经管理 API 清除 fail point：{name}"));
        }
        _ => return err(405, "einval", "只支持 GET、POST、DELETE".into()),
    }
    let failpoints = failpoint::list().into_iter().map(|(name, action)| Failpoint { name, action }).collect();
    json(200, &FailpointList { failpoints })
}

fn json<T: serde::Serialize>(status: u16, v: &T) -> Reply {
    let b = serde_json::to_vec(v).unwrap_or_else(|e| panic!("不变量破坏：管理 API 的回复无法编码为 JSON：{e}"));
    Reply { status, content_type: "application/json", body: b }
}

fn err(status: u16, error: &str, message: String) -> Reply {
    json(status, &ErrorReply { error: error.into(), message })
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::TcpStream;

    use sha2::{Digest, Sha256};
    use wmfs_api::auth::{auth_header, sign_input};
    use wmfs_api::hex::hex;

    use super::*;
    use crate::base::clock;
    use crate::init::InitKeys;
    use crate::proto::handshake::{derive_secret, hmac_sha256};

    const MASTER: [u8; 32] = [3; 32];

    fn start(failpoints_api: bool) -> String {
        let m = MgmtServer::bind("127.0.0.1:0").unwrap();
        let addr = m.addr().to_string();
        let health = Health {
            ok: true,
            node: wmfs_api::types::NodeAddrs { node_id: 7, ..Default::default() },
            ..Default::default()
        };
        let keys = Arc::new(InitKeys::new(MASTER, 0));
        m.start(Arc::new(MgmtCtx { health, role: None, keys, failpoints_api })).unwrap();
        addr
    }

    /// 发一个 HTTP/1.1 请求；`key` = Some(key 号) 时按 7.1 签名。返回 (状态码, body)。
    fn http(addr: &str, method: &str, path: &str, key: Option<u32>, body: &str) -> (u16, String) {
        let mut req =
            format!("{method} {path} HTTP/1.1\r\nHost: t\r\nConnection: close\r\nContent-Length: {}\r\n", body.len());
        if let Some(k) = key {
            let ts = clock::wall_ms();
            let text = sign_input(method, path, ts, &hex(&Sha256::digest(body.as_bytes())));
            let sig = hmac_sha256(&derive_secret(&MASTER, k), &[text.as_bytes()]);
            req += &format!("Authorization: {}\r\n", auth_header(k, ts, &hex(&sig)));
        }
        let mut s = TcpStream::connect(addr).unwrap();
        s.write_all(format!("{req}\r\n{body}").as_bytes()).unwrap();
        let mut resp = String::new();
        s.read_to_string(&mut resp).unwrap();
        let status = resp[9..12].parse().unwrap();
        (status, resp.split_once("\r\n\r\n").map(|(_, b)| b.to_string()).unwrap_or_default())
    }

    #[test]
    fn health_metrics_and_signed_failpoints() {
        let a = start(true);
        let (s, b) = http(&a, "GET", "/api/v1/health", None, "");
        assert_eq!(s, 200, "health 免认证");
        assert_eq!(serde_json::from_str::<Health>(&b).unwrap().node.node_id, 7);
        assert_eq!(http(&a, "GET", "/metrics", None, "").0, 200, "/metrics 免认证");
        assert_eq!(http(&a, "GET", "/nope", None, "").0, 404);
        let body = r#"{"name":"test.mgmt.x","action":"times(1,return(EIO))"}"#;
        let (s, b) = http(&a, "POST", "/api/v1/failpoints", None, body);
        assert!(s == 401 && b.contains("auth_failed"), "不带签名的写被拒：{s} {b}");
        assert_eq!(http(&a, "POST", "/api/v1/failpoints", Some(2), body).0, 403, "client key 不能写");
        let (s, b) = http(&a, "POST", "/api/v1/failpoints", Some(1), body);
        assert!(s == 200 && b.contains("test.mgmt.x"), "{s} {b}");
        assert!(http(&a, "GET", "/api/v1/failpoints", Some(2), "").1.contains("test.mgmt.x"), "读只要任一已认证 key");
        assert_eq!(http(&a, "POST", "/api/v1/failpoints", Some(1), r#"{"name":"x","action":"bogus"}"#).0, 400);
        let (s, b) = http(&a, "DELETE", "/api/v1/failpoints?name=test.mgmt.x", Some(1), "");
        assert!(s == 200 && !b.contains("test.mgmt.x"), "{s} {b}");
        let (s, b) = http(&a, "DELETE", "/api/v1/failpoints?name=test.mgmt.x", Some(1), "");
        assert!(s == 404 && b.contains("enoent"), "清除没设置过的名字（拼错了）不能静默成功：{s} {b}");
        let (s, b) = http(&a, "POST", "/api/v1/failpoints", Some(1), r#"{"name":"a&b","action":"off"}"#);
        assert!(s == 400 && b.contains("einval"), "名字只能由字母、数字和 . _ - 组成：{s} {b}");
    }

    /// 声明了一个巨大的 body 又不发完就断开：进程不能因此退出（tiny_http 丢弃 body 时会按声明的长度分配内存），
    /// 之后照常服务。
    #[test]
    fn huge_declared_body_does_not_kill_the_process() {
        let a = start(true);
        let mut s = TcpStream::connect(&a).unwrap();
        let head = "POST /api/v1/failpoints HTTP/1.1\r\nHost: t\r\nContent-Length: 99999999999999\r\n\r\n";
        s.write_all(format!("{head}{}", "x".repeat(2000)).as_bytes()).unwrap();
        drop(s);
        std::thread::sleep(std::time::Duration::from_millis(200));
        assert_eq!(http(&a, "GET", "/api/v1/health", None, "").0, 200, "还活着");
    }

    #[test]
    fn failpoint_api_is_absent_unless_enabled() {
        let a = start(false);
        assert_eq!(http(&a, "GET", "/api/v1/failpoints", Some(1), "").0, 404);
        assert_eq!(http(&a, "GET", "/api/v1/health", None, "").0, 200);
    }
}
