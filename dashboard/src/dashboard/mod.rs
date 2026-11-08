//! dashboard（mgmt_ext，design.md 4.14）：单页 HTML/JS（`index.html`，无外部库，`include_str!` 编进二进制）+ 两个只读代理。
//!
//! - `GET /`：页面。它每 5 s 经下面两个代理取数据，画六个面板：集群、容量与配额、声明 vs 实际（输入路径，每分钟刷新一次：要扫子树）、
//!   QoS 与存储类、后台任务、事件与告警（mgr 的 `GET /alerts`：规则的状态与 mgr 最近收到的 changelog 记录）；
//! - `GET /dash/api?path=/api/v1/…`：按 leader 跟随转给管理 API（mgr 的 key 签名），原样回 JSON；
//! - `GET /dash/each?path=/api/v1/…`：节点本地的端点（`/api/v1/node`、`/api/v1/qos`）问集群里的每个节点（并发，每个最多 3 s；节点列表
//!   来自 `GET /api/v1/cluster`，`prom::each_node`），回 `[{addr, reply} | {addr, error}]`。
//!
//! 只转发 GET、只转发 `/api/v1/` 开头的路径（mgr 持有的可能是 admin key：页面只能读）。QoS 面板的等待 p99 由页面按 mgr 的 `/metrics`
//! 里 `wmfs_qos_wait_seconds` 的直方图算。浏览器访问要带 token（`?token=`，见 `host/serve.rs`）。

use serde_json::{Value, json};
use wmfs_api::metrics::{self, Counter};
use wmfs_api::url;

use crate::api::{ApiClient, ApiError};
use crate::host::{HttpIn, HttpOut, MgmtExt, Router};
use crate::prom;

pub struct Dashboard;

impl MgmtExt for Dashboard {
    fn manifest_toml(&self) -> &'static str {
        include_str!("manifest.toml")
    }

    fn start(&mut self, api: &ApiClient, router: &mut Router) -> Result<(), String> {
        let page = |_: &HttpIn| HttpOut {
            status: 200,
            content_type: "text/html; charset=utf-8",
            body: include_str!("index.html").as_bytes().to_vec(),
        };
        router.add("/", Box::new(page));
        let hits = metrics::counter("wmfs_dashboard_proxy_total", "dashboard 代理到管理 API 的请求数", &[]);
        let (a, h) = (api.clone(), hits.clone());
        router.add("/dash/api", Box::new(move |r| proxy(&a, &h, r, false)));
        let a = api.clone();
        router.add("/dash/each", Box::new(move |r| proxy(&a, &hits, r, true)));
        Ok(())
    }

    fn tick(&mut self, _api: &ApiClient) {}
}

fn proxy(api: &ApiClient, hits: &Counter, r: &HttpIn, each: bool) -> HttpOut {
    let Some(path) = url::param(&r.query, "path").filter(|p| p.starts_with("/api/v1/") && r.method == "GET") else {
        return HttpOut::text(400, "只转发 GET，path 要以 /api/v1/ 开头".into());
    };
    hits.inc();
    if each {
        let replies = prom::each_node(api, |a| api.get_at::<Value>(a, &path));
        let one = |(a, r): (String, Result<Value, ApiError>)| match r {
            Ok(v) => json!({"addr": a, "reply": v}),
            Err(e) => json!({"addr": a, "error": e.to_string()}),
        };
        return HttpOut::json(200, &replies.into_iter().map(one).collect::<Vec<_>>());
    }
    match api.get::<Value>(&path) {
        Ok(v) => HttpOut::json(200, &v),
        Err(e) => {
            HttpOut::json(if e.status == 0 { 502 } else { e.status }, &json!({"error": e.code, "message": e.message}))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::host::check_manifest;

    #[test]
    fn manifest_and_proxy_rules() {
        assert_eq!(check_manifest(Dashboard.manifest_toml(), "mgmt_ext").unwrap(), "dashboard");
        let api = ApiClient::new(vec![], wmfs_api::types::KeyFile { key_id: 1, secret: [0; 32] });
        let c = metrics::counter("wmfs_dashboard_proxy_total", "", &[]);
        let r = |m: &str, q: &str| HttpIn { method: m.into(), path: "/dash/api".into(), query: q.into(), body: vec![] };
        assert_eq!(proxy(&api, &c, &r("GET", "path=%2Fmetrics"), false).status, 400, "只转发 /api/v1/");
        assert_eq!(proxy(&api, &c, &r("POST", "path=%2Fapi%2Fv1%2Fcluster"), false).status, 400, "只转发 GET");
        assert_eq!(proxy(&api, &c, &r("GET", "path=%2Fapi%2Fv1%2Fcluster"), false).status, 502, "连不上");
        assert!(include_str!("index.html").contains("声明 vs 实际"));
    }
}
