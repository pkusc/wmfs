//! mgr 的 HTTP 服务（tiny_http，4 个处理线程）与 mgmt_ext 插件的 tick 线程（每 5 s 一次，design.md 3.7.3、4.14）。
//!
//! 除了 `/metrics`（给 Prometheus 抓，与节点的 `/metrics` 一样免认证），每个请求都要带 dashboard token（`?token=`，启动日志与
//! `<dir>/token` 里的那一串）：默认只监听 127.0.0.1，token 再挡一层同机的别的用户。mgr 的页面都是 GET：请求体一律不读；声明了超过
//! 1 MiB 请求体的请求不回、不交还 tiny_http（与 core 的管理 API 相同：tiny_http 丢弃没读完的 body 时按声明的长度一次分配内存）。

use std::sync::Arc;
use std::time::Duration;

use wmfs_api::url;

use super::{Hooked, HttpIn, HttpOut, MgmtExt, Router};
use crate::api::ApiClient;

/// 开始监听并起处理线程；返回实际监听的地址。
pub(crate) fn serve(listen: &str, token: String, router: Router) -> Result<String, String> {
    let server = tiny_http::Server::http(listen).map_err(|e| format!("mgr 无法监听 {listen}：{e}"))?;
    let addr = server.server_addr().to_ip().map_or_else(|| listen.to_string(), |a| a.to_string());
    let (server, router) = (Arc::new(server), Arc::new(router));
    for i in 0..4 {
        let (s, r, t) = (server.clone(), router.clone(), token.clone());
        let spawn = std::thread::Builder::new().name(format!("mgr-http-{i}")).spawn(move || {
            while let Ok(req) = s.recv() {
                answer(&r, &t, req);
            }
        });
        spawn.map_err(|e| format!("起 mgr 的 HTTP 线程失败：{e}"))?;
    }
    Ok(addr)
}

fn answer(router: &Router, token: &str, req: tiny_http::Request) {
    if req.body_length().is_some_and(|n| n > 1 << 20) {
        std::mem::forget(req); // 见文件头
        return;
    }
    let u = req.url().to_string();
    let (path, query) = u.split_once('?').unwrap_or((&u, ""));
    let input = HttpIn { method: req.method().as_str().into(), path: path.into(), query: query.into(), body: vec![] };
    let out = if path != "/metrics" && url::param(query, "token").as_deref() != Some(token) {
        HttpOut::text(401, "需要 dashboard token：网址后面加 ?token=…（mgr 的启动日志与 <dir>/token 里有）".into())
    } else {
        router.route(&input)
    };
    let h = tiny_http::Header::from_bytes("Content-Type", out.content_type)
        .unwrap_or_else(|_| panic!("不变量破坏：固定的 HTTP 头"));
    let _ = req.respond(tiny_http::Response::from_data(out.body).with_status_code(out.status).with_header(h));
}

/// tick 线程：每 5 s 依次调每个 mgmt_ext 插件的 tick（经 [`Hooked`]：fail point、计时、计数）。
pub(crate) fn ticks(api: ApiClient, mut exts: Vec<(Box<dyn MgmtExt>, Hooked)>) -> Result<(), String> {
    let t = std::thread::Builder::new().name("mgr-tick".into()).spawn(move || {
        loop {
            for (e, h) in &mut exts {
                let r = h.call(|| {
                    e.tick(&api);
                    Ok(())
                });
                if let Err(why) = r {
                    crate::log(format_args!("{} 的 tick 失败：{why}", h.name));
                }
            }
            std::thread::sleep(Duration::from_secs(5));
        }
    });
    t.map(|_| ()).map_err(|e| format!("起 mgr 的 tick 线程失败：{e}"))
}
