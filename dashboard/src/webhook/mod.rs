//! webhook（event_sink，design.md 4.13）：把 changelog 记录按 JSON 数组 POST 给配置的网址（mgr.toml 的 `[webhook] url`；不写就不启用，
//! 也就没有它的游标）。每 100 条或 1 s 一批：宿主给它攒批（不到 100 条时从第一次看到起等 1 s，`host/events.rs`），一次交来的多于 100 条
//! 就拆成几次 POST；回的不是 2xx 或连不上 → 这一批失败，宿主按 sink 指数退避（1 s 起，上限 5 min）之后整批重发（至少一次：接收方按 seq
//! 去重）；全部成功之后宿主才提交游标 `webhook`。http 与 https 都支持（ureq 打开了 rustls，design.md 2.3 的 M1 注记）；默认不走代理
//! （`proxy(None)`：本机设置了 http_proxy），配置了 `[webhook] proxy` 才走它。单次 POST 最多 3 s（宿主只有一个 changelog 循环线程：
//! 卡住的接收方每次重试会让别的 sink 晚约 3 s）。[`agent`]、[`post_json`] 也给告警规则的 webhook 用。指标 `wmfs_webhook_posted_total`。

use std::sync::Arc;
use std::time::Duration;

use wmfs_api::metrics::{self, Counter};
use wmfs_api::types::ChangeEvent;

use crate::host::EventSink;

pub struct Webhook {
    url: String,
    agent: ureq::Agent,
    posted: Arc<Counter>,
}

impl Webhook {
    /// `url` 要以 http:// 或 https:// 开头；`proxy` 为空 = 不走代理。
    pub fn new(url: &str, proxy: &str) -> Result<Webhook, String> {
        if !url.starts_with("http://") && !url.starts_with("https://") {
            return Err(format!("mgr.toml 的 [webhook] url 应以 http:// 或 https:// 开头：{url:?}"));
        }
        let posted = metrics::counter("wmfs_webhook_posted_total", "webhook 成功 POST 出去的记录数", &[]);
        Ok(Webhook { url: url.to_string(), agent: agent(proxy)?, posted })
    }
}

/// 发 webhook 用的 agent（见文件头）：`proxy` 为空 = 不走代理，否则走它（4.13）；非 2xx 也交回状态码；单次最多 3 s。
pub fn agent(proxy: &str) -> Result<ureq::Agent, String> {
    let proxy = match proxy {
        "" => None,
        p => Some(ureq::Proxy::new(p).map_err(|e| format!("mgr.toml 的 [webhook] proxy 写错了：{e}"))?),
    };
    let cfg = ureq::Agent::config_builder().proxy(proxy).http_status_as_error(false);
    Ok(cfg.timeout_global(Some(Duration::from_secs(3))).build().into())
}

/// POST 一个 JSON body：回 2xx 才算成功。
pub fn post_json(agent: &ureq::Agent, url: &str, body: &[u8]) -> Result<(), String> {
    let r = agent.post(url).header("Content-Type", "application/json").send(body);
    match r.map(|resp| resp.status().as_u16()) {
        Ok(s) if (200..300).contains(&s) => Ok(()),
        Ok(s) => Err(format!("{url} 回了 HTTP {s}")),
        Err(e) => Err(format!("POST {url} 失败：{e}")),
    }
}

impl EventSink for Webhook {
    fn manifest_toml(&self) -> &'static str {
        include_str!("manifest.toml")
    }

    fn deliver(&mut self, batch: &[ChangeEvent]) -> Result<(), String> {
        for part in batch.chunks(100) {
            let body = serde_json::to_vec(part).map_err(|e| format!("编码失败：{e}"))?;
            post_json(&self.agent, &self.url, &body)?;
            self.posted.add(part.len() as u64);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 假的接收方：每个请求读完 body 交给测试；`fail` 次之前回 500。
    fn receiver(fail: usize) -> (String, std::sync::mpsc::Receiver<Vec<ChangeEvent>>) {
        let s = tiny_http::Server::http("127.0.0.1:0").unwrap();
        let addr = format!("http://{}/hook", s.server_addr().to_ip().unwrap());
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for (i, mut req) in s.incoming_requests().enumerate() {
                let mut b = Vec::new();
                req.as_reader().read_to_end(&mut b).unwrap();
                let code = if i < fail { 500 } else { 200 };
                let _ = tx.send(serde_json::from_slice(&b).unwrap());
                let _ = req.respond(tiny_http::Response::empty(code));
            }
        });
        (addr, rx)
    }

    #[test]
    fn posts_batches_of_100_and_reports_non_2xx() {
        assert_eq!(crate::host::check_manifest(include_str!("manifest.toml"), "event_sink").unwrap(), "webhook");
        assert!(Webhook::new("ftp://x", "").is_err());
        let (url, rx) = receiver(1);
        let mut w = Webhook::new(&url, "").unwrap();
        let evs: Vec<ChangeEvent> = (1..=250).map(|seq| ChangeEvent { seq, ..Default::default() }).collect();
        let e = w.deliver(&evs).unwrap_err();
        assert!(e.contains("HTTP 500"), "{e}");
        assert_eq!(rx.recv().unwrap().len(), 100);
        w.deliver(&evs).unwrap();
        let sizes: Vec<usize> = (0..3).map(|_| rx.recv().unwrap().len()).collect();
        assert_eq!(sizes, [100, 100, 50], "每批最多 100 条");
        assert_eq!(w.posted.get(), 250);
    }
}
