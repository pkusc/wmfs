//! prometheus-exporter（mgmt_ext，design.md 4.14）：mgr 的 `GET /metrics` = 抓取所有节点的 `/metrics`、加 `node="…"` 标签合并输出
//! （`prom.rs`），Prometheus 只需配一个抓取目标；另附集群级指标（mgr 自己算、经管理 API 拿数据，core 里没有常驻的全树扫描）：
//! - `wmfs_quota_used_bytes{dir}`、`wmfs_quota_limit_bytes{dir,kind}`（kind = hard / soft；按类的配额另带 `class` 标签）：每 30 s 一次——
//!   `GET /api/v1/attr?path=/&recursive=1` 找出设了 quota 的目录（最多 1000 个），逐个 `GET /api/v1/usage?path=`；删掉了的配额，gauge 归 0；
//! - `wmfs_declared_mismatch_files{attr}`（声明 vs 实际的不一致文件数）：每 5 min 一次 `GET /api/v1/status?path=/`（沿用 status 的上限：
//!   最多看 100 万个 inode，受后台限速）；这次没算成（leader 正在换、超时）就保留上次的值，30 s 后再试。
//!
//! 这些管理 API 调用在 exporter 自己的线程上做（请求最多等 300 s：大树上 status 要扫很久），每 5 s 的 tick 只"踢"它一下（它还忙着就跳过）——
//! 告警的求值与 dashboard 不会被一次长扫描卡住；fail point `plugin.prometheus-exporter.mgmt_ext` 照样挡在 tick 外层。
//!
//! 实现注记（M14）：7.3 的这几个指标（M11、M13 注记推迟到 M14 的）选"mgr 的 exporter 经管理 API 算"——不重复（只有 mgr 一份，不会每个
//! 节点各报一份）、换 leader 不留旧值，4.14 也写明 exporter 附带"配额目录的用量/上限比、声明 vs 实际计数"。target 的用量与容量是每个
//! 节点自己的，在节点的 `/metrics` 里（`wmfs_target_used_bytes{target}`，`data/node_hb.rs`）。

use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::time::{Duration, Instant};

use wmfs_api::metrics;
use wmfs_api::types::{AttrGetReply, StatusReply, UsageReply};
use wmfs_api::url;

use crate::api::ApiClient;
use crate::host::{HttpOut, MgmtExt, Router};
use crate::prom;

/// 配额 gauge 多久算一次；声明 vs 实际多久算一次、没算成多久再试；exporter 线程的请求最多等多久（秒）。
const QUOTA_EVERY: Duration = Duration::from_secs(30);
const MISMATCH_EVERY: Duration = Duration::from_secs(300);
const MISMATCH_RETRY: Duration = Duration::from_secs(30);
const WORKER_TIMEOUT_S: u64 = 300;

#[derive(Default)]
pub struct Exporter {
    kick: Option<SyncSender<()>>, // 踢一下 exporter 线程（见文件头）
}

impl MgmtExt for Exporter {
    fn manifest_toml(&self) -> &'static str {
        include_str!("manifest.toml")
    }

    fn start(&mut self, api: &ApiClient, router: &mut Router) -> Result<(), String> {
        let a = api.clone();
        let metrics = move |_: &crate::host::HttpIn| HttpOut {
            status: 200,
            content_type: "text/plain; version=0.0.4",
            body: prom::merge(&prom::scrape(&a)).into_bytes(),
        };
        router.add("/metrics", Box::new(metrics));
        let (tx, rx) = sync_channel(1);
        let slow_api = api.clone().with_timeout(WORKER_TIMEOUT_S);
        let t = std::thread::Builder::new().name("mgr-exporter".into()).spawn(move || worker(&slow_api, &rx));
        t.map_err(|e| format!("起 exporter 线程失败：{e}"))?;
        self.kick = Some(tx);
        Ok(())
    }

    fn tick(&mut self, _api: &ApiClient) {
        if let Some(k) = &self.kick {
            let _ = k.try_send(()); // 线程还忙着上一次：这一下跳过
        }
    }
}

/// exporter 线程：每被踢一下，看看配额、声明 vs 实际到点了没有（见文件头）。
fn worker(api: &ApiClient, kicks: &Receiver<()>) {
    let mut seen = Vec::new();
    let (mut next_quota, mut next_mismatch) = (Instant::now(), Instant::now());
    while kicks.recv().is_ok() {
        if Instant::now() >= next_quota {
            seen = quotas(api, &seen);
            next_quota = Instant::now() + QUOTA_EVERY;
        }
        if Instant::now() >= next_mismatch {
            next_mismatch = Instant::now() + if mismatch(api) { MISMATCH_EVERY } else { MISMATCH_RETRY };
        }
    }
}

/// 一个配额：(目录, 类（空 = 不分类）, 用量, 硬限, 软限)。
type Quota = (String, String, u64, u64, u64);

/// 算一轮配额 gauge；返回这一轮报过的 (目录, 类)。问不到目录列表就什么也不改。
fn quotas(api: &ApiClient, seen: &[(String, String)]) -> Vec<(String, String)> {
    let Ok(r) = api.get::<AttrGetReply>("/api/v1/attr?path=/&recursive=1") else { return seen.to_vec() };
    let dirs = r.subtree.iter().filter(|d| d.attrs.keys().any(|k| k == "quota" || k.starts_with("quota.")));
    let mut cur: Vec<Quota> = Vec::new();
    for d in dirs {
        let Ok(u) = api.get::<UsageReply>(&format!("/api/v1/usage?path={}", url::encode(&d.path))) else {
            continue;
        };
        for q in u.quotas.iter().filter(|q| q.dir == d.path) {
            cur.push((d.path.clone(), q.class.clone().unwrap_or_default(), q.used_bytes, q.hard, q.soft));
        }
    }
    publish(seen, &cur)
}

/// 设这一轮的配额 gauge；上一轮报过（`seen`）、这一轮没有的归 0。返回这一轮报过的 (目录, 类)。
fn publish(seen: &[(String, String)], cur: &[Quota]) -> Vec<(String, String)> {
    let now: Vec<(String, String)> = cur.iter().map(|(dir, class, ..)| (dir.clone(), class.clone())).collect();
    for (dir, class, used, hard, soft) in cur {
        gauges(dir, class, *used as f64, *hard as f64, *soft as f64);
    }
    for (dir, class) in seen.iter().filter(|k| !now.contains(k)) {
        gauges(dir, class, 0.0, 0.0, 0.0);
    }
    now
}

/// 一个配额的三个 gauge（按类的配额另带 `class` 标签）。
fn gauges(dir: &str, class: &str, used: f64, hard: f64, soft: f64) {
    let (u, l) = ("配额管着的用量（字节）", "配额上限（字节；kind = hard / soft，0 = 不限）");
    let c = if class.is_empty() { None } else { Some(("class", class)) };
    let with = |kind: Option<&'static str>| {
        let mut v = vec![("dir", dir)];
        v.extend(c);
        v.extend(kind.map(|k| ("kind", k)));
        v
    };
    metrics::gauge("wmfs_quota_used_bytes", u, &with(None)).set(used);
    metrics::gauge("wmfs_quota_limit_bytes", l, &with(Some("hard"))).set(hard);
    metrics::gauge("wmfs_quota_limit_bytes", l, &with(Some("soft"))).set(soft);
}

/// 算一次 `wmfs_declared_mismatch_files{attr}`；没算成返回 false。
fn mismatch(api: &ApiClient) -> bool {
    let Ok(s) = api.get::<StatusReply>("/api/v1/status?path=/") else { return false };
    let help = "声明与实际不同的已有文件数（mgr 每 5 min 按 GET /api/v1/status?path=/ 算）";
    for a in &s.attrs {
        if let Some(n) = a.actual.mismatched_files {
            metrics::gauge("wmfs_declared_mismatch_files", help, &[("attr", &a.key)]).set(n as f64);
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text() -> String {
        let mut t = String::new();
        metrics::render_prometheus(&mut t);
        t
    }

    #[test]
    fn manifest_and_quota_gauges() {
        assert_eq!(
            crate::host::check_manifest(Exporter::default().manifest_toml(), "mgmt_ext").unwrap(),
            "prometheus-exporter"
        );
        let q = |dir: &str, class: &str, used, hard, soft| (dir.to_string(), class.to_string(), used, hard, soft);
        let seen = publish(&[], &[q("/t-exp", "", 5, 10, 8), q("/t-exp", "gold", 1, 2, 0), q("/t-gone", "", 3, 4, 0)]);
        let t = text();
        assert!(t.contains("wmfs_quota_used_bytes{dir=\"/t-exp\"} 5\n"), "{t}");
        assert!(t.contains("wmfs_quota_limit_bytes{dir=\"/t-exp\",kind=\"hard\"} 10\n"));
        assert!(t.contains("wmfs_quota_limit_bytes{dir=\"/t-exp\",class=\"gold\",kind=\"soft\"} 0\n"));
        assert!(t.contains("wmfs_quota_used_bytes{dir=\"/t-gone\"} 3\n"));
        let seen = publish(&seen, &[q("/t-exp", "", 6, 10, 8), q("/t-exp", "gold", 1, 2, 0)]);
        let t = text();
        assert!(t.contains("wmfs_quota_used_bytes{dir=\"/t-exp\"} 6\n"));
        for gone in [
            "wmfs_quota_used_bytes{dir=\"/t-gone\"} 0\n",
            "wmfs_quota_limit_bytes{dir=\"/t-gone\",kind=\"hard\"} 0\n",
            "wmfs_quota_limit_bytes{dir=\"/t-gone\",kind=\"soft\"} 0\n",
        ] {
            assert!(t.contains(gone), "删掉了的配额归 0：{gone}");
        }
        assert_eq!(seen.len(), 2);
    }

    /// 管理 API 连不上：配额一项也不改（返回上一轮的），声明 vs 实际报"没算成"（30 s 后再试）。
    #[test]
    fn an_unreachable_cluster_changes_nothing() {
        let api = ApiClient::new(vec!["127.0.0.1:1".into()], wmfs_api::types::KeyFile { key_id: 1, secret: [0; 32] });
        let seen = vec![("/t-keep".to_string(), String::new())];
        assert_eq!(quotas(&api, &seen), seen);
        assert!(!mismatch(&api));
    }
}
