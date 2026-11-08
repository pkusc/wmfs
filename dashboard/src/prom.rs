//! 抓取与合并各节点的 `/metrics`（Prometheus 文本，design.md 4.14）：prometheus-exporter 的 `/metrics` 与 alert 的指标型规则共用。
//!
//! 节点 = 集群里全部没有移除的节点（`ApiClient::node_addrs`：之后 join 的节点也抓得到），并发地问（每个节点最多 3 s：一个冻住的节点
//! 不拖住整个 `/metrics`）。每个样本加上 `node="<管理地址>"` 标签；mgr 自己的指标（插件的 `wmfs_hook_*`、exporter 算的集群级指标、
//! `wmfs_mgr_*`）当作 `node="mgr"` 一起合并。文本格式要求同一个指标的行连在一起、HELP/TYPE 只写一次，所以按指标名归并（各节点的版本相同，HELP/TYPE 取
//! 第一份）。直方图的 `_bucket`、`_sum`、`_count` 跟着它前面的 `# HELP`/`# TYPE` 归在同一个指标下（与 wmfs-api 的输出格式一致）。

use std::collections::HashMap;

use wmfs_api::metrics;

use crate::api::ApiClient;

/// 一个样本：指标名、标签原文（`{` 与 `}` 之间，没有就是空串）、值。
#[derive(Clone, Debug, PartialEq)]
pub struct Sample {
    pub name: String,
    pub labels: String,
    pub value: f64,
}

impl Sample {
    /// 标签里有 `k="v"` 吗（只认不带转义的值，规则里的标签值都是这样）。
    pub fn has(&self, k: &str, v: &str) -> bool {
        let want = format!("{k}=\"{v}\"");
        self.labels.match_indices(&want).any(|(i, _)| i == 0 || self.labels[..i].ends_with(','))
    }
}

/// 全部节点的 `/metrics` 原文 (节点地址, 文本)，最后是 mgr 自己的 ("mgr", 文本)；连不上的节点跳过。
pub fn scrape(api: &ApiClient) -> Vec<(String, String)> {
    let mut v = each_node(api, |a| api.get_at::<String>(a, "/metrics").ok())
        .into_iter()
        .filter_map(|(a, t)| Some((a.trim_start_matches("http://").to_string(), t?)))
        .collect::<Vec<_>>();
    let mut own = String::new();
    metrics::render_prometheus(&mut own);
    v.push(("mgr".into(), own));
    v
}

/// 并发地对集群里每个节点（`ApiClient::node_addrs`）调一次 `f`，按节点的顺序交回 (地址, 结果)。dashboard 的逐节点代理也用它。
pub fn each_node<T: Send>(api: &ApiClient, f: impl Fn(&str) -> T + Sync) -> Vec<(String, T)> {
    let addrs = api.node_addrs();
    std::thread::scope(|s| {
        let hs: Vec<_> = addrs.iter().map(|a| (a, s.spawn(|| f(a)))).collect();
        hs.into_iter().filter_map(|(a, h)| h.join().ok().map(|r| (a.clone(), r))).collect()
    })
}

/// 一个样本行：(指标名, 标签原文, 值)；不是样本行 → None。
fn split(line: &str) -> Option<(&str, &str, &str)> {
    if line.is_empty() || line.starts_with('#') {
        return None;
    }
    let end = line.find(['{', ' '])?;
    let (labels, rest) = match line[end..].strip_prefix('{') {
        Some(r) => r.rsplit_once('}')?, // 标签值里也可能有 }：取最后一个（值里不会有）
        None => ("", &line[end..]),
    };
    Some((&line[..end], labels, rest.split_whitespace().next()?))
}

/// 解析合并过的文本里的样本（[`merge`] 的输出，或单个节点的原文）。
pub fn parse(text: &str) -> Vec<Sample> {
    let num = |v: &str| v.parse::<f64>().ok();
    text.lines()
        .filter_map(split)
        .filter_map(|(n, l, v)| num(v).map(|value| Sample { name: n.into(), labels: l.into(), value }))
        .collect()
}

/// 合并成一份 Prometheus 文本（见文件头）。
pub fn merge(parts: &[(String, String)]) -> String {
    let mut order: Vec<String> = Vec::new();
    let mut fams: HashMap<String, (Vec<String>, Vec<String>)> = HashMap::new(); // 名字 → (HELP/TYPE 行, 样本行)
    for (node, text) in parts {
        let mut cur = String::new();
        for line in text.lines() {
            let head = line.strip_prefix("# HELP ").or_else(|| line.strip_prefix("# TYPE "));
            if let Some(rest) = head {
                cur = rest.split(' ').next().unwrap_or_default().to_string(); // 指标名
            }
            let f = fams.entry(cur.clone()).or_insert_with(|| {
                order.push(cur.clone());
                (vec![], vec![])
            });
            match split(line) {
                Some((n, "", v)) => f.1.push(format!("{n}{{node=\"{node}\"}} {v}")),
                Some((n, l, v)) => f.1.push(format!("{n}{{node=\"{node}\",{l}}} {v}")),
                // HELP、TYPE 各只留第一份（各节点的版本相同）
                None if head.is_some()
                    && !f.0.iter().any(|x| x.starts_with("# HELP") == line.starts_with("# HELP")) =>
                {
                    f.0.push(line.to_string())
                }
                None => {}
            }
        }
    }
    let mut out = String::new();
    for name in order {
        let (head, samples) = &fams[&name];
        for l in head.iter().chain(samples) {
            out.push_str(l);
            out.push('\n');
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merges_by_family_with_a_node_label() {
        let a = "# HELP wmfs_x x\n# TYPE wmfs_x counter\nwmfs_x{op=\"a\"} 1\nwmfs_x 2\n# HELP wmfs_h h\n# TYPE wmfs_h histogram\n\
                 wmfs_h_bucket{le=\"1\"} 3\nwmfs_h_count 3\n";
        let b = "# HELP wmfs_x x\n# TYPE wmfs_x counter\nwmfs_x{op=\"a\"} 5\n";
        let m = merge(&[("n1".into(), a.into()), ("n2".into(), b.into())]);
        let want = "# HELP wmfs_x x\n# TYPE wmfs_x counter\nwmfs_x{node=\"n1\",op=\"a\"} 1\nwmfs_x{node=\"n1\"} 2\n\
                    wmfs_x{node=\"n2\",op=\"a\"} 5\n# HELP wmfs_h h\n# TYPE wmfs_h histogram\nwmfs_h_bucket{node=\"n1\",le=\"1\"} 3\n\
                    wmfs_h_count{node=\"n1\"} 3\n";
        assert_eq!(m, want, "同名指标的行连在一起、HELP/TYPE 只写一次");
        let s = parse(&m);
        assert_eq!(s.len(), 5);
        assert!(s[0].has("op", "a") && s[0].has("node", "n1") && !s[0].has("node", "n2"));
        assert_eq!((s[2].value, s[4].name.as_str()), (5.0, "wmfs_h_count"));
        let odd = parse("wmfs_y{dir=\"a}b\",x=\"1\"} +Inf\n");
        assert_eq!((odd[0].labels.as_str(), odd[0].value), ("dir=\"a}b\",x=\"1\"", f64::INFINITY));
    }

    /// 三个"节点"，其中一个接受连接却永远不回（被冻住）：scrape 并发地问、每个节点最多 3 s，不会被它拖到 10 s 以上。
    #[test]
    fn a_frozen_node_does_not_stall_the_scrape() {
        let mk = || {
            let s = tiny_http::Server::http("127.0.0.1:0").unwrap();
            let a = s.server_addr().to_ip().unwrap().to_string();
            std::thread::spawn(move || {
                for r in s.incoming_requests() {
                    let text = "# HELP wmfs_x x\n# TYPE wmfs_x gauge\nwmfs_x 1\n";
                    let _ = r.respond(tiny_http::Response::from_string(text));
                }
            });
            a
        };
        let frozen = std::net::TcpListener::bind("127.0.0.1:0").unwrap(); // listen 了但从不 accept、不回复
        let fa = frozen.local_addr().unwrap().to_string();
        let api = ApiClient::new(vec![mk(), fa, mk()], wmfs_api::types::KeyFile { key_id: 1, secret: [0; 32] });
        let t0 = std::time::Instant::now();
        let parts = scrape(&api);
        let took = t0.elapsed();
        assert!(took < std::time::Duration::from_secs(6), "并发、每个节点最多 3 s：用了 {took:?}");
        assert_eq!(parts.len(), 3, "两个节点 + mgr 自己");
        drop(frozen);
    }
}
