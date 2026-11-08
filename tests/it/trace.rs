//! trace 与节点本地的端点（design.md 3.11、7.1、7.3；M14）：同一个进程里的 3 节点集群、3 副本的文件——
//! - 一次 WriteChunk 在链头、中间、链尾各留一个 span：同一个 trace_id，第 0、1、2 跳在三个不同的节点上，耗时一跳比一跳短（前一跳要等
//!   后一跳确认）；各节点的 `GET /api/v1/trace/<id>` 回本节点的那一个，`GET /api/v1/trace/recent` 列出链头上的这个请求；
//! - `GET /api/v1/node` 多了 target、boot_id、内存池、线程数（只增）；节点的 `/metrics` 有 `wmfs_target_used_bytes`、
//!   `wmfs_target_capacity_bytes`、`wmfs_clock_skew_seconds`、`wmfs_threads`、`wmfs_targets_offline`；
//! - 慢请求阈值（node.toml 的 `[obs]`，进程级的 `trace::set_slow_ms`）调成 0 之后，每个请求都计入
//!   `wmfs_slow_requests_total{family="DataCall"}`；慢日志那一行列出这个请求走过的各阶段耗时（ADR-0045 第 9 条）：元数据写有 decode、
//!   排队、authz、quota、执行、raft 提交，链头上的数据写有 decode、排队、authz、磁盘、转发，没走过的不写。日志是进程级的：写进最后起的
//!   那个节点（node3）的 server.log。
//!
//! 用到进程级的设置：全部放在一个测试函数里。

use wmfs_core::api::metrics;
use wmfs_core::api::types::{NodeInfoReply, TraceReply};
use wmfs_core::meta::schema::keys::ROOT_INO;
use wmfs_core::obs::trace;
use wmfs_mgr::api::ApiClient;
use wmfs_testkit::harness::bg::api_at;
use wmfs_testkit::harness::cluster::Cluster;
use wmfs_testkit::harness::node::{put, wait_for, who};

fn admin(c: &Cluster, i: usize) -> ApiClient {
    api_at(&c.node_dir(1), &c.run(i).mgmt_addr, "admin")
}

/// 节点 1..=3 上属于 `id` 的 span（经管理 API）。
fn spans(c: &Cluster, id: &str) -> Vec<wmfs_core::api::types::TraceSpan> {
    let mut v: Vec<_> =
        (1..=3).flat_map(|i| admin(c, i).get::<TraceReply>(&format!("/api/v1/trace/{id}")).unwrap().spans).collect();
    v.sort_by_key(|s| s.span_id);
    v
}

#[test]
fn a_chain_write_leaves_one_span_per_hop() {
    let c = Cluster::start("trace", 3);
    let (cl, me) = (c.client("admin"), who(1000, 1000));
    // 找一次走满三跳的 WriteChunk：刚起的集群里链可能还在补成员，多写几个文件
    let mut found = None;
    for k in 0..30 {
        put(&cl, &me, ROOT_INO, &format!("f{k}"), &vec![7u8; 300_000]).unwrap();
        let heads = (1..=3).flat_map(|i| trace::recent(c.run(i).node_id, 50));
        for s in heads.filter(|s| s.op == "WriteChunk") {
            let id = format!("{:016x}", s.trace_id);
            let v = spans(&c, &id);
            if v.iter().map(|s| s.span_id).collect::<Vec<_>>() == [0, 1, 2] && v.iter().all(|s| s.code == "OK") {
                found = Some(id); // 没有重试、没有碰上新链的 STALE_CHAIN（同一个操作的重试用同一个 trace_id）
            }
        }
        if found.is_some() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(500));
    }
    let id = found.expect("30 次写里至少有一次走满了链头 → 中间 → 链尾");
    let v = spans(&c, &id);
    assert_eq!(v.iter().map(|s| s.span_id).collect::<Vec<_>>(), [0, 1, 2], "{v:?}");
    let ops: Vec<&str> = v.iter().map(|s| s.op.as_str()).collect();
    assert_eq!(ops, ["WriteChunk", "ForwardWrite", "ForwardWrite"]);
    assert_eq!((v[0].component.as_str(), v[1].component.as_str()), ("DataCall", "PeerCall"));
    let mut nodes: Vec<u32> = v.iter().map(|s| s.node).collect();
    nodes.dedup();
    assert_eq!(nodes.len(), 3, "三跳在三个不同的节点上：{v:?}");
    assert!(v[0].dur_us >= v[1].dur_us && v[1].dur_us >= v[2].dur_us, "前一跳要等后一跳确认：{v:?}");
    assert!(v.iter().all(|s| s.code == "OK" && s.trace_id == id));
    let head = (1..=3).find(|i| c.run(*i).node_id == v[0].node).unwrap();
    let r: TraceReply = admin(&c, head).get("/api/v1/trace/recent?n=1000").unwrap();
    assert!(
        r.spans.iter().any(|s| s.trace_id == id) && r.spans.iter().all(|s| s.span_id == 0),
        "recent 只列客户端请求"
    );
    let middle = (1..=3).find(|i| c.run(*i).node_id == v[1].node).unwrap();
    let r: TraceReply = admin(&c, middle).get("/api/v1/trace/recent?n=1000").unwrap();
    assert!(r.spans.iter().all(|s| s.trace_id != id), "中间节点上这个 trace 只有转发来的第 1 跳：不在 recent 里");
    assert!(admin(&c, 1).get::<TraceReply>("/api/v1/trace/zz").is_err(), "trace id 不是 hex → einval");

    let n: NodeInfoReply = admin(&c, 2).get("/api/v1/node").unwrap();
    assert_eq!(n.targets.len(), 1);
    assert!(n.targets[0].capacity > 0 && n.targets[0].path.ends_with("targets/1"));
    assert!(n.boot_id.len() == 16 && n.boot_id != "0000000000000000" && n.threads > 10, "{n:?}");
    assert_eq!(n.pool.iter().map(|p| p.class.as_str()).collect::<Vec<_>>(), ["S", "M", "L"]);
    let text: String = admin(&c, 1).get_at(&admin(&c, 1).addrs()[0], "/metrics").unwrap();
    for m in [
        "wmfs_target_used_bytes{target=",
        "wmfs_target_capacity_bytes{target=",
        "wmfs_clock_skew_seconds ",
        "wmfs_threads ",
    ] {
        assert!(text.contains(m), "/metrics 里有 {m}");
    }
    assert!(wait_for(10, || admin(&c, 1)
        .get_at::<String>(&admin(&c, 1).addrs()[0], "/metrics")
        .unwrap()
        .contains("wmfs_targets_offline 0")));

    let slow = || metrics::counter("wmfs_slow_requests_total", "", &[("family", "DataCall")]).get();
    let before = slow();
    trace::set_slow_ms(0, 0); // = node.toml 里 [obs] slow_meta_ms = 0、slow_data_ms = 0
    put(&cl, &me, ROOT_INO, "slow", &vec![1u8; 300_000]).unwrap(); // Create、WriteChunk（三跳）、CommitWrite
    trace::set_slow_ms(50, 100);
    assert!(slow() > before, "阈值调成 0：这次写计入慢请求");
    let log = std::fs::read_to_string(c.node_dir(3).join("logs/server.log")).unwrap();
    let line = |what: &str| {
        let mut v = log.lines().filter(|l| l.contains("慢请求：") && l.contains(what));
        v.next_back().unwrap_or_else(|| panic!("慢日志里有 {what}")).to_string()
    };
    let has = |l: &str, s: &str| l.contains(&format!("{s} ")) && l.contains(" ms");
    let meta = line("MetaCall Create");
    for s in ["decode", "排队", "authz", "quota", "执行", "raft 提交"] {
        assert!(has(&meta, s), "元数据写的慢日志有 {s}：{meta}");
    }
    let data = line("DataCall WriteChunk");
    for s in ["decode", "排队", "authz", "磁盘", "转发"] {
        assert!(has(&data, s), "数据写的慢日志有 {s}：{data}");
    }
    assert!(!data.contains("raft 提交") && !data.contains("quota"), "没走过的阶段不写：{data}");
}
