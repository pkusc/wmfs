//! 金样：节点本地端点（M14 的 trace、node 只增的 target、boot_id、内存池、线程数）的 JSON 逐字固定。

use super::*;
use crate::types::same;

#[test]
fn trace_and_node_json_golden() {
    let s = TraceSpan {
        trace_id: "00000000000000ff".into(),
        span_id: 1,
        node: 2,
        component: "PeerCall".into(),
        op: "ForwardWrite".into(),
        start_ms: 1700000000000,
        dur_us: 1500,
        code: "OK".into(),
    };
    same(
        &TraceReply { node_id: 2, spans: vec![s] },
        r#"{"node_id":2,"spans":[{"trace_id":"00000000000000ff","span_id":1,"node":2,"component":"PeerCall","op":"ForwardWrite","start_ms":1700000000000,"dur_us":1500,"code":"OK"}]}"#,
    );
    let t = NodeTarget {
        id: 3,
        path: "/d/n1/targets/1".into(),
        used: 4096,
        capacity: 1 << 30,
        read_only: false,
        phys_devs: vec!["nvme0n1".into()],
    };
    let p = PoolClass { class: "S".into(), slot_size: 65536, slots: 64, free_normal: 40, free_repl: 16 };
    same(
        &NodeInfoReply {
            node_id: 1,
            controlled: true,
            boot_id: "00000000000000aa".into(),
            targets: vec![t],
            pool: vec![p],
            threads: 77,
            ..Default::default()
        },
        r#"{"node_id":1,"controlled":true,"tasks":[],"boot_id":"00000000000000aa","targets":[{"id":3,"path":"/d/n1/targets/1","used":4096,"capacity":1073741824,"read_only":false,"phys_devs":["nvme0n1"]}],"pool":[{"class":"S","slot_size":65536,"slots":64,"free_normal":40,"free_repl":16}],"threads":77}"#,
    );
}
