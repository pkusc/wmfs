//! 金样：M8 管理端点的 JSON 逐字固定（服务端与 ApiClient 都用这些结构，字段名不靠猜）。

use super::*;

fn same<T: Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug>(v: &T, want: &str) {
    assert_eq!(serde_json::to_string(v).unwrap(), want);
    assert_eq!(&serde_json::from_str::<T>(want).unwrap(), v);
}

fn node() -> NodeInfo {
    NodeInfo {
        node_id: 2,
        addrs: vec![("tcp".into(), "127.0.0.1:19200".into())],
        mgmt_addr: "127.0.0.1:19280".into(),
        hostname: "h".into(),
        host_id: "host-2".into(),
        state: "active".into(),
        member: "voter".into(),
        member_wanted: "learner".into(),
        leader: false,
        matched: 1 << 33,
        caught_up: true,
    }
}

const NODE: &str = r#"{"node_id":2,"addrs":[["tcp","127.0.0.1:19200"]],"mgmt_addr":"127.0.0.1:19280","hostname":"h","host_id":"host-2","state":"active","member":"voter","member_wanted":"learner","leader":false,"matched":8589934592,"caught_up":true}"#;

#[test]
fn cluster_json_golden() {
    same(&PluginsReply { plugins: vec!["authz:posix-mode".into()] }, r#"{"plugins":["authz:posix-mode"]}"#);
    same(
        &PluginsReq { add: vec!["authz:deny-uid".into()], remove: vec![] },
        r#"{"add":["authz:deny-uid"],"remove":[]}"#,
    );
    same(&node(), NODE);
    let c = ClusterReply {
        leader: 1,
        term: 7,
        commit: 1 << 34,
        applied: 9,
        voters: vec![1, 2, 3],
        learners: vec![4],
        voters_outgoing: vec![],
        nodes: vec![node()],
        warnings: vec!["w".into()],
    };
    let want = format!(
        r#"{{"leader":1,"term":7,"commit":17179869184,"applied":9,"voters":[1,2,3],"learners":[4],"voters_outgoing":[],"nodes":[{NODE}],"warnings":["w"]}}"#
    );
    same(&c, &want);
    same(&NodeOpReply { node: node(), message: "m".into() }, &format!(r#"{{"node":{NODE},"message":"m"}}"#));
    let h = MetaHashReply { node_id: 3, applied: 1 << 35, state_version: 5, keys: 12, hash: "ab".repeat(16) };
    let want =
        format!(r#"{{"node_id":3,"applied":34359738368,"state_version":5,"keys":12,"hash":"{}"}}"#, "ab".repeat(16));
    same(&h, &want);
}
