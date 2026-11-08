//! 金样：M8 管理端点的 JSON 逐字固定（服务端与 ApiClient 都用这些结构，字段名不靠猜）。

use super::*;
use crate::types::same;

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
        targets: vec![ClusterTarget { id: 4, node: 2, state: "offline".into() }],
        chains: 3,
        sessions: 1 << 33,
    };
    let want = format!(
        r#"{{"leader":1,"term":7,"commit":17179869184,"applied":9,"voters":[1,2,3],"learners":[4],"voters_outgoing":[],"nodes":[{NODE}],"warnings":["w"],"targets":[{{"id":4,"node":2,"state":"offline"}}],"chains":3,"sessions":8589934592}}"#
    );
    same(&c, &want);
    let old = r#"{"leader":1,"term":7,"commit":1,"applied":1,"voters":[1],"learners":[],"voters_outgoing":[],"nodes":[],"warnings":[]}"#;
    let r: ClusterReply = serde_json::from_str(old).unwrap();
    assert_eq!((r.targets.len(), r.chains, r.sessions), (0, 0, 0), "M14 之前的回复也解得出来（只增）");
    same(&NodeOpReply { node: node(), message: "m".into() }, &format!(r#"{{"node":{NODE},"message":"m"}}"#));
    let h = MetaHashReply { node_id: 3, applied: 1 << 35, state_version: 5, keys: 12, hash: "ab".repeat(16) };
    let want =
        format!(r#"{{"node_id":3,"applied":34359738368,"state_version":5,"keys":12,"hash":"{}"}}"#, "ab".repeat(16));
    same(&h, &want);
}

#[test]
fn chains_json_golden() {
    let m = |target, node, state: &str| ChainMember { target, node, state: state.into() };
    let c = ChainInfo {
        id: 3,
        ver: 1 << 33,
        incarnation: 2,
        class: "default".into(),
        replicas: 3,
        want: 3,
        members: vec![m(1, 1, "serving"), m(3, 3, "syncing")],
        last_serving: None,
        removed: vec![2],
    };
    let one = r#"{"id":3,"ver":8589934592,"incarnation":2,"class":"default","replicas":3,"want":3,"members":[{"target":1,"node":1,"state":"serving"},{"target":3,"node":3,"state":"syncing"}],"last_serving":null,"removed":[2]}"#;
    same(&c, one);
    same(
        &ChainsReply { chains: vec![c], warnings: vec!["w".into()] },
        &format!(r#"{{"chains":[{one}],"warnings":["w"]}}"#),
    );
    same(&ForceServingReq { i_accept_data_loss: true }, r#"{"i_accept_data_loss":true}"#);
}

#[test]
fn features_json_golden() {
    let f = FeaturesReply {
        enabled: vec!["chunk.snapshots".into()],
        nodes: vec![
            NodeFeatures { node_id: 2, features: vec!["chunk.snapshots".into(), "meta.ec-layout".into()] },
            NodeFeatures { node_id: 3, features: vec![] },
        ],
    };
    let want = r#"{"enabled":["chunk.snapshots"],"nodes":[{"node_id":2,"features":["chunk.snapshots","meta.ec-layout"]},{"node_id":3,"features":[]}]}"#;
    same(&f, want);
}
