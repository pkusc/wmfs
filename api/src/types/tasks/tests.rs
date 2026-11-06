//! 金样：M11 管理端点（后台任务、本节点进度、数据密钥）的 JSON 逐字固定。

use super::*;
use crate::types::same;

#[test]
fn tasks_and_datakeys_json_golden() {
    let t = TaskInfo {
        kind: "reconciler".into(),
        runs_on: RUNS_ON_LEADER.into(),
        node: 0,
        state: STATE_RUNNING.into(),
        pass: 2,
        done: 9,
        backlog: 11,
        progress: 0.5,
        rate_bps: 2097152,
        bw: 2097152,
        items: 0,
        updated_ms: 1,
        last_error: String::new(),
        runs: 0,
    };
    same(
        &TasksReply { tasks: vec![t.clone()] },
        r#"{"tasks":[{"kind":"reconciler","runs_on":"leader","node":0,"state":"running","pass":2,"done":9,"backlog":11,"progress":0.5,"rate_bps":2097152,"bw":2097152,"items":0,"updated_ms":1,"last_error":"","runs":0}]}"#,
    );
    same(&TaskSetReq { bw: Some("2M".into()), items: None }, r#"{"bw":"2M","items":null}"#);
    let short: TaskSetReq = serde_json::from_str(r#"{"items":5}"#).unwrap();
    assert_eq!(short, TaskSetReq { bw: None, items: Some(5) });
    assert!(t.on_leader());
    let n = NodeInfoReply {
        node_id: 2,
        controlled: true,
        tasks: vec![TaskInfo { runs_on: RUNS_ON_EVERY_NODE.into(), node: 2, ..t }],
    };
    assert!(serde_json::to_string(&n).unwrap().starts_with(
        r#"{"node_id":2,"controlled":true,"tasks":[{"kind":"reconciler","runs_on":"every-node","node":2,"#
    ));
    // 只做数据的节点（M11 审查，ADR-0042 第 12 条）；线上的取值（ADR-0042 第 4 条）
    same(
        &NodeInfoReply { node_id: 4, controlled: false, tasks: vec![] },
        r#"{"node_id":4,"controlled":false,"tasks":[]}"#,
    );
    assert_eq!((RUNS_ON_LEADER, RUNS_ON_EVERY_NODE), ("leader", "every-node"));
    assert_eq!((STATE_RUNNING, STATE_PAUSED, STATE_DONE), ("running", "paused", "done"));
    same(&DataKeyCreateReq { name: "k1".into(), tenant: 0 }, r#"{"name":"k1","tenant":0}"#);
    let short: DataKeyCreateReq = serde_json::from_str(r#"{"name":"k1"}"#).unwrap();
    assert_eq!(short.tenant, 0);
    same(&DataKeyCreateReply { id: 1, name: "k1".into() }, r#"{"id":1,"name":"k1"}"#);
}
