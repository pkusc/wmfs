//! 金样：M13 存储类端点的 JSON 逐字固定。

use super::*;
use crate::types::same;

#[test]
fn classes_json_golden() {
    let q = ClassQos { iops_min: 0, iops_max: 300, bw_min: 0, bw_max: 1048576, burst_ms: 1000, weight: 1 };
    let k = ClassInfo {
        id: 2,
        name: "gold".into(),
        label: "*".into(),
        dedicated: false,
        replica: 2,
        priority: "normal".into(),
        locality: "any".into(),
        latency_target_us: 500,
        tier_to: Some("silver".into()),
        tier_when: Some("idle>30d".into()),
        qos: q.clone(),
        nodes: 3,
        per_node: ClassQos { iops_max: 100, bw_max: 349525, ..q },
        isolation: "shared".into(),
        promise: "只承诺份额，不承诺尾延迟".into(),
        downgraded: false,
    };
    same(
        &ClassReply { class: k.clone(), reconcile: true },
        r#"{"class":{"id":2,"name":"gold","label":"*","dedicated":false,"replica":2,"priority":"normal","locality":"any","latency_target_us":500,"tier_to":"silver","tier_when":"idle>30d","qos":{"iops_min":0,"iops_max":300,"bw_min":0,"bw_max":1048576,"burst_ms":1000,"weight":1},"nodes":3,"per_node":{"iops_min":0,"iops_max":100,"bw_min":0,"bw_max":349525,"burst_ms":1000,"weight":1},"isolation":"shared","promise":"只承诺份额，不承诺尾延迟","downgraded":false},"reconcile":true}"#,
    );
    same(&ClassesReply { classes: vec![] }, r#"{"classes":[]}"#);
}

#[test]
fn qos_json_golden() {
    let q = QosQueue {
        tenant: Some(7),
        qos_id: Some(18),
        class_id: Some(2),
        depth: 3,
        served_per_s: 99.5,
        deficit_per_s: 0.0,
        limited_total: 41,
    };
    let bg = QosQueue { depth: 1, ..Default::default() };
    same(
        &QosReply { plugin: "mclock".into(), queues: vec![q, bg] },
        r#"{"plugin":"mclock","queues":[{"tenant":7,"qos_id":18,"class_id":2,"depth":3,"served_per_s":99.5,"deficit_per_s":0.0,"limited_total":41},{"tenant":null,"qos_id":null,"class_id":null,"depth":1,"served_per_s":0.0,"deficit_per_s":0.0,"limited_total":0}]}"#,
    );
}
