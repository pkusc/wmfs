//! 金样：M7 管理端点的 JSON 逐字固定（服务端与 ApiClient 都用这些结构，字段名不靠猜）。

use super::*;
use crate::types::ChangeEvent;
use crate::types::same;

fn map(kv: &[(&str, &str)]) -> BTreeMap<String, String> {
    kv.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
}

#[test]
fn attr_json_golden() {
    same(
        &AttrSetReq { path: "/p".into(), spec: "chunk=1M,stripe=2".into() },
        r#"{"path":"/p","spec":"chunk=1M,stripe=2"}"#,
    );
    let set =
        AttrSetReply { path: "/p".into(), set: map(&[("stripe", "2"), ("chunk", "1M")]), warnings: vec!["w".into()] };
    same(&set, r#"{"path":"/p","set":{"chunk":"1M","stripe":"2"},"warnings":["w"]}"#);
    let un = AttrUnsetReply { path: "/p".into(), key: "chunk".into(), old: "8M".into() };
    same(&un, r#"{"path":"/p","key":"chunk","old":"8M"}"#);
    let e = AttrEntry { key: "chunk".into(), value: "8M".into(), source: "/p/a".into(), effect: "新文件".into() };
    let g = AttrGetReply {
        path: "/p/a/b".into(),
        dir: "/p/a/b".into(),
        attrs: vec![e],
        warnings: vec![],
        semantics: "consistency=cto".into(),
        subtree: vec![DirAttrs { path: "/p".into(), attrs: map(&[("chunk", "1M")]) }],
        truncated: false,
    };
    same(
        &g,
        r#"{"path":"/p/a/b","dir":"/p/a/b","attrs":[{"key":"chunk","value":"8M","source":"/p/a","effect":"新文件"}],"warnings":[],"semantics":"consistency=cto","subtree":[{"path":"/p","attrs":{"chunk":"1M"}}],"truncated":false}"#,
    );
}

#[test]
fn usage_status_semantics_json_golden() {
    let q = QuotaUsage {
        dir: "/p".into(),
        class: None,
        hard: 100 << 20,
        soft: 80 << 20,
        grace_ms: 60_000,
        files: 1000,
        quantum: 1 << 20,
        used_bytes: 90 << 20,
        used_files: 3,
        soft_since_ms: 1_792_454_400_123,
        grace_left_ms: Some(42_000),
    };
    let u = UsageReply {
        path: "/p".into(),
        bytes: 1 << 40,
        files: 3,
        dirs: 2,
        per_class: vec![ClassUsage { class: "default".into(), bytes: 1 << 40, files: 3 }],
        quotas: vec![q],
    };
    same(
        &u,
        r#"{"path":"/p","bytes":1099511627776,"files":3,"dirs":2,"per_class":[{"class":"default","bytes":1099511627776,"files":3}],"quotas":[{"dir":"/p","class":null,"hard":104857600,"soft":83886080,"grace_ms":60000,"files":1000,"quantum":1048576,"used_bytes":94371840,"used_files":3,"soft_since_ms":1792454400123,"grace_left_ms":42000}]}"#,
    );
    let a = StatusAttr {
        key: "replicas".into(),
        declared: "3".into(),
        source: "/home".into(),
        effect: "新文件 + 协调器".into(),
        actual: AttrActual { new_files: "1".into(), ..Default::default() },
        mismatched: true,
    };
    let s = StatusReply { path: "/".into(), warnings: vec!["w".into()], attrs: vec![a], ..Default::default() };
    same(
        &s,
        r#"{"path":"/","warnings":["w"],"attrs":[{"key":"replicas","declared":"3","source":"/home","effect":"新文件 + 协调器","actual":{"new_files":"1"},"mismatched":true}],"scanned":0,"truncated":false,"degraded_files":0,"degraded_bytes":0,"reconciler":null}"#,
    );
    let old: StatusReply = serde_json::from_str(r#"{"path":"/","warnings":[],"attrs":[]}"#).unwrap();
    assert_eq!(old, StatusReply { path: "/".into(), ..Default::default() }, "M11 之前的回复照样解得开（只增）");
    let a2 = AttrActual { new_files: "3".into(), short_files: Some(1 << 33), ..Default::default() }; // M9：replicas 的 short_files
    same(&a2, r#"{"new_files":"3","short_files":8589934592}"#);
    let a3 = AttrActual {
        new_files: "zstd:3".into(),
        files: Some(20),
        mismatched_files: Some(19),
        open_files: Some(1),
        migrates: Some(true),
        ..Default::default()
    }; // M11：逐文件的统计
    same(&a3, r#"{"new_files":"zstd:3","files":20,"mismatched_files":19,"open_files":1,"migrates":true}"#);
    let q1 = QuotaUsage { dir: "/p".into(), hard: 100, used_bytes: 3, ..Default::default() };
    let a4 = AttrActual { new_files: "100B".into(), quotas: Some(vec![q1]), ..Default::default() }; // M11：quota 的实际
    same(
        &a4,
        r#"{"new_files":"100B","quotas":[{"dir":"/p","class":null,"hard":100,"soft":0,"grace_ms":0,"files":0,"quantum":0,"used_bytes":3,"used_files":0,"soft_since_ms":0,"grace_left_ms":null}]}"#,
    );
    let a5 = AttrActual { new_files: "7d".into(), files: Some(2), bytes: Some(1 << 33), ..Default::default() }; // M12：trash
    same(&a5, r#"{"new_files":"7d","files":2,"bytes":8589934592}"#);
    let a6 = AttrActual { new_files: "on".into(), lag: Some(5), ..Default::default() }; // M12：audit
    same(&a6, r#"{"new_files":"on","lag":5}"#);
    let m = SemanticsReply {
        consistency: vec!["c".into()],
        relaxations: vec!["r".into()],
        settings: map(&[("cap_ttl", "10m")]),
    };
    same(&m, r#"{"consistency":["c"],"relaxations":["r"],"settings":{"cap_ttl":"10m"}}"#);
}

#[test]
fn changelog_cursor_settings_json_golden() {
    let ev = ChangeEvent { seq: 7, op: "AttrSet".into(), ino: 16, ..Default::default() };
    let p = ChangelogPage { events: vec![ev], next: 7, last_seq: 1 << 33 };
    let j = serde_json::to_value(&p).unwrap();
    assert_eq!((j["next"].as_u64(), j["last_seq"].as_u64()), (Some(7), Some(1 << 33)));
    assert_eq!(j["events"][0]["op"], "AttrSet");
    assert_eq!(serde_json::from_value::<ChangelogPage>(j).unwrap(), p);
    let gone = GoneReply { error: "gone".into(), message: "m".into(), oldest: 5 };
    same(&gone, r#"{"error":"gone","message":"m","oldest":5}"#);
    same(&CursorReq { name: "audit-log".into(), seq: 9 }, r#"{"name":"audit-log","seq":9}"#);
    same(
        &CursorReply { name: "a".into(), seq: 9, updated_ms: 1_792_454_400_123 },
        r#"{"name":"a","seq":9,"updated_ms":1792454400123}"#,
    );
    same(
        &ClusterSettingsReply { settings: map(&[("epoch_gc_grace", "0s")]) },
        r#"{"settings":{"epoch_gc_grace":"0s"}}"#,
    );
    let nl = NotLeaderReply { error: "not_leader".into(), message: "m".into(), leader: Some("http://h:1".into()) };
    same(&nl, r#"{"error":"not_leader","message":"m","leader":"http://h:1"}"#);
}
