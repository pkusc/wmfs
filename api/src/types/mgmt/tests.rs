//! 金样：M7 管理端点的 JSON 逐字固定（服务端与 ApiClient 都用这些结构，字段名不靠猜）。

use super::*;
use crate::types::ChangeEvent;

fn same<T: Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug>(v: &T, want: &str) {
    assert_eq!(serde_json::to_string(v).unwrap(), want);
    assert_eq!(&serde_json::from_str::<T>(want).unwrap(), v);
}

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
    let u = UsageReply {
        path: "/p".into(),
        bytes: 1 << 40,
        files: 3,
        dirs: 2,
        per_class: vec![ClassUsage { class: "default".into(), bytes: 1 << 40, files: 3 }],
    };
    same(
        &u,
        r#"{"path":"/p","bytes":1099511627776,"files":3,"dirs":2,"per_class":[{"class":"default","bytes":1099511627776,"files":3}]}"#,
    );
    let a = StatusAttr {
        key: "replicas".into(),
        declared: "3".into(),
        source: "/home".into(),
        effect: "新文件 + 协调器".into(),
        actual: AttrActual { new_files: "1".into(), short_files: None },
        mismatched: true,
    };
    let s = StatusReply { path: "/".into(), warnings: vec!["w".into()], attrs: vec![a] };
    same(
        &s,
        r#"{"path":"/","warnings":["w"],"attrs":[{"key":"replicas","declared":"3","source":"/home","effect":"新文件 + 协调器","actual":{"new_files":"1"},"mismatched":true}]}"#,
    );
    let a2 = AttrActual { new_files: "3".into(), short_files: Some(1 << 33) }; // M9：replicas 的 short_files
    same(&a2, r#"{"new_files":"3","short_files":8589934592}"#);
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
