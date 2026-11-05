//! 金样：M10 管理端点（key 与会话）的 JSON 逐字固定（服务端与 ApiClient 都用这些结构，字段名不靠猜）。

use super::*;
use crate::types::same;

#[test]
fn keys_and_sessions_json_golden() {
    let req = KeyCreateReq {
        name: "q-only".into(),
        role: "client".into(),
        tenant: 0,
        uids: "1000-1999".into(),
        paths: vec!["/home/q".into()],
        paths_ro: vec![],
        root_squash: true,
    };
    same(
        &req,
        r#"{"name":"q-only","role":"client","tenant":0,"uids":"1000-1999","paths":["/home/q"],"paths_ro":[],"root_squash":true}"#,
    );
    let short: KeyCreateReq = serde_json::from_str(r#"{"name":"k","role":"client"}"#).unwrap();
    assert_eq!(short, KeyCreateReq { name: "k".into(), role: "client".into(), ..Default::default() });
    same(&KeyCreateReply { id: 3, key_file: "3:ab".into() }, r#"{"id":3,"key_file":"3:ab"}"#);
    let k = KeyInfo {
        id: 3,
        name: "q-only".into(),
        role: "client".into(),
        tenant: 0,
        uids: "0-4294967295".into(),
        root_squash: false,
        paths: vec![PathCapInfo { ino: 7, path: "/home/q".into(), read_only: false }],
        revoked: false,
        created_ms: 1,
    };
    same(
        &KeysReply { keys: vec![k] },
        r#"{"keys":[{"id":3,"name":"q-only","role":"client","tenant":0,"uids":"0-4294967295","root_squash":false,"paths":[{"ino":7,"path":"/home/q","read_only":false}],"revoked":false,"created_ms":1}]}"#,
    );
    same(&KeyRevokeReply { id: 3, evicted_sessions: 1 }, r#"{"id":3,"evicted_sessions":1}"#);
    let s = SessionRow {
        id: "1f".into(),
        state: "evicted".into(),
        client_name: String::new(),
        host: String::new(),
        mount: String::new(),
        key_id: 0,
        created_ms: 0,
        evicted_ms: Some(5),
    };
    same(
        &SessionsReply { sessions: vec![s] },
        r#"{"sessions":[{"id":"1f","state":"evicted","client_name":"","host":"","mount":"","key_id":0,"created_ms":0,"evicted_ms":5}]}"#,
    );
}
