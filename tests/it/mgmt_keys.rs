//! M10 的认证 key 与会话端点（design.md 7.1、3.4.3、3.4.4、4.2），完整节点 + 带签名的 `ApiClient`（与 CLI 同一个客户端）：
//! - `POST /api/v1/keys`：带路径限制、uid 范围的 client key——用它连上来只能访问那个子树（子树里能建文件；上级目录只能查名字与取属性；
//!   子树外 `EACCES`）、只能声明范围里的 uid；管理 API 按路径的查询也只能查那个子树（子树外 403，ADR-0038 第 2 条）；`GET /api/v1/keys`
//!   列出它，限制跟着被 rename 的目录。
//! - `POST /api/v1/keys/<id>/revoke`：它的会话被驱逐、开不了新会话；集群图更新之后用它建立的连接被关闭、重连时握手被拒；它签名的
//!   管理请求 401；最后一把有效的 admin key 不能撤销。
//! - `GET /api/v1/sessions`、`POST /api/v1/sessions/<id>/evict`：手动驱逐 → `evicting` → `evicted`（仍然列出）。

use wmfs_core::api::types::{
    AttrGetReply, ChangelogPage, KeyCreateReply, KeyCreateReq, KeyFile, KeyRevokeReply, KeysReply, SessionRow,
    SessionsReply, UsageReply,
};
use wmfs_core::base::ErrCode;
use wmfs_core::meta::schema::keys::{FIRST_INO as HOME_INO, ROOT_INO}; // init 建的 /home 是第一个 inode
use wmfs_core::proto::meta::{MetaOp, MetaReply};
use wmfs_mgr::api::{ApiClient, ApiError};
use wmfs_testkit::harness::node::{TempDir, client, init, node_dir, start, wait_for, who};

fn api(dir: &std::path::Path, mgmt: &str, key: &str) -> ApiClient {
    let k = KeyFile::parse(&std::fs::read_to_string(node_dir(dir).join(format!("keys/{key}.key"))).unwrap()).unwrap();
    ApiClient::new(vec![mgmt.to_string()], k)
}

fn status<T: std::fmt::Debug>(r: Result<T, ApiError>) -> u16 {
    r.map(|x| panic!("应当失败：{x:?}")).unwrap_or_else(|e| e.status)
}

fn sessions(a: &ApiClient) -> Vec<SessionRow> {
    a.get::<SessionsReply>("/api/v1/sessions").unwrap().sessions
}

#[test]
fn keys_with_path_caps_revocation_and_sessions() {
    let d = TempDir::new("mgmt-keys");
    init(&d.0);
    let run = start(&d.0);
    let admin = api(&d.0, &run.mgmt_addr, "admin");
    let c = client(&d.0, &run, "admin");
    let me = who(1000, 1000);
    let mk = |parent, name: &str| match c
        .meta
        .write(&me, MetaOp::Mkdir { parent, name: name.into(), mode: 0o755, umask: 0 })
    {
        Ok(MetaReply::Entry { attr }) => attr.ino,
        r => panic!("{r:?}"),
    };
    let (q, private) = (mk(HOME_INO, "q"), mk(HOME_INO, "private"));

    let req = KeyCreateReq {
        name: "q-only".into(),
        role: "client".into(),
        uids: "1000-1999".into(),
        paths: vec!["/home/q".into()],
        ..Default::default()
    };
    let r: KeyCreateReply = admin.post("/api/v1/keys", &req).unwrap();
    assert_eq!(KeyFile::parse(&r.key_file).unwrap().key_id, r.id, "回复里是一次性的 key 文件");
    let bad = KeyCreateReq { paths: vec!["/home/nope".into()], ..req.clone() };
    assert_eq!(status(admin.post::<_, KeyCreateReply>("/api/v1/keys", &bad)), 404, "路径不存在");
    std::fs::write(node_dir(&d.0).join("keys/q-only.key"), &r.key_file).unwrap();
    let k = client(&d.0, &run, "q-only");
    let create = |parent, who| {
        k.meta.write(&who, MetaOp::Create { parent, name: b"f".to_vec(), mode: 0o644, umask: 0, excl: true })
    };
    assert!(create(q, me.clone()).is_ok(), "子树里照常");
    assert_eq!(create(private, me.clone()).err(), Some(ErrCode::EACCES), "子树外");
    assert_eq!(create(q, who(3000, 3000)).err(), Some(ErrCode::EACCES), "uid 不在这把 key 的范围里");
    let lookup = k.meta.read(&me, MetaOp::Lookup { parent: HOME_INO, name: b"q".to_vec() });
    assert!(lookup.is_ok(), "上级目录：只为路径遍历，能查名字");
    let rd = k.meta.read(&me, MetaOp::ReadDir { ino: ROOT_INO, after: vec![], limit: 10, plus: false });
    assert_eq!(rd.err(), Some(ErrCode::EACCES), "上级目录不能列");

    let ka = api(&d.0, &run.mgmt_addr, "q-only");
    assert!(ka.get::<UsageReply>("/api/v1/usage?path=/home/q").is_ok(), "管理 API：子树里能查");
    assert_eq!(status(ka.get::<UsageReply>("/api/v1/usage?path=/home/private")), 403, "子树外 403");
    assert_eq!(status(ka.get::<AttrGetReply>("/api/v1/attr?path=/&recursive=0")), 403);

    assert!(
        c.meta
            .write(
                &me,
                MetaOp::Rename {
                    parent: HOME_INO,
                    name: b"q".to_vec(),
                    new_parent: HOME_INO,
                    new_name: b"q2".to_vec(),
                    flags: 0
                }
            )
            .is_ok()
    );
    let ks: KeysReply = admin.get("/api/v1/keys").unwrap();
    let mine = ks.keys.iter().find(|x| x.id == r.id).unwrap();
    assert_eq!(
        (mine.uids.as_str(), mine.paths[0].path.as_str(), mine.revoked),
        ("1000-1999", "/home/q2", false),
        "限制跟着目录走"
    );
    assert!(ks.keys.iter().any(|x| x.id == 1 && x.role == "admin" && x.uids == "0-4294967295"));

    let ks_sid = format!("{:x}", k.meta.session());
    assert!(sessions(&admin).iter().any(|s| s.id == ks_sid && s.state == "active" && s.key_id == r.id));
    let rv: KeyRevokeReply = admin.post(&format!("/api/v1/keys/{}/revoke", r.id), &()).unwrap();
    assert_eq!(rv.evicted_sessions, 1, "它的会话一并驱逐");
    let r = k.meta.read(&me, MetaOp::GetAttr { ino: q }).err();
    assert!(matches!(r, Some(ErrCode::SESSION_EVICTED | ErrCode::AUTH_FAILED)), "会话已被驱逐（或连接已关闭）：{r:?}");
    assert_eq!(k.meta.session(), u64::from_str_radix(&ks_sid, 16).unwrap(), "撤销了的 key 开不了新会话");
    let closed = || k.meta.read(&me, MetaOp::GetAttr { ino: q }).err() == Some(ErrCode::AUTH_FAILED);
    assert!(wait_for(10, closed), "集群图更新之后已建立的连接被关闭，重连时握手被拒（ADR-0009）");
    assert_eq!(status(ka.get::<UsageReply>("/api/v1/usage?path=/home/q2")), 401, "撤销了的 key 签名的请求");
    assert_eq!(status(admin.post::<_, KeyRevokeReply>("/api/v1/keys/1/revoke", &())), 403, "最后一把有效的 admin key");

    let sid = format!("{:x}", c.meta.session());
    let ev: SessionRow = admin.post(&format!("/api/v1/sessions/{sid}/evict"), &()).unwrap();
    assert_eq!(ev.state, "evicting");
    assert!(wait_for(20, || sessions(&admin).iter().any(|s| s.id == sid && s.state == "evicted")), "两个阶段做完");
    assert_eq!(status(admin.post::<_, SessionRow>("/api/v1/sessions/123/evict", &())), 404);

    // 金样（M10 审查）：M10 新加的 changelog 事件的 extra 字段名与次序（只增，ADR-0029 的做法）
    let page: ChangelogPage = admin.get("/api/v1/changelog?after=0&limit=1000").unwrap();
    let names = |op: &str| {
        let e = page.events.iter().find(|e| e.op == op).unwrap_or_else(|| panic!("changelog 里没有 {op}"));
        e.extra.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>().join(",")
    };
    assert_eq!(names("KeyCreate"), "key,name,role");
    assert_eq!(names("KeyRevoke"), "key,name");
    assert_eq!(names("SessionEvicted"), "session,why,map_epoch");
}
