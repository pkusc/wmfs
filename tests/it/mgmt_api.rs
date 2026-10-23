//! M7 的管理 API（design.md 7.1、3.8、3.11），完整节点 + 带签名的 `ApiClient`（与 CLI 同一个客户端）：
//! - 属性：set（服务端切分、规范化、警告）/ get（来源）/ unset；错误码与 HTTP 状态（未知属性 400 einval、strong 400 eopnotsupp、
//!   路径不存在 404、client key 写 403）；路径里有空格与中文（百分号编码）。
//! - 递归用量、声明 vs 实际、语义、集群运行参数（`epoch_gc_grace=0s` 立即进集群图）。
//! - changelog：分页、AttrSet/Mkdir/Create 都在、长轮询在新记录提交后**立即**被唤醒（不是等到 wait_ms 到期）、同时最多 2 个长轮询
//!   （第 3 个立即回空页）、消费者游标（不能超过最新的 seq，名字有限制）。

use std::sync::Arc;
use std::time::{Duration, Instant};

use wmfs_core::api::types::{
    AttrGetReply, AttrSetReply, AttrSetReq, AttrUnsetReply, ChangelogPage, ClusterSettingsReply, CursorReply,
    CursorReq, KeyFile, SemanticsReply, StatusReply, UsageReply,
};
use wmfs_core::api::url::encode;
use wmfs_core::meta::schema::keys::ROOT_INO;
use wmfs_core::proto::meta::{MetaOp, MetaReply};
use wmfs_mgr::api::ApiClient;
use wmfs_testkit::harness::node::{TempDir, client, init, node_dir, put, start, who};

fn api(dir: &std::path::Path, mgmt: &str, key: &str) -> ApiClient {
    let k = KeyFile::parse(&std::fs::read_to_string(node_dir(dir).join(format!("keys/{key}.key"))).unwrap()).unwrap();
    ApiClient::new(vec![mgmt.to_string()], k)
}

fn set(a: &ApiClient, path: &str, spec: &str) -> Result<AttrSetReply, wmfs_mgr::api::ApiError> {
    a.post("/api/v1/attr", &AttrSetReq { path: path.into(), spec: spec.into() })
}

fn get(a: &ApiClient, path: &str) -> AttrGetReply {
    a.get(&format!("/api/v1/attr?path={}&recursive=0", encode(path))).unwrap()
}

fn value(g: &AttrGetReply, key: &str) -> (String, String) {
    let e = g.attrs.iter().find(|e| e.key == key).unwrap();
    (e.value.clone(), e.source.clone())
}

fn changelog(a: &ApiClient, after: u64, wait_ms: u64) -> ChangelogPage {
    a.get(&format!("/api/v1/changelog?after={after}&limit=1000&wait_ms={wait_ms}")).unwrap()
}

#[test]
fn attributes_usage_status_semantics_and_settings_over_http() {
    let d = TempDir::new("mgmt-api");
    init(&d.0);
    let run = start(&d.0);
    let a = api(&d.0, &run.mgmt_addr, "admin");
    let c = client(&d.0, &run, "admin");
    let me = who(1000, 1000);
    let mk = |parent, name: &str| {
        let op = MetaOp::Mkdir { parent, name: name.as_bytes().to_vec(), mode: 0o755, umask: 0 };
        match c.meta.write(&me, op).unwrap() {
            MetaReply::Entry { attr } => attr.ino,
            r => panic!("{r:?}"),
        }
    };
    let p = mk(ROOT_INO, "proj");
    mk(mk(p, "a b"), "中文");
    let r = set(&a, "/proj", "chunk=1M,stripe=2").unwrap();
    assert_eq!(r.set.get("chunk").map(String::as_str), Some("1M"));
    assert!(r.warnings.iter().any(|w| w.contains("只有 1 条可用的链")), "{:?}", r.warnings);
    set(&a, "/proj/a b", "chunk=8M").unwrap();
    let g = get(&a, "/proj/a b/中文");
    assert_eq!(
        (value(&g, "chunk"), value(&g, "stripe")),
        (("8M".into(), "/proj/a b".into()), ("2".into(), "/proj".into()))
    );
    assert!(g.semantics.starts_with("consistency=cto attr_ttl=1s"), "{}", g.semantics);
    let e = set(&a, "/proj", "chunck=4M").unwrap_err();
    assert_eq!((e.status, e.code.as_str(), e.message.as_str()), (400, "einval", "未知属性 chunck"));
    let e = set(&a, "/proj", "consistency=strong").unwrap_err();
    assert_eq!((e.status, e.code.as_str()), (400, "eopnotsupp"));
    assert_eq!(set(&a, "/nope", "chunk=1M").unwrap_err().status, 404);
    let e = set(&api(&d.0, &run.mgmt_addr, "client"), "/proj", "chunk=2M").unwrap_err();
    assert_eq!(e.status, 403, "写要 admin key");
    let g2: AttrGetReply = api(&d.0, &run.mgmt_addr, "client").get("/api/v1/attr?path=/proj").unwrap();
    assert_eq!(value(&g2, "chunk").0, "1M", "读对所有已认证的 key 开放");
    let u: AttrUnsetReply = a.delete(&format!("/api/v1/attr?path={}&key=chunk", encode("/proj/a b"))).unwrap();
    assert_eq!(u.old, "8M");
    assert_eq!(value(&get(&a, "/proj/a b/中文"), "chunk"), ("1M".into(), "/proj".into()));
    // 用量：写一个文件
    put(&c, &me, p, "f", &vec![7u8; 100_000]).unwrap();
    let us: UsageReply = a.get("/api/v1/usage?path=/proj").unwrap();
    assert_eq!((us.bytes, us.files, us.dirs), (100_000, 1, 2));
    let st: StatusReply = a.get("/api/v1/status?path=/").unwrap();
    assert!(st.warnings.iter().any(|w| w.contains("/home")), "{:?}", st.warnings);
    assert!(st.attrs.iter().all(|x| !x.declared.is_empty() && !x.actual.new_files.is_empty()));
    let sem: SemanticsReply = a.get("/api/v1/semantics").unwrap();
    assert_eq!(sem.settings.get("cap_ttl").map(String::as_str), Some("10m"));
    let mut m = std::collections::BTreeMap::new();
    m.insert("epoch_gc_grace".to_string(), "0s".to_string());
    let s: ClusterSettingsReply = a.post("/api/v1/cluster/settings", &m).unwrap();
    assert_eq!(s.settings.get("epoch_gc_grace").map(String::as_str), Some("0s"));
    assert_eq!(run.meta.cluster_map().unwrap().settings.epoch_gc_grace_ms, 0, "进了集群图");
    m.insert("session_lease".into(), "0s".into());
    assert_eq!(a.post::<_, ClusterSettingsReply>("/api/v1/cluster/settings", &m).unwrap_err().code, "einval");
    c.meta.close();
    run.stop();
}

#[test]
fn changelog_pages_long_polls_and_cursors() {
    let d = TempDir::new("mgmt-changelog");
    init(&d.0);
    let run = start(&d.0);
    let a = Arc::new(api(&d.0, &run.mgmt_addr, "admin"));
    let c = client(&d.0, &run, "admin");
    let me = who(1000, 1000);
    let mkdir = |name: &str| {
        let op = MetaOp::Mkdir { parent: ROOT_INO, name: name.as_bytes().to_vec(), mode: 0o755, umask: 0 };
        c.meta.write(&me, op).unwrap();
    };
    mkdir("d1");
    set(&a, "/d1", "chunk=1M").unwrap();
    put(&c, &me, ROOT_INO, "f", b"hi").unwrap();
    let p = changelog(&a, 0, 0);
    let ops: Vec<&str> = p.events.iter().map(|e| e.op.as_str()).collect();
    for want in ["SessionOpen", "Mkdir", "AttrSet", "Create", "SetSize"] {
        assert!(ops.contains(&want), "{want} 在 changelog 里：{ops:?}");
    }
    assert_eq!((p.next, p.last_seq), (p.events.last().unwrap().seq, p.events.last().unwrap().seq));
    assert!(changelog(&a, p.last_seq, 0).events.is_empty(), "没有新记录：立即回空页");
    // 长轮询：新记录一提交就被唤醒
    let (a2, after) = (a.clone(), p.last_seq);
    let t = std::thread::spawn(move || {
        let t0 = Instant::now();
        (changelog(&a2, after, 8000), t0.elapsed())
    });
    std::thread::sleep(Duration::from_millis(300));
    mkdir("d2");
    let (got, took) = t.join().unwrap();
    assert_eq!(got.events.first().map(|e| e.op.as_str()), Some("Mkdir"), "{got:?}");
    assert!(took < Duration::from_secs(4), "提交之后立即唤醒（不是等到 8 s 到期）：{took:?}");
    // 同时最多 2 个长轮询：第 3 个立即回
    let last = got.last_seq;
    let polls: Vec<_> = (0..2)
        .map(|_| {
            let a2 = a.clone();
            std::thread::spawn(move || changelog(&a2, last, 2500))
        })
        .collect();
    std::thread::sleep(Duration::from_millis(400));
    let t0 = Instant::now();
    let third = changelog(&a, last, 2500);
    assert!(t0.elapsed() < Duration::from_millis(1500) && third.events.is_empty(), "第 3 个不等：{:?}", t0.elapsed());
    for p in polls {
        assert!(p.join().unwrap().events.is_empty());
    }
    // 游标
    let cr: CursorReply = a.post("/api/v1/changelog/cursor", &CursorReq { name: "t-1".into(), seq: last }).unwrap();
    assert_eq!(cr.seq, last);
    let back: CursorReply = a.get("/api/v1/changelog/cursor?name=t-1").unwrap();
    assert_eq!((back.seq, back.updated_ms), (last, cr.updated_ms));
    let e = a.post::<_, CursorReply>("/api/v1/changelog/cursor", &CursorReq { name: "t-1".into(), seq: last + 99 });
    assert_eq!(e.unwrap_err().code, "einval", "游标不能超过最新的 seq");
    let e = a.post::<_, CursorReply>("/api/v1/changelog/cursor", &CursorReq { name: "a b".into(), seq: 1 });
    assert_eq!(e.unwrap_err().code, "einval");
    assert_eq!(a.get::<CursorReply>("/api/v1/changelog/cursor?name=nope").unwrap_err().status, 404);
    assert_eq!(changelog(&a, 0, 0).last_seq, last, "游标提交不写 changelog");
    c.meta.close();
    run.stop();
}
