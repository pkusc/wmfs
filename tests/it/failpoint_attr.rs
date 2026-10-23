//! fail point 用例（design.md 9.4）：属性的设置经执行器提交（`meta.exec.before_propose`，3.11）——
//! - `return(EIO)`：这个批次作废（什么也没应用），`wmfs set` 报 500 eio；执行器随即清空属性解析缓存（`meta/attrs.rs`：缓存里可能有
//!   按作废批次的覆盖层算出的项）——于是之后的第一次解析是未命中（`wmfs_attr_cache_total{result="miss"}` 增加；不清空的话同一个
//!   attr_epoch 下会命中），读到的仍是旧值；再设一次照常成功；changelog 里只有成功那一次的 AttrSet。"按覆盖层算出的项为什么错"
//!   由 core 的单元测试 `meta::attrs` 直接验证。
//! - `return(RETRY)`：可重试（没有应用过），管理 API 自己退避重试，`wmfs set` 一次成功。
//!
//! fail point 是进程级的：单独一个测试程序。

use wmfs_core::api::types::{AttrGetReply, AttrSetReply, AttrSetReq, ChangelogPage, KeyFile};
use wmfs_core::api::{failpoint, metrics};
use wmfs_core::meta::schema::keys::ROOT_INO;
use wmfs_core::proto::meta::MetaOp;
use wmfs_mgr::api::{ApiClient, ApiError};
use wmfs_testkit::harness::node::{TempDir, client, init, node_dir, start, who};

fn set(a: &ApiClient, spec: &str) -> Result<AttrSetReply, ApiError> {
    a.post("/api/v1/attr", &AttrSetReq { path: "/p".into(), spec: spec.into() })
}

fn chunk(a: &ApiClient) -> String {
    let g: AttrGetReply = a.get("/api/v1/attr?path=/p").unwrap();
    g.attrs.into_iter().find(|e| e.key == "chunk").unwrap().value
}

/// 属性解析缓存的命中 / 未命中次数（节点就在这个测试进程里，同一份指标）。
fn cache(result: &str) -> u64 {
    metrics::counter("wmfs_attr_cache_total", "目录属性解析缓存的命中与未命中", &[("result", result)]).get()
}

#[test]
fn a_failed_batch_leaves_attributes_and_their_cache_untouched() {
    let d = TempDir::new("fp-attr");
    init(&d.0);
    let run = start(&d.0);
    let key = KeyFile::parse(&std::fs::read_to_string(node_dir(&d.0).join("keys/admin.key")).unwrap()).unwrap();
    let a = ApiClient::new(vec![run.mgmt_addr.clone()], key);
    let c = client(&d.0, &run, "admin");
    let op = MetaOp::Mkdir { parent: ROOT_INO, name: b"p".to_vec(), mode: 0o755, umask: 0 };
    c.meta.write(&who(0, 0), op).unwrap();
    assert_eq!(chunk(&a), "4M");
    let hit0 = cache("hit");
    assert_eq!(chunk(&a), "4M");
    assert!(cache("hit") > hit0, "同一个 attr_epoch 下再解析一次：命中缓存");
    failpoint::set("meta.exec.before_propose", "times(1,return(EIO))").unwrap();
    let e = set(&a, "chunk=1M").unwrap_err();
    assert_eq!((e.status, e.code.as_str()), (500, "eio"), "{e}");
    let miss0 = cache("miss");
    assert_eq!(chunk(&a), "4M", "作废的批次什么也没应用");
    assert!(cache("miss") > miss0, "批次作废时执行器清空了属性缓存：之后的第一次解析是未命中");
    set(&a, "chunk=2M").unwrap();
    assert_eq!(chunk(&a), "2M", "再设一次照常成功（作废批次的 attr_epoch 被重新用上）");
    failpoint::set("meta.exec.before_propose", "times(1,return(RETRY))").unwrap();
    assert_eq!(set(&a, "chunk=8M").unwrap().set["chunk"], "8M", "RETRY：管理 API 自己重试");
    failpoint::clear("meta.exec.before_propose");
    let log: ChangelogPage = a.get("/api/v1/changelog?after=0").unwrap();
    let news: Vec<String> = log
        .events
        .iter()
        .filter(|e| e.op == "AttrSet")
        .flat_map(|e| e.extra.iter().filter(|(k, _)| k == "new").map(|(_, v)| v.clone()))
        .collect();
    assert_eq!(news, vec!["2M", "8M"], "changelog 里只有真正提交了的");
    c.meta.close();
    run.stop();
}
