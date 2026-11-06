//! M11 的后台任务用例共用的小工具（design.md 3.10）：带签名的管理 API（与 CLI 同一个 `ApiClient`）——设属性、看 status、看 changelog
//! （含事件 `extra` 字段名的金样）、控制任务、建数据密钥——与"等到 status 里某个键的已有文件都与声明一致"；建目录、好压缩的带标记文本。

use std::path::Path;

use wmfs_core::api::types::{
    AttrSetReply, AttrSetReq, ChangeEvent, ChangelogPage, DataKeyCreateReply, DataKeyCreateReq, KeyFile, StatusAttr,
    StatusReply, TaskInfo, TaskSetReq, TasksReply,
};
use wmfs_core::api::url::encode;
use wmfs_core::proto::meta::{MetaOp, MetaReply};
use wmfs_mgr::api::{ApiClient, ApiError};

use super::node::{Client, node_dir, wait_for, who};

/// [`text`] 里每一行开头的标记（加密之后 target 目录里应当找不到它）。
pub const MARK: &[u8] = b"WMFS-PLAINTEXT-MARKER";

/// 好压缩、带标记的文本（`n` 字节）。
pub fn text(seed: u32, n: usize) -> Vec<u8> {
    let mut v = Vec::with_capacity(n + 64);
    let mut i = 0;
    while v.len() < n {
        v.extend_from_slice(format!("WMFS-PLAINTEXT-MARKER {seed} {i}\n").as_bytes());
        i += 1;
    }
    v.truncate(n);
    v
}

/// 在 `parent` 下建目录 `name`（uid 1000，0755）。
pub fn mkdir(c: &Client, parent: u64, name: &str) -> u64 {
    let op = MetaOp::Mkdir { parent, name: name.as_bytes().to_vec(), mode: 0o755, umask: 0 };
    match c.meta.write(&who(1000, 1000), op).unwrap() {
        MetaReply::Entry { attr } => attr.ino,
        r => panic!("mkdir {name}：{r:?}"),
    }
}

/// 用节点目录 `nd` 里的 `<key>.key` 连 `mgmt`（可以给几个地址，逗号分隔）。
pub fn api_at(nd: &Path, mgmt: &str, key: &str) -> ApiClient {
    let k = KeyFile::parse(&std::fs::read_to_string(nd.join(format!("keys/{key}.key"))).unwrap()).unwrap();
    ApiClient::new(mgmt.split(',').map(String::from).collect(), k)
}

/// 单节点（`harness::node` 的目录布局）的 admin 客户端。
pub fn api(dir: &Path, mgmt: &str) -> ApiClient {
    api_at(&node_dir(dir), mgmt, "admin")
}

pub fn set(a: &ApiClient, path: &str, spec: &str) -> Result<AttrSetReply, ApiError> {
    a.post("/api/v1/attr", &AttrSetReq { path: path.into(), spec: spec.into() })
}

pub fn status(a: &ApiClient, path: &str) -> StatusReply {
    a.get(&format!("/api/v1/status?path={}", encode(path))).unwrap()
}

/// status 里一个键的那一行。
pub fn attr(s: &StatusReply, key: &str) -> StatusAttr {
    s.attrs.iter().find(|x| x.key == key).cloned().unwrap_or_else(|| panic!("status 里没有 {key}"))
}

/// 等到 `path` 子树里 `key` 的已有文件都与声明一致（最多 `secs` 秒）。
pub fn wait_converted(a: &ApiClient, path: &str, key: &str, secs: u64) -> bool {
    wait_for(secs, || attr(&status(a, path), key).actual.mismatched_files == Some(0))
}

pub fn changelog(a: &ApiClient, after: u64) -> ChangelogPage {
    a.get(&format!("/api/v1/changelog?after={after}&limit=1000&wait_ms=0")).unwrap()
}

/// changelog 里第一个操作是 `op`、`extra` 里有 (k, v) 的事件（`kv` 为 None 就只看操作）。
pub fn find_event(a: &ApiClient, op: &str, kv: Option<(&str, &str)>) -> Option<ChangeEvent> {
    let hit = |e: &ChangeEvent| e.op == op && kv.is_none_or(|(k, v)| e.extra.iter().any(|(x, y)| x == k && y == v));
    changelog(a, 0).events.into_iter().find(hit)
}

/// changelog 里有没有这样的事件（见 [`find_event`]）。
pub fn has_event(a: &ApiClient, op: &str, kv: Option<(&str, &str)>) -> bool {
    find_event(a, op, kv).is_some()
}

/// 这个事件 `extra` 的字段名，按次序、逗号分隔：changelog 的金样（只增；M10 起的做法，见 `tests/it/mgmt_keys.rs`）。
pub fn extra_keys(a: &ApiClient, op: &str, kv: Option<(&str, &str)>) -> String {
    let e = find_event(a, op, kv).unwrap_or_else(|| panic!("changelog 里没有 {op} {kv:?}"));
    e.extra.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>().join(",")
}

pub fn task(a: &ApiClient, kind: &str, action: &str, body: Option<TaskSetReq>) -> TaskInfo {
    a.post(&format!("/api/v1/tasks/{kind}/{action}"), &body.unwrap_or_default()).unwrap()
}

pub fn tasks(a: &ApiClient) -> Vec<TaskInfo> {
    a.get::<TasksReply>("/api/v1/tasks").unwrap().tasks
}

pub fn datakey(a: &ApiClient, name: &str) -> u32 {
    a.post::<_, DataKeyCreateReply>("/api/v1/datakeys", &DataKeyCreateReq { name: name.into(), tenant: 0 }).unwrap().id
}
