//! M7 的管理端点（依赖元数据的那些）的请求/响应结构（design.md 7.1、3.8、3.9、3.10、3.1.5、3.11）：目录属性、递归用量、
//! 声明 vs 实际、语义、changelog 与消费者游标、集群运行参数、409 的回复。字段只增不改（ADR-0005）；每个结构的 JSON 都有逐字的
//! 金样测试（`mgmt/tests.rs`）。
//!
//! 属性值一律是**显示形式**（`chunk` 是 `"1M"`、`attr_ttl` 是 `"3s"`），与 `wmfs set` 接受的写法相同；DIRATTR 里存的规范化形式
//! （`"1048576"`）不出现在管理 API 里。路径都是文件系统内的绝对路径（CLI 负责把挂载点下的路径换算过来，7.2）。

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// `POST /api/v1/attr` 的请求：`spec` 是原样的属性字符串（`chunk=1M,stripe=2`），由服务端切分、解析、规范化（3.8）。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct AttrSetReq {
    pub path: String,
    pub spec: String,
}

/// `POST /api/v1/attr` 的回复：设置了的键值（规范化之后的显示形式）与警告（声明在当前集群无法满足等，3.8）。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct AttrSetReply {
    pub path: String,
    pub set: BTreeMap<String, String>,
    pub warnings: Vec<String>,
}

/// `DELETE /api/v1/attr?path=&key=` 的回复：删掉的键与它原来的值。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct AttrUnsetReply {
    pub path: String,
    pub key: String,
    pub old: String,
}

/// `AttrEntry.source`、`StatusAttr.source` 里表示"内置默认值"的写法（服务端写、CLI 比较，都用这一个常量）。
pub const DEFAULT_SOURCE: &str = "(默认值)";

/// 一个属性的生效值：`source` 是来源目录的路径，内置默认值是 [`DEFAULT_SOURCE`]；`effect` 是生效方式（"新文件" 等）。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct AttrEntry {
    pub key: String,
    pub value: String,
    pub source: String,
    pub effect: String,
}

/// 一个显式设置了属性的目录（`wmfs get -r`）。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct DirAttrs {
    pub path: String,
    pub attrs: BTreeMap<String, String>,
}

/// `GET /api/v1/attr?path=&recursive=0|1`：`dir` 是生效属性所属的目录（`path` 是文件时 = 它所在的目录）；`semantics` 是
/// `wmfs get` 最后一行 `semantics:` 后面的内容（3.1.5）；`warnings` 在它之前打印（声明与实际不同）；`recursive=1` 时 `subtree`
/// 列出子树里显式设置了属性的目录（最多 1000 个，多了 `truncated`）。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct AttrGetReply {
    pub path: String,
    pub dir: String,
    pub attrs: Vec<AttrEntry>,
    pub warnings: Vec<String>,
    pub semantics: String,
    pub subtree: Vec<DirAttrs>,
    pub truncated: bool,
}

/// 一个存储类的用量。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct ClassUsage {
    pub class: String,
    pub bytes: u64,
    pub files: u64,
}

/// `GET /api/v1/usage?path=`：目录的递归用量（RSTAT，常量时间，3.9）——子树里普通文件的逻辑字节、非目录文件数、子目录数
/// （都不含目录自己）。配额在 M10 加入（只增）。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct UsageReply {
    pub path: String,
    pub bytes: u64,
    pub files: u64,
    pub dirs: u64,
    pub per_class: Vec<ClassUsage>,
}

/// 一个属性的"实际"（3.10）。M7：`new_files` = 这个目录下的新文件此刻实际得到的值（例如 replicas 是
/// min(声明, 可用故障域数)）；逐文件扫描已有文件的统计在 M11 加入（只增）。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct AttrActual {
    pub new_files: String,
}

/// `GET /api/v1/status` 里一个属性的声明 vs 实际。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct StatusAttr {
    pub key: String,
    pub declared: String,
    pub source: String,
    pub effect: String,
    pub actual: AttrActual,
    pub mismatched: bool,
}

/// `GET /api/v1/status?path=`：`warnings` 是集群警告与这棵子树里无法满足的声明（`wmfs status` 第一行起打印）。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct StatusReply {
    pub path: String,
    pub warnings: Vec<String>,
    pub attrs: Vec<StatusAttr>,
}

/// `GET /api/v1/semantics`：一致性等级与放松清单的要点（semantics.md）、当前的集群运行参数（显示形式）。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct SemanticsReply {
    pub consistency: Vec<String>,
    pub relaxations: Vec<String>,
    pub settings: BTreeMap<String, String>,
}

/// `GET /api/v1/changelog?after=&limit=&wait_ms=`：`after` 之后的事件（按 seq 升序）；`next` = 下一次的 `after`（最后一条的 seq，
/// 没有新事件时就是请求里的 `after`）；`last_seq` = 当前最新的 seq（0 = 还没有事件）。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct ChangelogPage {
    pub events: Vec<super::ChangeEvent>,
    pub next: u64,
    pub last_seq: u64,
}

/// `410 Gone`：`after` 之后的一段已被修剪（保留期之外），`oldest` 是最早还在的 seq——缺口是显式的（3.11）。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct GoneReply {
    pub error: String,
    pub message: String,
    pub oldest: u64,
}

/// `POST /api/v1/changelog/cursor` 的请求（消费者提交游标，存在 CURSOR 里）。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct CursorReq {
    pub name: String,
    pub seq: u64,
}

/// 游标（`POST` 与 `GET /api/v1/changelog/cursor?name=` 的回复）。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct CursorReply {
    pub name: String,
    pub seq: u64,
    pub updated_ms: u64,
}

/// `POST /api/v1/cluster/settings` 的回复：全部集群运行参数（名字去掉 `_ms`，值是显示形式）；请求是 `{名字: 值}`（3.2.8）。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct ClusterSettingsReply {
    pub settings: BTreeMap<String, String>,
}

/// `409`：本节点不是元数据 leader，`leader` 是 leader 的管理地址（`http://host:port`；不知道时为 null），ApiClient 自动跟随（7.1）。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct NotLeaderReply {
    pub error: String,
    pub message: String,
    pub leader: Option<String>,
}

#[cfg(test)]
mod tests;
