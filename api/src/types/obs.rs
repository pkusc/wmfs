//! 节点本地端点的结构（design.md 3.11、7.1；与 `core/src/mgmt/node.rs` 一一对应）：trace（`GET /api/v1/trace/<id>`、
//! `GET /api/v1/trace/recent?n=`，M14）与 `GET /api/v1/node`（M11 起；M14 只增 target、boot_id、内存池、线程数）。字段只增不改
//! （ADR-0005）；JSON 金样在 `obs/tests.rs`（`NodeInfoReply` 的 M11 部分的金样仍在 `tasks/tests.rs`）。

use serde::{Deserialize, Serialize};

/// 一个 span：本节点处理一个请求的一段（从收到请求到回复发出）。`trace_id` 写成 16 位小写 hex（u64 放进 JSON 数字会被浏览器与 jq
/// 丢精度）；`span_id` 是第几跳（客户端发来的请求是 0，链上每转发一跳 + 1）；`component` 是消息族（`MetaCall`、`DataCall`、
/// `PeerCall`）；`op` 是操作名（`WriteChunk`、`ForwardWrite`、`Create`……）；`start_ms` 是收到请求时的墙钟毫秒，`dur_us` 是到回复发出的
/// 微秒数；`code` 是结果（`OK` 或错误码名）。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct TraceSpan {
    pub trace_id: String,
    pub span_id: u16,
    pub node: u32,
    pub component: String,
    pub op: String,
    pub start_ms: u64,
    pub dur_us: u64,
    pub code: String,
}

/// `GET /api/v1/trace/<id>`（本节点关于这个 trace 的 span，按 span_id 排）与 `GET /api/v1/trace/recent?n=`（本节点最近的 n 个
/// 客户端请求——span_id 为 0 的，从新到旧）。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct TraceReply {
    pub node_id: u32,
    pub spans: Vec<TraceSpan>,
}

/// `GET /api/v1/node`（每个节点回答本节点的）：节点号、本节点看不看得到任务的控制（`controlled`：它在元数据 raft 里、已应用的 KV 里
/// 有集群级的任务记录；只做数据的节点为 false——`wmfs task pause|set|run` 到不了它，它一直按默认值跑）与本节点上 `every-node` 任务的
/// 进度（M11，原在 `types/tasks.rs`）。实现注记（M14，只增，ADR-0042 第 4 条）：`boot_id`（本次启动的随机号，16 位 hex）、`targets`、内存池 `pool`、
/// 进程的线程数 `threads`（7.1 表里这一行的其余字段）。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
pub struct NodeInfoReply {
    pub node_id: u32,
    pub controlled: bool,
    pub tasks: Vec<super::TaskInfo>,
    #[serde(default)]
    pub boot_id: String,
    #[serde(default)]
    pub targets: Vec<NodeTarget>,
    #[serde(default)]
    pub pool: Vec<PoolClass>,
    #[serde(default)]
    pub threads: u64,
}

/// `GET /api/v1/node` 里本节点的一个 target（M14，只增）：编号、目录、已用与容量（字节；容量是 statfs 与 node.toml 上限的较小者）、
/// 是否只读、它所在的物理整盘（6.3）。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct NodeTarget {
    pub id: u32,
    pub path: String,
    pub used: u64,
    pub capacity: u64,
    pub read_only: bool,
    pub phys_devs: Vec<String>,
}

/// `GET /api/v1/node` 里内存池的一档（M14，只增；3.6.1）：名字（S/M/L）、槽位大小、槽位数、两张空闲表的长度（复制 lane 只给复制流量）。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct PoolClass {
    pub class: String,
    pub slot_size: u64,
    pub slots: u32,
    pub free_normal: u64,
    pub free_repl: u64,
}

#[cfg(test)]
mod tests;
