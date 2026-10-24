//! M8 的管理端点的请求/响应结构（design.md 7.1、3.4.1.9、3.7.2）：集群概览与节点（`GET /api/v1/cluster`、
//! `POST /api/v1/nodes/<id>/{drain,remove,promote}`）、集群插件清单（`GET/POST /api/v1/cluster/plugins`）、各节点的元数据哈希
//! （`GET /api/v1/debug/meta-hash`，`wmfs debug meta-hash` 用）。M9：链（`GET /api/v1/chains`、`POST /api/v1/chains/<id>/force-serving`）。
//! 字段只增不改（ADR-0005）；JSON 金样在 `cluster/tests.rs`。

use serde::{Deserialize, Serialize};

/// `GET /api/v1/cluster/plugins` 与 `POST /api/v1/cluster/plugins` 的回复：集群插件清单（排好序的 `钩子:名字`，3.7.2）。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct PluginsReply {
    pub plugins: Vec<String>,
}

/// `POST /api/v1/cluster/plugins` 的请求：要加的与要删的（改完之后各节点需要用新配置重启）。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct PluginsReq {
    pub add: Vec<String>,
    pub remove: Vec<String>,
}

/// `GET /api/v1/cluster` 里的一个节点。`member` 是元数据 raft 里的实际成员身份（`voter` / `learner` / `none`），`member_wanted` 是
/// 指定的身份（NodeRecV1.meta_role：join `--meta auto` 是 learner，追上之后在 voter 不足 3 个时自动提升；`wmfs node promote` 指定为
/// voter）；`matched` 是 leader 看到的它的日志复制到了哪里，`caught_up` = 是成员、确认过数据（matched > 0）且 `matched + 1000 ≥ commit`
/// （3.4.1.9 的"追上"）。实现注记（M8 审查）：两个字段原名 `meta` / `meta_role`——与 health 里取值不同的 `meta_role` 同名，改了。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct NodeInfo {
    pub node_id: u32,
    pub addrs: Vec<(String, String)>,
    pub mgmt_addr: String,
    pub hostname: String,
    pub host_id: String,
    pub state: String, // "active" / "draining" / "removed"
    pub member: String,
    pub member_wanted: String,
    pub leader: bool,
    pub matched: u64,
    pub caught_up: bool,
}

/// `GET /api/v1/cluster`：集群概览——节点、元数据 raft 的 leader/term/commit/applied/成员，以及集群警告（`ClusterMap.warnings`）。
/// 实现注记（M8）：target、链、会话数随用到它们的里程碑加进来（只增）。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct ClusterReply {
    pub leader: u32,
    pub term: u64,
    pub commit: u64,
    pub applied: u64,
    pub voters: Vec<u32>,
    pub learners: Vec<u32>,
    pub voters_outgoing: Vec<u32>,
    pub nodes: Vec<NodeInfo>,
    pub warnings: Vec<String>,
}

/// `POST /api/v1/nodes/<id>/{drain,remove,promote}` 的回复：改完之后的这个节点与一句说明（成员变更由 cluster-mgr 接着做）。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct NodeOpReply {
    pub node: NodeInfo,
    pub message: String,
}

/// `GET /api/v1/debug/meta-hash`（每个节点都回答本节点的，不跟随 leader）：已应用到哪里、kv 表的条数与哈希（sha256 的前 16 字节，
/// 小写 hex）。各节点 `applied` 相同时 `hash` 必须相同（状态机是确定性的）。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct MetaHashReply {
    pub node_id: u32,
    pub applied: u64,
    pub state_version: u64,
    pub keys: u64,
    pub hash: String,
}

/// `GET /api/v1/chains` 里一条链的一个成员（链序）：`state` 是 `serving` 或 `syncing`。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct ChainMember {
    pub target: u32,
    pub node: u32,
    pub state: String,
}

/// 一条链（M9）：版本、incarnation、存储类、声明副本数、期望长度 `want`（= min(声明副本数, 这个存储类能用的节点数)，`wmfs dev wait
/// chains-serving` 等的就是"每条链都有 want 个成员且全部 Serving"）、成员、最后一个 Serving 成员被移除时记下的 `last_serving`、最近被
/// 移除的 target（rejoin_window 内回来可直接以 Syncing 追加）。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct ChainInfo {
    pub id: u32,
    pub ver: u64,
    pub incarnation: u32,
    pub class: String,
    pub replicas: u8,
    pub want: u8,
    pub members: Vec<ChainMember>,
    pub last_serving: Option<u32>,
    pub removed: Vec<u32>,
}

/// `GET /api/v1/chains`：全部链与集群警告。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct ChainsReply {
    pub chains: Vec<ChainInfo>,
    pub warnings: Vec<String>,
}

/// `POST /api/v1/chains/<id>/force-serving?target=` 的请求体：必须明确写 `{"i_accept_data_loss": true}`（7.1）。回复是改完的
/// [`ChainInfo`]。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct ForceServingReq {
    pub i_accept_data_loss: bool,
}

#[cfg(test)]
mod tests;
