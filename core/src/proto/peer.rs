//! 服务端 ↔ 服务端（design.md 3.3.6）：`PeerOp`（数据节点之间，0x0300）、`NodeOp`（节点 ↔ 元数据 leader，0x0400）、
//! 集群图 `ClusterMap`，以及快照流 `SnapshotFetch`（0x0501）的消息（3.4.1.7）。
//!
//! `GetClusterMap` 在 `since_epoch < map_epoch` 时返回完整集群图（小集群，几十 KB），否则 `Ok`。
//! 集群图在 leader 上由已提交状态构建并按 `map_epoch` 缓存；**任何**改变节点、target、链、存储类、key、
//! 数据密钥、QoS 表、被驱逐列表、集群参数或插件清单的批次都在同一批次里 `map_epoch += 1`。
//! `warnings` 在构建集群图时顺带计算（不进 KV）。client 角色只能拿到过滤后的集群图（不含 keys、datakeys）。

use serde::{Deserialize, Serialize};

use crate::base::types::ChunkId;
use crate::meta::schema::records::{
    ChainRecV1, ClusterSettingsV1, DataKeyV1, EvictedV1, FeatureSetV1, KeyMetaV1, NodeRecV1, PluginListV1, QosSpecV1,
    StorageClassV1, TargetRecV1,
};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum PeerOp {
    ForwardWrite(ForwardWrite),
    ForwardSync { chain: u32, chain_ver: u64, from_target: u32, chunks: Vec<ChunkId> },
    ForwardDeleteEpoch { chain: u32, chain_ver: u64, from_target: u32, ino: u64, epoch: u32 },
    ListChunks { chain: u32, after: Option<ChunkId>, limit: u32 }, // 同步源列出已提交版本
    FetchChunk { chunk: ChunkId },                                 // 响应 bulk = packed chunk（3.2.4）
}

impl PeerOp {
    /// 变体名：指标里的 `op` 标签（3.3.3）。
    pub fn name(&self) -> &'static str {
        match self {
            PeerOp::ForwardWrite(_) => "ForwardWrite",
            PeerOp::ForwardSync { .. } => "ForwardSync",
            PeerOp::ForwardDeleteEpoch { .. } => "ForwardDeleteEpoch",
            PeerOp::ListChunks { .. } => "ListChunks",
            PeerOp::FetchChunk { .. } => "FetchChunk",
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ForwardWrite {
    pub chunk: ChunkId,
    pub chain_ver: u64,
    pub from_target: u32,
    pub base_version: u64, // 增量转发的基础版本；0 = chunk 原本不存在
    pub version: u64,
    pub full: bool,                  // true：bulk 是完整 packed chunk，忽略 base_version
    pub header: Vec<u8>,             // 新版本的 256 字节固定头（含 header_crc，覆盖新块表）
    pub blocks: Vec<(u32, Vec<u8>)>, // 改动块的 (块号, 72 字节块表条目)；bulk = 这些块的存储字节按此顺序拼接
    pub sync: bool,                  // O_SYNC：每个成员提交前先 fdatasync
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum PeerReply {
    Ack { version: u64 },
    NeedFull,
    Chunks { items: Vec<(ChunkId, u64)>, more: bool },
    Fetched { version: u64 },
    Ok,
    Deleted { files: u64 },
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum NodeOp {
    Heartbeat(NodeHeartbeat),
    GetClusterMap { since_epoch: u64 },
    Join(JoinReq),
    SyncDone { target: u32, chain: u32, chain_ver: u64 },
    CheckEpochs { items: Vec<(u64, u32)> }, // 孤儿扫描：(ino, epoch) 是否已死
    ReportCorrupt { target: u32, chunk: ChunkId },
}

impl NodeOp {
    /// 变体名：指标里的 `op` 标签（3.3.3）。
    pub fn name(&self) -> &'static str {
        match self {
            NodeOp::Heartbeat(_) => "Heartbeat",
            NodeOp::GetClusterMap { .. } => "GetClusterMap",
            NodeOp::Join(_) => "Join",
            NodeOp::SyncDone { .. } => "SyncDone",
            NodeOp::CheckEpochs { .. } => "CheckEpochs",
            NodeOp::ReportCorrupt { .. } => "ReportCorrupt",
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct NodeHeartbeat {
    pub node: u32,
    pub boot_id: u64, // boot_id：每次进程启动随机生成
    pub host_id: String,
    pub machine_id: String,
    pub map_epoch_seen: u64,
    pub targets: Vec<TargetReport>,
    pub forward_failures: Vec<(u32, u32, u64)>, // (chain, 下游 target, 持续失败起始 ms)
    pub features: Vec<String>,
    pub wall_ms: u64,
    pub qos_deficit: Vec<(u64, u32)>, // (qos_id, 最近 10 s 平均保底欠额，成本单位/秒)；只报非 0 的，给"声明 vs 实际"用（3.10）
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct TargetReport {
    pub target: u32,
    pub used: u64,
    pub capacity: u64,
    pub phys_devs: Vec<String>,
    pub read_only: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct JoinReq {
    pub hostname: String,
    pub host_id: String,
    pub machine_id: String,
    pub addrs: Vec<(String, String)>, // 同 NodeRecV1.addrs
    pub mgmt_addr: String,
    pub meta_role: u8,                                         // 0 none 1 auto
    pub targets: Vec<(String, Vec<String>, u64, Vec<String>)>, // (path, labels, capacity, phys_devs)
    pub features: Vec<String>,
    pub plugins: Vec<String>, // 本节点的插件清单；与 GLOBAL plugins 不同 → 拒绝加入并列出差异
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum NodeReply {
    HeartbeatOk { lease_ms: u64, map_epoch: u64, leader_wall_ms: u64 },
    Map(Box<ClusterMap>),
    Joined { node_id: u32, target_ids: Vec<u32>, map: Box<ClusterMap> }, // 带上当前集群图：新节点的 KV 是空的，靠它找到 leader 与快照源
    Ok,
    Epochs { dead: Vec<bool> },
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct ClusterMap {
    pub map_epoch: u64,
    pub cluster_id: [u8; 16],
    pub leader: Option<u32>,
    pub settings: ClusterSettingsV1,
    pub features: FeatureSetV1,
    pub nodes: Vec<(u32, NodeRecV1)>,
    pub targets: Vec<(u32, TargetRecV1)>,
    pub chains: Vec<(u32, ChainRecV1)>,
    pub classes: Vec<StorageClassV1>,
    pub keys: Vec<(u32, KeyMetaV1)>,
    pub datakeys: Vec<(u32, DataKeyV1)>,
    pub qos: Vec<(u64, QosSpecV1)>,
    pub evicted: Vec<(u64, EvictedV1)>,
    pub plugins: PluginListV1, // 集群插件清单（3.7.2）
    pub warnings: Vec<String>, // leader 由已提交状态算出的集群警告（中文）：短链、故障域不足、故障域重叠、2 voter 无高可用……
}

// ---- SnapshotFetch（0x0501，3.4.1.7）：follower 向快照源要快照，leader 用 StreamChunk 帧流式返回 ----

/// raft `Snapshot.data` 的内容：`postcard(SnapDesc)`——快照内容不进 raft 消息，follower 凭它去 `source_node` 下载。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct SnapDesc {
    pub index: u64,
    pub term: u64,
    pub source_node: u32,
}

/// 请求 body：要哪一个快照。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct SnapshotFetch {
    pub index: u64,
    pub term: u64,
}

/// 首帧 body；中间帧 body = `Vec<(Vec<u8>, Vec<u8>)>`（每帧 ≤ 1 MiB）；末帧置 LAST_CHUNK、body = `SnapTrailer`。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct SnapHeader {
    pub index: u64,
    pub term: u64,
    pub state_version: u64,
    pub conf_state: Vec<u8>, // protobuf ConfState
    pub pairs_total: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct SnapTrailer {
    pub pairs: u64,
    pub crc32c: u32,
}
