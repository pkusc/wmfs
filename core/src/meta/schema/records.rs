//! 元数据 KV 的记录结构（design.md 3.2.8）与 raft 条目负载 `WriteBatchV1`（3.2.9），ADR-0020。
//!
//! 所有持久化记录 = 1 字节记录版本号 + postcard 编码的结构体（例外：`INLINE` 是原始字节），记录版本号均为 1。
//! 版本纪律（3.2.11）：**永不原地修改已发布的结构体**——改字段就新增 `XxxV2` 与新版本字节，解码器同时认识所有旧版本。
//! 文档里写了固定数值的字段（角色、种类、状态……）一律存 `u8`（postcard 编码 enum 用的是变体序号，0.2 第 11 条）。
//! 角色编码 `Role` 与时间戳 `Ts` 在 `base/types.rs`。

use std::collections::BTreeMap;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::base::ErrCode;
use crate::base::types::Ts;

/// 记录版本字节。
pub const REC_V1: u8 = 1;

/// 编码一条 V1 记录：`[1] ‖ postcard(rec)`。
pub fn encode_v1<T: Serialize>(rec: &T) -> Vec<u8> {
    postcard::to_extend(rec, vec![REC_V1]).unwrap_or_else(|e| panic!("不变量破坏：记录编码失败：{e}"))
}

/// 解码一条 V1 记录：版本字节不是 1、内容解不开或有多余字节 → `INTERNAL`（持久化状态损坏或格式漂移）。
pub fn decode_v1<T: DeserializeOwned>(b: &[u8]) -> Result<T, ErrCode> {
    match b.split_first() {
        Some((&REC_V1, body)) => match postcard::take_from_bytes::<T>(body) {
            Ok((v, [])) => Ok(v),
            _ => Err(ErrCode::INTERNAL),
        },
        _ => Err(ErrCode::INTERNAL),
    }
}

pub const KIND_FILE: u8 = 1;
pub const KIND_DIR: u8 = 2;
pub const KIND_SYMLINK: u8 = 3;
pub const KIND_FIFO: u8 = 4;
pub const KIND_SOCK: u8 = 5;
pub const KIND_CHR: u8 = 6;
pub const KIND_BLK: u8 = 7;
pub const F_INLINE: u32 = 1;
pub const F_ORPHAN: u32 = 2;
pub const F_HAS_ACL: u32 = 4;
pub const F_TRASH_DIR: u32 = 8;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct InodeV1 {
    pub kind: u8,
    pub mode: u32, // 权限位 + suid/sgid/sticky（不含类型位）
    pub uid: u32,
    pub gid: u32,
    pub nlink: u32,
    pub size: u64, // 文件：逻辑大小；目录：直接子项数（rmdir 判空）；符号链接：目标长度
    pub atime: Ts,
    pub mtime: Ts,
    pub ctime: Ts,
    pub btime: Ts,
    pub rdev: u32,
    pub parent: u64,                      // 主链接所在目录（递归计数归属于它）
    pub name: Vec<u8>,                    // 主链接的名字（拼路径用）
    pub extra_links: Vec<(u64, Vec<u8>)>, // 其余硬链接 (parent, name)
    pub flags: u32,
    pub change_id: u64,           // 数据或属性每变一次 +1
    pub class_id: u16,            // 文件数据实际所在的存储类；目录为 0
    pub layout: Option<LayoutV1>, // 只有普通文件有
    pub pending_epoch: u32,       // 迁移中的新 epoch（0 = 无）
    pub max_epoch: u32,           // 曾分配过的最大 epoch；新 epoch = max_epoch + 1
    pub trash_expire_ms: u64,     // 在回收站里 = 到期时间，否则 0
    pub symlink: Vec<u8>,         // 只有符号链接
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct LayoutV1 {
    pub epoch: u32, // fencing token
    pub chunk_size: u32,
    pub block_size: u32,
    pub chains: Vec<u32>, // 实际条带数 k = chains.len()（可用链不够时少于声明的 stripe，3.4.2.1）
    pub replicas: u8,     // 创建时声明的副本数
    pub codec: u8,        // 1 = replica
    pub checksum: u8,
    pub compress: u8,
    pub compress_level: i8,
    pub encrypt: u8,
    pub encrypt_key: u32,
    pub inline_max: u32, // 创建时的 inline 阈值
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DentryV1 {
    pub ino: u64,
    pub kind: u8,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct XattrsV1 {
    pub entries: Vec<(Vec<u8>, Vec<u8>)>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct AttrMapV1 {
    pub entries: BTreeMap<String, String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct RStatV1 {
    pub bytes: u64,
    pub files: u64,
    pub dirs: u64,
    pub per_class: Vec<(u16, u64, u64)>, // (class, bytes, files)，按 class 升序
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct QuotaStateV1 {
    pub soft_since_ms: u64,
}

pub const LOCK_EOF: u64 = i64::MAX as u64; // "到文件尾"：与内核 OFFSET_MAX 一致（3.5.3）

/// [start, end] 闭区间；end ≤ LOCK_EOF
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct LockRecV1 {
    pub session: u64,
    pub owner: u64,
    pub pid: u32,
    pub start: u64,
    pub end: u64,
    pub write: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct LockTableV1 {
    pub locks: Vec<LockRecV1>, // 按 (start, session, owner) 排序
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct SessionRecV1 {
    pub client_name: String,
    pub host: String,
    pub host_id: String,
    pub mount: String,
    pub key_id: u32,
    pub tenant: u32,
    pub created_ms: u64,
    pub locked_inos: Vec<u64>, // 本会话持有锁的 inode（释放/驱逐时点操作，不扫描）
    pub state: u8,             // 1 Active，2 Evicting（第一阶段已完成，3.4.4）
    pub evict_since_ms: u64,
    pub evict_map_epoch: u64,
}

/// code 0 = 成功（reply 为 postcard(MetaReply)），否则 POSIX errno
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DedupV1 {
    pub code: u16,
    pub reply: Vec<u8>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct GcRecV1 {
    pub due_ms: u64,
    pub chains: Vec<u32>,
    pub created_ms: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ChangeRecV1 {
    pub time_ms: u64,
    pub op: u16,
    pub result: u16,
    pub trace_id: u64,
    pub uid: u32,
    pub gid: u32,
    pub tenant: u32,
    pub session: u64,
    pub job_id: u64,
    pub ino: u64,
    pub parent: u64,
    pub name: Vec<u8>,
    pub new_parent: u64,
    pub new_name: Vec<u8>,
    pub size: u64,
    pub extra: Vec<(String, String)>,
    pub audited: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct NodeRecV1 {
    pub addrs: Vec<(String, String)>, // (scheme, 地址)，例如 ("tcp", "127.0.0.1:19100")、("rdma", "10.77.0.1:19101")；来自各传输插件 listen() 的返回值
    pub mgmt_addr: String,
    pub host_id: String,
    pub machine_id: String,
    pub hostname: String,
    pub meta_role: u8, // 0 不参与，1 learner，2 voter
    pub features: Vec<String>,
    pub joined_ms: u64,
    pub state: u8, // 1 Active，2 Draining，3 Removed
}

/// `NodeRecV1.meta_role`（指定的元数据角色，3.4.1.9）与 `NodeRecV1.state`。
pub const META_NONE: u8 = 0;
pub const META_LEARNER: u8 = 1;
pub const META_VOTER: u8 = 2;
pub const NODE_ACTIVE: u8 = 1;
pub const NODE_DRAINING: u8 = 2;
pub const NODE_REMOVED: u8 = 3;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct TargetRecV1 {
    pub node: u32,
    pub path: String,
    pub labels: Vec<String>,
    pub phys_devs: Vec<String>, // target 所在的物理整盘/namespace（例如 "nvme0n1"，6.3）；空 = unknown（loop、解析失败），一律按共享设备对待
    pub capacity: u64,
    pub boot_id: u64,
    pub state: u8, // 1 Up，2 Offline，3 Draining，4 Removed
    pub last_up_ms: u64,
    pub read_only: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ChainRecV1 {
    pub ver: u64,         // 任何成员/状态变化 +1；数据请求携带它作为 fencing
    pub incarnation: u32, // 唯一幸存者重启/强制 serving 时 +1（3.4.2.7）；从 1 开始
    pub class_id: u16,
    pub replicas: u8,              // 期望长度
    pub members: Vec<(u32, u8)>,   // (target, 1 Serving | 2 Syncing)；顺序即链序；不变式 I0
    pub last_serving: Option<u32>, // 最后一个 Serving 成员被移除时记下它
    pub removed: Vec<(u32, u64)>,  // 最近被移除的 (target, removed_ms)，rejoin_window 内回来可直接以 Syncing 追加
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct StorageClassV1 {
    pub id: u16,
    pub name: String,
    pub label: String,
    pub dedicated: bool,
    pub replica: u8,
    pub iops_min: u32,
    pub iops_max: u32,
    pub bw_min: u64,
    pub bw_max: u64,
    pub burst_ms: u32,
    pub weight: u32,
    pub latency_target_us: u32,
    pub priority: u8,       // 1 normal，2 low（label 为 hdd 的类默认 low，4.5、6.3）
    pub failure_domain: u8, // 首发只有 1 = node
    pub locality: u8,       // 0 any，1 prefer-client
    pub tier_to: Option<String>,
    pub tier_when: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ClassIdV1 {
    pub id: u16,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct TaskRecV1 {
    pub state: u8,       // 1 Running 2 Paused 3 Done 4 Failed
    pub cursor: Vec<u8>, // 对 core 不透明；每步之后持久化 ⇒ 断点续跑
    pub pass: u64,
    pub done: u64,
    pub total_hint: u64,
    pub backlog: u64,
    pub bytes_per_sec: u64,
    pub items_per_sec: u64,
    pub updated_ms: u64,
    pub last_error: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct KeyMetaV1 {
    pub name: String,
    pub role: u8, // Role::as_u8()：1 admin，2 client（KEYMETA 里不会出现 3 server，server 只用 key 0）
    pub tenant: u32,
    pub uid_ranges: Vec<(u32, u32)>,
    pub root_squash: bool,
    pub path_caps: Vec<(u64, bool)>, // (允许的子树根 ino, 只读?)；空 = 不限
    pub revoked: bool,
    pub created_ms: u64,
}

/// wrapped = nonce(12) ‖ 密文(32) ‖ tag(16)
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DataKeyV1 {
    pub name: String,
    pub tenant: u32,
    pub wrapped: Vec<u8>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DataKeyIdV1 {
    pub id: u32,
}

/// 集群级数值；0 = 不设
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct QosSpecV1 {
    pub r: u32,
    pub w: u32,
    pub l: u32,
    pub bw_max: u64,
    pub burst_ms: u32,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ReconcileItemV1 {
    pub dir: u64,
    pub reason: u8, // 1 属性变化，2 目录 rename，3 存储类变化
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CursorV1 {
    pub seq: u64,
    pub updated_ms: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct PluginListV1 {
    pub plugins: Vec<String>, // 排好序的 "钩子:名字"，例如 "authz:posix-mode"（只比名字，3.7.2）
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct EvictedV1 {
    pub evicted_ms: u64,
    pub map_epoch: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ClusterInfoV1 {
    pub cluster_id: [u8; 16],
    pub created_ms: u64,
    pub format: u16,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct FeatureSetV1 {
    pub compat: Vec<String>,
    pub ro_compat: Vec<String>,
    pub incompat: Vec<String>,
}

/// 集群运行参数：必须全集群一致（租约安全依赖它），所以存在复制状态里
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ClusterSettingsV1 {
    pub session_lease_ms: u64,  // 30_000
    pub target_lease_ms: u64,   // 8_000
    pub cap_ttl_ms: u64,        // 600_000
    pub repair_delay_ms: u64,   // 1_800_000
    pub rejoin_window_ms: u64,  // 1_800_000
    pub epoch_gc_grace_ms: u64, // 60_000
    pub bg_bytes_per_sec: u64,  // 200 MiB
}

impl Default for ClusterSettingsV1 {
    /// init 写入的默认值（3.2.8 注释里的数值）。
    fn default() -> ClusterSettingsV1 {
        ClusterSettingsV1 {
            session_lease_ms: 30_000,
            target_lease_ms: 8_000,
            cap_ttl_ms: 600_000,
            repair_delay_ms: 1_800_000,
            rejoin_window_ms: 1_800_000,
            epoch_gc_grace_ms: 60_000,
            bg_bytes_per_sec: 200 << 20,
        }
    }
}

// ---- 3.2.9 raft 条目负载 WriteBatchV1（契约）----
// EntryNormal：data 为空 = 新 leader 的空条目（no-op）；否则 data = [0x01] ‖ postcard(WriteBatchV1)；context 为空
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct WriteBatchV1 {
    pub expect_version: u64, // 执行器构建批次时读到的 state_version（3.4.1.5）
    pub leader_term: u64,    // 构建时的 term（提议前校验；apply 时只作诊断）
    pub ops: Vec<KvOp>,      // 已按 key 排序、同 key 只留最后一次
    pub trace_ids: Vec<u64>, // 诊断用
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum KvOp {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}
// EntryConfChangeV2：raft-rs 自己的 protobuf；wmfs 只提议 V2，从不提议 v1 EntryConfChange
// follower 只做"按顺序应用 ops"——没有业务逻辑，所以不同软件版本的节点可以混跑（滚动升级安全），只要 KV 格式不变。

impl WriteBatchV1 {
    /// raft 条目的 data：`[0x01] ‖ postcard(WriteBatchV1)`。
    pub fn encode(&self) -> Vec<u8> {
        encode_v1(self)
    }
    pub fn decode(data: &[u8]) -> Result<WriteBatchV1, ErrCode> {
        decode_v1(data)
    }
}
