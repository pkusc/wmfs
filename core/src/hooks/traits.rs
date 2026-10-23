//! 各钩子的 Rust trait（design.md 3.7.3；`QueueStat`/`StoreStat` 见 3.12），ADR-0025。
//!
//! 全部对象安全，跨模块一律用 `Arc<dyn …>` / `Box<dyn …>`。热路径插件不得阻塞、不得做大块分配、
//! 不得发起网络请求（transport 除外）；每次调用都经过 core 的包装（fail point、计时、预算，3.7.4）。
//! 本体对 `Conn` 的包装 `ConnHandle` 在 `net/conn.rs`；meta_engine 见 `meta/engine/txn.rs` 的 `MetaEngine`；
//! bg_task 见 `bg/task.rs` 的 `BgTask`；event_sink / mgmt_ext 是进程外的，trait 在 wmfs-mgr（`dashboard/src/host.rs`）。
//!
//! 接口骨架（M0）：本文件是这些 trait 的唯一真源；任何变化都要 `CORE_ABI_VERSION + 1` 并写 ADR。

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::base::ErrCode;
use crate::base::types::{ChunkId, Principal, Role};
use crate::buf::{Buf, MemPool, RegionInfo};
use crate::hooks::manifest::Manifest;
use crate::meta::attrs::EffectiveAttrs;
use crate::meta::schema::records::{KeyMetaV1, RStatV1, StorageClassV1};
use crate::proto::frame::{InFrame, OutFrame};

// ---- transport（3.6.4）----
pub trait Transport: Send + Sync {
    fn manifest(&self) -> &'static Manifest;
    fn scheme(&self) -> &'static str; // "tcp" | "rdma" | "mem"……core 只把它当字符串比较
    fn register_region(&self, region: &RegionInfo) -> Result<(), ErrCode>; // 启动时一次
    /// 按插件自己的部署配置（[plugin.<name>] listen = …，构造时传入）开始监听；
    /// 返回要写进 NodeRecV1.addrs 的地址（不带 scheme）；None = 本节点不监听这种传输（例如客户端）
    fn listen(
        &self,
        pool: MemPool,
        on_conn: Arc<dyn Fn(Box<dyn Conn>) + Send + Sync>,
    ) -> Result<Option<String>, ErrCode>;
    /// 只建立"能收发帧"的通道；3.3.2 握手由 core 在返回的 Conn 上进行（与传输无关）
    fn connect(&self, addr: &str, pool: MemPool, timeout: Duration) -> Result<Box<dyn Conn>, ErrCode>;
}

pub trait Conn: Send + Sync {
    fn send(&self, f: OutFrame) -> Result<(), ErrCode>; // 只由 core 为该连接起的写线程调用（阻塞；TCP 写超时 5 s）；bulk / recv_into 的所有权交给传输层
    fn recv(&self) -> Result<InFrame, ErrCode>; // 只由该连接的读线程调用（阻塞）
    fn mark_authenticated(&self) {} // core 完成 3.3.2 握手后调用；单边传输在此之前拒绝执行任何 READ/WRITE
    fn peer(&self) -> String;
    fn close(&self); // 任意线程可调用；之后 send/recv 立即返回错误
}

// ---- authn：连接建立时 ----
pub struct AuthnInput<'a> {
    pub key_id: u32,
    pub role: Role, // Hello.role 已由 core 转成 Role；非法值直接 AUTH_FAILED
    pub cluster_id: [u8; 16],
    pub client_nonce: [u8; 16],
    pub server_nonce: [u8; 16],
    pub proof: &'a [u8; 32],
    pub peer: &'a str,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConnIdentity {
    pub key_id: u32,
    pub role: Role,
    pub tenant: u32,
    pub uid_ranges: Vec<(u32, u32)>,
    pub root_squash: bool,
    pub path_caps: Vec<(u64, bool)>,
}

pub enum AuthnResult {
    Accept(ConnIdentity),
    Reject,
    Abstain,
}

pub trait KeyLookup {
    fn secret(&self, key_id: u32) -> Option<[u8; 32]>;
    fn meta(&self, key_id: u32) -> Option<KeyMetaV1>;
}

pub trait AuthnPlugin: Send + Sync {
    fn manifest(&self) -> &'static Manifest;
    fn authenticate(&self, input: &AuthnInput, keys: &dyn KeyLookup) -> AuthnResult;
    fn server_proof(&self, input: &AuthnInput, keys: &dyn KeyLookup) -> Option<[u8; 32]>;
}

// ---- authz：每个元数据请求（数据请求凭 cap，3.3.5）----
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthzOp {
    Lookup,
    GetAttr,
    ReadDir,
    ReadLink,
    Open { read: bool, write: bool },
    Create,
    Mkdir,
    Mknod,
    Symlink,
    Link,
    Unlink,
    Rmdir,
    RenameFrom,
    RenameTo,
    SetAttr { mode: bool, owner: bool, size: bool, times: bool, times_now: bool },
    GetXattr,
    SetXattr,
    ListXattr,
    RemoveXattr,
    Lock,
    Admin,
}

pub struct InodeView<'a> {
    pub ino: u64,
    pub kind: u8,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub acl_access: Option<&'a [u8]>,
}

pub struct AuthzInput<'a> {
    pub who: &'a Principal, // uid（squash 后）、gid、groups、tenant、job_id、ConnIdentity
    pub op: AuthzOp,
    pub target: Option<&'a InodeView<'a>>, // 被访问对象
    pub parent: Option<&'a InodeView<'a>>, // 目录项操作的父目录
    pub victim: Option<&'a InodeView<'a>>, // unlink/rename 被删对象（粘滞位）
    pub ancestors: &'a [u64],              // 根 → 父（core 缓存；path-caps 用）
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decision {
    Allow,
    Deny(ErrCode),
    Abstain,
}

pub trait AuthzPlugin: Send + Sync {
    fn manifest(&self) -> &'static Manifest;
    fn check(&self, input: &AuthzInput) -> Decision;
}

// ---- quota_admission：分配空间或创建对象前（执行器闭包内）----
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QuotaScope {
    pub dir: u64,
    pub class: Option<u16>, // None = 总量配额
    pub hard_bytes: u64,
    pub soft_bytes: u64,
    pub grace_ms: u64,
    pub hard_files: u64, // 0 = 不限
    pub quantum: u64,
    pub used: RStatV1,
    pub soft_since_ms: u64,
}

pub struct QuotaInput<'a> {
    pub who: &'a Principal,
    pub scopes: &'a [QuotaScope],
    pub delta_bytes: i64,
    pub delta_files: i64,
    pub class_id: u16,
    pub now_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QuotaDecision {
    Allow { max_growth: u64 },
    SoftWarn { dir: u64, max_growth: u64 },
    Deny { dir: u64, code: ErrCode },
}

pub trait QuotaPlugin: Send + Sync {
    fn manifest(&self) -> &'static Manifest;
    fn admit(&self, input: &QuotaInput) -> QuotaDecision; // 纯函数：状态由 core 提供，soft_since 由 core 写回
}

// ---- qos：请求进入执行队列（数据节点）----
#[derive(Clone, Copy, Hash, PartialEq, Eq, Debug)]
pub struct QueueKey {
    pub tenant: u32,
    pub qos_id: u64,
    pub class_id: u16,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QosItem {
    pub key: QueueKey,
    pub job_id: u64,
    pub cost: u32,
    pub bytes: u32,
    pub background: bool,
    pub token: u64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct QosSpec {
    pub r: f64, // 本节点份额；0 = 不设
    pub w: f64,
    pub l: f64,
    pub bw_max: f64,
    pub burst_ms: u32,
    pub low_priority: bool, // 来自存储类 priority = low（4.5）
}

pub trait QosPlugin: Send + Sync {
    fn manifest(&self) -> &'static Manifest;
    fn enqueue(&self, item: QosItem, spec: QosSpec); // 不得阻塞；token → 请求的映射表由 core 维护
    /// 非阻塞。Ok(token) = 立即执行；Err(Some(t)) = 最早 t 时刻再问；Err(None) = 队列全空。睡眠与唤醒由 core 负责（4.5）
    fn dequeue(&self, now: Instant) -> Result<u64, Option<Instant>>;
    fn stats(&self) -> Vec<QueueStat>; // 含每队列的保底欠额（"声明 vs 实际"与心跳 qos_deficit 用）
}

/// 每队列的统计（3.12）。
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct QueueStat {
    pub key: QueueKey,
    pub depth: u32,
    pub served_per_s: f64,
    pub deficit_per_s: f64,
    pub limited_total: u64,
}

// ---- placement：create 时与迁移时 ----
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChainView {
    pub id: u32,
    pub class_id: u16,
    pub replicas: u8,
    pub serving: u8,
    pub members: Vec<(u32, u32)>, // (target, node)
    pub used: u64,
    pub capacity: u64,
}

pub struct PlacementInput<'a> {
    pub eff: &'a EffectiveAttrs,
    pub class: &'a StorageClassV1,
    pub replicas: u8,
    pub stripe: u16,
    pub client_node: Option<u32>,
    pub seed: u64,
    pub exclude: &'a [u32],
    pub size_hint: u64, // 理念文档 4.1 的"大小提示"：create 与截断到 0 时为 0，迁移时为文件大小
}

/// 插件只排序；core 取排序后前 k = min(stripe, 互不相同的链数) 条；k = 0 → ENOSPC（3.4.2.1）。
pub trait PlacementPlugin: Send + Sync {
    fn manifest(&self) -> &'static Manifest;
    fn rank(&self, input: &PlacementInput, candidates: &mut Vec<ChainView>) -> Result<(), ErrCode>;
}

// ---- codec：写入分片 / 读取重组 ----
/// 链上每个成员存什么。首发只有 `Whole`；二期 ec 会加 `Data { index: u8, k: u8, m: u8 }`。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShardSpec {
    Whole,
}

/// `LayoutV1.codec` 的编号：1 = replica（多副本；cap 里没有 codec，首发只有这一种）。实现注记（M5）：新增（只增）。
pub const CODEC_REPLICA: u8 = 1;

pub trait CodecPlugin: Send + Sync {
    fn manifest(&self) -> &'static Manifest;
    fn codec_id(&self) -> u8; // 1 = replica（CODEC_REPLICA）
    fn shards(&self, chain_len: usize) -> Vec<ShardSpec>; // 链上每个成员存什么
    fn read_order(&self, serving: &[(u32, u32)], prefer_node: Option<u32>, chunk: &ChunkId) -> Vec<u32>; // 读副本的尝试顺序
}

// ---- transform：字节 ↔ 字节（每个 64 KiB 块）----
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TransformKind {
    Checksum,
    Compress,
    Encrypt,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct BlockMeta {
    pub checksum: [u8; 32],
    pub flags: u8,
    pub nonce: [u8; 12],
    pub tag: [u8; 16],
    pub plain_len: u32,
}

pub struct TransformCtx<'a> {
    pub chunk: &'a ChunkId,
    pub block_idx: u32,
    pub level: i8,
    pub key_id: u32,
    pub key: Option<&'a [u8; 32]>,
    pub aad: &'a [u8],
}

pub trait TransformPlugin: Send + Sync {
    fn manifest(&self) -> &'static Manifest;
    fn kind(&self) -> TransformKind;
    fn alg_id(&self) -> u8; // 写进 chunk 头
    fn encode(&self, ctx: &TransformCtx, input: Buf, meta: &mut BlockMeta, pool: &MemPool) -> Result<Buf, ErrCode>;
    fn decode(&self, ctx: &TransformCtx, input: Buf, meta: &BlockMeta, pool: &MemPool) -> Result<Buf, ErrCode>;
}

// ---- chunk_store：最内层，只存字节；chunk 格式由 core 定义并编解码（3.2.4），插件不解析头部 ----
pub trait ChunkStore: Send + Sync {
    fn manifest(&self) -> &'static Manifest;
    fn open_target(&self, root: &Path, target_id: u32) -> Result<Arc<dyn TargetStore>, ErrCode>;
}

#[allow(clippy::type_complexity)] // list_epochs 的返回类型按 design.md 原样
pub trait TargetStore: Send + Sync {
    fn read_at(&self, id: &ChunkId, off: u64, buf: &mut [u8]) -> Result<Option<usize>, ErrCode>; // 已提交版本；不存在 → None
    fn open_committed(&self, id: &ChunkId) -> Result<Option<Box<dyn ChunkReader>>, ErrCode>; // 句柄在之后的 rename 中仍指向原版本
    fn write_pending(&self, id: &ChunkId, writes: &[(u64, &[u8])], copies: &[(u64, u64)]) -> Result<(), ErrCode>; // R1
    fn read_pending_at(&self, id: &ChunkId, off: u64, buf: &mut [u8]) -> Result<usize, ErrCode>; // 打包 pending 版本
    fn commit_pending(&self, id: &ChunkId, sync: bool) -> Result<(), ErrCode>; // rename；新目录时 fsync 父目录
    fn abort_pending(&self, id: &ChunkId) -> Result<(), ErrCode>;
    fn install(&self, id: &ChunkId, writes: &[(u64, &[u8])]) -> Result<(), ErrCode>; // R3
    fn sync(&self, ids: &[ChunkId]) -> Result<(), ErrCode>; // R2
    fn delete_epoch(&self, chain: u32, ino: u64, epoch: u32) -> Result<u64, ErrCode>;
    fn delete_chunk(&self, id: &ChunkId) -> Result<(), ErrCode>; // 只给同步与 scrub 用
    fn list_chain(&self, chain: u32, after: Option<&ChunkId>, limit: usize) -> Result<Vec<ChunkId>, ErrCode>; // 升序
    fn list_epochs(&self, after: Option<(u32, u64, u32)>, limit: usize) -> Result<Vec<(u32, u64, u32, u64)>, ErrCode>; // (chain, ino, epoch, 目录 mtime ms)
    fn delete_chain_dir(&self, chain: u32) -> Result<(), ErrCode>;
    fn clear_temp(&self) -> Result<(), ErrCode>; // 启动：清空 p/、t/（R5）
    fn statfs(&self) -> Result<StoreStat, ErrCode>; // 容量、已用（物理盘由 core 的 devinfo 从目录的 st_dev 解析，6.3）
}

#[allow(clippy::len_without_is_empty)] // 按 design.md 原样
pub trait ChunkReader: Send {
    fn read_at(&mut self, off: u64, buf: &mut [u8]) -> Result<usize, ErrCode>;
    fn len(&self) -> u64;
}

/// target 的容量与已用（3.12）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StoreStat {
    pub capacity: u64,
    pub used: u64,
}

// ---- meta_engine：见 3.5.1 的 MetaEngine（meta/engine/txn.rs）----

// ---- cache_store：客户端 ----
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CacheKey {
    pub ino: u64,
    pub change_id: u64,
    pub block: u64, // 文件内 1 MiB 块号
}

pub trait CacheStore: Send + Sync {
    fn manifest(&self) -> &'static Manifest;
    fn get(&self, key: &CacheKey, into: &mut [u8]) -> Option<usize>;
    fn put(&self, key: &CacheKey, data: &[u8]);
    fn invalidate_ino(&self, ino: u64);
    /// 二期 passthrough 预留：完整缓存了该文件某个 change_id 的明文时，返回本地文件路径
    fn local_file(&self, _ino: u64, _change_id: u64) -> Option<std::path::PathBuf> {
        None
    }
}

// ---- bg_task：见 3.10 的 BgTask（bg/task.rs）----

// ---- event_sink / mgmt_ext：进程外，trait 定义在 wmfs-mgr crate（dashboard/src/host.rs），只能用管理 API 客户端 ----

#[cfg(test)]
mod tests {
    use super::*;

    /// 编译期检查（design.md 0.2 第 3 条）：trait 必须对象安全。下面这几个在 M0 里还没被别处以 dyn 形式用过
    /// （其余的已经出现在 `Arc<dyn …>` / `&dyn …` 签名里，编译器会检查）；以后给它们加了泛型方法之类的东西，
    /// 这里会立刻编译失败，而不是等到某个里程碑真去用 `Arc<dyn …>` 时才发现。
    #[allow(dead_code)]
    fn hook_traits_are_object_safe(
        _: &dyn AuthnPlugin,
        _: &dyn AuthzPlugin,
        _: &dyn QuotaPlugin,
        _: &dyn PlacementPlugin,
        _: &dyn ChunkStore,
        _: &dyn CacheStore,
    ) {
    }
}
