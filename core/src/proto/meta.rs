//! 客户端 ↔ 元数据：`MetaCall` / `MetaOp` / `MetaReply`（design.md 3.3.4，msg_type 0x0100）。
//!
//! 所有客户端→元数据请求以公共头 `ReqHead` 开始。`seq` 由客户端为每个**修改型**请求递增（只读请求 `seq = 0`）；
//! 每个会话同时在途的修改型请求不超过 [`DEDUP_WINDOW`] = 256 个。
//! "R*"：会话心跳走只读路径（必须先过 ReadIndex，确认自己仍是 leader 才续租），并更新 leader 内存里的
//! `last_seen` 与打开文件集合（3.5.6）。
//!
//! postcard 编码 enum 用的是变体序号：**变体顺序就是线格式**，新变体只能加在末尾（3.2.11）。
//! 实现注记（M5）：`MetaOp::Uninline { ino }` 按 ADR-0034 追加在末尾（金样 `tests/golden/meta.golden` 的 `MetaOp.Uninline`）：
//! inline 小文件这次会长过 `inline_max` 时，客户端请服务端把旧内容搬进 chunk（只有写权限的人也能把小文件写大，
//! `GetInline` 仍只给能读的人）。

use serde::{Deserialize, Serialize};

use crate::base::types::Ts;
use crate::meta::schema::records::LayoutV1;

/// 每个会话同时在途的修改型请求上限（3.5.2）。
pub const DEDUP_WINDOW: u64 = 256;

// `Rename.flags` 与 `SetXattr.flags` 原样沿用 Linux 的数值（renameat2(2)、setxattr(2)）；实现注记（M4）：规格没写数值，
// 取 Linux 的，钉在 tests/golden/constants.golden。RENAME_WHITEOUT 不支持（EINVAL，3.1.1）。
pub const RENAME_NOREPLACE: u32 = 1;
pub const RENAME_EXCHANGE: u32 = 2;
pub const RENAME_WHITEOUT: u32 = 4;
pub const XATTR_CREATE: u32 = 1;
pub const XATTR_REPLACE: u32 = 2;
// `Open.flags` 的访问方式（O_ACCMODE 那两位）：原样沿用 Linux open(2) 的数值。实现注记（M5）：新增（只增），钉在
// tests/golden/constants.golden。
pub const O_RDONLY: u32 = 0;
pub const O_WRONLY: u32 = 1;
pub const O_RDWR: u32 = 2;
pub const O_ACCMODE: u32 = 3;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct ReqHead {
    pub session: u64,
    pub seq: u64,
    pub uid: u32,
    pub gid: u32,
    pub groups: Vec<u32>,
    pub pid: u32,
    pub job_id: u64,
}

/// 0x0100 的 body：`MetaCall { head: ReqHead, op: MetaOp }` → `MetaReply`（3.3.3）。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct MetaCall {
    pub head: ReqHead,
    pub op: MetaOp,
}

// 排版保持与 design.md 3.3.4 相同：一行一个操作（rustfmt 会因为 CommitWrite 太长而把所有变体都拆成多行）。
#[rustfmt::skip]
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum MetaOp {
    // ---- 会话 ----
    SessionOpen { client_name: String, host: String, host_id: String, mount: String, version: String }, // W；session 由客户端随机生成，按 session 幂等
    SessionHeartbeat { opened: Vec<(u64, bool)>, closed: Vec<u64>, full: Option<Vec<(u64, bool)>> },    // R*；(ino, 是否写打开)
    SessionClose,                                                                                    // W
    // ---- 只读（R：ReadIndex 后在 meta-read 线程执行）----
    Lookup { parent: u64, name: Vec<u8> },
    GetAttr { ino: u64 },
    ReadDir { ino: u64, after: Vec<u8>, limit: u32, plus: bool },        // 按名字分页；after 为空 = 从头；limit ≤ 1024，服务端还保证回复编码 ≤ 512 KiB（先到先停，eof = false）
    ReadLink { ino: u64 },
    GetXattr { ino: u64, name: Vec<u8> },
    ListXattr { ino: u64 },
    StatFs,
    Open { ino: u64, flags: u32 },                                       // 不写 raft（flags 里不会有 O_TRUNC：内核随后另发 SetAttr{size: 0}，8.1）
    GetInline { ino: u64 },
    GetLock { ino: u64, owner: u64, start: u64, end: u64, write: bool },
    RenewCap { ino: u64, write: bool },
    ResolvePath { path: Vec<u8> },                                       // CLI / 调试客户端
    // ---- 修改（W：进执行器、走 raft）----
    Create { parent: u64, name: Vec<u8>, mode: u32, umask: u32, excl: bool },
    Mkdir { parent: u64, name: Vec<u8>, mode: u32, umask: u32 },
    Mknod { parent: u64, name: Vec<u8>, mode: u32, umask: u32, kind: u8, rdev: u32 },
    Symlink { parent: u64, name: Vec<u8>, target: Vec<u8> },
    Link { ino: u64, new_parent: u64, new_name: Vec<u8> },
    Unlink { parent: u64, name: Vec<u8> },
    Rmdir { parent: u64, name: Vec<u8> },
    Rename { parent: u64, name: Vec<u8>, new_parent: u64, new_name: Vec<u8>, flags: u32 },
    SetAttr { ino: u64, set: SetAttrIn },                                // 改了 size → 回 Opened(OpenOut)，否则回 Entry
    SetXattr { ino: u64, name: Vec<u8>, value: Vec<u8>, flags: u32 },
    RemoveXattr { ino: u64, name: Vec<u8> },
    SetAtime { items: Vec<(u64, Ts)> },                                  // relatime 批量更新（尽力而为）
    CommitWrite { ino: u64, epoch: u32, new_size: u64, mtime: Ts,
                  inline_patches: Vec<(u64, Vec<u8>)>,                   // inline 文件：按区间打补丁
                  uninline_expect_change_id: Option<u64>,                // Some = 内容已写进 chunk，清除 INLINE（CAS）
                  want_quota: bool, close: bool },
    AppendReserve { ino: u64, epoch: u32, len: u64 },                    // strong 的 O_APPEND：原子分配偏移
    Fallocate { ino: u64, offset: u64, len: u64, mode: u32 },           // 只支持 mode 0
    SetLock { ino: u64, owner: u64, pid: u32, start: u64, end: u64, kind: u8 /*1 读 2 写 3 解锁*/ },  // 永远非阻塞：冲突 → EAGAIN（F_SETLKW 由客户端轮询，3.5.3）
    // ---- 调试（仅 admin key，且服务端 node.toml 里 debug_ops = true）----
    DebugPut { key: Vec<u8>, value: Vec<u8> },                           // 写 0x2F 键空间
    DebugGet { key: Vec<u8> },
    // ---- ADR-0034（M5 追加，只增不改）----
    Uninline { ino: u64 },                                               // 服务端把 inline 内容搬进 chunk：要写权限；幂等；只读路径、seq 0；回 Opened
}

impl MetaOp {
    /// 变体名：指标里的 `op` 标签（3.3.3）。
    pub fn name(&self) -> &'static str {
        match self {
            MetaOp::SessionOpen { .. } => "SessionOpen",
            MetaOp::SessionHeartbeat { .. } => "SessionHeartbeat",
            MetaOp::SessionClose => "SessionClose",
            MetaOp::Lookup { .. } => "Lookup",
            MetaOp::GetAttr { .. } => "GetAttr",
            MetaOp::ReadDir { .. } => "ReadDir",
            MetaOp::ReadLink { .. } => "ReadLink",
            MetaOp::GetXattr { .. } => "GetXattr",
            MetaOp::ListXattr { .. } => "ListXattr",
            MetaOp::StatFs => "StatFs",
            MetaOp::Open { .. } => "Open",
            MetaOp::GetInline { .. } => "GetInline",
            MetaOp::GetLock { .. } => "GetLock",
            MetaOp::RenewCap { .. } => "RenewCap",
            MetaOp::ResolvePath { .. } => "ResolvePath",
            MetaOp::Create { .. } => "Create",
            MetaOp::Mkdir { .. } => "Mkdir",
            MetaOp::Mknod { .. } => "Mknod",
            MetaOp::Symlink { .. } => "Symlink",
            MetaOp::Link { .. } => "Link",
            MetaOp::Unlink { .. } => "Unlink",
            MetaOp::Rmdir { .. } => "Rmdir",
            MetaOp::Rename { .. } => "Rename",
            MetaOp::SetAttr { .. } => "SetAttr",
            MetaOp::SetXattr { .. } => "SetXattr",
            MetaOp::RemoveXattr { .. } => "RemoveXattr",
            MetaOp::SetAtime { .. } => "SetAtime",
            MetaOp::CommitWrite { .. } => "CommitWrite",
            MetaOp::AppendReserve { .. } => "AppendReserve",
            MetaOp::Fallocate { .. } => "Fallocate",
            MetaOp::SetLock { .. } => "SetLock",
            MetaOp::DebugPut { .. } => "DebugPut",
            MetaOp::DebugGet { .. } => "DebugGet",
            MetaOp::Uninline { .. } => "Uninline",
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct SetAttrIn {
    pub mode: Option<u32>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub size: Option<u64>,
    pub atime: Option<TimeSet>,
    pub mtime: Option<TimeSet>,
    pub epoch_seen: u32,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimeSet {
    Now,
    At(Ts),
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)] // 线格式按 design.md 原样定义（Box 不改变编码，但改变了源码形态）
pub enum MetaReply {
    Session { lease_ms: u64, map_epoch: u64 },
    Heartbeat { lease_ms: u64, map_epoch: u64, need_full_open_set: bool },
    Entry { attr: AttrOut },
    Dir { entries: Vec<DirEntryOut>, eof: bool },
    Link { target: Vec<u8> },
    Xattr { value: Vec<u8> },
    XattrNames { names: Vec<Vec<u8>> },
    StatFs { bsize: u32, blocks: u64, bfree: u64, files: u64, ffree: u64, namelen: u32 },
    Opened(OpenOut), // Open、Create、改了 size 的 SetAttr 共用
    Inline { data: Vec<u8>, change_id: u64 },
    Lock { conflict: Option<LockOut> },
    Cap { cap: Vec<u8> },
    Resolved { ino: u64, kind: u8 },
    Committed { attr: AttrOut, quota_end: u64 },
    Offset { offset: u64, attr: AttrOut },
    Ok,
    DebugValue { value: Option<Vec<u8>> },
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct AttrOut {
    pub ino: u64,
    pub kind: u8,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub nlink: u32,
    pub size: u64,
    pub atime: Ts,
    pub mtime: Ts,
    pub ctime: Ts,
    pub btime: Ts,
    pub rdev: u32,
    pub change_id: u64,
    pub epoch: u32,
    pub consistency: u8, // 1 cto，2 strong（该 inode 所在目录的生效属性）
    pub ttl_ms: u32,     // 内核 entry/attr 缓存时长：cto = 生效的 attr_ttl，strong = 0（3.1.4 C1）
    pub flags: u32,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DirEntryOut {
    pub name: Vec<u8>,
    pub ino: u64,
    pub kind: u8,
    pub attr: Option<AttrOut>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct OpenOut {
    pub attr: AttrOut,
    pub layout: Option<LayoutV1>,
    pub cap: Vec<u8>,
    pub inline: Option<Vec<u8>>, // inline 文件的完整内容（≤ 64 KiB），小文件 open+read 一次往返
    pub quota_end: u64,          // 本次授予的写入上限（字节偏移）；u64::MAX = 子树无配额
    pub atime_due: bool,         // relatime 条件成立，客户端应排队一次 SetAtime
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct LockOut {
    pub start: u64,
    pub end: u64,
    pub write: bool,
    pub pid: u32,
}
