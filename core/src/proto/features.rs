//! 协议 feature 位与 feature 名（design.md 3.3.2、3.2.10）。
//!
//! 握手里 `Hello.features` 是本端能力，`Hello.required` 是本端要求对方必须有的位；对方不支持 required 中的位 →
//! 拒绝连接（`PROTO_UNSUPPORTED`）；`Challenge.features` 是双方交集。消息枚举的新变体只能在对端声明支持后发送（MetaClient 发
//! `MetaOp::feature` 不是 0 的请求之前核对这条连接的交集，`RpcClient::features`）。

/// bit0：本端能处理单边 bulk 描述符（RDMA）
pub const FEAT_BULK_REMOTE: u64 = 1 << 0;
pub const FEAT_STRONG: u64 = 1 << 1;
pub const FEAT_LOCKS: u64 = 1 << 2;
pub const FEAT_INLINE: u64 = 1 << 3;
// bit4 保留（二期 DYN_TRANSFORM）；bit 32 起保留（PIPELINED_META、SNAPSHOT、EC）

/// 首发 core 认识的全部协议 feature 位。
pub const FEAT_KNOWN: u64 = FEAT_BULK_REMOTE | FEAT_STRONG | FEAT_LOCKS | FEAT_INLINE;

/// 本端在握手里声明的位（`Hello.features`，节点与客户端相同；ADR-0043 第 3 条）：已实现的 strong（`AppendReserve`）、锁（`SetLock`、
/// `GetLock`）与 inline（M5）。`FEAT_BULK_REMOTE` 不声明：单边 bulk 描述符只出现在两端都是 RDMA 传输的连接上，由传输插件两端自己处理
/// （3.3.2 的 M2 注记），而握手与传输无关——在 TCP 连接上声明它就不对了。金样 `tests/golden/handshake.golden` 的 Hello 与 Challenge。
pub const OUR_FEATURES: u64 = FEAT_STRONG | FEAT_LOCKS | FEAT_INLINE;

/// 预留的 incompat 特性名（3.2.10；都是二期，首发没有默认启用的可选 incompat 特性）。
/// 启用只能通过 `wmfs feature enable <name>`，不可逆。
pub const RESERVED_FEATURE_NAMES: &[&str] =
    &["chunk.snapshots", "chunk.dyn_transform", "meta.ec-layout", "proto.pipelined-meta"];
