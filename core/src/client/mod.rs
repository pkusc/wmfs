//! 客户端库（design.md 3.12、第 8 章）：FUSE 适配层（cli/src/fuse/）、CLI 与后台任务都用它；不依赖 fuser，
//! 将来的内核客户端协议适配或 LD_PRELOAD 库也能复用它与同一套线上协议。
//!
//! - [`meta_client`]：MetaClient——跟随 leader、seq、按 3.3.8 重试（M0 骨架，M4 实现）。
//! - [`data_client`]：DataClient——选副本、重定向、换副本、重试（M0 骨架，M5 实现）。
//! - [`inode`]（M6）：一个 inode 在本挂载里的状态（属性、layout、cap、inline 副本、写回缓冲）与 C2 的"见过的 change_id"。
//! - [`files`]（M6）：FileClient——inode 表、文件句柄表、目录句柄的快照；打开、关闭、重新 Open、取 cap。
//! - [`writeback`]（M6）：按槽位计的写回缓冲、write、flush、fsync。
//! - [`flusher`]（M6）：写回用到的请求（WriteChunk、SyncChunks、CommitWrite、Uninline），以及后台写回（4 个写线程、flusher 线程）。
//! - [`read`]（M6）：读路径（读自己的写、inline、cache_store、数据节点）。
//! - [`strong`]（M6）：同步写路径（M6 先给 cto 下的 `O_DIRECT` 用）与截断。
//! - [`session`]（M6）：会话心跳（上报打开集合）与 relatime 批量（atime 线程）。
//!
//! 实现注记（M6）：design.md 2.2 的文件表里没有 `inode.rs`、`flusher.rs`、`read.rs`、`session.rs`：为了每个文件 ≤ 400 行，从
//! files.rs 与 writeback.rs 拆出来。锁（`locks.rs`）在 M12。

use std::time::Duration;

pub mod data_client;
pub mod files;
pub mod flusher;
pub mod inode;
pub mod meta_client;
pub mod read;
pub mod session;
pub mod strong;
pub mod writeback;

/// 客户端从内存池取一个缓冲最多等多久（写回缓冲、读、同步写都用它）。服务端是 [`crate::buf::POOL_WAIT`]（100 ms，取不到回
/// `BUSY` 让客户端退避重试）；客户端没有人替它重试，所以等得久。
pub(crate) const CLIENT_POOL_WAIT: Duration = Duration::from_secs(10);

/// `STALE_LAYOUT` 之后最多重来几次（重新 Open、按新 layout 重写或重读，C4）；`wmfs fs truncate` 也用它。
pub const STALE_RETRIES: usize = 3;
