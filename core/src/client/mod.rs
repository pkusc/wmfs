//! 客户端库（design.md 3.12、第 8 章）：FUSE 适配层（cli/src/fuse/）、CLI 与后台任务都用它；不依赖 fuser，
//! 将来的内核客户端协议适配或 LD_PRELOAD 库也能复用它与同一套线上协议。
//!
//! M0 只有 [`meta_client`]、[`data_client`] 的接口骨架；写回缓冲、文件表、strong 同步写路径等在 M6 起加入。

pub mod data_client;
pub mod meta_client;
