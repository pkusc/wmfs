//! 元数据 Raft（design.md 3.4.1，ADR-0019）：raft-rs 0.7 + 一个 redb 文件（raft 日志、硬状态、状态机 KV 同库）。
//!
//! - [`storage`]：`raft::Storage` 的 redb 实现 `RedbStorage`（3.4.1.2）与 init 写的 index = 1 初始状态（3.4.1.9）
//! - [`driver`]：raft 线程的全部状态 `Driver` 与驱动循环（3.4.1.3 的 Ready 处理顺序、3.2.6 的写入规则 M1–M4、
//!   日志压缩 3.4.1.8），以及它的输入输出（`RaftInput`、`ProposeOutcome`、`Published`）、打开参数 `DriverOpts` 与
//!   raft 参数 `config`（这几样在 types.rs，driver 原样转出）
//!
//! 线程归属（1.2）：`RawNode` 与 redb 写事务只属于 raft 线程；别的线程只经 [`driver::RaftInput`] 投递请求、
//! 只读 [`driver::Published`] 里发布的角色与状态快照。Driver 设计成"输入一步、处理一次 Ready"的结构体方法，
//! 确定性模拟（9.2）直接调用它，不起线程。
//! 实现注记（M3）：快照的导出、带外传输与安装（3.4.1.7、规则 M5）、raft-send 线程（peers.rs）、成员变更的发起
//! （membership.rs）属于 M8；M3 的单节点集群不会收到快照，也没有要发给别人的 raft 消息。

pub mod driver;
pub mod storage;
mod types;

use crate::base::{ErrCode, log};

/// fail-stop（design.md 0.2 第 5 条）：raft 线程与执行器线程上的 redb 错误（`begin_write/commit/读表` 失败）
/// 一律写一条错误日志并 `abort`——状态机写一半时继续运行比崩溃更危险；重启后由 raft 日志恢复。伪代码里的 `or_abort(..)`。
pub fn or_abort<T, E: std::fmt::Display>(r: Result<T, E>, what: &str) -> T {
    match r {
        Ok(v) => v,
        Err(e) => {
            log::error(
                "raft",
                format_args!("redb {what} 失败：{e}——按 fail-stop 规则立即退出（重启后由 raft 日志恢复）"),
            );
            std::process::abort()
        }
    }
}

/// 不在原地 abort 的 redb 错误：写一行带原因的错误日志，返回 `EIO`。`RedbStorage` 的写方法用它（之后由 driver
/// `or_abort`），meta-read 线程的读事务与本地读也用它（把 `EIO` 回给请求方）。
pub fn redb_eio(what: &str, e: impl std::fmt::Display) -> ErrCode {
    log::error("meta", format_args!("redb {what} 失败：{e}"));
    ErrCode::EIO
}
