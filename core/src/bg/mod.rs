//! 后台任务框架（design.md 3.10，ADR-0027）：可限速、可断点续跑、可观测；协调器是第一个实例。
//!
//! - [`task`]：`BgTask` trait 与 `TaskCtx`（M0 骨架）
//! - [`scheduler`]：每个节点一个 `bg-sched` 线程轮转后台任务（M5 骨架：只有 leader 任务）
//! - [`limiter`]：任务级 + 节点级限速（M5）
//!
//! 遍历器 walker.rs、迁移原语 migrator.rs、"声明 vs 实际" status.rs 在 M11。

pub mod limiter;
pub mod scheduler;
pub mod task;
