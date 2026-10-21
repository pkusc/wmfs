//! 后台任务框架（design.md 3.10，ADR-0027）：可限速、可断点续跑、可观测；协调器是第一个实例。
//!
//! M0 只有 [`task`]：`BgTask` trait 与 `TaskCtx`。调度器 scheduler.rs（M5）、限速器 limiter.rs、遍历器 walker.rs、
//! 迁移原语 migrator.rs、"声明 vs 实际" status.rs（M11）随里程碑加入。

pub mod task;
