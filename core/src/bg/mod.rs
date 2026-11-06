//! 后台任务框架（design.md 3.10，ADR-0027）：可限速、可断点续跑、可观测；协调器是第一个实例。
//!
//! - [`task`]：`BgTask` trait 与 `TaskCtx`（M0 骨架）
//! - [`scheduler`]：每个节点一个 `bg-sched` 线程轮转后台任务（M5 骨架：只有 leader 任务）
//! - [`limiter`]：任务级 + 节点级限速（M5）
//! - [`progress`]：任务的控制与进度存在哪里（集群级的 TaskRecV1、节点任务的本地游标文件、给管理 API 的进度板；M11）
//! - [`walker`]：可续跑的命名空间遍历（子树 DFS、全量扁平扫描；M11）
//! - [`migrator`]：迁移原语（换布局的六步，含"因写打开而跳过"的计数与 24 h 告警；M11）
//! - [`status`]："声明 vs 实际"——`declared(file)` 与 `wmfs status` 的逐文件统计（M11）

pub mod limiter;
pub mod migrator;
pub mod progress;
pub mod scheduler;
pub mod status;
pub mod task;
pub mod walker;
