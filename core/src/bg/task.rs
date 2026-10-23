//! 后台任务 trait 与上下文（design.md 3.10）。
//!
//! 调度：每个节点一个 `bg-sched` 线程轮转本节点该跑的任务：`MetaLeader` 任务只在元数据 leader 就绪时运行，
//! 失去 leader 立即停（新 leader 从 `TaskRecV1` 游标续跑）；`EveryDataNode` 任务在每个数据节点运行，游标由 core
//! 以 `atomic_write` 存在 `<data_dir>/run/task-<kind>.cursor`。每步：取令牌 → `step` → 持久化游标与进度。
//! `step` 必须幂等：它的副作用与游标持久化不在同一个事务里，崩溃后从旧游标重放不能出错。
//!
//! 接口骨架（M0）：签名以这里为准；`Walker`、`Migrator` 这里先是空壳，实现时移到 walker.rs、migrator.rs（M11）。
//! 实现注记（M5）：`RateLimiter` 移到了 limiter.rs；`TaskCtx` 加了一个字段 `live`（leader 内存里的打开文件集合，`sessions::Live`）——
//! 孤儿 GC 的 3.5.6 条件 ①③ 要看它，而 M0 的骨架没有预见；首发之前还没有任何 bg_task 插件，所以 `CORE_ABI_VERSION` 不变。

use std::sync::Arc;
use std::time::Duration;

use crate::base::ErrCode;
use crate::client::data_client::DataClient;
use crate::hooks::manifest::Manifest;
use crate::hooks::policy::PolicyEvaluator;
use crate::hooks::traits::TargetStore;
use crate::meta::engine::txn::MetaEngine;
use crate::meta::sessions::Live;

pub use super::limiter::RateLimiter;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunOn {
    MetaLeader,
    EveryDataNode,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct StepResult {
    pub cursor: Vec<u8>,
    pub items: u64,
    pub bytes: u64,
    pub backlog: u64,
    pub pass_done: bool,
}

pub trait BgTask: Send {
    fn manifest(&self) -> &'static Manifest;
    fn kind(&self) -> &'static str;
    fn runs_on(&self) -> RunOn;
    fn interval(&self) -> Duration; // 一轮结束后隔多久开始下一轮
    /// 做一小步（≤ 100 个对象 / ≤ 64 MiB / ≤ 1 s）。必须幂等：从同一游标重放，效果相同。
    fn step(&mut self, ctx: &mut TaskCtx, cursor: &[u8]) -> Result<StepResult, ErrCode>;
}

pub struct TaskCtx<'a> {
    pub meta: &'a dyn MetaEngine,            // 内部身份（admin）：只读 read() 与内部 txn
    pub data: &'a DataClient,                // 数据面客户端：管理 cap，请求标记 background（走 QoS 低优先级队列）
    pub walker: &'a Walker,                  // 可续跑的命名空间遍历
    pub migrate: &'a Migrator,               // 迁移原语
    pub limiter: &'a RateLimiter,            // 任务级 + 节点级令牌桶
    pub targets: &'a [Arc<dyn TargetStore>], // 只有 EveryDataNode 任务有
    pub policy: &'a dyn PolicyEvaluator,
    pub now_ms: u64,
    pub live: &'a Live, // 实现注记（M5）：leader 内存里的打开文件集合（孤儿 GC 的条件，3.5.6）
}

/// 遍历器（3.10）：子树 DFS（游标 = `Vec<(dir_ino, 最后处理的名字)>` 的 postcard 编码）与全量扁平扫描（按 ino）。M11。
pub struct Walker {}

/// 迁移原语 `Migrator::migrate(ino, declared)`（3.10）：协调器、tiering、rebalance 共用。M11。
pub struct Migrator {}

#[cfg(test)]
mod tests {
    use super::*;

    /// 编译期检查（design.md 0.2 第 3 条）：`BgTask` 必须对象安全（调度器持有 `Box<dyn BgTask>`）。
    #[allow(dead_code)]
    fn bg_task_is_object_safe(_: &dyn BgTask) {}
}
