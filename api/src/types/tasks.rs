//! M11 的管理端点的请求/响应结构（design.md 7.1、3.10）：后台任务（列表、暂停、恢复、立即跑一轮、限速）、数据加密密钥的建立
//! （本节点的任务进度 `GET /api/v1/node` 的 `NodeInfoReply` 在 `types/obs.rs`）。字段只增不改（ADR-0005）；JSON 都有逐字的金样（`tasks/tests.rs`）。

use serde::{Deserialize, Serialize};

/// [`TaskInfo::runs_on`] 的取值。
pub const RUNS_ON_LEADER: &str = "leader";
pub const RUNS_ON_EVERY_NODE: &str = "every-node";
/// [`TaskInfo::state`] 的取值。
pub const STATE_RUNNING: &str = "running";
pub const STATE_PAUSED: &str = "paused";
pub const STATE_DONE: &str = "done";

/// 一个后台任务此刻的样子。`runs_on` 是 `leader`（只在元数据 leader 上跑，进度是集群级的）或 `every-node`（每个数据节点各跑各的，`node`
/// 是哪个节点；集群级的那一行 `node` 为 0，只有控制字段）。`state` 是 `running` / `paused` / `done`（这一轮做完了，等下一轮）；`pass`
/// 是做完了几轮，`done` 是累计处理的对象数，`backlog` 是任务自己报的积压（协调器：队列里还有几项，每项是一个目录），`progress`
/// 是这一轮的进度（0–1；协调器没有，为 0：[`backlog_in_dirs`]），`rate_bps` 是最近一步的速率（字节/秒，含限速等待），`bw`
/// 是生效的字节/秒限速（没设时是任务自己的默认值，0 = 只受节点级上限）、`items` 是对象/秒限速（0 = 不限），`updated_ms`
/// 是最近一次持久化进度的时刻（墙钟毫秒），`last_error` 是最近一次失败的一步的错误（之后成功了就清空）；`runs`
/// 是运行请求号（`every-node` 任务：集群级的一行 = 收到过几次 `wmfs task run`，节点的一行 = 这个节点已经照办到第几次；
/// leader 任务为 0）。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
pub struct TaskInfo {
    pub kind: String,
    pub runs_on: String,
    pub node: u32,
    pub state: String,
    pub pass: u64,
    pub done: u64,
    pub backlog: u64,
    pub progress: f64,
    pub rate_bps: u64,
    pub bw: u64,
    pub items: u64,
    pub updated_ms: u64,
    pub last_error: String,
    pub runs: u64,
}

impl TaskInfo {
    pub fn on_leader(&self) -> bool {
        self.runs_on == RUNS_ON_LEADER
    }
}

/// 积压按目录数计的任务（协调器：队列里每项是一个目录，处理的却是文件）：它的"这一轮的进度比例"没有意义——`progress` 为 0、没有
/// `wmfs_task_progress_ratio`，`wmfs task list` 显示"积压 N 个目录"（实现注记（M11 审查））。
pub fn backlog_in_dirs(kind: &str) -> bool {
    kind == "reconciler"
}

/// `GET /api/v1/tasks`（只由元数据 leader 回答）：每个任务一行——leader 任务带集群级的进度，节点任务只有集群级的控制字段（各节点的
/// 进度见各节点的 `GET /api/v1/node`）。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
pub struct TasksReply {
    pub tasks: Vec<TaskInfo>,
}

/// `POST /api/v1/tasks/<kind>/set` 的请求：`bw` 是字节/秒（写法与尺寸相同：`2M`；`0` = 用任务自己的默认值），`items` 是对象/秒
/// （0 = 不限）；不给 = 不改。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct TaskSetReq {
    #[serde(default)]
    pub bw: Option<String>,
    #[serde(default)]
    pub items: Option<u64>,
}

/// `POST /api/v1/datakeys` 的请求：名字（`encrypt=tenant:<名字>` 用它）与它属于哪个租户。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct DataKeyCreateReq {
    pub name: String,
    #[serde(default)]
    pub tenant: u32,
}

/// `POST /api/v1/datakeys` 的回复：新数据密钥的数字 id（密钥本身不出现在管理 API 里）。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct DataKeyCreateReply {
    pub id: u32,
    pub name: String,
}

#[cfg(test)]
mod tests;
