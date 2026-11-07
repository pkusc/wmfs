//! M13 的管理端点：存储类（design.md 6.1、6.3、7.1）与本节点的 QoS 队列快照（`GET /api/v1/qos`，4.5）——`GET /api/v1/classes`、`POST /api/v1/classes?name=<名字>`（body = 存储类的 TOML）、
//! `PUT /api/v1/classes/<名字>`（body = TOML，6.1 的修改规则）、`DELETE /api/v1/classes/<名字>`。字段只增不改（ADR-0005）；JSON 金样在
//! `classes/tests.rs`。

use serde::{Deserialize, Serialize};

/// 一组 QoS 数值：iops 是成本单位/秒（一次 IO = 1 + 字节 / 128 KiB：4 KiB = 1、4 MiB = 33，4.5），bw 是字节/秒，burst 是毫秒；0 = 不设。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct ClassQos {
    pub iops_min: u32,
    pub iops_max: u32,
    pub bw_min: u64,
    pub bw_max: u64,
    pub burst_ms: u32,
    pub weight: u32,
}

/// 一个存储类：定义（`priority` 是 `normal`/`low`，`locality` 是 `any`/`prefer-client`，`latency_target_us` 只测量不承诺），以及由实际部署
/// 算出的——`nodes` = 这个类现在有 Up target 的数据节点数（QoS 按它静态均分，ADR-0010），`per_node` = 每个节点的份额，`isolation` =
/// `dedicated` / `shared` / `low`（6.3），`promise` = 给人看的承诺，`downgraded` = 声明了 dedicated、实际做不到（isolation_downgraded）。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct ClassInfo {
    pub id: u16,
    pub name: String,
    pub label: String,
    pub dedicated: bool,
    pub replica: u8,
    pub priority: String,
    pub locality: String,
    pub latency_target_us: u32,
    pub tier_to: Option<String>,
    pub tier_when: Option<String>,
    pub qos: ClassQos,
    pub nodes: u32,
    pub per_node: ClassQos,
    pub isolation: String,
    pub promise: String,
    pub downgraded: bool,
}

/// `GET /api/v1/classes`：全部存储类（按 id）。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct ClassesReply {
    pub classes: Vec<ClassInfo>,
}

/// 增、改、删的回复：改完（删掉之前）的这个类；`reconcile` = 这次修改写了一条整树协调（改了 replica 或 [lifecycle]，6.1）。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct ClassReply {
    pub class: ClassInfo,
    pub reconcile: bool,
}

/// `GET /api/v1/qos` 里的一个队列（节点本地；4.5、3.12 的 QueueStat）：键 = (租户, qos 属性的来源目录, 存储类)——后台队列三个都是 null；
/// `served_per_s` 是最近 10 s 派发的成本单位/秒，`deficit_per_s` 是保底欠额，`limited_total` 是被上限推迟过的请求数。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
pub struct QosQueue {
    pub tenant: Option<u32>,
    pub qos_id: Option<u64>,
    pub class_id: Option<u16>,
    pub depth: u32,
    pub served_per_s: f64,
    pub deficit_per_s: f64,
    pub limited_total: u64,
}

/// `GET /api/v1/qos`（每个节点回答本节点的，需认证）：本节点的 qos 插件与它的队列（fifo 不分队列：空表）。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
pub struct QosReply {
    pub plugin: String,
    pub queues: Vec<QosQueue>,
}

#[cfg(test)]
mod tests;
