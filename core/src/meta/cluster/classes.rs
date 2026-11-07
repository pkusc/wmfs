//! 存储类（design.md 6.1、6.3，ADR-0007、ADR-0010、ADR-0011；实现注记（M13））：TOML 定义的解析与校验、`wmfs class update` 的规则、
//! QoS 包络、按节点均分用的节点数、由实际部署算出的隔离承诺。增删改的执行器闭包在 `mgmt/classes.rs`。
//!
//! - **TOML**（[`parse`]）：`name`、`[devices] label/dedicated`、`[redundancy] replica`（`ec` → `FEATURE_UNSUPPORTED`，二期）、`[qos]
//!   iops_min/iops_max/bw_min/bw_max/burst/weight/latency_target/priority`、`[placement] failure_domain/locality`、`[lifecycle] tier_to/when`；
//!   不认识的键 = 错误。规格没写的缺省值取最小方案：label `"*"`、weight 1、priority 按 label（`hdd` → low，其余 normal）、failure_domain
//!   `node`、locality `any`；`replica` 必须写（不猜副本数）。
//! - **update**（[`changes`]）：改 label、dedicated、failure_domain → 拒绝（新建一个类再改目录的 class）；改 replica 或 [lifecycle] → 要整树协调；
//!   其余（[qos]、locality）只下发新值。
//! - **QoS 包络**（[`envelope`]，集群级数值）：iops_min + bw_min 折算（每 128 KiB 算 1 个成本单位）→ r，iops_max → l，weight → w，
//!   bw_max 与 burst → 字节令牌桶。数据节点按这个类现在有 Up target 的节点数（[`nodes`]）均分（ADR-0010 第 3 条，[`per_node`]：数据节点
//!   执行的与 `wmfs status` 显示的是同一份，小数不取整）。
//! - **隔离承诺**（[`isolation`]，6.3）：按 target 上报的 `phys_devs`（物理整盘 / namespace）算——声明了 `dedicated = true`、这个类的 target
//!   都解析得出物理盘、且与别的类的 target 不共用 → dedicated；否则 shared；`priority = low` 的类只承诺低优先级。实现注记（M13）：6.3 的表
//!   没写"要声明 dedicated"，取更诚实的一种——没声明独占的类随时可能有别的类建在同一批 target 上，物理上碰巧不共用也不承诺尾延迟（所以只有
//!   一块盘的本机永远显示 shared）。一个类"用的 target"= 归它用的（标签匹配、没被别的 dedicated 类占走；只读的也算：它照样服务读）再加上
//!   它已有的链的全部成员（M13 审查：后建的 dedicated 类占走了 target，别的类的旧链与旧数据还在上面）。`isolation_downgraded`（[`downgraded`]）
//!   只看物理上独不独占，与显示哪种承诺无关（priority = low 的类照样看）。
//! - **新建 dedicated 类**（[`starved`]，M13 审查）：占走 target 之后让别的类一个可用的 target 也没有（例如 label `*`）→ 拒绝。
//! - **"label hdd 位于 nvme 设备上（测试用途）"**（[`hdd_on_nvme`]，ADR-0044 第 7 条）：label 是 `hdd` 的类用的 target 报的 `phys_devs` 里有
//!   `nvme` 开头的设备名 → `wmfs status`、`wmfs class show` 如实标注（按设备名判断，不另加心跳字段）。

use std::collections::BTreeSet;

use serde::Deserialize;

use super::chains::{candidates, owned};
use crate::base::units::{parse_duration, parse_size};
use crate::base::{ErrCode, Failed};
use crate::meta::attr_registry::qos_tier::{class_name_ok, locality_id, parse_cond, parse_locality};
use crate::meta::schema::records::{QosSpecV1, StorageClassV1};
use crate::proto::peer::ClusterMap;

/// `StorageClassV1.priority`：1 normal、2 low；`failure_domain`：首发只有 1 = node。
pub const PRIORITY_NORMAL: u8 = 1;
pub const PRIORITY_LOW: u8 = 2;
pub const DOMAIN_NODE: u8 = 1;
/// 成本单位：一次 IO = 1 + 字节 / 128 KiB（4.5）；bw_min 按每 128 KiB 1 个单位折算进 r。
pub const COST_BYTES: u64 = 128 << 10;

/// 一次 `bytes` 字节的 IO 的成本（4 KiB = 1、1 MiB = 9、4 MiB = 33）。
pub fn cost(bytes: u64) -> u32 {
    (1 + bytes / COST_BYTES).min(u32::MAX as u64) as u32
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ClassToml {
    name: Option<String>,
    #[serde(default)]
    devices: Devices,
    #[serde(default)]
    redundancy: Redundancy,
    #[serde(default)]
    qos: Qos,
    #[serde(default)]
    placement: Placement,
    lifecycle: Option<Lifecycle>,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct Devices {
    label: Option<String>,
    #[serde(default)]
    dedicated: bool,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct Redundancy {
    replica: Option<u8>,
    ec: Option<String>,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct Qos {
    #[serde(default)]
    iops_min: u32,
    #[serde(default)]
    iops_max: u32,
    bw_min: Option<String>,
    bw_max: Option<String>,
    burst: Option<String>,
    weight: Option<u32>,
    latency_target: Option<String>,
    priority: Option<String>,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct Placement {
    failure_domain: Option<String>,
    locality: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Lifecycle {
    tier_to: String,
    when: String,
}

fn bad(m: impl Into<String>) -> Failed {
    Failed::new(ErrCode::EINVAL, m)
}

/// 解析并校验一个存储类的 TOML（见文件头）；`name` = 命令行或路径里的名字（没给就用文件里的 name；都给了必须相同）。id 由调用方填。
pub fn parse(text: &str, name: Option<&str>) -> Result<StorageClassV1, Failed> {
    let t: ClassToml = toml::from_str(text).map_err(|e| bad(format!("存储类定义不是合法的 TOML：{e}")))?;
    let name = name.or(t.name.as_deref()).unwrap_or_default().to_string();
    let name = name.as_str();
    if t.name.as_deref().is_some_and(|n| n != name) || !class_name_ok(name) {
        return Err(bad(format!("存储类名应是 1–32 个小写字母、数字或 -，且与文件里的 name 相同：{name:?}")));
    }
    if t.redundancy.ec.is_some() {
        return Err(Failed::new(ErrCode::FEATURE_UNSUPPORTED, "[redundancy] ec（纠删码）是二期功能"));
    }
    let replica = t.redundancy.replica.ok_or_else(|| bad("[redundancy] 要写 replica = 副本数"))?;
    let label = t.devices.label.unwrap_or_else(|| "*".into());
    let size = |s: Option<String>| s.map_or(Ok(0), |s| parse_size(&s)).map_err(bad);
    let us = |s: Option<String>| s.map_or(Ok(0), |s| parse_duration(&s).map(|d| d.as_micros() as u64)).map_err(bad);
    let q = t.qos;
    let priority = match q.priority.as_deref() {
        None if label == "hdd" => PRIORITY_LOW, // label 为 hdd 的类缺省 low（4.5、6.3）
        None | Some("normal") => PRIORITY_NORMAL,
        Some("low") => PRIORITY_LOW,
        Some(p) => return Err(bad(format!("[qos] priority 应是 normal 或 low：{p:?}"))),
    };
    if !matches!(t.placement.failure_domain.as_deref(), None | Some("node")) {
        return Err(bad("[placement] failure_domain 首发只有 node"));
    }
    let locality = t.placement.locality.map_or(Ok("any".into()), |l| parse_locality(&l, &[])).map_err(bad)?;
    let (tier_to, tier_when) = match t.lifecycle {
        Some(l) if class_name_ok(&l.tier_to) && l.tier_to != name => {
            parse_cond(&l.when).map_err(|e| bad(format!("[lifecycle] when：{e}")))?;
            (Some(l.tier_to), Some(l.when))
        }
        Some(_) => return Err(bad("[lifecycle] tier_to 应是另一个存储类的名字")),
        None => (None, None),
    };
    let k = StorageClassV1 {
        id: 0,
        name: name.into(),
        label,
        dedicated: t.devices.dedicated,
        replica,
        iops_min: q.iops_min,
        iops_max: q.iops_max,
        bw_min: size(q.bw_min)?,
        bw_max: size(q.bw_max)?,
        burst_ms: u32::try_from(us(q.burst)? / 1000).map_err(|_| bad("[qos] burst 太长"))?,
        weight: q.weight.unwrap_or(1),
        latency_target_us: u32::try_from(us(q.latency_target)?).map_err(|_| bad("[qos] latency_target 太长"))?,
        priority,
        failure_domain: DOMAIN_NODE,
        locality: locality_id(&locality),
        tier_to,
        tier_when,
    };
    let e = envelope(&k); // 与目录的 qos 同一条规则（第 5 章）：l ≠ 0 时 r ≤ l（r 含 bw_min 的折算）
    if !(1..=8).contains(&k.replica) || k.weight == 0 || (e.l != 0 && e.r > e.l) {
        return Err(bad(format!(
            "要求 replica 在 1–8、weight ≥ 1，且 iops_max ≠ 0 时保底 iops_min + bw_min / 128 KiB（= {}）≤ iops_max",
            e.r
        )));
    }
    Ok(k)
}

/// `wmfs class update` 的规则（6.1）：label、dedicated、failure_domain 不能改；回"要不要整树协调"（改了 replica 或 [lifecycle]）。
pub fn changes(old: &StorageClassV1, new: &StorageClassV1) -> Result<bool, Failed> {
    if old.label != new.label || old.dedicated != new.dedicated || old.failure_domain != new.failure_domain {
        return Err(bad(
            "不能修改 label、dedicated、failure_domain：请新建一个类，再把目录的 class 改过去，由协调器迁移（design.md 6.1）",
        ));
    }
    Ok(old.replica != new.replica || old.tier_to != new.tier_to || old.tier_when != new.tier_when)
}

/// 存储类的 QoS 包络（集群级数值，见文件头）。
pub fn envelope(k: &StorageClassV1) -> QosSpecV1 {
    let r = (k.iops_min as u64 + k.bw_min / COST_BYTES).min(u32::MAX as u64) as u32;
    QosSpecV1 { r, w: k.weight.max(1), l: k.iops_max, bw_max: k.bw_max, burst_ms: k.burst_ms }
}

/// 这个类现在有 Up target 的数据节点数（QoS 按它均分，ADR-0010 第 3 条；至少按 1 算）。
pub fn nodes(m: &ClusterMap, k: &StorageClassV1) -> u32 {
    (candidates(m, k).iter().map(|(_, n)| *n).collect::<BTreeSet<u32>>().len() as u32).max(1)
}

/// 每个节点的份额（见文件头）：r、l 是成本单位/秒，bw_max 是字节/秒（0 = 不设），按 `nodes` 个节点均分之后的小数。
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PerNode {
    pub r: f64,
    pub w: u32,
    pub l: f64,
    pub bw_max: f64,
    pub burst_ms: u32,
    pub nodes: u32,
}

/// 类 `k` 里 qos 来源目录为 `qos_id` 的队列在每个节点上的份额：`qos_id` 在集群图的 QOS 表里就用目录的 qos，否则用类的包络（4.5）。
pub fn per_node(m: &ClusterMap, k: &StorageClassV1, qos_id: u64) -> PerNode {
    let dir = m.qos.iter().find(|(d, _)| qos_id != 0 && *d == qos_id).map(|(_, q)| q.clone());
    let q = dir.unwrap_or_else(|| envelope(k));
    let n = nodes(m, k);
    let each = |x: f64| x / n as f64;
    let (r, l, bw_max) = (each(q.r.into()), each(q.l.into()), each(q.bw_max as f64));
    PerNode { r, w: q.w.max(1), l, bw_max, burst_ms: q.burst_ms, nodes: n }
}

/// qos 插件不执行份额（fifo）时的承诺（3.8 ⑦、6.3：`wmfs get`、`wmfs status`、`wmfs class show` 同一个说法）。
pub const FIFO_PROMISE: &str = "先进先出，不承诺份额与尾延迟";

/// 由实际部署算出的隔离承诺（6.3）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Isolation {
    Dedicated,
    Shared,
    Low,
}

impl Isolation {
    pub fn name(self) -> &'static str {
        match self {
            Isolation::Dedicated => "dedicated",
            Isolation::Shared => "shared",
            Isolation::Low => "low",
        }
    }

    /// 给人看的承诺（6.3 的表）。
    pub fn promise(self) -> &'static str {
        match self {
            Isolation::Dedicated => "份额 + 尾延迟（以设备能力为限）",
            Isolation::Shared => "只承诺份额，不承诺尾延迟",
            Isolation::Low => "只承诺容量、带宽份额与低优先级（mClock 权重阶段排在 normal 之后），不承诺延迟",
        }
    }
}

/// 类 `c` 用的 target（见文件头）：归它用的（[`owned`]），加上它已有的链的全部成员。
fn used_targets(m: &ClusterMap, c: &StorageClassV1) -> BTreeSet<u32> {
    let mine = m.targets.iter().filter(|(_, t)| owned(m, c, t)).map(|(id, _)| *id);
    let chains = m.chains.iter().filter(|(_, ch)| ch.class_id == c.id);
    mine.chain(chains.flat_map(|(_, ch)| ch.members.iter().map(|(t, _)| *t))).collect()
}

/// target `t` 上报的物理盘（空 = unknown，或集群图里没有它）。
fn phys(m: &ClusterMap, t: u32) -> &[String] {
    m.targets.iter().find(|(id, _)| *id == t).map_or(&[], |(_, r)| r.phys_devs.as_slice())
}

/// 类 `k` 在物理上独占它的盘吗（6.3 的第一行）：它用的 target 都解析得出物理盘（没有 unknown），而且与别的类用的 target 的物理盘
/// 都不相交；一个 target 也没有 → 不算（承诺不了独占）。
fn alone(m: &ClusterMap, k: &StorageClassV1) -> bool {
    let others = m.classes.iter().filter(|o| o.id != k.id);
    let theirs: BTreeSet<&String> = others.flat_map(|o| used_targets(m, o)).flat_map(|t| phys(m, t)).collect();
    let mine = used_targets(m, k);
    let ok = |t: &u32| !phys(m, *t).is_empty() && phys(m, *t).iter().all(|d| !theirs.contains(d));
    !mine.is_empty() && mine.iter().all(ok)
}

/// 类 `k` 的隔离承诺（见文件头；`m` 里要有 `k` 自己——新建、修改时调用方先把它放进去）。
pub fn isolation(m: &ClusterMap, k: &StorageClassV1) -> Isolation {
    if k.priority == PRIORITY_LOW {
        Isolation::Low
    } else if k.dedicated && alone(m, k) {
        Isolation::Dedicated
    } else {
        Isolation::Shared
    }
}

/// 声明了 dedicated、物理上却做不到（`isolation_downgraded`，见文件头）。
pub fn downgraded(m: &ClusterMap, k: &StorageClassV1) -> bool {
    k.dedicated && !alone(m, k)
}

/// 6.3 正文的标注（ADR-0044 第 7 条）：label 是 hdd 的类、它用的 target 报的物理盘里有 nvme 开头的设备名（按设备名判断）。
pub const HDD_ON_NVME: &str = "label hdd 位于 nvme 设备上（测试用途）";

/// 类 `k` 要标注 [`HDD_ON_NVME`] 吗（`wmfs status` 的 warnings 与 `wmfs class show` 的承诺）。
pub fn hdd_on_nvme(m: &ClusterMap, k: &StorageClassV1) -> bool {
    k.label == "hdd" && used_targets(m, k).iter().any(|t| phys(m, *t).iter().any(|d| d.starts_with("nvme")))
}

/// 新建 dedicated 类 `k` 会让哪个别的类一个可用的 target 也没有（此刻有、`k` 占走之后没了：那个类的新文件全部 `ENOSPC`）。
pub fn starved(m: &ClusterMap, k: &StorageClassV1) -> Option<String> {
    if !k.dedicated {
        return None;
    }
    let after = with_class(m, k);
    let lost = |o: &&StorageClassV1| o.id != k.id && !candidates(m, o).is_empty() && candidates(&after, o).is_empty();
    m.classes.iter().find(lost).map(|o| o.name.clone())
}

/// `m` 加上（或换上）类 `k` 之后的集群图（新建、修改时算隔离承诺用）。
pub fn with_class(m: &ClusterMap, k: &StorageClassV1) -> ClusterMap {
    let mut m2 = m.clone();
    m2.classes.retain(|c| c.id != k.id);
    m2.classes.push(k.clone());
    m2
}

#[cfg(test)]
mod tests;
