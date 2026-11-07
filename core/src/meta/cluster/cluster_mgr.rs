//! cluster-mgr 线程（design.md 1.2：只在 leader 上干活，每 200 ms 一轮；所有决定都是提交给执行器的内部操作）。
//!
//! 实现注记（M4）：本节点是就绪 leader 时取一次集群图（`map_epoch` 没变就是缓存，开销是一次点查），集群图第一次建出来或警告变了时
//! 由 `MapCache` 写日志。于是"leader 就绪时、以及警告变化时"server 日志里一定有集群警告（3.1.5、ADR-0016），即使还没有任何客户端请求。
//! 实现注记（M7）：每 5 s（每个节点都做，按本地已应用的状态）刷新一次 changelog 消费者的落后条数（`changelog::refresh_lag`）。
//! 实现注记（M8）：
//! - 插件清单（3.7.2）：本节点已应用的 GLOBAL `plugins` 与本地清单不一致（raft 线程发布的 `mismatch`）→ 不论什么角色，列出差异、1 s 后
//!   以退出码 3 退出（`plugins::exit_mismatch`）；`--adopt-local-plugins` 的节点不退出，等它当选、就绪（它不看清单）之后提交一个批次把
//!   清单改成本地的（`plugins::reset`，changelog `PluginListReset`，日志大声警告）。
//! - 成员变更（3.4.1.9）：就绪的 leader 上每 1 s 一次 `Membership::tick`（learner 加入、joint 提升为 3 voter、移除、drain 时让出 leader、
//!   leave-joint 兜底）。
//!
//! 实现注记（M9）：链管理器（target 租约、boot_id、唯一幸存者、回来、修复、drain、新 target，`chain_mgr.rs`）在就绪的 leader 上每轮
//! （200 ms）一次。
//! 实现注记（M10）：会话租约到期与两阶段驱逐（`meta/evict.rs`）每 1 s 一次。插件清单不一致时的差异照抄 raft 线程与 `mismatch`
//! 一起发布的那一份（写事务提交之后才发布），不再自己读 kv 重算——以前读到提交之前的旧清单时，退出日志里的差异是空的。
//! 实现注记（M13）：就绪的 leader 上每 1 s 看一次心跳报的保底欠额：持续 10 s 大于 0 的 qos_id 写一次 changelog `PolicyViolation`
//! （`ino` = qos 的来源目录，`extra` = `kind=qos_reservation_unmet`/`deficit`，3.11、6.2 第 4 层；`TargetUsage::unmet`）。
//! 实现注记（M13 收尾，ADR-0044 第 5 条）：同时看心跳报的各类 p99 延迟：某个节点上某个类的 p99 连续 10 s 超过它的 `latency_target`
//! → 写一次 `PolicyViolation`（`extra` = `kind=latency_target_exceeded`/`class`（类名）/`node`/`p99_us`/`target_us`；
//! `TargetUsage::slow`），回落之后再超再写。

use std::sync::{Arc, Weak};
use std::time::Duration;

use super::chain_mgr::ChainMgr;
use super::plugins;
use super::targets::TargetUsage;
use crate::base::types::ReqCtx;
use crate::base::{ErrCode, clock, log};
use crate::meta::changelog::{self, Change};
use crate::meta::engine::EmbeddedEngine;
use crate::meta::engine::txn::{MetaEngine, TxnOp, txn_blocking};
use crate::meta::evict::Evictor;
use crate::meta::raft::membership::Membership;
use crate::meta::sessions::Live;
use crate::proto::meta::MetaReply;

const PERIOD: Duration = Duration::from_millis(200);
/// 每多少轮刷新一次 changelog 消费者的落后条数（25 × 200 ms = 5 s）、做一次成员变更（5 × 200 ms = 1 s）、看一次会话租约与驱逐（1 s）。
const LAG_EVERY: u64 = 25;
const MEMBERS_EVERY: u64 = 5;
const EVICT_EVERY: u64 = 5;

/// 起 cluster-mgr 线程；引擎被丢弃或停止（`shutdown`）后线程自己退出。`usage` = leader 内存里的 target 心跳情况、`live` = 会话的
/// 心跳情况（都由元数据服务记）。
pub fn start(eng: &Arc<EmbeddedEngine>, usage: Arc<TargetUsage>, live: Arc<Live>) -> Result<(), String> {
    let weak: Weak<EmbeddedEngine> = Arc::downgrade(eng);
    std::thread::Builder::new()
        .name("cluster-mgr".into())
        .spawn(move || {
            let (mut members, mut chains, mut evictor) =
                (Membership::default(), ChainMgr::default(), Evictor::default());
            for round in 1u64.. {
                std::thread::sleep(PERIOD);
                let Some(eng) = weak.upgrade() else { return };
                if eng.stopped() {
                    return;
                }
                if let Some(why) = eng.plugins_mismatch() {
                    mismatch(&eng, &why);
                }
                if eng.role().ready {
                    let _ = eng.cluster_map(); // 出错（redb 读错误）只影响这一轮，日志已由引擎写好
                    if round % MEMBERS_EVERY == 0 {
                        members.tick(&*eng, &eng.status(), clock::mono_ms());
                        unmet(&eng, &usage);
                        slow(&eng, &usage);
                    }
                }
                chains.tick(&eng, &usage); // 不是就绪的 leader 时它只把链的指标归零
                if round % EVICT_EVERY == 0 {
                    evictor.tick(&eng, &live, &usage);
                }
                if round % LAG_EVERY == 0 {
                    let _ = eng.local_scan(&mut |v| changelog::refresh_lag(v)); // 只是指标：出错就等下一次
                }
            }
        })
        .map(|_| ())
        .map_err(|e| format!("起 cluster-mgr 线程失败：{e}"))
}

/// 已应用的插件清单与本地的不一致（`why` = raft 线程比对时算好的差异，见文件头）：退出，或（逃生口）就绪之后把集群清单改成本地的。
fn mismatch(eng: &EmbeddedEngine, why: &str) {
    if !eng.adopting() {
        plugins::exit_mismatch(why);
    }
    if !eng.role().ready {
        return; // 逃生口：当选、就绪之后才改清单
    }
    let local = eng.local_plugins();
    log::warn(
        "plugins",
        format_args!(
            "--adopt-local-plugins：把集群插件清单改成本节点的 {:?}（原来不一致：{why}）；其他节点随后按新清单比对",
            local.plugins
        ),
    );
    let mine = local.clone();
    let op: TxnOp = Box::new(move |v, c| plugins::reset(v, c, &mine).map(|()| MetaReply::Ok));
    if let Err(e) = txn_blocking(eng as &dyn MetaEngine, ReqCtx::internal(), op)
        && e != ErrCode::NOT_LEADER
    {
        log::warn("plugins", format_args!("改集群插件清单失败：{e}（下一轮再试）"));
    }
}

/// 保底欠额持续不退的 qos_id：各写一次 `PolicyViolation`（见文件头）；写不成只记日志（下一段欠额再出现时再写）。
fn unmet(eng: &EmbeddedEngine, usage: &TargetUsage) {
    for (dir, deficit) in usage.unmet(clock::mono_ms()) {
        let why = format!("目录 {dir} 的 qos 保底持续没给够（欠额 {deficit}）");
        event(eng, violation(dir, deficit), &why);
    }
}

/// p99 延迟持续超过 `latency_target` 的 (类, 节点)：各写一次 `PolicyViolation`（ADR-0044 第 5 条，见文件头）。
fn slow(eng: &EmbeddedEngine, usage: &TargetUsage) {
    let Ok(map) = eng.cluster_map() else { return }; // 读不出来：这一轮不看
    let targets: Vec<(u16, u32)> =
        map.classes.iter().filter(|k| k.latency_target_us > 0).map(|k| (k.id, k.latency_target_us)).collect();
    for ((class, node), p99) in usage.slow(clock::mono_ms(), &targets) {
        let target = targets.iter().find(|(k, _)| *k == class).map_or(0, |(_, t)| *t);
        let name = map.class_name(class);
        let why = format!("存储类 {name} 在节点 {node} 上的 p99 延迟 {p99} µs 持续超过目标 {target} µs");
        event(eng, latency_violation(&name, node, p99, target), &why);
    }
}

/// 写一条告警事件（一个内部批次）；写不成只记日志（下一段再出现时再写）。
fn event(eng: &EmbeddedEngine, ev: Change<'static>, why: &str) {
    let op: TxnOp = Box::new(move |v, c| {
        changelog::append(v, c, ev)?;
        Ok(MetaReply::Ok)
    });
    if let Err(e) = txn_blocking(eng as &dyn MetaEngine, ReqCtx::internal(), op) {
        log::warn("qos", format_args!("{why}：告警事件没写成（{e}）"));
    }
}

/// 保底欠额的 `PolicyViolation`：`ino` = qos 的来源目录，`extra` = kind、deficit（只增的取值，单元测试钉住键与次序）。
fn violation(dir: u64, deficit: u64) -> Change<'static> {
    let extra = vec![("kind".into(), "qos_reservation_unmet".into()), ("deficit".into(), deficit.to_string())];
    Change { op: "PolicyViolation", ino: dir, extra, ..Default::default() }
}

/// 类的 p99 超过目标的 `PolicyViolation`：`extra` = kind、class（类名）、node、p99_us、target_us（只增的取值，单元测试钉住键与次序）。
fn latency_violation(class: &str, node: u32, p99_us: u64, target_us: u32) -> Change<'static> {
    let extra = vec![
        ("kind".into(), "latency_target_exceeded".into()),
        ("class".into(), class.into()),
        ("node".into(), node.to_string()),
        ("p99_us".into(), p99_us.to_string()),
        ("target_us".into(), target_us.to_string()),
    ];
    Change { op: "PolicyViolation", extra, ..Default::default() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_qos_violation_event_is_golden() {
        let c = violation(18, 120);
        let keys: Vec<&str> = c.extra.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!((c.op, c.ino, keys), ("PolicyViolation", 18, vec!["kind", "deficit"]));
        assert_eq!(c.extra[0].1, "qos_reservation_unmet");
    }

    /// ADR-0044 第 5 条：`latency_target_exceeded` 的 extra 键与次序（金样）。
    #[test]
    fn the_latency_violation_event_is_golden() {
        let c = latency_violation("gold", 2, 1_200, 500);
        let kv: Vec<(&str, &str)> = c.extra.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        let want = [
            ("kind", "latency_target_exceeded"),
            ("class", "gold"),
            ("node", "2"),
            ("p99_us", "1200"),
            ("target_us", "500"),
        ];
        assert_eq!((c.op, c.ino, kv), ("PolicyViolation", 0, want.to_vec()));
    }
}
