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
//!   leave-joint 兜底）。会话与 target 租约、两阶段驱逐、链管理、修复在 M9、M10。

use std::sync::{Arc, Weak};
use std::time::Duration;

use super::plugins;
use crate::base::types::ReqCtx;
use crate::base::{ErrCode, clock, log};
use crate::meta::changelog;
use crate::meta::engine::EmbeddedEngine;
use crate::meta::engine::txn::{MetaEngine, TxnOp, txn_blocking};
use crate::meta::raft::membership::Membership;
use crate::meta::schema::keys::{self, G_PLUGINS};
use crate::meta::schema::records::{PluginListV1, decode_v1};
use crate::proto::meta::MetaReply;

const PERIOD: Duration = Duration::from_millis(200);
/// 每多少轮刷新一次 changelog 消费者的落后条数（25 × 200 ms = 5 s）、做一次成员变更（5 × 200 ms = 1 s）。
const LAG_EVERY: u64 = 25;
const MEMBERS_EVERY: u64 = 5;

/// 起 cluster-mgr 线程；引擎被丢弃或停止（`shutdown`）后线程自己退出。
pub fn start(eng: &Arc<EmbeddedEngine>) -> Result<(), String> {
    let weak: Weak<EmbeddedEngine> = Arc::downgrade(eng);
    std::thread::Builder::new()
        .name("cluster-mgr".into())
        .spawn(move || {
            let mut members = Membership::default();
            for round in 1u64.. {
                std::thread::sleep(PERIOD);
                let Some(eng) = weak.upgrade() else { return };
                if eng.stopped() {
                    return;
                }
                if eng.plugins_mismatch() {
                    mismatch(&eng);
                }
                if eng.role().ready {
                    let _ = eng.cluster_map(); // 出错（redb 读错误）只影响这一轮，日志已由引擎写好
                    if round % MEMBERS_EVERY == 0 {
                        members.tick(&*eng, &eng.status(), clock::mono_ms());
                    }
                }
                if round % LAG_EVERY == 0 {
                    let _ = eng.local_scan(&mut |v| changelog::refresh_lag(v)); // 只是指标：出错就等下一次
                }
            }
        })
        .map(|_| ())
        .map_err(|e| format!("起 cluster-mgr 线程失败：{e}"))
}

/// 已应用的插件清单与本地的不一致（见文件头）：退出，或（逃生口）就绪之后把集群清单改成本地的。
fn mismatch(eng: &EmbeddedEngine) {
    let local = eng.local_plugins();
    let committed =
        eng.local_get(&keys::global(G_PLUGINS)).ok().flatten().and_then(|b| decode_v1::<PluginListV1>(&b).ok());
    let why = plugins::why(local, committed.as_ref());
    if !eng.adopt_local_plugins() {
        plugins::exit_mismatch(&why);
    }
    if !eng.role().ready {
        return; // 逃生口：当选、就绪之后才改清单
    }
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
