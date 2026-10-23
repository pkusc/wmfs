//! cluster-mgr 线程（design.md 1.2：只在 leader 上干活，每 200 ms 一轮；所有决定都是提交给执行器的内部操作）。
//!
//! 实现注记（M4）：现在只做一件事——本节点是就绪 leader 时取一次集群图（`map_epoch` 没变就是缓存，开销是一次点查），
//! 集群图第一次建出来或警告变了时由 `MapCache` 写日志。于是"leader 就绪时、以及警告变化时"server 日志里一定有集群警告
//! （3.1.5、ADR-0016），即使还没有任何客户端请求。会话与 target 租约、两阶段驱逐、链管理、成员变更、修复在 M5、M8–M10 加入。
//! 实现注记（M7）：每 5 s（每个节点都做，按本地已应用的状态）刷新一次 changelog 消费者的落后条数（`changelog::refresh_lag`）。

use std::sync::{Arc, Weak};
use std::time::Duration;

use crate::meta::changelog;
use crate::meta::engine::EmbeddedEngine;
use crate::meta::engine::txn::MetaEngine;

const PERIOD: Duration = Duration::from_millis(200);
/// 每多少轮刷新一次 changelog 消费者的落后条数（25 × 200 ms = 5 s）。
const LAG_EVERY: u64 = 25;

/// 起 cluster-mgr 线程；引擎被丢弃或停止（`shutdown`）后线程自己退出。
pub fn start(eng: &Arc<EmbeddedEngine>) -> Result<(), String> {
    let weak: Weak<EmbeddedEngine> = Arc::downgrade(eng);
    std::thread::Builder::new()
        .name("cluster-mgr".into())
        .spawn(move || {
            for round in 1u64.. {
                std::thread::sleep(PERIOD);
                let Some(eng) = weak.upgrade() else { return };
                if eng.stopped() {
                    return;
                }
                if eng.role().ready {
                    let _ = eng.cluster_map(); // 出错（redb 读错误）只影响这一轮，日志已由引擎写好
                }
                if round % LAG_EVERY == 0 {
                    let _ = eng.local_scan(&mut |v| changelog::refresh_lag(v)); // 只是指标：出错就等下一次
                }
            }
        })
        .map(|_| ())
        .map_err(|e| format!("起 cluster-mgr 线程失败：{e}"))
}
