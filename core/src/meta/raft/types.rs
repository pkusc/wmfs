//! raft 线程用到的几个类型（design.md 3.4.1.3、3.4.1.1）：别的线程投递给 raft 线程的输入 [`RaftInput`]、提议的结局
//! [`ProposeOutcome`]、raft 线程发布的只读快照 [`Published`]、打开 Driver 的参数 [`DriverOpts`]，以及 raft 参数 [`config`]。
//! `driver` 模块原样转出它们。

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::Duration;

use crossbeam_channel::Sender;
use raft::eraftpb::{ConfChangeV2, Message};
use raft::{Config, ReadOnlyOption, StateRole};

use crate::base::{ErrCode, lock};
use crate::meta::engine::txn::{EngineStatus, RoleInfo};
use crate::meta::schema::records::PluginListV1;

/// 发给 raft 线程的输入（3.4.1.3）。
pub enum RaftInput {
    Msg(Message),                                                              // 对端 raft 消息
    Propose { data: Vec<u8>, leader_term: u64, done: Sender<ProposeOutcome> }, // 执行器
    ConfChange { cc: ConfChangeV2, done: Sender<ProposeOutcome> },             // cluster-mgr 的成员管理（M8）
    ReadIndex { done: Sender<Result<(), ErrCode>> },                           // meta-read 线程（3.4.1.6）
    ReportSnapshot { to: u64, ok: bool },
    ReportUnreachable(u64),
    TransferLeader(u64),
    Shutdown,
}

/// 一个提议的结局：raft 线程保证每个提议最终得到三者之一。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProposeOutcome {
    Applied { ok: bool }, // 已应用；ok = false：批次作废（expect_version 不符）或 conf change 被 raft-rs 改写成空条目
    Lost,                 // 结果不确定（失去 leader 身份、条目被覆盖）：靠去重表兜底
    Dropped,              // 没有进日志（不是同一任期的就绪 leader、超过 max_uncommitted_size……）
}

/// raft 线程发布给其他线程的快照：其他线程只读它，绝不碰 RawNode。实现注记（M7）：另有一个"写了 changelog 的批次"计数，
/// changelog 长轮询（3.11）在它上面等：raft 线程应用了写 0x0E 的批次（写事务提交之后）就加一并唤醒全部等待者。
/// 实现注记（M8）：失去 leader 身份时也加一（在发布了新角色之后：长轮询醒来看到自己不再是 leader，立即回 409/503，不挂到超时）；
/// `mismatch` = 本节点已应用的 GLOBAL plugins 与本地插件清单不一致（还没有清单、applied 0 的新节点不算）——cluster-mgr 在任何角色上
/// 据此退出（退出码 3），`--adopt-local-plugins` 的节点则在当选就绪之后把清单改成本地的（3.7.2）。
pub struct Published {
    pub(super) role: Mutex<RoleInfo>,
    pub(super) status: Mutex<EngineStatus>,
    changelog: Mutex<u64>,
    changelog_cv: Condvar,
    pub(super) mismatch: AtomicBool,
}

impl Published {
    pub(super) fn new() -> Published {
        let role = RoleInfo { role: StateRole::Follower, term: 0, leader_id: 0, ready: false };
        let (changelog, changelog_cv) = (Mutex::new(0), Condvar::new());
        let (status, mismatch) = (Mutex::new(EngineStatus::default()), AtomicBool::new(false));
        Published { role: Mutex::new(role), status, changelog, changelog_cv, mismatch }
    }
    /// 已应用的集群插件清单与本地的不一致（见类型的说明）。
    pub fn plugins_mismatch(&self) -> bool {
        self.mismatch.load(Ordering::Relaxed)
    }
    pub fn role(&self) -> RoleInfo {
        *lock(&self.role)
    }
    pub fn status(&self) -> EngineStatus {
        lock(&self.status).clone()
    }
    /// "写了 changelog 的批次"计数的当前值（长轮询先记下它，再读一页）。
    pub fn changelog_seen(&self) -> u64 {
        *lock(&self.changelog)
    }
    /// 等到计数不再是 `seen`（有新的 changelog 记录提交了）或超时；返回当前值。
    pub fn wait_changelog(&self, seen: u64, timeout: Duration) -> u64 {
        let g = lock(&self.changelog);
        let (g, _) =
            self.changelog_cv.wait_timeout_while(g, timeout, |n| *n == seen).unwrap_or_else(|e| e.into_inner());
        *g
    }
    /// raft 线程：写了 changelog 的批次应用之后（写事务已提交），或本节点失去 leader 时（M8，发布了新角色之后）——计数加一、
    /// 唤醒全部等待者。
    pub(super) fn wake_changelog(&self) {
        *lock(&self.changelog) += 1;
        self.changelog_cv.notify_all();
    }
}

/// 打开 Driver 的参数。实现注记（M8）：`snap_dir` = 下载的快照文件放在哪（`<data_dir>/meta`，3.4.1.7）；`adopt_local_plugins` =
/// `wmfs server --adopt-local-plugins`：插件清单不一致也当就绪 leader（之后由 cluster-mgr 把 GLOBAL plugins 改成本地清单，3.7.2 第 5 条）。
pub struct DriverOpts {
    pub node_id: u32,
    pub plugins: PluginListV1, // 本节点的插件清单：与已提交的 GLOBAL plugins 相同才算就绪（3.4.1.4 规则 3）
    pub snap_dir: PathBuf,
    pub adopt_local_plugins: bool,
}

/// raft 参数（3.4.1.1）。`applied` 来自 `raft_state["applied"]`（3.2.6 的写入规则 M4，坑 P9）。
pub fn config(id: u32, applied: u64) -> Config {
    Config {
        id: id as u64,
        election_tick: 10,
        heartbeat_tick: 2,
        applied,
        check_quorum: true,                     // leader 失去多数派后下台（坑 P16）
        pre_vote: true,                         // 被隔离后回来的节点不打断集群（坑 P16）
        read_only_option: ReadOnlyOption::Safe, // 绝不用 LeaseBased（3.4.1.6，坑 P7）
        max_size_per_msg: 512 << 10,            // limit_size 总会保留第一条：大条目靠批次上限 + RaftMsg 走 bulk 兜住
        max_inflight_msgs: 256,
        max_uncommitted_size: 64 << 20,
        max_committed_size_per_ready: 16 << 20, // 坑 P18
        ..Default::default()
    }
}
