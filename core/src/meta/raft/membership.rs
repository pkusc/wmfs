//! 成员变更的决定（design.md 3.4.1.9，坑 P13、P21）：cluster-mgr 在就绪的 leader 上每秒调用一次 [`Membership::tick`]。
//!
//! 节点记录 NodeRecV1 是"指定"，raft 的成员配置（raft 线程发布的 `EngineStatus`）是"实际"，这里一步一步让实际跟上指定——每一步
//! 都按当前状态重新算（[`plan`]，纯函数、可以单测），所以重复执行无害，换了 leader 也接着做：
//! 1. `state = Removed` 还在配置里 → `RemoveNode`；移除的是 voter、剩下的 voter 不到 3 个而有已追上的 learner → 同一个 joint 变更里
//!    顺带提升它（3.4.1.9 "移除"）。要移除的是 leader 自己 → 先把 leader 让给接替的人（[`heir`]）。
//! 2. 指定为元数据成员（`META_LEARNER`：join `--meta auto`；`META_VOTER`：`wmfs node promote`）还不在配置里 → `AddLearnerNode`
//!    （leader 发现它缺少已压缩的日志就发快照）。
//! 3. 提升：指定为 voter 且已追上（`EngineStatus::caught_up`）的 learner；另外 voter 不到 3 个、且 voter + 已追上的 learner ≥ 3 时，
//!    按节点号补足 3 个——**一个包含多个 `AddNode` 的 ConfChangeV2**（raft-rs 对多变更自动进入 joint consensus），1 voter 一步变 3
//!    voter，不经过"2 voter、任一宕机即不可用"的中间态；2 节点集群保持 1 voter + 1 learner（集群警告"元数据没有高可用"）。
//! 4. 本节点（leader）被 `wmfs node drain` 了 → 把 leader 让给接替的人；没有活着、追上了的 → 不让（不能把 leader 让给死掉的节点：
//!    raft-rs 在转让期间丢掉所有提议）。
//! 5. 记录跟上实际：成了 voter 的记 `META_VOTER`，移除完的记 `META_NONE`（一个内部批次，`map_epoch += 1`）。
//!
//! 同一时间只有一个在途的 conf change（raft-rs 的 `pending_conf_index`）：上一个没有结局之前不提新的；被 raft-rs 改写成空条目
//! （`Applied{ok: false}`，坑 P21）或结果不确定时，下一轮按当时的状态重新判断。**leave-joint 兜底**（坑 P13）：raft-rs 在 leader
//! 应用 joint 配置时自动追加退出 joint 的空 ConfChangeV2，但 leader 中途下台时可能没人退出——`voters_outgoing` 非空超过 5 s 且没有
//! 在途的 conf change，就提议空的 `ConfChangeV2::default()`（经 `RaftInput::ConfChange`，在 raft 线程的输入阶段 propose，坑 P1）。
//!
//! 为什么有 [`Members`] 这个 trait：决定要做的几件事（提议 conf change、让出 leader、改节点记录）由它交出去，`EmbeddedEngine`
//! （真的 cluster-mgr）与确定性模拟（`tests/it/raft_sim_cluster.rs`，直接驱动 Driver）各实现一次——模拟跑的就是这里的 `tick`
//! （含 leave-joint 兜底），不是一份抄来的副本。时间由调用方给（`now_ms`）：模拟按步数走，不依赖真实时间。

use std::collections::BTreeSet;

use crossbeam_channel::{Receiver, TryRecvError};
use raft::eraftpb::{ConfChangeSingle, ConfChangeType, ConfChangeV2};

use super::types::ProposeOutcome;
use crate::base::log;
use crate::meta::engine::txn::EngineStatus;
use crate::meta::schema::records::{
    META_LEARNER, META_NONE, META_VOTER, NODE_ACTIVE, NODE_DRAINING, NODE_REMOVED, NodeRecV1,
};

/// joint 状态最多等多久就兜底退出（坑 P13）。
const JOINT_GRACE_MS: u64 = 5000;

/// 下一步（见文件头）。
#[derive(Debug, PartialEq)]
pub enum Step {
    Conf(ConfChangeV2, String),
    Transfer(u32, String),
    Roles(Vec<(u32, u8)>),
}

/// 成员变更要读的节点记录与要做的几件事（见文件头为什么是 trait）。
pub trait Members {
    fn node_id(&self) -> u32;
    /// 本地已应用的全部节点记录（成员变更只在 leader 上算，leader 的本地状态就是最新的已提交状态）。
    fn nodes(&self) -> Vec<(u32, NodeRecV1)>;
    fn propose_conf_change(&self, cc: ConfChangeV2) -> Receiver<ProposeOutcome>;
    fn transfer_leader(&self, to: u32);
    /// 把这些节点记录的 `meta_role` 改成给的值（内部批次，`map_epoch += 1`）。
    fn set_roles(&self, roles: &[(u32, u8)]);
}

/// cluster-mgr 里的成员变更状态：在途的 conf change、joint 状态从什么时候开始。
#[derive(Default)]
pub struct Membership {
    inflight: Option<(Receiver<ProposeOutcome>, String)>,
    joint_since: Option<u64>,
}

impl Membership {
    /// 就绪的 leader 上每秒一次（见文件头）：`st` 是 raft 线程发布的状态，`now_ms` 是单调时钟（模拟里是逻辑时间）。
    pub fn tick(&mut self, m: &dyn Members, st: &EngineStatus, now_ms: u64) {
        if let Some((rx, what)) = &self.inflight {
            match rx.try_recv() {
                Ok(o) => log::info("raft", format_args!("成员变更「{what}」的结局：{o:?}")),
                Err(TryRecvError::Empty) => return, // 还没有结局：不提新的
                Err(TryRecvError::Disconnected) => {}
            }
            self.inflight = None;
        }
        if !st.voters_outgoing.is_empty() {
            let since = *self.joint_since.get_or_insert(now_ms);
            if now_ms.saturating_sub(since) > JOINT_GRACE_MS {
                log::warn(
                    "raft",
                    "joint 配置超过 5 s 没有退出（leader 中途换过？）：提议空的 ConfChangeV2 退出 joint（坑 P13）",
                );
                self.inflight = Some((m.propose_conf_change(ConfChangeV2::default()), "leave-joint 兜底".into()));
                self.joint_since = None;
            }
            return;
        }
        self.joint_since = None;
        match plan(st, &m.nodes(), m.node_id()) {
            Some(Step::Conf(cc, what)) => {
                log::info("raft", format_args!("成员变更：{what}"));
                self.inflight = Some((m.propose_conf_change(cc), what));
            }
            Some(Step::Transfer(to, why)) => {
                log::info("raft", format_args!("{why}：把 leader 让给节点 {to}"));
                m.transfer_leader(to);
            }
            Some(Step::Roles(r)) => m.set_roles(&r),
            None => {}
        }
    }
}

fn change(list: &[(ConfChangeType, u32)]) -> ConfChangeV2 {
    let mut cc = ConfChangeV2::default();
    let one = |(t, id): &(ConfChangeType, u32)| {
        let mut s = ConfChangeSingle::default();
        s.set_change_type(*t);
        s.node_id = *id as u64;
        s
    };
    cc.set_changes(list.iter().map(one).collect::<Vec<_>>().into());
    cc
}

/// 接替 leader 的人（drain、移除 leader 自己、SIGTERM 时让位都用它）：不是自己、没在 drain/移除、最近一个选举超时里来过消息
/// （`EngineStatus::active`）、已追上的 voter 里 matched 最大的；没有 → None（不让位）。
pub fn heir(st: &EngineStatus, nodes: &[(u32, NodeRecV1)], me: u32) -> Option<u32> {
    let active = |v: &u32| nodes.iter().any(|(i, n)| i == v && n.state == NODE_ACTIVE);
    st.voters
        .iter()
        .copied()
        .filter(|v| *v != me && active(v) && st.active.contains(v) && st.caught_up(*v))
        .max_by_key(|v| st.matched(*v))
}

/// 下一步该做什么（见文件头的 1–5，按顺序第一个成立的）；什么也不用做 → None。`st` 是 leader 发布的状态（有 progress）。
pub fn plan(st: &EngineStatus, nodes: &[(u32, NodeRecV1)], me: u32) -> Option<Step> {
    let voters: BTreeSet<u32> = st.voters.iter().copied().collect();
    let member = |id: u32| voters.contains(&id) || st.learners.contains(&id);
    let rec = |id: u32| nodes.iter().find(|(i, _)| *i == id).map(|(_, n)| n);
    // 指定为 `role`、没在 drain/移除、已追上的 learner
    let learners_ok = |role: u8| -> Vec<u32> {
        st.learners
            .iter()
            .copied()
            .filter(|l| rec(*l).is_some_and(|n| n.state == NODE_ACTIVE && n.meta_role == role) && st.caught_up(*l))
            .collect()
    };
    for (id, _) in nodes.iter().filter(|(id, n)| n.state == NODE_REMOVED && member(*id)) {
        if *id == me {
            return heir(st, nodes, me).map(|h| Step::Transfer(h, format!("要移除的是 leader 自己（节点 {id}）")));
        }
        let mut list = vec![(ConfChangeType::RemoveNode, *id)];
        let heir_learner = learners_ok(META_VOTER).into_iter().chain(learners_ok(META_LEARNER)).next();
        if voters.contains(id)
            && voters.len() <= 3
            && let Some(l) = heir_learner
        {
            list.push((ConfChangeType::AddNode, l)); // 3.4.1.9：有已追上的 learner 就在同一个 joint 变更里顺带提升它
        }
        if voters.contains(id) && voters.len() == 1 && list.len() == 1 {
            continue; // 不能移除最后一个 voter（管理 API 已拒绝；这里再挡一次）
        }
        let also = if list.len() > 1 { "（顺带提升一个 learner）" } else { "" };
        return Some(Step::Conf(change(&list), format!("移除节点 {id}{also}")));
    }
    let wanted = |n: &NodeRecV1| n.meta_role != META_NONE && n.state != NODE_REMOVED;
    if let Some((id, _)) = nodes.iter().find(|(id, n)| wanted(n) && !member(*id)) {
        return Some(Step::Conf(
            change(&[(ConfChangeType::AddLearnerNode, *id)]),
            format!("节点 {id} 以 learner 加入"),
        ));
    }
    let mut up = learners_ok(META_VOTER);
    let auto = learners_ok(META_LEARNER);
    if voters.len() + up.len() < 3 && voters.len() + up.len() + auto.len() >= 3 {
        up.extend(auto.iter().take(3 - voters.len() - up.len()));
    }
    if !up.is_empty() {
        let list: Vec<_> = up.iter().map(|id| (ConfChangeType::AddNode, *id)).collect();
        return Some(Step::Conf(change(&list), format!("提升 {up:?} 为 voter（voter {:?}）", st.voters)));
    }
    if rec(me).is_some_and(|n| n.state == NODE_DRAINING)
        && let Some(h) = heir(st, nodes, me)
    {
        return Some(Step::Transfer(h, format!("本节点（{me}）正在 drain")));
    }
    let roles: Vec<(u32, u8)> = nodes
        .iter()
        .filter_map(|(id, n)| match (voters.contains(id), n.state == NODE_REMOVED && !member(*id)) {
            (true, _) if n.meta_role != META_VOTER => Some((*id, META_VOTER)),
            (_, true) if n.meta_role != META_NONE => Some((*id, META_NONE)),
            _ => None,
        })
        .collect();
    (!roles.is_empty()).then_some(Step::Roles(roles))
}

#[cfg(test)]
mod tests;
