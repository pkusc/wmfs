//! health 里的元数据角色与 leader 的地址、NOT_LEADER 的 `leader_hint`（design.md 7.1 的 M3 注记、3.3.1）。实现注记（M11）：从 server.rs
//! 挪出来（每个文件 ≤ 400 行）。

use raft::StateRole;
use wmfs_api::types::NodeAddrs;

use crate::meta::engine::EmbeddedEngine;
use crate::meta::engine::txn::{MetaEngine, RoleInfo};

/// health 里的元数据角色与 leader 的地址（实现注记（M3），7.1）。
pub(super) fn meta_role(eng: &EmbeddedEngine, me: &NodeAddrs) -> (String, Option<NodeAddrs>) {
    let r = eng.role();
    let learner = r.role == StateRole::Follower && eng.status().learners.contains(&me.node_id);
    (role_name(&r, learner).to_string(), leader_addrs(r.leader_id, me, &|id| eng.node_addrs(id)))
}

/// leader 的地址（health 与 NOT_LEADER 的 `leader_hint` 共用）：不知道 leader（`leader_id` 为 0）→ None；是自己 → 本节点
/// 实际监听的地址；是别人 → `others(id)`（已应用的 NodeRecV1，查不到 → None。注意：`wmfs dev up --rdma` 改了 node.toml
/// 之后，KV 里的地址不会跟着变——那只影响 rdma 一项，control_scheme 的 tcp 地址不变）。
pub(super) fn leader_addrs(
    leader_id: u32,
    me: &NodeAddrs,
    others: &dyn Fn(u32) -> Option<NodeAddrs>,
) -> Option<NodeAddrs> {
    match leader_id {
        0 => None,
        id if id == me.node_id => Some(me.clone()),
        id => others(id),
    }
}

/// NOT_LEADER 的 `leader_hint`（实现注记（M4），3.3.1、3.3.7）：leader 在 control_scheme 上的 rpc 地址（"127.0.0.1:19100"）。
pub(super) fn hint_of(leader: Option<NodeAddrs>, scheme: &str) -> Option<String> {
    leader?.addrs.into_iter().find(|(s, _)| s == scheme).map(|(_, a)| a)
}

/// `meta_role` 的取值：`leader` = **就绪**的 leader（可以服务请求，`wmfs dev wait leader` 等的就是它）；`candidate` =
/// 正在选举，或已当选但还没就绪；`follower` / `learner`。
fn role_name(r: &RoleInfo, learner: bool) -> &'static str {
    match r.role {
        StateRole::Leader if r.ready => "leader",
        StateRole::Leader | StateRole::Candidate | StateRole::PreCandidate => "candidate",
        StateRole::Follower if learner => "learner",
        StateRole::Follower => "follower",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_role_names_follow_7_1() {
        let r = |role, ready| RoleInfo { role, term: 2, leader_id: 1, ready };
        assert_eq!(role_name(&r(StateRole::Leader, true), false), "leader");
        assert_eq!(
            role_name(&r(StateRole::Leader, false), false),
            "candidate",
            "已当选、还没就绪的 leader 不算 leader"
        );
        assert_eq!(role_name(&r(StateRole::Candidate, false), false), "candidate");
        assert_eq!(role_name(&r(StateRole::PreCandidate, false), false), "candidate");
        assert_eq!(role_name(&r(StateRole::Follower, false), false), "follower");
        assert_eq!(role_name(&r(StateRole::Follower, false), true), "learner");
    }

    #[test]
    fn leader_hint_is_the_leaders_control_scheme_address() {
        let node = |id: u32, port: u16| NodeAddrs {
            node_id: id,
            addrs: vec![("mem".into(), format!("mem-{id}")), ("tcp".into(), format!("127.0.0.1:{port}"))],
            mgmt_addr: String::new(),
        };
        let me = node(1, 19100);
        let others = |id: u32| (id == 2).then(|| node(2, 19200));
        let hint = |leader_id| hint_of(leader_addrs(leader_id, &me, &others), "tcp");
        assert_eq!(hint(0), None, "不知道 leader");
        assert_eq!(hint(1), Some("127.0.0.1:19100".into()), "leader 是自己：本节点实际监听的地址");
        assert_eq!(
            hint(2),
            Some("127.0.0.1:19200".into()),
            "leader 是别人：已应用的 NodeRecV1 里 control_scheme 的那一项"
        );
        assert_eq!(hint(3), None, "集群图里查不到这个节点");
        assert_eq!(hint_of(leader_addrs(2, &me, &others), "mem"), Some("mem-2".into()), "按 control_scheme 挑地址");
        assert_eq!(hint_of(leader_addrs(2, &me, &others), "udp"), None, "leader 没有这种传输");
        assert_eq!(leader_addrs(1, &me, &others), Some(me.clone()), "health 里的 leader 地址");
    }
}
