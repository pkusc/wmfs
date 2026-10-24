//! `EmbeddedEngine` 的成员变更一侧（design.md 3.4.1.9，实现注记（M8））：给 cluster-mgr 的 `Membership` 用的 [`Members`] 实现
//! （提议 conf change、让出 leader、读与改节点记录），以及 `wmfs debug meta-hash` 要的本节点元数据哈希。是 `engine.rs` 的子模块，
//! 可以直接用 EmbeddedEngine 的私有字段。

use crossbeam_channel::{Receiver, bounded};
use raft::eraftpb::ConfChangeV2;

use super::EmbeddedEngine;
use crate::api::types::MetaHashReply;
use crate::base::types::ReqCtx;
use crate::base::{ErrCode, log};
use crate::meta::engine::txn::{TxnOp, txn_blocking_exclusive};
use crate::meta::raft::driver::{ProposeOutcome, RaftInput};
use crate::meta::raft::membership::Members;
use crate::meta::raft::snapshot;
use crate::meta::schema::keys::{self, T_NODE};
use crate::meta::schema::kv;
use crate::meta::schema::records::{NodeRecV1, decode_v1};
use crate::proto::meta::MetaReply;

impl Members for EmbeddedEngine {
    fn node_id(&self) -> u32 {
        self.node_id
    }
    fn nodes(&self) -> Vec<(u32, NodeRecV1)> {
        let mut out = Vec::new();
        let _ = self.local_scan(&mut |v| {
            for (k, b) in kv::scan_tag(v, T_NODE)? {
                out.push((kv::key_id(&k) as u32, decode_v1(&b)?));
            }
            Ok(())
        });
        out
    }
    fn propose_conf_change(&self, cc: ConfChangeV2) -> Receiver<ProposeOutcome> {
        let (done, rx) = bounded(1);
        let _ = self.shared.raft_tx.try_send(RaftInput::ConfChange { cc, done }); // 发不进去：rx 断开，下一轮重来
        rx
    }
    fn transfer_leader(&self, to: u32) {
        let _ = self.shared.raft_tx.try_send(RaftInput::TransferLeader(to as u64));
    }
    fn set_roles(&self, roles: &[(u32, u8)]) {
        let roles = roles.to_vec();
        let op: TxnOp = Box::new(move |v, _c| {
            for (id, role) in &roles {
                if let Some(mut n) = kv::get::<NodeRecV1>(v, &keys::node(*id))? {
                    n.meta_role = *role;
                    kv::put(v, &keys::node(*id), &n);
                }
            }
            kv::bump_map_epoch(v)?; // 改了节点记录：同批 map_epoch += 1（3.3.6）
            Ok(MetaReply::Ok)
        });
        // 改节点记录是拓扑类内部操作：exclusive，本批最后一个（3.4.1.4 规则 5）
        if let Err(e) = txn_blocking_exclusive(self, ReqCtx::internal(), op) {
            log::info("meta", format_args!("改节点记录的元数据角色失败：{e}（下一轮再试）"));
        }
    }
}

impl EmbeddedEngine {
    /// 本节点已应用的状态的哈希（`GET /api/v1/debug/meta-hash`，每个节点都回答本节点的）：applied 与 kv 表来自同一个读事务。
    pub fn meta_hash(&self) -> Result<MetaHashReply, ErrCode> {
        let (a, keys, hash) = snapshot::kv_hash(&self.shared.db)?;
        let node_id = self.node_id;
        Ok(MetaHashReply { node_id, applied: a.index, state_version: a.state_version, keys, hash })
    }
}
