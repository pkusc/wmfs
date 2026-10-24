//! `NodeOp::Join` 的 leader 一侧（design.md 3.4.3 第 2 步、3.7.2，ADR-0025 第 6 条）：新节点带着自己的插件清单、地址与 target 来加入。
//!
//! 1. 线性一致读：`JoinReq.plugins` 与已提交的 GLOBAL `plugins` 不同 → 拒绝并列出差异（`EINVAL`，原因在 `ErrorBody.msg` 里，例如
//!    「本节点多出 authz:deny-uid」；`wmfs join` 据此以退出码 3 结束，与运行中的节点发现不一致时相同）。
//! 2. 一个批次（拓扑类内部操作，`exclusive`，3.4.1.4 规则 5）：再核对一次清单；地址与一个没被移除的节点相同 → 是同一次 join 的重试
//!    就回原来的结果（[`same_join`]），否则 `EEXIST`（多半是它丢了数据目录：raft 节点号不能带着空存储重新用，坑 P14 的同类问题——先
//!    `wmfs node remove`）；分配 node_id 与 target_id，写 NodeRecV1
//!    （`--meta auto` 且现有元数据成员不到 3 个 → `meta_role = 1`，cluster-mgr 接着 `AddLearnerNode`；否则 0，只做数据节点）与各
//!    TargetRecV1，`map_epoch += 1`，changelog `NodeJoin`（实现注记（M8）：`extra` = `node`（新节点号），名字是 hostname）。
//! 3. 回 `Joined { node_id, target_ids, map }`：带上**完整的**集群图（新节点是 Server 角色）——它的 kv 是空的，靠它找到 leader 与快照源。
//!
//! 实现注记（M8）：新节点的 target 在 M8 只登记（TargetRecV1）；补进已有的链（Syncing）与以它为 head 的新链由 M9 的链管理器做。

use crossbeam_channel::bounded;

use super::chains::TARGET_UP;
use super::plugins;
use crate::base::types::ReqCtx;
use crate::base::{ErrCode, Failed};
use crate::meta::changelog::{self, Change};
use crate::meta::engine::txn::{KvTxn, MetaEngine, OpCtx, TxnOp, txn_blocking_exclusive};
use crate::meta::schema::keys::{self, G_NEXT_NODE, G_NEXT_TARGET, G_PLUGINS};
use crate::meta::schema::kv;
use crate::meta::schema::records::{
    META_LEARNER, META_NONE, NODE_ACTIVE, NODE_REMOVED, NodeRecV1, PluginListV1, TargetRecV1,
};
use crate::proto::meta::MetaReply;
use crate::proto::peer::{ClusterMap, JOIN_META_AUTO, JoinReq, NodeReply};

/// 元数据成员（`meta_role ≥ 1`）最多自动加到几个（3.4.3：现有元数据成员 < 3 才以 learner 加入）。
const META_MEMBERS: usize = 3;

/// 处理一个 Join（见文件头）；错误带给人看的原因。
pub fn join(eng: &dyn MetaEngine, ctx: &ReqCtx, req: JoinReq) -> Result<NodeReply, Failed> {
    let map = read_map(eng, ctx)?;
    if let Some(d) = plugins::diff(&req.plugins, &map.plugins.plugins) {
        return Err(Failed::new(ErrCode::EINVAL, format!("加入被拒绝：插件清单与集群不一致——{d}。{}", plugins::FIX)));
    }
    // 执行器的闭包只能回 MetaReply：分到的 (node_id, target_ids) 经这个通道带出来
    let (tx, rx) = bounded(1);
    let op: TxnOp = Box::new(move |v, c| {
        let r = add(v, c, &req);
        let code = r.as_ref().err().copied();
        let _ = tx.try_send(r);
        code.map_or(Ok(MetaReply::Ok), Err)
    });
    let mut c = ctx.clone();
    (c.seq, c.internal) = (0, true);
    txn_blocking_exclusive(eng, c, op).map_err(|e| match e {
        ErrCode::EEXIST => {
            Failed::new(e, "这个节点的地址已经属于一个没被移除的节点（它丢了数据目录？先 wmfs node remove 它）")
        }
        e => e.into(),
    })?;
    // 到这里批次确实有了结果（提交了，或者闭包什么也没写、回的是已提交的事实）才读闭包的结果：批次作废时上面已经返回了
    let (node_id, target_ids) = rx.try_recv().map_err(|_| Failed::from(ErrCode::INTERNAL))??;
    let map = read_map(eng, ctx)?;
    Ok(NodeReply::Joined { node_id, target_ids, map: Box::new(map) })
}

/// 线性一致读一张完整的集群图。
fn read_map(eng: &dyn MetaEngine, ctx: &ReqCtx) -> Result<ClusterMap, Failed> {
    let mut out = None;
    // 这个闭包只用来在 ReadIndex 之后的视图上取集群图快照，它的返回值丢弃
    eng.read(ctx, &mut |_v, c| {
        out = Some(c.map.clone());
        Ok(MetaReply::Ok)
    })?;
    out.ok_or_else(|| ErrCode::INTERNAL.into())
}

/// 第 2 步的闭包：返回 (node_id, target_ids)。
fn add(v: &mut dyn KvTxn, c: &OpCtx, req: &JoinReq) -> Result<(u32, Vec<u32>), ErrCode> {
    let committed = kv::get::<PluginListV1>(v, &keys::global(G_PLUGINS))?.unwrap_or_default();
    if plugins::diff(&req.plugins, &committed.plugins).is_some() {
        return Err(ErrCode::EINVAL); // 读与批次之间清单变了（极少）：照样拒绝
    }
    let live: Vec<&(u32, NodeRecV1)> = c.map.nodes.iter().filter(|(_, n)| n.state != NODE_REMOVED).collect();
    if let Some((id, n)) = live.iter().find(|(_, n)| n.addrs.iter().any(|a| req.addrs.contains(a))) {
        return same_join(c, *id, n, req).ok_or(ErrCode::EEXIST);
    }
    let members = live.iter().filter(|(_, n)| n.meta_role != META_NONE).count();
    let node_id = u32::try_from(kv::next_id(v, G_NEXT_NODE)?).map_err(|_| ErrCode::ENOSPC)?;
    let now = c.req.now_ms;
    let rec = NodeRecV1 {
        addrs: req.addrs.clone(),
        mgmt_addr: req.mgmt_addr.clone(),
        host_id: req.host_id.clone(),
        machine_id: req.machine_id.clone(),
        hostname: req.hostname.clone(),
        meta_role: if req.meta_role == JOIN_META_AUTO && members < META_MEMBERS { META_LEARNER } else { META_NONE },
        features: req.features.clone(),
        joined_ms: now,
        state: NODE_ACTIVE,
    };
    kv::put(v, &keys::node(node_id), &rec);
    let mut ids = Vec::new();
    for (path, labels, capacity, phys_devs) in &req.targets {
        let id = u32::try_from(kv::next_id(v, G_NEXT_TARGET)?).map_err(|_| ErrCode::ENOSPC)?;
        let t = TargetRecV1 {
            node: node_id,
            path: path.clone(),
            labels: labels.clone(),
            phys_devs: phys_devs.clone(),
            capacity: *capacity,
            boot_id: 0,
            state: TARGET_UP,
            last_up_ms: now,
            read_only: false,
        };
        kv::put(v, &keys::target(id), &t);
        ids.push(id);
    }
    kv::bump_map_epoch(v)?;
    let extra = vec![("node".to_string(), node_id.to_string())];
    changelog::append(v, c, Change { op: "NodeJoin", name: req.hostname.as_bytes(), extra, ..Default::default() })?;
    Ok((node_id, ids))
}

/// 同一次 join 的重试吗（实现注记（M8 审查））：批次已经提交、`Joined` 回复却丢了（超时、leader 刚提交完就崩），本地侧按 3.3.8 重试。
/// 认定条件：节点记录的地址、管理地址、主机号、machine_id、hostname 都与请求相同，target 的路径也相同，而且它的 target 一个都没心跳过
/// （boot_id 全是 0：那个节点从没启动过）——是就回原来的 (node_id, target_ids)，什么也不写。其余情况（丢了数据目录的旧节点，或者没有
/// target、分不清的）→ None，调用方照旧回 `EEXIST`。
fn same_join(c: &OpCtx, id: u32, n: &NodeRecV1, req: &JoinReq) -> Option<(u32, Vec<u32>)> {
    let same = n.addrs == req.addrs
        && n.mgmt_addr == req.mgmt_addr
        && n.host_id == req.host_id
        && n.machine_id == req.machine_id
        && n.hostname == req.hostname;
    let mine: Vec<&(u32, TargetRecV1)> = c.map.targets.iter().filter(|(_, t)| t.node == id).collect();
    let paths = mine.len() == req.targets.len() && mine.iter().zip(&req.targets).all(|((_, t), r)| t.path == r.0);
    let never_ran = !mine.is_empty() && mine.iter().all(|(_, t)| t.boot_id == 0);
    (same && paths && never_ran).then(|| (id, mine.iter().map(|(t, _)| *t).collect()))
}
