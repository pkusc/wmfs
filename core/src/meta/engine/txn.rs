//! 元数据事务模型（design.md 3.5.1；`TxnOp`/`TxnRequest` 见 3.4.1.4；`RoleInfo`/`EngineStatus` 见 3.12）。
//!
//! 隔离级别 = 严格可串行化（真正的串行执行，回复在提交之后发出）；原子性 = 一个闭包的全部写入进同一个 raft 条目；
//! 持久性 = raft 多数派 fsync。authz、quota_admission、placement 三个钩子都在闭包**内部**、对着同一个视图调用，
//! 判定与修改之间没有 TOCTOU 窗口。执行器闭包里只允许按已知 key 点查（没有 scan），不得做网络 IO、不得睡眠。
//!
//! 接口骨架（M0）：签名以这里为准；实现（M3）在同目录的 engine.rs、executor.rs、view.rs、read_gate.rs、dedup.rs。

use crate::base::ErrCode;
use crate::base::types::ReqCtx;
use crate::hooks::manifest::Manifest;
use crate::hooks::pipeline::Pipeline;
use crate::meta::attrs::AttrResolver;
use crate::meta::schema::records::ClusterSettingsV1;
use crate::proto::meta::MetaReply;
use crate::proto::peer::ClusterMap;

pub trait KvRead {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ErrCode>;
}

#[allow(clippy::type_complexity)] // 返回类型按 design.md 3.5.1 原样
pub trait KvScan: KvRead {
    /// [start, end) 升序，最多 limit 条；只在只读视图上提供（执行器闭包拿不到它，3.4.1.4 规则 1）
    fn scan(&self, start: &[u8], end: &[u8], limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ErrCode>;
}

pub trait KvTxn: KvRead {
    fn put(&mut self, key: &[u8], value: Vec<u8>);
    fn delete(&mut self, key: &[u8]);
}

pub type ReadFn<'a> = &'a mut dyn FnMut(&dyn KvScan, &OpCtx) -> Result<MetaReply, ErrCode>;

/// meta_engine 钩子（唯一）。首发实现 embedded-kv = raft-rs + redb（3.4.1，代码在 core/src/meta/，因为元数据 Raft 是契约）。
/// 将来的 TiKV / FoundationDB 插件：read 映射为它们的线性一致快照读；txn 映射为带写集的乐观事务（执行器仍是单写者，冲突时重跑闭包）。
pub trait MetaEngine: Send + Sync {
    fn manifest(&self) -> &'static Manifest;
    /// 线性一致读：回调被调用时已确认 leader 身份且 applied ≥ read index（阻塞调用者，只在 meta-read 线程、后台任务、管理 API 线程调用）
    fn read(&self, ctx: &ReqCtx, f: ReadFn) -> Result<MetaReply, ErrCode>;
    /// 读写事务：投递后立即返回；req.done 恰好被调用一次（提交并应用之后，或明确失败：NOT_LEADER / RETRY / 业务错误）
    fn txn(&self, req: TxnRequest);
    /// 角色、term、leader、就绪
    fn role(&self) -> RoleInfo;
    /// applied、commit、voters、learners（管理 API）
    fn status(&self) -> EngineStatus;
}

/// 内部调用者用：bounded(1) 包一层（阻塞到批次有了确定结果）。
pub fn txn_blocking(eng: &dyn MetaEngine, ctx: ReqCtx, op: TxnOp) -> Result<MetaReply, ErrCode> {
    let (tx, rx) = crossbeam_channel::bounded(1);
    let done = Box::new(move |r| {
        let _ = tx.try_send(r);
    });
    eng.txn(TxnRequest { ctx, op, done, exclusive: false });
    rx.recv().unwrap_or(Err(ErrCode::NOT_LEADER))
}

/// 闭包能用到的一切。插件拿不到 redb 本身。
pub struct OpCtx<'a> {
    pub req: &'a ReqCtx, // principal、session、seq、trace_id、now（leader 墙钟，写进写集，follower 不再取时间）
    pub pipeline: &'a Pipeline, // authz / quota / placement / policy 钩子
    pub attrs: &'a AttrResolver, // 目录属性解析（带 attr_epoch 缓存，3.8）
    pub map: &'a ClusterMap, // 批次开始时的集群图快照（placement 用）
    pub settings: &'a ClusterSettingsV1,
}

// ---- 3.4.1.4 串行执行器的请求 ----

/// 执行器闭包看到的操作级覆盖层视图：只有 get/put/delete，没有 scan（3.4.1.4 规则 1）。
/// M3 的批次覆盖层（engine/view.rs）实现 `KvTxn`；命名空间代码一律写成 `fn op(v: &mut dyn KvTxn, c: &OpCtx, ..)`。
pub type OpView<'a> = dyn KvTxn + 'a;

pub type TxnOp = Box<dyn FnOnce(&mut OpView, &OpCtx) -> Result<MetaReply, ErrCode> + Send>;

pub struct TxnRequest {
    pub ctx: ReqCtx, // principal、session、seq（内部操作为 0）、trace_id、now（leader 墙钟，写进写集）
    pub op: TxnOp,
    pub done: Box<dyn FnOnce(Result<MetaReply, ErrCode>) + Send>, // RPC 层：编码后经 Responder 回复（只 try_send，不碰 socket）；内部调用者：发进 bounded(1)
    pub exclusive: bool,                                          // 集群拓扑类内部操作：必须是本批最后一个（规则 5）
}

// ---- 3.12 ----

/// ready 见 3.4.1.4 规则 3：`state == Leader && apply_to_current_term() && 插件清单一致`。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RoleInfo {
    pub role: raft::StateRole,
    pub term: u64,
    pub leader_id: u32,
    pub ready: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct EngineStatus {
    pub applied: u64,
    pub commit: u64,
    pub voters: Vec<u32>,
    pub learners: Vec<u32>,
    pub progress: Vec<(u32, u64)>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    /// 一个借用外部 map 的视图：证明 TxnOp 能接受非 'static 的覆盖层（M3 的覆盖层借用 redb 读事务）。
    struct View<'m>(&'m mut BTreeMap<Vec<u8>, Vec<u8>>);
    impl KvRead for View<'_> {
        fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ErrCode> {
            Ok(self.0.get(key).cloned())
        }
    }
    impl KvTxn for View<'_> {
        fn put(&mut self, key: &[u8], value: Vec<u8>) {
            self.0.insert(key.to_vec(), value);
        }
        fn delete(&mut self, key: &[u8]) {
            self.0.remove(key);
        }
    }

    fn put_if_absent(v: &mut dyn KvTxn, key: &[u8]) -> Result<MetaReply, ErrCode> {
        let r: &dyn KvRead = v; // trait 向上转换：命名空间代码把 &mut dyn KvTxn 交给只读的辅助函数
        if r.get(key)?.is_some() {
            return Err(ErrCode::EEXIST);
        }
        v.put(key, b"1".to_vec());
        Ok(MetaReply::Ok)
    }

    fn empty_map() -> ClusterMap {
        ClusterMap {
            map_epoch: 1,
            cluster_id: [0; 16],
            leader: None,
            settings: ClusterSettingsV1::default(),
            features: Default::default(),
            nodes: vec![],
            targets: vec![],
            chains: vec![],
            classes: vec![],
            keys: vec![],
            datakeys: vec![],
            qos: vec![],
            evicted: vec![],
            plugins: Default::default(),
            warnings: vec![],
        }
    }

    #[test]
    fn txn_op_accepts_borrowed_views() {
        use crate::base::types::{Principal, Ts};
        use crate::hooks::traits::ConnIdentity;
        let conn = std::sync::Arc::new(ConnIdentity {
            key_id: 1,
            role: crate::base::types::Role::Admin,
            tenant: 0,
            uid_ranges: vec![(0, u32::MAX)],
            root_squash: false,
            path_caps: vec![],
        });
        let who = Principal { uid: 0, gid: 0, groups: vec![], tenant: 0, job_id: 0, conn };
        let req =
            ReqCtx { who, session: 0, seq: 0, pid: 0, trace_id: 0, now: Ts::default(), now_ms: 0, internal: true };
        let (pipeline, attrs, map, settings) = (
            Pipeline::new(crate::hooks::registry::PluginSet::new(&[])),
            AttrResolver {},
            empty_map(),
            ClusterSettingsV1::default(),
        );
        let ctx = OpCtx { req: &req, pipeline: &pipeline, attrs: &attrs, map: &map, settings: &settings };

        let mut kv = BTreeMap::new();
        let op: TxnOp = Box::new(|v, _c| put_if_absent(v, b"k"));
        let mut view = View(&mut kv); // 借用局部变量的视图：执行器的覆盖层也是这样借用 redb 读事务的
        assert_eq!(op(&mut view, &ctx), Ok(MetaReply::Ok));
        let again: TxnOp = Box::new(|v, _c| put_if_absent(v, b"k"));
        assert_eq!(again(&mut view, &ctx), Err(ErrCode::EEXIST));
        assert_eq!(kv.get(b"k".as_slice()), Some(&b"1".to_vec()));
    }
}
