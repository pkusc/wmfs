//! 链管理器（design.md 3.4.2.7、3.4.2.10；cluster-mgr 线程上、只在就绪的 leader 上）：每 200 ms 按 leader 内存里的心跳情况算一次
//! （`chain_plan.rs` 的纯函数），有要改的就提交一个 exclusive 内部批次——CAS：这些链与 target 的记录还是算的时候那样才写，写完
//! `map_epoch += 1`；不一样了（别的批次先改了）就什么也不写，下一轮重新算（3.4.1.4 规则 6）。
//!
//! - 计时重置（3.4.1.10）：本任期就绪之后 1 个 target 租约之内不判离线、不修不迁（换 leader 之后 target 的最近心跳都当作"就绪那一刻"，
//!   移除最多推迟一个租约周期——推迟是安全的，提前才危险）；重启的处理要等有重启过的 Serving 成员的链都来过心跳（`chain_plan.rs` 的
//!   `Facts.grace`：整链重启要等听全了才选幸存者，3.4.2.7；单节点重启后的唯一幸存者马上就能确认，不必干等 8 s）。
//! - 调小租约的过渡（ADR-0038 第 5 条）：判定过期用 max(当前值, GLOBAL `settings_prev`)；`settings_prev` 在本任期第一次看到它（再调小
//!   一次就从头计时）、再过一个旧租约周期之后由一个批次删掉（新 leader 上任时看到它还在：照样按 max 计时，上任一个旧租约周期之后再删）。
//! - [`sync_done`]（`NodeOp::SyncDone`，3.4.2.8 第 4 步）与 [`force_serving`]（`wmfs chain force-serving`）也在这里。
//! - 指标：`wmfs_chains_degraded`（Serving 成员少于期望的链）、`wmfs_chains_unavailable`（没有 Serving 成员的链）、
//!   `wmfs_chain_members{chain,state}`（7.3）。只有就绪的 leader 报；不是（或不再是）就绪的 leader → 全部归零（换了 leader 旧值不留着）。
//! - 转发失败的报告（3.4.2.10）只认报告者最近 FRESH_MS 内的心跳里的（它停了心跳，最后一次报告不能一直当作"持续"的证据），时长按
//!   同一台机器的钟算（`chain_plan::FwdFailure`）。

use std::collections::HashMap;
use std::sync::Arc;

use wmfs_api::metrics::{self, Gauge};

use super::chain_plan::{self, FRESH_MS, Facts, Plan, sole};
use super::chains::{SERVING, SYNCING, serving_count};
use super::settings::max_each;
use super::targets::TargetUsage;
use crate::base::types::ReqCtx;
use crate::base::{ErrCode, Failed, clock, log};
use crate::meta::changelog::{self, Change};
use crate::meta::engine::EmbeddedEngine;
use crate::meta::engine::txn::{KvTxn, MetaEngine, OpCtx, TxnOp, txn_blocking_exclusive};
use crate::meta::schema::keys::{self, G_NEXT_CHAIN, G_SETTINGS_PREV};
use crate::meta::schema::kv;
use crate::meta::schema::records::{ChainRecV1, ClusterSettingsV1, TargetRecV1, decode_v1};
use crate::proto::meta::MetaReply;
use crate::proto::peer::ClusterMap;

/// 链管理器在 cluster-mgr 线程上的状态（换了 term 就重来）。
#[derive(Default)]
pub struct ChainMgr {
    term: u64,
    ready_since: u64,
    prev_since: Option<((u64, u64), u64)>, // 第一次看到 settings_prev 时的 (当前的 target、会话租约) 与那一刻
    gauges: HashMap<(u32, u8), Arc<Gauge>>,
    changed: HashMap<u32, u64>, // 链 → 本 leader 最近一次改它的时刻：这之前（再加一个心跳周期）报上来的转发失败已经过时
}

/// 链改过之后多久之内收到的"转发不到后继"不算（数据节点拿到新集群图、清掉旧记录之后，下一次心跳才不再报它）。
const STALE_REPORT_MS: u64 = 1500;

impl ChainMgr {
    /// 一轮（见文件头）。cluster-mgr 每一轮都调用（不是就绪的 leader 时只把指标归零）。
    pub fn tick(&mut self, eng: &EmbeddedEngine, usage: &TargetUsage) {
        let (role, now) = (eng.role(), clock::mono_ms());
        if !role.ready {
            if self.term != 0 {
                self.zero_gauges();
                self.term = 0; // 再当选（哪怕同一任期）时从头计时：推迟决定是安全的
            }
            return;
        }
        if role.term != self.term {
            (self.term, self.ready_since, self.prev_since) = (role.term, now, None);
            self.changed.clear();
        }
        let Ok(map) = eng.cluster_map() else { return };
        self.gauges(&map);
        // settings_prev 读不出来、解不开：这一轮什么也不做——绝不退回到调小之后的较短租约（ADR-0038 第 5 条）
        let prev = match eng.local_get(&keys::global(G_SETTINGS_PREV)) {
            Ok(b) => b.map(|b| decode_v1::<ClusterSettingsV1>(&b).map(|p| (b, p))).transpose(),
            Err(e) => Err(e),
        };
        let prev = match prev {
            Ok(p) => p,
            Err(e) => return log::error("chain", format_args!("读 settings_prev 失败：{e}（这一轮链管理器不做决定）")),
        };
        let settings = prev.as_ref().map_or(map.settings.clone(), |(_, p)| max_each(&map.settings, p));
        if let Some((bytes, p)) = &prev {
            // 再调小一次（当前的租约变了）就从头计时：要等的是最近这次修改之后的一个旧租约周期
            let key = (map.settings.target_lease_ms, map.settings.session_lease_ms);
            if self.prev_since.is_none_or(|(k, _)| k != key) {
                self.prev_since = Some((key, now));
            }
            let since = self.prev_since.map_or(now, |(_, t)| t);
            let cur = &map.settings;
            let longer = |old: u64, now: u64| if old > now { old } else { 0 }; // 只等被调小了的那个租约的旧周期
            let wait =
                longer(p.target_lease_ms, cur.target_lease_ms).max(longer(p.session_lease_ms, cur.session_lease_ms));
            if now - since >= wait {
                drop_prev(eng, bytes.clone());
            }
        } else {
            self.prev_since = None;
        }
        let grace = now - self.ready_since < settings.target_lease_ms; // 计时重置：就绪之后一个租约周期之内不判离线（chain_plan.rs）
        let alive = usage.seen(role.term, now, settings.target_lease_ms);
        let after_change = |c: u32, at: u64| self.changed.get(&c).is_none_or(|t| at > t + STALE_REPORT_MS);
        let mut failures = usage.failures();
        failures.retain(|x| now.saturating_sub(x.heard_ms) <= FRESH_MS && after_change(x.chain, x.heard_ms));
        for x in &mut failures {
            x.lasted_ms += now.saturating_sub(x.heard_ms); // 加上 leader 收到之后过去的时间（同一台机器的 mono_ms）
        }
        let f = Facts { now_ms: clock::wall_ms(), settings, alive, failures, grace };
        let plan = chain_plan::plan(&map, &f);
        if plan.is_empty() {
            return;
        }
        log::info(
            "chain",
            format_args!(
                "链管理器：改 {} 条链、{} 个 target，新建 {} 条链",
                plan.chains.len(),
                plan.targets.len(),
                plan.new_chains.len()
            ),
        );
        let ids: Vec<u32> = plan.chains.iter().map(|(id, _)| *id).collect();
        match apply(eng, &map, plan) {
            Ok(()) => self.changed.extend(ids.into_iter().map(|id| (id, now))),
            Err(ErrCode::RETRY) => {}
            Err(e) => log::warn("chain", format_args!("链管理器的批次没有提交：{e}（下一轮重新算）")),
        }
    }

    fn gauges(&mut self, map: &ClusterMap) {
        let degraded = map.chains.iter().filter(|(_, c)| serving_count(c) < c.replicas as usize).count();
        let down = map.chains.iter().filter(|(_, c)| serving_count(c) == 0).count();
        degraded_gauge().set(degraded as f64);
        unavailable_gauge().set(down as f64);
        for (id, c) in &map.chains {
            for (state, name) in [(SERVING, "serving"), (SYNCING, "syncing")] {
                let g = self.gauges.entry((*id, state)).or_insert_with(|| {
                    let chain = id.to_string();
                    metrics::gauge("wmfs_chain_members", "链的成员数", &[("chain", &chain), ("state", name)])
                });
                g.set(c.members.iter().filter(|(_, s)| *s == state).count() as f64);
            }
        }
    }

    fn zero_gauges(&self) {
        degraded_gauge().set(0.0);
        unavailable_gauge().set(0.0);
        self.gauges.values().for_each(|g| g.set(0.0));
    }
}

fn degraded_gauge() -> Arc<Gauge> {
    metrics::gauge("wmfs_chains_degraded", "Serving 成员少于声明副本数的链（只有就绪的 leader 报）", &[])
}
fn unavailable_gauge() -> Arc<Gauge> {
    metrics::gauge("wmfs_chains_unavailable", "没有 Serving 成员（不可用）的链（只有就绪的 leader 报）", &[])
}

/// 一条记录还是 `old` 那样吗（CAS）。
fn same<T: serde::Serialize>(v: &dyn KvTxn, key: &[u8], old: &T) -> Result<bool, ErrCode> {
    Ok(v.get(key)? == Some(crate::meta::schema::records::encode_v1(old)))
}

/// 把一轮的决定写进一个 exclusive 批次（见文件头）。
fn apply(eng: &EmbeddedEngine, map: &ClusterMap, plan: Plan) -> Result<(), ErrCode> {
    let old_c: Vec<(u32, ChainRecV1)> = map.chains.clone();
    let old_t: Vec<(u32, TargetRecV1)> = map.targets.clone();
    let op: TxnOp = Box::new(move |v, _c| {
        for (id, _) in &plan.chains {
            let old = old_c.iter().find(|(i, _)| i == id).map(|(_, c)| c).ok_or(ErrCode::RETRY)?;
            if !same(v, &keys::chain(*id), old)? {
                return Err(ErrCode::RETRY);
            }
        }
        for (id, _) in &plan.targets {
            let old = old_t.iter().find(|(i, _)| i == id).map(|(_, t)| t).ok_or(ErrCode::RETRY)?;
            if !same(v, &keys::target(*id), old)? {
                return Err(ErrCode::RETRY);
            }
        }
        for (id, c) in &plan.chains {
            kv::put(v, &keys::chain(*id), c);
        }
        for (id, t) in &plan.targets {
            kv::put(v, &keys::target(*id), t);
        }
        for c in &plan.new_chains {
            let id = u32::try_from(kv::next_id(v, G_NEXT_CHAIN)?).map_err(|_| ErrCode::ENOSPC)?;
            kv::put(v, &keys::chain(id), c);
        }
        kv::bump_map_epoch(v)?;
        Ok(MetaReply::Ok)
    });
    txn_blocking_exclusive(eng as &dyn MetaEngine, ReqCtx::internal(), op).map(|_| ())
}

/// 删掉 `settings_prev`（还是 `bytes` 那个值才删）。
fn drop_prev(eng: &EmbeddedEngine, bytes: Vec<u8>) {
    let op: TxnOp = Box::new(move |v, _c| {
        if v.get(&keys::global(G_SETTINGS_PREV))? == Some(bytes) {
            v.delete(&keys::global(G_SETTINGS_PREV));
        }
        Ok(MetaReply::Ok)
    });
    if txn_blocking_exclusive(eng as &dyn MetaEngine, ReqCtx::internal(), op).is_ok() {
        log::info("chain", "旧租约周期已过：删掉 settings_prev（ADR-0038 第 5 条）");
    }
}

/// `NodeOp::SyncDone`（3.4.2.8 第 4 步）：链版本没变、`target` 紧跟最后一个 Serving 成员 → 变 Serving（`ver += 1`）；否则 `RETRY`
/// （Syncing 成员重跑同步）。
pub fn sync_done(eng: &dyn MetaEngine, ctx: &ReqCtx, target: u32, chain: u32, chain_ver: u64) -> Result<(), ErrCode> {
    let op: TxnOp = Box::new(move |v, _c| {
        let mut c: ChainRecV1 = kv::get(v, &keys::chain(chain))?.ok_or(ErrCode::RETRY)?;
        if !chain_plan::promote(&mut c, target, chain_ver) {
            return Err(ErrCode::RETRY);
        }
        kv::put(v, &keys::chain(chain), &c);
        kv::bump_map_epoch(v)?;
        Ok(MetaReply::Ok)
    });
    let mut c = ctx.clone();
    (c.seq, c.internal) = (0, true);
    txn_blocking_exclusive(eng, c, op).map(|_| ())
}

/// `wmfs chain force-serving <chain> <target>`（管理员明确接受可能的数据丢失，3.4.2.7 最后一行）：`target`（链上的成员，或最近被移除的）
/// 成为唯一 Serving，`incarnation += 1`，其余全部 Syncing；写 changelog `ChainForceServing`。
pub fn force_serving(v: &mut dyn KvTxn, c: &OpCtx, chain: u32, target: u32) -> Result<ChainRecV1, Failed> {
    let no = || Failed::new(ErrCode::ENOENT, format!("没有链 {chain}"));
    let mut rec: ChainRecV1 = kv::get(v, &keys::chain(chain))?.ok_or_else(no)?;
    let known = rec.members.iter().any(|(t, _)| *t == target) || rec.removed.iter().any(|(t, _)| *t == target);
    if !known {
        let msg = format!("target {target} 不是链 {chain} 的成员，也不在最近移除的里（wmfs chain show {chain}）");
        return Err(Failed::new(ErrCode::EINVAL, msg));
    }
    sole(&mut rec, target);
    rec.ver += 1;
    kv::put(v, &keys::chain(chain), &rec);
    kv::bump_map_epoch(v)?;
    let extra = vec![("chain".into(), chain.to_string()), ("target".into(), target.to_string())];
    changelog::append(v, c, Change { op: "ChainForceServing", extra, ..Default::default() })?;
    Ok(rec)
}
