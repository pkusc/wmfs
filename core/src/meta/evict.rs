//! 会话租约到期与两阶段驱逐（design.md 3.4.4，ADR-0023；cluster-mgr 线程上、只在就绪的 leader 上，每 1 s 一轮）。
//!
//! - **到期**：距 leader 最近一次收到它的心跳（`sessions::Live`，本任期记下的）超过 `session_lease` → 第一阶段。换了 leader（或刚
//!   就绪）时计时重置为就绪那一刻（3.4.1.10：驱逐最多推迟一个租约周期，推迟是安全的，提前才危险）。调小租约的过渡（ADR-0038
//!   第 5 条）：判定用 max(当前值, GLOBAL `settings_prev`)；settings_prev 由链管理器在一个旧租约周期之后删掉（`chain_mgr.rs`）。
//! - **第一阶段**（[`evict`]，一个批次；手动驱逐 `POST /api/v1/sessions/<id>/evict` 与撤销 key 也走它）：`state = Evicting`、
//!   `evict_since_ms`、`map_epoch += 1`、`EVICTED(session) = {evicted_ms, map_epoch: 新值}`、changelog `SessionEvicted`
//!   （`extra` 的 `why` 是原因）。之后它的任何请求 → `SESSION_EVICTED`；数据节点拿到新集群图就拒绝它的 cap（`data/service.rs`，
//!   io 线程执行时校验）。
//! - **等待**：每个 Up 的 target 都在本任期的心跳里报过 `map_epoch_seen ≥ evict_map_epoch`，或已超过 `target_lease`（同样取 max）
//!   没有心跳——它已自我隔离（I5），不可能再接受写；计时同样从就绪算起（`TargetUsage::caught_up`）。满足了写一行"数据节点确认"。
//! - **第二阶段**（一个批次，CAS：还是 Evicting）：释放它的锁（`locked_inos` 里每个 inode 的 LOCKS 记录中属于它的项，点操作，
//!   `locks::release_session`；M12 起 SetLock 维护这个列表），删除会话记录（EVICTED 记录保留 24 h，由 gc 删除；去重记录也交给 gc）。所以日志里"第二阶段：
//!   释放锁"一定在"数据节点确认"之后（`scripts/accept.d/m10.sh` 检查这个顺序；第二阶段没提交成、下一轮重来时"数据节点确认"会再写
//!   一行）。换了 leader 之后从 `state = Evicting` 与 `evict_map_epoch` 重新判断（3.4.1.10 的表）。
//! - 指标 `wmfs_meta_sessions`（7.3：Active 的会话数，只有就绪的 leader 报，每 1 s 刷新）。

use std::sync::Arc;

use wmfs_api::metrics::{self, Gauge};

use super::cluster::chains::TARGET_UP;
use super::cluster::settings::{max_each, prev_local};
use super::cluster::targets::TargetUsage;
use super::sessions::{Live, SESSION_ACTIVE, SESSION_EVICTING};
use crate::base::types::ReqCtx;
use crate::base::{ErrCode, clock, log};
use crate::meta::changelog::{self, Change};
use crate::meta::engine::EmbeddedEngine;
use crate::meta::engine::txn::{KvTxn, MetaEngine, OpCtx, TxnOp, txn_blocking};
use crate::meta::locks;
use crate::meta::schema::keys::{self, G_MAP_EPOCH, T_SESSION};
use crate::meta::schema::kv;
use crate::meta::schema::records::{EvictedV1, SessionRecV1, decode_v1};
use crate::proto::meta::MetaReply;

/// EVICTED 记录保留多久（3.4.4 第 4 步；`wmfs session list` 列出这段时间里的，之后由 gc 删除）。
pub const EVICTED_KEEP_MS: u64 = 24 * 3600 * 1000;

/// 驱逐在 cluster-mgr 线程上的状态（换了 term 就重来）。
#[derive(Default)]
pub struct Evictor {
    term: u64,
    ready_since: u64,
}

impl Evictor {
    /// 一轮（见文件头）。不是就绪的 leader 时什么也不做（再当选时从头计时）。
    pub fn tick(&mut self, eng: &EmbeddedEngine, live: &Live, usage: &TargetUsage) {
        let (role, now) = (eng.role(), clock::mono_ms());
        if !role.ready {
            self.term = 0;
            sessions_gauge().set(0.0);
            return;
        }
        if role.term != self.term {
            (self.term, self.ready_since) = (role.term, now);
        }
        // 集群图或 settings_prev 读不出来：这一轮不做决定——绝不退回到调小之后的较短租约
        let Ok(map) = eng.cluster_map() else { return };
        let Ok(prev) = prev_local(eng) else { return };
        let s = prev.map_or_else(|| map.settings.clone(), |(_, p)| max_each(&map.settings, &p)); // ADR-0038 第 5 条
        let mut rows = Vec::new();
        if eng.local_scan(&mut |v| kv::scan_tag(v, T_SESSION).map(|r| rows = r)).is_err() {
            return;
        }
        let mut active = 0;
        for (k, b) in rows {
            let (sid, Ok(rec)) = (kv::key_id(&k), decode_v1::<SessionRecV1>(&b)) else { continue };
            if rec.state == SESSION_ACTIVE {
                active += 1;
                let idle = now.saturating_sub(live.last_seen(sid, role.term).unwrap_or(0).max(self.ready_since));
                if idle > s.session_lease_ms {
                    let why = format!("{idle} ms 没有收到心跳（会话租约 {} ms）", s.session_lease_ms);
                    let _ = evict(eng, sid, why); // 失败（例如刚失去 leader）：下一轮再看
                }
                continue;
            }
            live.forget(sid); // 第一阶段已完成：它打开的文件不再挡着孤儿 GC
            let epoch = rec.evict_map_epoch;
            let mut up = map.targets.iter().filter(|(_, r)| r.state == TARGET_UP);
            if !up.all(|(t, _)| usage.caught_up(*t, role.term, epoch, now, self.ready_since, s.target_lease_ms)) {
                continue;
            }
            let how = "全部 Up 的 target 已报 map_epoch_seen ≥ 它，或已超过 target 租约没有心跳、自我隔离";
            log::info("evict", format_args!("会话 {sid:x}：数据节点确认（map_epoch {epoch}：{how}）"));
            match phase2(eng, sid, rec) {
                Ok(n) => {
                    let what = "删除会话记录（EVICTED 记录保留 24 h）";
                    log::info("evict", format_args!("会话 {sid:x} 驱逐第二阶段：释放锁（{n} 个 inode），{what}"));
                }
                Err(e) => log::warn("evict", format_args!("会话 {sid:x} 驱逐第二阶段没有提交：{e}（下一轮再试）")),
            }
        }
        sessions_gauge().set(active as f64);
    }
}

fn sessions_gauge() -> Arc<Gauge> {
    metrics::gauge("wmfs_meta_sessions", "Active 的会话数（只有就绪的 leader 报）", &[])
}

/// 驱逐第一阶段（一个批次，见文件头）：会话不存在 → `ENOENT`；已经在驱逐 → 什么也不做（也不写日志）。`why` 写进日志与 changelog。
pub fn evict(eng: &dyn MetaEngine, sid: u64, why: String) -> Result<(), ErrCode> {
    let w = why.clone();
    let op: TxnOp = Box::new(move |v, c| phase1(v, c, sid, &w).map(|()| MetaReply::Ok));
    let mut ctx = ReqCtx::internal();
    ctx.session = sid; // changelog 里的 session 是被驱逐的会话（seq 0 的内部操作：执行器不核对会话）
    match txn_blocking(eng, ctx, op) {
        Ok(_) => {}
        Err(ErrCode::SESSION_EVICTED) => return Ok(()), // 早已在驱逐（例如再手动驱逐一次）：批次什么也没写
        Err(e) => return Err(e),
    }
    log::warn(
        "evict",
        format_args!("会话 {sid:x} 驱逐第一阶段：{why}；集群图 map_epoch + 1，数据节点拿到新图后拒绝它的 cap"),
    );
    Ok(())
}

/// 执行器闭包：第一阶段。会话已经不是 Active → `SESSION_EVICTED`（什么也不写）。
fn phase1(v: &mut dyn KvTxn, c: &OpCtx, sid: u64, why: &str) -> Result<(), ErrCode> {
    let key = keys::session(sid);
    let mut s: SessionRecV1 = kv::get(v, &key)?.ok_or(ErrCode::ENOENT)?;
    if s.state != SESSION_ACTIVE {
        return Err(ErrCode::SESSION_EVICTED);
    }
    kv::bump_map_epoch(v)?;
    let epoch: u64 = kv::get(v, &keys::global(G_MAP_EPOCH))?.ok_or(ErrCode::INTERNAL)?;
    (s.state, s.evict_since_ms, s.evict_map_epoch) = (SESSION_EVICTING, c.req.now_ms, epoch);
    kv::put(v, &key, &s);
    kv::put(v, &keys::evicted(sid), &EvictedV1 { evicted_ms: c.req.now_ms, map_epoch: epoch });
    let extra = vec![
        ("session".into(), format!("{sid:x}")),
        ("why".into(), why.into()),
        ("map_epoch".into(), epoch.to_string()),
    ];
    changelog::append(v, c, Change { op: "SessionEvicted", extra, ..Default::default() })
}

/// 第二阶段（一个批次，CAS：会话记录还是 `rec`）：释放锁、删会话记录。返回释放了几个 inode 上的锁。
fn phase2(eng: &dyn MetaEngine, sid: u64, rec: SessionRecV1) -> Result<usize, ErrCode> {
    let n = rec.locked_inos.len();
    let op: TxnOp = Box::new(move |v, _c| {
        let key = keys::session(sid);
        if kv::get::<SessionRecV1>(v, &key)?.as_ref() != Some(&rec) {
            return Err(ErrCode::RETRY); // 别的批次先改了它：下一轮重新判断
        }
        locks::release_session(v, sid, &rec.locked_inos)?; // 别的客户端的锁轮询随后就能拿到（3.4.4 第 3 步）
        v.delete(&key);
        Ok(MetaReply::Ok)
    });
    txn_blocking(eng, ReqCtx::internal(), op).map(|_| n)
}
