//! Driver 的"应用与发布"一侧（design.md 3.4.1.3 的 apply_entries/after_apply/publish_role、3.4.1.5、3.4.1.8、3.7.2）：
//! 按顺序应用已提交条目、认领在途提议、日志压缩、发布角色与状态、比对集群插件清单。是 `driver.rs` 的子模块，
//! 可以直接用 Driver 的私有字段。实现注记（M8）：另有快照的安装（规则 M5，[`Driver::install`]）与 raft 线程的指标。

use std::sync::Arc;
use std::sync::atomic::Ordering;

use protobuf::Message as _;
use raft::StateRole;
use raft::eraftpb::{ConfChangeV2, Entry, EntryType, SnapshotMetadata};
use redb::{Durability, WriteTransaction};
use wmfs_api::failpoint::{fail_point, fail_point_value};
use wmfs_api::metrics::{self, Counter, Gauge, Histogram};

use super::{ACTIVE_TICKS, COMPACT_EVERY, COMPACT_KEEP, Driver, ProposeOutcome};
use crate::base::{lock, log};
use crate::meta::cluster::plugins;
use crate::meta::engine::txn::{EngineStatus, RoleInfo};
use crate::meta::raft::or_abort;
use crate::meta::raft::snapshot;
use crate::meta::raft::storage::{self, KV};
use crate::meta::schema::keys;
use crate::meta::schema::raft_state::{Applied, KEY_APPLIED};
use crate::meta::schema::records::{KvOp, PluginListV1, WriteBatchV1, decode_v1};

pub(super) struct Metrics {
    pub(super) term: Arc<Gauge>,
    pub(super) leader: Arc<Gauge>,
    pub(super) commit: Arc<Gauge>,
    pub(super) applied: Arc<Gauge>,
    pub(super) persist: Arc<Histogram>,
    pub(super) stale: Arc<Counter>,
    pub(super) dropped: Arc<Counter>,
    pub(super) installed: Arc<Counter>,
}

impl Metrics {
    pub(super) fn new() -> Metrics {
        Metrics {
            term: metrics::gauge("wmfs_raft_term", "元数据 raft 的当前 term", &[]),
            leader: metrics::gauge("wmfs_raft_is_leader", "本节点是否是元数据 leader（1/0）", &[]),
            commit: metrics::gauge("wmfs_raft_commit_index", "元数据 raft 的 commit index", &[]),
            applied: metrics::gauge("wmfs_raft_applied_index", "元数据 raft 已应用到的 index", &[]),
            persist: metrics::histogram(
                "wmfs_raft_persist_seconds",
                "Ready 的第一个写事务提交耗时（含 fsync）",
                &[],
                metrics::SECONDS_BUCKETS,
            ),
            stale: metrics::counter(
                "wmfs_meta_batch_stale_total",
                "expect_version 不符而整批作废的批次数（应恒为 0，非 0 即告警）",
                &[],
            ),
            dropped: metrics::counter("wmfs_raft_msg_dropped_total", "没能发出的 raft 消息数", &[]),
            installed: metrics::counter("wmfs_raft_snapshot_installed_total", "本节点安装过的元数据快照数", &[]),
        }
    }
}

/// 一条已应用的条目：在途提议按 (index, term) 认领它。
pub(super) struct AppliedEntry {
    index: u64,
    term: u64,
    ok: bool,        // 批次应用了（空条目、作废的批次为 false）
    changelog: bool, // 这个批次写了 changelog（0x0E）：提交之后唤醒长轮询
}

impl Driver {
    /// 规则 M5 的安装（在第一个写事务里）：kv、raft_log、snap_meta、conf_state 由 `install_snapshot` 写，这里写 applied。
    pub(super) fn install(&mut self, tx: &WriteTransaction, m: &SnapshotMetadata) {
        let file = snapshot::file_path(&self.snap_dir, m.index, m.term);
        let sv = or_abort(self.node.mut_store().install_snapshot(tx, m, &file), "安装快照");
        self.applied = Applied { index: m.index, term: m.term, state_version: sv };
        or_abort(storage::put_state(tx, KEY_APPLIED, &self.applied.encode()), "写 applied");
    }

    /// 快照安装的写事务提交之后：删掉快照文件，重新比对插件清单（kv 整个换了），日志压缩从这里重新计数。
    pub(super) fn installed(&mut self, m: &SnapshotMetadata) {
        let _ = std::fs::remove_file(snapshot::file_path(&self.snap_dir, m.index, m.term));
        let rtx = or_abort(self.db.begin_read(), "读事务");
        let committed = or_abort(storage::read_global::<PluginListV1>(&rtx, keys::G_PLUGINS), "读 GLOBAL plugins");
        self.check_plugins(committed.as_ref());
        self.compact_checked = m.index;
        self.metrics.installed.inc();
        log::info("raft", format_args!("node{} 安装了快照（index {}，term {}）", self.id, m.index, m.term));
    }

    /// 在写事务 `tx` 里按顺序应用已提交条目（3.4.1.3），最后写 `applied`（与 KV 同事务）。
    pub(super) fn apply_entries(&mut self, tx: &WriteTransaction, ents: Vec<Entry>) -> Vec<AppliedEntry> {
        if ents.is_empty() {
            return Vec::new();
        }
        let _ = fail_point("raft.apply.before");
        let mut out = Vec::with_capacity(ents.len());
        for e in ents {
            let mut a = AppliedEntry { index: e.index, term: e.term, ok: false, changelog: false };
            match e.get_entry_type() {
                // 新 leader 的空条目，或被 raft-rs 改写成空条目的 conf change（坑 P4 的反面、P21）
                EntryType::EntryNormal if e.data.is_empty() => {}
                EntryType::EntryNormal => self.apply_batch(tx, &e, &mut a),
                EntryType::EntryConfChangeV2 => {
                    self.apply_conf_change(tx, &e);
                    a.ok = true;
                }
                EntryType::EntryConfChange => {
                    panic!("wmfs 只提议 ConfChangeV2，日志里出现了 v1 EntryConfChange（index {}，坑 P22）", e.index)
                }
            }
            (self.applied.index, self.applied.term) = (e.index, e.term);
            out.push(a);
        }
        or_abort(storage::put_state(tx, KEY_APPLIED, &self.applied.encode()), "写 applied"); // 与 KV 同事务
        out
    }

    /// 一个批次：`expect_version == state_version` 才应用并令 `state_version = index`；不等则整批跳过
    /// （所有副本看到同样的日志、做同样的判断，所以是确定性的，3.4.1.5）。follower 只做"按顺序应用 ops"，没有业务逻辑。
    /// 应用了 → `a.ok = true`；批次里写了 changelog → `a.changelog = true`。
    fn apply_batch(&mut self, tx: &WriteTransaction, e: &Entry, a: &mut AppliedEntry) {
        let wb = WriteBatchV1::decode(&e.data)
            .unwrap_or_else(|c| panic!("不变量破坏：已提交的 raft 条目 {} 无法解码为 WriteBatchV1（{c}）", e.index));
        if wb.expect_version != self.applied.state_version {
            self.metrics.stale.inc();
            let (want, have) = (wb.expect_version, self.applied.state_version);
            log::warn("raft", format_args!("批次 {} 作废：expect_version {want} ≠ state_version {have}", e.index));
            return;
        }
        let plugins_key = keys::global(keys::G_PLUGINS);
        let mut plugins_changed = None;
        a.changelog =
            wb.ops.iter().any(|op| matches!(op, KvOp::Put { key, .. } if key.first() == Some(&keys::T_CHANGELOG)));
        {
            let mut kv = or_abort(tx.open_table(KV), "打开 kv");
            for op in &wb.ops {
                match op {
                    KvOp::Put { key, value } => {
                        or_abort(kv.insert(key.as_slice(), value.as_slice()), "写 kv");
                        if *key == plugins_key {
                            plugins_changed = Some(decode_v1::<PluginListV1>(value).ok());
                        }
                    }
                    KvOp::Delete { key } => {
                        or_abort(kv.remove(key.as_slice()), "删 kv");
                        if *key == plugins_key {
                            plugins_changed = Some(None);
                        }
                    }
                }
            }
        }
        if let Some(c) = plugins_changed {
            self.check_plugins(c.as_ref()); // 集群插件清单变了：重新比对（3.7.2）
        }
        self.applied.state_version = e.index;
        a.ok = true;
    }

    /// conf change：空 data = raft-rs 自动发起的 leave-joint，必须应用（坑 P4）；新的 ConfState 与条目同事务落盘（坑 P5）。
    fn apply_conf_change(&mut self, tx: &WriteTransaction, e: &Entry) {
        let mut cc = ConfChangeV2::default();
        if !e.data.is_empty() {
            cc.merge_from_bytes(&e.data)
                .unwrap_or_else(|err| panic!("不变量破坏：conf change {} 损坏：{err}", e.index));
        }
        let cs = self
            .node
            .apply_conf_change(&cc)
            .unwrap_or_else(|err| panic!("不变量破坏：已提交的 conf change {} 无法应用：{err}", e.index));
        or_abort(self.node.mut_store().set_conf_state(tx, &cs), "写 conf_state");
        log::info(
            "raft",
            format_args!("应用成员变更（index {}）：voters {:?}，learners {:?}", e.index, cs.voters, cs.learners),
        );
    }

    /// 写事务提交之后：在途提议按 (index, term) 认领已应用的条目——队首 index < 条目 → Lost；相等且 term 相同 →
    /// Applied{ok}；term 不同 → Lost（条目被新 leader 覆盖）。
    pub(super) fn after_apply(&mut self, applied: &[AppliedEntry]) {
        for a in applied {
            while self.pending.front().is_some_and(|p| p.index <= a.index) {
                let Some(p) = self.pending.pop_front() else { break };
                let ok = p.index == a.index && p.term == a.term;
                let _ = p.done.send(if ok { ProposeOutcome::Applied { ok: a.ok } } else { ProposeOutcome::Lost });
            }
        }
        // 集群图缓存按 GLOBAL map_epoch 失效（改变集群图的批次都在同一批次里 map_epoch += 1，读者每次比对它，
        // meta/cluster/map.rs），这里不用另外通知。实现注记（M7）：写了 changelog（0x0E）的批次 → 唤醒 changelog 长轮询的
        // 等待者（ADR-0033 第 9 条；写事务已经提交，它们新开的读事务看得到）
        if applied.iter().any(|a| a.changelog) {
            self.published.wake_changelog();
        }
    }

    /// 日志压缩（3.4.1.8，3.2.6 的写入规则 M3）：每应用 10 000 条检查一次，压缩点 C = durable_applied − 10 000（给慢 follower 留余量）。
    /// 因为落盘的 hs.commit ≥ 落盘的 applied ≥ C，重启时 raft-rs 的 `hs.commit ≥ first_index − 1` 检查恒成立（坑 P8）。
    /// 单独一个 None 事务：崩溃时丢了它也无妨（日志里只是多留了一些条目）。
    pub(super) fn maybe_compact(&mut self) {
        let (every, keep) = match fail_point_value("raft.compact.small") {
            Some(n) if n > 0 => (n as u64, n as u64), // 测试用的小阈值（见 driver.rs 的 COMPACT_EVERY）
            _ => (COMPACT_EVERY, COMPACT_KEEP),
        };
        if self.applied.index < self.compact_checked + every {
            return;
        }
        self.compact_checked = self.applied.index;
        let upto = self.durable_applied.saturating_sub(keep);
        if upto <= self.node.store().snap_index() {
            return;
        }
        let mut tx = or_abort(self.db.begin_write(), "begin_write");
        or_abort(self.node.mut_store().compact(&tx, upto), "压缩日志");
        tx.set_durability(Durability::None);
        or_abort(tx.commit(), "提交");
        self.note_commit(false);
        log::info("raft", format_args!("日志压缩到 {upto}（applied {}）", self.applied.index));
    }

    /// 发布角色（`RoleInfo`，ready 见 3.4.1.4 规则 3）与状态（`EngineStatus`）并更新 raft 指标：其他线程只读这份快照，
    /// 绝不碰 RawNode（3.4.1.3 的 publish_role）。
    pub(super) fn publish(&mut self) {
        let r = &self.node.raft;
        let leader = r.state == StateRole::Leader;
        let role = RoleInfo { role: r.state, term: r.term, leader_id: r.leader_id as u32, ready: self.is_ready() };
        let cs = r.prs().conf().to_conf_state();
        let ids = |v: &[u64]| {
            let mut ids: Vec<u32> = v.iter().map(|x| *x as u32).collect();
            ids.sort(); // raft-rs 的成员是集合：排好序，输出稳定
            ids
        };
        let mut progress: Vec<(u32, u64)> =
            if leader { r.prs().iter().map(|(id, p)| (*id as u32, p.matched)).collect() } else { vec![] };
        progress.sort();
        // 最近一个选举超时里来过消息的成员（含自己）：挑接替 leader 的人时不挑已经死掉的（membership::heir）。用 `<`：从前任 leader
        // 停下到自己当选至少隔了一个选举超时，前任刚好在边界上时也不算活着
        let recent =
            |id: &u32| *id == self.id || self.heard.get(&(*id as u64)).is_some_and(|t| self.ticks - t < ACTIVE_TICKS);
        let active = progress.iter().map(|p| p.0).filter(recent).collect();
        let commit = r.raft_log.committed;
        let status = EngineStatus {
            applied: self.applied.index,
            commit,
            voters: ids(&cs.voters),
            learners: ids(&cs.learners),
            progress,
            voters_outgoing: ids(&cs.voters_outgoing),
            active,
        };
        self.metrics.term.set(r.term as f64);
        self.metrics.leader.set(if leader { 1.0 } else { 0.0 });
        self.metrics.commit.set(commit as f64);
        self.metrics.applied.set(self.applied.index as f64);
        *lock(&self.published.role) = role;
        *lock(&self.published.status) = status;
    }

    /// 已提交的 GLOBAL plugins 与本节点的清单比对（3.7.2）。不一致 → 不就绪（执行器不建批次、读闸门不发 ReadIndex），
    /// 发布 `mismatch`，写一条错误日志列出差异。实现注记（M8）：cluster-mgr 看到 `mismatch` 就在任何角色上退出进程（退出码 3，
    /// `cluster/plugins.rs`）——follower 不必等拿到新集群图（改清单的 leader 1 s 后就退出了，可能没人再给它图）；还没有清单的新节点
    /// （applied 0）不算不一致。
    /// 实现注记（M9，ADR-0039 第 6 条）：一致了、或已应用的集群清单变了（逃生口被用过了），就不再豁免（`Published::adopt_done`）。
    /// 实现注记（M9）：applied 0 的节点一律不算——只做数据的节点不在 raft 里，它的 kv 只有 join 时预写的、之后不再更新的旧清单（按它
    /// 比对的话，改清单之后按新配置重启的节点会因旧清单退出）；它由心跳线程按每一张集群图比对（`server.rs` 的 `on_map`）。
    pub(super) fn check_plugins(&mut self, committed: Option<&PluginListV1>) {
        self.plugins_ok = committed == Some(&self.plugins);
        if self.plugins_ok || self.seen_plugins.as_ref() != committed {
            self.published.adopt_done();
        }
        self.seen_plugins = committed.cloned();
        let mismatch = !self.plugins_ok && self.applied.index > 0;
        self.published.mismatch.store(mismatch, Ordering::Relaxed);
        if !mismatch {
            return;
        }
        let why = plugins::why(&self.plugins, committed);
        log::error(
            "raft",
            format_args!("本节点的插件集合与集群插件清单不一致：{why}——不当就绪 leader（3.7.2）。{}", plugins::FIX),
        );
    }
}
