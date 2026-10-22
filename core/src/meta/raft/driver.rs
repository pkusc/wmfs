//! raft 线程的全部状态 `Driver` 与驱动循环（design.md 3.4.1.3、1.2，附录 A）。
//!
//! `Driver` 独占 `RawNode<RedbStorage>` 与 redb 写事务；别的线程只经 [`RaftInput`] 投递请求（执行器的提议、读闸门的
//! ReadIndex、对端的 raft 消息……），只读 [`Published`] 里的角色与状态快照。一轮循环 = 吃光输入（[`Driver::handle_input`]）
//! → 到点就 tick → [`Driver::process_ready`]（读闸门按需发起 read_index、处理一次 Ready、发布角色）。
//! 坑 P1：step/propose/read_index/campaign 只在 Ready 处理之外调用——`handle_input`、`tick`、`process_ready` 开头的
//! 读闸门 pump、`open` 里的 campaign；`on_ready`（`ready()` 到 `advance()` 之间）里从不调用它们。
//! 确定性模拟（9.2）直接调用 `handle_input` / `tick` / `process_ready`，不起线程；要发给对端的 raft 消息攒在 outbox 里
//! （M3 没有对端：驱动循环把它们计入 `wmfs_raft_msg_dropped_total`；M8 的 raft-send 线程从这里取）。
//!
//! Ready 的处理顺序（3.4.1.3）：① 角色变化 → 在途提议判 Lost、读闸门全部 NOT_LEADER；② leader 的消息可在落盘前发出；
//! ③ 第一个写事务：应用已提交条目（它们本地已持久，坑 P3）→ 追加新条目 → 硬状态（3.2.6 的写入规则 M1）→ applied，
//! `must_sync` 时 Immediate、否则 None（规则 M2）；④ follower 的消息必须在落盘之后发出（坑 P2）；read states；
//! ⑤ advance → LightReady：第二个写事务写含新 commit 的硬状态（坑 P20）与新应用的条目，一律 None；`advance_apply`。
//! 时间一律按 tick 计（1 tick = 100 ms）：读闸门的重发与超时、每 1 s 一次的"补 Immediate 提交"（规则 M2）。

mod apply;

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender};
use raft::eraftpb::Message;
use raft::{RawNode, SnapshotStatus, StateRole};
use redb::{Database, Durability};
use wmfs_api::failpoint::fail_point;
use wmfs_api::metrics::{self, Counter, Gauge, Histogram};

use super::or_abort;
use super::storage::{self, RedbStorage};
pub use super::types::{DriverOpts, ProposeOutcome, Published, RaftInput, config};
use crate::base::log::{self, SlogBridge};
use crate::base::{ErrCode, clock};
use crate::meta::engine::read_gate::ReadGate;
use crate::meta::schema::keys;
use crate::meta::schema::raft_state::Applied;
use crate::meta::schema::records::PluginListV1;

/// 1 tick = 100 ms；选举超时 10 tick（raft-rs 随机化到 [1 s, 2 s)），心跳 2 tick（3.4.1.1）。
const TICK_MS: u64 = 100;
/// 每 10 tick（1 s）检查一次：自上次 Immediate 之后有过 None 提交 → 补一次空的 Immediate 提交（规则 M2）。
const DURABLE_TICKS: u64 = 10;
/// 每应用 10 000 条检查一次日志压缩；压缩点 = durable_applied − 10 000（3.4.1.8，规则 M3）。
pub const COMPACT_EVERY: u64 = 10_000;
pub const COMPACT_KEEP: u64 = 10_000;

struct Pending {
    index: u64,
    term: u64,
    done: Sender<ProposeOutcome>,
}

struct Metrics {
    term: Arc<Gauge>,
    leader: Arc<Gauge>,
    commit: Arc<Gauge>,
    applied: Arc<Gauge>,
    persist: Arc<Histogram>,
    stale: Arc<Counter>,
    dropped: Arc<Counter>,
}

impl Metrics {
    fn new() -> Metrics {
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
        }
    }
}

pub struct Driver {
    node: RawNode<RedbStorage>,
    db: Arc<Database>,
    id: u32,
    pending: VecDeque<Pending>,
    gate: ReadGate,
    applied: Applied, // 已应用到的 (index, term) 与 state_version（最近一个改变了 KV 的批次条目的 index，3.4.1.5）
    durable_applied: u64, // 最近一次 Immediate 提交时的 applied（规则 M3）
    dirty: bool,      // 上次 Immediate 之后有过 None 提交（规则 M2）
    ticks: u64,
    next_compact: u64,
    plugins: PluginListV1,
    plugins_ok: bool,
    was_leader: bool,
    stopped: bool,
    outbox: Vec<Message>,
    published: Arc<Published>,
    metrics: Metrics,
}

impl Driver {
    /// 打开已初始化的元数据库，建 RawNode；`conf_state.voters == [自己]` 时立即 campaign 一次（3.4.1.9）。
    /// 空 ConfState 的节点（M8 的 joiner）不 campaign（坑 P14）。只在启动时调用，错误是给人看的中文。
    pub fn open(db: Arc<Database>, o: DriverOpts) -> Result<Driver, String> {
        let store = RedbStorage::open(db.clone())?;
        let rtx = db.begin_read().map_err(|e| format!("redb 读事务失败：{e}"))?;
        let a = storage::read_applied(&rtx)?;
        let committed_plugins = storage::read_global::<PluginListV1>(&rtx, keys::G_PLUGINS)?;
        let solo = store.conf_state().voters == [o.node_id as u64];
        let cfg = config(o.node_id, a.index);
        cfg.validate().map_err(|e| format!("raft 参数不合法：{e}"))?;
        let logger = slog::Logger::root(SlogBridge, slog::o!());
        let mut node = RawNode::new(&cfg, store, &logger).map_err(|e| format!("raft 初始化失败：{e}"))?;
        if solo && let Err(e) = node.campaign() {
            log::warn("raft", format_args!("campaign 失败：{e}"));
        }
        let mut d = Driver {
            node,
            db,
            id: o.node_id,
            pending: VecDeque::new(),
            gate: ReadGate::new(),
            applied: a,
            durable_applied: a.index,
            dirty: false,
            ticks: 0,
            next_compact: a.index + COMPACT_EVERY,
            plugins: o.plugins,
            plugins_ok: false,
            was_leader: false,
            stopped: false,
            outbox: Vec::new(),
            published: Arc::new(Published::new()),
            metrics: Metrics::new(),
        };
        d.check_plugins(committed_plugins.as_ref());
        d.publish();
        Ok(d)
    }

    /// 处理一个输入（在 Ready 处理之外，所以可以调用 step/propose 等，坑 P1）。
    pub fn handle_input(&mut self, input: RaftInput) {
        match input {
            RaftInput::Msg(m) => {
                if let Err(e) = self.node.step(m) {
                    log::debug("raft", format_args!("step 失败（忽略）：{e}")); // 例如未知对端的响应 StepPeerNotFound（坑 P15）
                }
            }
            RaftInput::Propose { data, leader_term, done } => {
                let before = self.node.raft.raft_log.last_index();
                let ok = self.proposable(leader_term) && self.node.propose(vec![], data).is_ok();
                self.track(before, ok, done);
            }
            RaftInput::ConfChange { cc, done } => {
                let (before, term) = (self.node.raft.raft_log.last_index(), self.node.raft.term);
                let ok = self.proposable(term) && self.node.propose_conf_change(vec![], cc).is_ok();
                self.track(before, ok, done);
            }
            RaftInput::ReadIndex { done } => self.gate.add(done),
            RaftInput::ReportSnapshot { to, ok } => {
                self.node.report_snapshot(to, if ok { SnapshotStatus::Finish } else { SnapshotStatus::Failure })
            }
            RaftInput::ReportUnreachable(id) => self.node.report_unreachable(id),
            RaftInput::TransferLeader(id) => self.node.transfer_leader(id),
            RaftInput::Shutdown => self.shutdown(),
        }
    }

    /// 提议前确认自己仍是"同一任期、已就绪"的 leader：降级成 follower 的 raft-rs 会把 MsgPropose 原样转发给新 leader，
    /// 过期写集绝不能进入新 leader 的日志（坑 P19）。
    fn proposable(&self, leader_term: u64) -> bool {
        self.is_ready() && self.node.raft.term == leader_term
    }

    /// propose 不返回 index（坑 P10）：比较提议前后的 last_index，之后按 (index, term) 认领。
    fn track(&mut self, before: u64, proposed: bool, done: Sender<ProposeOutcome>) {
        if proposed && self.node.raft.raft_log.last_index() == before + 1 {
            self.pending.push_back(Pending { index: before + 1, term: self.node.raft.term, done });
        } else {
            let _ = done.send(ProposeOutcome::Dropped); // 例如 max_uncommitted_size 超限、leader 转移进行中
        }
    }

    /// 逻辑时钟走一格（100 ms）：raft tick、读闸门的重发与超时、每 1 s 补一次 Immediate 提交。
    pub fn tick(&mut self) {
        self.node.tick();
        self.ticks += 1;
        let leader = self.node.raft.state == StateRole::Leader;
        let node = &mut self.node;
        self.gate.on_tick(leader, &mut |ctx| node.read_index(ctx));
        if self.ticks.is_multiple_of(DURABLE_TICKS) {
            self.durable_tick();
        }
    }

    /// 输入处理完之后的一步：读闸门需要时发起 read_index（还在 Ready 处理之前，坑 P1）→ 处理一次 Ready → 发布角色与状态。
    pub fn process_ready(&mut self) {
        let (leader, ready) = (self.node.raft.state == StateRole::Leader, self.is_ready());
        let node = &mut self.node;
        self.gate.pump(leader, ready, &mut |ctx| node.read_index(ctx));
        self.on_ready();
        self.publish();
    }

    /// 处理一次 Ready（`ready()` 到 `advance()` 之间不调用 step/propose/read_index/campaign，坑 P1）。
    fn on_ready(&mut self) {
        if !self.node.has_ready() {
            return;
        }
        let mut rd = self.node.ready();
        if let Some(ss) = rd.ss() {
            self.on_role(ss.raft_state); // ①
        }
        self.outbox.extend(rd.take_messages()); // ② leader 的消息可在落盘前发出（follower 时这里为空，坑 P2）
        assert!(rd.snapshot().is_empty(), "不变量破坏：收到了快照，而快照安装（规则 M5）M8 才实现"); // 坑 P17、P23
        let committed = rd.take_committed_entries();
        if !committed.is_empty() || !rd.entries().is_empty() || rd.hs().is_some() {
            // ③ 第一个写事务（规则 M1、M2）；下面的 or_abort = 出错就写日志并 abort（fail-stop）
            let mut tx = or_abort(self.db.begin_write(), "begin_write");
            let applied = self.apply_entries(&tx, committed); // 这些条目本地已持久（坑 P3），可与新条目同事务
            or_abort(self.node.mut_store().append(&tx, rd.entries()), "追加条目");
            if let Some(hs) = rd.hs() {
                or_abort(self.node.mut_store().set_hard_state(&tx, hs), "写硬状态");
            } else if !applied.is_empty() {
                let hs = self.node.raft.hard_state(); // 规则 M1：写入 applied 的事务必须同时写入 commit ≥ applied 的硬状态
                or_abort(self.node.mut_store().set_hard_state(&tx, &hs), "写硬状态");
            }
            let sync = rd.must_sync();
            tx.set_durability(if sync { Durability::Immediate } else { Durability::None });
            let _ = fail_point("raft.persist.before_commit"); // 测试里配置成 abort
            let t0 = Instant::now();
            or_abort(tx.commit(), "提交"); // ← 需要时在这里 fsync
            self.metrics.persist.observe(t0.elapsed().as_secs_f64());
            self.note_commit(sync);
            let _ = fail_point("raft.persist.after_commit");
            self.after_apply(&applied);
        }
        self.outbox.extend(rd.take_persisted_messages()); // ④ follower 的消息必须在落盘之后发出（坑 P2）
        for rs in rd.take_read_states() {
            self.gate.on_read_state(rs.index, &rs.request_ctx);
        }
        let mut light = self.node.advance(rd); // ⑤ 新的 commit index、消息、更多已提交条目
        let ents = light.take_committed_entries();
        if light.commit_index().is_some() || !ents.is_empty() {
            // 第二个写事务：必须同时写入含新 commit 的硬状态（规则 M1，修复"单节点 applied > hs.commit 重启 fatal"，坑 P20）
            let mut tx = or_abort(self.db.begin_write(), "begin_write");
            let hs = self.node.raft.hard_state();
            or_abort(self.node.mut_store().set_hard_state(&tx, &hs), "写硬状态");
            let applied = self.apply_entries(&tx, ents);
            tx.set_durability(Durability::None); // 条目已在日志里持久，崩溃后可重放（规则 M2）
            or_abort(tx.commit(), "提交");
            self.note_commit(false);
            self.after_apply(&applied);
        }
        self.outbox.extend(light.take_messages());
        self.node.advance_apply();
        self.gate.on_applied(self.applied.index);
        self.maybe_compact();
    }

    /// 角色变化：不再是 leader → 在途提议判 Lost（结果不确定，靠去重表兜底）、读闸门全部 NOT_LEADER。
    fn on_role(&mut self, now: StateRole) {
        let leader = now == StateRole::Leader;
        if self.was_leader && !leader {
            for p in self.pending.drain(..) {
                let _ = p.done.send(ProposeOutcome::Lost);
            }
            self.gate.fail_all(ErrCode::NOT_LEADER);
        }
        if leader != self.was_leader {
            let term = self.node.raft.term;
            log::info(
                "raft",
                format_args!("node{} {}（term {term}）", self.id, if leader { "成为 leader" } else { "不再是 leader" }),
            );
        }
        self.was_leader = leader;
    }

    /// 记下一次写事务提交：Immediate → 之前的一切都已落盘；None → 记下"有未落盘的提交"。
    fn note_commit(&mut self, immediate: bool) {
        if immediate {
            (self.durable_applied, self.dirty) = (self.applied.index, false);
        } else {
            self.dirty = true;
        }
    }

    /// 规则 M2：自上次 Immediate 之后有过 None 提交 → 补一次空的 Immediate 提交（限制"已应用未持久"的滞后，并让
    /// redb 回收页面）。也在退出前调用。
    fn durable_tick(&mut self) {
        if self.dirty {
            let tx = or_abort(self.db.begin_write(), "begin_write"); // 默认 Durability::Immediate
            or_abort(tx.commit(), "提交");
            self.note_commit(true);
        }
    }

    /// leader 就绪（3.4.1.4 规则 3）：`state == Leader && apply_to_current_term() && 插件清单一致`。
    pub fn is_ready(&self) -> bool {
        let r = &self.node.raft;
        r.state == StateRole::Leader && r.apply_to_current_term() && self.plugins_ok
    }

    /// 退出前：补一次 Immediate 提交（已应用的 None 提交落盘），在途提议判 Lost，读闸门全部 NOT_LEADER。
    pub fn shutdown(&mut self) {
        self.durable_tick();
        for p in self.pending.drain(..) {
            let _ = p.done.send(ProposeOutcome::Lost);
        }
        self.gate.fail_all(ErrCode::NOT_LEADER);
        self.stopped = true;
    }

    /// 取走攒下的要发给对端的 raft 消息（测试与 M8 的 raft-send 线程用）。
    pub fn take_outbox(&mut self) -> Vec<Message> {
        std::mem::take(&mut self.outbox)
    }

    pub fn published(&self) -> Arc<Published> {
        self.published.clone()
    }
    /// 已应用到的 (index, term) 与 state_version（内存里的；落盘的见 raft_state["applied"]）。
    pub fn applied(&self) -> Applied {
        self.applied
    }
    pub fn durable_applied(&self) -> u64 {
        self.durable_applied
    }
    /// 只读地看一眼 RawNode（测试用）。
    pub fn raw(&self) -> &RawNode<RedbStorage> {
        &self.node
    }

    /// raft 线程本体（3.4.1.3 的 raft_loop）：输入通道断开或收到 Shutdown 就退出。
    pub fn run(mut self, rx: Receiver<RaftInput>) {
        let mut next_tick = clock::mono_ms() + TICK_MS;
        while !self.stopped {
            match rx.recv_timeout(Duration::from_millis(next_tick.saturating_sub(clock::mono_ms()))) {
                Ok(input) => self.handle_input(input),
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => self.shutdown(),
            }
            while !self.stopped
                && let Ok(input) = rx.try_recv()
            {
                self.handle_input(input); // 一次吃光，减少 Ready 次数
            }
            if clock::mono_ms() >= next_tick {
                self.tick();
                next_tick = (next_tick + TICK_MS).max(clock::mono_ms()); // 落后很多时不连续补 tick
            }
            if !self.stopped {
                self.process_ready();
            }
            let lost = self.take_outbox().len(); // M3 没有对端（raft-send 线程在 M8）
            if lost > 0 {
                self.metrics.dropped.add(lost as u64);
            }
        }
    }
}
