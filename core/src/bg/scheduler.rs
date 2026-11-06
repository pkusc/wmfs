//! 后台调度器（design.md 3.10，ADR-0027）：每个节点一个 `bg-sched` 线程，轮转本节点该跑的后台任务。
//!
//! - `MetaLeader` 任务只在本节点是**就绪**的元数据 leader 时运行，失去 leader 立即停；新 leader（或换了 term）先从 KV 的 TaskRecV1
//!   （`15 <kind> 00`，name 为空）读回游标与进度再接着做——断点续跑。`EveryDataNode` 任务（scrub、gc-local，实现注记（M11））在每个
//!   有 target 的节点上跑，进度在本地的游标文件里（`progress.rs` 的文件头）。
//! - 控制（M11）：每秒读一次集群级记录的控制字段——`Paused` 就不跑（暂停只是控制字段：调度器自己的状态只有 Running / Done，换了 leader
//!   读回来的记录是 Paused 时也当 Running 接着做）；leader 任务这一轮做完（Done）之后又看到 `Running`（`wmfs task run`）就马上开始下一轮；
//!   节点任务看"运行请求号"：比照办过的大就从头开始新的一轮（这一轮做到一半也重来：运行请求之前走过的那些也要再查）。限速取任务级
//!   （`wmfs task set`；没设时用任务自己的默认值，例如 scrub 的 50 MiB/s）与节点级（集群参数 `bg_bytes_per_sec`）里更严的。节点任务重启
//!   之后本地游标文件里是 Done：隔满 `interval()`（从那一轮做完算）才开始下一轮，不因为重启就重做一遍。
//! - 每步：`step`（经 3.7.4 的包装：fail point `plugin.<名字>.bg_task` 触发 = 这一步失败）→ 这一步处理了对象时过 fail point
//!   `bg.step.crash_after_effect`（副作用已经发生、游标还没持久化——用来验证 step 的幂等）→ 记指标 → 有进展就持久化游标与进度（每 5 s
//!   最多一次，一轮结束时一定写）→ 按限速睡够（limiter.rs）。一轮做完（`pass_done`）记为 Done，隔 `interval()` 再开始下一轮，游标清空。
//! - 失败只影响这一步：记 `wmfs_task_errors_total`、写一行警告、记进 `last_error`（之后成功的一步把它清掉），隔 `interval()`（实现注记
//!   （M11）：最多 10 s——scrub 一轮隔 24 h，刚启动时还没拉到集群图那一下失败不能等一天）从同一个游标重试（step 必须幂等）。
//!
//! 指标：`wmfs_task_items_total{task}`、`wmfs_task_bytes_total{task}`、`wmfs_task_backlog{task}`、`wmfs_task_step_seconds{task}`、
//! `wmfs_task_errors_total{task}`、`wmfs_task_progress_ratio{task}`（M11：这一轮已做的 / (已做的 + 积压)；
//! 积压按目录数计的协调器没有它，`api::types::backlog_in_dirs`）。

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use wmfs_api::failpoint::fail_point;
use wmfs_api::metrics::{self, Counter, Gauge, Histogram};

use super::limiter::RateLimiter;
use super::migrator::Migrator;
use super::progress::{self, DONE, LocalTaskV1, PAUSED, RUNNING, TaskBoard};
use super::task::{BgTask, RunOn, StepResult, TaskCtx};
use super::walker::Walker;
use crate::api::types::{RUNS_ON_EVERY_NODE, RUNS_ON_LEADER, TaskInfo, backlog_in_dirs};
use crate::base::{ErrCode, clock, log};
use crate::client::data_client::DataClient;
use crate::data::DataNode;
use crate::hooks::manifest::Hook;
use crate::hooks::pipeline::{HookCall, Pipeline};
use crate::hooks::traits::TargetStore;
use crate::meta::engine::txn::MetaEngine;
use crate::meta::schema::keys;
use crate::meta::schema::records::{TaskRecV1, decode_v1};
use crate::meta::sessions::Live;

/// 调度器空转的节拍、持久化进度的最长间隔、多久读一次控制字段。
const TICK: Duration = Duration::from_millis(100);
const SAVE_EVERY_MS: u64 = 5000;
const CONTROL_EVERY_MS: u64 = 1000;
const RETRY_MAX_MS: u64 = 10_000;

/// 本地已应用的 KV 里的一个键（只做数据的节点一个条目也没应用过 → 一律 None）。
pub type LocalGet = Arc<dyn Fn(&[u8]) -> Option<Vec<u8>> + Send + Sync>;

/// 调度器要用到的本节点的东西。实现注记（M11）：多了本节点的数据面（`EveryDataNode` 任务用；没有 target → None）、本地 KV、本地游标
/// 文件的目录、给管理 API 的进度板与本节点号。
pub struct BgDeps {
    pub eng: Arc<dyn MetaEngine>,
    pub data: Arc<DataClient>, // 后台用的数据面客户端（cap 带管理权限位的请求走 QoS 低优先级队列）
    pub live: Arc<Live>,
    pub pipeline: Arc<Pipeline>, // 策略求值器（Pipeline::policy）
    pub node: Option<Arc<DataNode>>,
    pub local: LocalGet,
    pub run_dir: PathBuf, // <data_dir>/run
    pub board: Arc<TaskBoard>,
    pub node_id: u32,
}

/// 一个任务在调度器里的状态。
struct Slot {
    task: Box<dyn BgTask>,
    hook: HookCall,
    limiter: RateLimiter,
    next_at: u64,    // mono_ms：什么时候再跑
    term: u64,       // leader 任务：游标是在哪个 term 里读回来的（0 = 还没读）
    loaded: bool,    // 节点任务：读过本地游标文件了吗
    rec: TaskRecV1,  // 进度与本调度器眼里的状态（Running / Done）
    ctl: TaskRecV1,  // 集群级记录里的控制字段（state、限速；节点任务的 pass = 运行请求号）
    ctl_at: u64,     // 上次读控制字段的时刻（mono_ms；0 = 还没读）
    ctl_bad: bool,   // 节点任务：本地的集群级记录解不开（警告过了，好了再说）
    node_bps: u64,   // 节点级限速
    run_seen: u64,   // 节点任务：照办到第几个运行请求
    saved_state: u8, // 库里（或本地文件里）现在的 state：已经是 Done 的空转轮不必每轮都写
    dirty: bool,
    saved_at: u64,
    pass_items: u64,    // 这一轮做了多少（进度比例）
    last_start_ms: u64, // 上一步开始的时刻与它搬的字节（速率）
    last_bytes: u64,
    rate: u64,
    items: Arc<Counter>,
    bytes: Arc<Counter>,
    errors: Arc<Counter>,
    backlog: Arc<Gauge>,
    ratio: Option<Arc<Gauge>>, // 积压按目录数计的任务没有（见文件头）
    secs: Arc<Histogram>,
}

impl Slot {
    fn on_leader(&self) -> bool {
        self.task.runs_on() == RunOn::MetaLeader
    }

    /// 任务级的字节/秒：设了就用设的，没设用任务自己的默认值（limiter.rs）。
    fn task_bps(&self) -> u64 {
        if self.ctl.bytes_per_sec > 0 { self.ctl.bytes_per_sec } else { self.task.default_rate() }
    }
}

/// 这一轮的进度比例：已做的 / (已做的 + 积压)；什么都没有 = 做完了。积压按目录数计的任务没有比例（见文件头）：0。
fn ratio(s: &Slot, backlog: u64) -> f64 {
    let done = s.pass_items;
    if backlog_in_dirs(s.task.kind()) {
        0.0
    } else if done + backlog == 0 {
        1.0
    } else {
        done as f64 / (done + backlog) as f64
    }
}

/// 起 `bg-sched` 线程（随进程一直运行）。
pub fn start(tasks: Vec<Box<dyn BgTask>>, deps: BgDeps) -> Result<(), String> {
    let mut slots: Vec<Slot> = tasks.into_iter().map(slot).collect();
    let (walker, migrate) = (Walker::new(), Migrator::new());
    for s in &slots {
        deps.board.set_default_bw(s.task.kind(), s.task.default_rate());
        deps.board.put(info(s, &deps)); // 每个节点都认得全部任务（管理 API 据此知道一个任务在哪里跑）
    }
    std::thread::Builder::new()
        .name("bg-sched".into())
        .spawn(move || {
            loop {
                std::thread::sleep(TICK);
                for s in slots.iter_mut() {
                    turn(s, &deps, &walker, &migrate);
                }
            }
        })
        .map(|_| ())
        .map_err(|e| format!("起 bg-sched 线程失败：{e}"))
}

fn slot(task: Box<dyn BgTask>) -> Slot {
    let l = [("task", task.kind())];
    let hook = HookCall::new(task.manifest(), Hook::BgTask);
    let ratio = (!backlog_in_dirs(task.kind()))
        .then(|| metrics::gauge("wmfs_task_progress_ratio", "后台任务这一轮的进度（0–1）", &l));
    Slot {
        task,
        hook,
        limiter: RateLimiter::new(),
        next_at: 0,
        term: 0,
        loaded: false,
        rec: progress::new_rec(),
        ctl: progress::new_rec(),
        ctl_at: 0,
        ctl_bad: false,
        node_bps: 0,
        run_seen: 0,
        saved_state: 0,
        dirty: false,
        saved_at: 0,
        pass_items: 0,
        last_start_ms: 0,
        last_bytes: 0,
        rate: 0,
        items: metrics::counter("wmfs_task_items_total", "后台任务处理过的对象数", &l),
        bytes: metrics::counter("wmfs_task_bytes_total", "后台任务搬过的字节数", &l),
        errors: metrics::counter("wmfs_task_errors_total", "后台任务失败的步数", &l),
        backlog: metrics::gauge("wmfs_task_backlog", "后台任务的积压（任务自己报的）", &l),
        ratio,
        secs: metrics::histogram("wmfs_task_step_seconds", "后台任务一步的耗时", &l, metrics::SECONDS_BUCKETS),
    }
}

/// 轮到这个任务：该跑就跑一步（见文件头）。
fn turn(s: &mut Slot, d: &BgDeps, walker: &Walker, migrate: &Migrator) {
    let (leader, now) = (s.on_leader(), clock::mono_ms());
    if leader {
        let role = d.eng.role();
        if !role.ready {
            if s.term != 0 {
                migrate.forget_all(); // 不再是 leader：迁移原语记的"因写打开被跳过"归新 leader 管
            }
            s.term = 0; // 不是就绪的 leader：不跑；再当上 leader 时重新读游标
            return;
        }
        if s.term != role.term {
            match progress::load_leader(&*d.eng, s.task.kind()) {
                Ok(l) => {
                    s.rec = l.rec;
                    if s.rec.state == PAUSED {
                        s.rec.state = RUNNING; // 暂停只是控制字段（见文件头）：恢复之后接着做，不把 Paused 当成自己的状态写回去
                    }
                    s.term = role.term;
                    s.saved_state = s.rec.state;
                    s.ctl_at = 0;
                    s.next_at = 0;
                }
                Err(e) => return fail(s, now, e),
            }
        }
    } else if d.node.is_none() {
        return; // 没有 target 的节点不跑节点任务
    } else if !s.loaded {
        if let Some(l) = progress::load_local(&d.run_dir, s.task.kind()) {
            (s.rec, s.run_seen) = (l.rec, l.run_seen);
        }
        (s.loaded, s.saved_state) = (true, s.rec.state);
        if s.rec.state == DONE {
            let since = clock::wall_ms().saturating_sub(s.rec.updated_ms); // 那一轮做完（持久化）之后过了多久
            s.next_at = now + (s.task.interval().as_millis() as u64).saturating_sub(since);
        }
    }
    if s.ctl_at == 0 || now.saturating_sub(s.ctl_at) >= CONTROL_EVERY_MS {
        if let Err(e) = read_control(s, d, leader) {
            return fail(s, now, e);
        }
        s.ctl_at = now;
        d.board.put(info(s, d)); // 改了限速、暂停了：睡着的任务也马上看得到
    }
    if s.ctl.state == PAUSED {
        return;
    }
    if leader && s.rec.state == DONE && s.ctl.state == RUNNING {
        (s.next_at, s.dirty) = (0, true); // wmfs task run：一轮做完了就立即开始新的一轮
    }
    if !leader && s.ctl.pass > s.run_seen {
        (s.run_seen, s.next_at, s.dirty) = (s.ctl.pass, 0, true);
        (s.rec.state, s.rec.cursor) = (DONE, vec![]); // 运行请求：从头开始新的一轮（见文件头）
    }
    if now < s.next_at {
        return;
    }
    if s.rec.state == DONE {
        (s.rec.state, s.pass_items) = (RUNNING, 0); // 新的一轮
    }
    step(s, d, walker, migrate);
    d.board.put(info(s, d));
}

/// 读控制字段（leader 任务：线性一致读；节点任务：本地已应用的 KV 与本节点的集群图）、更新限速。
fn read_control(s: &mut Slot, d: &BgDeps, leader: bool) -> Result<(), ErrCode> {
    if leader {
        let l = progress::load_leader(&*d.eng, s.task.kind())?;
        (s.ctl, s.node_bps) = (l.rec, l.node_bps);
    } else {
        let r = (d.local)(&keys::task(s.task.kind(), "")).map(|b| decode_v1::<TaskRecV1>(&b));
        match &r {
            Some(Err(e)) if !s.ctl_bad => {
                log::warn("bg", format_args!("任务 {} 的集群级记录解不开：{e}（按默认值跑）", s.task.kind()));
                s.ctl_bad = true;
            }
            Some(Err(_)) => {} // 警告过了
            _ => s.ctl_bad = false,
        }
        s.ctl = r.and_then(|r| r.ok()).unwrap_or_else(progress::new_rec);
        s.node_bps = d.node.as_ref().and_then(|n| n.map.get()).map_or(0, |m| m.settings.bg_bytes_per_sec);
    }
    s.limiter.set(s.node_bps, s.task_bps(), s.ctl.items_per_sec);
    Ok(())
}

fn step(s: &mut Slot, d: &BgDeps, walker: &Walker, migrate: &Migrator) {
    let node_task = !s.on_leader();
    let targets: Vec<Arc<dyn TargetStore>> = match &d.node {
        Some(n) if node_task => n.targets.values().map(|t| t.store.clone()).collect(),
        _ => vec![], // 只有 EveryDataNode 任务有
    };
    let mut ctx = TaskCtx {
        meta: &*d.eng,
        data: &d.data,
        walker,
        migrate,
        limiter: &s.limiter,
        targets: &targets,
        policy: d.pipeline.policy(),
        now_ms: clock::wall_ms(),
        live: &d.live,
        node: d.node.as_ref().filter(|_| node_task),
    };
    let t0 = clock::mono_ms();
    if s.last_start_ms > 0 {
        s.rate = s.last_bytes * 1000 / t0.saturating_sub(s.last_start_ms).max(1); // 上一步搬的 / 上一步开始到现在（含限速等待）
    }
    let cursor = s.rec.cursor.clone();
    let r = s.hook.call(|| s.task.step(&mut ctx, &cursor));
    // 副作用已经发生（这一步处理了对象）、游标还没持久化：fail point 在这里模拟崩溃，验证 step 的幂等（3.10）
    let r = r.and_then(|sr| match sr.items {
        0 => Ok(sr),
        _ => fail_point("bg.step.crash_after_effect").map(|()| sr).map_err(ErrCode),
    });
    let took = Duration::from_millis(clock::mono_ms().saturating_sub(t0));
    s.secs.observe(took.as_secs_f64());
    match r {
        Ok(sr) => {
            (s.last_start_ms, s.last_bytes) = (t0, sr.bytes);
            on_step_ok(s, d, sr, took)
        }
        Err(e) => fail(s, clock::mono_ms(), e),
    }
}

/// 一步成功：记指标、推进游标、按规则持久化、按限速安排下一步。
fn on_step_ok(s: &mut Slot, d: &BgDeps, sr: StepResult, took: Duration) {
    s.items.add(sr.items);
    s.bytes.add(sr.bytes);
    s.backlog.set(sr.backlog as f64);
    s.dirty |= sr.items > 0 || sr.cursor != s.rec.cursor || !s.rec.last_error.is_empty();
    s.pass_items += sr.items;
    s.rec.done += sr.items;
    s.rec.backlog = sr.backlog;
    s.rec.cursor = sr.cursor;
    s.rec.last_error.clear(); // 之前失败过的一步已经好了
    let now = clock::mono_ms();
    if sr.pass_done {
        s.rec.pass += 1;
        s.rec.cursor.clear();
        s.rec.state = DONE;
        s.dirty |= s.pass_items > 0 || s.saved_state != DONE; // 一直空转的任务不必每轮都写一次
        s.next_at = now + s.task.interval().as_millis() as u64;
    }
    if let Some(g) = &s.ratio {
        g.set(ratio(s, sr.backlog));
    }
    if s.dirty && (sr.pass_done || now.saturating_sub(s.saved_at) >= SAVE_EVERY_MS) {
        match save(s, d) {
            Ok(()) => {
                (s.dirty, s.saved_at, s.saved_state) = (false, now, s.rec.state);
                if s.on_leader() && s.ctl.state != PAUSED {
                    s.ctl.state = s.rec.state; // 库里现在就是它（不必等下一次读控制字段：否则刚写的 Done 被当成"又要跑"）
                }
            }
            Err(e) => log::warn("bg", format_args!("持久化任务 {} 的进度失败：{e}（下一步再试）", s.task.kind())),
        }
    }
    let wait = s.limiter.wait_after(sr.bytes, sr.items, took).as_millis() as u64;
    s.next_at = s.next_at.max(now + wait);
}

/// 进度写回：leader 任务写进 KV（只改进度那一半），节点任务写本地游标文件。
fn save(s: &mut Slot, d: &BgDeps) -> Result<(), ErrCode> {
    s.rec.updated_ms = clock::wall_ms();
    match s.task.runs_on() {
        RunOn::MetaLeader => progress::save_leader(&*d.eng, s.task.kind(), &s.rec),
        RunOn::EveryDataNode => {
            let l = LocalTaskV1 { rec: s.rec.clone(), run_seen: s.run_seen };
            progress::save_local(&d.run_dir, s.task.kind(), &l)
        }
    }
}

fn fail(s: &mut Slot, now: u64, e: ErrCode) {
    s.errors.inc();
    s.rec.last_error = e.to_string();
    let wait = (s.task.interval().as_millis() as u64).min(RETRY_MAX_MS);
    log::warn("bg", format_args!("后台任务 {} 这一步失败：{e}（{wait} ms 后从同一个游标重试）", s.task.kind()));
    s.next_at = now + wait;
}

/// 给管理 API 看的样子（[`TaskBoard`]）。
fn info(s: &Slot, d: &BgDeps) -> TaskInfo {
    let leader = s.on_leader();
    let state = if s.ctl.state == PAUSED { PAUSED } else { s.rec.state };
    TaskInfo {
        kind: s.task.kind().into(),
        runs_on: if leader { RUNS_ON_LEADER } else { RUNS_ON_EVERY_NODE }.into(),
        node: if leader { 0 } else { d.node_id },
        state: progress::state_name(state).into(),
        pass: s.rec.pass,
        done: s.rec.done,
        backlog: s.rec.backlog,
        progress: ratio(s, s.rec.backlog),
        rate_bps: s.rate,
        bw: s.task_bps(),
        items: s.ctl.items_per_sec,
        updated_ms: s.rec.updated_ms,
        last_error: s.rec.last_error.clone(),
        runs: if leader { 0 } else { s.run_seen },
    }
}
