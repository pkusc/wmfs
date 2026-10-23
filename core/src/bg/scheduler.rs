//! 后台调度器（design.md 3.10，ADR-0027）：每个节点一个 `bg-sched` 线程，轮转本节点该跑的后台任务。
//!
//! - `MetaLeader` 任务只在本节点是**就绪**的元数据 leader 时运行，失去 leader 立即停；新 leader（或换了 term）先从 KV 的 TaskRecV1
//!   （`15 <kind> 00`，name 为空）读回游标与进度再接着做——断点续跑。`Paused` 的任务不跑（`wmfs task pause` 在 M11）。
//! - 每步：`step`（经 3.7.4 的包装：fail point `plugin.<名字>.bg_task` 触发 = 这一步失败）→ 这一步处理了对象时过 fail point
//!   `bg.step.crash_after_effect`（副作用已经发生、游标还没持久化——用来验证 step 的幂等）→ 记指标 → 有进展就持久化游标与进度（每 5 s 最多一次，一轮结束时
//!   一定写）→ 按任务级与节点级限速睡够（limiter.rs）。一轮做完（`pass_done`）隔 `interval()` 再开始下一轮，游标清空。
//! - 失败只影响这一步：记 `wmfs_task_errors_total`、写一行警告，隔 `interval()` 从同一个游标重试（step 必须幂等）。
//!
//! 实现注记（M5）：骨架——只有 `MetaLeader` 任务（gc 的 leader 部分）；`EveryDataNode` 任务（scrub、本地孤儿扫描）与它们的本地游标文件
//! `<data_dir>/run/task-<kind>.cursor`、`wmfs task …` 控制命令、`task_progress_ratio` 在 M11（所以 `TaskCtx.targets` 先给空）。
//! 策略求值器来自管线（`Pipeline::policy`，M5 一律回"默认"）；一步的耗时用 `base::clock` 的单调时钟量（它决定限速要睡多久）。
//! 指标：`wmfs_task_items_total{task}`、`wmfs_task_bytes_total{task}`、`wmfs_task_backlog{task}`、`wmfs_task_step_seconds{task}`、
//! `wmfs_task_errors_total{task}`。

use std::sync::Arc;
use std::time::Duration;

use wmfs_api::failpoint::fail_point;
use wmfs_api::metrics::{self, Counter, Gauge, Histogram};

use super::limiter::RateLimiter;
use super::task::{BgTask, Migrator, RunOn, StepResult, TaskCtx, Walker};
use crate::base::types::ReqCtx;
use crate::base::{ErrCode, clock, log};
use crate::client::data_client::DataClient;
use crate::hooks::manifest::Hook;
use crate::hooks::pipeline::{HookCall, Pipeline};
use crate::meta::engine::txn::{MetaEngine, TxnOp, txn_blocking};
use crate::meta::schema::keys;
use crate::meta::schema::kv;
use crate::meta::schema::records::TaskRecV1;
use crate::meta::sessions::Live;
use crate::proto::meta::MetaReply;

/// 调度器空转的节拍、持久化进度的最长间隔。
const TICK: Duration = Duration::from_millis(100);
const SAVE_EVERY_MS: u64 = 5000;
/// TaskRecV1.state（3.2.8）。
const RUNNING: u8 = 1;
const PAUSED: u8 = 2;

/// 调度器要用到的本节点的东西。
pub struct BgDeps {
    pub eng: Arc<dyn MetaEngine>,
    pub data: Arc<DataClient>, // 后台用的数据面客户端（cap 带管理权限位的请求走 QoS 低优先级队列）
    pub live: Arc<Live>,
    pub pipeline: Arc<Pipeline>, // 策略求值器（Pipeline::policy）
}

/// 一个任务在调度器里的状态。
struct Slot {
    task: Box<dyn BgTask>,
    hook: HookCall,
    limiter: RateLimiter,
    next_at: u64, // mono_ms：什么时候再跑
    term: u64,    // 游标是在哪个 term 里读回来的（0 = 还没读）
    rec: TaskRecV1,
    dirty: bool,
    saved_at: u64,
    items: Arc<Counter>,
    bytes: Arc<Counter>,
    errors: Arc<Counter>,
    backlog: Arc<Gauge>,
    secs: Arc<Histogram>,
}

/// 起 `bg-sched` 线程（随进程一直运行）。
pub fn start(tasks: Vec<Box<dyn BgTask>>, deps: BgDeps) -> Result<(), String> {
    let mut slots: Vec<Slot> = tasks.into_iter().map(slot).collect();
    std::thread::Builder::new()
        .name("bg-sched".into())
        .spawn(move || {
            loop {
                std::thread::sleep(TICK);
                for s in slots.iter_mut() {
                    turn(s, &deps);
                }
            }
        })
        .map(|_| ())
        .map_err(|e| format!("起 bg-sched 线程失败：{e}"))
}

fn slot(task: Box<dyn BgTask>) -> Slot {
    let l = [("task", task.kind())];
    let hook = HookCall::new(task.manifest(), Hook::BgTask);
    let rec = TaskRecV1 {
        state: RUNNING,
        cursor: vec![],
        pass: 0,
        done: 0,
        total_hint: 0,
        backlog: 0,
        bytes_per_sec: 0,
        items_per_sec: 0,
        updated_ms: 0,
        last_error: String::new(),
    };
    Slot {
        task,
        hook,
        limiter: RateLimiter::new(),
        next_at: 0,
        term: 0,
        rec,
        dirty: false,
        saved_at: 0,
        items: metrics::counter("wmfs_task_items_total", "后台任务处理过的对象数", &l),
        bytes: metrics::counter("wmfs_task_bytes_total", "后台任务搬过的字节数", &l),
        errors: metrics::counter("wmfs_task_errors_total", "后台任务失败的步数", &l),
        backlog: metrics::gauge("wmfs_task_backlog", "后台任务的积压（任务自己报的）", &l),
        secs: metrics::histogram("wmfs_task_step_seconds", "后台任务一步的耗时", &l, metrics::SECONDS_BUCKETS),
    }
}

/// 轮到这个任务：该跑就跑一步（见文件头）。
fn turn(s: &mut Slot, d: &BgDeps) {
    let role = d.eng.role();
    if s.task.runs_on() != RunOn::MetaLeader || !role.ready {
        s.term = 0; // 不是就绪的 leader（或是 M11 才有的节点任务）：不跑；再当上 leader 时重新读游标
        return;
    }
    let now = clock::mono_ms();
    if now < s.next_at {
        return;
    }
    if s.term != role.term {
        match load(d, s) {
            Ok(()) => s.term = role.term,
            Err(e) => return fail(s, now, e),
        }
    }
    if s.rec.state == PAUSED {
        return;
    }
    let mut ctx = TaskCtx {
        meta: &*d.eng,
        data: &d.data,
        walker: &Walker {},
        migrate: &Migrator {},
        limiter: &s.limiter,
        targets: &[], // 只有 EveryDataNode 任务有（M11）
        policy: d.pipeline.policy(),
        now_ms: clock::wall_ms(),
        live: &d.live,
    };
    let t0 = clock::mono_ms();
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
        Ok(sr) => progress(s, d, sr, took),
        Err(e) => fail(s, clock::mono_ms(), e),
    }
}

/// 一步成功：记指标、推进游标、按规则持久化、按限速安排下一步。
fn progress(s: &mut Slot, d: &BgDeps, sr: StepResult, took: Duration) {
    s.items.add(sr.items);
    s.bytes.add(sr.bytes);
    s.backlog.set(sr.backlog as f64);
    s.dirty |= sr.items > 0 || sr.cursor != s.rec.cursor;
    (s.rec.done, s.rec.backlog) = (s.rec.done + sr.items, sr.backlog);
    s.rec.cursor = sr.cursor;
    let now = clock::mono_ms();
    if sr.pass_done {
        s.rec.pass += 1;
        s.rec.cursor.clear();
        s.next_at = now + s.task.interval().as_millis() as u64;
    }
    if s.dirty && (sr.pass_done || now.saturating_sub(s.saved_at) >= SAVE_EVERY_MS) {
        match save(d, s) {
            Ok(()) => (s.dirty, s.saved_at) = (false, now),
            Err(e) => log::warn("bg", format_args!("持久化任务 {} 的进度失败：{e}（下一步再试）", s.task.kind())),
        }
    }
    let wait = s.limiter.wait_after(sr.bytes, sr.items, took).as_millis() as u64;
    s.next_at = s.next_at.max(now + wait);
}

fn fail(s: &mut Slot, now: u64, e: ErrCode) {
    s.errors.inc();
    s.rec.last_error = e.to_string();
    log::warn(
        "bg",
        format_args!("后台任务 {} 这一步失败：{e}（{:?} 后从同一个游标重试）", s.task.kind(), s.task.interval()),
    );
    s.next_at = now + s.task.interval().as_millis() as u64;
}

/// 从 KV 读回游标、进度与限速设置（没有记录 = 第一次跑），以及节点级限速（集群参数）。
fn load(d: &BgDeps, s: &mut Slot) -> Result<(), ErrCode> {
    let key = keys::task(s.task.kind(), "");
    let (mut rec, mut node_bps) = (None, 0);
    // 这个闭包只用来在 ReadIndex 之后的视图上读记录，它的返回值丢弃
    d.eng.read(&ReqCtx::internal(), &mut |v, c| {
        rec = kv::get::<TaskRecV1>(v, &key)?;
        node_bps = c.settings.bg_bytes_per_sec;
        Ok(MetaReply::Ok)
    })?;
    if let Some(r) = rec {
        s.rec = r;
    }
    s.limiter.set(node_bps, s.rec.bytes_per_sec, s.rec.items_per_sec);
    Ok(())
}

/// 把游标与进度写进 TaskRecV1（一个内部批次）。
fn save(d: &BgDeps, s: &mut Slot) -> Result<(), ErrCode> {
    s.rec.updated_ms = clock::wall_ms();
    let (key, rec) = (keys::task(s.task.kind(), ""), s.rec.clone());
    let op: TxnOp = Box::new(move |v, _c| {
        kv::put(v, &key, &rec);
        Ok(MetaReply::Ok)
    });
    txn_blocking(&*d.eng, ReqCtx::internal(), op).map(|_| ())
}
