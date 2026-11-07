//! mclock：qos 插件（design.md 4.5、6.2 第 1 层，altitude 410，ADR-0010）——数据节点入口的 mClock 调度：保底 r、权重 w、上限 l，
//! 外加每队列一个字节令牌桶与低优先级。`cost = 1 + 字节 / 128 KiB`（4 KiB = 1、4 MiB = 33），r、l 的单位是"成本单位/秒"。
//!
//! - **队列**：每个 `QueueKey (租户, qos_id, 存储类)` 一个；队列里按作业 id 分子队列**轮转**（理念文档 6.2"作业 id 参与分类"）。
//!   后台请求（`background`）一律进一个固定的低优先级队列（`BG_QUEUE`；core 给的规格：w = 1、令牌桶 = 节点级后台限速）。空闲 60 s 的
//!   队列回收，再来请求时标签从现在重新算。
//! - **标签**（mClock，按"轮到它当队首"时算，等价于入队时 `R = max(R + cost/r, 到达)`）：队首的 R/L/P = max(上一个派发的 + 它的 cost /
//!   速率, 它的到达时刻)；r = 0 时不参加保底阶段，l = 0 时不限。P 只在**同一优先级**的活跃队列之间比大小：一个队列从空变成有请求时，它的
//!   P 从同一优先级的活跃队列里最小的 P 起算（mClock 对"刚活跃的队列"的处理；不然一直忙着的队列的 P 早就跑到前面去了，新来的会把它饿死。
//!   M13 审查：被饿着的后台、low 队列的 P 停在到达时刻附近，拿它们起算的话，新来的 normal 队列照样会把一直忙着的那个饿死）。
//! - **派发**（`dequeue`）：① 保底阶段——R ≤ now 的队首里 R 最小的（与优先级无关）；② 权重阶段——**先只在 normal 队列里**找"L ≤ now 且
//!   令牌桶够"的队首里 P 最小的，没有才看 low 队列（存储类 `priority = low`）；选中后把这个队列的 R 回退 cost/r（权重阶段派发的不算保底）；
//!   ③ 都没有 → 最早什么时候能再问（`Err(Some(t))`），全空 → `Err(None)`。不阻塞、不睡眠：睡眠与唤醒归 core（4.5）。
//! - **令牌桶**：深 = bw_max × burst（burst 是 0 = 没写，按 1 s），按 bw_max 补；新建的队列**桶满**起步（空闲很久、队列被回收之后再来的
//!   请求照样有 burst）；只在权重阶段检查（保底优先），派发时扣掉请求的字节（可以扣成负的：比桶还大的请求等桶满就放，平均速率仍是 bw_max）。
//! - **统计**（`stats`，心跳的保底欠额、`GET /api/v1/qos` 用）：每 10 s 一个窗口，记派发了多少成本单位、有积压（队列里有请求）的时长；
//!   欠额 = 上一个窗口**大半时间都有积压**时 max(0, r − 它的派发速率)，否则 0（刚活跃的队列、单流的客户端不会因为采样那一刻碰巧在排队就
//!   报欠额，M13 审查）。
//! - 时刻：`dequeue` 给的 `Instant`（3.7.3 的签名）；`enqueue` 与 `stats` 取"真实时间与见过的最新 now 的较大者"（0.2 第 6 条的 M13 注记）。
//!
//! 指标：`wmfs_qos_served_total{tenant,qos_id,phase}`（phase = reservation/weight）、`wmfs_qos_queue_depth{tenant,class}`、
//! `wmfs_qos_wait_seconds{tenant,class}`、`wmfs_qos_reservation_deficit{tenant,qos_id}`、`wmfs_job_io_bytes_total{job}`（只保留前 100 个作业，
//! 其余记为 other）；后台队列的 tenant、qos_id 标签是 `bg`（插件建好时就登记它的派发数：每个节点的 /metrics 都有这一族）。fail point
//! `plugin.mclock.qos`（core 包在 enqueue 外层）。

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use wmfs_core::api::metrics::{self, Counter, Gauge, Histogram};
use wmfs_core::base::lock;
use wmfs_core::hooks::manifest::{CORE_ABI_VERSION, Hook, Manifest};
use wmfs_core::hooks::traits::{BG_QUEUE, QosItem, QosPlugin, QosSpec, QueueKey, QueueStat};

pub static MANIFEST: Manifest = Manifest {
    name: "mclock",
    version: "0.1.0",
    hooks: &[Hook::Qos],
    altitude: 410,
    hot_path: true,
    config_schema: "",
    abi_version: CORE_ABI_VERSION,
    attrs: &["qos", "class"],
    budget_ns: 2_000, // 3.7.4：qos 2 µs
    fail_points: &["plugin.mclock.qos"],
};

const IDLE_S: f64 = 60.0;
const WINDOW_S: f64 = 10.0;
const MAX_JOBS: usize = 100;

struct Item {
    token: u64,
    job: u64,
    cost: f64,
    bytes: f64,
    at: f64, // 到达时刻（秒，从插件建好算起）
}

/// 一个请求的三个 mClock 标签（秒）。
#[derive(Clone, Copy)]
struct Tags {
    r: f64,
    l: f64,
    p: f64,
}

struct Queue {
    spec: QosSpec,
    jobs: VecDeque<(u64, VecDeque<Item>)>, // 作业 id → 它的请求；轮转：队首作业派发一个之后排到最后
    depth: usize,
    last: Tags,       // 上一个派发出去的请求的标签
    head: Tags,       // 队首请求的标签（depth > 0 时有效）
    tokens: f64,      // 字节令牌桶里的令牌（可以是负的）
    filled: f64,      // 上一次补令牌的时刻
    idle_since: f64,  // 最近一次派发的时刻（空闲回收用）
    busy_from: f64,   // 这一段积压从什么时候开始（depth > 0 时有效）
    win_start: f64,   // 这个统计窗口从什么时候开始
    win_cost: f64,    // 这个窗口里派发了多少成本单位
    win_busy: f64,    // 这个窗口里已经结束的积压段加起来多长（秒）
    rate: f64,        // 上一个窗口的派发速率（成本单位/秒）
    backlogged: bool, // 上一个窗口大半时间都有积压吗
    limited: u64,     // 被上限推迟过的请求数
    served_resv: Arc<Counter>,
    served_weight: Arc<Counter>,
    depth_g: Arc<Gauge>,
    wait: Arc<Histogram>,
    deficit_g: Arc<Gauge>,
}

/// 队列的指标标签 [tenant, qos_id, class]：后台队列都是 `bg`。
fn labels(k: &QueueKey) -> [String; 3] {
    if *k == BG_QUEUE {
        ["bg".into(), "bg".into(), "bg".into()]
    } else {
        [k.tenant.to_string(), k.qos_id.to_string(), k.class_id.to_string()]
    }
}

fn served(k: &QueueKey, phase: &str) -> Arc<Counter> {
    let [tenant, qos_id, _] = labels(k);
    let l = [("tenant", tenant.as_str()), ("qos_id", qos_id.as_str()), ("phase", phase)];
    metrics::counter("wmfs_qos_served_total", "mclock 派发的请求数（按阶段）", &l)
}

fn job_bytes(job: &str) -> Arc<Counter> {
    metrics::counter("wmfs_job_io_bytes_total", "按作业 id 统计的数据请求字节数（jobstats）", &[("job", job)])
}

impl Queue {
    fn new(k: &QueueKey, spec: QosSpec, t: f64) -> Queue {
        let [tenant, qos_id, class] = labels(k);
        let tc = [("tenant", tenant.as_str()), ("class", class.as_str())];
        let tq = [("tenant", tenant.as_str()), ("qos_id", qos_id.as_str())];
        let mut q = Queue {
            spec,
            jobs: VecDeque::new(),
            depth: 0,
            last: Tags { r: f64::NEG_INFINITY, l: f64::NEG_INFINITY, p: f64::NEG_INFINITY },
            head: Tags { r: f64::INFINITY, l: f64::INFINITY, p: f64::INFINITY },
            tokens: 0.0,
            filled: t,
            idle_since: t,
            busy_from: t,
            win_start: t,
            win_cost: 0.0,
            win_busy: 0.0,
            rate: 0.0,
            backlogged: false,
            limited: 0,
            served_resv: served(k, "reservation"),
            served_weight: served(k, "weight"),
            depth_g: metrics::gauge("wmfs_qos_queue_depth", "mclock 队列里等着派发的请求数", &tc),
            wait: metrics::histogram(
                "wmfs_qos_wait_seconds",
                "请求在 mclock 队列里等了多久",
                &tc,
                metrics::SECONDS_BUCKETS,
            ),
            deficit_g: metrics::gauge("wmfs_qos_reservation_deficit", "保底欠额（成本单位/秒）", &tq),
        };
        q.tokens = q.bucket(); // 桶满起步（见文件头）
        q
    }

    /// 令牌桶的深度（字节）。
    fn bucket(&self) -> f64 {
        let ms = if self.spec.burst_ms == 0 { 1000 } else { self.spec.burst_ms };
        self.spec.bw_max * (ms as f64 / 1000.0)
    }

    /// 时刻 `t` 桶里有多少令牌。
    fn tokens_at(&self, t: f64) -> f64 {
        (self.tokens + (t - self.filled).max(0.0) * self.spec.bw_max).min(self.bucket())
    }

    /// 队首请求的令牌够了的时刻（不限字节 → 0）。
    fn bucket_ready(&self) -> f64 {
        let Some(h) = self.jobs.front().and_then(|(_, q)| q.front()) else { return 0.0 };
        if self.spec.bw_max <= 0.0 {
            return 0.0;
        }
        let need = h.bytes.min(self.bucket()); // 比桶还大的请求：等桶满
        self.filled + ((need - self.tokens).max(0.0) / self.spec.bw_max)
    }

    /// 权重阶段能派发队首吗：L ≤ t 且令牌够。
    fn ready(&self, t: f64) -> bool {
        self.head.l <= t && self.bucket_ready() <= t
    }

    /// 给新的队首算标签（见文件头）。
    fn retag(&mut self) {
        let Some(h) = self.jobs.front().and_then(|(_, q)| q.front()) else { return };
        let (c, at, s) = (h.cost, h.at, self.spec);
        let tag = |last: f64, rate: f64| if rate > 0.0 { (last + c / rate).max(at) } else { at };
        let r = if s.r > 0.0 { tag(self.last.r, s.r) } else { f64::INFINITY };
        self.head = Tags { r, l: tag(self.last.l, s.l), p: tag(self.last.p, s.w) };
        if self.head.l > at {
            self.limited += 1;
        }
    }

    /// 入队；`min_p` = 同一优先级的别的活跃队列的队首里最小的 P（这个队列刚从空变成有请求时，P 从它起算，见文件头）。
    fn push(&mut self, it: Item, min_p: Option<f64>) {
        let (job, at) = (it.job, it.at);
        match self.jobs.iter_mut().find(|(j, _)| *j == job) {
            Some((_, q)) => q.push_back(it),
            None => self.jobs.push_back((job, VecDeque::from([it]))),
        }
        self.depth += 1;
        self.depth_g.add(1.0);
        if self.depth == 1 {
            self.busy_from = at; // 一段积压开始
            self.retag();
            self.head.p = self.head.p.max(min_p.unwrap_or(f64::NEG_INFINITY));
        }
    }

    /// 派发队首（`weight` = 权重阶段：R 回退，见文件头）。
    fn pop(&mut self, t: f64, weight: bool) -> Item {
        let (job, mut q) = self.jobs.pop_front().unwrap_or_else(|| panic!("不变量破坏：depth > 0 的队列没有请求"));
        let it = q.pop_front().unwrap_or_else(|| panic!("不变量破坏：作业子队列不该是空的"));
        if !q.is_empty() {
            self.jobs.push_back((job, q)); // 轮转：这个作业排到最后
        }
        (self.depth, self.idle_since) = (self.depth - 1, t);
        self.depth_g.add(-1.0);
        if self.depth == 0 {
            self.win_busy += (t - self.busy_from.max(self.win_start)).max(0.0); // 这一段积压结束
        }
        self.wait.observe((t - it.at).max(0.0));
        self.last = self.head;
        if weight && self.spec.r > 0.0 {
            self.last.r -= it.cost / self.spec.r;
        }
        if self.spec.bw_max > 0.0 {
            (self.tokens, self.filled) = (self.tokens_at(t) - it.bytes, t);
        }
        if weight {
            self.served_weight.inc()
        } else {
            self.served_resv.inc()
        }
        self.win_cost += it.cost;
        self.roll(t);
        self.retag();
        it
    }

    /// 统计窗口满了 10 s 就结算：派发速率、大半时间有没有积压（见文件头）。
    fn roll(&mut self, t: f64) {
        let len = t - self.win_start;
        if len < WINDOW_S {
            return;
        }
        let open = if self.depth > 0 { (t - self.busy_from.max(self.win_start)).max(0.0) } else { 0.0 }; // 还没结束的这一段
        (self.rate, self.backlogged) = (self.win_cost / len, (self.win_busy + open) * 2.0 >= len);
        (self.win_start, self.win_cost, self.win_busy) = (t, 0.0, 0.0);
    }

    /// 保底欠额（成本单位/秒，见文件头）。
    fn deficit(&self) -> f64 {
        if self.backlogged && self.spec.r > 0.0 { (self.spec.r - self.rate).max(0.0) } else { 0.0 }
    }
}

struct State {
    t0: Instant,
    now: f64, // 见过的最新时刻（enqueue 没有 now 参数：到达时刻取它与真实时间的较大者）
    queues: HashMap<QueueKey, Queue>,
    jobs: HashMap<u64, Arc<Counter>>, // 前 MAX_JOBS 个作业各自的字节数
    other: Arc<Counter>,              // 其余作业的字节数（job="other"）
}

impl State {
    fn secs(&self, t: Instant) -> f64 {
        t.saturating_duration_since(self.t0).as_secs_f64()
    }

    /// 权重阶段里 `low` 这一档能派发的队首里 P 最小的。
    fn weight_pick(&self, t: f64, low: bool) -> Option<QueueKey> {
        let ok = |q: &&Queue| q.depth > 0 && q.spec.low_priority == low && q.ready(t);
        self.queues.iter().filter(|(_, q)| ok(q)).min_by(|a, b| a.1.head.p.total_cmp(&b.1.head.p)).map(|(k, _)| *k)
    }

    fn serve(&mut self, k: QueueKey, t: f64, weight: bool) -> u64 {
        let q = self.queues.get_mut(&k).unwrap_or_else(|| panic!("不变量破坏：刚选中的队列不见了"));
        let it = q.pop(t, weight);
        if !self.jobs.contains_key(&it.job) && self.jobs.len() < MAX_JOBS {
            self.jobs.insert(it.job, job_bytes(&it.job.to_string()));
        }
        self.jobs.get(&it.job).unwrap_or(&self.other).add(it.bytes as u64);
        it.token
    }
}

pub struct Mclock {
    st: Mutex<State>,
}

impl Mclock {
    pub fn new() -> Mclock {
        let _ = (served(&BG_QUEUE, "reservation"), served(&BG_QUEUE, "weight")); // 先登记后台队列的派发数（见文件头）
        let (queues, jobs, other) = (HashMap::new(), HashMap::new(), job_bytes("other"));
        Mclock { st: Mutex::new(State { t0: Instant::now(), now: 0.0, queues, jobs, other }) }
    }
}

impl Default for Mclock {
    fn default() -> Mclock {
        Mclock::new()
    }
}

impl QosPlugin for Mclock {
    fn manifest(&self) -> &'static Manifest {
        &MANIFEST
    }

    fn enqueue(&self, item: QosItem, spec: QosSpec) {
        let mut st = lock(&self.st);
        let at = st.secs(Instant::now()).max(st.now);
        let k = if item.background { BG_QUEUE } else { item.key };
        let spec = QosSpec { low_priority: spec.low_priority || item.background, ..spec }; // 后台一律低优先级
        let same = |q: &Queue| q.depth > 0 && q.spec.low_priority == spec.low_priority; // 同一优先级的活跃队列
        let min_p = st.queues.iter().filter(|(x, q)| **x != k && same(q)).map(|(_, q)| q.head.p).min_by(f64::total_cmp);
        let q = st.queues.entry(k).or_insert_with(|| Queue::new(&k, spec, at));
        q.spec = spec; // 规格随集群图变：用最新的
        let it = Item { token: item.token, job: item.job_id, cost: item.cost as f64, bytes: item.bytes as f64, at };
        q.push(it, min_p);
    }

    fn dequeue(&self, now: Instant) -> Result<u64, Option<Instant>> {
        let mut st = lock(&self.st);
        let t = st.secs(now);
        st.now = st.now.max(t);
        st.queues.retain(|_, q| q.depth > 0 || t - q.idle_since < IDLE_S); // 空闲的队列回收
        let resv = st.queues.iter().filter(|(_, q)| q.depth > 0 && q.head.r <= t);
        if let Some(k) = resv.min_by(|a, b| a.1.head.r.total_cmp(&b.1.head.r)).map(|(k, _)| *k) {
            return Ok(st.serve(k, t, false)); // ① 保底
        }
        for low in [false, true] {
            if let Some(k) = st.weight_pick(t, low) {
                return Ok(st.serve(k, t, true)); // ② 权重：normal 先，没有可派发的才轮到 low
            }
        }
        let busy = st.queues.values().filter(|q| q.depth > 0);
        let next = busy.map(|q| q.head.r.min(q.head.l.max(q.bucket_ready()))).min_by(f64::total_cmp);
        Err(next.map(|x| st.t0 + Duration::from_secs_f64(x.max(t))))
    }

    fn stats(&self) -> Vec<QueueStat> {
        let mut st = lock(&self.st);
        let t = st.secs(Instant::now()).max(st.now);
        let mut out = vec![];
        for (k, q) in st.queues.iter_mut() {
            q.roll(t);
            let deficit = q.deficit();
            q.deficit_g.set(deficit);
            let (depth, served_per_s, limited_total) = (q.depth as u32, q.rate, q.limited);
            out.push(QueueStat { key: *k, depth, served_per_s, deficit_per_s: deficit, limited_total });
        }
        out
    }
}

#[cfg(test)]
mod tests;
