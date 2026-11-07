//! mclock 的单元测试（roadmap M13 的验收"虚拟时钟单元测试"）：时间全部是给 `dequeue` 的虚拟时刻（插件建好之后的第几秒），一个"io 线程"
//! 每一步派发一个请求。权重 1:3 的份额误差 < 5%；`priority = low` 的队列只在 normal 队列没有可派发请求时才被服务；上限、保底、字节令牌桶
//! （桶满起步）、作业轮转、后台队列、空闲队列回收；刚活跃的队列的 P 只从同一优先级的队列起算；保底欠额只在窗口大半时间有积压时才报。

use std::time::{Duration, Instant};

use wmfs_core::hooks::manifest::check_manifest_toml;

use super::*;

fn key(qos_id: u64) -> QueueKey {
    QueueKey { tenant: 7, qos_id, class_id: 1 }
}

fn spec(r: f64, w: f64, l: f64) -> QosSpec {
    QosSpec { r, w, l, bw_max: 0.0, burst_ms: 0, low_priority: false }
}

/// 第 `n` 个请求的 token：高 32 位是队列号，便于数每个队列派发了几个。
fn push(m: &Mclock, (q, n): (u64, u64), (cost, bytes): (u32, u32), job: u64, s: QosSpec, bg: bool) {
    let item = QosItem { key: key(q), job_id: job, cost, bytes, background: bg, token: q << 32 | n };
    m.enqueue(item, s);
}

/// 虚拟时钟：先拿"第 1000 秒"问一次空的插件，之后入队的请求的到达时刻就都是虚拟的第 0 秒（到达 = max(真实时间, 见过的最新时刻)，
/// 与入队花了多久、机器忙不忙无关）；返回"虚拟第 s 秒"的换算。
fn clock(m: &Mclock) -> impl Fn(f64) -> Instant {
    let t0 = Instant::now();
    let at = move |s: f64| t0 + Duration::from_secs_f64(1000.0 + s);
    assert_eq!(m.dequeue(at(0.0)), Err(None));
    at
}

/// 一个"io 线程"在 [0, secs) 里每 1 ms 派发一个请求（没有能派发的就空过这一拍）：每个派发的 (队列, 时刻)。
fn run(m: &Mclock, at: &dyn Fn(f64) -> Instant, secs: f64) -> Vec<(u64, f64)> {
    let tick = |i: u64| i as f64 / 1000.0;
    (0..(secs * 1000.0) as u64).filter_map(|i| m.dequeue(at(tick(i))).ok().map(|t| (t >> 32, tick(i)))).collect()
}

fn count(got: &[(u64, f64)], q: u64) -> usize {
    got.iter().filter(|(x, _)| *x == q).count()
}

#[test]
fn manifest_toml_matches_code() {
    check_manifest_toml(&MANIFEST, include_str!("manifest.toml")).unwrap();
}

#[test]
fn weights_1_to_3_share_within_5_percent() {
    let m = Mclock::new();
    let at = clock(&m);
    for n in 0..3000 {
        push(&m, (1, n), (1, 4096), 0, spec(0.0, 1.0, 0.0), false);
        push(&m, (2, n), (1, 4096), 0, spec(0.0, 3.0, 0.0), false);
    }
    let got = run(&m, &at, 2.0);
    let share = count(&got, 2) as f64 / (count(&got, 1) + count(&got, 2)) as f64;
    assert!((share - 0.75).abs() / 0.75 < 0.05, "权重 1:3 的份额应是 0.75：{share}");
}

#[test]
fn low_priority_is_served_only_when_normal_has_nothing_dispatchable() {
    let m = Mclock::new();
    let at = clock(&m);
    let low = QosSpec { low_priority: true, ..spec(0.0, 100.0, 0.0) }; // 权重大得多也排在 normal 后面
    for n in 0..5000 {
        push(&m, (1, n), (1, 4096), 0, spec(0.0, 1.0, 10.0), false); // normal：上限 10 个/秒
        push(&m, (2, n), (1, 4096), 0, low, false);
    }
    let got = run(&m, &at, 2.0);
    let normal: Vec<f64> = got.iter().filter(|(q, _)| *q == 1).map(|(_, t)| *t).collect();
    // normal 每 0.1 s 才能派发一个：它一可以派发就轮到它（low 从不抢它的位置），其余每一拍都给 low
    assert_eq!(normal.len(), 20, "{normal:?}");
    for (k, t) in normal.iter().enumerate() {
        assert!((t - k as f64 * 0.1).abs() < 0.0015, "第 {k} 个 normal 请求在 {t}：它一可以派发就该轮到它：{normal:?}");
    }
    assert_eq!(count(&got, 2), 2000 - normal.len(), "normal 不能派发的每一拍都给了 low");
}

#[test]
fn limit_and_reservation() {
    let m = Mclock::new();
    let at = clock(&m);
    for n in 0..3000 {
        push(&m, (1, n), (9, 1 << 20), 0, spec(0.0, 1.0, 90.0), false); // 1 MiB 的请求（cost 9），上限 90/s → 10 个/秒
        push(&m, (2, n), (1, 4096), 0, spec(50.0, 1.0, 0.0), false); // 保底 50/s
        push(&m, (3, n), (1, 4096), 0, spec(0.0, 1000.0, 0.0), false); // 权重大、不限
    }
    let got = run(&m, &at, 10.0);
    assert_eq!(count(&got, 1), 100, "上限 90 成本单位/秒、每个 9：10 s 里 100 个");
    assert!(count(&got, 2) >= 499, "保底 50 个/秒：10 s 里至少约 500 个（权重只有 1/1000 也一样）：{}", count(&got, 2));
    let st = m.stats();
    assert!(st.iter().find(|s| s.key == key(1)).unwrap().limited_total > 0, "被上限推迟过");
}

#[test]
fn byte_bucket() {
    let m = Mclock::new();
    let at = clock(&m);
    let bw = QosSpec { bw_max: 1048576.0, burst_ms: 1000, ..spec(0.0, 1.0, 0.0) };
    for n in 0..100 {
        push(&m, (4, n), (3, 256 << 10), 0, bw, false); // 字节令牌桶 1 MiB/s：每秒 4 个 256 KiB
    }
    let got = run(&m, &at, 10.0);
    let n = count(&got, 4);
    assert!((42..=45).contains(&n), "令牌桶 1 MiB/s、256 KiB 一个：桶满起步的 4 个 + 之后 10 s 里约 40 个：{n}");
}

#[test]
fn the_bucket_is_full_even_after_a_long_idle_time() {
    let m = Mclock::new();
    let at = clock(&m);
    let bw = QosSpec { bw_max: 1048576.0, burst_ms: 5000, ..spec(0.0, 1.0, 0.0) }; // 桶深 5 MiB
    let burst = |from: f64, base: u64| {
        for n in 0..4 {
            push(&m, (4, base + n), (9, 1 << 20), 0, bw, false);
        }
        (0..4).filter(|i| m.dequeue(at(from + *i as f64 * 0.001)).is_ok()).count()
    };
    assert_eq!(burst(0.0, 0), 4, "新队列桶满起步：4 个 1 MiB 马上放行");
    assert_eq!(m.dequeue(at(200.0)), Err(None), "空闲 200 s：队列被回收");
    assert_eq!(burst(200.0, 10), 4, "回收之后重建：照样桶满，不是空桶");
}

#[test]
fn jobs_rotate_background_is_low_priority_and_idle_queues_go_away() {
    let m = Mclock::new();
    let at = clock(&m);
    for n in 0..4 {
        push(&m, (1, n), (1, 4096), 10, spec(0.0, 1.0, 0.0), false);
    }
    for n in 4..8 {
        push(&m, (1, n), (1, 4096), 20, spec(0.0, 1.0, 0.0), false);
    }
    let order: Vec<u64> = (0..8).map(|i| m.dequeue(at(i as f64 * 0.001)).unwrap() & 0xffff).collect();
    assert_eq!(order, vec![0, 4, 1, 5, 2, 6, 3, 7], "队列里按作业 id 轮转");
    push(&m, (9, 0), (1, 4096), 0, spec(0.0, 1.0, 0.0), true); // 后台：进固定的低优先级队列（不论给的规格）
    push(&m, (1, 100), (1, 4096), 10, spec(0.0, 1.0, 0.0), false);
    assert_eq!(m.dequeue(at(1.0)).unwrap(), 1 << 32 | 100, "normal 先");
    assert_eq!(m.dequeue(at(1.0)).unwrap(), 9 << 32, "没有 normal 了才轮到后台");
    assert_eq!(m.dequeue(at(1.0)), Err(None), "全空");
    assert!(m.stats().iter().any(|s| s.key == BG_QUEUE));
    assert_eq!(m.dequeue(at(70.0)), Err(None));
    assert!(m.stats().is_empty(), "空闲 60 s 的队列回收");
}

#[test]
fn a_queue_that_becomes_active_is_not_starved_nor_starving() {
    let m = Mclock::new();
    let at = clock(&m);
    for n in 0..2000 {
        push(&m, (1, n), (1, 4096), 0, spec(0.0, 1.0, 0.0), false);
    }
    for i in 0..1000 {
        m.dequeue(at(i as f64 * 0.001)).unwrap(); // 队列 1 一直忙：它的 P 已经跑到 1000 了
    }
    for n in 0..1000 {
        push(&m, (2, n), (1, 4096), 0, spec(0.0, 1.0, 0.0), false);
    }
    let next: Vec<u64> = (0..100).map(|i| m.dequeue(at(1.0 + i as f64 * 0.001)).unwrap() >> 32).collect();
    let two = next.iter().filter(|q| **q == 2).count();
    assert!(
        (45..=55).contains(&two),
        "刚活跃的队列从活跃队列最小的 P 起算：与一直忙着的那个对半分，不独占也不被饿死：{two}"
    );
}

#[test]
fn a_starved_low_queue_does_not_set_the_start_of_a_new_normal_queue() {
    let m = Mclock::new();
    let at = clock(&m);
    for n in 0..20_000 {
        push(&m, (1, n), (1, 4096), 0, spec(0.0, 1.0, 0.0), false);
    }
    push(&m, (9, 0), (1, 4096), 0, spec(0.0, 1.0, 0.0), true); // 后台请求：一直被 normal 饿着，P 停在到达时刻附近
    for i in 0..10_000 {
        m.dequeue(at(i as f64 * 0.001)).unwrap(); // 队列 1 独自忙了 10 s：它的 P 跑到了 10000
    }
    for n in 0..2000 {
        push(&m, (2, n), (1, 4096), 0, spec(0.0, 1.0, 0.0), false);
    }
    let next: Vec<u64> = (0..2000).map(|i| m.dequeue(at(10.0 + i as f64 * 0.001)).unwrap() >> 32).collect();
    let two = next.iter().filter(|q| **q == 2).count();
    assert!((900..=1100).contains(&two), "新来的 normal 队列只从 normal 队列的 P 起算：与一直忙着的那个对半分：{two}");
}

#[test]
fn a_deficit_needs_a_window_that_was_mostly_backlogged() {
    let m = Mclock::new();
    let at = clock(&m);
    let deficit = |m: &Mclock, q: u64| m.stats().iter().find(|s| s.key == key(q)).map(|s| s.deficit_per_s);
    for n in 0..20_000 {
        push(&m, (1, n), (1, 4096), 0, spec(2000.0, 1.0, 0.0), false); // 保底 2000/s：比"io 线程"每秒能派发的 1000 个多
    }
    run(&m, &at, 5.0);
    assert_eq!(deficit(&m, 1), Some(0.0), "第一个 10 s 窗口还没结束：不报（刚活跃的队列不误报）");
    for i in 5000..11_000 {
        m.dequeue(at(i as f64 / 1000.0)).unwrap();
    }
    let d = deficit(&m, 1).unwrap();
    assert!((d - 1000.0).abs() < 50.0, "一直有积压、每秒只派发了约 1000：欠额约 2000 − 1000：{d}");

    let m = Mclock::new();
    let at = clock(&m);
    for i in 0..1100 {
        let t = i as f64 * 0.01; // 单流的客户端：每 10 ms 一个请求，来了马上就派发
        let _ = m.dequeue(at(t));
        push(&m, (2, i), (1, 4096), 0, spec(2000.0, 1.0, 0.0), false);
        m.dequeue(at(t)).unwrap();
    }
    assert_eq!(deficit(&m, 2), Some(0.0), "派发速率远低于保底，但几乎从不积压：不是保底没给够");
}

#[test]
fn only_the_first_100_jobs_get_their_own_counter() {
    let m = Mclock::new();
    let at = clock(&m);
    for job in 0..150 {
        push(&m, (1, job), (1, 4096), 1_000_000 + job, spec(0.0, 1.0, 0.0), false);
    }
    for i in 0..150 {
        m.dequeue(at(i as f64 * 0.001)).unwrap();
    }
    assert_eq!(lock(&m.st).jobs.len(), MAX_JOBS, "之后的作业不再占内存：一律记进 other");
}
