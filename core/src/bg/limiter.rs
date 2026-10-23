//! 后台任务的限速（design.md 3.10）：任务级（`TaskRecV1.bytes_per_sec / items_per_sec`，`wmfs task set` 改，0 = 不限）与节点级
//! （`ClusterSettingsV1.bg_bytes_per_sec`，默认 200 MiB/s）两个上限，取更严的那个。
//!
//! 实现：按"这一步做了多少"算出它在限速下至少该花多久，没花够就让调度器在下一步之前睡够（与令牌桶等价、不攒额度的写法）。
//! 一步本身很小（≤ 100 个对象 / ≤ 64 MiB / ≤ 1 s，3.10），所以不攒额度也不会让速率明显低于上限。

use std::time::Duration;

/// 任务级 + 节点级的限速器（调度器每个任务一个，只在 bg-sched 线程上用，所以不加锁）。三个上限：节点级字节/秒、任务级字节/秒、
/// 任务级对象/秒（0 = 不限）。
#[derive(Default)]
pub struct RateLimiter {
    node_bps: u64,
    task_bps: u64,
    task_ips: u64,
}

impl RateLimiter {
    pub fn new() -> RateLimiter {
        RateLimiter::default()
    }

    /// 换上新的上限（调度器每次从 TaskRecV1 与集群参数读到之后调用）。
    pub fn set(&mut self, node_bps: u64, task_bps: u64, task_ips: u64) {
        (self.node_bps, self.task_bps, self.task_ips) = (node_bps, task_bps, task_ips);
    }

    /// 这一步做了 `bytes` 字节、`items` 个对象、花了 `took`：下一步之前还要等多久。
    pub fn wait_after(&self, bytes: u64, items: u64, took: Duration) -> Duration {
        let bps = [self.node_bps, self.task_bps].into_iter().filter(|x| *x > 0).min();
        let need_b = bps.map_or(0.0, |r| bytes as f64 / r as f64);
        let need_i = if self.task_ips > 0 { items as f64 / self.task_ips as f64 } else { 0.0 };
        Duration::from_secs_f64(need_b.max(need_i)).saturating_sub(took)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_stricter_limit_wins_and_time_already_spent_counts() {
        let mut r = RateLimiter::new();
        assert_eq!(r.wait_after(1 << 30, 1000, Duration::ZERO), Duration::ZERO, "不限");
        r.set(100, 0, 0);
        assert_eq!(r.wait_after(200, 0, Duration::ZERO), Duration::from_secs(2), "节点级 100 B/s");
        r.set(100, 50, 0);
        assert_eq!(r.wait_after(200, 0, Duration::ZERO), Duration::from_secs(4), "任务级更严");
        r.set(0, 0, 10);
        assert_eq!(
            r.wait_after(0, 30, Duration::from_secs(1)),
            Duration::from_secs(2),
            "对象数上限，减去已经花掉的 1 s"
        );
        assert_eq!(r.wait_after(0, 5, Duration::from_secs(1)), Duration::ZERO, "已经花够了");
    }
}
