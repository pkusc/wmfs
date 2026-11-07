//! 按存储类统计客户端请求的延迟（design.md 3.11、6.2 第 4 层，ADR-0044 第 5 条）：数据服务收到请求时（`service.rs` 的
//! `Service::call`）给回复句柄登记一个回调（`Responder::on_done`），回复成功发出时记下从到达到完成的时长；只统计集群图里设了
//! `latency_target` 的类，后台请求不算。hb 线程每次心跳报每个类最近 [`WINDOW_MS`] 的 p99（[`ClassLatency::p99s`] →
//! `NodeHeartbeat.class_p99`），leader 的 cluster-mgr 拿它与目标比。
//!
//! 实现注记（M13 收尾）：最简单的写法——每类最多留最近的 [`MAX_SAMPLES`] 个样本（多了丢最旧的），算 p99 时排序（最近秩：排好序的第
//! ⌈0.99 n⌉ 个）。样本少于 [`MIN_SAMPLES`] 个的类不报：那时 p99 就是最大值，一个慢请求就能触发告警。请求很多时（每秒几百个以上）
//! 4096 个样本只覆盖窗口里最近的一段，算的是最近这些请求的 p99。时长取自回复句柄（`Instant`，与每个消息族的耗时直方图相同：
//! `base::clock` 只有毫秒，而目标是微秒级的）；样本的时刻用 `clock::mono_ms`。

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

use crate::base::lock;

/// 统计窗口：最近 10 s（ADR-0044 第 5 条）。
pub const WINDOW_MS: u64 = 10_000;
/// 每类最多留多少个样本、至少多少个样本才报 p99（见文件头）。
const MAX_SAMPLES: usize = 4096;
const MIN_SAMPLES: usize = 100;

/// 各类最近的样本：类 id → (完成时刻 mono_ms, 微秒)，按完成的先后。
#[derive(Default)]
pub struct ClassLatency {
    m: Mutex<HashMap<u16, VecDeque<(u64, u32)>>>,
}

impl ClassLatency {
    /// 记一个在 `at_ms` 完成、用了 `us` 微秒的请求。
    pub fn record(&self, class: u16, at_ms: u64, us: u32) {
        let mut m = lock(&self.m);
        let q = m.entry(class).or_default();
        if q.len() >= MAX_SAMPLES {
            q.pop_front();
        }
        q.push_back((at_ms, us));
    }

    /// 每个类最近 [`WINDOW_MS`] 的 p99（微秒；样本够的才有，按类 id 排序）。顺便丢掉窗口之前的样本与没有样本了的类。
    pub fn p99s(&self, now_ms: u64) -> Vec<(u16, u32)> {
        let mut m = lock(&self.m);
        m.retain(|_, q| {
            while q.front().is_some_and(|(t, _)| now_ms.saturating_sub(*t) > WINDOW_MS) {
                q.pop_front();
            }
            !q.is_empty()
        });
        let mut out: Vec<(u16, u32)> =
            m.iter().filter_map(|(k, q)| p99(q.iter().map(|(_, us)| *us).collect()).map(|p| (*k, p))).collect();
        out.sort_unstable();
        out
    }
}

/// p99（见文件头）；样本少于 [`MIN_SAMPLES`] 个 → None。
fn p99(mut us: Vec<u32>) -> Option<u32> {
    if us.len() < MIN_SAMPLES {
        return None;
    }
    us.sort_unstable();
    us.get((us.len() * 99).div_ceil(100) - 1).copied()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn p99_is_the_nearest_rank_and_needs_enough_samples() {
        assert_eq!(p99((1..=100).collect()), Some(99), "100 个样本：第 99 个");
        assert_eq!(p99((1..=1000).rev().collect()), Some(990), "乱序也一样：1000 个里的第 990 个");
        assert_eq!(p99(vec![7; 99]), None, "不到 100 个样本不报");
        let mut slow = vec![100; 199];
        slow.push(90_000);
        assert_eq!(p99(slow), Some(100), "200 个里只有一个慢的：p99 不是它");
    }

    #[test]
    fn only_the_last_10_s_and_the_last_4096_samples_count() {
        let l = ClassLatency::default();
        for i in 0..150 {
            l.record(3, 1_000, 5_000 + i); // 第 1 s：慢
            l.record(4, 1_000, 10);
        }
        for i in 0..150 {
            l.record(3, 9_000, 100 + i); // 第 9 s：快
        }
        assert_eq!(l.p99s(10_000), vec![(3, 5_146), (4, 10)], "窗口里 300 个样本：p99 落在慢的那一批里");
        assert_eq!(l.p99s(11_001), vec![(3, 248)], "第 1 s 的样本过了 10 s：只剩快的；类 4 没有样本了");
        for i in 0..1000 + 4096 {
            l.record(5, 12_000, if i < 1000 { 1_000_000 } else { 20 });
        }
        assert_eq!(l.p99s(12_000).last(), Some(&(5, 20)), "每类只留最近的 4096 个：最早的 1000 个慢样本被挤掉了");
    }
}
