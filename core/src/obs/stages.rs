//! 一个请求在本节点上走过的各阶段耗时（design.md 3.11 的"慢日志含各阶段耗时"，ADR-0045 第 9 条）。
//!
//! 起点是回复句柄 `Responder` 收到请求时的 `Instant`（与每个消息族的耗时直方图、ADR-0044 第 5 条的延迟统计同一个起点）。走完一个阶段时
//! [`Stages::mark`]：从上一个标记到现在的时长记在这个阶段名下，同一个阶段走了几次就累加（例如写在链头上的"磁盘" = 写 pending + 提交）。
//! 只有超过阈值的请求才把它写进慢日志（`obs::trace` 的慢日志那一行），只列走过的阶段。每个阶段一个 u32 微秒、一个"走过"位，大小固定、
//! 跟着请求走（回复句柄里；元数据的修改型请求经 `TxnRequest` 带进执行器、随 `done` 交回）：热路径上不加锁、不分配。用 `Cell`：同一时刻
//! 只有拿着这个请求的那一个线程在标记。
//!
//! 阶段：decode（连接读线程解出请求）、排队、authz、quota、执行、raft 提交（批次从提议到应用，含同一批里排在后面的请求的执行）、磁盘、
//! 转发（等后继确认）。实现注记（M14 收尾）：3.11 没有列"排队"（元数据：等执行器或 meta-read 线程；数据：QoS 队列、repl 队列、同一个
//! chunk 上前一个写），不单列的话排队的时间会算进下一个阶段——QoS 限流正是数据请求变慢最常见的原因。

use std::cell::Cell;
use std::fmt;
use std::time::Instant;

/// 阶段（见文件头）；数值是它在 [`Stages`] 里的下标。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stage {
    Decode,
    Queue,
    Authz,
    Quota,
    Exec,
    Raft,
    Disk,
    Forward,
}

/// 各阶段在慢日志里的名字（与 [`Stage`] 同序）。
const NAMES: [&str; 8] = ["decode", "排队", "authz", "quota", "执行", "raft 提交", "磁盘", "转发"];

/// 一个请求的各阶段耗时（见文件头）。
pub struct Stages {
    last: Cell<Instant>, // 上一个标记的时刻
    us: [Cell<u32>; 8],  // 每个阶段累计的微秒
    seen: Cell<u8>,      // 走过的阶段（第 i 位 = 第 i 个阶段）
}

impl Stages {
    /// 从 `t0`（收到请求的时刻）起计。
    pub fn new(t0: Instant) -> Stages {
        Stages { last: Cell::new(t0), us: Default::default(), seen: Cell::new(0) }
    }

    /// 走完阶段 `s`：从上一个标记到现在的时长记在它名下。
    pub fn mark(&self, s: Stage) {
        let now = Instant::now();
        let us = now.saturating_duration_since(self.last.replace(now)).as_micros().min(u32::MAX as u128) as u32;
        let i = s as usize;
        self.us[i].set(self.us[i].get().saturating_add(us));
        self.seen.set(self.seen.get() | (1 << i));
    }
}

/// 慢日志里的写法：`decode 0.0 ms、排队 0.1 ms、……`，只列走过的阶段（一个也没走过 → 空）。
impl fmt::Display for Stages {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut sep = "";
        for (i, name) in NAMES.iter().enumerate().filter(|(i, _)| self.seen.get() & (1 << *i) != 0) {
            write!(f, "{sep}{name} {:.1} ms", self.us[i].get() as f64 / 1000.0)?;
            sep = "、";
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn only_the_stages_walked_are_listed_and_repeats_add_up() {
        let s = Stages::new(Instant::now());
        assert_eq!(s.to_string(), "", "一个阶段也没走过");
        s.mark(Stage::Decode);
        std::thread::sleep(Duration::from_millis(3));
        s.mark(Stage::Disk);
        s.mark(Stage::Forward);
        std::thread::sleep(Duration::from_millis(2));
        s.mark(Stage::Disk);
        let t = s.to_string();
        assert!(t.starts_with("decode 0.") && t.contains("、磁盘 ") && t.ends_with(" ms"), "{t}");
        assert!(!t.contains("authz") && !t.contains("raft"), "没走过的不写：{t}");
        assert!(s.us[Stage::Disk as usize].get() >= 5000, "同一个阶段走了两次：累加（{t}）");
        let order: Vec<&str> = t.split('、').map(|p| p.split(' ').next().unwrap()).collect();
        assert_eq!(order, ["decode", "磁盘", "转发"], "按阶段的固定次序列出");
    }
}
