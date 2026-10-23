//! fifo：qos 插件（design.md 4.5，altitude 401）——最简单的先进先出，给不需要 QoS 的部署用（`minimal` profile；M5 还没有 mclock）。
//!
//! `enqueue` 把 token 放到队尾，`dequeue` 从队首取（队列空 → `Err(None)`，本体在 Condvar 上睡 100 ms 或到有新请求时被唤醒——
//! 睡眠与唤醒归本体，4.5）。不看 QosSpec、不分租户与作业；"qos 只允许一个活跃"的规则不变（与 mclock 二选一）。
//! 指标 `wmfs_fifo_depth`、`wmfs_fifo_served_total`；fail point `plugin.fifo.qos`（本体包在 enqueue 外层，触发 = 这个请求以注入的码失败）。

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use wmfs_core::api::metrics::{self, Counter, Gauge};
use wmfs_core::base::lock;
use wmfs_core::hooks::manifest::{CORE_ABI_VERSION, Hook, Manifest};
use wmfs_core::hooks::traits::{QosItem, QosPlugin, QosSpec, QueueStat};

pub static MANIFEST: Manifest = Manifest {
    name: "fifo",
    version: "0.1.0",
    hooks: &[Hook::Qos],
    altitude: 401,
    hot_path: true,
    config_schema: "",
    abi_version: CORE_ABI_VERSION,
    attrs: &[],
    budget_ns: 2_000, // 3.7.4：qos 2 µs
    fail_points: &["plugin.fifo.qos"],
};

pub struct Fifo {
    q: Mutex<VecDeque<u64>>,
    depth: Arc<Gauge>,
    served: Arc<Counter>,
}

impl Fifo {
    pub fn new() -> Fifo {
        Fifo {
            q: Mutex::new(VecDeque::new()),
            depth: metrics::gauge("wmfs_fifo_depth", "fifo 队列里的请求数", &[]),
            served: metrics::counter("wmfs_fifo_served_total", "fifo 派发过的请求数", &[]),
        }
    }
}

impl Default for Fifo {
    fn default() -> Fifo {
        Fifo::new()
    }
}

impl QosPlugin for Fifo {
    fn manifest(&self) -> &'static Manifest {
        &MANIFEST
    }

    fn enqueue(&self, item: QosItem, _spec: QosSpec) {
        let mut q = lock(&self.q);
        q.push_back(item.token);
        self.depth.set(q.len() as f64);
    }

    fn dequeue(&self, _now: Instant) -> Result<u64, Option<Instant>> {
        let mut q = lock(&self.q);
        let t = q.pop_front().ok_or(None)?;
        self.depth.set(q.len() as f64);
        self.served.inc();
        Ok(t)
    }

    fn stats(&self) -> Vec<QueueStat> {
        vec![] // 不分队列，没有保底欠额可报
    }
}

#[cfg(test)]
mod tests;
