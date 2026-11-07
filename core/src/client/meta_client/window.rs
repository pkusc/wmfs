//! MetaClient 按 seq 计的在途窗口（design.md 3.5.2；`meta_client.rs` 文件头的"seq"一条）：分配 seq 时，新 seq 必须小于"最小的在途
//! seq + `DEDUP_WINDOW`"，否则等——所有在途请求的去重记录这时一定还在。实现注记（M12）：从 `meta_client.rs` 挪出来（每个文件
//! ≤ 400 行）。

use std::collections::BTreeSet;
use std::sync::{Condvar, Mutex};

use crate::base::lock;
use crate::proto::meta::DEDUP_WINDOW;

/// 在途的修改型请求：下一个要分配的 seq，与还没结束的请求的 seq。
struct Inflight {
    next: u64,
    seqs: BTreeSet<u64>,
}

/// 按 seq 计的在途窗口（见文件头）。
pub(super) struct Window {
    m: Mutex<Inflight>,
    cv: Condvar,
}

/// 占着一个在途的 seq；Drop 时（请求成功或最终失败）把它从在途集合里去掉，并唤醒等待者。
pub(super) struct Slot<'a> {
    w: &'a Window,
    pub(super) seq: u64,
}

impl Window {
    pub(super) fn new() -> Window {
        Window { m: Mutex::new(Inflight { next: 1, seqs: BTreeSet::new() }), cv: Condvar::new() }
    }

    pub(super) fn acquire(&self) -> Slot<'_> {
        let mut g = lock(&self.m);
        while g.seqs.first().is_some_and(|min| g.next >= min + DEDUP_WINDOW) {
            g = self.cv.wait(g).unwrap_or_else(|e| e.into_inner());
        }
        let seq = g.next;
        g.next += 1;
        g.seqs.insert(seq);
        Slot { w: self, seq }
    }
}

impl Drop for Slot<'_> {
    fn drop(&mut self) {
        lock(&self.w.m).seqs.remove(&self.seq);
        self.w.cv.notify_all(); // 最小的在途 seq 可能变大了：等待者都重新看一遍
    }
}
