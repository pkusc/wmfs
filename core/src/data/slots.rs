//! 数据服务的排队位置（design.md 4.5，ADR-0044 第 6 条）：整个节点最多 [`MAX_QUEUED`] 个排队的请求（M5 起），每个 QoS 队列
//! （`QueueKey`；后台请求都在 `BG_QUEUE` 这一个队列里）最多 [`MAX_PER_QUEUE`] 个。M13 起上限 `l` 会让请求积压在 mclock 里：只有节点
//! 共用的上限时，一个被限速的租户就能占满全部位置，别的租户也拿到 `BUSY`，打破"每 (租户, 类) 一个三元组"的隔离。超了只对这个请求回
//! `BUSY`（客户端退避重试）；总上限不变。

use std::collections::HashMap;

use crate::hooks::traits::QueueKey;

/// 整个节点、每个 QoS 队列最多排多少个请求（= 4096 / 8）。
pub const MAX_QUEUED: usize = 4096;
pub const MAX_PER_QUEUE: usize = MAX_QUEUED / 8;

/// 排了多少个：全部与每个队列的（排着的才有一项）。
#[derive(Default)]
pub struct Slots {
    total: usize,
    per_queue: HashMap<QueueKey, usize>,
}

impl Slots {
    /// 给队列 `q` 的一个请求占一个位置；节点或这个队列已经排满 → false（回 `BUSY`）。
    pub fn take(&mut self, q: QueueKey) -> bool {
        if self.total >= MAX_QUEUED || self.per_queue.get(&q).is_some_and(|n| *n >= MAX_PER_QUEUE) {
            return false;
        }
        self.total += 1;
        *self.per_queue.entry(q).or_default() += 1;
        true
    }

    /// 还回队列 `q` 的一个位置（请求出队了，或没能交给 qos 插件）。
    pub fn give_back(&mut self, q: QueueKey) {
        if let Some(n) = self.per_queue.get_mut(&q) {
            self.total -= 1;
            *n -= 1;
            if *n == 0 {
                self.per_queue.remove(&q);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hooks::traits::BG_QUEUE;

    fn key(tenant: u32) -> QueueKey {
        QueueKey { tenant, qos_id: 18, class_id: 1 }
    }

    #[test]
    fn a_full_queue_gets_busy_and_the_others_do_not() {
        let mut s = Slots::default();
        assert!((0..512).all(|_| s.take(key(1))), "一个队列最多排 512 个");
        assert!(!s.take(key(1)), "塞满了：它的新请求 BUSY");
        assert!(s.take(key(2)) && s.take(BG_QUEUE), "别的租户的队列、后台队列照常收");
        s.give_back(key(1));
        assert!(s.take(key(1)), "出队一个，又能收一个");
        assert!((0..511).all(|_| s.take(BG_QUEUE)) && !s.take(BG_QUEUE), "后台队列同样最多 512 个");
        let mut all = Slots::default();
        assert!((0..4096).all(|i| all.take(key(i / 512))), "8 个队列各 512 个 = 总上限 4096");
        assert!(!all.take(key(100)), "总上限不变：新的队列也 BUSY");
        all.give_back(key(0));
        assert!(all.take(key(100)) && all.per_queue.len() == 9);
    }
}
