//! 挂载级的写回槽位计数（design.md 3.6.1：一个挂载的写回缓冲最多占 M+L 档槽位的 60%；用法见 `writeback.rs` 文件头的"按槽位计"）。
//! 实现注记（M10）：为了 writeback.rs ≤ 400 行从那里挪出来。

use std::sync::{Condvar, Mutex};
use std::time::Duration;

use crate::base::lock;
use crate::buf::MemPool;

pub struct Slots {
    used: Mutex<usize>,
    cv: Condvar,
    pub(super) max: usize,
}

impl Slots {
    pub fn for_pool(p: &MemPool) -> Slots {
        let ml: u32 = p.stats().iter().filter(|c| c.name != "S").map(|c| c.slots).sum();
        Slots { used: Mutex::new(0), cv: Condvar::new(), max: (ml as usize * 3 / 5).max(1) }
    }

    pub(super) fn used(&self) -> usize {
        *lock(&self.used)
    }

    /// 还有空槽位就占一个（返回 true）。
    pub(super) fn try_take(&self) -> bool {
        let mut u = lock(&self.used);
        if *u >= self.max {
            return false;
        }
        *u += 1;
        true
    }

    /// 等别人放掉槽位，最多 `ms` 毫秒。
    pub(super) fn wait(&self, ms: u64) {
        let u = lock(&self.used);
        let _ = self.cv.wait_timeout(u, Duration::from_millis(ms));
    }

    pub(super) fn release(&self, n: usize) {
        if n > 0 {
            *lock(&self.used) -= n;
            self.cv.notify_all();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Slots;
    use crate::buf::{MemPool, PoolConfig};

    #[test]
    fn slots_are_sixty_percent_of_the_m_and_l_slots() {
        let pool = MemPool::new(&PoolConfig { s_mb: 1, m_slots: 9, l_slots: 1, repl_lane: false }).unwrap();
        let s = Slots::for_pool(&pool);
        assert_eq!(s.max, 6, "(9 + 1) × 60%");
        assert!((0..6).all(|_| s.try_take()) && !s.try_take(), "占满 6 个之后再要就没有了");
        s.release(6);
        assert_eq!(s.used(), 0);
    }
}
