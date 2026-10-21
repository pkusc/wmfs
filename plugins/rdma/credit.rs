//! 信用控制（design.md 3.6.4；实现注记（M2）⑦）：对端为这条连接只 post 了 16 个接收槽，所以在途的请求帧最多 16 个——
//! 第 17 个 `send` 阻塞**写线程**（不影响别的线程），于是永不触发 RNR（`rnr_retry = 7` 只兜底）。
//!
//! 按**帧**计，不按 req_id：每发出一个 Request 帧（不管 req_id 是多少：握手的 Hello/Auth、`req_id = 0` 的保活 Ping 都算）
//! 减一，每收到一个 Response 帧（含 ERROR）加一（不超过上限）。按 req_id 记账的话，两个 `req_id = 0` 的保活会互相覆盖，
//! 信用要么泄漏要么多放。
//!
//! 等信用**不设期限**：一直等到收到回复（还回信用）或连接关闭（`close` 唤醒等待者，返回 `ECONNRESET`）。"16 个请求都还没
//! 回"说明对端回得慢，不是"对端不读 socket"（那才是写超时），不能因此断开——tcp 在同样的情况下也不断开。对端真的死了由读
//! 线程发现（15 s 收不到任何帧，或引导 TCP 读到 EOF），它关闭连接时会唤醒这里。

use std::sync::{Condvar, Mutex, MutexGuard};

use wmfs_core::base::ErrCode;

pub struct Credits {
    max: u32,
    st: Mutex<CreditState>,
    cv: Condvar,
}

/// 同一把锁下的信用状态。
struct CreditState {
    left: u32,    // 剩余信用
    closed: bool, // 连接已关闭
}

impl Credits {
    pub fn new(max: u32) -> Credits {
        Credits { max, st: Mutex::new(CreditState { left: max, closed: false }), cv: Condvar::new() }
    }

    fn lock(&self) -> MutexGuard<'_, CreditState> {
        self.st.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// 取一个信用；没有就一直等（只阻塞写线程），直到收到回复，或连接关闭（→ `ECONNRESET`）。
    pub fn take(&self) -> Result<(), ErrCode> {
        let mut st = self.lock();
        loop {
            if st.closed {
                return Err(ErrCode::ECONNRESET);
            }
            if st.left > 0 {
                st.left -= 1;
                return Ok(());
            }
            st = self.cv.wait(st).unwrap_or_else(|e| e.into_inner());
        }
    }

    /// 收到一个 Response 帧：还回一个信用（多出来的回复不会让信用超过上限）。
    pub fn give(&self) {
        let mut st = self.lock();
        st.left = (st.left + 1).min(self.max);
        drop(st);
        self.cv.notify_one();
    }

    /// 连接关闭：唤醒所有等待者（它们返回 `ECONNRESET`）。
    pub fn close(&self) {
        self.lock().closed = true;
        self.cv.notify_all();
    }

    /// 剩余信用（测试用）。
    #[cfg(test)]
    pub fn available(&self) -> u32 {
        self.lock().left
    }
}
