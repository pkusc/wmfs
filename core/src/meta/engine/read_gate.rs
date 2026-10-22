//! 读闸门：线性一致读的批量 ReadIndex（design.md 3.4.1.6，ADR-0019；否决 LeaseBased）。只在 raft 线程上用。
//!
//! 1. meta-read 线程发 `RaftInput::ReadIndex{done}` 后阻塞等待（3 s 超时）；读者先进 `waiting`。
//! 2. [`ReadGate::pump`]：没有在途轮次、有等待者、leader 已就绪 → 把**当前全部等待者**打包成一轮，调用一次
//!    `read_index(ctx)`（ctx 递增）；之后到达的读者等下一轮（否则可能漏掉本轮确认之后才提交的写）。
//!    不是 leader → 全部回 `NOT_LEADER`；是 leader 但未就绪 → 继续等（坑 P6：此时 raft-rs 会静默丢弃 MsgReadIndex）。
//! 3. Ready 里出现 ctx 匹配的 ReadState{index} → 这一包移入 `ready`；`applied ≥ index` 时逐个回 `Ok`，读者随后各自开
//!    redb 读事务读取（redb 的 None 提交对新读事务可见）。
//! 4. [`ReadGate::on_tick`]（每 100 ms）：在途轮次 500 ms 没有 ReadState → 用新 ctx 为同一批读者重发；首次发出已超过 2 s →
//!    全部回 `RETRY`；等待超过 2 s 的读者也回 `RETRY`。角色变化 → 全部 `NOT_LEADER`。
//!
//! 单节点集群：raft-rs 对单成员组立即产生 ReadState，没有额外开销。时间按 tick 计（确定性模拟里不依赖真实时间）。

use std::collections::VecDeque;

use crossbeam_channel::Sender;

use crate::base::ErrCode;

/// 在途轮次多少 tick 没有 ReadState 就重发（500 ms）、多少 tick 之后放弃（2 s）。
const RESEND_TICKS: u64 = 5;
const GIVE_UP_TICKS: u64 = 20;

struct Waiter {
    done: Sender<Result<(), ErrCode>>,
    arrived: u64, // tick
}

struct Round {
    ctx: u64,
    readers: Vec<Waiter>,
    sent: u64,
    first_sent: u64,
}

pub struct ReadGate {
    waiting: Vec<Waiter>,                // 尚未进入任何一轮的读者
    inflight: Option<Round>,             // 已调用 read_index、等待 ReadState
    ready: VecDeque<(u64, Vec<Waiter>)>, // 已拿到 ReadState，等 applied ≥ index
    next_ctx: u64,
    tick: u64,
}

impl Default for ReadGate {
    fn default() -> ReadGate {
        ReadGate::new()
    }
}

impl ReadGate {
    pub fn new() -> ReadGate {
        ReadGate { waiting: Vec::new(), inflight: None, ready: VecDeque::new(), next_ctx: 1, tick: 0 }
    }

    /// 一个读者到达。
    pub fn add(&mut self, done: Sender<Result<(), ErrCode>>) {
        self.waiting.push(Waiter { done, arrived: self.tick });
    }

    /// 需要时发起一轮 read_index（只在 Ready 处理之前调用，坑 P1）。
    pub fn pump(&mut self, leader: bool, ready: bool, read_index: &mut dyn FnMut(Vec<u8>)) {
        if self.inflight.is_some() || self.waiting.is_empty() {
            return;
        }
        if !leader {
            reply(std::mem::take(&mut self.waiting), Err(ErrCode::NOT_LEADER));
            return;
        }
        if ready {
            let readers = std::mem::take(&mut self.waiting);
            let ctx = send(&mut self.next_ctx, read_index);
            self.inflight = Some(Round { ctx, readers, sent: self.tick, first_sent: self.tick });
        }
    }

    /// Ready 里的一个 ReadState：ctx 与在途轮次相同 → 这一包等 applied ≥ index。
    pub fn on_read_state(&mut self, index: u64, ctx: &[u8]) {
        if self.inflight.as_ref().is_some_and(|r| r.ctx.to_be_bytes() == ctx)
            && let Some(r) = self.inflight.take()
        {
            self.ready.push_back((index, r.readers));
        }
    }

    /// 应用推进到 `applied`：read index ≤ applied 的读者可以读了。
    pub fn on_applied(&mut self, applied: u64) {
        while self.ready.front().is_some_and(|(i, _)| *i <= applied) {
            if let Some((_, readers)) = self.ready.pop_front() {
                reply(readers, Ok(()));
            }
        }
    }

    /// 每 tick：重发、超时。
    pub fn on_tick(&mut self, leader: bool, read_index: &mut dyn FnMut(Vec<u8>)) {
        self.tick += 1;
        let now = self.tick;
        let (expired, still_waiting): (Vec<Waiter>, Vec<Waiter>) =
            std::mem::take(&mut self.waiting).into_iter().partition(|w| now - w.arrived >= GIVE_UP_TICKS);
        self.waiting = still_waiting;
        reply(expired, Err(ErrCode::RETRY));
        let Some(r) = &mut self.inflight else { return };
        if now - r.first_sent >= GIVE_UP_TICKS {
            let readers = self.inflight.take().map(|r| r.readers).unwrap_or_default();
            reply(readers, Err(ErrCode::RETRY));
        } else if now - r.sent >= RESEND_TICKS && leader {
            // 同一批读者换个新 ctx 重发：leader 本任期尚未提交时 raft-rs 会静默丢弃 MsgReadIndex（坑 P6）
            (r.ctx, r.sent) = (send(&mut self.next_ctx, read_index), now);
        }
    }

    /// 角色变化、退出：全部读者以 `code` 结束。
    pub fn fail_all(&mut self, code: ErrCode) {
        reply(std::mem::take(&mut self.waiting), Err(code));
        if let Some(r) = self.inflight.take() {
            reply(r.readers, Err(code));
        }
        for (_, readers) in std::mem::take(&mut self.ready) {
            reply(readers, Err(code));
        }
    }

    /// 还没有结果的读者数（测试用）。
    #[cfg(test)]
    fn pending(&self) -> usize {
        self.waiting.len()
            + self.inflight.as_ref().map_or(0, |r| r.readers.len())
            + self.ready.iter().map(|(_, r)| r.len()).sum::<usize>()
    }
}

/// 发起一次 read_index（ctx 递增），返回这次用的 ctx。
fn send(next_ctx: &mut u64, read_index: &mut dyn FnMut(Vec<u8>)) -> u64 {
    let ctx = *next_ctx;
    *next_ctx += 1;
    read_index(ctx.to_be_bytes().to_vec());
    ctx
}

fn reply(ws: Vec<Waiter>, r: Result<(), ErrCode>) {
    for w in ws {
        let _ = w.done.try_send(r);
    }
}

#[cfg(test)]
mod tests;
