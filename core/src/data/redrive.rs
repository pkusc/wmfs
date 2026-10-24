//! 前滚（re-drive，design.md 3.4.2.6）：pending 版本**从不回滚**——它可能已在下游提交并被读到；前滚永远安全（写者还没收到确认，
//! "发生"或"不发生"都合法，写者的重试会作为新版本写入同样的内容）。
//!
//! - **链变化**（[`switch`]：hb 线程拉到新集群图时，**先**对每条变了的链做完下面的处理，**再**把本地视图切换到新版本——读写都带
//!   `chain_ver`，视图切换之前不会有人按新链读到它；解决了 pending 的 chunk 上排着的写，换上新视图之后才交给 repl 线程做——按旧视图
//!   做的话会转发给刚被移除的后继、或在已不是 head 的节点上分配新版本，也不在 hb 线程上写盘）：
//!   1. 自己被移出该链，或被降为 Syncing → 放弃自己在该链上的全部 pending（删 pending 文件，回复 upstream RETRY）；
//!   2. 自己在新链中是 last serving，而旧链的 last serving 已不在新链的 Serving 成员里 → 把自己在该链上的 pending 版本**立即本地
//!      提交**（被移除的旧 L 可能已把它们返回给读者；之后只可能是 Syncing 成员，没有读者能观察到暂时违反的 I2）；
//!   3. 自己成为 tail（没有后继了）→ 提交全部 pending 并向 upstream 确认；
//!   4. 后继变了 → 把 pending 版本（完整形式）重新转发给新后继：切换之前只在 pending 上记下新后继（旧后继迟到的确认就不算了，
//!      replica.rs）并让 chain-timer 下一拍（≤ 0.5 s）就重新驱动——按切换之后的视图发（新链的 chain_ver、按新图找新后继的地址：新加入
//!      的节点在旧图里没有地址），打包读盘也不在 hb 线程上。后继没变、只是链版本变了的也这样（旧版本下发出的转发会被已换图的后继拒绝；
//!      本节点换图晚于后继时就不必等满 3 s，重新驱动永远安全）。
//! - **chain-timer**（[`start`]）：每 500 ms 扫描 pending 表，超过 3 s 未确认的按当前视图重新驱动（有后继 → 完整重发；没有后继 →
//!   提交并确认；自己已不在链上 → 放弃）。计数 `wmfs_craq_redrive_total`。

use std::sync::Arc;
use std::time::Duration;

use super::replica::ReplJob;
use super::state::PendingWrite;
use super::write::{self, WriteJob};
use super::{DataNode, chain, forward};
use crate::base::types::ChunkId;
use crate::base::{clock, lock, log};
use crate::meta::cluster::chains::SERVING;
use crate::proto::peer::ClusterMap;

/// chain-timer 的周期与"多久没确认就重新驱动"。
pub const TICK_MS: u64 = 500;
pub const REDRIVE_MS: u64 = 3000;
/// 链变化时等一个正忙（正在写盘）的 chunk 最多多久。
const BUSY_WAIT_MS: u64 = 2000;

/// 起 `chain-timer` 线程（随进程一直运行）。
pub fn start(node: &Arc<DataNode>) -> Result<(), String> {
    let node = node.clone();
    std::thread::Builder::new()
        .name("chain-timer".into())
        .spawn(move || {
            loop {
                std::thread::sleep(Duration::from_millis(TICK_MS));
                let now = clock::mono_ms();
                for id in node.chunks.pendings(|_, p| now.saturating_sub(p.last_sent) >= REDRIVE_MS) {
                    redrive(&node, &id);
                }
            }
        })
        .map(|_| ())
        .map_err(|e| format!("起 chain-timer 线程失败：{e}"))
}

/// 按当前视图重新驱动一个 pending（见文件头）。
pub fn redrive(node: &Arc<DataNode>, id: &ChunkId) {
    node.craq.redrive.inc();
    let Some(map) = node.map.get() else { return };
    let here = chain::find(&map, id.chain).and_then(|c| chain::local(node, c).map(|(_, pos)| chain::successor(c, pos)));
    match here {
        Some(Some(s)) => forward::resend_full(node, id, s), // 发出去了，回复由 repl 线程处理
        Some(None) => {
            if let Some(p) = node.chunks.take_pending(id, None) {
                write::drain(node, forward::settle(node, id, p)); // 自己是尾了：提交并确认
            }
        }
        None => {
            if let Some(p) = node.chunks.take_pending(id, None) {
                write::drain(node, forward::abandon(node, id, p)); // 已经不在这条链上
            }
        }
    }
}

/// 换上一张新集群图（hb 线程拉到新图时；组件测试直接调用）：先按 3.4.2.6 处理变了的链上的 pending，再切换视图，最后把解决了 pending
/// 的 chunk 上排着的写交给 repl 线程（见文件头）。
pub fn switch(node: &Arc<DataNode>, new: Arc<ClusterMap>) {
    let queued = apply_map(node, node.map.get().as_deref(), &new);
    node.map.set(new);
    for j in queued {
        let j = match node.repl.get() {
            Some(q) => match q.try_send(ReplJob::Drain(j)) {
                Ok(()) => continue,
                Err(e) => e.into_inner(),
            },
            None => ReplJob::Drain(j),
        };
        if let ReplJob::Drain(j) = j {
            write::drain(node, Some(j)); // repl 队列满（或组件测试里没有 repl 线程）：就在这里做
        }
    }
}

/// 取出 pending（chunk 正在写盘就等一会儿，最多 2 s：链变化的处理要在切换视图之前做完）。
fn take_wait(node: &DataNode, id: &ChunkId) -> Option<PendingWrite> {
    let deadline = clock::mono_ms() + BUSY_WAIT_MS;
    loop {
        if let Some(p) = node.chunks.take_pending(id, None) {
            return Some(p);
        }
        if node.chunks.with_pending(id, None, |_| ()).is_none() || clock::mono_ms() > deadline {
            return None;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// 链变化（见文件头）：[`switch`] 在 `map.set(new)` 之前调用；返回解决了 pending 的 chunk 上排着的写。另外忘掉"转发不到后继"的记录里
/// 后继已经换掉的那些（否则心跳会一直报它，那个 target 回来、被追加进链之后又被链管理器当成"10 s 转发不到"移除）。
#[allow(clippy::vec_box)] // 排着的写在状态表里本来就是装箱的（WriteJob 带着 cap），原样交出去，不拆箱再装箱
fn apply_map(node: &Arc<DataNode>, old: Option<&ClusterMap>, new: &ClusterMap) -> Vec<Box<WriteJob>> {
    let mut queued = Vec::new();
    let succ_of =
        |c: u32| chain::find(new, c).and_then(|c| chain::local(node, c).and_then(|(_, p)| chain::successor(c, p)));
    lock(&node.fwd_fail).retain(|(c, s), _| succ_of(*c) == Some(*s));
    for id in node.chunks.pendings(|_, _| true) {
        let (c0, c1) = (old.and_then(|m| chain::find(m, id.chain)), chain::find(new, id.chain));
        if c0.map(|c| c.ver) == c1.map(|c| c.ver) {
            continue;
        }
        let was_serving = c0.and_then(|c| chain::local(node, c).map(|(_, p)| chain::serving_at(c, p))).unwrap_or(false);
        let here = c1.and_then(|c| chain::local(node, c).map(|(t, pos)| (c, t, pos)));
        let Some((c, t, pos)) = here.filter(|(c, _, pos)| !was_serving || chain::serving_at(c, *pos)) else {
            if let Some(p) = take_wait(node, &id) {
                log::info("data", format_args!("{id:?}：本节点被移出链或降为 Syncing，放弃 pending v{}", p.version));
                queued.extend(forward::abandon(node, &id, p)); // 规则 1
            }
            continue;
        };
        let old_l = c0.and_then(chain::last_serving);
        let old_l_gone = old_l.is_some_and(|l| !c.members.iter().any(|(x, s)| *x == l && *s == SERVING));
        if chain::last_serving(c) == Some(t) && old_l_gone {
            commit_local(node, &id); // 规则 2
        }
        match chain::successor(c, pos) {
            None => {
                if let Some(p) = take_wait(node, &id) {
                    queued.extend(forward::settle(node, &id, p)); // 规则 3
                }
            }
            // 规则 4（后继变了），以及后继没变、只是链版本变了的：旧链版本下发出的转发会被已换图的后继拒绝（STALE_CHAIN）。都记下
            // 当前后继，让 chain-timer 下一拍按新视图完整重发（见文件头）
            Some(s) => {
                node.chunks.with_pending(&id, None, |p| (p.successor, p.last_sent) = (Some(s), 0));
            }
        }
    }
    queued
}

/// 规则 2：把 pending 版本立即在本地提交（之后仍等后面的成员确认，再回复 upstream）。提交失败：pending 照样留着（还没提交）——后面的
/// 成员可能已经提交了这个版本，不能放弃（同 forward.rs 的 `settle`），chain-timer 重新驱动时再试。
fn commit_local(node: &Arc<DataNode>, id: &ChunkId) {
    let Some(mut p) = take_wait(node, id) else { return };
    if !p.committed {
        match write::commit(node, id, p.target, p.sync, p.clean) {
            Ok(()) => p.committed = true,
            Err(e) => {
                log::warn("data", format_args!("{id:?}：新 last serving 本地提交 v{} 失败：{e}（稍后重试）", p.version))
            }
        }
    }
    node.chunks.set_pending(id, p);
}
