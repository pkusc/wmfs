//! MetaClient 认识的节点（design.md 3.3.8 的实现注记（M4）与（M9 审查））：当前认为的 leader、`connect` 给的种子与集群图里的节点。
//! 没有 leader_hint 的 `NOT_LEADER`、连不上、超时（被冻住的 leader 既不回复也不断开）时换下一个认识的节点（[`Peers::rotate_from`]）。
//!
//! 实现注记（M15）：只在失败的那个节点**还是**当前选中的节点时才换，换的时候跳过与它同一个地址的项。以前每个失败的调用都把序号加一：
//! 几个调用同时卡在一个冻住的 leader 上（会话心跳、取集群图、一次写……它们等的是同一次建连，3.3.8 的单飞），在同一刻一起超时、各换
//! 一次——3 个节点换 3 次又回到那个冻住的节点，一起再等 10 s，如此循环到它恢复或总期限（120 s）用完，会话也因为心跳到不了新 leader
//! 而被驱逐；跟随 hint 换上的 leader 不在轮换的序号里，只换一次也可能正好换回它；一个过时的超时还会把别的调用刚换上的、能用的 leader
//! 换走。从 `meta_client.rs` 挪出来（每个文件 ≤ 400 行）。
//!
//! 换走的同时记下"放弃了哪个节点、什么时候"（[`Peers::gave_up_on`]）：别的调用要是还在等这个节点的回复，就不再等满自己的 10 s，按超时
//! 处理、马上到新选中的节点重试（同一个 seq，3.5.2）。否则在冻住之前或之后不久发给它的每个调用都要各自等满 10 s——数据写超时之后先拉
//! 集群图，那一次要是恰好在换走之前发给了冻住的 leader，数据面就要多停 10 s（M9 冻住 leader 20 s 的那条验收）。

use crate::net::client::Endpoint;

/// 认识的节点：当前认为的 leader、种子与集群图里的节点（轮流试）。
pub(super) struct Peers {
    pub leader: Endpoint,
    pub known: Vec<Endpoint>,
    pub next: usize,
    pub left: Option<(Endpoint, u64)>, // 最近一次换走的节点与换走的时刻（mono_ms），见文件头
}

impl Peers {
    /// 发给 `failed` 的请求没成（见文件头）：它还是当前选中的节点 → 换成下一个地址与它不同的认识的节点，记下在 `now`（mono_ms）放弃了它；
    /// 别的调用已经换过了 → 不动。
    pub fn rotate_from(&mut self, failed: &Endpoint, now: u64) {
        if !same_node(&self.leader, failed) {
            return;
        }
        for _ in 0..self.known.len() {
            self.next = (self.next + 1) % self.known.len();
            if !same_node(&self.known[self.next], failed) {
                self.leader = self.known[self.next].clone();
                self.left = Some((failed.clone(), now));
                return;
            }
        }
    }

    /// 在 `since`（这个调用选中 `ep` 的时刻，mono_ms）之后，有别的调用放弃了 `ep`、换到了别的节点吗：是就不必再等 `ep` 的回复（见文件头）。
    pub fn gave_up_on(&self, ep: &Endpoint, since: u64) -> bool {
        self.left.as_ref().is_some_and(|(e, at)| *at >= since && same_node(e, ep))
    }
}

/// 是不是同一个节点：有一个相同的 (scheme, 地址)。跟随 leader_hint 换上的端点只有 control_scheme 的地址、节点号是 0，集群图里的有全部。
fn same_node(a: &Endpoint, b: &Endpoint) -> bool {
    a.addrs.iter().any(|x| b.addrs.contains(x))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ep(n: u32) -> Endpoint {
        Endpoint { node: n, addrs: vec![("tcp".into(), format!("127.0.0.1:19{n}00"))] }
    }

    fn peers(leader: Endpoint, next: usize) -> Peers {
        Peers { leader, known: vec![ep(1), ep(2), ep(3)], next, left: None }
    }

    #[test]
    fn calls_timing_out_together_on_a_frozen_leader_move_away_from_it_once() {
        // 跟随 hint 换上的 leader 是节点 2（节点号 0、只有 control_scheme 的地址），轮换的序号还指着节点 1
        let frozen = Endpoint { node: 0, addrs: ep(2).addrs };
        let mut p = peers(frozen.clone(), 0);
        for _ in 0..3 {
            p.rotate_from(&frozen, 1000); // 卡在它上面的 3 个调用同一刻超时
        }
        assert_eq!(p.leader, ep(3), "跳过冻住的节点 2，只换一次（以前换 3 次又回到节点 2）");
        p.rotate_from(&frozen, 1001);
        assert_eq!(p.leader, ep(3), "过时的超时不把能用的 leader 换走");
    }

    #[test]
    fn not_leader_without_a_hint_tries_every_other_node_in_turn() {
        let mut p = peers(ep(1), 0);
        let mut seen = vec![];
        for _ in 0..3 {
            let failed = p.leader.clone();
            p.rotate_from(&failed, 1000);
            seen.push(p.leader.node);
        }
        assert_eq!(seen, [2, 3, 1]);
        // 集群图里的端点还带着别的 scheme 的地址：仍认作同一个节点
        let both =
            Endpoint { node: 1, addrs: vec![("tcp".into(), "127.0.0.1:19100".into()), ("mem".into(), "m1".into())] };
        p.rotate_from(&both, 1000);
        assert_eq!(p.leader, ep(2));
        // 只认识一个节点：只能再试它
        let mut one = Peers { leader: ep(1), known: vec![ep(1)], next: 0, left: None };
        one.rotate_from(&ep(1), 1000);
        assert_eq!(one.leader, ep(1));
        assert!(!one.gave_up_on(&ep(1), 0), "没有可换的节点：不算放弃它");
    }

    #[test]
    fn calls_still_waiting_on_a_node_another_call_gave_up_on_stop_waiting() {
        let frozen = ep(2);
        let mut p = peers(frozen.clone(), 1);
        assert!(!p.gave_up_on(&frozen, 100));
        p.rotate_from(&frozen, 200); // 一个调用等满了 10 s：换走
        assert_eq!(p.leader, ep(3));
        assert!(p.gave_up_on(&frozen, 150), "换走之前发出、还在等它的调用不再等");
        assert!(p.gave_up_on(&Endpoint { node: 0, addrs: frozen.addrs.clone() }, 200), "跟随 hint 发给它的也一样");
        assert!(!p.gave_up_on(&frozen, 250), "换走之后（例如跟着 hint 又换回来）才发的照常等");
        assert!(!p.gave_up_on(&ep(3), 150), "发给别的节点的照常等");
    }
}
