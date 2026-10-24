//! 确定性 raft 模拟的多节点部分（design.md 9.2，roadmap M8 的验收："1000 个种子，内存后端"）：3 个 `RawNode<RedbStorage>` + Driver，
//! 消息走内存总线（`harness::sim_net`），存储是模拟掉电的 `MemCrashBackend`。每个种子随机地：投递（任意顺序 = 重排）、丢消息、
//! 分区、tick、提议批次、线性一致读、换 leader（TransferLeader）、在任意一次 fsync 处掉电重启、日志压缩（fail point
//! `raft.compact.small` 把阈值调成 8，落后与重启的节点经常要走快照）；一半的种子从 1 个 voter + 2 个空节点开始，由 cluster-mgr 的
//! 同一个 `Membership::tick`（经 `harness::sim_net::SimMembers` 对接，时间是逻辑时间：每一步 100 ms）加 learner、快照追上、一次 joint 变更提升为
//! 3 voter，joint 超过 5 s 没人退出就兜底（坑 P13）。每个写批次还删掉上一批写下的一个临时键（日志里有删除：装快照时没先删掉旧 kv
//! 的节点会留下多余的键，① 抓得到）。每一步检查 3.4.1 的安全规则：
//! ① 前缀一致：任何节点在同一个 applied 位置的状态（kv 的哈希）都相同；② 规则 M1：落盘的 applied ≤ hs.commit；
//! ③ 读闸门线性一致：读放行时本节点的状态包含发起读之前已确认的全部写；
//! 结束时（愈合、全部投递、等追平、退出 joint）：④ 各节点状态相同；⑤ 已确认的写一个不少；⑥ 作废（`Applied{ok:false}`）与没进日志
//! （`Dropped`）的写一个也没有；⑦ 读到过的写都还在（读不到会消失的写）；⑧ 还能选出 leader、再写一个（活性）。
//! 失败时消息里有种子与步数。种子数：普通 `cargo test` 跑 `WMFS_SIM_SEEDS` 或 100 个，`scripts/accept.sh M8` 跑 1000 个。
//! raft-rs 的选举超时随机化用它自己的随机数，所以同一个种子只复现我们这边的调度（失败时把种子与步数打印出来，便于缩小范围）。

use std::collections::BTreeMap;

use crossbeam_channel::{Receiver, TryRecvError};
use wmfs_core::api::failpoint;
use wmfs_core::base::ErrCode;
use wmfs_core::base::rand::XorShift64;
use wmfs_core::meta::raft::driver::{ProposeOutcome, RaftInput};
use wmfs_core::meta::raft::membership::Membership;
use wmfs_core::meta::schema::keys::{self, T_DEBUG};
use wmfs_core::meta::schema::records::{KvOp, META_LEARNER, encode_v1};
use wmfs_testkit::harness::meta::{Sim, bootstrap, dkey, empty};
use wmfs_testkit::harness::sim_net::{Net, SimMembers, node_rec};

const STEPS: u32 = 400;

/// 一个在途的批次：`kv` = 这个写的唯一键值（成员模式下改节点记录的批次没有）。
struct Write {
    rx: Receiver<ProposeOutcome>,
    node: u64,
    kv: Option<(Vec<u8>, Vec<u8>)>,
}

struct Read {
    rx: Receiver<Result<(), ErrCode>>,
    node: u64,
    must: usize, // 发起读时已确认的写的个数（confirmed 的前缀）
}

#[derive(Default)]
struct Book {
    confirmed: Vec<(Vec<u8>, Vec<u8>)>,
    absent: Vec<Vec<u8>>, // 作废或没进日志的写：最后一定不在
    seen: BTreeMap<Vec<u8>, Vec<u8>>,
    states: BTreeMap<u64, String>,
    writes: Vec<Write>,
    reads: Vec<Read>,
    next: u64,
}

struct Run {
    net: Net,
    r: XorShift64,
    b: Book,
    members: Vec<Membership>, // 每个节点的 cluster-mgr（节点掉电重启时从头来）
    now_ms: u64,              // 逻辑时间：每一步 100 ms
    leave_joint: u64,         // leave-joint 兜底提议过几次（统计）
    at: String,
}

impl Run {
    fn new(seed: u64) -> Run {
        let r = XorShift64::new(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
        let membership = seed.is_multiple_of(2);
        let nodes = (1..=3u32)
            .map(|id| {
                let b = if !membership {
                    bootstrap(&[1, 2, 3])
                } else if id == 1 {
                    bootstrap(&[1])
                } else {
                    empty()
                };
                Sim::start(b, id)
            })
            .collect();
        let members = (0..3).map(|_| Membership::default()).collect();
        Run { net: Net::new(nodes), r, b: Book::default(), members, now_ms: 0, leave_joint: 0, at: String::new() }
    }

    /// 这个节点上有没有在途的批次：和执行器一样单飞（同一时刻 raft 里最多一个批次，3.4.1.4），否则后一个会因 expect_version 作废。
    fn busy(&self, node: u64) -> bool {
        self.b.writes.iter().any(|w| w.node == node)
    }

    /// 提议一个写：唯一的 key `w<n>`（只写一次，从不删除），值 = 序号；外加一个临时键 `x<n>`，并删掉上一批的 `x<n-1>`（见文件头：
    /// 日志里要有删除）。成员模式下第一个写同时登记节点 2、3（learner 加入）。
    fn write(&mut self, leader: u64) {
        if self.busy(leader) {
            return;
        }
        self.b.next += 1;
        let (key, value) = (dkey(&format!("w{}", self.b.next)), self.b.next.to_be_bytes().to_vec());
        let mut ops = vec![KvOp::Put { key: key.clone(), value: value.clone() }];
        ops.push(KvOp::Put { key: dkey(&format!("x{}", self.b.next)), value: vec![1] });
        ops.push(KvOp::Delete { key: dkey(&format!("x{}", self.b.next - 1)) });
        if self.net.node_recs(leader).len() < 3 {
            for id in 2..=3u32 {
                ops.push(KvOp::Put { key: keys::node(id), value: encode_v1(&node_rec(META_LEARNER)) });
            }
        }
        let rx = self.net.node(leader).propose(ops, None);
        self.b.writes.push(Write { rx, node: leader, kv: Some((key, value)) });
    }

    fn read(&mut self, leader: u64) {
        let (done, rx) = crossbeam_channel::bounded(1);
        self.net.node(leader).driver.handle_input(RaftInput::ReadIndex { done });
        self.b.reads.push(Read { rx, node: leader, must: self.b.confirmed.len() });
    }

    /// 在节点 `id` 的某一次 fsync 处掉电：这一步的 Ready 里做的事都不算数（发出的消息丢掉、给出的确认作废），然后重启。
    fn power_cut(&mut self, id: u64) {
        let k = self.r.below(3);
        self.net.node(id).backend.freeze_after(k);
        self.net.node(id).driver.process_ready();
        self.net.node(id).driver.take_outbox();
        self.b.writes.retain(|w| w.node != id); // 这个节点上在途的写：这一步给出的确认作废，结果不确定
        self.b.reads.retain(|r| r.node != id);
        self.crash(id);
    }

    /// 节点 `id` 掉电重启（它的 cluster-mgr 也从头来）。
    fn crash(&mut self, id: u64) {
        self.net.crash(id);
        self.members[id as usize - 1] = Membership::default();
    }

    /// 成员变更：就绪的 leader 上跑一次 cluster-mgr 的 `Membership::tick`（3.4.1.9、坑 P13），经 [`SimMembers`] 对接。
    fn membership(&mut self, leader: u64) {
        let st = self.net.node(leader).driver.published().status();
        let busy = self.busy(leader);
        let m = SimMembers::new(&mut self.net, leader, busy);
        self.members[leader as usize - 1].tick(&m, &st, self.now_ms);
        let SimMembers { roles, leave_joint, .. } = m;
        self.leave_joint += leave_joint.into_inner();
        if let Some(rx) = roles.into_inner() {
            self.b.writes.push(Write { rx, node: leader, kv: None });
        }
    }

    /// 收结果、做每一步的检查（①②③）。
    fn settle(&mut self) {
        let mut still = Vec::new();
        for w in std::mem::take(&mut self.b.writes) {
            match w.rx.try_recv() {
                Ok(ProposeOutcome::Applied { ok: true }) => self.b.confirmed.extend(w.kv),
                Ok(ProposeOutcome::Applied { ok: false } | ProposeOutcome::Dropped) => {
                    self.b.absent.extend(w.kv.map(|kv| kv.0))
                }
                Ok(ProposeOutcome::Lost) | Err(TryRecvError::Disconnected) => {} // 结果不确定（节点重启了也是）
                Err(TryRecvError::Empty) => still.push(w),
            }
        }
        self.b.writes = still;
        let mut still = Vec::new();
        for r in std::mem::take(&mut self.b.reads) {
            match r.rx.try_recv() {
                Ok(Ok(())) => {
                    let kv = self.net.node(r.node).kv(T_DEBUG);
                    for (k, v) in &self.b.confirmed[..r.must] {
                        assert_eq!(kv.get(k), Some(v), "{}：读闸门放行后读不到之前已确认的写（③）", self.at);
                    }
                    self.b.seen.extend(kv.into_iter().filter(|(k, _)| k.get(1) == Some(&b'w'))); // 临时键 x<n> 不算
                }
                Ok(Err(_)) | Err(TryRecvError::Disconnected) => {}
                Err(TryRecvError::Empty) => still.push(r),
            }
        }
        self.b.reads = still;
        for id in 1..=3 {
            let (applied, h) = self.net.state(id);
            let want = self.b.states.entry(applied).or_insert_with(|| h.clone());
            assert_eq!(*want, h, "{}：node{id} 在 applied {applied} 的状态与别的节点不同（①）", self.at);
            let (hs, a, _) = self.net.node(id).raft_state();
            assert!(
                a.index <= hs.commit,
                "{}：node{id} 规则 M1 被破坏：applied {} > hs.commit {}",
                self.at,
                a.index,
                hs.commit
            );
        }
    }

    fn step(&mut self) {
        self.now_ms += 100;
        let leaders = self.net.ready_leaders();
        let leader = leaders.get(self.r.below(leaders.len().max(1) as u64) as usize).copied();
        match self.r.below(100) {
            0..=44 => {
                for _ in 0..1 + self.r.below(10) {
                    if self.net.flight.is_empty() {
                        break;
                    }
                    let i = self.r.below(self.net.flight.len() as u64) as usize;
                    self.net.deliver(i, &mut self.r);
                }
            }
            45..=57 => {
                for _ in 0..1 + self.r.below(3) {
                    self.net.tick(1 + self.r.below(3));
                }
            }
            58..=77 => {
                if let Some(l) = leader {
                    self.write(l);
                }
            }
            78..=83 => {
                if let Some(l) = leader {
                    self.read(l);
                }
            }
            84..=86 => {
                let id = 1 + self.r.below(3);
                self.crash(id);
            }
            87 => {
                let id = 1 + self.r.below(3);
                self.power_cut(id);
            }
            88..=91 => {
                if self.net.cut.is_empty() {
                    self.net.cut.insert(1 + self.r.below(3));
                } else {
                    self.net.cut.clear();
                }
            }
            92..=93 => self.net.drop_pct = [0, 10, 30][self.r.below(3) as usize],
            94..=95 => {
                if let Some(l) = leader {
                    let to = 1 + (l + self.r.below(2)) % 3;
                    self.net.node(l).driver.handle_input(RaftInput::TransferLeader(to));
                }
            }
            _ => {
                if let Some(l) = leader {
                    self.membership(l);
                }
            }
        }
        self.net.ready_all();
        self.settle();
    }

    fn deliver_all(&mut self) {
        while !self.net.flight.is_empty() {
            let i = self.r.below(self.net.flight.len() as u64) as usize;
            self.net.deliver(i, &mut self.r);
        }
    }

    /// 愈合、全部投递、等追平、退出 joint（④⑧）：返回就绪的 leader。
    fn quiesce(&mut self) -> u64 {
        self.net.cut.clear();
        self.net.drop_pct = 0;
        for round in 0..4000 {
            self.now_ms += 100;
            self.deliver_all();
            if round % 3 == 0 {
                (1..=3).for_each(|id| self.net.tick(id));
            }
            if let Some(&l) = self.net.ready_leaders().first() {
                self.membership(l);
                let st = self.net.node(l).driver.published().status();
                let states: Vec<(u64, String)> = (1..=3).map(|id| self.net.state(id)).collect();
                if st.voters.len() == 3
                    && st.voters_outgoing.is_empty()
                    && states.iter().all(|s| *s == states[0] && s.0 == st.commit)
                    && self.b.writes.is_empty()
                {
                    return l;
                }
            }
            self.net.ready_all();
            self.settle();
        }
        panic!("{}：4000 轮之后还没有追平（活性 ⑧）", self.at);
    }
}

/// 一个种子：返回 (装上的快照数, 确认的写数, leave-joint 兜底的次数)。
fn run_seed(seed: u64) -> (u64, u64, u64) {
    let mut run = Run::new(seed);
    for step in 0..STEPS {
        run.at = format!("种子 {seed} 第 {step} 步");
        run.step();
    }
    run.at = format!("种子 {seed} 结束时");
    let l = run.quiesce();
    let kv = run.net.node(l).kv(T_DEBUG);
    for (k, v) in &run.b.confirmed {
        assert_eq!(kv.get(k), Some(v), "{}：已确认的写丢了（⑤）", run.at);
    }
    for k in &run.b.absent {
        assert!(!kv.contains_key(k), "{}：作废或没进日志的写出现了（⑥）", run.at);
    }
    for (k, v) in &run.b.seen {
        assert_eq!(kv.get(k), Some(v), "{}：读到过的写消失了（⑦）", run.at);
    }
    let before = run.b.confirmed.len();
    for i in 0..400 {
        if let Some(&l) = run.net.ready_leaders().first().filter(|_| run.b.writes.is_empty()) {
            run.write(l); // 每次都找当前的 leader；有批次在途时不提（单飞）
        }
        run.net.ready_all();
        run.deliver_all();
        if i % 3 == 0 {
            (1..=3).for_each(|id| run.net.tick(id));
        }
        run.settle();
        if run.b.confirmed.len() > before {
            break;
        }
    }
    let st = run.net.node(l).driver.published(); // quiesce 时的 leader（之后可能换过）
    assert_eq!(
        run.b.confirmed.len(),
        before + 1,
        "{}：最后一个写没有确认（活性 ⑧；leader {l} 的角色 {:?}，状态 {:?}，absent {}）",
        run.at,
        st.role(),
        st.status(),
        run.b.absent.len()
    );
    (run.net.snapshots, run.b.confirmed.len() as u64, run.leave_joint)
}

#[test]
fn three_nodes_survive_crashes_partitions_reordering_and_membership_changes() {
    failpoint::set("raft.compact.small", "return(8)").unwrap(); // 日志压缩得很勤：落后与重启的节点经常要走快照
    let seeds: u64 = std::env::var("WMFS_SIM_SEEDS").ok().and_then(|v| v.parse().ok()).unwrap_or(100);
    let t0 = std::time::Instant::now();
    let (mut snaps, mut writes, mut leave) = (0, 0, 0);
    for (s, w, l) in (1..=seeds).map(run_seed) {
        (snaps, writes, leave) = (snaps + s, writes + w, leave + l);
    }
    let secs = t0.elapsed().as_secs_f64();
    println!(
        "{seeds} 个种子 × {STEPS} 步，用时 {secs:.1} s，确认了 {writes} 个写，装了 {snaps} 个快照，leave-joint 兜底 {leave} 次"
    );
    assert!(snaps > seeds, "除了新节点加入，落后与重启的节点也应走到过快照安装");
}
