//! 确定性 raft 模拟的多节点部分（design.md 9.2，M8）：几个直接驱动的 `Sim`（Driver + 模拟掉电的 `MemCrashBackend`）与它们之间的
//! 内存总线。总线上的消息可以按任意顺序投递（重排）、丢掉、按分区挡住；节点可以随时"掉电重启"（只留下已落盘的字节）。MsgSnapshot
//! 投递之前先做 snap-recv 的事——从快照源（`SnapDesc.source_node`）的 pin 按真实的帧把键值写成快照文件（`snapshot.rs` 的同一套
//! 代码），再交给接收方（Ready 里安装，规则 M5），并像 snap-serve 那样向源报告成败（坑 P11）。
//! 时间全部按 tick 走（没有真实时间）；raft-rs 自己的选举超时随机化用它自己的随机数，所以同一个种子只复现我们这边的调度。
//! 另有 [`SimMembers`]：cluster-mgr 的 `Membership::tick` 在模拟里的对接（`Members` trait 的第二个实现，第一个是 `EmbeddedEngine`）。

use std::cell::RefCell;
use std::collections::BTreeSet;

use crossbeam_channel::{Receiver, bounded};
use raft::eraftpb::{ConfChangeV2, Message, MessageType};
use wmfs_core::base::rand::XorShift64;
use wmfs_core::meta::raft::driver::{ProposeOutcome, RaftInput};
use wmfs_core::meta::raft::membership::Members;
use wmfs_core::meta::raft::snapshot::{self, SnapWriter};
use wmfs_core::meta::schema::keys::{self, T_NODE};
use wmfs_core::meta::schema::records::{KvOp, NODE_ACTIVE, NodeRecV1, decode_v1, encode_v1};
use wmfs_core::proto::decode_body;
use wmfs_core::proto::peer::{SnapDesc, SnapTrailer};

use super::meta::Sim;

/// 一组节点与总线。`nodes[i]` 是节点 i + 1。
pub struct Net {
    pub nodes: Vec<Sim>,
    pub flight: Vec<Message>,
    pub cut: BTreeSet<u64>, // 被分区隔开的节点：进出的消息都丢掉
    pub drop_pct: u64,      // 投递时随机丢掉的百分比
    pub snapshots: u64,     // 成功装进去的快照数（下载 + 投递）
}

impl Net {
    pub fn new(nodes: Vec<Sim>) -> Net {
        Net { nodes, flight: Vec::new(), cut: BTreeSet::new(), drop_pct: 0, snapshots: 0 }
    }

    pub fn node(&mut self, id: u64) -> &mut Sim {
        &mut self.nodes[id as usize - 1]
    }

    /// 每个节点处理一次 Ready，把发出的消息放上总线。
    pub fn ready_all(&mut self) {
        for s in &mut self.nodes {
            s.driver.process_ready();
            self.flight.extend(s.driver.take_outbox());
        }
    }

    pub fn tick(&mut self, id: u64) {
        self.node(id).driver.tick();
    }

    /// 投递总线上的第 i 条（被分区挡住或随机丢掉就只是拿掉它）。
    pub fn deliver(&mut self, i: usize, r: &mut XorShift64) {
        let m = self.flight.swap_remove(i);
        let lost = self.cut.contains(&m.from) || self.cut.contains(&m.to) || r.below(100) < self.drop_pct;
        if m.get_msg_type() == MessageType::MsgSnapshot {
            return self.snapshot(m, lost);
        }
        if !lost && (m.to as usize) <= self.nodes.len() {
            self.node(m.to).driver.handle_input(RaftInput::Msg(m));
        }
    }

    /// MsgSnapshot：先按 snap-recv 的做法从源的 pin 下载成文件，再投递；向源报告成败（坑 P11）。
    fn snapshot(&mut self, m: Message, lost: bool) {
        let (from, to) = (m.from, m.to);
        let ok = !lost && self.download(&m).is_some();
        if ok {
            self.node(to).driver.handle_input(RaftInput::Msg(m));
            self.snapshots += 1;
        }
        self.node(from).driver.handle_input(RaftInput::ReportSnapshot { to, ok });
    }

    fn download(&mut self, m: &Message) -> Option<()> {
        let d: SnapDesc = decode_body(&m.get_snapshot().data).ok()?;
        let pin = self.node(d.source_node as u64).driver.pins().find(d.index, d.term)?;
        let dir = self.node(m.to).snaps.0.clone();
        let mut w = SnapWriter::create(&dir, pin.header().ok()?).ok()?;
        let (pairs, crc32c) = pin.frames(&mut |kv| w.pairs(&kv)).ok()?;
        w.finish(&SnapTrailer { pairs, crc32c }).ok().map(|_| ())
    }

    /// 掉电重启节点 `id`（只留下已落盘的字节；发给它的、还在总线上的消息照样留着）。
    pub fn crash(&mut self, id: u64) {
        let i = id as usize - 1;
        let s = self.nodes.remove(i);
        self.nodes.insert(i, s.crash());
    }

    /// 自认是就绪 leader 的节点（分区时可能有两个：旧的那个提交不了）。
    pub fn ready_leaders(&self) -> Vec<u64> {
        self.nodes.iter().filter(|s| s.driver.is_ready()).map(|s| s.id as u64).collect()
    }

    /// 在节点 `id` 上提议一个成员变更（经输入阶段，坑 P1）。
    pub fn conf_change(&mut self, id: u64, cc: ConfChangeV2) -> Receiver<ProposeOutcome> {
        let (done, rx) = bounded(1);
        self.node(id).driver.handle_input(RaftInput::ConfChange { cc, done });
        rx
    }

    /// 节点 `id` 本地已应用的全部节点记录。
    pub fn node_recs(&self, id: u64) -> Vec<(u32, NodeRecV1)> {
        let s = &self.nodes[id as usize - 1];
        s.kv(T_NODE)
            .into_iter()
            .map(|(k, v)| (u32::from_be_bytes([k[1], k[2], k[3], k[4]]), decode_v1(&v).unwrap()))
            .collect()
    }

    /// 节点 `id` 的 (applied, kv 的哈希)。
    pub fn state(&self, id: u64) -> (u64, String) {
        let (a, _, h) = snapshot::kv_hash(&self.nodes[id as usize - 1].db).unwrap();
        (a.index, h)
    }
}

/// 模拟里用的节点记录（只有 meta_role 有意义，其余留空）。
pub fn node_rec(role: u8) -> NodeRecV1 {
    NodeRecV1 {
        addrs: vec![],
        mgmt_addr: String::new(),
        host_id: String::new(),
        machine_id: String::new(),
        hostname: String::new(),
        meta_role: role,
        features: vec![],
        joined_ms: 0,
        state: NODE_ACTIVE,
    }
}

/// cluster-mgr 的 `Membership::tick` 在模拟里的对接（`Members` 的第二个实现）：在就绪的 leader 节点上提议 conf change、让位、改节点
/// 记录。改节点记录与执行器一样单飞：这个节点有批次在途时不改（下一轮 tick 按当时的状态再算）。
pub struct SimMembers<'a> {
    pub net: RefCell<&'a mut Net>,
    pub leader: u64,
    pub busy: bool,
    pub roles: RefCell<Option<Receiver<ProposeOutcome>>>, // set_roles 提议的批次（调用方交给它的账本跟踪）
    pub leave_joint: RefCell<u64>,                        // 提议了几次空的 ConfChangeV2（leave-joint 兜底）
}

impl<'a> SimMembers<'a> {
    /// 在就绪的 leader `leader` 上；`busy` = 这个节点有批次在途（改节点记录要等它）。
    pub fn new(net: &'a mut Net, leader: u64, busy: bool) -> SimMembers<'a> {
        SimMembers { net: RefCell::new(net), leader, busy, roles: RefCell::new(None), leave_joint: RefCell::new(0) }
    }
}

impl Members for SimMembers<'_> {
    fn node_id(&self) -> u32 {
        self.leader as u32
    }
    fn nodes(&self) -> Vec<(u32, NodeRecV1)> {
        self.net.borrow().node_recs(self.leader)
    }
    fn propose_conf_change(&self, cc: ConfChangeV2) -> Receiver<ProposeOutcome> {
        if cc == ConfChangeV2::default() {
            *self.leave_joint.borrow_mut() += 1;
        }
        self.net.borrow_mut().conf_change(self.leader, cc)
    }
    fn transfer_leader(&self, to: u32) {
        self.net.borrow_mut().node(self.leader).driver.handle_input(RaftInput::TransferLeader(to as u64));
    }
    fn set_roles(&self, roles: &[(u32, u8)]) {
        if self.busy {
            return;
        }
        let ops = roles.iter().map(|(id, role)| KvOp::Put { key: keys::node(*id), value: encode_v1(&node_rec(*role)) });
        *self.roles.borrow_mut() = Some(self.net.borrow_mut().node(self.leader).propose(ops.collect(), None));
    }
}
