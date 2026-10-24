//! 元数据副本之间的网络（design.md 1.2 的 `raft-send-<peer>`、`snap-recv`、`snap-serve-<i>` 线程，3.3.3、3.4.1.7）：[`RaftNet`]。
//!
//! - **发**：raft 线程每一轮把 outbox 交给 [`RaftNet::send`]，按对端放进各自的 raft-send 队列（有界 1024，满了就丢、
//!   `report_unreachable`）。raft-send 线程把 protobuf 编码的 `eraftpb::Message` 写进内存池的 Buf，作为 **bulk** 放进一个 OneWay
//!   `RaftMsg` 帧（body 为空；一个 MsgAppend 可能超过 1 MiB 的 body 上限，3.4.1.4、附录 C 的 C-13），经到该对端的连接发出
//!   （`RpcClient::send_oneway`，永远走 control_scheme，断了就重连）；发不出去 → 丢掉这一条并 `report_unreachable`（raft 会重发），
//!   并退避：之后 100 ms（每次失败翻倍，最多 1 s）里给它的消息直接丢掉、不再建连（对端宕机时不每条消息都建一次连接、写一行日志；
//!   MsgSnapshot 不退避，它的结局由看门狗兜底）。
//!   对端地址取本节点已应用的 NodeRecV1（新节点在快照之前用 join 预写的，`boot.rs`）；查不到——这个节点在本地已应用的状态之后才加入，
//!   例如落后到要走快照的 follower 碰上新加入的 leader——就问认识的节点要集群图（[`RaftNet::discover`]，实现注记（M8 审查））。
//! - **收**：连接读线程收到 RaftMsg → [`RaftNet::deliver`]：MsgSnapshot **先不 step**，交给 snap-recv 线程下载（正在下载就记下最新的
//!   一条、那次结束后接着下载：leader 报告失败后马上重发，丢了要等 60 s 的看门狗，M9 验证），其余直接投给 raft 线程（队列满就丢）。
//! - **snap-recv**：向 `SnapDesc.source_node` 发 `SnapshotFetch`（流，相邻两帧最多等 10 min），按 `snapshot.rs` 的格式写文件、校验、
//!   fsync，**之后**才把原 MsgSnapshot 交给 raft 线程（Ready 里安装，规则 M5）。
//! - **snap-serve**：`SnapshotFetch` 的服务（只给 Server 角色，3.3.3 的角色表）：找到对应的 pin，起一个 `snap-serve` 线程用 StreamChunk
//!   帧流式发出（首帧 SnapHeader，中间帧 ≤ 1 MiB 的键值，末帧 SnapTrailer；发之前等发送队列腾出地方），结束后报告成败（坑 P11）；
//!   同时最多 2 个。fail point `snapshot.fetch.midway`：发完第一个中间帧之后以注入的码失败（follower 丢掉不完整的文件，leader 报告失败、
//!   之后重发）。
//! - **看门狗**（坑 P11）：raft-send 线程每秒看一次，发给本对端的快照 60 s 还没下载完 → `ReportSnapshot{ok: false}`。

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, bounded};
use protobuf::Message as _;
use raft::eraftpb::{Message, MessageType};
use redb::Database;
use wmfs_api::failpoint::fail_point;
use wmfs_api::metrics::{self, Counter};

use super::snapshot::{self, Pairs, PinnedSnap, Pins, SnapWriter};
use super::storage::KV;
use super::types::RaftInput;
use crate::base::{ErrCode, clock, lock, log};
use crate::buf::{Lane, MemPool, POOL_WAIT};
use crate::meta::schema::keys;
use crate::meta::schema::records::{NodeRecV1, decode_v1};
use crate::net::client::{Endpoint, Family, RpcClient};
use crate::net::server::{Responder, Service, StreamTx};
use crate::proto::frame::{
    FLAG_LAST_CHUNK, FrameHeader, FrameKind, InFrame, MSG_NODE_CALL, MSG_RAFT, MSG_SNAPSHOT_FETCH, OutFrame,
};
use crate::proto::peer::{ClusterMap, NodeOp, NodeReply, SnapDesc, SnapHeader, SnapTrailer, SnapshotFetch};
use crate::proto::{decode_body, encode_body};

/// raft-send 的队列长度；单次发送（含建连）最多等多久；快照流相邻两帧之间最多等多久；snap-serve 同时最多几个。
const QUEUE: usize = 1024;
const SEND_TIMEOUT: Duration = Duration::from_secs(5);
const FETCH_TIMEOUT: Duration = Duration::from_secs(600);
const MAX_SERVE: usize = 2;
/// 发送失败之后的退避（毫秒）：从 100 起翻倍，最多 1000；问别人要不认识的节点的地址，最多每 5 s 一次。
const BACKOFF_MIN_MS: u64 = 100;
const BACKOFF_MAX_MS: u64 = 1000;
const DISCOVER_EVERY_MS: u64 = 5000;

/// 一个节点的元数据网络（见文件头）。
pub struct RaftNet {
    // 指向自己的弱引用：`send`/`deliver`/`Service::call` 只拿到 `&self`，起 raft-send、snap-recv、snap-serve 线程时要交给线程一个能
    // 带走的引用（线程里 upgrade，RaftNet 被丢弃之后线程自然退出）
    this: Weak<RaftNet>,
    rpc: Arc<RpcClient>,
    pool: MemPool,
    db: Arc<Database>,
    raft_tx: Sender<RaftInput>,
    pins: Arc<Pins>,
    snap_dir: PathBuf,
    queues: Mutex<HashMap<u64, Sender<Message>>>,
    found: Mutex<(u64, HashMap<u64, Endpoint>)>, // 问别人找到的地址，与上一次去问的时刻（discover）
    recv: Mutex<RecvQueue>,
    serving: AtomicUsize,
    sent: Arc<Counter>,
    dropped: Arc<Counter>,
    snaps_sent: Arc<Counter>,
}

impl RaftNet {
    pub fn new(
        rpc: Arc<RpcClient>,
        db: Arc<Database>,
        raft_tx: Sender<RaftInput>,
        pins: Arc<Pins>,
        snap_dir: PathBuf,
    ) -> Arc<RaftNet> {
        let c = |n, h| metrics::counter(n, h, &[]);
        Arc::new_cyclic(|this| RaftNet {
            this: this.clone(),
            pool: rpc.pool().clone(),
            rpc,
            db,
            raft_tx,
            pins,
            snap_dir,
            queues: Mutex::new(HashMap::new()),
            found: Mutex::new((0, HashMap::new())),
            recv: Mutex::new(RecvQueue::default()),
            serving: AtomicUsize::new(0),
            sent: c("wmfs_raft_msg_sent_total", "发给其他元数据副本的 raft 消息数"),
            dropped: c("wmfs_raft_msg_dropped_total", "没能发出的 raft 消息数"),
            snaps_sent: c("wmfs_raft_snapshot_sent_total", "流式发出完整的元数据快照数（snap-serve）"),
        })
    }

    /// raft 线程：把消息放进各对端的 raft-send 队列（第一次用到某个对端时起它的线程）。
    pub fn send(&self, msgs: Vec<Message>) {
        for m in msgs {
            let to = m.to;
            if self.queue(to).try_send(m).is_err() {
                self.unreachable(to, "raft-send 队列满了");
            }
        }
    }

    fn queue(&self, to: u64) -> Sender<Message> {
        let mut q = lock(&self.queues);
        if let Some(tx) = q.get(&to) {
            return tx.clone();
        }
        let (tx, rx) = bounded(QUEUE);
        let w = self.this.clone();
        match std::thread::Builder::new().name(format!("raft-send-{to}")).spawn(move || sender(w, to, rx)) {
            Ok(_) => {
                q.insert(to, tx.clone());
            }
            // 不登记：这一条随 rx 一起被丢掉（try_send 失败、计入没发出去），下一条消息再试一次起线程
            Err(e) => log::error("raft", format_args!("起 raft-send-{to} 线程失败：{e}")),
        }
        tx
    }

    /// 发不出去：丢掉这一条，告诉 raft（它会重发，并暂时放慢对这个对端的复制）。
    fn unreachable(&self, to: u64, why: impl std::fmt::Display) {
        self.dropped.inc();
        log::debug("raft", format_args!("给节点 {to} 的 raft 消息没发出去：{why}"));
        let _ = self.raft_tx.try_send(RaftInput::ReportUnreachable(to));
    }

    /// 某个节点的地址：本节点已应用的 NodeRecV1；查不到就用问别人找到的（[`RaftNet::discover`]）。都没有 → None。
    fn addr(&self, id: u64) -> Option<Endpoint> {
        let local = || {
            let rtx = self.db.begin_read().ok()?;
            let t = rtx.open_table(KV).ok()?;
            let n: NodeRecV1 = decode_v1(t.get(keys::node(id as u32).as_slice()).ok()??.value()).ok()?;
            Some(Endpoint { node: id as u32, addrs: n.addrs })
        };
        local().or_else(|| self.discover(id))
    }

    /// 本地不认识节点 `id`（见文件头）：依次问本地认识的节点要集群图（`NodeOp::GetClusterMap`，服务端身份，拿到完整的图）；问到的是
    /// follower 就按它回的 `NOT_LEADER` 里的 leader_hint 再问 leader。找到的地址记下来（之后本地应用到它的 NodeRecV1 就以本地的为准）。
    /// 最多每 5 s 去问一次（问不到就等 raft 重发）。
    fn discover(&self, id: u64) -> Option<Endpoint> {
        let now = clock::mono_ms();
        {
            let mut f = lock(&self.found);
            if let Some(ep) = f.1.get(&id) {
                return Some(ep.clone());
            }
            if now < f.0 + DISCOVER_EVERY_MS {
                return None;
            }
            f.0 = now;
        }
        let scheme = &self.rpc.net().control_scheme;
        let known = self.known();
        for ep in &known {
            let (map, hint) = self.ask_map(ep);
            let leader = |h: String| Endpoint { node: 0, addrs: vec![(scheme.clone(), h)] };
            let map = map.or_else(|| self.ask_map(&leader(hint?)).0);
            if let Some((_, n)) = map.and_then(|m| m.nodes.into_iter().find(|(i, _)| *i as u64 == id)) {
                log::info("raft", format_args!("本地还不认识节点 {id}：从集群图里找到它的地址 {:?}", n.addrs));
                let ep = Endpoint { node: id as u32, addrs: n.addrs };
                lock(&self.found).1.insert(id, ep.clone());
                return Some(ep);
            }
        }
        log::warn("raft", format_args!("找不到节点 {id} 的地址（问了 {} 个认识的节点）：5 s 后再问", known.len()));
        None
    }

    /// 本地已应用的 NodeRecV1 里的全部节点。
    fn known(&self) -> Vec<Endpoint> {
        let Ok(rtx) = self.db.begin_read() else { return vec![] };
        let Ok(t) = rtx.open_table(KV) else { return vec![] };
        let (lo, hi) = ([keys::T_NODE], [keys::T_NODE + 1]);
        let Ok(rows) = t.range::<&[u8]>(lo.as_slice()..hi.as_slice()) else { return vec![] };
        let decode = |k: &[u8], v: &[u8]| {
            let n: NodeRecV1 = decode_v1(v).ok()?;
            Some(Endpoint { node: u32::from_be_bytes(k.get(1..5)?.try_into().ok()?), addrs: n.addrs })
        };
        rows.flatten().filter_map(|(k, v)| decode(k.value(), v.value())).collect()
    }

    /// 向 `ep` 要一张集群图：(集群图, 回 `NOT_LEADER` 时的 leader_hint)。
    fn ask_map(&self, ep: &Endpoint) -> (Option<ClusterMap>, Option<String>) {
        let hdr = FrameHeader::new(FrameKind::Request, MSG_NODE_CALL, 0, 0);
        let body = encode_body(&NodeOp::GetClusterMap { since_epoch: 0 });
        match self.rpc.call(ep, Family::Control, OutFrame { hdr, body, bulk: vec![], recv_into: None }, SEND_TIMEOUT) {
            Ok(f) => match (f.error(), decode_body::<NodeReply>(&f.body)) {
                (Some(e), _) => (None, e.leader_hint),
                (None, Ok(NodeReply::Map(m))) => (Some(*m), None),
                _ => (None, None),
            },
            Err(_) => (None, None),
        }
    }

    fn send_one(&self, ep: &mut Option<Endpoint>, to: u64, m: &Message) -> Result<(), ErrCode> {
        if m.get_msg_type() == MessageType::MsgSnapshot {
            let md = m.get_snapshot().get_metadata();
            self.pins.note_sent(to, md.index, md.term);
        }
        let bytes = m.write_to_bytes().map_err(|_| ErrCode::INTERNAL)?;
        let mut b = self.pool.get(bytes.len().max(1), Lane::Normal, POOL_WAIT)?;
        b.set_len(bytes.len());
        b.as_mut_slice().copy_from_slice(&bytes);
        let f = OutFrame {
            hdr: FrameHeader::new(FrameKind::OneWay, MSG_RAFT, 0, 0),
            body: vec![],
            bulk: vec![b.freeze().whole()],
            recv_into: None,
        };
        if ep.is_none() {
            *ep = self.addr(to);
        }
        let e = ep.as_ref().ok_or(ErrCode::NOT_SERVING)?;
        self.rpc.send_oneway(e, f, SEND_TIMEOUT)?;
        self.sent.inc();
        Ok(())
    }

    /// 连接读线程：一个 RaftMsg 到了（见文件头）。
    pub fn deliver(&self, f: InFrame) {
        let Some(b) = f.bulk else { return };
        let m = match Message::parse_from_bytes(b.as_slice()) {
            Ok(m) => m,
            Err(e) => {
                log::warn("raft", format_args!("收到的 RaftMsg 解不开（丢掉）：{e}"));
                return;
            }
        };
        if m.get_msg_type() != MessageType::MsgSnapshot {
            let _ = self.raft_tx.try_send(RaftInput::Msg(m)); // 满了就丢：raft 会重发
            return;
        }
        let Some(m) = lock(&self.recv).arrive(m) else { return }; // 正在下载别的：记下了，那次结束后接着处理
        let w = self.this.clone();
        let spawned = std::thread::Builder::new().name("snap-recv".into()).spawn(move || {
            let Some(net) = w.upgrade() else { return };
            let mut next = Some(m);
            while let Some(m) = next {
                let index = m.get_snapshot().get_metadata().index;
                let r = net.download(&m);
                if let Err(e) = &r {
                    log::warn("raft", format_args!("下载快照失败：{e}（leader 稍后重发）"));
                } else {
                    let _ = net.raft_tx.send(RaftInput::Msg(m)); // 文件已经就位：现在才交给 raft 线程
                }
                next = lock(&net.recv).done(r.is_ok(), index);
            }
        });
        if spawned.is_err() {
            *lock(&self.recv) = RecvQueue::default(); // 这一条与期间记下的都丢掉：看门狗兜底
        }
    }

    /// snap-recv：下载 MsgSnapshot 说的那个快照，写成文件（见文件头）。
    fn download(&self, m: &Message) -> Result<(), ErrCode> {
        snapshot::cleanup(&self.snap_dir, false);
        let d: SnapDesc = decode_body(&m.get_snapshot().data)?;
        let ep = self.addr(d.source_node as u64).ok_or(ErrCode::NOT_SERVING)?;
        log::info("raft", format_args!("从节点 {} 下载快照（index {}，term {}）", d.source_node, d.index, d.term));
        let req = encode_body(&SnapshotFetch { index: d.index, term: d.term });
        let hdr = FrameHeader::new(FrameKind::Request, MSG_SNAPSHOT_FETCH, 0, 0);
        let mut rx = self.rpc.stream(&ep, OutFrame { hdr, body: req, bulk: vec![], recv_into: None }, FETCH_TIMEOUT)?;
        let h: SnapHeader = decode_body(&rx.next()?.ok_or(ErrCode::EIO)?.body)?;
        if (h.index, h.term) != (d.index, d.term) {
            return Err(ErrCode::EIO);
        }
        let mut w = SnapWriter::create(&self.snap_dir, h)?;
        loop {
            let f = rx.next()?.ok_or(ErrCode::EIO)?;
            if f.hdr.flags & FLAG_LAST_CHUNK != 0 {
                let path = w.finish(&decode_body::<SnapTrailer>(&f.body)?)?;
                log::info("raft", format_args!("快照已下载到 {}", path.display()));
                return Ok(());
            }
            w.pairs(&decode_body::<Pairs>(&f.body)?)?;
        }
    }

    /// snap-serve 线程：把 `pin` 流式发出（见文件头）。
    fn serve(&self, pin: &PinnedSnap, tx: &mut StreamTx) -> Result<(), ErrCode> {
        tx.send(encode_body(&pin.header()?), vec![], false)?;
        let mut frames = 0;
        let (pairs, crc32c) = pin.frames(&mut |kv| {
            if frames == 1 {
                fail_point("snapshot.fetch.midway").map_err(ErrCode)?;
            }
            frames += 1;
            tx.wait_room(8)?;
            tx.send(encode_body(&kv), vec![], false)
        })?;
        tx.wait_room(8)?;
        tx.send(encode_body(&SnapTrailer { pairs, crc32c }), vec![], true)
    }
}

/// snap-recv 的排队（见文件头）：同时只下载一个；下载期间到的 MsgSnapshot 只留最新的一条，那次下载结束后接着处理。
#[derive(Default)]
struct RecvQueue {
    busy: bool,
    next: Option<Message>,
}

impl RecvQueue {
    /// 一条 MsgSnapshot 到了：空闲 → 还给调用方（由它起 snap-recv 线程下载）；正在下载 → 记下（替换更早记下的那条）。
    fn arrive(&mut self, m: Message) -> Option<Message> {
        if self.busy {
            self.next = Some(m);
            return None;
        }
        self.busy = true;
        Some(m)
    }

    /// 一次下载结束（`ok`：`index` 处的快照已下载好、交给了 raft 线程）：接着下载的那条；没有、或它不比刚下载好的新 → None（空闲）。
    fn done(&mut self, ok: bool, index: u64) -> Option<Message> {
        let next = self.next.take().filter(|n| !ok || n.get_snapshot().get_metadata().index > index);
        self.busy = next.is_some();
        next
    }
}

/// raft-send 线程：按顺序发，发不出去就退避（见文件头）；每秒也醒一次，看发给这个对端的快照是不是超时了（坑 P11 的看门狗）。
fn sender(w: Weak<RaftNet>, to: u64, rx: Receiver<Message>) {
    let (mut ep, mut backoff, mut down_until) = (None, 0, 0);
    loop {
        let m = match rx.recv_timeout(Duration::from_secs(1)) {
            Ok(m) => Some(m),
            Err(RecvTimeoutError::Timeout) => None,
            Err(RecvTimeoutError::Disconnected) => return,
        };
        let Some(net) = w.upgrade() else { return };
        if net.pins.overdue(to, clock::mono_ms()) {
            log::warn("raft", format_args!("发给节点 {to} 的快照 60 s 还没下载完：报告失败"));
            let _ = net.raft_tx.try_send(RaftInput::ReportSnapshot { to, ok: false });
        }
        let Some(m) = m else { continue };
        if clock::mono_ms() < down_until && m.get_msg_type() != MessageType::MsgSnapshot {
            net.unreachable(to, "刚才连不上，退避中");
            continue;
        }
        match net.send_one(&mut ep, to, &m) {
            Ok(()) => backoff = 0,
            Err(e) => {
                ep = None; // 下一次重新查地址（节点可能换了地址）
                backoff = (backoff * 2).clamp(BACKOFF_MIN_MS, BACKOFF_MAX_MS);
                down_until = clock::mono_ms() + backoff;
                net.unreachable(to, e);
            }
        }
    }
}

/// `SnapshotFetch`（0x0501）的服务：在连接读线程上调用，起 snap-serve 线程之后立即返回。
impl Service for RaftNet {
    fn call(&self, f: InFrame, r: Responder) {
        let Ok(q) = decode_body::<SnapshotFetch>(&f.body) else { return r.error(ErrCode::FEATURE_UNSUPPORTED, 0) };
        let Some(pin) = self.pins.find(q.index, q.term) else { return r.error(ErrCode::ENOENT, 0) };
        if self.serving.fetch_add(1, Ordering::SeqCst) >= MAX_SERVE {
            self.serving.fetch_sub(1, Ordering::SeqCst);
            return r.error(ErrCode::BUSY, 0);
        }
        let w = self.this.clone();
        let spawned = std::thread::Builder::new().name("snap-serve".into()).spawn(move || {
            let Some(net) = w.upgrade() else { return };
            let mut tx = r.into_stream();
            let r = net.serve(&pin, &mut tx);
            match &r {
                Ok(()) => net.snaps_sent.inc(),
                Err(e) => log::warn("raft", format_args!("发快照（index {}）失败：{e}", pin.index)),
            }
            drop(tx); // 没发完的流：自动回 EIO（客户端的流立即结束）
            for to in net.pins.take_sent(pin.index, pin.term) {
                let _ = net.raft_tx.try_send(RaftInput::ReportSnapshot { to, ok: r.is_ok() });
            }
            net.serving.fetch_sub(1, Ordering::SeqCst);
        });
        if spawned.is_err() {
            self.serving.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

#[cfg(test)]
mod tests;
