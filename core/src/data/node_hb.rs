//! target 心跳的最小版与本节点的集群图视图（design.md 1.2 的 `hb` 线程、3.3.6 的 NodeHeartbeat、3.4.2.10、6.3）。
//!
//! `hb` 线程每 1 s（刚启动、还没拉到过集群图时每 100 ms）向元数据 leader 发一次 `NodeOp::Heartbeat`（Server 角色，key 0）：本进程启动时随机生成的
//! `boot_id`、每个 target 的容量与已用（`statfs`，经 3.7.4 的包装）、它所在的物理整盘 `phys_devs`（`base::devinfo`，建 Target 时
//! 解析一次）。leader 据此更新 TargetRecV1
//! （boot_id、phys_devs、read_only 变了才写，同批 `map_epoch += 1`），回复 `HeartbeatOk{map_epoch}`；回复里的 map_epoch 比本地的新
//! 就拉一张新集群图（`GetClusterMap`）。数据服务发现请求带的链比本地视图新、或本地还不认识这条链时调用 [`MapHolder::poke`]，
//! hb 线程立刻拉一次（3.4.2.4：通知 hb 线程立即拉集群图）。
//!
//! 实现注记（M5）：只是"最小版"——target 租约、自我隔离（I5 的租约与 boot_id 核对）、转发失败上报在 M9；leader 还不按心跳把
//! target 判为离线。实现注记（M8）：每拿到一张集群图先交给 `on_map`（服务端装配时给的：比对集群插件清单，不一致 → 退出码 3，3.7.2）。
//! 往哪发（[`Finder`]，M8 审查后）：raft 知道 leader 就发给它；不知道——只做数据的节点不在 raft 配置里、收不到 raft 消息，raft 永远
//! 不知道 leader（3.4.3 第 3 步"用 Joined 里的集群图找到 leader"）——就发给上一次成功的地址，或者上一次 `NOT_LEADER` 回复里
//! `leader_hint` 指的地址（与 MetaClient 的做法相同）；都没有就在认识的节点（本地已应用的 NodeRecV1，join 时预写的也算）里轮流试。

use std::sync::{Arc, Mutex};
use std::time::Duration;

use crossbeam_channel::{Receiver, Sender, bounded};

use super::DataNode;
use crate::base::{ErrCode, clock, lock, log};
use crate::net::client::{Endpoint, Family, RpcClient};
use crate::proto::frame::{FrameHeader, FrameKind, MSG_NODE_CALL, OutFrame};
use crate::proto::peer::{ClusterMap, NodeHeartbeat, NodeOp, NodeReply, TargetReport};
use crate::proto::{decode_body, encode_body};

/// 心跳周期（刚启动、还没拉到过集群图时短一些：数据节点拿到集群图之前对所有请求回 `NOT_READY`）与单次 RPC 超时。
const PERIOD: Duration = Duration::from_secs(1);
const START_PERIOD: Duration = Duration::from_millis(100);
const RPC_TIMEOUT: Duration = Duration::from_secs(5);

/// 本节点的集群图视图（数据服务按它核对链版本、找本节点是不是 head；hb 线程更新它）。
pub struct MapHolder {
    cur: Mutex<Option<Arc<ClusterMap>>>,
    poke_tx: Sender<()>,
    poke_rx: Receiver<()>,
}

impl Default for MapHolder {
    fn default() -> MapHolder {
        MapHolder::new()
    }
}

impl MapHolder {
    pub fn new() -> MapHolder {
        let (poke_tx, poke_rx) = bounded(1);
        MapHolder { cur: Mutex::new(None), poke_tx, poke_rx }
    }

    /// 当前的集群图（还没拉到过 → None）。
    pub fn get(&self) -> Option<Arc<ClusterMap>> {
        lock(&self.cur).clone()
    }

    /// 换上一张集群图（比手上的旧就不要）。
    pub fn set(&self, m: Arc<ClusterMap>) {
        let mut cur = lock(&self.cur);
        if cur.as_ref().is_none_or(|c| c.map_epoch <= m.map_epoch) {
            *cur = Some(m);
        }
    }

    pub fn epoch(&self) -> u64 {
        lock(&self.cur).as_ref().map_or(0, |m| m.map_epoch)
    }

    /// 请 hb 线程马上拉一次集群图（不阻塞；已经请过了就算了）。
    pub fn poke(&self) {
        let _ = self.poke_tx.try_send(());
    }
}

/// 心跳线程要的东西（target、本节点的集群图视图都在 `data` 里）。
pub struct HbDeps {
    pub node: u32,
    pub boot_id: u64,
    pub host_id: String,
    pub machine_id: String,
    pub data: Arc<DataNode>,
    pub rpc: Arc<RpcClient>,
    pub leader: Arc<dyn Fn() -> Option<Endpoint> + Send + Sync>, // raft 知道的元数据 leader（不知道 → None）
    pub peers: Arc<dyn Fn() -> Vec<Endpoint> + Send + Sync>, // 认识的其他节点（见文件头：raft 不知道 leader 时轮流试）
    pub on_map: Arc<dyn Fn(&ClusterMap) + Send + Sync>, // 每拿到一张集群图（M8：比对插件清单，不一致就退出，3.7.2）
}

/// 起 `hb` 线程（随进程一直运行）。第一次心跳不等满 1 s，拉到集群图之前每 100 ms 试一次：数据节点越早拿到集群图，刚启动时回
/// `NOT_READY` 的窗口越短（这段时间里 leader 多半还没选出来，失败只记 INFO）。
pub fn start(hb: HbDeps) -> Result<(), String> {
    std::thread::Builder::new()
        .name("hb".into())
        .spawn(move || {
            let (mut last_err, mut poked, mut finder) = (None, false, Finder::default());
            loop {
                let r = beat(&hb, &mut finder, poked);
                let starting = hb.data.map.epoch() == 0; // 还没拉到过集群图：刚启动
                if r.as_ref().err() != last_err.as_ref() {
                    match &r {
                        Ok(()) => log::info("hb", "target 心跳正常"),
                        Err(e) if starting => {
                            log::info("hb", format_args!("target 心跳还没成功：{e}（刚启动，稍后重试）"))
                        }
                        Err(e) => log::warn("hb", format_args!("target 心跳失败：{e}（每秒重试）")),
                    }
                }
                last_err = r.err();
                let wait = if starting { START_PERIOD } else { PERIOD };
                poked = hb.data.map.poke_rx.recv_timeout(wait).is_ok();
            }
        })
        .map(|_| ())
        .map_err(|e| format!("起 hb 线程失败：{e}"))
}

/// 往哪发心跳（见文件头）：上一次成功的、或 leader_hint 指的地址，与轮流试到第几个认识的节点。
#[derive(Default)]
struct Finder {
    last: Option<Endpoint>,
    next: usize,
}

impl Finder {
    fn pick(&mut self, hb: &HbDeps) -> Option<Endpoint> {
        if let Some(ep) = (hb.leader)().or_else(|| self.last.clone()) {
            return Some(ep);
        }
        let peers = (hb.peers)();
        self.next = (self.next + 1) % peers.len().max(1);
        peers.get(self.next).cloned()
    }
}

/// 一次 NodeCall；回 ERROR 帧时把 `leader_hint` 交给 `hint`（`NOT_LEADER` 时 core 自动填：leader 在 control_scheme 上的地址）。
fn call(rpc: &RpcClient, ep: &Endpoint, op: &NodeOp, hint: &mut Option<String>) -> Result<NodeReply, ErrCode> {
    let hdr = FrameHeader::new(FrameKind::Request, MSG_NODE_CALL, 0, 0);
    let f = OutFrame { hdr, body: encode_body(op), bulk: vec![], recv_into: None };
    let f = rpc.call(ep, Family::Control, f, RPC_TIMEOUT)?;
    if let Some(e) = f.error() {
        *hint = e.leader_hint;
        return Err(ErrCode(e.code));
    }
    decode_body(&f.body)
}

/// 发一次心跳（地址见 [`Finder`]）：成功就记住这个地址；失败时有 leader_hint 下次就去那里，没有就下次换一个节点。
fn beat(hb: &HbDeps, f: &mut Finder, poked: bool) -> Result<(), ErrCode> {
    let ep = f.pick(hb).ok_or(ErrCode::NOT_LEADER)?;
    let mut hint = None;
    let r = beat_to(hb, &ep, poked, &mut hint);
    let scheme = &hb.rpc.net().control_scheme;
    f.last = if r.is_ok() { Some(ep) } else { hint.map(|a| Endpoint { node: 0, addrs: vec![(scheme.clone(), a)] }) };
    r
}

/// 向 `ep` 发一次心跳；leader 的 map_epoch 更新（或被 poke 了）就拉新集群图。
fn beat_to(hb: &HbDeps, ep: &Endpoint, poked: bool, hint: &mut Option<String>) -> Result<(), ErrCode> {
    let mut targets = Vec::new();
    for t in hb.data.targets.values() {
        let st = hb.data.store(|| t.stat())?;
        let (target, phys_devs, read_only) = (t.id, t.phys_devs.clone(), t.read_only);
        targets.push(TargetReport { target, used: st.used, capacity: st.capacity, phys_devs, read_only });
    }
    let map = &hb.data.map;
    let seen = map.epoch();
    let msg = NodeHeartbeat {
        node: hb.node,
        boot_id: hb.boot_id,
        host_id: hb.host_id.clone(),
        machine_id: hb.machine_id.clone(),
        map_epoch_seen: seen,
        targets,
        forward_failures: vec![],
        features: vec![],
        wall_ms: clock::wall_ms(),
        qos_deficit: vec![],
    };
    let epoch = match call(&hb.rpc, ep, &NodeOp::Heartbeat(msg), hint)? {
        NodeReply::HeartbeatOk { map_epoch, .. } => map_epoch,
        _ => return Err(ErrCode::INTERNAL),
    };
    if epoch > seen || poked || seen == 0 {
        let since_epoch = if poked { 0 } else { seen };
        if let NodeReply::Map(m) = call(&hb.rpc, ep, &NodeOp::GetClusterMap { since_epoch }, hint)? {
            (hb.on_map)(&m);
            map.set(Arc::new(*m));
        }
    }
    Ok(())
}
