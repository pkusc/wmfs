//! DataCall（0x0200）消息族的服务端（design.md 1.2 的 io 线程、3.3.5、4.5 的"睡眠与唤醒归 core"、3.4.2.3 第 1 步）。
//!
//! 连接读线程上的 [`Service::call`] 只做三件事：解出请求、**不验 MAC 地**看一眼 cap 取 QoS 队列键（租户、qos_id、存储类），
//! 把请求放进 token → 请求 的表并交给 qos 插件排队（`enqueue` 经 3.7.4 的包装：fail point `plugin.<名字>.qos` 触发 = 这个请求
//! 以注入的码失败），然后 `notify_one`。16 个 `io` 线程循环 `qos.dequeue(now)`：`Ok(token)` → 取出请求执行；`Err(Some(t))` →
//! 在 Condvar 上睡到 t；`Err(None)` → 睡 100 ms（插件写错也只会派发不准，不会忙等或丢失唤醒）。
//!
//! 出队之后才校验（3.3.5：在 io 线程上，不是入队时）：MAC、被驱逐的会话（集群图的 evicted → `SESSION_EVICTED`）、链版本与本地视图
//! 一致（不一致 → `STALE_CHAIN`，`extra` = 本地的 chain_ver；请求的更新或本地不认识这条链 → 先 poke hb 线程拉集群图）、本节点
//! 是这条链的 head（写类）或一个 Serving 成员（读），然后才按 cap 的权限位与 chunk 身份放行。
//!
//! 实现注记（M5）：QosSpec 先只带存储类的低优先级标记（mclock 的份额在 M13）；后台请求 = cap 带管理权限位（`proto/cap.rs` 的注记）；
//! 只读的 target（3.2.3）上写、截断与 DeleteEpoch 一律 `EROFS`。
//! 实现注记（M9）：校验多了 I5（租约、boot_id、集群图不旧，不满足 → `FENCED`，`chain.rs`）；读到有未提交版本的 chunk、本成员又不是
//! last serving → `CHUNK_DIRTY{extra = L}`（3.4.2.5）；SyncChunks 与 DeleteEpoch 在 head 上做完本地的部分，再沿链传播（ForwardSync /
//! ForwardDeleteEpoch，`call_cb`，尾确认之后逆向回复，3.4.2.9）；同步源的 ListChunks / FetchChunk（PeerCall）也进这个队列，标为后台。

use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use wmfs_api::metrics::{self, Gauge};

use super::chain::{self, Fail};
use super::write::{self, JobKind, WriteJob};
use super::{DataNode, Target, forward, read, replica, sync};
use crate::base::types::ChunkId;
use crate::base::{ErrCode, clock, lock};
use crate::buf::Buf;
use crate::hooks::traits::{CODEC_REPLICA, QosItem, QosSpec, QueueKey, ShardSpec};
use crate::meta::cluster::chains::SERVING;
use crate::net::server::{Responder, Service};
use crate::proto::cap::{self, CapV1, RIGHT_ADMIN, RIGHT_READ, RIGHT_WRITE};
use crate::proto::data::{DataCall, DataOp, DataReply};
use crate::proto::frame::InFrame;
use crate::proto::peer::{ClusterMap, PeerOp};
use crate::proto::{decode_body, encode_body};

/// io 线程数（1.2 的线程表）、排队请求的上限（再多回 `BUSY`）。
const IO_THREADS: usize = 16;
const MAX_QUEUED: usize = 4096;
/// 低优先级存储类（`StorageClassV1.priority = 2`，4.5）。
const PRIORITY_LOW: u8 = 2;
/// 同步源的一个 PeerCall 在 QoS 里按多少字节计：FetchChunk 一次最多取回一整个 chunk（chunk_size 的上限 32 MiB）。
const SYNC_COST_BYTES: u32 = 32 << 20;

/// 排队中的一个请求：客户端的 DataCall，或同步源的 PeerCall（ListChunks / FetchChunk，M9）。
enum Work {
    Data(DataCall),
    Peer(PeerOp),
}

struct Job {
    work: Work,
    bulk: Option<Buf>,
    r: Responder,
}

/// 排队中的请求：token → 请求（qos 插件只认 token）。
struct Jobs {
    by_token: HashMap<u64, Job>,
    next_token: u64,
}

struct Queue {
    jobs: Mutex<Jobs>,
    cv: Condvar,
    depth: Arc<Gauge>,
}

pub struct DataService {
    node: Arc<DataNode>,
    q: Arc<Queue>,
}

impl DataService {
    /// 建服务并起 16 个 io 线程（随进程一直运行）。服务端装配时已检查恰好有一个 qos 插件。
    pub fn start(node: Arc<DataNode>) -> Result<Arc<DataService>, String> {
        let depth = metrics::gauge("wmfs_data_queue_depth", "数据节点 QoS 队列里等着执行的请求数", &[]);
        let jobs = Jobs { by_token: HashMap::new(), next_token: 1 };
        let q = Arc::new(Queue { jobs: Mutex::new(jobs), cv: Condvar::new(), depth });
        for i in 0..IO_THREADS {
            let (node, q) = (node.clone(), q.clone());
            std::thread::Builder::new()
                .name(format!("io-{i}"))
                .spawn(move || {
                    loop {
                        let job = take(&node, &q);
                        handle(&node, job);
                    }
                })
                .map_err(|e| format!("起 io 线程失败：{e}"))?;
        }
        Ok(Arc::new(DataService { node, q }))
    }

    /// 同步源的 ListChunks / FetchChunk（PeerCall，replica.rs 交过来）：进同一个 QoS 队列，标为后台（3.4.2.8）。
    pub fn peer(&self, op: PeerOp, r: Responder) {
        let key = QueueKey { tenant: 0, qos_id: 0, class_id: 0 };
        let spec = spec_of(None, 0);
        let job = Job { work: Work::Peer(op), bulk: None, r };
        self.enqueue(job, key, 0, SYNC_COST_BYTES, true, spec); // true = 后台（低优先级）
    }

    fn enqueue(&self, job: Job, key: QueueKey, job_id: u64, bytes: u32, background: bool, spec: QosSpec) {
        let Some((qos, hook)) = self.node.pipeline.qos_call() else {
            return job.r.error(ErrCode::FEATURE_UNSUPPORTED, 0);
        };
        let mut g = lock(&self.q.jobs);
        if g.by_token.len() >= MAX_QUEUED {
            drop(g);
            return job.r.error(ErrCode::BUSY, 0);
        }
        let token = g.next_token;
        g.next_token += 1;
        let item = QosItem { key, job_id, cost: 1 + bytes / (128 << 10), bytes, background, token };
        let queued = hook.call(|| {
            qos.enqueue(item, spec);
            Ok(())
        });
        if let Err(e) = queued {
            drop(g);
            return job.r.error(e, 0); // fail point plugin.<名字>.qos：这个请求以注入的码失败
        }
        g.by_token.insert(token, job);
        self.q.depth.set(g.by_token.len() as f64);
        drop(g);
        self.q.cv.notify_one();
    }
}

impl Service for DataService {
    fn call(&self, f: InFrame, r: Responder) {
        let call: DataCall = match decode_body(&f.body) {
            Ok(c) => c,
            Err(e) => return r.error(e, 0),
        };
        let peek = match cap::peek(&call.cap) {
            Ok(c) => c,
            Err(e) => return r.error(e, 0),
        };
        let bytes = match &call.op {
            DataOp::ReadChunk { len, .. } => *len,
            _ => f.bulk.as_ref().map_or(0, |b| b.len() as u32),
        };
        let key = QueueKey { tenant: peek.tenant, qos_id: peek.qos_id, class_id: peek.class_id };
        let spec = spec_of(self.node.map.get().as_deref(), peek.class_id);
        let background = peek.rights & RIGHT_ADMIN != 0;
        self.enqueue(Job { work: Work::Data(call), bulk: f.bulk, r }, key, peek.job_id, bytes, background, spec);
    }
}

/// QosSpec（4.5）：先只带存储类的低优先级标记，份额都是 0（不设）——mclock 在 M13。
fn spec_of(map: Option<&ClusterMap>, class_id: u16) -> QosSpec {
    let low = map.and_then(|m| m.classes.iter().find(|c| c.id == class_id)).is_some_and(|c| c.priority == PRIORITY_LOW);
    QosSpec { r: 0.0, w: 0.0, l: 0.0, bw_max: 0.0, burst_ms: 0, low_priority: low }
}

/// io 线程：等 qos 插件派发一个请求（见文件头）。
fn take(node: &DataNode, q: &Queue) -> Job {
    let qos = node.pipeline.qos();
    let mut g = lock(&q.jobs);
    loop {
        let wait = match qos.dequeue(Instant::now()) {
            Ok(t) => match g.by_token.remove(&t) {
                Some(j) => {
                    q.depth.set(g.by_token.len() as f64);
                    return j;
                }
                None => continue, // 插件给了一个不认识的 token：问下一个
            },
            Err(Some(at)) => at.saturating_duration_since(Instant::now()).max(Duration::from_millis(1)),
            Err(None) => Duration::from_millis(100),
        };
        g = q.cv.wait_timeout(g, wait).unwrap_or_else(|e| e.into_inner()).0;
    }
}

/// 一个请求校验之后的去向：直接回复（body + 可选的 bulk）、交给写队列（同一 chunk 串行，write.rs 负责回复），或本地做完之后
/// 沿链传播（SyncChunks / DeleteEpoch：整条链确认之后用这个 body 回复）。
enum Done {
    Reply(Vec<u8>, Option<Buf>),
    Write(Box<WriteJob>), // 装箱：WriteJob 带着 cap，比回复大得多
    Relay(PeerOp, u32, Vec<u8>),
}

fn handle(node: &Arc<DataNode>, job: Job) {
    let Job { work, bulk, r } = job;
    let call = match work {
        Work::Data(c) => c,
        Work::Peer(op) => {
            return match sync::serve(node, op) {
                Ok((body, bulk)) => r.reply(body, bulk.map(|b| vec![b.freeze().whole()]).unwrap_or_default()),
                Err(e) => r.error(e, 0),
            };
        }
    };
    match exec(node, call, bulk) {
        Ok(Done::Reply(body, bulk)) => r.reply(body, bulk.map(|b| vec![b.freeze().whole()]).unwrap_or_default()),
        Ok(Done::Write(mut w)) => {
            w.reply = Some(r);
            write::submit(node, w);
        }
        Ok(Done::Relay(op, succ, ok)) => forward::relay(node, op, succ, r, ok),
        Err(Fail(e, extra)) => r.error(e, extra),
    }
}

/// 校验并执行一个请求（见文件头）。
fn exec(node: &Arc<DataNode>, call: DataCall, bulk: Option<Buf>) -> Result<Done, Fail> {
    let cap = cap::verify(&node.cap_key, &call.cap)?;
    let map = node.map.get().ok_or(ErrCode::NOT_READY)?; // 还没拉到集群图（刚启动）
    if cap.session != 0 && map.evicted.iter().any(|(s, _)| *s == cap.session) {
        return Err(ErrCode::SESSION_EVICTED.into());
    }
    let now = clock::wall_ms();
    match call.op {
        DataOp::ReadChunk { chunk, chain_ver, off, len } => {
            let (t, l) = member(node, &map, chunk.chain, chain_ver, false)?;
            cap.allows(&chunk, RIGHT_READ, now)?;
            if node.chunks.has_dirty(&chunk) && l != Some(t.id) {
                node.craq.dirty_reads.inc(); // 有未提交版本、本成员不是 last serving：改问 L（3.4.2.5）
                return Err(Fail(ErrCode::CHUNK_DIRTY, l.unwrap_or(0) as u64));
            }
            let (buf, n, version) = read::read(node, t, &chunk, off, len)?;
            Ok(Done::Reply(encode_body(&DataReply::Read { len: n, version }), (n > 0).then_some(buf)))
        }
        DataOp::WriteChunk { chunk, chain_ver, extents, sync } => {
            let total: u64 = extents.iter().map(|(_, l)| *l as u64).sum();
            if total != bulk.as_ref().map_or(0, |b| b.len() as u64) {
                return Err(ErrCode::EINVAL.into());
            }
            write_job(node, &map, cap, (chunk, chain_ver), JobKind::Write { extents, sync }, bulk, now)
        }
        DataOp::TruncateChunk { chunk, chain_ver, new_len } => {
            write_job(node, &map, cap, (chunk, chain_ver), JobKind::Truncate { new_len }, None, now)
        }
        DataOp::SyncChunks { chain, chain_ver, chunks } => {
            let (t, _) = member(node, &map, chain, chain_ver, true)?;
            for c in &chunks {
                cap.allows(c, RIGHT_READ | RIGHT_WRITE, now)?;
            }
            node.store(|| t.store.sync(&chunks))?;
            let op = PeerOp::ForwardSync { chain, chain_ver, from_target: t.id, chunks };
            Ok(relay_or_reply(&map, chain, op, encode_body(&DataReply::Ok)))
        }
        DataOp::DeleteEpoch { chain, chain_ver, ino, epoch } => {
            let (t, _) = member(node, &map, chain, chain_ver, true)?;
            cap.check_time_and_right(RIGHT_ADMIN, now)?;
            if cap.ino != ino || cap.epoch != epoch || !cap.chains.contains(&chain) {
                return Err(ErrCode::BAD_CAP.into());
            }
            if t.read_only {
                return Err(ErrCode::EROFS.into()); // 与写、截断一样（3.2.3：只读的 target 不进写路径）
            }
            replica::drop_epoch(node, chain, ino, epoch);
            let files = node.store(|| t.store.delete_epoch(chain, ino, epoch))?;
            let op = PeerOp::ForwardDeleteEpoch { chain, chain_ver, from_target: t.id, ino, epoch };
            Ok(relay_or_reply(&map, chain, op, encode_body(&DataReply::Deleted { files })))
        }
    }
}

/// head 上本地做完了：有后继就沿链传播（整条链确认之后回 `ok`），否则直接回 `ok`。
fn relay_or_reply(map: &ClusterMap, chain_id: u32, op: PeerOp, ok: Vec<u8>) -> Done {
    match chain::find(map, chain_id).and_then(|c| chain::successor(c, 0)) {
        Some(s) => Done::Relay(op, s, ok),
        None => Done::Reply(ok, None),
    }
}

/// 写类请求：本节点要是 head、cap 要有写权限、codec 说本成员存整块（replica），然后交给写队列。
fn write_job(
    node: &DataNode,
    map: &ClusterMap,
    cap: CapV1,
    (chunk, chain_ver): (ChunkId, u64),
    kind: JobKind,
    bulk: Option<Buf>,
    now: u64,
) -> Result<Done, Fail> {
    let target = member(node, map, chunk.chain, chain_ver, true)?.0.id;
    cap.allows(&chunk, RIGHT_WRITE, now)?;
    let (codec, hook) = node.pipeline.codec_call(CODEC_REPLICA).ok_or(ErrCode::FEATURE_UNSUPPORTED)?;
    let shards = hook.call(|| Ok(codec.shards(1)))?; // 首发只有 replica codec：每个成员都存整块，看第一项就够
    if shards.first() != Some(&ShardSpec::Whole) {
        return Err(ErrCode::FEATURE_UNSUPPORTED.into()); // 首发只有多副本：每个成员存整块
    }
    Ok(Done::Write(Box::new(WriteJob { chunk, target, cap, kind, bulk, reply: None })))
}

/// 本节点上这条链的成员与这条链的 last serving（3.4.2.3 第 1 步、3.4.2.5）：链版本与本地视图一致（`chain::current`）；`head` = 要
/// 本节点是 head（第一个 Serving 成员），否则只要是一个 Serving 成员；I5 不成立 → `FENCED`。
fn member<'a>(
    node: &'a DataNode,
    map: &ClusterMap,
    chain_id: u32,
    chain_ver: u64,
    head: bool,
) -> Result<(&'a Target, Option<u32>), Fail> {
    let c = chain::current(node, map, chain_id, chain_ver)?;
    let mut serving = c.members.iter().filter(|(_, s)| *s == SERVING).map(|(t, _)| *t);
    let local = |t: u32| node.targets.get(&t);
    let t = if head { serving.next().and_then(local) } else { serving.find_map(local) };
    let t = t.ok_or(Fail(ErrCode::NOT_SERVING, 0))?;
    chain::check_i5(node, map, t.id)?;
    Ok((t, chain::last_serving(c)))
}
