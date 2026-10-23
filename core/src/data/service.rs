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
//! SyncChunks 与 DeleteEpoch 只在本节点执行（链长 1；沿链传播在 M9）；只读的 target（3.2.3）上写、截断与 DeleteEpoch 一律 `EROFS`。

use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use wmfs_api::metrics::{self, Gauge};

use super::write::{self, JobKind, WriteJob};
use super::{DataNode, Target, read};
use crate::base::types::ChunkId;
use crate::base::{ErrCode, clock, lock};
use crate::buf::Buf;
use crate::hooks::traits::{CODEC_REPLICA, QosItem, QosSpec, QueueKey, ShardSpec};
use crate::meta::cluster::chains::SERVING;
use crate::net::server::{Responder, Service};
use crate::proto::cap::{self, CapV1, RIGHT_ADMIN, RIGHT_READ, RIGHT_WRITE};
use crate::proto::data::{DataCall, DataOp, DataReply};
use crate::proto::frame::InFrame;
use crate::proto::peer::ClusterMap;
use crate::proto::{decode_body, encode_body};

/// io 线程数（1.2 的线程表）、排队请求的上限（再多回 `BUSY`）。
const IO_THREADS: usize = 16;
const MAX_QUEUED: usize = 4096;
/// 低优先级存储类（`StorageClassV1.priority = 2`，4.5）。
const PRIORITY_LOW: u8 = 2;

/// 排队中的一个请求。
struct Job {
    call: DataCall,
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

/// 一个失败的回复：错误码与 ErrorBody.extra（`STALE_CHAIN` 带本地的 chain_ver）。
struct Fail(ErrCode, u64);

impl From<ErrCode> for Fail {
    fn from(e: ErrCode) -> Fail {
        Fail(e, 0)
    }
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
        let Some((qos, hook)) = self.node.pipeline.qos_call() else { return r.error(ErrCode::FEATURE_UNSUPPORTED, 0) };
        let bytes = match &call.op {
            DataOp::ReadChunk { len, .. } => *len,
            _ => f.bulk.as_ref().map_or(0, |b| b.len() as u32),
        };
        let key = QueueKey { tenant: peek.tenant, qos_id: peek.qos_id, class_id: peek.class_id };
        let spec = spec_of(self.node.map.get().as_deref(), peek.class_id);
        let mut g = lock(&self.q.jobs);
        if g.by_token.len() >= MAX_QUEUED {
            drop(g);
            return r.error(ErrCode::BUSY, 0);
        }
        let token = g.next_token;
        g.next_token += 1;
        let background = peek.rights & RIGHT_ADMIN != 0;
        let item = QosItem { key, job_id: peek.job_id, cost: 1 + bytes / (128 << 10), bytes, background, token };
        let queued = hook.call(|| {
            qos.enqueue(item, spec);
            Ok(())
        });
        if let Err(e) = queued {
            drop(g);
            return r.error(e, 0); // fail point plugin.<名字>.qos：这个请求以注入的码失败
        }
        g.by_token.insert(token, Job { call, bulk: f.bulk, r });
        self.q.depth.set(g.by_token.len() as f64);
        drop(g);
        self.q.cv.notify_one();
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

/// 一个请求校验之后的去向：直接回复（body + 可选的 bulk），或交给写队列（同一 chunk 串行，write.rs 负责回复）。
enum Done {
    Reply(Vec<u8>, Option<Buf>),
    Write(Box<WriteJob>), // 装箱：WriteJob 带着 cap，比回复大得多
}

fn handle(node: &DataNode, job: Job) {
    let Job { call, bulk, r } = job;
    match exec(node, call, bulk) {
        Ok(Done::Reply(body, bulk)) => r.reply(body, bulk.map(|b| vec![b.freeze().whole()]).unwrap_or_default()),
        Ok(Done::Write(mut w)) => {
            w.reply = Some(r);
            write::submit(node, w);
        }
        Err(Fail(e, extra)) => r.error(e, extra),
    }
}

/// 校验并执行一个请求（见文件头）。
fn exec(node: &DataNode, call: DataCall, bulk: Option<Buf>) -> Result<Done, Fail> {
    let cap = cap::verify(&node.cap_key, &call.cap)?;
    let map = node.map.get().ok_or(ErrCode::NOT_READY)?; // 还没拉到集群图（刚启动）
    if cap.session != 0 && map.evicted.iter().any(|(s, _)| *s == cap.session) {
        return Err(ErrCode::SESSION_EVICTED.into());
    }
    let now = clock::wall_ms();
    match call.op {
        DataOp::ReadChunk { chunk, chain_ver, off, len } => {
            let t = member(node, &map, chunk.chain, chain_ver, false)?;
            cap.allows(&chunk, RIGHT_READ, now)?;
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
            let t = member(node, &map, chain, chain_ver, true)?;
            for c in &chunks {
                cap.allows(c, RIGHT_READ | RIGHT_WRITE, now)?;
            }
            node.store(|| t.store.sync(&chunks))?;
            Ok(Done::Reply(encode_body(&DataReply::Ok), None))
        }
        DataOp::DeleteEpoch { chain, chain_ver, ino, epoch } => {
            let t = member(node, &map, chain, chain_ver, true)?;
            cap.check_time_and_right(RIGHT_ADMIN, now)?;
            if cap.ino != ino || cap.epoch != epoch || !cap.chains.contains(&chain) {
                return Err(ErrCode::BAD_CAP.into());
            }
            if t.read_only {
                return Err(ErrCode::EROFS.into()); // 与写、截断一样（3.2.3：只读的 target 不进写路径）
            }
            let files = node.store(|| t.store.delete_epoch(chain, ino, epoch))?;
            Ok(Done::Reply(encode_body(&DataReply::Deleted { files }), None))
        }
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
    let target = member(node, map, chunk.chain, chain_ver, true)?.id;
    cap.allows(&chunk, RIGHT_WRITE, now)?;
    let (codec, hook) = node.pipeline.codec_call(CODEC_REPLICA).ok_or(ErrCode::FEATURE_UNSUPPORTED)?;
    let shards = hook.call(|| Ok(codec.shards(1)))?; // 链长 1（M5）：head 就是全部成员
    if shards.first() != Some(&ShardSpec::Whole) {
        return Err(ErrCode::FEATURE_UNSUPPORTED.into()); // 首发只有多副本：每个成员存整块
    }
    Ok(Done::Write(Box::new(WriteJob { chunk, target, cap, kind, bulk, reply: None })))
}

/// 本节点上这条链的成员（3.4.2.3 第 1 步、3.4.2.5）：链版本与本地视图一致；`head` = 要本节点是 head（第一个 Serving 成员），
/// 否则只要是一个 Serving 成员。
fn member<'a>(
    node: &'a DataNode,
    map: &ClusterMap,
    chain: u32,
    chain_ver: u64,
    head: bool,
) -> Result<&'a Target, Fail> {
    let Some((_, c)) = map.chains.iter().find(|(id, _)| *id == chain) else {
        node.map.poke(); // 本地还不认识这条链：多半是刚生成的，马上拉新集群图
        return Err(Fail(ErrCode::STALE_CHAIN, 0));
    };
    if c.ver != chain_ver {
        if chain_ver > c.ver {
            node.map.poke();
        }
        return Err(Fail(ErrCode::STALE_CHAIN, c.ver));
    }
    let mut serving = c.members.iter().filter(|(_, s)| *s == SERVING).map(|(t, _)| *t);
    let local = |t: u32| node.targets.get(&t);
    let t = if head { serving.next().and_then(local) } else { serving.find_map(local) };
    t.ok_or(Fail(ErrCode::NOT_SERVING, 0))
}
