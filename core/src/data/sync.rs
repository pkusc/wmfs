//! 重同步（design.md 3.4.2.8）：Syncing 成员主动从 **last serving**（不是直接前驱——前驱可能也在同步）拉取。
//!
//! 每个节点 2 个 `sync-<i>` 线程（最多 2 条链并发），轮流找本节点是 Syncing 成员的链：
//! 1. 分页 `ListChunks(chain, after, 1000)` 向源要 [(chunk, 已提交版本)]（源遍历 `c/<链>/` 读文件头）；
//! 2. 本地缺失或版本与源不同（无论高低：比源高只可能是从未在全链提交的旧 pending 残留，或属于更早的 incarnation）→ `FetchChunk`：
//!    源打开已提交文件（rename 之后旧 fd 仍指向原版本，天然是一致快照）返回 packed 形式；本地占住 chunk，本轮收到过转发的（touched）
//!    丢掉取回的数据，否则 `install`（t/ 临时文件 → fdatasync → rename → fsync 目录，R3）；
//! 3. 本地有、源的列表里没有、也不在 touched 里的 chunk → 删除；
//! 4. 交给 hb 线程报 `SyncDone(target, chain, chain_ver)`：leader 那边 chain_ver 未变、本成员紧跟最后一个 Serving → 变 Serving；
//!    否则什么也不变，下一轮重跑（只拷有差异的 chunk，代价很小）。
//!
//! 同步线程只在安装那一刻占住 chunk，所以不会有"源端等本端的确认、本端等源端的 FetchChunk"的死锁；那一刻 chunk 正忙（正有转发在
//! 写它）→ 这一轮作废重跑（审查后：以前当作装上了，转发若没在本地提交，本地就带着旧版本交了 SyncDone）。源端的 ListChunks /
//! FetchChunk 在 io 线程上、走 QoS 的后台低优先级队列（[`serve`]）。fail point `sync.fetch.midway`：取回第一个 chunk 之后以注入的码
//! 结束这一轮。
//!
//! 实现注记（M9 审查）：本节点**第一次**看到自己是某条链的 Syncing 成员（刚被追加、或刚被降级）之后，等 0.8 × `target_lease` 才开始
//! 第一轮：同步源（L）这时也许还没拿到这张图、还当自己是尾，它这段时间提交的写既不会转发过来、也不一定在已经取到的列表里。等这么久，
//! 还在服务的成员要么已换上不旧于它的图，要么按 I5 自我隔离了（心跳回复带新的 map_epoch）。代价：重同步晚开始约 6.4 s。

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use wmfs_api::failpoint::fail_point;

use super::{DataNode, Target, chain, packed, read};
use crate::base::types::ChunkId;
use crate::base::{ErrCode, clock, lock, log};
use crate::buf::Buf;
use crate::net::client::{Endpoint, Family, RpcClient, check_reply};
use crate::proto::peer::{PeerOp, PeerReply};
use crate::proto::{decode_body, encode_body};

const THREADS: usize = 2;
const PAGE: u32 = 1000;
const RPC_TIMEOUT: Duration = Duration::from_secs(10);
/// 一轮做完之后等集群图里这条链的版本变（SyncDone 生效）最多多久，再重跑。
const SETTLE_MS: u64 = 3000;

/// 两个同步线程共用：正在同步的 (链, target)，与本节点第一次看到自己是这条链的 Syncing 成员的时刻（mono_ms，见文件头）。
#[derive(Default)]
struct Picker {
    running: HashSet<(u32, u32)>,
    since: HashMap<(u32, u32), u64>,
}

/// 起同步线程（随进程一直运行）。
pub fn start(node: &Arc<DataNode>) -> Result<(), String> {
    let picker: Arc<Mutex<Picker>> = Arc::default();
    for i in 0..THREADS {
        let (node, picker) = (node.clone(), picker.clone());
        std::thread::Builder::new()
            .name(format!("sync-{i}"))
            .spawn(move || {
                loop {
                    match pick(&node, &picker) {
                        Some(job) => {
                            run(&node, job);
                            lock(&picker).running.remove(&(job.chain, job.target));
                        }
                        None => std::thread::sleep(Duration::from_millis(200)),
                    }
                }
            })
            .map_err(|e| format!("起同步线程失败：{e}"))?;
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct Job {
    chain: u32,
    target: u32,
    ver: u64,
    src: u32,
}

/// 下一条要同步的链：本节点是它的 Syncing 成员、第一次看到这件事已过了 0.8 × target_lease（见文件头），有 last serving，别的同步
/// 线程没在做。
fn pick(node: &DataNode, picker: &Mutex<Picker>) -> Option<Job> {
    let map = node.map.get()?;
    let (mut p, now, wait) = (lock(picker), clock::mono_ms(), map.settings.target_lease_ms * 4 / 5);
    let syncing: Vec<(u32, u32, u64, Option<u32>)> = map
        .chains
        .iter()
        .filter_map(|(id, c)| {
            chain::local(node, c).filter(|(_, pos)| !chain::serving_at(c, *pos)).map(|(t, _)| (id, t, c))
        })
        .map(|(id, t, c)| (*id, t, c.ver, chain::last_serving(c)))
        .collect();
    p.since.retain(|k, _| syncing.iter().any(|(c, t, ..)| (*c, *t) == *k));
    for (chain, target, ver, src) in syncing {
        let since = *p.since.entry((chain, target)).or_insert(now);
        let Some(src) = src else { continue }; // 没有 Serving 成员：等
        if now >= since + wait && p.running.insert((chain, target)) {
            return Some(Job { chain, target, ver, src });
        }
    }
    None
}

/// 一轮同步（见文件头）；做完了交 SyncDone，再等一会儿集群图里这条链的版本变。
fn run(node: &Arc<DataNode>, j: Job) {
    lock(&node.touched).insert((j.chain, j.target), HashSet::new());
    let r = copy(node, j);
    lock(&node.touched).remove(&(j.chain, j.target));
    match r {
        Ok(n) => {
            log::info(
                "sync",
                format_args!("链 {} 的 target {}：同步完了（取回 {n} 个 chunk），报 SyncDone", j.chain, j.target),
            );
            lock(&node.sync_done).push((j.chain, j.target, j.ver));
        }
        Err(e) => log::warn("sync", format_args!("链 {} 的 target {} 同步失败：{e}（稍后重跑）", j.chain, j.target)),
    }
    let deadline = clock::mono_ms() + SETTLE_MS;
    while clock::mono_ms() < deadline {
        if node.map.get().and_then(|m| chain::find(&m, j.chain).map(|c| c.ver)) != Some(j.ver) {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn call(rpc: &RpcClient, ep: &Endpoint, op: &PeerOp) -> Result<(PeerReply, Option<Buf>), ErrCode> {
    let f = check_reply(rpc.call(ep, Family::Data, super::forward::peer_frame(op, vec![]), RPC_TIMEOUT)?)?;
    Ok((decode_body(&f.body)?, f.bulk))
}

/// 第 1–3 步；返回取回了几个 chunk。
fn copy(node: &Arc<DataNode>, j: Job) -> Result<u64, ErrCode> {
    let rpc = node.peer.get().ok_or(ErrCode::NOT_READY)?;
    let ep = node.map.get().and_then(|m| chain::endpoint(&m, j.src)).ok_or(ErrCode::NOT_SERVING)?;
    let t = node.targets.get(&j.target).ok_or(ErrCode::NOT_SERVING)?;
    let (mut items, mut after) = (Vec::new(), None);
    loop {
        match call(rpc, &ep, &PeerOp::ListChunks { chain: j.chain, after, limit: PAGE })? {
            (PeerReply::Chunks { items: page, more }, _) => {
                after = page.last().map(|(id, _)| *id);
                items.extend(page);
                if !more {
                    break;
                }
            }
            _ => return Err(ErrCode::INTERNAL),
        }
    }
    let mut fetched = 0;
    for (id, v) in &items {
        if touched(node, j, id) || read::committed_version(node, t, id) == Some(*v) {
            continue;
        }
        let (rep, bulk) = match call(rpc, &ep, &PeerOp::FetchChunk { chunk: *id }) {
            Err(ErrCode::CHUNK_NOT_FOUND) => continue, // 源端只有 pending（还在路上的写会转发过来）或刚被 DeleteEpoch 删掉
            r => r?,
        };
        let (PeerReply::Fetched { .. }, Some(b)) = (rep, bulk) else { return Err(ErrCode::INTERNAL) };
        install(node, t, j, id, b.as_slice())?;
        fetched += 1;
        fail_point("sync.fetch.midway").map_err(ErrCode)?;
    }
    let have: HashSet<ChunkId> = items.iter().map(|(id, _)| *id).collect();
    let mut after = None;
    loop {
        let page = node.store(|| t.store.list_chain(j.chain, after.as_ref(), PAGE as usize))?;
        for id in page.iter().filter(|id| !have.contains(id)) {
            if node.chunks.claim(id) {
                // 源没有它：删掉（第 3 步）；删不掉下一轮再删
                if !touched(node, j, id)
                    && let Err(e) = node.store(|| t.store.delete_chunk(id))
                {
                    log::warn("sync", format_args!("链 {} 的 target {}：删除 {id:?} 失败：{e}", j.chain, j.target));
                }
                node.chunks.finish(id);
            }
        }
        if page.len() < PAGE as usize {
            return Ok(fetched);
        }
        after = page.last().copied();
    }
}

fn touched(node: &DataNode, j: Job, id: &ChunkId) -> bool {
    lock(&node.touched).get(&(j.chain, j.target)).is_some_and(|s| s.contains(id))
}

/// Syncing 成员在本地提交了转发来的版本（自己是尾、重复、收到后继的确认）：本轮同步不要再拿源端也许更旧的已提交版本覆盖它，也不要
/// 当成"源端没有的"删掉（3.4.2.8 注记 ②）。本节点这个 target 不在同步这条链 → 什么也不做。
pub fn touch(node: &DataNode, id: &ChunkId, target: u32) {
    if let Some(s) = lock(&node.touched).get_mut(&(id.chain, target)) {
        s.insert(*id);
    }
}

/// 装上取回的 packed 版本（第 2 步）：本轮收到过转发 → 丢掉；chunk 正忙 → `BUSY`（这一轮作废重跑，见文件头）。
fn install(node: &DataNode, t: &Target, j: Job, id: &ChunkId, packed: &[u8]) -> Result<(), ErrCode> {
    let (h, writes) = packed::unpack(packed)?;
    if (h.ino, h.epoch, h.chunk_idx, h.chain_id) != (id.ino, id.epoch, id.idx, id.chain) {
        return Err(ErrCode::EINVAL);
    }
    if !node.chunks.claim(id) {
        return Err(ErrCode::BUSY);
    }
    let r = if touched(node, j, id) { Ok(()) } else { node.store(|| t.store.install(id, &writes)) };
    if r.is_ok() {
        node.chunks.clear_corrupt(id);
        node.craq.resync_bytes.add(packed.len() as u64);
    }
    node.chunks.finish(id);
    r
}

/// 同步源的服务（io 线程，后台低优先级）：ListChunks 与 FetchChunk。返回 (回复 body, bulk)。
pub fn serve(node: &DataNode, op: PeerOp) -> Result<(Vec<u8>, Option<Buf>), ErrCode> {
    let map = node.map.get().ok_or(ErrCode::NOT_READY)?;
    let chain_id = match &op {
        PeerOp::ListChunks { chain, .. } => *chain,
        PeerOp::FetchChunk { chunk } => chunk.chain,
        _ => return Err(ErrCode::INTERNAL),
    };
    let c = chain::find(&map, chain_id).ok_or(ErrCode::STALE_CHAIN)?;
    let (t, _) = chain::local(node, c).ok_or(ErrCode::NOT_SERVING)?;
    let store = &node.targets.get(&t).ok_or(ErrCode::NOT_SERVING)?.store;
    match op {
        PeerOp::ListChunks { after, limit, .. } => {
            let n = limit.min(PAGE) as usize;
            let ids = node.store(|| store.list_chain(chain_id, after.as_ref(), n))?;
            let (more, upto) = (ids.len() == n, ids.last().copied());
            let mut items = BTreeMap::new();
            for id in &ids {
                if let Some(c) = read::open(node, &**store, id)? {
                    items.insert(*id, c.header.chunk_version);
                }
            }
            // 有 pending 的 chunk 报 pending 的版本（只有 pending 的也列出）：这个写可能已在后面的成员上提交了，不能被同步换回旧版本或删掉
            let mine = |c: &ChunkId| c.chain == chain_id && after.is_none_or(|a| *c > a) && (!more || Some(*c) <= upto);
            for id in node.chunks.pendings(|c, _| mine(c)) {
                if let Some(v) = node.chunks.with_pending(&id, None, |p| p.version) {
                    let e = items.entry(id).or_insert(v);
                    *e = (*e).max(v);
                }
            }
            let items = items.into_iter().collect();
            Ok((encode_body(&PeerReply::Chunks { items, more }), None))
        }
        PeerOp::FetchChunk { chunk } => {
            let mut rd = node.store(|| store.open_committed(&chunk))?.ok_or(ErrCode::CHUNK_NOT_FOUND)?;
            let (b, h) = packed::pack(&node.pool, &mut |off, buf| node.store(|| rd.read_at(off, buf)))?;
            Ok((encode_body(&PeerReply::Fetched { version: h.chunk_version }), Some(b)))
        }
        _ => Err(ErrCode::INTERNAL),
    }
}
