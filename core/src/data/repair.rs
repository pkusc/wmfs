//! 校验与修复一个 chunk 副本（design.md 3.2.5 R5、4.12 的 scrub、ADR-0040 第 10 条；实现注记（M11））：scrub 插件与写路径的"先修复"
//! 共用。
//!
//! - **检查**（[`verify`]）：打开本地已提交版本、把每一块都读出来解码（decrypt → decompress → 校验和）。头读不出、CRC 不对、文件比块表短、
//!   任何一块校验不符 → 坏了（标 Corrupt，`wmfs_chunk_corrupt_total`）；还拿不到数据密钥、少了某个变换插件之类 → 现在查不了（不算坏）。
//! - **修复**（[`repair`]）：在同一条链别的 **Serving** 成员那里找一份好的——它此刻没有在途的版本（ListChunks 报的版本 = FetchChunk 拿到的
//!   已提交版本）、不比本地的旧、取回来的 packed 形式每一块都解得开——装到本地（[`install`]：t/ → fdatasync → rename → fsync 目录，R3），
//!   清掉 Corrupt。装之前（占住这个 chunk 之后）再核对一次本地：没有 pending、已提交的版本还是修复开始时看到的那个（头坏的还是读不出）——
//!   取回数据的这段时间里有转发在本地提交了更新的版本（例如 Syncing 的链尾回 NeedFull、前驱马上发了完整转发），就不能拿取回的旧版本把它
//!   换回去（同 3.4.2.8 的 touched），这一次作废。本地这个 chunk 被占着、对方有在途的版本、连不上 → 稍后（不算丢）；链上没有别的 Serving
//!   成员、或它们的副本也都是坏的（块校验不符，或头读不出：对方的 FetchChunk 回 `CORRUPT`）→ **数据丢失**（诚实上报，之后读它照旧
//!   `CORRUPT` → 客户端 `EIO`；同时记为"丢失"，写路径不再为它等修复）。只读的 target 不写盘。每修一个 chunk 问一次对方这一个 chunk 的列表
//!   项（看在途的版本；local-fs 要列一遍这条链的目录）——修复本来就少，不另加协议。
//!   为什么"双方都没有在途的版本"就够：本地不占着它 = 本地分配或转发过的版本都已全链确认（I3），而对方没有在途的版本 = 它已提交的就是
//!   它见过的最新的；同一条链上两个这样的成员已提交的版本相同（I2、I6）。
//! - **读写路径要的修复**（[`want_repair`]，`repair` 线程，3.2.5 R5"先用与 scrub 相同的函数修复再继续"）：head 上的写碰上用不了的本地副本
//!   （头读不出——不知道旧版本号；或要保留的旧块坏了）、链上还有别的 Serving 成员时，write.rs 把它交给本节点的 `repair` 线程、回可重试的
//!   `BUSY`（io 线程不等网络）；线程修好之后客户端的重试照常做。读路径发现的坏块也马上交给它（ADR-0042 第 7 条，read.rs），不等 scrub
//!   的下一轮；客户端照旧换下一个副本读。修好或修不了照常上报（[`report`]）。
//! - **上报**（[`report`]）：NodeCall `ReportCorrupt{target, chunk}` 交给元数据 leader，leader 写 changelog——`target` = 修好之后持有
//!   好副本的 target（就是本地这个）→ `ChecksumRepaired`；`target = 0` = 一个好副本也没有 → `DataLossDetected`。规格只给了这一个消息、
//!   没区分两种结局：用 target 的保留值 0（id 从 1 开始，3.2.7）表示"没有"是最小的做法（见 design.md 3.2.5 的 M11 注记）。
//! - ADR-0040 第 10 条的两件也在这里落地：同步源（L）上的坏 chunk 被它自己的 scrub 修好之后，重同步自然完成；Serving 成员缺了 L 有的版本
//!   （[`missing_from_last`]）由 scrub 从 L 补上。

use std::sync::Arc;
use std::time::Duration;

use super::forward::peer_call;
use super::node_hb::leader_call;
use super::{DataNode, Target, chain, packed, read, write};
use crate::base::types::ChunkId;
use crate::base::{ErrCode, log};
use crate::buf::{Buf, Lane, POOL_WAIT};
use crate::meta::cluster::chains::SERVING;
use crate::proto::peer::{ClusterMap, NodeOp, PeerOp, PeerReply};

/// 写路径交给 `repair` 线程的 chunk 最多排多少个（满了照样回 `BUSY`，客户端下一次重试再排）。
const QUEUE: usize = 256;

/// 检查的结论。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    Good { version: u64, bytes: u64 }, // bytes = 读过的存储字节（限速用）
    Missing,
    Bad { version: Option<u64> }, // 头读得出来时知道它的版本
}

/// 修复的结局。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Repair {
    Repaired { from: u32 },
    Lost,
    Later(ErrCode),
}

fn bad(e: ErrCode) -> bool {
    matches!(e, ErrCode::CORRUPT | ErrCode::CHECKSUM_MISMATCH)
}

/// 检查本地已提交版本的每一块（见文件头）。查不了 → `Err`（不算坏）。
pub fn verify(node: &DataNode, t: &Target, id: &ChunkId) -> Result<Verdict, ErrCode> {
    let mut c = match read::open(node, &*t.store, id) {
        Ok(Some(c)) => c,
        Ok(None) => return Ok(Verdict::Missing),
        Err(e) if bad(e) => return Ok(Verdict::Bad { version: None }),
        Err(e) => return Err(e),
    };
    let (version, mut bytes) = (c.header.chunk_version, 0);
    for i in 0..c.header.nblocks {
        bytes += c.table[i as usize].stored_len as u64;
        match c.block(node, id, i) {
            Ok(_) => {}
            Err(e) if bad(e) => return Ok(Verdict::Bad { version: Some(version) }),
            Err(e) => return Err(e),
        }
    }
    Ok(Verdict::Good { version, bytes })
}

/// 一份 packed 形式（对方取回来的）是不是每一块都解得开。
fn check_packed(node: &DataNode, id: &ChunkId, b: &[u8]) -> Result<(), ErrCode> {
    let u = packed::unpack(b)?;
    let h = &u.header;
    if (h.ino, h.epoch, h.chunk_idx, h.chain_id) != (id.ino, id.epoch, id.idx, id.chain) {
        return Err(ErrCode::CORRUPT);
    }
    for (i, e) in u.table.iter().enumerate().filter(|(_, e)| e.stored_len > 0) {
        let data = u.writes.iter().find(|(off, _)| *off == h.block_offset(i as u32)).ok_or(ErrCode::CORRUPT)?.1;
        let mut b: Buf = node.pool.get(data.len().max(1), Lane::Normal, POOL_WAIT)?;
        b.set_len(data.len());
        b.as_mut_slice().copy_from_slice(data);
        read::decode(node, h, id, i as u32, e, b)?;
    }
    Ok(())
}

/// ListChunks 里正好从 `id` 开始的那一页（`after` = 排在它前面的最大可能的身份）。
fn before(id: &ChunkId) -> ChunkId {
    match id.idx.checked_sub(1) {
        Some(idx) => ChunkId { idx, ..*id },
        None => ChunkId { epoch: id.epoch.saturating_sub(1), idx: u32::MAX, ..*id },
    }
}

/// 从 Serving 成员 `src` 取一份能用的（见文件头的修复规则）：Ok(None) = 它的副本也是坏的；`Err` = 现在用不了（稍后）。
fn fetch_good(
    node: &DataNode,
    map: &ClusterMap,
    src: u32,
    id: &ChunkId,
    ours: Option<u64>,
) -> Result<Option<Buf>, ErrCode> {
    let ep = chain::endpoint(map, src).ok_or(ErrCode::NOT_SERVING)?;
    // 对方此刻报的版本（含在途的）；它这一页里有头读不出的 chunk 时整页报坏码 → 不知道（取回来再看）
    let listed = match peer_call(node, &ep, &PeerOp::ListChunks { chain: id.chain, after: Some(before(id)), limit: 1 })
    {
        Ok((PeerReply::Chunks { items, .. }, _)) => items.into_iter().find(|(c, _)| c == id).map(|(_, v)| v),
        Ok(_) => return Err(ErrCode::INTERNAL),
        Err(e) if bad(e) => None,
        Err(e) => return Err(e),
    };
    let (rep, bulk) = match peer_call(node, &ep, &PeerOp::FetchChunk { chunk: *id }) {
        Err(e) if bad(e) => return Ok(None), // 对方的头也读不出：也是坏的
        r => r?,
    };
    let (PeerReply::Fetched { version }, Some(b)) = (rep, bulk) else { return Err(ErrCode::INTERNAL) };
    match check_packed(node, id, b.as_slice()) {
        Ok(()) => {}
        Err(e) if bad(e) => return Ok(None),
        Err(e) => return Err(e),
    }
    if listed != Some(version) || ours.is_some_and(|v| version < v) {
        return Err(ErrCode::BUSY); // 对方有在途的版本（或看不出来）、或比本地的旧：稍后再看
    }
    Ok(Some(b))
}

/// 修复本地的 `id`（`ours` = 本地已提交的版本，头坏了时不知道；见文件头）。
pub fn repair(node: &Arc<DataNode>, t: &Target, id: &ChunkId, ours: Option<u64>) -> Repair {
    if t.read_only() {
        return Repair::Later(ErrCode::EROFS);
    }
    if node.chunks.held(id) {
        return Repair::Later(ErrCode::BUSY); // 有在途的版本：稍后
    }
    let Some(map) = node.map.get() else { return Repair::Later(ErrCode::NOT_READY) };
    let Some(c) = chain::find(&map, id.chain) else { return Repair::Later(ErrCode::STALE_CHAIN) };
    let sources: Vec<u32> = c.members.iter().filter(|(m, s)| *s == SERVING && *m != t.id).map(|(m, _)| *m).collect();
    let mut later = None;
    for src in sources {
        match fetch_good(node, &map, src, id, ours) {
            Ok(Some(b)) => match install(node, t, id, b.as_slice(), ours) {
                Ok(()) => return Repair::Repaired { from: src },
                Err(e) => later = Some(e),
            },
            Ok(None) => log::warn("data", format_args!("{id:?}：target {src} 上的副本也是坏的")),
            Err(e) => later = Some(e),
        }
    }
    later.map_or(Repair::Lost, Repair::Later) // 都试过了：有用不了的就稍后再试，全都是坏的（或没有别的成员）才算丢
}

/// 装上取回来的版本（见文件头）：占住这个 chunk（正忙 → `BUSY`），本地有 pending、或已提交的版本已经不是 `ours` → `BUSY`（作废）。
/// 占着它的这段时间里排进来的写，放下之后接着做（`write::drain`）。
pub fn install(node: &Arc<DataNode>, t: &Target, id: &ChunkId, b: &[u8], ours: Option<u64>) -> Result<(), ErrCode> {
    let u = packed::unpack(b)?;
    if !node.chunks.claim(id) {
        return Err(ErrCode::BUSY);
    }
    let same = !node.chunks.has_pending(id) && read::committed_version(node, t, id) == ours;
    let r = if same { node.store(|| t.store.install(id, &u.writes)) } else { Err(ErrCode::BUSY) };
    if r.is_ok() {
        node.chunks.clear_corrupt(id);
    }
    write::drain(node, node.chunks.finish(id));
    r
}

/// 修复并上报、写日志（scrub 与 `repair` 线程共用）：修好了 → `ChecksumRepaired`；修不了 → 记为丢失、`DataLossDetected`；稍后 → 不报。
pub fn repair_and_report(node: &Arc<DataNode>, t: &Target, id: &ChunkId, ours: Option<u64>) -> Repair {
    let r = repair(node, t, id, ours);
    let to = match r {
        Repair::Repaired { from } => {
            log::warn("data", format_args!("target {} 上的 {id:?} 从 target {from} 修好了", t.id));
            Some(Some(t.id))
        }
        Repair::Lost => {
            node.chunks.mark_lost(id);
            log::error("data", format_args!("target {} 上的 {id:?} 坏了，没有任何好副本：数据丢失", t.id));
            Some(None)
        }
        Repair::Later(e) => {
            log::info("data", format_args!("target {} 上的 {id:?} 现在修不了：{e}（稍后再试）", t.id));
            None
        }
    };
    if let Some(repaired) = to
        && let Err(e) = report(node, repaired, id)
    {
        log::warn("data", format_args!("{id:?} 的修复结果没能报给元数据 leader：{e}"));
    }
    r
}

/// 起 `repair` 线程（随进程一直运行，`replica::start` 调用）：一个一个修读写路径交来的 chunk（见文件头）。
pub fn start(node: &Arc<DataNode>) -> Result<(), String> {
    let (tx, rx) = crossbeam_channel::bounded::<(u32, ChunkId)>(QUEUE);
    let _ = node.repair_q.set(tx);
    let node = node.clone();
    let run = move || {
        while let Ok((t, id)) = rx.recv() {
            let Some(tg) = node.targets.get(&t) else { continue };
            // 交来它的那个写这时还占着它（马上就放）：等一会儿；已经修好了（重试的写把它排了好几次）就跳过
            for _ in 0..100 {
                if !node.chunks.held(&id) {
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            if node.chunks.is_corrupt(&id) && !node.chunks.is_lost(&id) {
                repair_and_report(&node, tg, &id, read::committed_version(&node, tg, &id));
            }
        }
    };
    std::thread::Builder::new()
        .name("repair".into())
        .spawn(run)
        .map(|_| ())
        .map_err(|e| format!("起 repair 线程失败：{e}"))
}

/// 读写路径（read.rs、write.rs）：请 `repair` 线程修 target `t` 上的 `id`。没有这个线程（组件测试里单独的数据节点）→ false。
pub fn want_repair(node: &DataNode, t: u32, id: &ChunkId) -> bool {
    let Some(q) = node.repair_q.get() else { return false };
    let _ = q.try_send((t, *id)); // 满了：这一次只回 BUSY，下一次重试再排
    true
}

/// [`missing_from_last`] 的一页：要从 L 补的 (chunk, 本地已提交的版本)、L 这一页的最后一个、L 还有没有更多。
pub struct MissingPage {
    pub todo: Vec<(ChunkId, Option<u64>)>,
    pub last: Option<ChunkId>,
    pub more: bool,
}

/// Serving 但不是 L 的成员：L 有、本地没有或比本地新（而双方都不占着）的 chunk（"Serving 成员缺基础版本"，ADR-0040 第 10 条）。
/// 一次最多看 L 列出的 `limit` 个（从 `after` 之后）。
pub fn missing_from_last(
    node: &DataNode,
    t: &Target,
    chain_id: u32,
    after: Option<ChunkId>,
    limit: u32,
) -> Result<MissingPage, ErrCode> {
    let map = node.map.get().ok_or(ErrCode::NOT_READY)?;
    let c = chain::find(&map, chain_id).ok_or(ErrCode::STALE_CHAIN)?;
    let l = chain::last_serving(c).filter(|l| *l != t.id).ok_or(ErrCode::NOT_SERVING)?;
    let ep = chain::endpoint(&map, l).ok_or(ErrCode::NOT_SERVING)?;
    let (items, more) = match peer_call(node, &ep, &PeerOp::ListChunks { chain: chain_id, after, limit })? {
        (PeerReply::Chunks { items, more }, _) => (items, more),
        _ => return Err(ErrCode::INTERNAL),
    };
    let last = items.last().map(|(id, _)| *id);
    let mut todo = vec![];
    for (id, v) in items {
        let mine = read::committed_version(node, t, &id);
        if mine.is_none_or(|m| m < v) && !node.chunks.held(&id) {
            todo.push((id, mine));
        }
    }
    Ok(MissingPage { todo, last, more })
}

/// 上报一个修好了（`repaired` = 本地的 target）或修不了（`None`，数据丢失）的坏块（见文件头）。
pub fn report(node: &DataNode, repaired: Option<u32>, id: &ChunkId) -> Result<(), ErrCode> {
    leader_call(node, &NodeOp::ReportCorrupt { target: repaired.unwrap_or(0), chunk: *id }).map(|_| ())
}
