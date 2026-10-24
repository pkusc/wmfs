//! 构建 chunk 的新版本（design.md 3.2.4、3.2.5 R1、3.4.2.3 第 3 步、3.4.2.9）。已提交版本的打开与逐块解码在 read.rs（读改写用它）。
//!
//! - **新版本**（[`build`]，按顺序调几个小函数）：写（[`plan_write`]）= 涉及的每个块：整块覆盖 → 直接用新数据；部分覆盖 → 先解码
//!   已提交的块再打补丁（读改写）；`logical_len = max(旧值, 最大 extent 末尾)`。截断到 `new_len` = 之后的块变成洞，边界块重新编码
//!   （[`cut_boundary`]：之后的部分丢掉，读为 0），`logical_len = min(旧值, new_len)`，从不删除 chunk 文件（版本单调，3.4.2.9）。
//!   改动块按 checksum → compress → encrypt 编码（[`encode_blocks`]），块表条目随之更新；没改动、有数据的块原样从已提交版本同偏移
//!   复制（`copies`）。块的明文长度 `plain_len` 可以小于块大小，`[plain_len, block_size)` 读为 0（与洞一样），所以扩展长度不必重写
//!   前面的块。
//! - **Corrupt 的 chunk**（3.2.5 R5）：不保留任何旧块的写（每个有数据的旧块都被整块覆盖、截断到 0）照常执行；要保留旧块的写在
//!   单副本上修不了 → `EIO`（链上有其他 Serving 成员时先修复，M9/M11）。头坏到读不出块表时连旧块在哪都不知道：只接受覆盖整个
//!   chunk 的写与截断到 0。新版本没有从旧版本原样复制任何块（`copies` 为空）时，write.rs 提交之后清除 Corrupt 标记。

use super::DataNode;
use super::format::{
    BLK_COMPRESSED, BLK_ENCRYPTED, BlockEntry, CHUNK_HEADER_LEN, CMP_NONE, ChunkHeader, ENC_NONE,
    INCOMPAT_BLOCK_COMPRESS, INCOMPAT_BLOCK_ENCRYPT, block_aad, encode_table, layout_for,
};
use super::read::Committed;
use crate::base::types::ChunkId;
use crate::base::{ErrCode, clock};
use crate::buf::{Buf, BufSlice, Lane, POOL_WAIT};
use crate::hooks::traits::{BlockMeta, TransformCtx, TransformKind};
use crate::proto::cap::CapV1;

/// 写：checksum(明文) → compress → encrypt（3.2.4 的写顺序），得到存储字节与块表条目。
fn encode(node: &DataNode, cap: &CapV1, id: &ChunkId, i: u32, b: Buf) -> Result<(Buf, BlockEntry), ErrCode> {
    let mut meta = BlockMeta { plain_len: b.len() as u32, ..Default::default() };
    let aad = block_aad(&node.cluster_id, id.ino, id.epoch, id.idx, i);
    let ctx = TransformCtx {
        chunk: id,
        block_idx: i,
        level: cap.compress_level,
        key_id: cap.encrypt_key,
        key: None,
        aad: &aad,
    };
    let pool = &node.pool;
    let mut b = node.transform(TransformKind::Checksum, cap.checksum, |p| p.encode(&ctx, b, &mut meta, pool))?;
    if cap.compress != CMP_NONE {
        b = node.transform(TransformKind::Compress, cap.compress, |p| p.encode(&ctx, b, &mut meta, pool))?;
    }
    if cap.encrypt != ENC_NONE {
        b = node.transform(TransformKind::Encrypt, cap.encrypt, |p| p.encode(&ctx, b, &mut meta, pool))?;
    }
    let e = BlockEntry {
        stored_len: b.len() as u32,
        plain_len: meta.plain_len,
        flags: meta.flags,
        nonce: meta.nonce,
        gcm_tag: meta.tag,
        checksum: meta.checksum,
    };
    Ok((b, e))
}

/// 这次对 chunk 改什么。
pub enum ChunkEdit<'a> {
    Write { extents: &'a [(u32, u32)], data: &'a [u8] }, // data = 各 extent 依次拼接
    Truncate { new_len: u32 },
}

/// 一个改动块的存储字节放在哪：(pending 文件里的偏移, `out` 缓冲里的偏移, 长度)。
type Placed = (u64, usize, usize);

/// 一个新版本：写进 pending 文件的内容（头 + 块表在偏移 0，各改动块的存储字节在各自的固定偏移）与要原样复制的区间。
/// 实现注记（M9）：另记着改动了哪些块表条目（重新编码的块按 `out` 里的顺序，之后是变成洞的块），增量转发给后继用（`packed.rs`）。
pub struct NewVersion {
    head: Vec<u8>,
    out: Option<Buf>,            // 改动块的存储字节依次放在这里
    blocks: Vec<Placed>,         // 各改动块
    pub copies: Vec<(u64, u64)>, // (偏移, 长度)：从已提交版本同偏移复制
    changed: Vec<u32>,           // 改动了块表条目的块（重新编码的在前，变成洞的在后）
    table: Vec<BlockEntry>,
}

impl NewVersion {
    /// 增量转发的固定头（含覆盖新块表的 header_crc）与改动块的 (块号, 72 字节块表条目)（3.3.6 的 ForwardWrite）。
    pub fn forward_parts(&self) -> (Vec<u8>, Vec<(u32, Vec<u8>)>) {
        let blocks = self.changed.iter().map(|b| (*b, self.table[*b as usize].encode().to_vec())).collect();
        (self.head[..CHUNK_HEADER_LEN].to_vec(), blocks)
    }

    /// 改动块的存储字节（增量转发的 bulk，按 `forward_parts` 的顺序）：冻结缓冲、给出切片，不拷贝（3.6.3）。
    pub fn into_bulk(self) -> Vec<BufSlice> {
        let n: usize = self.blocks.iter().map(|(_, _, n)| *n).sum();
        self.out.filter(|_| n > 0).map(|b| vec![b.freeze().slice(0, n as u32)]).unwrap_or_default()
    }

    /// `write_pending` 的 writes。
    pub fn writes(&self) -> Vec<(u64, &[u8])> {
        let out = self.out.as_ref().map_or(&[][..], |b| b.as_slice());
        let blocks = self.blocks.iter().map(|(f, o, n)| (*f, &out[*o..*o + *n]));
        std::iter::once((0, self.head.as_slice())).chain(blocks).collect()
    }

    /// 这次真正写下去的字节数（指标）。
    pub fn bytes(&self) -> u64 {
        self.head.len() as u64 + self.blocks.iter().map(|(_, _, n)| *n as u64).sum::<u64>()
    }
}

/// `extents` 与 `[a, z)` 的交集（按 extent 的先后，带它在 data 里的起点）。
fn clipped(extents: &[(u32, u32)], a: u32, z: u32) -> Vec<(u32, u32, usize)> {
    let mut cur = 0usize;
    let mut out = Vec::new();
    for &(off, len) in extents {
        let (s, e) = (off.max(a), (off + len).min(z));
        if s < e {
            out.push((s, e, cur + (s - off) as usize));
        }
        cur += len as usize;
    }
    out
}

/// `extents` 是否把 `[a, z)` 整个盖住。
fn covers(extents: &[(u32, u32)], a: u32, z: u32) -> bool {
    let mut iv: Vec<(u32, u32)> = clipped(extents, a, z).into_iter().map(|(s, e, _)| (s, e)).collect();
    iv.sort();
    let mut pos = a;
    for (s, e) in iv {
        if s > pos {
            return false;
        }
        pos = pos.max(e);
    }
    pos >= z
}

/// 各段的长度之和必须正好是 bulk 的长度、每段都不越过 chunk_size（否则 `EINVAL`）；返回非空各段的最大末尾。
fn check_extents(extents: &[(u32, u32)], data_len: usize, chunk_size: u32) -> Result<u32, ErrCode> {
    if extents.iter().map(|(_, l)| *l as u64).sum::<u64>() != data_len as u64 {
        return Err(ErrCode::EINVAL);
    }
    let mut end = 0;
    for &(off, len) in extents.iter().filter(|(_, l)| *l > 0) {
        end = end.max(off.checked_add(len).filter(|e| *e <= chunk_size).ok_or(ErrCode::EINVAL)?);
    }
    Ok(end)
}

/// 构建新版本 `version`（见文件头）。`old` = 已提交版本（None = chunk 原本不存在，或头坏了而 `corrupt` 为真）。
/// 要保留的旧块解码失败 → `CHECKSUM_MISMATCH`（调用方标 Corrupt、回 `EIO`）。
pub fn build(
    node: &DataNode,
    cap: &CapV1,
    id: &ChunkId,
    old: Option<&mut Committed>,
    corrupt: bool,
    version: u64,
    edit: ChunkEdit,
) -> Result<NewVersion, ErrCode> {
    let (bs, nb, h4k) = layout_for(cap.chunk_size)?;
    if cap.block_size != bs {
        return Err(ErrCode::EINVAL);
    }
    let (mut table, old_len) = match &old {
        Some(o) => {
            let h = &o.header;
            let same = (h.chunk_size, h.checksum_alg, h.compress_alg, h.encrypt_alg)
                == (cap.chunk_size, cap.checksum, cap.compress, cap.encrypt);
            if !same {
                return Err(ErrCode::EINVAL); // 同一 (ino, epoch) 的变换参数必然相同（3.2.4）：cap 与这个 chunk 对不上
            }
            (o.table.clone(), h.logical_len)
        }
        None => (vec![BlockEntry::default(); nb as usize], 0),
    };
    let base = table.clone();
    let unknown = corrupt && old.is_none(); // 头坏了：不知道旧块在哪
    let (plains, new_len) = match edit {
        ChunkEdit::Write { extents, data } => {
            let end = check_extents(extents, data.len(), cap.chunk_size)?;
            if unknown && !covers(extents, 0, cap.chunk_size) {
                return Err(ErrCode::EIO); // 头坏了：只接受覆盖整个 chunk 的写
            }
            let kept = |b: u32| table[b as usize].stored_len > 0 && !covers(extents, b * bs, b * bs + bs);
            if corrupt && (0..nb).any(kept) {
                return Err(ErrCode::EIO); // R5：有数据的旧块没被整块覆盖（要保留它），单副本修不了
            }
            (plan_write(node, id, old, &table, extents, data, bs)?, old_len.max(end))
        }
        ChunkEdit::Truncate { new_len } => {
            if new_len > cap.chunk_size {
                return Err(ErrCode::EINVAL);
            }
            if unknown && new_len > 0 {
                return Err(ErrCode::EIO); // 头坏了：只接受截断到 0
            }
            for e in table.iter_mut().skip(new_len.div_ceil(bs) as usize) {
                *e = BlockEntry::default(); // 整块在 new_len 之后：变成洞
            }
            if corrupt && table.iter().any(|e| e.stored_len > 0) {
                return Err(ErrCode::EIO); // R5：截断之后还要保留旧块
            }
            (cut_boundary(node, id, old, &table, new_len, bs)?, old_len.min(new_len))
        }
    };
    let mut changed: Vec<u32> = plains.iter().map(|(b, _)| *b).collect();
    let mut header = new_header(node, cap, id, h4k, nb, version, new_len);
    let (out, blocks) = encode_blocks(node, cap, id, &header, &mut table, plains)?;
    let copies = (0..nb)
        .filter(|b| table[*b as usize].stored_len > 0 && !changed.contains(b))
        .map(|b| (header.block_offset(b), table[b as usize].stored_len as u64))
        .collect();
    let holed: Vec<u32> = (0..nb).filter(|b| table[*b as usize] != base[*b as usize] && !changed.contains(b)).collect();
    changed.extend(holed);
    header.features_incompat = incompat_of(&table);
    let tb = encode_table(&table);
    let mut head = header.encode(&tb).to_vec();
    head.extend_from_slice(&tb);
    Ok(NewVersion { head, out, blocks, copies, changed, table })
}

/// 写：涉及的每个块的新明文（块号, 明文）。整块覆盖 → 直接用新数据；部分覆盖一个有数据的旧块 → 先解码它再打补丁（读改写）。
fn plan_write(
    node: &DataNode,
    id: &ChunkId,
    mut old: Option<&mut Committed>,
    table: &[BlockEntry],
    extents: &[(u32, u32)],
    data: &[u8],
    bs: u32,
) -> Result<Vec<(u32, Buf)>, ErrCode> {
    let mut touched: Vec<u32> = Vec::new();
    for &(off, len) in extents.iter().filter(|(_, l)| *l > 0) {
        touched.extend(off / bs..=(off + len - 1) / bs);
    }
    touched.sort();
    touched.dedup();
    let mut plains = Vec::new();
    for b in touched {
        let (a, z) = (b * bs, b * bs + bs);
        let keep = table[b as usize].stored_len > 0 && !covers(extents, a, z); // 这块的旧数据要保留
        let parts = clipped(extents, a, z);
        let mut plen = if keep { table[b as usize].plain_len } else { 0 };
        plen = parts.iter().fold(plen, |p, (_, e, _)| p.max(e - a));
        let mut buf = node.pool.get(bs as usize, Lane::Normal, POOL_WAIT)?;
        buf.set_len(plen as usize);
        buf.as_mut_slice().fill(0);
        // 有数据的旧块一定来自已提交版本（old 为 None 时块表全是洞）
        if keep
            && let Some(o) = old.as_deref_mut()
            && let Some(prev) = o.block(node, id, b)?
        {
            buf.as_mut_slice()[..prev.len()].copy_from_slice(prev.as_slice());
        }
        for (s, e, at) in parts {
            buf.as_mut_slice()[(s - a) as usize..(e - a) as usize].copy_from_slice(&data[at..at + (e - s) as usize]);
        }
        plains.push((b, buf));
    }
    Ok(plains)
}

/// 截断：`new_len` 落在某块中间、而这块的明文比 `new_len` 长 → 重新编码这一块（只留 `new_len` 之前的部分）；否则什么也不用改。
fn cut_boundary(
    node: &DataNode,
    id: &ChunkId,
    old: Option<&mut Committed>,
    table: &[BlockEntry],
    new_len: u32,
    bs: u32,
) -> Result<Vec<(u32, Buf)>, ErrCode> {
    let (b0, keep) = (new_len / bs, new_len % bs);
    if keep == 0 || table[b0 as usize].plain_len <= keep {
        return Ok(vec![]);
    }
    let prev = match old {
        Some(o) => o.block(node, id, b0)?,
        None => None,
    };
    let prev = prev.ok_or(ErrCode::CHECKSUM_MISMATCH)?; // 块表说这块有数据，却读不出来：当坏块处理
    let mut buf = node.pool.get(bs as usize, Lane::Normal, POOL_WAIT)?;
    buf.set_len(keep as usize);
    buf.as_mut_slice().copy_from_slice(&prev.as_slice()[..keep as usize]);
    Ok(vec![(b0, buf)])
}

/// 新版本的固定头（features_incompat 先填 0，块编码完再按块表补上）。
fn new_header(node: &DataNode, cap: &CapV1, id: &ChunkId, h4k: u16, nb: u32, version: u64, len: u32) -> ChunkHeader {
    ChunkHeader {
        header_len_4k: h4k,
        features_compat: 0,
        features_ro_compat: 0,
        features_incompat: 0,
        ino: id.ino,
        epoch: id.epoch,
        chunk_idx: id.idx,
        chain_id: id.chain,
        block_size: cap.block_size,
        chunk_version: version,
        chunk_size: cap.chunk_size,
        logical_len: len,
        nblocks: nb,
        checksum_alg: cap.checksum,
        compress_alg: cap.compress,
        compress_level: cap.compress_level,
        encrypt_alg: cap.encrypt,
        encrypt_key_id: cap.encrypt_key,
        snap_epoch: 0,
        cluster_id: node.cluster_id,
        write_time_ms: clock::wall_ms(),
    }
}

/// 逐块编码新明文，存储字节依次放进一个缓冲，块表条目随之更新；返回 (缓冲, 各块放在哪)。
fn encode_blocks(
    node: &DataNode,
    cap: &CapV1,
    id: &ChunkId,
    header: &ChunkHeader,
    table: &mut [BlockEntry],
    plains: Vec<(u32, Buf)>,
) -> Result<(Option<Buf>, Vec<Placed>), ErrCode> {
    let mut out = match plains.len() {
        0 => None,
        n => Some(node.pool.get(n * header.block_size as usize, Lane::Normal, POOL_WAIT)?),
    };
    let (mut blocks, mut cur) = (Vec::new(), 0usize);
    for (b, plain) in plains {
        let (stored, e) = encode(node, cap, id, b, plain)?;
        let n = stored.len();
        let o = out.as_mut().filter(|o| cur + n <= o.len()).ok_or(ErrCode::INTERNAL)?; // 变换不会让一块变得比块还大
        o.as_mut_slice()[cur..cur + n].copy_from_slice(stored.as_slice());
        blocks.push((header.block_offset(b), cur, n));
        table[b as usize] = e;
        cur += n;
    }
    Ok((out, blocks))
}

/// 块表里有压缩 / 加密过的块，就置对应的 incompat 位（3.2.4）。
fn incompat_of(table: &[BlockEntry]) -> u32 {
    let mut bits = 0;
    for e in table {
        if e.flags & BLK_COMPRESSED != 0 {
            bits |= INCOMPAT_BLOCK_COMPRESS;
        }
        if e.flags & BLK_ENCRYPTED != 0 {
            bits |= INCOMPAT_BLOCK_ENCRYPT;
        }
    }
    bits
}
