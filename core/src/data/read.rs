//! 读已提交版本（design.md 3.4.2.5、3.2.4 的读顺序、3.2.5 R5）。实现注记（M5）：单节点——本成员就是 last serving，有 pending 也
//! 照常读已提交的版本（CRAQ 的脏读重定向 `CHUNK_DIRTY` 在 M9）。
//!
//! - **打开**（[`open`]，读路径与 build.rs 的读改写共用）：经 `open_committed` 拿一个钉住这个版本的读句柄（之后的 rename 不影响它），
//!   读固定头与块表、校验 CRC 与 chunk 身份；坏了 → `CORRUPT`。**逐块解码**（[`Committed::block`]）：读存储字节 → decrypt →
//!   decompress → 校验 checksum；读不全或校验不符 → `CHECKSUM_MISMATCH`——宁可报错，也不返回错误的数据。
//! - **读**（[`read`]）：chunk 不存在 → `CHUNK_NOT_FOUND`（客户端先 GetAttr 确认 epoch 没变，再当空洞读 0，C5）；被标为 Corrupt →
//!   `CORRUPT`；否则逐块解码、复制所需区间：洞 = 0，`[plain_len, block_size)` = 0；回复的长度截到 `logical_len`（之后的部分由
//!   客户端补 0）。头或任一块校验不符 → 标 Corrupt、回 `CHECKSUM_MISMATCH`。

use super::format::{BLK_COMPRESSED, BLK_ENCRYPTED, BlockEntry, CHUNK_HEADER_LEN, ChunkHeader};
use super::format::{block_aad, decode_table, table_len_of};
use super::{DataNode, Target};
use crate::base::ErrCode;
use crate::base::types::ChunkId;
use crate::buf::{Buf, Lane, POOL_WAIT};
use crate::hooks::traits::{BlockMeta, ChunkReader, TargetStore, TransformCtx, TransformKind};

/// 一个已提交版本：头、块表、钉住这个版本的读句柄。
pub struct Committed {
    pub header: ChunkHeader,
    pub table: Vec<BlockEntry>,
    reader: Box<dyn ChunkReader>,
}

/// 从 `off` 起读满 `buf`（每次 `read_at` 经 3.7.4 的包装）；读到文件尾还不满 → `CORRUPT`（文件比头里说的短，例如掉电后
/// 只剩一部分）。
fn read_full(node: &DataNode, r: &mut dyn ChunkReader, off: u64, buf: &mut [u8]) -> Result<(), ErrCode> {
    let mut done = 0;
    while done < buf.len() {
        let n = node.store(|| r.read_at(off + done as u64, &mut buf[done..]))?;
        if n == 0 {
            return Err(ErrCode::CORRUPT);
        }
        done += n;
    }
    Ok(())
}

/// 打开 `id` 的已提交版本：不存在 → None；头读不全、CRC 不对、与 chunk 身份（ino、epoch、idx、链、集群）不符 → `CORRUPT`；
/// 不认识的版本或 incompat 位 → `FEATURE_UNSUPPORTED`。
pub fn open(node: &DataNode, store: &dyn TargetStore, id: &ChunkId) -> Result<Option<Committed>, ErrCode> {
    let Some(mut reader) = node.store(|| store.open_committed(id))? else { return Ok(None) };
    let mut fixed = [0u8; CHUNK_HEADER_LEN];
    read_full(node, &mut *reader, 0, &mut fixed)?;
    let mut table = vec![0u8; table_len_of(&fixed)];
    read_full(node, &mut *reader, CHUNK_HEADER_LEN as u64, &mut table)?;
    let header = ChunkHeader::decode(&fixed, &table)?;
    let who = (header.ino, header.epoch, header.chunk_idx, header.chain_id);
    if who != (id.ino, id.epoch, id.idx, id.chain) || header.cluster_id != node.cluster_id {
        return Err(ErrCode::CORRUPT);
    }
    Ok(Some(Committed { header, table: decode_table(&table)?, reader }))
}

/// `id` 的已提交版本号（不存在、头坏了 → None）。
pub fn committed_version(node: &DataNode, t: &Target, id: &ChunkId) -> Option<u64> {
    open(node, &*t.store, id).ok().flatten().map(|c| c.header.chunk_version)
}

impl Committed {
    /// 第 i 块的明文（`len()` = plain_len）；洞 → None。存储字节读不全、解码或校验不符 → `CHECKSUM_MISMATCH`。
    pub fn block(&mut self, node: &DataNode, id: &ChunkId, i: u32) -> Result<Option<Buf>, ErrCode> {
        let e = *self.table.get(i as usize).ok_or(ErrCode::CORRUPT)?;
        if e.stored_len == 0 {
            return Ok(None);
        }
        let bs = self.header.block_size;
        if e.stored_len > bs || e.plain_len > bs {
            return Err(ErrCode::CHECKSUM_MISMATCH);
        }
        let mut b = node.pool.get(bs as usize, Lane::Normal, POOL_WAIT)?;
        b.set_len(e.stored_len as usize);
        match read_full(node, &mut *self.reader, self.header.block_offset(i), b.as_mut_slice()) {
            Err(ErrCode::CORRUPT) => return Err(ErrCode::CHECKSUM_MISMATCH),
            r => r?,
        }
        decode(node, &self.header, id, i, &e, b).map(Some)
    }
}

/// 解码一块：decrypt → decompress → 校验 checksum（只做块表标明用过的那几步，3.2.4 的读顺序）。
fn decode(node: &DataNode, h: &ChunkHeader, id: &ChunkId, i: u32, e: &BlockEntry, b: Buf) -> Result<Buf, ErrCode> {
    let meta =
        BlockMeta { checksum: e.checksum, flags: e.flags, nonce: e.nonce, tag: e.gcm_tag, plain_len: e.plain_len };
    let aad = block_aad(&h.cluster_id, id.ino, id.epoch, id.idx, i);
    let ctx = TransformCtx {
        chunk: id,
        block_idx: i,
        level: h.compress_level,
        key_id: h.encrypt_key_id,
        key: None,
        aad: &aad,
    };
    let pool = &node.pool;
    let mut b = b;
    if e.flags & BLK_ENCRYPTED != 0 {
        b = node.transform(TransformKind::Encrypt, h.encrypt_alg, |p| p.decode(&ctx, b, &meta, pool))?;
    }
    if e.flags & BLK_COMPRESSED != 0 {
        b = node.transform(TransformKind::Compress, h.compress_alg, |p| p.decode(&ctx, b, &meta, pool))?;
    }
    let b = node.transform(TransformKind::Checksum, h.checksum_alg, |p| p.decode(&ctx, b, &meta, pool))?;
    if b.len() != e.plain_len as usize {
        return Err(ErrCode::CHECKSUM_MISMATCH);
    }
    Ok(b)
}

/// 读 `[off, off + len)`：返回 (缓冲, 实际长度, 已提交版本号)。
pub fn read(node: &DataNode, t: &Target, id: &ChunkId, off: u32, len: u32) -> Result<(Buf, u32, u64), ErrCode> {
    if node.chunks.is_corrupt(id) {
        return Err(ErrCode::CORRUPT);
    }
    let mut c = match open(node, &*t.store, id) {
        Ok(Some(c)) => c,
        Ok(None) => return Err(ErrCode::CHUNK_NOT_FOUND),
        Err(e) => {
            if e == ErrCode::CORRUPT {
                node.report_corrupt(t, id, e);
            }
            return Err(e);
        }
    };
    let h = c.header;
    let end_req = off.checked_add(len).filter(|e| *e <= h.chunk_size).ok_or(ErrCode::EINVAL)?;
    let end = end_req.min(h.logical_len);
    let n = end.saturating_sub(off);
    let mut out = node.pool.get(n.max(1) as usize, Lane::Normal, POOL_WAIT)?;
    out.set_len(n as usize);
    let bs = h.block_size;
    let mut pos = off;
    while pos < end {
        let b = pos / bs;
        let (a, z) = (b * bs, (b * bs + bs).min(end));
        let dst = &mut out.as_mut_slice()[(pos - off) as usize..(z - off) as usize];
        dst.fill(0);
        match c.block(node, id, b) {
            Ok(Some(plain)) => {
                let p = plain.as_slice();
                let (s, e) = (((pos - a) as usize).min(p.len()), ((z - a) as usize).min(p.len()));
                dst[..e - s].copy_from_slice(&p[s..e]);
            }
            Ok(None) => {} // 洞：读为 0
            Err(e) => {
                if e == ErrCode::CHECKSUM_MISMATCH {
                    node.report_corrupt(t, id, e);
                }
                return Err(e);
            }
        }
        pos = z;
    }
    t.read_bytes.add(n as u64);
    Ok((out, n, h.chunk_version))
}
