//! chunk 版本在网络上的两种样子（design.md 3.2.4 的 packed 形式、3.3.6 的 ForwardWrite、3.4.2.4）：
//!
//! - **packed**（完整转发、NeedFull、重新驱动、同步、修复）= 固定头 ‖ 块表 ‖ 依块号顺序拼接所有 `stored_len > 0` 的块的存储字节。
//!   接收方把头和块表写在偏移 0、各块写回各自的固定偏移，得到与源逐字节相同的稀疏文件（[`pack`] / [`unpack`]）。
//! - **增量**（head 写完 pending 之后的第一次转发）：新固定头 + 改动块的 (块号, 块表条目) + 改动块的存储字节；后继在自己已提交版本的
//!   块表上换掉这些条目、核对新固定头（header_crc 覆盖新块表），没改动的块从已提交版本同偏移复制（[`incremental`]）。

use super::format::{
    BLOCK_ENTRY_LEN, BlockEntry, CHUNK_HEADER_LEN, ChunkHeader, decode_table, encode_table, table_len_of,
};
use crate::base::ErrCode;
use crate::buf::{Buf, Lane, MemPool, POOL_WAIT};

/// 读源文件的一段（pending 或已提交的版本）：返回读到的字节数，0 = 文件尾。
pub type ReadAt<'a> = &'a mut dyn FnMut(u64, &mut [u8]) -> Result<usize, ErrCode>;

fn read_full(r: ReadAt, off: u64, buf: &mut [u8]) -> Result<(), ErrCode> {
    let mut done = 0;
    while done < buf.len() {
        match r(off + done as u64, &mut buf[done..])? {
            0 => return Err(ErrCode::CORRUPT), // 比头里说的短
            n => done += n,
        }
    }
    Ok(())
}

/// 把一个版本打包（见文件头）；缓冲从复制 lane 取（3.6.1：NeedFull 与重新驱动打包用 Repl lane）。
pub fn pack(pool: &MemPool, r: ReadAt) -> Result<(Buf, ChunkHeader), ErrCode> {
    let mut fixed = [0u8; CHUNK_HEADER_LEN];
    read_full(r, 0, &mut fixed)?;
    let mut table = vec![0u8; table_len_of(&fixed)];
    read_full(r, CHUNK_HEADER_LEN as u64, &mut table)?;
    let h = ChunkHeader::decode(&fixed, &table)?;
    let entries = decode_table(&table)?;
    let data: usize = entries.iter().map(|e| e.stored_len as usize).sum();
    let head = CHUNK_HEADER_LEN + table.len();
    let mut b = pool.get(head + data, Lane::Repl, POOL_WAIT)?;
    b.set_len(head + data);
    let s = b.as_mut_slice();
    s[..CHUNK_HEADER_LEN].copy_from_slice(&fixed);
    s[CHUNK_HEADER_LEN..head].copy_from_slice(&table);
    let mut at = head;
    for (i, e) in entries.iter().enumerate().filter(|(_, e)| e.stored_len > 0) {
        if e.stored_len > h.block_size {
            return Err(ErrCode::CORRUPT);
        }
        read_full(r, h.block_offset(i as u32), &mut s[at..at + e.stored_len as usize])?;
        at += e.stored_len as usize;
    }
    Ok((b, h))
}

/// 要写进文件的 (偏移, 字节)。
pub type Writes<'a> = Vec<(u64, &'a [u8])>;

/// 解 packed：(头, 要写的 (偏移, 字节))。长度对不上、头 CRC 不符 → `EINVAL`/`CORRUPT`。
pub fn unpack(b: &[u8]) -> Result<(ChunkHeader, Writes<'_>), ErrCode> {
    let fixed: &[u8; CHUNK_HEADER_LEN] =
        b.get(..CHUNK_HEADER_LEN).and_then(|x| x.try_into().ok()).ok_or(ErrCode::EINVAL)?;
    let head = CHUNK_HEADER_LEN + table_len_of(fixed);
    let table = b.get(CHUNK_HEADER_LEN..head).ok_or(ErrCode::EINVAL)?;
    let h = ChunkHeader::decode(fixed, table)?;
    let mut writes = vec![(0, &b[..head])];
    let mut at = head;
    for (i, e) in decode_table(table)?.iter().enumerate().filter(|(_, e)| e.stored_len > 0) {
        let d = b.get(at..at + e.stored_len as usize).ok_or(ErrCode::EINVAL)?;
        writes.push((h.block_offset(i as u32), d));
        at += d.len();
    }
    if at != b.len() {
        return Err(ErrCode::EINVAL);
    }
    Ok((h, writes))
}

/// 一个增量转发落到本地之后要写的东西：头 + 新块表（写在偏移 0）、改动块（各自的固定偏移）、要从已提交版本复制的块。
pub struct Applied<'a> {
    pub header: ChunkHeader,
    pub head: Vec<u8>,
    pub blocks: Writes<'a>,
    pub copies: Vec<(u64, u64)>,
}

/// 增量转发（见文件头）：`base` = 本地已提交版本的块表（chunk 原本不存在 → 空）。
pub fn incremental<'a>(
    base: &[BlockEntry],
    header: &[u8],
    changed: &[(u32, Vec<u8>)],
    bulk: &'a [u8],
) -> Result<Applied<'a>, ErrCode> {
    let fixed: &[u8; CHUNK_HEADER_LEN] = header.try_into().map_err(|_| ErrCode::EINVAL)?;
    let nb = table_len_of(fixed) / BLOCK_ENTRY_LEN;
    let mut table = if base.is_empty() { vec![BlockEntry::default(); nb] } else { base.to_vec() };
    if table.len() != nb {
        return Err(ErrCode::EINVAL);
    }
    for (b, e) in changed {
        let e: &[u8; BLOCK_ENTRY_LEN] = e.as_slice().try_into().map_err(|_| ErrCode::EINVAL)?;
        *table.get_mut(*b as usize).ok_or(ErrCode::EINVAL)? = BlockEntry::decode(e);
    }
    let tb = encode_table(&table);
    let h = ChunkHeader::decode(fixed, &tb)?; // header_crc 覆盖新块表：换错了条目就对不上
    let (mut blocks, mut at) = (Vec::new(), 0usize);
    for (b, _) in changed {
        let n = table[*b as usize].stored_len as usize;
        if n > 0 {
            blocks.push((h.block_offset(*b), bulk.get(at..at + n).ok_or(ErrCode::EINVAL)?));
            at += n;
        }
    }
    if at != bulk.len() {
        return Err(ErrCode::EINVAL);
    }
    let copies = (0..nb as u32)
        .filter(|b| table[*b as usize].stored_len > 0 && !changed.iter().any(|(c, _)| c == b))
        .map(|b| (h.block_offset(b), table[b as usize].stored_len as u64))
        .collect();
    Ok(Applied { header: h, head: [fixed.as_slice(), &tb].concat(), blocks, copies })
}
