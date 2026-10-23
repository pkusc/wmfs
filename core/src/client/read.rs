//! 读路径（design.md 8.3 末尾）：本 inode 的写回缓冲（读自己的写）→ inline 内容 → cache_store（键带 change_id）→ 数据节点
//! （有 cache_store 时按 1 MiB 对齐读、放进 cache_store）。结果截断到 max(元数据长度, 本地脏区末尾)。
//!
//! - 副本选择、`CHUNK_DIRTY` 改问 last serving、坏副本换下一个、`STALE_CHAIN` 拉新集群图都在 DataClient 里；这里处理
//!   `CHUNK_NOT_FOUND`（C5：先 `GetAttr` 确认 epoch 没变，才把这一段当空洞读 0；epoch 变了 → 重新 Open 取新 layout 再读）与
//!   `BAD_CAP`（经 `FileClient::with_cap`：续一次 cap 再试，再失败 → `EACCES`）。
//! - 本地还没提交的数据在读之前先拷一份（在途的、脏区的），读完数据节点再盖上去：这期间后台写线程提交了、放掉了那份数据也不要紧。
//! - inline 文件：`[len(INLINE), size)` 读为 0（3.2.7 的不变式 `len(INLINE) ≤ size`；例如 `truncate -s 100K` 一个小文件之后）。
//!   手上的 inline 副本不是当前版本的（只写打开、别人改过，`inode.rs`）→ 读方式的 Open 取回最新的属性与内容再读（照常检查读权限）。
//! - direct 句柄（strong / `O_DIRECT`）与 `--cache none` 不进 cache_store，只读请求的那一段。cache_store 出错只是绕过它（写一行
//!   debug 日志），读照常从数据节点读。实现注记（M6）：块不跨 chunk——chunk 小于 1 MiB（第 5 节允许 64K 起）时一块就是一个 chunk，
//!   块号按块大小数（键里还有 change_id，同一个版本的块大小不变）。

use super::data_client::chunk_id;
use super::files::{FileClient, Handle, Reopen, expect_open_out};
use super::inode::Inode;
use super::meta_client::Caller;
use super::{CLIENT_POOL_WAIT, STALE_RETRIES};
use crate::base::{ErrCode, lock, log};
use crate::buf::Lane;
use crate::hooks::traits::CacheKey;
use crate::meta::schema::records::{F_INLINE, LayoutV1};
use crate::proto::meta::{MetaOp, MetaReply, O_RDONLY};

/// cache_store 的块大小（4.11：1 MiB；chunk 更小时取 chunk 大小）。
const BLOCK: u64 = 1 << 20;

/// 读之前从 InodeState 拍下的快照。
struct Snap {
    size: u64,     // max(元数据长度, 本地末尾)
    meta_end: u64, // 元数据长度：之后的部分只来自本地
    change_id: u64,
    layout: LayoutV1,
    inline_file: bool,
    inline: Option<Vec<u8>>, // inline 文件当前版本的内容（不知道 → None）
    local: Vec<u8>,          // 本地还没提交的数据盖在 [off, off + len) 上的样子
    mask: Vec<bool>,         // 哪些字节有本地数据
}

impl FileClient {
    /// 读 `[off, off + size)`（见文件头）；读到文件尾时返回的比 `size` 短。
    pub fn read(&self, who: &Caller, h: &Handle, off: u64, size: u32) -> Result<Vec<u8>, ErrCode> {
        let inode = self.inode(h.ino).ok_or(ErrCode::EBADF)?;
        for _ in 0..STALE_RETRIES {
            let s = snap(&inode, off, size)?;
            if off >= s.size {
                return Ok(vec![]);
            }
            let len = (size as u64).min(s.size - off) as usize;
            let mut out = vec![0u8; len];
            let committed = s.meta_end.saturating_sub(off).min(len as u64) as usize;
            if committed > 0 && s.inline_file && s.inline.is_none() {
                self.refetch_inline(&inode, who)?;
                continue;
            }
            let r = if committed == 0 {
                Ok(()) // 全在元数据长度之后：只有本地还没提交的数据
            } else if let Some(d) = &s.inline {
                copy_inline(d, off, &mut out[..committed]);
                Ok(())
            } else {
                self.read_chunks(&inode, who, &s, off, &mut out[..committed], h.direct)
            };
            match r {
                Err(ErrCode::STALE_LAYOUT) => {
                    self.stale(&inode, who, Reopen::Read)?;
                    continue;
                }
                r => r?,
            }
            for (i, b) in out.iter_mut().enumerate() {
                if s.mask[i] {
                    *b = s.local[i];
                }
            }
            return Ok(out);
        }
        Err(ErrCode::ESTALE)
    }

    /// 手上的 inline 副本不是当前版本的：读方式的 Open 取回最新的属性与内容（照常检查读权限），只换上、不前进"见过的 change_id"。
    fn refetch_inline(&self, inode: &Inode, who: &Caller) -> Result<(), ErrCode> {
        let o = expect_open_out(self.meta.read(who, MetaOp::Open { ino: inode.ino, flags: O_RDONLY }))?;
        lock(&inode.st).install(&o);
        Ok(())
    }

    /// 已提交的部分：有 cache_store 时按块（先问它，没有再问数据节点并放进去），否则按 chunk 直接读这一段。
    fn read_chunks(
        &self,
        inode: &Inode,
        who: &Caller,
        s: &Snap,
        off: u64,
        out: &mut [u8],
        direct: bool,
    ) -> Result<(), ErrCode> {
        let cache = self.cache.as_ref().filter(|_| !direct);
        let cs = s.layout.chunk_size as u64;
        let step = if cache.is_some() { BLOCK.min(cs) } else { cs }; // 没有 cache_store 时一块就是一个 chunk
        let end = off + out.len() as u64;
        let mut pos = off;
        while pos < end {
            let (lo, hi) = block_of(pos, cs, step);
            let piece_end = hi.min(end);
            let dst = &mut out[(pos - off) as usize..(piece_end - off) as usize];
            match cache {
                Some((c, hc)) => {
                    let key = CacheKey { ino: inode.ino, change_id: s.change_id, block: lo / step };
                    let mut blk = vec![0u8; (hi - lo) as usize];
                    let hit = match hc.call(|| Ok(c.get(&key, &mut blk))) {
                        Ok(h) => h,
                        Err(e) => {
                            log::debug("client", format_args!("cache_store 出错（{e}），绕过缓存"));
                            None
                        }
                    };
                    if hit.is_none() {
                        let n = self.fetch(inode, who, s, lo, &mut blk)?;
                        let put = hc.call(|| {
                            c.put(&key, &blk[..n]);
                            Ok(())
                        });
                        if let Err(e) = put {
                            log::debug("client", format_args!("cache_store 出错（{e}），绕过缓存"));
                        }
                    }
                    dst.copy_from_slice(&blk[(pos - lo) as usize..(piece_end - lo) as usize]);
                }
                None => {
                    self.fetch(inode, who, s, pos, dst)?;
                }
            }
            pos = piece_end;
        }
        Ok(())
    }

    /// 从数据节点读文件内 `[at, at + into.len())`（不跨 chunk）进 `into`，元数据长度之后与 chunk 的 logical_len 之后补 0；
    /// 返回实际读到的字节数。
    fn fetch(&self, inode: &Inode, who: &Caller, s: &Snap, at: u64, into: &mut [u8]) -> Result<usize, ErrCode> {
        let cs = s.layout.chunk_size as u64;
        let idx = u32::try_from(at / cs).map_err(|_| ErrCode::EFBIG)?;
        let want = (into.len() as u64).min(s.meta_end.saturating_sub(at)) as u32;
        if want == 0 {
            into.fill(0);
            return Ok(0);
        }
        let chunk = chunk_id(&s.layout, inode.ino, idx)?;
        let got = self.with_cap(inode, who, false, |cap| {
            let buf = self.data.pool().get(want as usize, Lane::Normal, CLIENT_POOL_WAIT)?;
            self.data.read(cap, &s.layout, chunk, (at % cs) as u32, want, buf, None)
        });
        let n = match got {
            Ok((b, k)) => {
                into[..k as usize].copy_from_slice(&b.as_slice()[..k as usize]);
                k as usize
            }
            Err(ErrCode::CHUNK_NOT_FOUND) => {
                // C5：先确认 epoch 没变（没被截断到 0 / 迁移），才把它当空洞；变了 → 让调用方重新 Open 再读
                let MetaReply::Entry { attr } = self.meta.read(who, MetaOp::GetAttr { ino: inode.ino })? else {
                    return Err(ErrCode::INTERNAL);
                };
                if attr.epoch != s.layout.epoch {
                    return Err(ErrCode::STALE_LAYOUT);
                }
                0
            }
            Err(e) => return Err(e),
        };
        into[n..].fill(0);
        Ok(n)
    }
}

/// `pos` 所在的块 `[lo, hi)`：块大小 `step`，不跨 chunk（`cs` 是 chunk 大小）。
fn block_of(pos: u64, cs: u64, step: u64) -> (u64, u64) {
    let lo = pos / step * step;
    let chunk_end = (pos / cs + 1) * cs;
    (lo, (lo + step).min(chunk_end))
}

/// inline 文件已提交的部分（`out` 已经是 0）：从内容里拷 `[off, off + out.len())` 落在内容之内的那段，其余（`[len(INLINE), size)`）
/// 留 0。偏移可以在内容之后（文件被截长、fallocate 过）。
fn copy_inline(d: &[u8], off: u64, out: &mut [u8]) {
    let start = usize::try_from(off).unwrap_or(usize::MAX).min(d.len());
    let n = (d.len() - start).min(out.len());
    out[..n].copy_from_slice(&d[start..start + n]);
}

/// 读之前拍快照：长度、layout、inline 内容，以及本地还没提交的数据（在途的与脏区的）盖在这一段上的样子。
fn snap(inode: &Inode, off: u64, size: u32) -> Result<Snap, ErrCode> {
    let st = lock(&inode.st);
    let layout = st.layout.clone().ok_or(ErrCode::EINVAL)?;
    let size_all = st.attr.size.max(st.dirty.local_end(layout.chunk_size));
    let len = (size as u64).min(size_all.saturating_sub(off)) as usize;
    let (mut local, mut mask) = (vec![0u8; len], vec![false; len]);
    st.dirty.overlay(layout.chunk_size, off, &mut local, &mut mask);
    let inline_file = st.attr.flags & F_INLINE != 0;
    let inline = if inline_file { st.inline_now().cloned() } else { None };
    let (meta_end, change_id) = (st.attr.size, st.attr.change_id);
    Ok(Snap { size: size_all, meta_end, change_id, layout, inline_file, inline, local, mask })
}

#[cfg(test)]
mod tests {
    use super::{BLOCK, block_of, copy_inline};

    #[test]
    fn cache_blocks_never_cross_a_chunk() {
        let (k64, m4) = (64u64 << 10, 4u64 << 20);
        let step = BLOCK.min(k64); // chunk 64 KiB：一块就是一个 chunk（不能是从前一个 chunk 开始的 1 MiB）
        assert_eq!(block_of(100_000, k64, step), (65_536, 131_072));
        assert_eq!(block_of(0, k64, step), (0, 65_536));
        assert_eq!(block_of(5_000_000, m4, BLOCK.min(m4)), (4 << 20, 5 << 20), "4 MiB 的 chunk：1 MiB 的块");
        assert_eq!(block_of(5_000_000, m4, m4), (4 << 20, 8 << 20), "没有 cache_store：按 chunk");
    }

    #[test]
    fn inline_reads_past_the_content_are_zeros() {
        let d = b"abc";
        let mut out = vec![0u8; 8];
        copy_inline(d, 1, &mut out);
        assert_eq!(&out, b"bc\0\0\0\0\0\0", "内容之后读为 0");
        let mut out = vec![0u8; 4096];
        copy_inline(d, 16384, &mut out); // 偏移在内容之后（truncate -s 100K 之后读第 4 页）：全是 0，不越界
        assert!(out.iter().all(|b| *b == 0));
        let mut out = vec![0u8; 0];
        copy_inline(d, 3, &mut out);
    }
}
