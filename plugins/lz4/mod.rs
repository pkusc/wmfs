//! lz4：transform 插件的压缩一类（design.md 4.8、3.2.4，altitude 731，算法编号 1）。
//!
//! 写（在校验和之后、加密之前）：`lz4_flex::block::compress_into` 把一块明文压进一个新的池缓冲；压完超过明文的 7/8（压不下去）
//! 就原样返回输入、不置 `COMPRESSED`——这一块按原文存，读的时候也不经过本插件。读：`decompress_into` 解到明文长度
//! （`plain_len`）；解不开或长度不对 → `CHECKSUM_MISMATCH`（与校验不符同样处理：标 Corrupt、换副本）。
//! 指标 `wmfs_lz4_blocks_total{result="compressed"|"stored"|"decoded"}`；fail point `plugin.lz4.transform`。

use std::sync::Arc;

use wmfs_core::api::metrics::{self, Counter};
use wmfs_core::base::ErrCode;
use wmfs_core::buf::{Buf, Lane, MemPool, POOL_WAIT};
use wmfs_core::data::format::{BLK_COMPRESSED, CMP_LZ4};
use wmfs_core::hooks::manifest::{CORE_ABI_VERSION, Hook, Manifest};
use wmfs_core::hooks::traits::{BlockMeta, TransformCtx, TransformKind, TransformPlugin};

pub static MANIFEST: Manifest = Manifest {
    name: "lz4",
    version: "0.1.0",
    hooks: &[Hook::Transform],
    altitude: 731,
    hot_path: true,
    config_schema: "",
    abi_version: CORE_ABI_VERSION,
    attrs: &["compress"],
    budget_ns: 150_000, // 3.7.4：lz4 每 64 KiB 块 150 µs
    fail_points: &["plugin.lz4.transform"],
};

pub struct Lz4 {
    compressed: Arc<Counter>,
    stored: Arc<Counter>,
    decoded: Arc<Counter>,
}

impl Lz4 {
    pub fn new() -> Lz4 {
        let c = |r| metrics::counter("wmfs_lz4_blocks_total", "lz4 处理过的块数", &[("result", r)]);
        Lz4 { compressed: c("compressed"), stored: c("stored"), decoded: c("decoded") }
    }
}

impl Default for Lz4 {
    fn default() -> Lz4 {
        Lz4::new()
    }
}

impl TransformPlugin for Lz4 {
    fn manifest(&self) -> &'static Manifest {
        &MANIFEST
    }

    fn kind(&self) -> TransformKind {
        TransformKind::Compress
    }

    fn alg_id(&self) -> u8 {
        CMP_LZ4
    }

    fn encode(&self, _ctx: &TransformCtx, input: Buf, meta: &mut BlockMeta, pool: &MemPool) -> Result<Buf, ErrCode> {
        let n = input.len();
        // lz4_flex 要求输出缓冲先有最坏情况的大小（64 KiB 的块约 72 KiB，放得进 S 档）；压完超过明文的 7/8 = 压不下去：存原文（4.8）
        let mut out = pool.get(lz4_flex::block::get_maximum_output_size(n), Lane::Normal, POOL_WAIT)?;
        match lz4_flex::block::compress_into(input.as_slice(), out.as_mut_slice()) {
            Ok(m) if n > 0 && m <= n * 7 / 8 => {
                out.set_len(m);
                meta.flags |= BLK_COMPRESSED;
                self.compressed.inc();
                Ok(out)
            }
            _ => {
                self.stored.inc();
                Ok(input)
            }
        }
    }

    fn decode(&self, _ctx: &TransformCtx, input: Buf, meta: &BlockMeta, pool: &MemPool) -> Result<Buf, ErrCode> {
        self.decoded.inc();
        let mut out = pool.get((meta.plain_len as usize).max(1), Lane::Normal, POOL_WAIT)?;
        out.set_len(meta.plain_len as usize);
        match lz4_flex::block::decompress_into(input.as_slice(), out.as_mut_slice()) {
            Ok(m) if m == meta.plain_len as usize => Ok(out),
            _ => Err(ErrCode::CHECKSUM_MISMATCH), // 解不开、长度不对：当坏块处理
        }
    }
}

#[cfg(test)]
mod tests;
