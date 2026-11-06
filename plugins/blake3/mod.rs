//! blake3：transform 插件的校验和一类（design.md 4.8、3.2.4，altitude 703，算法编号 3）。
//!
//! 写：对每个块的**明文**算 blake3-256，32 字节全部写进块表条目的 `checksum`，原样返回同一个缓冲（零拷贝）；读：重新算一遍，
//! 对不上 → `CHECKSUM_MISMATCH`。指标 `wmfs_blake3_blocks_total{dir}`、`wmfs_blake3_mismatches_total`；fail point
//! `plugin.blake3.transform`（本体包在每次调用外层）。

use std::sync::Arc;

use wmfs_core::api::metrics::{self, Counter};
use wmfs_core::base::ErrCode;
use wmfs_core::buf::{Buf, MemPool};
use wmfs_core::data::format::CK_BLAKE3;
use wmfs_core::hooks::manifest::{CORE_ABI_VERSION, Hook, Manifest};
use wmfs_core::hooks::traits::{BlockMeta, TransformCtx, TransformKind, TransformPlugin};

pub static MANIFEST: Manifest = Manifest {
    name: "blake3",
    version: "0.1.0",
    hooks: &[Hook::Transform],
    altitude: 703,
    hot_path: true,
    config_schema: "",
    abi_version: CORE_ABI_VERSION,
    attrs: &["checksum"],
    budget_ns: 60_000, // 4.8 的表：blake3 每 64 KiB 块 60 µs
    fail_points: &["plugin.blake3.transform"],
};

pub struct Blake3 {
    encoded: Arc<Counter>,
    decoded: Arc<Counter>,
    mismatches: Arc<Counter>,
}

impl Blake3 {
    pub fn new() -> Blake3 {
        let blocks = |d| metrics::counter("wmfs_blake3_blocks_total", "blake3 算过的块数", &[("dir", d)]);
        Blake3 {
            encoded: blocks("encode"),
            decoded: blocks("decode"),
            mismatches: metrics::counter("wmfs_blake3_mismatches_total", "blake3 校验不符的块数", &[]),
        }
    }
}

impl Default for Blake3 {
    fn default() -> Blake3 {
        Blake3::new()
    }
}

impl TransformPlugin for Blake3 {
    fn manifest(&self) -> &'static Manifest {
        &MANIFEST
    }

    fn kind(&self) -> TransformKind {
        TransformKind::Checksum
    }

    fn alg_id(&self) -> u8 {
        CK_BLAKE3
    }

    fn encode(&self, _ctx: &TransformCtx, input: Buf, meta: &mut BlockMeta, _pool: &MemPool) -> Result<Buf, ErrCode> {
        meta.checksum = *blake3::hash(input.as_slice()).as_bytes();
        self.encoded.inc();
        Ok(input)
    }

    fn decode(&self, _ctx: &TransformCtx, input: Buf, meta: &BlockMeta, _pool: &MemPool) -> Result<Buf, ErrCode> {
        self.decoded.inc();
        if meta.checksum != *blake3::hash(input.as_slice()).as_bytes() {
            self.mismatches.inc();
            return Err(ErrCode::CHECKSUM_MISMATCH);
        }
        Ok(input)
    }
}

#[cfg(test)]
mod tests;
