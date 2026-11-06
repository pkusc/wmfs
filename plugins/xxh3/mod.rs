//! xxh3：transform 插件的校验和一类（design.md 4.8、3.2.4，altitude 702，算法编号 2）。
//!
//! 写：对每个块的**明文**算 xxh3-64，小端写进块表条目 `checksum` 的前 8 字节，原样返回同一个缓冲（零拷贝）；读：重新算一遍，
//! 对不上 → `CHECKSUM_MISMATCH`（本体把 chunk 标为 Corrupt，客户端换副本）。与 crc32c 只差用哪个哈希。
//! 指标 `wmfs_xxh3_blocks_total{dir}`、`wmfs_xxh3_mismatches_total`；fail point `plugin.xxh3.transform`（本体包在每次调用外层）。

use std::sync::Arc;

use wmfs_core::api::metrics::{self, Counter};
use wmfs_core::base::ErrCode;
use wmfs_core::buf::{Buf, MemPool};
use wmfs_core::data::format::CK_XXH3;
use wmfs_core::hooks::manifest::{CORE_ABI_VERSION, Hook, Manifest};
use wmfs_core::hooks::traits::{BlockMeta, TransformCtx, TransformKind, TransformPlugin};

pub static MANIFEST: Manifest = Manifest {
    name: "xxh3",
    version: "0.1.0",
    hooks: &[Hook::Transform],
    altitude: 702,
    hot_path: true,
    config_schema: "",
    abi_version: CORE_ABI_VERSION,
    attrs: &["checksum"],
    budget_ns: 20_000, // 3.7.4：checksum 每 64 KiB 块 20 µs
    fail_points: &["plugin.xxh3.transform"],
};

pub struct Xxh3 {
    encoded: Arc<Counter>,
    decoded: Arc<Counter>,
    mismatches: Arc<Counter>,
}

impl Xxh3 {
    pub fn new() -> Xxh3 {
        let blocks = |d| metrics::counter("wmfs_xxh3_blocks_total", "xxh3 算过的块数", &[("dir", d)]);
        Xxh3 {
            encoded: blocks("encode"),
            decoded: blocks("decode"),
            mismatches: metrics::counter("wmfs_xxh3_mismatches_total", "xxh3 校验不符的块数", &[]),
        }
    }
}

impl Default for Xxh3 {
    fn default() -> Xxh3 {
        Xxh3::new()
    }
}

fn sum(b: &[u8]) -> [u8; 8] {
    xxhash_rust::xxh3::xxh3_64(b).to_le_bytes()
}

impl TransformPlugin for Xxh3 {
    fn manifest(&self) -> &'static Manifest {
        &MANIFEST
    }

    fn kind(&self) -> TransformKind {
        TransformKind::Checksum
    }

    fn alg_id(&self) -> u8 {
        CK_XXH3
    }

    fn encode(&self, _ctx: &TransformCtx, input: Buf, meta: &mut BlockMeta, _pool: &MemPool) -> Result<Buf, ErrCode> {
        meta.checksum = [0; 32];
        meta.checksum[..8].copy_from_slice(&sum(input.as_slice()));
        self.encoded.inc();
        Ok(input)
    }

    fn decode(&self, _ctx: &TransformCtx, input: Buf, meta: &BlockMeta, _pool: &MemPool) -> Result<Buf, ErrCode> {
        self.decoded.inc();
        if meta.checksum[..8] != sum(input.as_slice()) {
            self.mismatches.inc();
            return Err(ErrCode::CHECKSUM_MISMATCH);
        }
        Ok(input)
    }
}

#[cfg(test)]
mod tests;
