//! crc32c：transform 插件的校验和一类（design.md 4.8、3.2.4，altitude 701，算法编号 1）。
//!
//! 写：对每个块的**明文**算 crc32c（aarch64 有硬件指令），小端写进块表条目 `checksum` 的前 4 字节，原样返回同一个缓冲（零拷贝）；
//! 读：重新算一遍，与块表里的对不上 → `CHECKSUM_MISMATCH`（本体把 chunk 标为 Corrupt，客户端换副本，全坏了才 `EIO`）。
//! 指标 `wmfs_crc32c_blocks_total{dir="encode"|"decode"}`、`wmfs_crc32c_mismatches_total`；fail point `plugin.crc32c.transform`
//! （本体包在每次调用外层，触发 = 这一块以注入的码失败）。

use std::sync::Arc;

use wmfs_core::api::metrics::{self, Counter};
use wmfs_core::base::ErrCode;
use wmfs_core::buf::{Buf, MemPool};
use wmfs_core::data::format::CK_CRC32C;
use wmfs_core::hooks::manifest::{CORE_ABI_VERSION, Hook, Manifest};
use wmfs_core::hooks::traits::{BlockMeta, TransformCtx, TransformKind, TransformPlugin};

pub static MANIFEST: Manifest = Manifest {
    name: "crc32c",
    version: "0.1.0",
    hooks: &[Hook::Transform],
    altitude: 701,
    hot_path: true,
    config_schema: "",
    abi_version: CORE_ABI_VERSION,
    attrs: &["checksum"],
    budget_ns: 20_000, // 3.7.4：checksum 每 64 KiB 块 20 µs
    fail_points: &["plugin.crc32c.transform"],
};

pub struct Crc32c {
    encoded: Arc<Counter>,
    decoded: Arc<Counter>,
    mismatches: Arc<Counter>,
}

impl Crc32c {
    pub fn new() -> Crc32c {
        let blocks = |d| metrics::counter("wmfs_crc32c_blocks_total", "crc32c 算过的块数", &[("dir", d)]);
        Crc32c {
            encoded: blocks("encode"),
            decoded: blocks("decode"),
            mismatches: metrics::counter("wmfs_crc32c_mismatches_total", "crc32c 校验不符的块数", &[]),
        }
    }
}

impl Default for Crc32c {
    fn default() -> Crc32c {
        Crc32c::new()
    }
}

impl TransformPlugin for Crc32c {
    fn manifest(&self) -> &'static Manifest {
        &MANIFEST
    }

    fn kind(&self) -> TransformKind {
        TransformKind::Checksum
    }

    fn alg_id(&self) -> u8 {
        CK_CRC32C
    }

    fn encode(&self, _ctx: &TransformCtx, input: Buf, meta: &mut BlockMeta, _pool: &MemPool) -> Result<Buf, ErrCode> {
        meta.checksum = [0; 32];
        meta.checksum[..4].copy_from_slice(&crc32c::crc32c(input.as_slice()).to_le_bytes());
        self.encoded.inc();
        Ok(input)
    }

    fn decode(&self, _ctx: &TransformCtx, input: Buf, meta: &BlockMeta, _pool: &MemPool) -> Result<Buf, ErrCode> {
        self.decoded.inc();
        if meta.checksum[..4] != crc32c::crc32c(input.as_slice()).to_le_bytes() {
            self.mismatches.inc();
            return Err(ErrCode::CHECKSUM_MISMATCH);
        }
        Ok(input)
    }
}

#[cfg(test)]
mod tests;
