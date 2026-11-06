//! zstd：transform 插件的压缩一类（design.md 4.8、3.2.4，altitude 732，算法编号 2）。
//!
//! 与 lz4 相同的做法（压不进明文的 7/8 就存原文、不置 `COMPRESSED`；解不开或长度不对 → `CHECKSUM_MISMATCH`），只是用
//! `zstd::bulk`，级别来自 layout 的 `compress_level`（经 cap 与 chunk 头传到这里的 `TransformCtx.level`；0 = zstd 的默认 3）。
//! 每个线程缓存一个压缩上下文与一个解压上下文（4.8：建上下文要分配几百 KiB，不值得每块建一次）。
//! 指标 `wmfs_zstd_blocks_total{result="compressed"|"stored"|"decoded"}`；fail point `plugin.zstd.transform`。

use std::cell::RefCell;
use std::sync::Arc;

use wmfs_core::api::metrics::{self, Counter};
use wmfs_core::base::ErrCode;
use wmfs_core::buf::{Buf, Lane, MemPool, POOL_WAIT};
use wmfs_core::data::format::{BLK_COMPRESSED, CMP_ZSTD};
use wmfs_core::hooks::manifest::{CORE_ABI_VERSION, Hook, Manifest};
use wmfs_core::hooks::traits::{BlockMeta, TransformCtx, TransformKind, TransformPlugin};
use zstd::bulk::{Compressor, Decompressor};

pub static MANIFEST: Manifest = Manifest {
    name: "zstd",
    version: "0.1.0",
    hooks: &[Hook::Transform],
    altitude: 732,
    hot_path: true,
    config_schema: "",
    abi_version: CORE_ABI_VERSION,
    attrs: &["compress"],
    budget_ns: 600_000, // 3.7.4：zstd 每 64 KiB 块 600 µs
    fail_points: &["plugin.zstd.transform"],
};

/// `compress=zstd` 不写级别时的级别（与 zstd 自己的默认值相同）。
pub const DEFAULT_LEVEL: i32 = 3;

thread_local! {
    static CCTX: RefCell<Option<(i32, Compressor<'static>)>> = const { RefCell::new(None) };
    static DCTX: RefCell<Option<Decompressor<'static>>> = const { RefCell::new(None) };
}

pub struct Zstd {
    compressed: Arc<Counter>,
    stored: Arc<Counter>,
    decoded: Arc<Counter>,
}

impl Zstd {
    pub fn new() -> Zstd {
        let c = |r| metrics::counter("wmfs_zstd_blocks_total", "zstd 处理过的块数", &[("result", r)]);
        Zstd { compressed: c("compressed"), stored: c("stored"), decoded: c("decoded") }
    }
}

impl Default for Zstd {
    fn default() -> Zstd {
        Zstd::new()
    }
}

/// 用本线程的压缩上下文（级别变了就改级别）把 `src` 压进 `dst`：放不下（压不下去）、建上下文失败 → None。
fn compress(level: i32, src: &[u8], dst: &mut [u8]) -> Option<usize> {
    CCTX.with(|c| {
        let mut c = c.borrow_mut();
        if c.as_ref().is_none_or(|(l, _)| *l != level) {
            *c = Some((level, Compressor::new(level).ok()?));
        }
        c.as_mut()?.1.compress_to_buffer(src, dst).ok()
    })
}

fn decompress(src: &[u8], dst: &mut [u8]) -> Option<usize> {
    DCTX.with(|d| {
        let mut d = d.borrow_mut();
        if d.is_none() {
            *d = Some(Decompressor::new().ok()?);
        }
        d.as_mut()?.decompress_to_buffer(src, dst).ok()
    })
}

impl TransformPlugin for Zstd {
    fn manifest(&self) -> &'static Manifest {
        &MANIFEST
    }

    fn kind(&self) -> TransformKind {
        TransformKind::Compress
    }

    fn alg_id(&self) -> u8 {
        CMP_ZSTD
    }

    fn encode(&self, ctx: &TransformCtx, input: Buf, meta: &mut BlockMeta, pool: &MemPool) -> Result<Buf, ErrCode> {
        let n = input.len();
        let level = if ctx.level == 0 { DEFAULT_LEVEL } else { ctx.level as i32 };
        let mut out = pool.get(n.max(1), Lane::Normal, POOL_WAIT)?;
        out.set_len(n * 7 / 8); // 放不进 7/8 = 压不下去：存原文（4.8）
        match compress(level, input.as_slice(), out.as_mut_slice()) {
            Some(m) if n > 0 => {
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
        match decompress(input.as_slice(), out.as_mut_slice()) {
            Some(m) if m == meta.plain_len as usize => Ok(out),
            _ => Err(ErrCode::CHECKSUM_MISMATCH), // 解不开、长度不对：当坏块处理
        }
    }
}

#[cfg(test)]
mod tests;
