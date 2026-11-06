//! aes-gcm：transform 插件的加密一类（design.md 4.8、3.2.4，altitude 761，算法编号 1）。
//!
//! 写（变换链的最后一步）：AES-256-GCM **原地**加密一块（零拷贝），nonce 是每次用 `rand::fill` 现取的 12 个随机字节（绝不从版本号推导），
//! nonce 与 16 字节的 tag 写进块表条目，置 `ENCRYPTED`。附加数据（AAD）由本体给出：`WMFS-BLK1 ‖ cluster_id ‖ ino ‖ epoch ‖ chunk_idx ‖
//! block_idx`，块被挪到别的文件、位置或集群就解不开。读：原地解密并核对 tag，不符 → `CHECKSUM_MISMATCH`（与坏块同样处理）。
//! 密钥由本体给（`TransformCtx.key`：数据节点从集群图里的 DATAKEY 解包出来、只放在内存里的那把；还没拿到 → `NOT_READY`，客户端
//! 退避重试）。按 key_id 缓存已经展开的密钥（核对密钥本身没变）。
//! 本体也用这个插件、配 `K_wrap` 包裹与解包数据密钥本身（`data::datakey`，key_id 0），所以 core 不另外依赖 AES 的实现。
//! 指标 `wmfs_aes_gcm_blocks_total{dir}`、`wmfs_aes_gcm_auth_failures_total`；fail point `plugin.aes-gcm.transform`。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use ::aes_gcm::aead::{AeadInOut, Nonce, Tag};
use ::aes_gcm::{Aes256Gcm, KeyInit};
use wmfs_core::api::metrics::{self, Counter};
use wmfs_core::base::{ErrCode, lock, rand};
use wmfs_core::buf::{Buf, MemPool};
use wmfs_core::data::format::{BLK_ENCRYPTED, ENC_AES256GCM};
use wmfs_core::hooks::manifest::{CORE_ABI_VERSION, Hook, Manifest};
use wmfs_core::hooks::traits::{BlockMeta, TransformCtx, TransformKind, TransformPlugin};

pub static MANIFEST: Manifest = Manifest {
    name: "aes-gcm",
    version: "0.1.0",
    hooks: &[Hook::Transform],
    altitude: 761,
    hot_path: true,
    config_schema: "",
    abi_version: CORE_ABI_VERSION,
    attrs: &["encrypt"],
    budget_ns: 100_000, // 3.7.4：aes-gcm 每 64 KiB 块 100 µs
    fail_points: &["plugin.aes-gcm.transform"],
};

/// key_id → (这把密钥, 展开好的 cipher)：同一个 id 换了密钥就重新展开。
type Ciphers = HashMap<u32, ([u8; 32], Arc<Aes256Gcm>)>;

pub struct AesGcm {
    ciphers: Mutex<Ciphers>,
    encrypted: Arc<Counter>,
    decrypted: Arc<Counter>,
    auth_failures: Arc<Counter>,
}

impl AesGcm {
    pub fn new() -> AesGcm {
        let blocks = |d| metrics::counter("wmfs_aes_gcm_blocks_total", "aes-gcm 加解密过的块数", &[("dir", d)]);
        AesGcm {
            ciphers: Mutex::default(),
            encrypted: blocks("encode"),
            decrypted: blocks("decode"),
            auth_failures: metrics::counter("wmfs_aes_gcm_auth_failures_total", "aes-gcm 解密时 tag 不符的块数", &[]),
        }
    }

    /// `key_id` 的密钥展开好的 cipher（缓存；同一个 id 换了密钥就重新展开）。没给密钥 → `NOT_READY`。
    fn cipher(&self, ctx: &TransformCtx) -> Result<Arc<Aes256Gcm>, ErrCode> {
        let key = ctx.key.ok_or(ErrCode::NOT_READY)?;
        let mut m = lock(&self.ciphers);
        if let Some((k, c)) = m.get(&ctx.key_id)
            && k == key
        {
            return Ok(c.clone());
        }
        let c = Arc::new(Aes256Gcm::new_from_slice(key).map_err(|_| ErrCode::INTERNAL)?);
        m.insert(ctx.key_id, (*key, c.clone()));
        Ok(c)
    }
}

impl Default for AesGcm {
    fn default() -> AesGcm {
        AesGcm::new()
    }
}

impl TransformPlugin for AesGcm {
    fn manifest(&self) -> &'static Manifest {
        &MANIFEST
    }

    fn kind(&self) -> TransformKind {
        TransformKind::Encrypt
    }

    fn alg_id(&self) -> u8 {
        ENC_AES256GCM
    }

    fn encode(&self, ctx: &TransformCtx, input: Buf, meta: &mut BlockMeta, _pool: &MemPool) -> Result<Buf, ErrCode> {
        let mut input = input;
        let cipher = self.cipher(ctx)?;
        let mut nb = [0u8; 12];
        rand::fill(&mut nb);
        let nonce = Nonce::<Aes256Gcm>::try_from(&nb[..]).map_err(|_| ErrCode::INTERNAL)?;
        let tag = cipher
            .encrypt_inout_detached(&nonce, ctx.aad, input.as_mut_slice().into())
            .map_err(|_| ErrCode::INTERNAL)?;
        meta.nonce = nb;
        meta.tag.copy_from_slice(tag.as_slice());
        meta.flags |= BLK_ENCRYPTED;
        self.encrypted.inc();
        Ok(input)
    }

    fn decode(&self, ctx: &TransformCtx, input: Buf, meta: &BlockMeta, _pool: &MemPool) -> Result<Buf, ErrCode> {
        let mut input = input;
        let cipher = self.cipher(ctx)?;
        let nonce = Nonce::<Aes256Gcm>::try_from(&meta.nonce[..]).map_err(|_| ErrCode::INTERNAL)?;
        let tag = Tag::<Aes256Gcm>::try_from(&meta.tag[..]).map_err(|_| ErrCode::INTERNAL)?;
        self.decrypted.inc();
        if cipher.decrypt_inout_detached(&nonce, ctx.aad, input.as_mut_slice().into(), &tag).is_err() {
            self.auth_failures.inc();
            return Err(ErrCode::CHECKSUM_MISMATCH);
        }
        Ok(input)
    }
}

#[cfg(test)]
mod tests;
