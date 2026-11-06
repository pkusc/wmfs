//! 数据加密密钥（design.md 4.8、3.2.7 的 DATAKEY 与 DATAKEY_NAME；实现注记（M11））。
//!
//! `wmfs datakey create <名字>`：元数据 leader 随机生成 32 字节的数据密钥，用 `K_wrap`（集群主密钥派生，`proto::handshake::wrap_key`）
//! 以 AES-256-GCM 包裹成 `nonce(12) ‖ 密文(32) ‖ tag(16)` 存进 DATAKEY（同批写 DATAKEY_NAME 的名字索引，`mgmt/tasks.rs`），经集群图下发
//! （只给 Server 角色）；数据节点解包之后**只放在内存里**（[`DataKeys`]），按需解包、缓存。
//!
//! 包裹与解包用的就是加密插件（aes-gcm，key_id 0 = `K_wrap`）：core 只认 transform 钩子，不另外依赖 AES 的实现；所以停用 aes-gcm 时
//! 建不了数据密钥（`EINVAL`），也设不了 `encrypt`。附加数据 = `WMFS-DKEY1 ‖ cluster_id ‖ 名字`：包好的密钥挪到别的集群、换了名字都解不开。

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use super::DataNode;
use super::format::{BLK_ENCRYPTED, ENC_AES256GCM};
use crate::base::types::ChunkId;
use crate::base::{ErrCode, lock, log};
use crate::buf::{Lane, MemPool, POOL_WAIT};
use crate::hooks::pipeline::Pipeline;
use crate::hooks::traits::{BlockMeta, TransformCtx, TransformKind};

/// 包裹用的密钥在加密插件眼里的 key_id（数据密钥的 id 从 1 开始，3.2.7）。
pub const WRAP_KEY_ID: u32 = 0;
/// 包好的数据密钥 = nonce ‖ 密文 ‖ tag（AES-256-GCM）。
const NONCE_LEN: usize = 12;
const KEY_LEN: usize = 32;
const TAG_LEN: usize = 16;

/// 包裹 / 解包要的东西：管线（找加密插件）、内存池、`K_wrap`、集群号。
pub struct Wrapper {
    pub pipeline: Arc<Pipeline>,
    pub pool: MemPool,
    pub key: [u8; 32],
    pub cluster_id: [u8; 16],
}

impl Wrapper {
    fn call(&self, name: &str, data: &[u8], meta: &mut BlockMeta, enc: bool) -> Result<Vec<u8>, ErrCode> {
        let (p, h) = self.pipeline.transform_call(TransformKind::Encrypt, ENC_AES256GCM).ok_or(ErrCode::EINVAL)?;
        let aad = [&b"WMFS-DKEY1"[..], &self.cluster_id, name.as_bytes()].concat();
        let chunk = ChunkId { chain: 0, ino: 0, epoch: 0, idx: 0 };
        let ctx = TransformCtx {
            chunk: &chunk,
            block_idx: 0,
            level: 0,
            key_id: WRAP_KEY_ID,
            key: Some(&self.key),
            aad: &aad,
        };
        let mut b = self.pool.get(KEY_LEN, Lane::Normal, POOL_WAIT)?;
        b.as_mut_slice().copy_from_slice(data);
        let out = if enc {
            h.call(|| p.encode(&ctx, b, meta, &self.pool))?
        } else {
            h.call(|| p.decode(&ctx, b, meta, &self.pool))?
        };
        Ok(out.as_slice().to_vec())
    }

    /// 包裹一把数据密钥 → `nonce ‖ 密文 ‖ tag`。加密插件没有启用 → `EINVAL`。
    pub fn seal_datakey(&self, name: &str, key: &[u8; KEY_LEN]) -> Result<Vec<u8>, ErrCode> {
        let mut meta = BlockMeta { plain_len: KEY_LEN as u32, ..Default::default() };
        let ct = self.call(name, key, &mut meta, true)?;
        Ok([&meta.nonce[..], &ct, &meta.tag[..]].concat())
    }

    /// 解包；长度不对、tag 不符（被改过、换了集群或名字）→ 错误。
    pub fn open_datakey(&self, name: &str, wrapped: &[u8]) -> Result<[u8; KEY_LEN], ErrCode> {
        if wrapped.len() != NONCE_LEN + KEY_LEN + TAG_LEN {
            return Err(ErrCode::CORRUPT);
        }
        let (nonce, rest) = wrapped.split_at(NONCE_LEN);
        let (ct, tag) = rest.split_at(KEY_LEN);
        let mut meta = BlockMeta { flags: BLK_ENCRYPTED, plain_len: KEY_LEN as u32, ..Default::default() };
        meta.nonce.copy_from_slice(nonce);
        meta.tag.copy_from_slice(tag);
        let pt = self.call(name, ct, &mut meta, false)?;
        pt.try_into().map_err(|_| ErrCode::CORRUPT)
    }
}

/// 数据节点上解包过的数据密钥（只在内存里，见文件头）。
#[derive(Default)]
pub struct DataKeys {
    wrap: OnceLock<Arc<Wrapper>>,
    keys: Mutex<HashMap<u32, [u8; KEY_LEN]>>,
}

impl DataKeys {
    /// 服务端装配时交进包裹器（与管理 API 建数据密钥用的同一个；只第一次有效；组件测试不设置就没有加密）。
    pub fn set_wrap(&self, w: Arc<Wrapper>) {
        let _ = self.wrap.set(w);
    }

    /// 数据密钥 `id`：缓存里没有就在本节点的集群图里找到它、解包；拿不到（还没拉到带它的集群图、没设包裹器、解不开）→ None
    /// （加密插件回 `NOT_READY`，客户端退避重试）。
    pub fn get(&self, node: &DataNode, id: u32) -> Option<[u8; KEY_LEN]> {
        if let Some(k) = lock(&self.keys).get(&id) {
            return Some(*k);
        }
        let map = node.map.get()?;
        let dk = map.datakeys.iter().find(|(i, _)| *i == id).map(|(_, k)| k)?;
        match self.wrap.get()?.open_datakey(&dk.name, &dk.wrapped) {
            Ok(k) => {
                lock(&self.keys).insert(id, k);
                Some(k)
            }
            Err(e) => {
                log::error("data", format_args!("数据密钥 {id}（{}）解包失败：{e}", dk.name));
                None
            }
        }
    }
}
