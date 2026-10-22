//! 按记录类型读写元数据 KV 的小函数（design.md 3.2.7–3.2.8）：读一条 V1 记录、写一条 V1 记录、GLOBAL 计数器取号
//! （[`next_id`]）与 `map_epoch += 1`（[`bump_map_epoch`]）、扫完一个键空间。命名空间、会话、集群图、changelog 的代码都用它们，
//! 不各自拼"版本字节 + postcard"。
//!
//! GLOBAL 里的整数（next_ino、next_chain、map_epoch……）按规格是 u16/u32/u64，postcard 对它们的编码完全相同
//! （变长整数），所以这里一律按 u64 读写——写回的字节与按原类型编码的一样（金样 `init_kv.golden` 可以对照）。

use serde::Serialize;
use serde::de::DeserializeOwned;

use super::keys;
use super::records::{decode_v1, encode_v1};
use crate::base::ErrCode;
use crate::meta::engine::txn::{KvRead, KvScan, KvTxn};

/// 读一条记录：不存在 → None；解不开 → `INTERNAL`（持久化状态损坏）。
pub fn get<T: DeserializeOwned>(v: &dyn KvRead, key: &[u8]) -> Result<Option<T>, ErrCode> {
    v.get(key)?.map(|b| decode_v1(&b)).transpose()
}

pub fn put<T: Serialize>(v: &mut dyn KvTxn, key: &[u8], rec: &T) {
    v.put(key, encode_v1(rec));
}

/// GLOBAL 里的一个整数；没有这个键 → `INTERNAL`（init 总会写全部计数器）。
fn global_u64(v: &dyn KvRead, name: &str) -> Result<u64, ErrCode> {
    get::<u64>(v, &keys::global(name))?.ok_or(ErrCode::INTERNAL)
}

/// 取号（`next_ino`、`next_chain`、`next_changelog`……）：返回计数器的当前值（下一个可用的号），并把它加一写回。
pub fn next_id(v: &mut dyn KvTxn, name: &str) -> Result<u64, ErrCode> {
    let n = global_u64(v, name)?;
    put(v, &keys::global(name), &n.checked_add(1).ok_or(ErrCode::ENOSPC)?);
    Ok(n)
}

/// `map_epoch += 1`：改变了集群图里任何一项（链、target、QoS 表……）的批次都要在同一批里调用（3.3.6）。
pub fn bump_map_epoch(v: &mut dyn KvTxn) -> Result<(), ErrCode> {
    next_id(v, keys::G_MAP_EPOCH).map(|_| ())
}

/// (key, value) 的列表。
type Rows = Vec<(Vec<u8>, Vec<u8>)>;

/// 一个键空间里的全部 (key, value)，按 key 升序（分页扫完）。只在只读视图上用，而且只用于小表（集群图的节点、target、链……）。
pub fn scan_tag(v: &dyn KvScan, tag: u8) -> Result<Rows, ErrCode> {
    const PAGE: usize = 1024;
    let (mut start, end) = keys::tag_range(tag);
    let mut out = Vec::new();
    loop {
        let page = v.scan(&start, &end, PAGE)?;
        let more = page.len() == PAGE;
        if let Some((k, _)) = page.last() {
            start = [k.as_slice(), &[0]].concat(); // 紧跟在最后一个 key 之后的第一个 key
        }
        out.extend(page);
        if !more {
            return Ok(out);
        }
    }
}

/// key 里标签之后的大端整数（u32 或 u64 的 id）。
pub fn key_id(key: &[u8]) -> u64 {
    key.iter().skip(1).take(8).fold(0, |a, b| (a << 8) | *b as u64)
}
