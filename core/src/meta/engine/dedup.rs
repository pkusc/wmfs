//! 请求去重表 `DEDUP(session, seq)`（design.md 3.5.2、3.4.1.4 的 run_one，键空间 0x0C）。
//!
//! 每个带 seq 的修改型请求的**最终**结果（成功，或 1..=255 的 POSIX errno——含锁冲突的 `EAGAIN`）都与修改同批写进去重表：
//! 重试（同一个 seq）直接得到原结果，不会冒出假的 `EEXIST/ENOENT`；而且批次写集必然非空，所以错误回复也一定在
//! "确认自己仍是 leader 并提交成功"之后才发出。集群类错误码（≥ 1000）表示"没有执行，请重试"，不写去重表。
//! 客户端保证每个会话在途的修改型请求 ≤ 256 个、seq 逐个递增，所以写入 seq 的结果时删除 seq − 256 的结果是安全的（规则 4）。

use crate::base::ErrCode;
use crate::meta::engine::txn::{KvRead, KvTxn};
use crate::meta::schema::keys;
use crate::meta::schema::records::{DedupV1, decode_v1, encode_v1};
use crate::proto::meta::MetaReply;
use crate::proto::{decode_body, encode_body};

/// 去重命中 → 原结果；没有记录 → None。记录损坏（解不开）按 `INTERNAL`；读出错原样返回（执行器的视图上
/// 读错误已经 fail-stop，到不了这里）。
pub fn get(v: &dyn KvRead, session: u64, seq: u64) -> Option<Result<MetaReply, ErrCode>> {
    let b = match v.get(&keys::dedup(session, seq)) {
        Ok(Some(b)) => b,
        Ok(None) => return None,
        Err(e) => return Some(Err(e)),
    };
    let r = match decode_v1::<DedupV1>(&b) {
        Ok(DedupV1 { code: 0, reply }) => decode_body::<MetaReply>(&reply).map_err(|_| ErrCode::INTERNAL),
        Ok(DedupV1 { code, .. }) => Err(ErrCode(code)),
        Err(_) => Err(ErrCode::INTERNAL),
    };
    Some(r)
}

/// 这个结果要不要写进去重表：成功，或 POSIX errno。
pub fn is_final(r: &Result<MetaReply, ErrCode>) -> bool {
    match r {
        Ok(_) => true,
        Err(c) => c.is_posix(),
    }
}

/// 记下 (session, seq) 的结果：code 0 = 成功（reply 为 postcard(MetaReply)），否则 POSIX errno（3.2.8）。
pub fn put(v: &mut dyn KvTxn, session: u64, seq: u64, r: &Result<MetaReply, ErrCode>) {
    let rec = match r {
        Ok(reply) => DedupV1 { code: 0, reply: encode_body(reply) },
        Err(c) => DedupV1 { code: c.0, reply: vec![] },
    };
    v.put(&keys::dedup(session, seq), encode_v1(&rec));
}

pub fn delete(v: &mut dyn KvTxn, session: u64, seq: u64) {
    v.delete(&keys::dedup(session, seq));
}
