//! 客户端会话（design.md 3.4.4、3.5.2，键空间 0x0B）。
//!
//! M3 只有执行器要用的 [`is_active`]；SessionOpen / 心跳 / 关闭 / 驱逐两阶段在 M4、M10 加入。

use crate::meta::engine::txn::KvRead;
use crate::meta::schema::keys;
use crate::meta::schema::records::{SessionRecV1, decode_v1};

/// 会话状态（`SessionRecV1.state`）：1 Active，2 Evicting（驱逐第一阶段已完成）。
pub const SESSION_ACTIVE: u8 = 1;

/// 会话存在且是 Active。驱逐是单调的事实：Evicting 或已删除的会话的任何请求都回 `SESSION_EVICTED`（3.4.1.4 run_one）。
pub fn is_active(v: &dyn KvRead, session: u64) -> bool {
    match v.get(&keys::session(session)) {
        Ok(Some(b)) => decode_v1::<SessionRecV1>(&b).is_ok_and(|s| s.state == SESSION_ACTIVE),
        _ => false,
    }
}
