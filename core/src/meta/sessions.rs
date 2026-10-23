//! 客户端会话（design.md 3.4.4、3.5.2、3.4.1.10，键空间 0x0B）。
//!
//! - `SessionOpen`（写，seq 0）：session id 由客户端随机生成，按 session 幂等——已经打开过（重试）就照常回复；被驱逐过的
//!   session 不能再打开（驱逐是单调的事实）。写 SESSION 记录与 changelog `SessionOpen`。
//! - `SessionClose`（写）：删掉 SESSION 记录、写 changelog `SessionClose`；之后这个会话的修改型请求一律 `SESSION_EVICTED`。
//!   剩余的去重记录由 gc 任务扫描删除（M5）。
//! - `SessionHeartbeat`（R*：只读路径，先过 ReadIndex，确认自己仍是 leader 才续租）：会话还活着就更新 leader 内存里的
//!   `last_seen` 与打开文件集合（[`Live`]，3.5.6）；leader 换过之后（term 变了）内存状态作废，回复 `need_full_open_set = true`
//!   让客户端重发全集（3.4.1.10）。
//!
//! 实现注记（M4）：`SessionClose` 也用 seq 0（与 SessionOpen 一样不进去重表）：它本身就幂等（会话已经不在 → 照常回 Ok），
//! 而带 seq 的重试会因为"会话已经关闭"拿到 `SESSION_EVICTED`。正在被驱逐的会话（Evicting）收到 SessionClose 只回 Ok、
//! 不删记录——驱逐的两个阶段（M10）自己走完。会话租约到期的驱逐（cluster-mgr）在 M10。
//! 会话绑定打开它的 key（`SessionRecV1.key_id`）：session id 在线上是明文，别的 key 拿着它发修改、心跳、关闭或重新打开
//! 一律 `EACCES`（[`usable`]），否则别人能抢先用它的 seq 污染去重表，或者替它关掉会话。
//! 实现注记（M5）：签给客户端的 cap 带着会话号（被驱逐会话的 cap 由数据节点拒绝，ADR-0023 第 4 条），所以只读路径上签 cap 之前
//! 也核对会话（[`may_sign_cap`]）。

use std::collections::HashMap;
use std::sync::Mutex;

use crate::base::types::{ReqCtx, Role};
use crate::base::{ErrCode, lock};
use crate::meta::changelog::{self, Change};
use crate::meta::engine::txn::{KvRead, KvTxn, OpCtx};
use crate::meta::schema::keys;
use crate::meta::schema::kv;
use crate::meta::schema::records::SessionRecV1;
use crate::proto::meta::MetaReply;

/// 会话状态（`SessionRecV1.state`）：1 Active，2 Evicting（驱逐第一阶段已完成）。
pub const SESSION_ACTIVE: u8 = 1;

/// 会话存在且是 Active。驱逐是单调的事实：Evicting 或已删除的会话的任何请求都回 `SESSION_EVICTED`。执行器的 run_one 与
/// 心跳用同时核对 key 的 [`usable`]（3.4.1.4 的注记）。
pub fn is_active(v: &dyn KvRead, session: u64) -> bool {
    matches!(kv::get::<SessionRecV1>(v, &keys::session(session)), Ok(Some(s)) if s.state == SESSION_ACTIVE)
}

/// 用 key `key_id` 的连接能不能用这个会话（带 seq 的修改与心跳，3.4.1.4 run_one）：会话不存在或不是 Active →
/// `SESSION_EVICTED`（同 [`is_active`]）；是别的 key 打开的 → `EACCES`（见文件头）。
pub fn usable(v: &dyn KvRead, session: u64, key_id: u32) -> Result<(), ErrCode> {
    match kv::get::<SessionRecV1>(v, &keys::session(session)) {
        Ok(Some(s)) if s.state == SESSION_ACTIVE && s.key_id == key_id => Ok(()),
        Ok(Some(s)) if s.state == SESSION_ACTIVE => Err(ErrCode::EACCES),
        _ => Err(ErrCode::SESSION_EVICTED),
    }
}

/// 能不能签一张带 `req.session` 的 cap（3.3.5 的实现注记（M5））：Open、RenewCap、Uninline 走只读路径，执行器的会话检查覆盖
/// 不到它们，所以签 cap 之前在这里核对——会话要能用（[`usable`]：Active、属于这把 key）；session 0 的 cap 不属于任何会话、
/// 不会被驱逐，只签给内部请求与 Server 角色，别人 → `EINVAL`。执行器里签 cap 的修改（Create、改长度的 SetAttr）带 seq，
/// 执行器已经核对过会话（3.4.1.4）。
pub fn may_sign_cap(v: &dyn KvRead, req: &ReqCtx) -> Result<(), ErrCode> {
    if req.session != 0 {
        return usable(v, req.session, req.who.conn.key_id);
    }
    if req.internal || req.who.conn.role == Role::Server { Ok(()) } else { Err(ErrCode::EINVAL) }
}

/// SessionOpen 带来的客户端信息（`version` 现在不存、也不用：记录里没有这一项）。
pub struct OpenInfo {
    pub client_name: String,
    pub host: String,
    pub host_id: String,
    pub mount: String,
}

/// 执行器闭包：打开会话（见文件头）。
pub fn open(v: &mut dyn KvTxn, c: &OpCtx, info: OpenInfo) -> Result<MetaReply, ErrCode> {
    let (sid, now) = (c.req.session, c.req.now_ms);
    if sid == 0 {
        return Err(ErrCode::EINVAL); // session 0 表示"没有会话"（内部操作、只读请求）
    }
    if v.get(&keys::evicted(sid))?.is_some() {
        return Err(ErrCode::SESSION_EVICTED);
    }
    let id = &c.req.who.conn;
    match kv::get::<SessionRecV1>(v, &keys::session(sid))? {
        Some(s) if s.key_id != id.key_id => return Err(ErrCode::EACCES), // 别的 key 打开的会话
        Some(s) if s.state == SESSION_ACTIVE => {}                       // 重试：已经打开过
        Some(_) => return Err(ErrCode::SESSION_EVICTED),
        None => {
            let rec = SessionRecV1 {
                client_name: info.client_name,
                host: info.host,
                host_id: info.host_id,
                mount: info.mount,
                key_id: id.key_id,
                tenant: id.tenant,
                created_ms: now,
                locked_inos: vec![],
                state: SESSION_ACTIVE,
                evict_since_ms: 0,
                evict_map_epoch: 0,
            };
            kv::put(v, &keys::session(sid), &rec);
            changelog::append(v, c, Change { op: "SessionOpen", ..Default::default() })?;
        }
    }
    Ok(MetaReply::Session { lease_ms: c.settings.session_lease_ms, map_epoch: c.map.map_epoch })
}

/// 执行器闭包：关闭会话（见文件头）。
pub fn close(v: &mut dyn KvTxn, c: &OpCtx) -> Result<MetaReply, ErrCode> {
    let key = keys::session(c.req.session);
    match kv::get::<SessionRecV1>(v, &key)? {
        Some(s) if s.key_id != c.req.who.conn.key_id => return Err(ErrCode::EACCES), // 别的 key 打开的会话
        Some(s) if s.state == SESSION_ACTIVE => {
            v.delete(&key);
            changelog::append(v, c, Change { op: "SessionClose", ..Default::default() })?;
        }
        _ => {} // 已经不在（重试）或正在被驱逐：照常回 Ok
    }
    Ok(MetaReply::Ok)
}

/// leader 内存里一个会话的状态（故障切换时丢失，3.4.1.10）。
struct LiveSession {
    term: u64,                // 在哪个 term 里记下的（term 变了 = 换过 leader，作废）
    last_seen_ms: u64,        // 最近一次收到心跳（单调时钟；M10 的驱逐看它）
    open: HashMap<u64, bool>, // 打开文件集合：ino → 是否写打开（M5 的孤儿 GC、M11 的迁移看它）
}

/// 全部存活会话的内存状态（只在 leader 上有意义）。
#[derive(Default)]
pub struct Live {
    m: Mutex<HashMap<u64, LiveSession>>,
}

impl Live {
    /// 记一次心跳（`opened`/`closed` 是增量，`full` 是全集）；返回是否要客户端重发打开文件的全集
    /// （这个 term 里还没收到过它的全集，例如刚换过 leader）。
    pub fn heartbeat(
        &self,
        session: u64,
        term: u64,
        now_ms: u64,
        opened: Vec<(u64, bool)>,
        closed: Vec<u64>,
        full: Option<Vec<(u64, bool)>>,
    ) -> bool {
        let mut m = lock(&self.m);
        if let Some(all) = full {
            m.insert(session, LiveSession { term, last_seen_ms: now_ms, open: all.into_iter().collect() });
            return false;
        }
        match m.get_mut(&session) {
            Some(s) if s.term == term => {
                s.last_seen_ms = now_ms;
                s.open.extend(opened);
                for ino in &closed {
                    s.open.remove(ino);
                }
                false
            }
            _ => true,
        }
    }

    /// SessionOpen 成功之后：新会话还没有打开的文件，记下它（算收到过全集）；SessionOpen 的重试保留已经知道的打开文件；
    /// 旧 term 留下的状态作废（下一次心跳要全集）。
    pub fn touch(&self, session: u64, term: u64, now_ms: u64) {
        let mut m = lock(&self.m);
        match m.get(&session).map(|s| s.term == term) {
            None => {
                m.insert(session, LiveSession { term, last_seen_ms: now_ms, open: HashMap::new() });
            }
            Some(true) => {}
            Some(false) => {
                m.remove(&session);
            }
        }
    }

    /// SessionClose 之后：删掉它的内存状态。
    pub fn forget(&self, session: u64) {
        lock(&self.m).remove(&session);
    }

    /// 有没有哪个会话打开着 `ino`（孤儿 GC 的条件 ①，3.5.6）。不管记下它的是哪个 term：不知道就当作还开着。
    pub fn is_open(&self, ino: u64) -> bool {
        lock(&self.m).values().any(|s| s.open.contains_key(&ino))
    }

    /// `sessions`（全部存活会话）里的每一个都在 `term` 里报过打开文件的全集，或是在 `term` 里新开的（孤儿 GC 的条件 ③：
    /// 换过 leader 之后，要等每个存活会话都重发过全集，打开集合才可信）。
    pub fn covers(&self, term: u64, sessions: &[u64]) -> bool {
        let m = lock(&self.m);
        sessions.iter().all(|s| m.get(s).is_some_and(|x| x.term == term))
    }

    /// 某个会话打开着的文件，按 ino 排序（测试与 M5 的孤儿 GC 用）。
    pub fn open_files(&self, session: u64) -> Vec<(u64, bool)> {
        let m = lock(&self.m);
        let mut v: Vec<(u64, bool)> =
            m.get(&session).map(|s| s.open.iter().map(|(k, w)| (*k, *w)).collect()).unwrap_or_default();
        v.sort();
        v
    }
}

#[cfg(test)]
mod tests;
