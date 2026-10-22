//! `raft::Storage` 的 redb 实现 `RedbStorage`（design.md 3.4.1.2、3.2.6，ADR-0019）。
//!
//! 一个 `meta/meta.redb` 里三张表：`raft_log`（u64 → raft `Entry` 的 protobuf）、`raft_state`（&str → 字节：
//! `hard_state`、`conf_state` 是 raft-rs 的 protobuf，`snap_meta`、`applied`、`format` 的格式在 `schema/raft_state.rs`）、
//! `kv`（状态机，布局见 3.2.7）。
//!
//! - 读方法（`initial_state/entries/term/first_index/last_index/snapshot`）由 raft-rs 在 raft 线程上调用；
//!   最近 4096 条（且合计不超过 64 MiB）条目缓存在内存里，`term()/entries()` 的热路径不读盘。
//! - 写方法只由 raft 线程经 `node.mut_store()` 调用，都接收外部传入的写事务：条目、硬状态、conf_state、applied
//!   与状态机修改在同一个事务里落盘（3.2.6 的写入规则 M1–M3，坑 P5）。内存副本（hs、cs、压缩点、last_index、缓存）随写方法同步更新。
//! - raft 线程上的 redb 错误一律 fail-stop（写日志 + abort，0.2 第 5 条）；写方法返回 `EIO` 并已写好日志，
//!   由调用方（driver）`or_abort`。
//!
//! 实现注记（M3）：`snapshot()` 回 `SnapshotTemporarilyUnavailable`——快照的导出与安装（3.4.1.7、规则 M5）在 M8；
//! M3 的单节点集群没有需要快照的对端。

use std::collections::VecDeque;
use std::sync::Arc;

use protobuf::Message as _;
use raft::eraftpb::{ConfState, Entry, HardState, Snapshot};
use raft::{Error as RaftError, GetEntriesContext, RaftState, Storage, StorageError};
use redb::{Database, ReadTransaction, ReadableTable, TableDefinition, WriteTransaction};

use super::{or_abort, redb_eio};
use crate::base::{ErrCode, log};
use crate::meta::schema::keys;
use crate::meta::schema::raft_state::{
    Applied, KEY_APPLIED, KEY_CONF_STATE, KEY_FORMAT, KEY_HARD_STATE, KEY_SNAP_META, MetaFormatV1, SnapMeta,
};
use crate::meta::schema::records::decode_v1;

pub const RAFT_LOG: TableDefinition<u64, &[u8]> = TableDefinition::new("raft_log");
pub const RAFT_STATE: TableDefinition<&str, &[u8]> = TableDefinition::new("raft_state");
pub const KV: TableDefinition<&[u8], &[u8]> = TableDefinition::new("kv");

/// 内存里缓存的最近条目：最多这么多条、合计这么多字节（单条最大约 1.2 MiB，3.4.1.4）。
const CACHE_ENTRIES: usize = 4096;
const CACHE_BYTES: usize = 64 << 20;

pub struct RedbStorage {
    db: Arc<Database>,
    hs: HardState, // 内存副本，与 redb 同步更新（只在 raft 线程改）
    cs: ConfState,
    snap_index: u64, // 最近一次压缩点（init 后为 (1, 1)）
    snap_term: u64,
    last_index: u64, // 无条目时 = snap_index
    recent: VecDeque<Entry>,
    recent_bytes: usize,
}

impl RedbStorage {
    /// 打开已初始化的元数据库（init 写过 index = 1 的初始状态）。格式版本不认识 → 拒绝（3.2.10）。
    /// 只在启动时调用（还不在 raft 线程上），错误是给人看的中文。
    pub fn open(db: Arc<Database>) -> Result<RedbStorage, String> {
        let rtx = db.begin_read().map_err(|e| format!("redb 读事务失败：{e}"))?;
        let st = rtx
            .open_table(RAFT_STATE)
            .map_err(|e| format!("打不开 raft_state 表（不是 wmfs init 建的元数据库？）：{e}"))?;
        let get = |k: &str| -> Result<Vec<u8>, String> {
            match st.get(k) {
                Ok(Some(v)) => Ok(v.value().to_vec()),
                Ok(None) => Err(format!("raft_state 里没有 {k}（元数据库不完整）")),
                Err(e) => Err(format!("读 raft_state[{k}] 失败：{e}")),
            }
        };
        let fmt = MetaFormatV1::decode(&get(KEY_FORMAT)?).map_err(|e| format!("raft_state[format] 损坏：{e}"))?;
        if fmt.version != 1 || !fmt.incompat.is_empty() {
            return Err(format!("元数据格式 {fmt:?} 不受本版本支持（只认识 version 1、没有 incompat 特性，3.2.10）"));
        }
        let hs = HardState::parse_from_bytes(&get(KEY_HARD_STATE)?).map_err(|e| format!("hard_state 损坏：{e}"))?;
        let cs = ConfState::parse_from_bytes(&get(KEY_CONF_STATE)?).map_err(|e| format!("conf_state 损坏：{e}"))?;
        let snap = SnapMeta::decode(&get(KEY_SNAP_META)?).map_err(|e| format!("snap_meta 损坏：{e}"))?;
        let t = rtx.open_table(RAFT_LOG).map_err(|e| format!("打不开 raft_log 表：{e}"))?;
        let last = t.last().map_err(|e| format!("读 raft_log 失败：{e}"))?.map(|(k, _)| k.value());
        let last_index = last.unwrap_or(snap.index);
        let mut s = RedbStorage {
            db: db.clone(),
            hs,
            cs,
            snap_index: snap.index,
            snap_term: snap.term,
            last_index,
            recent: VecDeque::new(),
            recent_bytes: 0,
        };
        let from = last_index.saturating_sub(CACHE_ENTRIES as u64 - 1).max(snap.index + 1);
        if from <= last_index {
            for r in t.range(from..=last_index).map_err(|e| format!("读 raft_log 失败：{e}"))? {
                let (_, v) = r.map_err(|e| format!("读 raft_log 失败：{e}"))?;
                s.cache_push(Entry::parse_from_bytes(v.value()).map_err(|e| format!("raft_log 里的条目损坏：{e}"))?);
            }
        }
        Ok(s)
    }

    pub fn hard_state(&self) -> &HardState {
        &self.hs
    }
    pub fn conf_state(&self) -> &ConfState {
        &self.cs
    }
    /// 最近一次压缩点（first_index − 1）。
    pub fn snap_index(&self) -> u64 {
        self.snap_index
    }

    /// 追加条目：先删 index ≥ `ents[0].index` 的旧条目（被新 leader 覆盖的部分）。
    pub fn append(&mut self, tx: &WriteTransaction, ents: &[Entry]) -> Result<(), ErrCode> {
        if ents.is_empty() {
            return Ok(());
        }
        let (first, last) = (ents[0].index, ents[ents.len() - 1].index);
        assert!(
            first > self.snap_index && first <= self.last_index + 1,
            "不变量破坏：追加的条目从 {first} 开始，而日志是 ({}, {}]",
            self.snap_index,
            self.last_index
        );
        let mut t = tx.open_table(RAFT_LOG).map_err(|e| redb_eio("打开 raft_log", e))?;
        if first <= self.last_index {
            t.retain_in(first.., |_, _| false).map_err(|e| redb_eio("删除被覆盖的条目", e))?;
        }
        for e in ents {
            t.insert(e.index, encode(e).as_slice()).map_err(|e| redb_eio("写 raft_log", e))?;
        }
        while self.recent.back().is_some_and(|e| e.index >= first) {
            self.cache_pop_back();
        }
        for e in ents {
            self.cache_push(e.clone());
        }
        self.last_index = last;
        Ok(())
    }

    pub fn set_hard_state(&mut self, tx: &WriteTransaction, hs: &HardState) -> Result<(), ErrCode> {
        put_state(tx, KEY_HARD_STATE, &encode(hs))?;
        self.hs = hs.clone();
        Ok(())
    }

    pub fn set_conf_state(&mut self, tx: &WriteTransaction, cs: &ConfState) -> Result<(), ErrCode> {
        put_state(tx, KEY_CONF_STATE, &encode(cs))?;
        self.cs = cs.clone();
        Ok(())
    }

    /// 规则 M3：删除 index ≤ upto 的条目，`snap_meta = (upto, term(upto))`。调用方保证 upto ≤ durable_applied − 10 000。
    pub fn compact(&mut self, tx: &WriteTransaction, upto: u64) -> Result<(), ErrCode> {
        assert!(
            upto > self.snap_index && upto <= self.last_index,
            "不变量破坏：压缩点 {upto} 不在日志 ({}, {}] 里",
            self.snap_index,
            self.last_index
        );
        let term = self.entry(upto).term;
        let mut t = tx.open_table(RAFT_LOG).map_err(|e| redb_eio("打开 raft_log", e))?;
        t.retain_in(..=upto, |_, _| false).map_err(|e| redb_eio("删除已压缩的条目", e))?;
        put_state(tx, KEY_SNAP_META, &SnapMeta { index: upto, term }.encode())?;
        (self.snap_index, self.snap_term) = (upto, term);
        while self.recent.front().is_some_and(|e| e.index <= upto) {
            self.cache_pop_front();
        }
        Ok(())
    }

    /// 第 idx 条（调用方保证 snap_index < idx ≤ last_index）：先查缓存，再读盘。
    fn entry(&self, idx: u64) -> Entry {
        if let Some(e) = self.cached(idx) {
            return e.clone();
        }
        let mut got = None;
        self.load(idx, idx + 1, &mut |e| {
            got = Some(e);
            false
        });
        got.unwrap_or_else(|| {
            panic!("不变量破坏：raft_log 里没有第 {idx} 条（日志是 ({}, {}]）", self.snap_index, self.last_index)
        })
    }

    fn cached(&self, idx: u64) -> Option<&Entry> {
        let first = self.recent.front()?.index;
        idx.checked_sub(first).and_then(|i| self.recent.get(i as usize))
    }

    /// 从盘上依次读 [low, high) 的条目交给 `push`，`push` 返回 false 就停（raft 线程：redb 出错 = fail-stop）。
    fn load(&self, low: u64, high: u64, push: &mut dyn FnMut(Entry) -> bool) {
        let rtx = or_abort(self.db.begin_read(), "读事务");
        let t = or_abort(rtx.open_table(RAFT_LOG), "打开 raft_log");
        for r in or_abort(t.range(low..high), "读 raft_log") {
            let (_, v) = or_abort(r, "读 raft_log");
            if !push(decode_entry(v.value())) {
                return;
            }
        }
    }

    fn cache_push(&mut self, e: Entry) {
        self.recent_bytes += size(&e);
        self.recent.push_back(e);
        while self.recent.len() > CACHE_ENTRIES || (self.recent_bytes > CACHE_BYTES && self.recent.len() > 1) {
            self.cache_pop_front();
        }
    }

    /// 从缓存头（最旧的一条）或尾（最新的一条）弹出一条，同时减掉它的字节数。
    fn cache_pop_front(&mut self) {
        if let Some(e) = self.recent.pop_front() {
            self.recent_bytes -= size(&e);
        }
    }
    fn cache_pop_back(&mut self) {
        if let Some(e) = self.recent.pop_back() {
            self.recent_bytes -= size(&e);
        }
    }
}

impl Storage for RedbStorage {
    fn initial_state(&self) -> raft::Result<RaftState> {
        Ok(RaftState::new(self.hs.clone(), self.cs.clone()))
    }

    /// [low, high)；low ≤ 压缩点 → `Compacted`；high > last_index + 1 → panic（raft 合约）。
    /// 按 `max_size` 截断，但至少返回 1 条（坑 P12，与 `raft::util::limit_size` 相同）。
    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        _ctx: GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        if low <= self.snap_index {
            return Err(RaftError::Store(StorageError::Compacted));
        }
        assert!(
            high <= self.last_index + 1,
            "raft 合约：entries 的 high {high} 超过 last_index + 1 = {}",
            self.last_index + 1
        );
        let max = max_size.into().unwrap_or(u64::MAX);
        let (mut out, mut total) = (Vec::new(), 0u64);
        // push 返回 false = 已经超过 max_size，不再要了（但至少留 1 条，同 raft::util::limit_size）
        let mut push = |e: Entry| {
            total += u64::from(e.compute_size());
            if !out.is_empty() && total > max {
                return false;
            }
            out.push(e);
            true
        };
        // [low, cache_first) 在盘上、[cache_first, high) 在缓存里（缓存装的总是最新的一段）：先读盘、再读缓存
        let cache_first = self.recent.front().map_or(high, |e| e.index.max(low));
        let mut more = true;
        if low < cache_first {
            self.load(low, cache_first.min(high), &mut |e| {
                more = push(e);
                more
            });
        }
        for idx in cache_first..high {
            match self.cached(idx) {
                Some(e) if more => more = push(e.clone()),
                _ => break,
            }
        }
        Ok(out)
    }

    /// idx == 压缩点 → 它的 term；更早 → `Compacted`；晚于 last_index → `Unavailable`。
    fn term(&self, idx: u64) -> raft::Result<u64> {
        if idx == self.snap_index {
            return Ok(self.snap_term);
        }
        if idx < self.snap_index {
            return Err(RaftError::Store(StorageError::Compacted));
        }
        if idx > self.last_index {
            return Err(RaftError::Store(StorageError::Unavailable));
        }
        Ok(self.entry(idx).term)
    }

    fn first_index(&self) -> raft::Result<u64> {
        Ok(self.snap_index + 1)
    }

    fn last_index(&self) -> raft::Result<u64> {
        Ok(self.last_index)
    }

    fn snapshot(&self, request_index: u64, to: u64) -> raft::Result<Snapshot> {
        log::warn("raft", format_args!("节点 {to} 需要 index ≥ {request_index} 的快照：快照 M8 才实现（3.4.1.7）"));
        Err(RaftError::Store(StorageError::SnapshotTemporarilyUnavailable))
    }
}

/// init 写的 index = 1、term = 1 的初始状态（3.4.1.9，与 raft-rs 的 five_mem_node 示例一致）：
/// `snap_meta = (1, 1)`、`hard_state = {term: 1, vote: 0, commit: 1}`、`conf_state = {voters}`、
/// `applied = (1, 1, state_version = 1)`、`format`，以及初始 KV（3.4.3）。`Durability::Immediate`。
pub fn bootstrap(db: &Database, voters: &[u64], kv: &[(Vec<u8>, Vec<u8>)]) -> Result<(), String> {
    let err = |e: &dyn std::fmt::Display| format!("写元数据库的初始状态失败：{e}");
    let tx = db.begin_write().map_err(|e| err(&e))?;
    {
        tx.open_table(RAFT_LOG).map_err(|e| err(&e))?;
        let mut hs = HardState::default();
        (hs.term, hs.commit) = (1, 1);
        let cs = ConfState { voters: voters.to_vec(), ..Default::default() };
        let mut st = tx.open_table(RAFT_STATE).map_err(|e| err(&e))?;
        let format = MetaFormatV1 { version: 1, incompat: vec![] }.encode();
        let applied = Applied { index: 1, term: 1, state_version: 1 }.encode();
        let snap = SnapMeta { index: 1, term: 1 }.encode();
        for (k, v) in [
            (KEY_HARD_STATE, encode(&hs)),
            (KEY_CONF_STATE, encode(&cs)),
            (KEY_SNAP_META, snap.to_vec()),
            (KEY_APPLIED, applied.to_vec()),
            (KEY_FORMAT, format),
        ] {
            st.insert(k, v.as_slice()).map_err(|e| err(&e))?;
        }
        let mut t = tx.open_table(KV).map_err(|e| err(&e))?;
        for (k, v) in kv {
            t.insert(k.as_slice(), v.as_slice()).map_err(|e| err(&e))?;
        }
    }
    tx.commit().map_err(|e| err(&e))
}

/// 读 `raft_state["applied"]`（与 KV 同事务更新，所以与读事务里的 KV 一致）。
pub fn read_applied(rtx: &ReadTransaction) -> Result<Applied, String> {
    let st = rtx.open_table(RAFT_STATE).map_err(|e| format!("打不开 raft_state 表：{e}"))?;
    let v = st.get(KEY_APPLIED).map_err(|e| format!("读 raft_state[applied] 失败：{e}"))?;
    let v = v.ok_or("raft_state 里没有 applied")?;
    Applied::decode(v.value()).map_err(|e| format!("raft_state[applied] 损坏：{e}"))
}

/// 启动时读一条已应用的 GLOBAL 记录（3.2.7）：不存在 → `Ok(None)`；读盘出错或记录解不开 → 带键名的中文错误
/// （坏了的记录不能当成"没有"或默认值，启动就此失败）。只在启动装配时用（还不在 raft / 执行器线程上）。
pub fn read_global<T: serde::de::DeserializeOwned>(rtx: &ReadTransaction, name: &str) -> Result<Option<T>, String> {
    let t = rtx.open_table(KV).map_err(|e| format!("打不开 kv 表：{e}"))?;
    let v = t.get(keys::global(name).as_slice()).map_err(|e| format!("读 GLOBAL {name} 失败：{e}"))?;
    match v {
        None => Ok(None),
        Some(v) => decode_v1(v.value()).map(Some).map_err(|e| format!("GLOBAL {name} 损坏，解不开（{e}）")),
    }
}

/// 在写事务里写 `raft_state[key]`。
pub fn put_state(tx: &WriteTransaction, key: &str, v: &[u8]) -> Result<(), ErrCode> {
    let mut t = tx.open_table(RAFT_STATE).map_err(|e| redb_eio("打开 raft_state", e))?;
    t.insert(key, v).map_err(|e| redb_eio("写 raft_state", e))?;
    Ok(())
}

fn encode(m: &dyn protobuf::Message) -> Vec<u8> {
    m.write_to_bytes().unwrap_or_else(|e| panic!("不变量破坏：protobuf 编码失败：{e}"))
}

fn decode_entry(b: &[u8]) -> Entry {
    Entry::parse_from_bytes(b).unwrap_or_else(|e| panic!("不变量破坏：raft_log 里的条目无法解码（磁盘损坏？）：{e}"))
}

fn size(e: &Entry) -> usize {
    e.data.len() + e.context.len()
}

#[cfg(test)]
mod tests;
