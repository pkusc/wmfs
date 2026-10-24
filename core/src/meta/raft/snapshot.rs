//! 元数据快照（design.md 3.4.1.7、3.2.6 的写入规则 M5，ADR-0019）：leader 一侧的导出与 pin、快照文件的格式、follower 一侧的
//! 安装要读的文件。带外传输（snap-serve 流式发出、snap-recv 下载进文件）在 `peers.rs`。
//!
//! - **导出**（`Storage::snapshot`，raft 线程上，开销很小）：已有 pin 满足 `pin.index ≥ request_index`、没被日志压缩越过、成员配置里
//!   有接收方（实现注记（M8）：见 [`Pins::export`]）就复用；
//!   否则开一个 redb 读事务（MVCC，不阻塞写），从它读出 `applied(index, term, state_version)` 与 `conf_state`（与 KV 同事务更新，
//!   所以与读事务里的 KV 一致），记成一个 [`PinnedSnap`]（最多 2 个、10 min 过期）。raft 消息里只有 `SnapDesc`——快照内容不进 raft 消息。
//! - **报告**（坑 P11）：raft-send 发出 MsgSnapshot 时记下"向谁发了哪个快照"（[`Pins::note_sent`]）；snap-serve 流成功结束 → 报告成功，
//!   出错 → 报告失败（[`Pins::take_sent`]）；60 s 内对方没完成下载 → 看门狗报告失败（[`Pins::overdue`]），leader 不会卡在 Snapshot 状态。
//! - **文件**（follower 写、安装时读）：`meta/snap-recv-<index>-<term>.snap` =
//!   `b"WMFSSNP1"` ‖ u32 长度 + postcard(SnapHeader) ‖ pairs_total 个 `{klen: u32, vlen: u32, key, value}` ‖ `b"WMFSEND!"` ‖ pairs u64 ‖
//!   crc32c u32。实现注记（M8）：规格没写整数的字节序与 crc 覆盖哪些字节，取**小端**（与帧头、`snap_meta` 相同），crc32c 按顺序覆盖每条
//!   记录的 `klen ‖ vlen ‖ key ‖ value`（leader 流式发出时按同样的字节算，放进 SnapTrailer）；先写 `.tmp`，校验通过、fsync 之后才改名，
//!   所以带最终名字的文件一定是完整的。金样 `tests/golden/snapshot.golden`。实现注记（M8 审查）：文件是整个 kv 表的副本（inline 小文件的
//!   内容、xattr、密钥元数据都在里面），与 `meta.redb` 一样只给本用户读写（0600）；启动时删掉残留的快照文件（[`cleanup`]，3.2.2）。

use std::collections::VecDeque;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::UNIX_EPOCH;

use protobuf::Message as _;
use raft::eraftpb::{ConfState, Snapshot};
use raft::{Error as RaftError, StorageError};
use redb::{Database, ReadTransaction, ReadableTable, ReadableTableMetadata};

use super::redb_eio;
use super::storage::{KV, RAFT_STATE, read_applied};
use crate::base::{ErrCode, clock, lock, sys};
use crate::meta::schema::raft_state::Applied;
use crate::meta::schema::raft_state::KEY_CONF_STATE;
use crate::proto::peer::{SnapDesc, SnapHeader, SnapTrailer};
use crate::proto::{decode_body, encode_body};

/// 最多同时保留几个 pin、多久过期；对方多久没下载完就报告失败（3.4.1.7）。
const MAX_PINS: usize = 2;
const PIN_TTL_MS: u64 = 600_000;
const SENT_TIMEOUT_MS: u64 = 60_000;
/// 超过 10 min 的残留快照文件（raft-rs 判定过旧而没有安装的）由 snap-recv 在下一次下载之前删掉（3.4.1.7）。
const STALE_FILE_MS: u64 = 600_000;
/// 一个中间帧里的键值攒到多少字节就发出去（帧 body ≤ 1 MiB；每条记录另算 16 字节的长度前缀）。
const FRAME_BYTES: usize = (1 << 20) - (16 << 10);
const MAGIC: &[u8; 8] = b"WMFSSNP1";
const END: &[u8; 8] = b"WMFSEND!";

/// 一个中间帧的 body：按 key 顺序的键值（[`PinnedSnap::frames`] 交出、snap-recv 解码的都是它）。
pub type Pairs = Vec<(Vec<u8>, Vec<u8>)>;
/// 读快照文件时，每个键值交给谁。
pub type PutFn<'a> = dyn FnMut(&[u8], &[u8]) -> Result<(), ErrCode> + 'a;

/// leader 上一个快照的 pin：一个 redb 读事务（MVCC）与它看到的 applied、conf_state。
pub struct PinnedSnap {
    pub index: u64,
    pub term: u64,
    state_version: u64,
    conf_state: ConfState,
    txn: ReadTransaction,
    created_ms: u64,
}

/// 发出去还没有结果的快照：向谁、哪个、什么时候。
struct Sent {
    to: u64,
    index: u64,
    term: u64,
    at_ms: u64,
}

/// leader 的快照 pin 与"向谁发了哪个快照"的记录：`RedbStorage`、raft-send 与 snap-serve 线程共用同一个 Arc（`snapshot()` 只拿
/// `&self`，所以用 Mutex）。
#[derive(Default)]
pub struct Pins {
    pins: Mutex<VecDeque<Arc<PinnedSnap>>>,
    sent: Mutex<Vec<Sent>>,
}

impl Pins {
    /// `Storage::snapshot` 的实现（见文件头）：返回只带 `SnapDesc` 的 raft 快照。读不了盘 → `SnapshotTemporarilyUnavailable`
    /// （raft-rs 稍后再要）。
    pub fn export(
        &self,
        db: &Database,
        request_index: u64,
        to: u64,
        snap_index: u64,
        me: u32,
    ) -> raft::Result<Snapshot> {
        let now = clock::mono_ms();
        let mut g = lock(&self.pins);
        g.retain(|p| now - p.created_ms < PIN_TTL_MS && p.index >= snap_index);
        // 实现注记（M8）：复用还要求 pin 的 conf_state 里有接收方——在 pin 之后才加入的 learner 收到一个不含自己的快照，raft-rs 会拒绝
        // 安装（restore 的"not in the ConfState"），leader 就一直重发同一个快照
        if let Some(p) = g.iter().rev().find(|p| p.index >= request_index && p.has(to)) {
            return Ok(p.snapshot(me));
        }
        let unavailable = || RaftError::Store(StorageError::SnapshotTemporarilyUnavailable);
        let txn = db.begin_read().map_err(|_| unavailable())?;
        let a = read_applied(&txn).map_err(|_| unavailable())?;
        let cs = txn
            .open_table(RAFT_STATE)
            .ok()
            .and_then(|t| t.get(KEY_CONF_STATE).ok().flatten().map(|v| v.value().to_vec()));
        let conf_state = ConfState::parse_from_bytes(&cs.ok_or_else(unavailable)?).map_err(|_| unavailable())?;
        let p = Arc::new(PinnedSnap {
            index: a.index,
            term: a.term,
            state_version: a.state_version,
            conf_state,
            txn,
            created_ms: now,
        });
        if g.len() >= MAX_PINS {
            g.pop_front();
        }
        g.push_back(p.clone());
        Ok(p.snapshot(me))
    }

    /// snap-serve 要的那个 pin（没有了 → None：follower 下载失败，leader 稍后重发 MsgSnapshot）。
    pub fn find(&self, index: u64, term: u64) -> Option<Arc<PinnedSnap>> {
        lock(&self.pins).iter().find(|p| p.index == index && p.term == term).cloned()
    }

    /// raft-send 发出了一个 MsgSnapshot（同一个对端只记最新的一个）。
    pub fn note_sent(&self, to: u64, index: u64, term: u64) {
        let mut s = lock(&self.sent);
        s.retain(|x| x.to != to);
        s.push(Sent { to, index, term, at_ms: clock::mono_ms() });
    }

    /// snap-serve 发完（或发失败）了 (index, term)：收到它的对端（报告结果用），并忘掉它们。
    pub fn take_sent(&self, index: u64, term: u64) -> Vec<u64> {
        let mut s = lock(&self.sent);
        let to = s.iter().filter(|x| x.index == index && x.term == term).map(|x| x.to).collect();
        s.retain(|x| x.index != index || x.term != term);
        to
    }

    /// 到 `now_ms`（单调时钟）为止，发给 `to` 的快照已经超过 60 s 还没下载完：忘掉它，调用方报告失败（坑 P11 的看门狗）。
    pub fn overdue(&self, to: u64, now_ms: u64) -> bool {
        let mut s = lock(&self.sent);
        let late = s.iter().any(|x| x.to == to && now_ms.saturating_sub(x.at_ms) > SENT_TIMEOUT_MS);
        s.retain(|x| x.to != to || !late);
        late
    }
}

impl PinnedSnap {
    /// 这个快照的成员配置里有 `id` 吗（voter、learner 或 joint 里旧配置的 voter）。
    fn has(&self, id: u64) -> bool {
        let cs = &self.conf_state;
        cs.voters.contains(&id) || cs.learners.contains(&id) || cs.voters_outgoing.contains(&id)
    }

    fn snapshot(&self, me: u32) -> Snapshot {
        let mut s = Snapshot::default();
        let m = s.mut_metadata();
        (m.index, m.term) = (self.index, self.term);
        m.set_conf_state(self.conf_state.clone());
        s.data = encode_body(&SnapDesc { index: self.index, term: self.term, source_node: me }).into();
        s
    }

    /// 流的首帧。
    pub fn header(&self) -> Result<SnapHeader, ErrCode> {
        let t = self.txn.open_table(KV).map_err(|e| redb_eio("快照：打开 kv 表", e))?;
        let pairs_total = t.len().map_err(|e| redb_eio("快照：数 kv 表", e))?;
        let conf_state = self.conf_state.write_to_bytes().map_err(|_| ErrCode::INTERNAL)?;
        Ok(SnapHeader {
            index: self.index,
            term: self.term,
            state_version: self.state_version,
            conf_state,
            pairs_total,
        })
    }

    /// 按 key 顺序遍历 kv 表，每攒够约 1 MiB 交给 `frame` 一次（一个中间帧）；返回 (条数, crc32c)，放进 SnapTrailer。
    pub fn frames(&self, frame: &mut dyn FnMut(Pairs) -> Result<(), ErrCode>) -> Result<(u64, u32), ErrCode> {
        let t = self.txn.open_table(KV).map_err(|e| redb_eio("快照：打开 kv 表", e))?;
        let (mut batch, mut bytes, mut n, mut crc) = (Vec::new(), 0usize, 0u64, 0u32);
        for r in t.iter().map_err(|e| redb_eio("快照：遍历 kv", e))? {
            let (k, v) = r.map_err(|e| redb_eio("快照：遍历 kv", e))?;
            let (k, v) = (k.value().to_vec(), v.value().to_vec());
            let size = k.len() + v.len() + 16;
            if !batch.is_empty() && bytes + size > FRAME_BYTES {
                frame(std::mem::take(&mut batch))?;
                bytes = 0;
            }
            (crc, n, bytes) = (record_crc(crc, &k, &v), n + 1, bytes + size);
            batch.push((k, v));
        }
        if !batch.is_empty() {
            frame(batch)?;
        }
        Ok((n, crc))
    }
}

/// 依次把一条 kv 记录的字节交给 `feed`：`klen ‖ vlen ‖ key ‖ value`（长度 u32 小端）——快照文件（[`SnapWriter::pairs`]）、它的 crc32c
/// 与 meta-hash 都按这个顺序。
fn record(k: &[u8], v: &[u8], feed: &mut dyn FnMut(&[u8])) {
    feed(&(k.len() as u32).to_le_bytes());
    feed(&(v.len() as u32).to_le_bytes());
    feed(k);
    feed(v);
}

/// 一条记录（[`record`] 的字节）接在 `crc` 之后的 crc32c（SnapTrailer 的 crc32c = 从 0 开始依次接上每条记录）。
pub fn record_crc(crc: u32, k: &[u8], v: &[u8]) -> u32 {
    let mut crc = crc;
    record(k, v, &mut |b| crc = crc32c::crc32c_append(crc, b));
    crc
}

/// 快照文件的路径。
pub fn file_path(dir: &Path, index: u64, term: u64) -> PathBuf {
    dir.join(format!("snap-recv-{index}-{term}.snap"))
}

/// 删掉 `dir` 里的快照文件（`snap-recv-*`，含没下载完的 `.snap.tmp`）：`all` = 全部（Driver::open，raft 线程与 snap-recv 都还没起，
/// 3.2.2），否则只删超过 10 min 的（snap-recv 每次下载之前：raft-rs 判定过旧而没有安装的，3.4.1.7）。文件的年龄按 mtime 与
/// `clock::wall_ms()` 比（0.2 第 6 条）；读不到 mtime 的当作不旧。
pub fn cleanup(dir: &Path, all: bool) {
    let mtime_ms = |e: &std::fs::DirEntry| {
        let t = e.metadata().and_then(|m| m.modified()).ok()?;
        Some(t.duration_since(UNIX_EPOCH).ok()?.as_millis() as u64)
    };
    let stale = |e: &std::fs::DirEntry| all || mtime_ms(e).is_some_and(|t| t + STALE_FILE_MS < clock::wall_ms());
    let ours = |e: &std::fs::DirEntry| e.file_name().to_string_lossy().starts_with("snap-recv-");
    for e in std::fs::read_dir(dir).into_iter().flatten().flatten().filter(|e| ours(e) && stale(e)) {
        let _ = std::fs::remove_file(e.path());
    }
}

fn io(e: std::io::Error) -> ErrCode {
    ErrCode::from(e)
}

/// 快照文件的写入端（follower 的 snap-recv）：先写 `.tmp`，[`SnapWriter::finish`] 校验、fsync 之后才改成最终的名字。
pub struct SnapWriter {
    w: BufWriter<File>,
    tmp: PathBuf,
    path: PathBuf,
    header: SnapHeader,
    pairs: u64,
    crc: u32,
}

impl SnapWriter {
    pub fn create(dir: &Path, h: SnapHeader) -> Result<SnapWriter, ErrCode> {
        let path = file_path(dir, h.index, h.term);
        let tmp = path.with_extension("snap.tmp");
        // 整个 kv 表的副本：与 meta.redb 一样只给本用户读写（0600，3.2.1 的实现注记）
        let f = std::fs::OpenOptions::new().write(true).create(true).truncate(true).mode(0o600).open(&tmp);
        let mut w = BufWriter::new(f.map_err(io)?);
        let hb = encode_body(&h);
        w.write_all(MAGIC).and_then(|()| w.write_all(&(hb.len() as u32).to_le_bytes())).map_err(io)?;
        w.write_all(&hb).map_err(io)?;
        Ok(SnapWriter { w, tmp, path, header: h, pairs: 0, crc: 0 })
    }

    /// 追加一个中间帧里的键值。
    pub fn pairs(&mut self, kv: &[(Vec<u8>, Vec<u8>)]) -> Result<(), ErrCode> {
        for (k, v) in kv {
            self.w.write_all(&(k.len() as u32).to_le_bytes()).map_err(io)?;
            self.w.write_all(&(v.len() as u32).to_le_bytes()).map_err(io)?;
            self.w.write_all(k).and_then(|()| self.w.write_all(v)).map_err(io)?;
            (self.crc, self.pairs) = (record_crc(self.crc, k, v), self.pairs + 1);
        }
        Ok(())
    }

    /// 末帧：条数与 crc 都对得上（也等于首帧的 pairs_total）才写结尾、fsync、改名；否则 `EIO`（删掉临时文件）。返回最终路径。
    pub fn finish(mut self, t: &SnapTrailer) -> Result<PathBuf, ErrCode> {
        let ok = t.pairs == self.pairs && t.crc32c == self.crc && t.pairs == self.header.pairs_total;
        let r = if ok { self.seal(t) } else { Err(ErrCode::EIO) };
        if r.is_err() {
            let _ = std::fs::remove_file(&self.tmp);
        }
        r.map(|()| self.path)
    }

    fn seal(&mut self, t: &SnapTrailer) -> Result<(), ErrCode> {
        self.w.write_all(END).map_err(io)?;
        self.w
            .write_all(&t.pairs.to_le_bytes())
            .and_then(|()| self.w.write_all(&t.crc32c.to_le_bytes()))
            .map_err(io)?;
        self.w.flush().map_err(io)?;
        self.w.get_ref().sync_all().map_err(io)?;
        std::fs::rename(&self.tmp, &self.path).map_err(io)?;
        sys::fsync_dir(self.path.parent().unwrap_or(Path::new(".")))
    }
}

/// 读一个快照文件：校验魔数、条数、结尾与 crc，按顺序把每个键值交给 `put`；返回首帧的 SnapHeader。坏了 → `EIO`。
pub fn read_file(path: &Path, put: &mut PutFn<'_>) -> Result<SnapHeader, ErrCode> {
    let mut r = BufReader::new(File::open(path).map_err(io)?);
    let mut take = |n: usize| -> Result<Vec<u8>, ErrCode> {
        let mut b = vec![0u8; n];
        r.read_exact(&mut b).map_err(|_| ErrCode::EIO)?;
        Ok(b)
    };
    let le32 = |b: &[u8]| u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as usize;
    if take(8)? != MAGIC {
        return Err(ErrCode::EIO);
    }
    let n = le32(&take(4)?);
    let h: SnapHeader = decode_body(&take(n)?).map_err(|_| ErrCode::EIO)?;
    let mut crc = 0;
    for _ in 0..h.pairs_total {
        let lens = take(8)?;
        let (k, v) = (take(le32(&lens[..4]))?, take(le32(&lens[4..]))?);
        crc = record_crc(crc, &k, &v);
        put(&k, &v)?;
    }
    let tail = take(20)?;
    let pairs = u64::from_le_bytes(tail[8..16].try_into().map_err(|_| ErrCode::EIO)?);
    if tail[..8] != END[..] || pairs != h.pairs_total || le32(&tail[16..]) as u32 != crc {
        return Err(ErrCode::EIO);
    }
    Ok(h)
}

/// 本节点已应用的状态：(applied, kv 表的条数, sha256(按 key 顺序的每条记录，[`record`] 的字节) 的前 16 字节的 hex)。
/// applied 与 kv 来自同一个读事务，所以各节点 applied 相同时哈希必须相同（`wmfs debug meta-hash`）。
pub fn kv_hash(db: &Database) -> Result<(Applied, u64, String), ErrCode> {
    use sha2::{Digest, Sha256};
    let rtx = db.begin_read().map_err(|e| redb_eio("meta-hash 的读事务", e))?;
    let a = read_applied(&rtx).map_err(|_| ErrCode::EIO)?;
    let t = rtx.open_table(KV).map_err(|e| redb_eio("meta-hash：打开 kv 表", e))?;
    let (mut h, mut n) = (Sha256::new(), 0u64);
    for r in t.iter().map_err(|e| redb_eio("meta-hash：遍历 kv", e))? {
        let (k, v) = r.map_err(|e| redb_eio("meta-hash：遍历 kv", e))?;
        record(k.value(), v.value(), &mut |b| h.update(b));
        n += 1;
    }
    Ok((a, n, wmfs_api::hex::hex(&h.finalize()[..16])))
}

#[cfg(test)]
mod tests;
