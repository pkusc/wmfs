//! raft `Storage` 合约测试（design.md 9.1：照 raft-rs `MemStorage` 的测试改写——entries/term/compact 的边界、
//! max_size 至少一条、覆盖写、压缩点持久化），跑在 redb 自带的内存后端上。

use std::sync::Arc;

use raft::eraftpb::{ConfState, Entry, HardState};
use raft::{Error as RaftError, GetEntriesContext, Storage, StorageError};
use redb::{Database, Durability};

use super::*;
use crate::meta::schema::raft_state::{Applied, MetaFormatV1};

fn db() -> Arc<Database> {
    Arc::new(Database::builder().create_with_backend(redb::backends::InMemoryBackend::new()).unwrap())
}

fn entry(index: u64, term: u64) -> Entry {
    let mut e = Entry::default();
    (e.index, e.term) = (index, term);
    e.data = vec![index as u8; 3].into();
    e
}

/// 在一个写事务里做 `f`，然后提交。
fn write(db: &Database, f: impl FnOnce(&WriteTransaction)) {
    let mut tx = db.begin_write().unwrap();
    tx.set_durability(Durability::Immediate);
    f(&tx);
    tx.commit().unwrap();
}

/// init 的初始状态 + 追加 2..=5（term = index）+ 压缩到 2：压缩点 (2, 2)，日志里是 3、4、5。
fn storage_3_to_5() -> (Arc<Database>, RedbStorage) {
    let db = db();
    bootstrap(&db, &[1], &[]).unwrap();
    let mut s = RedbStorage::open(db.clone()).unwrap();
    write(&db, |tx| s.append(tx, &[entry(2, 2), entry(3, 3), entry(4, 4), entry(5, 5)]).unwrap());
    write(&db, |tx| s.compact(tx, 2).unwrap());
    (db, s)
}

fn ents(s: &RedbStorage, lo: u64, hi: u64, max: u64) -> raft::Result<Vec<Entry>> {
    s.entries(lo, hi, max, GetEntriesContext::empty(false))
}

#[test]
fn bootstrap_state_is_index_1_term_1() {
    let db = db();
    bootstrap(&db, &[1], &[(b"k".to_vec(), b"v".to_vec())]).unwrap();
    let s = RedbStorage::open(db.clone()).unwrap();
    assert_eq!((s.first_index(), s.last_index(), s.term(1)), (Ok(2), Ok(1), Ok(1)));
    let st = s.initial_state().unwrap();
    assert_eq!((st.hard_state.term, st.hard_state.vote, st.hard_state.commit), (1, 0, 1));
    assert_eq!(st.conf_state.voters, vec![1]);
    let rtx = db.begin_read().unwrap();
    assert_eq!(read_applied(&rtx), Ok(Applied { index: 1, term: 1, state_version: 1 }));
    let kv = rtx.open_table(KV).unwrap();
    assert_eq!(kv.get(b"k".as_slice()).unwrap().unwrap().value(), b"v");
    assert_eq!(s.term(2), Err(RaftError::Store(StorageError::Unavailable)));
}

#[test]
fn term_boundaries() {
    let (_db, s) = storage_3_to_5();
    assert_eq!(s.term(1), Err(RaftError::Store(StorageError::Compacted)));
    assert_eq!(s.term(2), Ok(2), "压缩点本身的 term 保留（用于日志匹配）");
    assert_eq!((s.term(3), s.term(4), s.term(5)), (Ok(3), Ok(4), Ok(5)));
    assert_eq!(s.term(6), Err(RaftError::Store(StorageError::Unavailable)));
    assert_eq!((s.first_index(), s.last_index()), (Ok(3), Ok(5)));
}

#[test]
fn entries_boundaries_and_max_size() {
    let (_db, s) = storage_3_to_5();
    let sz = |i: u64| u64::from(entry(i, i).compute_size());
    assert_eq!(ents(&s, 2, 6, u64::MAX), Err(RaftError::Store(StorageError::Compacted)));
    assert_eq!(ents(&s, 3, 4, u64::MAX), Ok(vec![entry(3, 3)]));
    assert_eq!(ents(&s, 4, 6, u64::MAX), Ok(vec![entry(4, 4), entry(5, 5)]));
    assert_eq!(ents(&s, 3, 6, 0), Ok(vec![entry(3, 3)]), "max_size = 0 也至少返回 1 条（坑 P12）");
    assert_eq!(ents(&s, 3, 6, sz(3) + sz(4)), Ok(vec![entry(3, 3), entry(4, 4)]));
    assert_eq!(ents(&s, 3, 6, sz(3) + sz(4) + sz(5) - 1), Ok(vec![entry(3, 3), entry(4, 4)]));
    assert_eq!(ents(&s, 3, 6, sz(3) + sz(4) + sz(5)).map(|v| v.len()), Ok(3));
    let none: Option<u64> = None;
    assert_eq!(s.entries(3, 6, none, GetEntriesContext::empty(false)).map(|v| v.len()), Ok(3), "None = 不限");
}

#[test]
#[should_panic(expected = "raft 合约")]
fn entries_beyond_last_index_plus_one_panic() {
    let (_db, s) = storage_3_to_5();
    let _ = ents(&s, 3, 7, u64::MAX);
}

#[test]
fn append_overwrites_conflicting_suffix_and_persists() {
    let (db, mut s) = storage_3_to_5();
    write(&db, |tx| s.append(tx, &[entry(4, 6), entry(5, 6)]).unwrap());
    assert_eq!(ents(&s, 3, 6, u64::MAX), Ok(vec![entry(3, 3), entry(4, 6), entry(5, 6)]));
    write(&db, |tx| s.append(tx, &[entry(6, 6), entry(7, 7)]).unwrap());
    write(&db, |tx| s.append(tx, &[entry(5, 8)]).unwrap());
    assert_eq!(s.last_index(), Ok(5), "从 5 开始覆盖：6、7 被删掉");
    assert_eq!(s.term(5), Ok(8));
    let again = RedbStorage::open(db.clone()).unwrap();
    assert_eq!((again.first_index(), again.last_index(), again.term(2)), (Ok(3), Ok(5), Ok(2)), "压缩点与日志都已落盘");
    assert_eq!(ents(&again, 3, 6, u64::MAX), Ok(vec![entry(3, 3), entry(4, 6), entry(5, 8)]));
}

#[test]
#[should_panic(expected = "不变量破坏")]
fn append_with_a_gap_panics() {
    let (db, mut s) = storage_3_to_5();
    write(&db, |tx| s.append(tx, &[entry(7, 7)]).unwrap());
}

#[test]
fn entries_beyond_the_cache_are_read_from_disk() {
    let db = db();
    bootstrap(&db, &[1], &[]).unwrap();
    let mut s = RedbStorage::open(db.clone()).unwrap();
    let all: Vec<Entry> = (2..5002).map(|i| entry(i, 2)).collect();
    write(&db, |tx| s.append(tx, &all).unwrap());
    assert!(s.cached(10).is_none() && s.cached(5001).is_some(), "缓存只留最近 4096 条");
    assert_eq!(ents(&s, 10, 13, u64::MAX), Ok(vec![entry(10, 2), entry(11, 2), entry(12, 2)]));
    let span = ents(&s, 900, 1000, u64::MAX).unwrap();
    assert_eq!(span.len(), 100, "跨过缓存边界（盘上 + 缓存）也连续");
    assert!(span.iter().zip(900..).all(|(e, i)| e.index == i));
    assert_eq!(s.term(3), Ok(2));
    let reopened = RedbStorage::open(db).unwrap();
    assert_eq!(ents(&reopened, 2, 4, u64::MAX), Ok(vec![entry(2, 2), entry(3, 2)]));
}

#[test]
fn compact_moves_the_first_index_and_keeps_the_term() {
    let (db, mut s) = storage_3_to_5();
    write(&db, |tx| s.compact(tx, 4).unwrap());
    assert_eq!((s.first_index(), s.term(4), s.term(5)), (Ok(5), Ok(4), Ok(5)));
    assert_eq!(s.term(3), Err(RaftError::Store(StorageError::Compacted)));
    assert_eq!(ents(&s, 4, 6, u64::MAX), Err(RaftError::Store(StorageError::Compacted)));
    let again = RedbStorage::open(db).unwrap();
    assert_eq!((again.first_index(), again.last_index(), again.term(4)), (Ok(5), Ok(5), Ok(4)));
}

#[test]
fn hard_state_and_conf_state_persist() {
    let (db, mut s) = storage_3_to_5();
    let mut hs = HardState::default();
    (hs.term, hs.vote, hs.commit) = (6, 1, 5);
    let mut cs = ConfState::default();
    (cs.voters, cs.learners) = (vec![1], vec![2, 3]);
    write(&db, |tx| {
        s.set_hard_state(tx, &hs).unwrap();
        s.set_conf_state(tx, &cs).unwrap();
    });
    assert_eq!((s.hard_state(), s.conf_state()), (&hs, &cs));
    let st = RedbStorage::open(db).unwrap().initial_state().unwrap();
    assert_eq!((st.hard_state, st.conf_state), (hs, cs));
}

#[test]
fn snapshot_is_not_available_until_m8() {
    let (_db, s) = storage_3_to_5();
    assert_eq!(s.snapshot(5, 2), Err(RaftError::Store(StorageError::SnapshotTemporarilyUnavailable)));
}

#[test]
fn open_rejects_unknown_formats_and_foreign_files() {
    let err = |db| RedbStorage::open(db).err().unwrap_or_default();
    assert!(err(db()).contains("raft_state"), "不是 init 建的库");
    let (db, _s) = storage_3_to_5();
    let fmt = MetaFormatV1 { version: 1, incompat: vec!["meta.ec-layout".into()] };
    write(&db, |tx| put_state(tx, KEY_FORMAT, &fmt.encode()).unwrap());
    assert!(err(db).contains("不受本版本支持"), "不认识的 incompat 特性 → 拒绝（3.2.10）");
}
