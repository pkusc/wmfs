//! 执行器的单元测试（design.md 9.1"执行器 + 覆盖层"）：写集 ≥ 768 KiB 结束本批、256 个操作、独占操作与新链结束本批、
//! 操作级回滚、去重（同 seq 重放得到同结果、错误也写去重记录、点删 seq − 256）、被驱逐的会话、批次结局到回复的映射，
//! 以及带种子的随机操作序列 vs 一个直接改 BTreeMap 的参考模型（失败时消息里有种子）。

use std::collections::BTreeMap;
use std::sync::Arc;

use crossbeam_channel::{Receiver, Sender, bounded, unbounded};
use redb::{Database, ReadableTable};

use super::*;
use crate::base::rand::XorShift64;
use crate::base::types::{Principal, Role, Ts};
use crate::hooks::pipeline::Pipeline;
use crate::hooks::registry::PluginSet;
use crate::hooks::traits::ConnIdentity;
use crate::meta::engine::txn::{KvRead, KvTxn};
use crate::meta::raft::storage::{KV, bootstrap};
use crate::meta::schema::keys;
use crate::meta::schema::records::{DedupV1, KvOp, SessionRecV1, encode_v1};
use crate::proto::encode_body;

fn db_with(kv: &[(Vec<u8>, Vec<u8>)]) -> Database {
    let db = Database::builder().create_with_backend(redb::backends::InMemoryBackend::new()).unwrap();
    bootstrap(&db, &[1], kv).unwrap();
    db
}

fn env() -> OpEnv {
    OpEnv::new(Arc::new(Pipeline::new(PluginSet::new(&[]))), [0; 16], Default::default())
}

fn ctx(session: u64, seq: u64) -> ReqCtx {
    let conn = Arc::new(ConnIdentity {
        key_id: 1,
        role: Role::Admin,
        tenant: 0,
        uid_ranges: vec![(0, u32::MAX)],
        root_squash: false,
        path_caps: vec![],
    });
    let who = Principal { uid: 0, gid: 0, groups: vec![], tenant: 0, job_id: 0, conn };
    ReqCtx { who, session, seq, pid: 1, trace_id: 7, now: Ts::default(), now_ms: 0, internal: false }
}

fn put_op(key: Vec<u8>, value: Vec<u8>) -> TxnOp {
    Box::new(move |v, _c| {
        v.put(&key, value);
        Ok(MetaReply::Ok)
    })
}

/// 一个"写 key = value"的请求；结果发进 `out`。
fn req(key: &str, value: Vec<u8>, exclusive: bool, out: &Sender<Result<MetaReply, ErrCode>>) -> TxnRequest {
    let out = out.clone();
    let done = Box::new(move |r| {
        let _ = out.send(r);
    });
    TxnRequest { ctx: ctx(0, 0), op: put_op(key.as_bytes().to_vec(), value), done, exclusive }
}

/// 在 `reqs` 上跑一次 fill：返回执行了几个、留在队列里几个、写集字节数。
fn batch(reqs: Vec<TxnRequest>) -> (usize, usize, BatchView) {
    let db = db_with(&[]);
    let rtx = db.begin_read().unwrap();
    let mut view = BatchView::new(&rtx);
    let (tx, rx): (Sender<TxnRequest>, Receiver<TxnRequest>) = unbounded();
    let mut it = reqs.into_iter();
    let first = it.next().unwrap();
    it.for_each(|r| tx.send(r).unwrap());
    let (done, traces) = fill(&mut view, first, &rx, &env());
    assert_eq!(traces.len(), done.len(), "每个请求的 trace_id 都进了 WriteBatch.trace_ids");
    (done.len(), rx.len(), view)
}

#[test]
fn write_set_of_768_kib_ends_the_batch() {
    let (out, _rx) = unbounded();
    let reqs = (0..20).map(|i| req(&format!("k{i:02}"), vec![7; 100 << 10], false, &out)).collect();
    let (n, left, view) = batch(reqs);
    // 每个 100 KiB + 3 字节的键：第 8 个执行完累计 ≥ 768 KiB，不再从队列取
    assert_eq!((n, left), (8, 12), "边取边执行：到了 768 KiB 就结束本批，没取的留给下一批");
    assert!(view.encoded_len() >= MAX_BATCH_BYTES && view.encoded_len() < MAX_BATCH_BYTES + (101 << 10));
}

#[test]
fn at_most_256_ops_per_batch() {
    let (out, _rx) = unbounded();
    let reqs = (0..300).map(|i| req(&format!("k{i}"), vec![1], false, &out)).collect();
    let (n, left, _) = batch(reqs);
    assert_eq!((n, left), (MAX_BATCH_OPS, 300 - MAX_BATCH_OPS));
}

#[test]
fn exclusive_ops_and_new_chains_end_the_batch() {
    let (out, _rx) = unbounded();
    let reqs = vec![req("a", vec![1], false, &out), req("b", vec![2], true, &out), req("c", vec![3], false, &out)];
    assert_eq!(batch(reqs).0, 2, "独占操作必须是本批最后一个（规则 5）");
    let chain = String::from_utf8(keys::chain(5)).unwrap();
    let reqs = vec![req(&chain, vec![1], false, &out), req("z", vec![2], false, &out)];
    let (n, _, view) = batch(reqs);
    assert!(n == 1 && view.topology_changed(), "生成了新链（写了 CHAIN 键）就结束本批");
}

#[test]
fn ops_are_sorted_last_write_wins_and_bytes_count_the_final_value() {
    let (out, _rx) = unbounded();
    let del: TxnOp = Box::new(|v, _c| {
        v.delete(b"a");
        Ok(MetaReply::Ok)
    });
    let (dtx, _drx) = bounded(1);
    let d = TxnRequest { ctx: ctx(0, 0), op: del, done: Box::new(move |r| drop(dtx.send(r))), exclusive: false };
    let reqs = vec![
        req("b", vec![1; 10], false, &out),
        req("a", vec![2; 5], false, &out),
        req("b", vec![3; 4], false, &out),
        d,
    ];
    let (_, _, view) = batch(reqs);
    assert_eq!(view.encoded_len(), (1 + 4) + 1, "b 只算最后一次（4 字节值），a 被删（只算键）");
    let ops = view.into_ops();
    assert_eq!(ops, vec![KvOp::Delete { key: b"a".to_vec() }, KvOp::Put { key: b"b".to_vec(), value: vec![3; 4] }]);
}

/// 会话 9 的记录（Active）。
fn session9() -> (Vec<u8>, Vec<u8>) {
    let s = SessionRecV1 {
        client_name: "t".into(),
        host: "h".into(),
        host_id: "h".into(),
        mount: "/m".into(),
        key_id: 2,
        tenant: 0,
        created_ms: 0,
        locked_inos: vec![],
        state: 1,
        evict_since_ms: 0,
        evict_map_epoch: 0,
    };
    (keys::session(9), encode_v1(&s))
}

/// 在一个视图上直接 run_one（会话 9 是 Active）。
fn view_with_session() -> (Database, BatchView) {
    let db = db_with(&[session9()]);
    let view = BatchView::new(&db.begin_read().unwrap());
    (db, view)
}

#[test]
fn failed_ops_roll_back_and_final_results_are_deduped() {
    let (_db, mut v) = view_with_session();
    let e = env();
    let fails: TxnOp = Box::new(|v, _c| {
        v.put(b"half", b"written".to_vec());
        Err(ErrCode::EEXIST)
    });
    assert_eq!(run_one(&mut v, &e, &ctx(9, 1), fails), Err(ErrCode::EEXIST));
    assert_eq!(v.get(b"half"), Ok(None), "失败的操作整体丢弃（原子性）");
    assert!(v.get(&keys::dedup(9, 1)).unwrap().is_some(), "错误也写去重记录：批次写集必然非空");
    let again = put_op(b"x".to_vec(), vec![1]);
    assert_eq!(run_one(&mut v, &e, &ctx(9, 1), again), Err(ErrCode::EEXIST), "同 seq 重放得到原结果，不再执行");
    assert_eq!(v.get(b"x"), Ok(None));
    let ok = put_op(b"y".to_vec(), vec![2]);
    assert_eq!(run_one(&mut v, &e, &ctx(9, 2), ok), Ok(MetaReply::Ok));
    assert_eq!(run_one(&mut v, &e, &ctx(9, 2), put_op(b"y".to_vec(), vec![3])), Ok(MetaReply::Ok));
    assert_eq!(v.get(b"y"), Ok(Some(vec![2])), "重试不会再改一次");
    let busy: TxnOp = Box::new(|_v, _c| Err(ErrCode::BUSY));
    assert_eq!(run_one(&mut v, &e, &ctx(9, 3), busy), Err(ErrCode::BUSY));
    assert_eq!(v.get(&keys::dedup(9, 3)), Ok(None), "集群类错误码（≥ 1000）= 没有执行：不写去重表");
}

#[test]
fn dedup_window_deletes_seq_minus_256_and_evicted_sessions_are_rejected() {
    let (_db, mut v) = view_with_session();
    let e = env();
    v.put(&keys::dedup(9, 44), vec![1]);
    v.commit_op();
    assert_eq!(run_one(&mut v, &e, &ctx(9, 300), put_op(b"k".to_vec(), vec![1])), Ok(MetaReply::Ok));
    assert_eq!(v.get(&keys::dedup(9, 44)), Ok(None), "写入 seq 300 的结果时点删 seq 44（规则 4）");
    let r = run_one(&mut v, &e, &ctx(8, 1), put_op(b"k".to_vec(), vec![2]));
    assert_eq!(r, Err(ErrCode::SESSION_EVICTED), "没有（或已驱逐的）会话：驱逐是单调的事实");
    assert_eq!(run_one(&mut v, &e, &ctx(8, 0), put_op(b"k".to_vec(), vec![3])), Ok(MetaReply::Ok), "seq 0 不查会话");
}

#[test]
fn batch_outcomes_map_to_replies() {
    let applied = ProposeOutcome::Applied { ok: true };
    assert_eq!(settle(applied, Ok(MetaReply::Ok)), Ok(MetaReply::Ok));
    assert_eq!(settle(applied, Err(ErrCode::EEXIST)), Err(ErrCode::EEXIST), "业务错误原样回给客户端");
    let stale = ProposeOutcome::Applied { ok: false };
    assert_eq!(settle(stale, Ok(MetaReply::Ok)), Err(ErrCode::RETRY), "批次作废：一个 op 也没应用，绝不能回成功");
    assert_eq!(settle(ProposeOutcome::Dropped, Ok(MetaReply::Ok)), Err(ErrCode::RETRY), "没进日志");
    assert_eq!(settle(ProposeOutcome::Lost, Ok(MetaReply::Ok)), Err(ErrCode::NOT_LEADER), "结果不确定：去重表兜底");
}

// ---- 9.1：随机操作序列 vs 参考模型 ----

/// 参考模型：批次开始时的已提交状态 + 本批写下的（None = 删除），外加去重表里记着的结果。直接改 BTreeMap。
#[derive(Default)]
struct Model {
    base: BTreeMap<Vec<u8>, Vec<u8>>,
    batch: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    replies: BTreeMap<u64, Result<MetaReply, ErrCode>>, // 会话 9 的 seq → 原结果
}

impl Model {
    fn get(&self, k: &[u8]) -> Option<Vec<u8>> {
        self.batch.get(k).cloned().unwrap_or_else(|| self.base.get(k).cloned())
    }

    /// run_one 的语义：驱逐的会话 → SESSION_EVICTED；去重命中 → 原结果；否则执行（失败就整体丢弃写入），
    /// 最终结果（成功或 POSIX errno）写进去重表并删掉 seq − 256 的记录。
    fn run(
        &mut self,
        seq: u64,
        session: u64,
        writes: &[(Vec<u8>, Option<Vec<u8>>)],
        probe: &[u8],
        fail: Option<ErrCode>,
    ) -> Result<MetaReply, ErrCode> {
        if seq != 0 && session != 9 {
            return Err(ErrCode::SESSION_EVICTED);
        }
        if let Some(r) = self.replies.get(&seq).filter(|_| seq != 0) {
            return r.clone();
        }
        let op: BTreeMap<Vec<u8>, Option<Vec<u8>>> = writes.iter().cloned().collect(); // 同 key 后写的为准
        let res = match fail {
            Some(c) => Err(c),
            None => Ok(MetaReply::DebugValue { value: op.get(probe).cloned().unwrap_or_else(|| self.get(probe)) }),
        };
        if res.is_ok() {
            self.batch.extend(op);
        }
        if seq != 0 && res.as_ref().err().is_none_or(|c| c.is_posix()) {
            let rec = match &res {
                Ok(reply) => DedupV1 { code: 0, reply: encode_body(reply) },
                Err(c) => DedupV1 { code: c.0, reply: vec![] },
            };
            self.batch.insert(keys::dedup(9, seq), Some(encode_v1(&rec)));
            self.replies.insert(seq, res.clone());
            if seq > DEDUP_WINDOW {
                self.batch.insert(keys::dedup(9, seq - DEDUP_WINDOW), None);
                self.replies.remove(&(seq - DEDUP_WINDOW));
            }
        }
        res
    }
}

fn random_value(r: &mut XorShift64) -> Vec<u8> {
    let mut v = vec![0u8; r.below(20) as usize];
    r.fill(&mut v);
    v
}

fn run_model(seed: u64) {
    let mut r = XorShift64::new(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
    let data_keys: Vec<Vec<u8>> = (0..6).map(|i| keys::debug(format!("k{i}").as_bytes())).collect();
    let seqs = [0, 1, 2, 3, 4, 257, 258, 259, 260]; // 257.. 写入时点删 1..（规则 4）
    let mut m = Model::default();
    m.base.extend([session9()]);
    for k in &data_keys {
        if r.below(2) == 0 {
            m.base.insert(k.clone(), random_value(&mut r));
        }
    }
    let db = db_with(&m.base.iter().map(|(k, v)| (k.clone(), v.clone())).collect::<Vec<_>>());
    let mut view = BatchView::new(&db.begin_read().unwrap());
    let env = env();
    let probe_keys: Vec<Vec<u8>> =
        data_keys.iter().cloned().chain(seqs[1..].iter().map(|s| keys::dedup(9, *s))).collect();
    for step in 0..40 {
        let at = format!("种子 {seed} 第 {step} 步");
        let seq = seqs[r.below(seqs.len() as u64) as usize];
        let session = if r.below(8) == 0 { 8 } else { 9 }; // 会话 8 不存在（= 已驱逐）
        let writes: Vec<(Vec<u8>, Option<Vec<u8>>)> = (0..r.below(4))
            .map(|_| {
                let k = data_keys[r.below(data_keys.len() as u64) as usize].clone();
                (k, if r.below(4) == 0 { None } else { Some(random_value(&mut r)) })
            })
            .collect();
        let probe = data_keys[r.below(data_keys.len() as u64) as usize].clone();
        let fail = [None, None, Some(ErrCode::EEXIST), Some(ErrCode::BUSY)][r.below(4) as usize]; // 写了一半再失败
        let want = m.run(seq, session, &writes, &probe, fail);
        let (w, p) = (writes.clone(), probe.clone());
        let op: TxnOp = Box::new(move |v, _c| {
            for (k, val) in w {
                match val {
                    Some(x) => v.put(&k, x),
                    None => v.delete(&k),
                }
            }
            let seen = v.get(&p)?;
            fail.map_or(Ok(MetaReply::DebugValue { value: seen }), Err)
        });
        assert_eq!(run_one(&mut view, &env, &ctx(session, seq), op), want, "{at}：回复与模型不同");
        for k in &probe_keys {
            assert_eq!(view.get(k), Ok(m.get(k)), "{at}：视图里 {k:?} 与模型不同");
        }
    }
    let bytes: usize = m.batch.iter().map(|(k, v)| k.len() + v.as_ref().map_or(0, Vec::len)).sum();
    assert_eq!(view.encoded_len(), bytes, "种子 {seed}：写集字节数 = 同 key 只算最后一次的 key + value");
    let want_ops: Vec<KvOp> = m
        .batch
        .iter()
        .map(|(k, v)| match v {
            Some(v) => KvOp::Put { key: k.clone(), value: v.clone() },
            None => KvOp::Delete { key: k.clone() },
        })
        .collect();
    let ops = view.into_ops();
    assert_eq!(ops, want_ops, "种子 {seed}：into_ops 已按 key 排序、同 key 只留最后一次");
    let tx = db.begin_write().unwrap();
    {
        let mut t = tx.open_table(KV).unwrap();
        for op in &ops {
            match op {
                KvOp::Put { key, value } => drop(t.insert(key.as_slice(), value.as_slice()).unwrap()),
                KvOp::Delete { key } => drop(t.remove(key.as_slice()).unwrap()),
            }
        }
    }
    tx.commit().unwrap();
    let rtx = db.begin_read().unwrap();
    let t = rtx.open_table(KV).unwrap();
    let got: BTreeMap<Vec<u8>, Vec<u8>> =
        t.iter().unwrap().map(|e| e.map(|(k, v)| (k.value().to_vec(), v.value().to_vec())).unwrap()).collect();
    let mut want = m.base.clone();
    for (k, v) in &m.batch {
        match v {
            Some(v) => want.insert(k.clone(), v.clone()),
            None => want.remove(k),
        };
    }
    assert_eq!(got, want, "种子 {seed}：写进 redb 之后的最终状态与模型不同");
}

#[test]
fn random_ops_match_a_btreemap_model() {
    for seed in 1..=200 {
        run_model(seed);
    }
}
