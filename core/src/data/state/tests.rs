//! chunk 状态表的单元测试：同一 chunk 的写排队与上限、pending 占着 chunk、claim / take_held / put_held、脏（没在本地提交）与
//! Corrupt 标记。

use super::*;
use crate::data::write::JobKind;
use crate::proto::cap::CapV1;

fn chunk(idx: u32) -> ChunkId {
    ChunkId { chain: 1, ino: 18, epoch: 1, idx }
}

fn job(idx: u32) -> Box<WriteJob> {
    let cap = CapV1 {
        ino: 18,
        epoch: 1,
        rights: 0,
        session: 0,
        tenant: 0,
        job_id: 0,
        chains: vec![1],
        class_id: 0,
        qos_id: 0,
        chunk_size: 0,
        block_size: 0,
        checksum: 0,
        compress: 0,
        compress_level: 0,
        encrypt: 0,
        encrypt_key: 0,
        expires_ms: 0,
    };
    let kind = JobKind::Truncate { new_len: idx };
    Box::new(WriteJob { chunk: chunk(idx % 2), target: 1, cap, kind, bulk: None, reply: None })
}

fn pending(version: u64) -> PendingWrite {
    let (successor, chain_ver, last_sent) = (Some(2), 1, 0);
    let (sync, clean, committed) = (false, true, false);
    PendingWrite { version, target: 1, upstream: None, successor, chain_ver, last_sent, sync, clean, committed }
}

#[test]
fn writes_to_one_chunk_run_one_at_a_time_and_the_queue_is_bounded() {
    let t = ChunkTable::new();
    assert!(matches!(t.begin(job(0)), Ok(Some(_))), "没有别的写在做：现在就做");
    for i in 1..=MAX_WAITERS as u32 {
        assert!(matches!(t.begin(job(2 * i)), Ok(None)), "第 {i} 个：进等待队列");
    }
    assert!(t.begin(job(100)).is_err(), "等待队列满了：调用者回 BUSY");
    assert!(matches!(t.begin(job(1)), Ok(Some(_))), "别的 chunk 不受影响");
    for i in 1..=MAX_WAITERS as u32 {
        let next = t.finish(&chunk(0)).expect("做完一个，接着做队首");
        assert!(matches!(next.kind, JobKind::Truncate { new_len } if new_len == 2 * i), "按到达的顺序");
    }
    assert!(t.finish(&chunk(0)).is_none(), "队列空了");
    assert!(matches!(t.begin(job(0)), Ok(Some(_))), "之后又可以直接做");
}

/// head 上一个版本转发出去之后，pending 占着这个 chunk（I1：最多一个）：后到的写排队；确认（take_pending + finish）之后才轮到队首。
#[test]
fn a_pending_version_holds_the_chunk_until_it_is_resolved() {
    let t = ChunkTable::new();
    assert!(matches!(t.begin(job(0)), Ok(Some(_))));
    t.set_pending(&chunk(0), pending(7)); // 写好 pending、转发出去：放下 busy，pending 占着
    assert!(t.has_dirty(&chunk(0)) && !t.has_dirty(&chunk(1)));
    assert!(matches!(t.begin(job(2)), Ok(None)), "有 pending：后到的写排队");
    assert!(!t.claim(&chunk(1)) || t.finish(&chunk(1)).is_none(), "别的 chunk 照常");
    assert!(t.take_pending(&chunk(0), Some(6)).is_none(), "不是这个版本的确认：不动");
    assert_eq!(t.with_pending(&chunk(0), None, |p| p.version), Some(7));
    let p = t.take_pending(&chunk(0), Some(7)).expect("确认到了");
    assert!(t.take_pending(&chunk(0), None).is_none(), "已经取走了：重复的确认什么也不做");
    assert_eq!(p.version, 7);
    let next = t.finish(&chunk(0)).expect("pending 解决了：接着做排队的写");
    assert!(matches!(next.kind, JobKind::Truncate { new_len: 2 }));
    assert!(t.finish(&chunk(0)).is_none());
}

/// 正在写盘（busy）的 chunk 上，确认取不走 pending（交给 chain-timer 重新驱动）；claim 失败。
#[test]
fn busy_chunks_refuse_claims_and_takes() {
    let t = ChunkTable::new();
    assert!(t.claim(&chunk(0)), "不忙：占住");
    assert!(!t.claim(&chunk(0)), "正忙：占不住");
    t.set_pending(&chunk(0), pending(3));
    assert!(t.claim(&chunk(0)), "set_pending 放下了 busy");
    assert!(t.take_pending(&chunk(0), None).is_none(), "正忙：确认取不走");
    assert!(t.take_held(&chunk(0)).is_some(), "占着 busy 的线程自己可以取");
    assert!(t.finish(&chunk(0)).is_none());
    assert!(t.pendings(|_, _| true).is_empty());
}

#[test]
fn committed_pendings_are_not_dirty_and_pendings_can_be_listed() {
    let t = ChunkTable::new();
    t.set_pending(&chunk(0), PendingWrite { committed: true, ..pending(5) });
    t.set_pending(&chunk(1), pending(9));
    assert!(!t.has_dirty(&chunk(0)), "已在本地提交（3.4.2.6 规则 2）：读不重定向");
    assert!(t.has_dirty(&chunk(1)));
    let mut ids = t.pendings(|_, p| p.version > 4);
    ids.sort();
    assert_eq!(ids, vec![chunk(0), chunk(1)]);
    assert_eq!(t.pendings(|_, p| p.version > 6), vec![chunk(1)]);
}

#[test]
fn corrupt_marks_stay_until_cleared() {
    let t = ChunkTable::new();
    t.mark_corrupt(&chunk(0));
    assert!(t.is_corrupt(&chunk(0)) && !t.is_corrupt(&chunk(1)));
    assert!(matches!(t.begin(job(0)), Ok(Some(_))));
    assert!(t.finish(&chunk(0)).is_none());
    assert!(t.is_corrupt(&chunk(0)), "一次写做完不会自动清除标记");
    t.clear_corrupt(&chunk(0));
    assert!(!t.is_corrupt(&chunk(0)));
}
