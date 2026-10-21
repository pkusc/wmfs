//! 内存池的单元测试：选档、独占、Repl lane、等待与超时、freeze/slice（从 pool.rs 拆出，免得那个文件超过 400 行）。

use std::time::{Duration, Instant};

use super::*;

/// 测试用的小池：S 1 MiB（8 槽）、M 2 槽、L 1 槽（≈ 25 MiB）。
fn tiny(repl: bool) -> MemPool {
    MemPool::new(&PoolConfig { s_mb: 1, m_slots: 2, l_slots: 1, repl_lane: repl }).unwrap()
}

const NO_WAIT: Duration = Duration::ZERO;

#[test]
fn preset_sizes_match_design() {
    // design.md 3.6.1：服务端 ≈ 1 GiB、客户端 ≈ 768 MiB、dev ≈ 384 MiB、CLI ≈ 70 MiB
    let mib = |c: PoolConfig| c.total_bytes() >> 20;
    assert!((1000..=1030).contains(&mib(PoolConfig::server())), "{}", mib(PoolConfig::server()));
    assert!((760..=775).contains(&mib(PoolConfig::client())));
    assert!((380..=390).contains(&mib(PoolConfig::dev_server())));
    assert!((65..=72).contains(&mib(PoolConfig::small())));
    assert!(PoolConfig::server().repl_lane && !PoolConfig::client().repl_lane);
}

#[test]
fn smallest_fitting_class_and_bounds() {
    let p = tiny(false);
    let r = p.region();
    let a = p.get(100, Lane::Normal, NO_WAIT).unwrap();
    assert_eq!((a.len(), a.capacity()), (100, S_SLOT));
    let b = p.get(S_SLOT + 1, Lane::Normal, NO_WAIT).unwrap();
    assert_eq!(b.capacity(), M_SLOT);
    let c = p.get(L_SLOT, Lane::Normal, NO_WAIT).unwrap();
    assert_eq!(c.capacity(), L_SLOT);
    assert_eq!(p.get(L_SLOT + 1, Lane::Normal, NO_WAIT).err(), Some(ErrCode::EINVAL), "比最大的槽位还大");
    for x in [&a, &b, &c] {
        assert!(x.addr() >= r.base && x.addr() + x.capacity() as u64 <= r.base + r.len, "缓冲必须在注册区域内");
    }
    assert!(MemPool::new(&PoolConfig { s_mb: 0, m_slots: 1, l_slots: 1, repl_lane: false }).is_err());
}

#[test]
fn slots_are_exclusive_and_returned_on_drop() {
    let p = tiny(false);
    let mut bufs: Vec<Buf> = (0..8).map(|_| p.get(S_SLOT, Lane::Normal, NO_WAIT).unwrap()).collect();
    for (i, b) in bufs.iter_mut().enumerate() {
        b.as_mut_slice().fill(i as u8);
    }
    for (i, b) in bufs.iter().enumerate() {
        assert!(b.as_slice().iter().all(|x| *x == i as u8), "两个 Buf 不能共用同一段内存");
    }
    assert_eq!(p.get(1, Lane::Normal, NO_WAIT).err(), Some(ErrCode::BUSY), "S 档用完了");
    bufs.pop();
    assert!(p.get(1, Lane::Normal, NO_WAIT).is_ok(), "Drop 之后槽位回到池里");
}

#[test]
fn repl_lane_is_reserved_for_replication() {
    let p = tiny(true);
    // S 8 槽 → Repl 2 槽（25%）；M 2 槽 → Repl 1 槽（向上取整）；L 1 槽 → Repl 1 槽（至少 1 个）
    let free = |i: usize| (p.stats()[i].free_normal, p.stats()[i].free_repl);
    assert_eq!(free(0), (6, 2));
    assert_eq!(free(1), (1, 1));
    assert_eq!(free(2), (0, 1));
    assert_eq!((p.stats()[1].name, p.stats()[1].slot_size, p.stats()[1].slots), ("M", M_SLOT, 2));
    let normal: Vec<Buf> = (0..6).map(|_| p.get(1, Lane::Normal, NO_WAIT).unwrap()).collect();
    assert_eq!(p.get(1, Lane::Normal, NO_WAIT).err(), Some(ErrCode::BUSY), "普通流量不能借用复制槽位");
    let r1 = p.get(1, Lane::Repl, NO_WAIT).unwrap();
    let r2 = p.get(1, Lane::Repl, NO_WAIT).unwrap();
    assert_eq!(p.get(1, Lane::Repl, NO_WAIT).err(), Some(ErrCode::BUSY));
    drop(normal);
    let r3 = p.get(1, Lane::Repl, NO_WAIT).unwrap();
    assert_eq!(free(0).0, 5, "复制流量可以借用普通槽位");
    drop((r1, r2, r3));
    assert_eq!(free(0), (6, 2), "借来的槽位归还到原来的表");
}

#[test]
fn waiter_is_woken_by_drop_and_times_out_with_busy() {
    let p = tiny(false);
    let only = p.get(L_SLOT, Lane::Normal, NO_WAIT).unwrap();
    let t = Instant::now();
    assert_eq!(p.get(L_SLOT, Lane::Normal, Duration::from_millis(100)).err(), Some(ErrCode::BUSY));
    assert!(t.elapsed() >= Duration::from_millis(90), "应等满 wait 才回 BUSY");
    let p2 = p.clone();
    let h = std::thread::spawn(move || p2.get(L_SLOT, Lane::Normal, Duration::from_secs(10)).map(|b| b.capacity()));
    std::thread::sleep(Duration::from_millis(50));
    drop(only);
    assert_eq!(h.join().unwrap(), Ok(L_SLOT), "归还槽位会叫醒等待者");
}

#[test]
fn freeze_slice_and_unfreeze() {
    let p = tiny(false);
    let mut b = p.get(10, Lane::Normal, NO_WAIT).unwrap();
    b.as_mut_slice().copy_from_slice(b"0123456789");
    let f = b.freeze();
    let s = f.slice(2, 3);
    assert_eq!(s.as_slice(), b"234");
    assert_eq!(s.addr(), f.addr() + 2);
    assert_eq!(f.whole().as_slice(), b"0123456789");
    let f = f.try_unfreeze().err().expect("还有一个 BufSlice 持有引用，不能取回可写所有权");
    drop(s);
    let mut b = f.try_unfreeze().ok().expect("引用计数为 1 时可以取回");
    b.set_len(4);
    assert_eq!(b.as_slice(), b"0123");
}

#[test]
#[should_panic(expected = "越界")]
fn slice_out_of_bounds_panics() {
    let p = tiny(false);
    let f = p.get(10, Lane::Normal, NO_WAIT).unwrap().freeze();
    let _ = f.slice(8, 3);
}
