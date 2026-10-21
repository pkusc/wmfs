//! 拆除顺序（design.md 3.6.4 实现注记（M2）⑨；Rust 的所有权管不到网卡 DMA）：交给网卡的缓冲只在它的工作请求完成之后、
//! 或 QP 销毁之后才回池。假网卡在 QP 销毁的那一刻调用钩子，记下那时池里的空闲槽数：那时缓冲必须一个都还没回池。
//! `hold_one_sided` 让 READ/WRITE 的完成事件迟迟不报（数据照样拷），这时连接被关掉，等完成事件的缓冲要进"墓地"。

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use wmfs_core::buf::{Lane, MemPool};
use wmfs_core::proto::frame::FrameKind;

use super::conn::{Pair, Z, authed, filled, out, pair};

/// (S 档, M 档) 的 Normal 空闲槽数。
fn free(p: &MemPool) -> (usize, usize) {
    (p.stats()[0].free_normal, p.stats()[1].free_normal)
}

/// 记下第一个被销毁的 QP 销毁的那一刻 `p` 的空闲槽数。
fn watch_destroy(p: &Pair, pool: &MemPool) -> Arc<Mutex<Option<(usize, usize)>>> {
    let seen = Arc::new(Mutex::new(None));
    let (seen2, pool2) = (seen.clone(), pool.clone());
    *p.nic.0.on_destroy.lock().unwrap() = Some(Box::new(move || {
        seen2.lock().unwrap().get_or_insert(free(&pool2));
    }));
    seen
}

/// 在途的 BufSlice、recv_into、接收槽在 close() 之后、QP 销毁之前一个都不回池。
#[test]
fn rdma_conn_buffers_return_only_after_the_qp_is_destroyed() {
    let p = authed(pair());
    let data = filled(&p.cp, 4096, 1).whole(); // 只有这个请求持有它
    let into = p.cp.get(1 << 20, Lane::Normal, Z).unwrap();
    p.c.send(out(FrameKind::Request, 1, b"", vec![data], Some(into))).unwrap(); // 服务端永不回复
    let held = free(&p.cp);
    let seen = watch_destroy(&p, &p.cp);
    p.c.close();
    assert_eq!(free(&p.cp), held, "close() 只让 QP 出错、不放任何缓冲");
    let Pair { c, s, cp, .. } = p;
    drop(c);
    assert_eq!(*seen.lock().unwrap(), Some(held), "QP 销毁的那一刻缓冲都还没回池");
    assert_eq!(
        free(&cp),
        (held.0 + 16 + 1 + 1, held.1 + 1),
        "QP 销毁之后：接收槽、发送暂存、在途的 bulk 与 recv_into 回池"
    );
    drop(s);
}

/// 回复的 WRITE 迟迟不完成时连接被关掉：WRITE 的来源进墓地，QP 销毁之后才回池（不能当场放掉——网卡可能还在读它）。
#[test]
fn rdma_conn_unfinished_write_sources_wait_for_the_qp() {
    let p = Arc::new(authed(pair()));
    let into = p.cp.get(1 << 20, Lane::Normal, Z).unwrap();
    p.c.send(out(FrameKind::Request, 1, b"", vec![], Some(into))).unwrap();
    p.s.recv().unwrap();
    p.nic.0.hold_one_sided.store(true, Ordering::SeqCst);
    let before = free(&p.sp);
    let bulk = filled(&p.sp, 1 << 20, 7).whole(); // 只有这个回复持有它（M 档）
    let p2 = p.clone();
    let closer = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(200));
        p2.s.close();
    });
    let r = p.s.send(out(FrameKind::Response, 1, b"", vec![bulk], None));
    closer.join().unwrap();
    assert!(r.is_err(), "WRITE 没有完成：发送失败、连接作废");
    assert_eq!(free(&p.sp), (before.0, before.1 - 1), "WRITE 的来源进了墓地，没有回池");
    let p = Arc::into_inner(p).unwrap();
    let seen = watch_destroy(&p, &p.sp);
    let Pair { c, s, sp, .. } = p;
    drop(s);
    assert_eq!(seen.lock().unwrap().map(|f| f.1), Some(before.1 - 1), "QP 销毁的那一刻还没回池");
    assert_eq!(free(&sp).1, before.1, "QP 销毁之后回池");
    drop(c);
}

/// 请求的 READ 迟迟不完成时连接被关掉：READ 的目标缓冲进墓地，QP 销毁之后才回池（网卡可能还在往里写）。
#[test]
fn rdma_conn_unfinished_read_target_waits_for_the_qp() {
    let p = Arc::new(authed(pair()));
    p.nic.0.hold_one_sided.store(true, Ordering::SeqCst);
    let before = free(&p.sp);
    let data = filled(&p.cp, 1 << 20, 8);
    p.c.send(out(FrameKind::Request, 1, b"", vec![data.whole()], None)).unwrap();
    let p2 = p.clone();
    let closer = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(200));
        p2.s.close();
    });
    let r = p.s.recv();
    closer.join().unwrap();
    assert!(r.is_err(), "READ 没有完成：收帧失败、连接作废");
    assert_eq!(free(&p.sp).1, before.1 - 1, "READ 的目标（M 档）进了墓地，没有回池");
    let p = Arc::into_inner(p).unwrap();
    let seen = watch_destroy(&p, &p.sp);
    let Pair { c, s, sp, .. } = p;
    drop(s);
    assert_eq!(seen.lock().unwrap().map(|f| f.1), Some(before.1 - 1), "QP 销毁的那一刻还没回池");
    assert_eq!(free(&sp).1, before.1, "QP 销毁之后回池");
    drop(c);
}
