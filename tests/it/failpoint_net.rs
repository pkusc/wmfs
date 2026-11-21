//! fail point `net.drop.to.<节点>`、`net.drop.from.<节点>`、`net.delay`（design.md 3.11 的首批，M15 补上）：不用 iptables 就能模拟
//! 网络分区与慢网络。多进程的开发集群上，分区就是在两边的节点上各设 `net.drop.to.<对面的节点>`（`scripts/accept.sh chaos`）。
//!
//! fail point 是进程级的：单独一个测试程序，全部用例按顺序放在一个 `#[test]` 里（并行的用例会互相看到对方的 fail point）。

use std::sync::Arc;
use std::time::{Duration, Instant};

use wmfs_core::api::{failpoint, metrics};
use wmfs_core::base::ErrCode;
use wmfs_core::base::types::Role;
use wmfs_core::net::client::{Family, check_reply};
use wmfs_core::proto::frame::{MSG_PING, MSG_SNAPSHOT_FETCH};
use wmfs_testkit::harness::net::*;

/// fail point `name` 触发了几次。
fn hits(name: &str) -> u64 {
    metrics::counter("wmfs_failpoint_hits_total", "", &[("name", name)]).get()
}

#[test]
fn net_drop_and_delay_simulate_partitions_and_slow_links() {
    let s = tcp();
    let c = s.client(key(2), Role::Client);
    let ping = |t: Duration| c.call(&s.ep, Family::Control, frame(MSG_PING, b"x"), t).and_then(check_reply).map(|_| ());
    assert_eq!(ping(T), Ok(()), "先建好连接");

    // net.drop.to.1：发往节点 1 的请求丢在路上——调用到期以 ETIMEDOUT 结束（不是马上失败：分区时对方什么也收不到）
    failpoint::set("net.drop.to.1", "return(EIO)").unwrap();
    let t0 = Instant::now();
    assert_eq!(ping(Duration::from_millis(300)), Err(ErrCode::ETIMEDOUT));
    assert!(t0.elapsed() >= Duration::from_millis(300), "等满了这次调用的期限：{:?}", t0.elapsed());
    let before = hits("net.drop.to.1");
    assert_eq!(
        c.send_oneway(&s.ep, frame(MSG_PING, b"oneway"), T),
        Ok(()),
        "OneWay 帧（raft）也丢：照样返回 Ok，raft 会重发"
    );
    assert_eq!(hits("net.drop.to.1"), before + 1, "OneWay 帧走了丢弃的路径");
    failpoint::set("net.drop.to.2", "return(EIO)").unwrap();
    failpoint::clear("net.drop.to.1");
    assert_eq!(ping(T), Ok(()), "只丢发往节点 2 的；清掉节点 1 的之后照常（连接一直没断）");
    failpoint::clear("net.drop.to.2");

    // net.drop.from.1：请求发出去了、服务端也回了，回复丢在路上
    failpoint::set("net.drop.from.1", "return(EIO)").unwrap();
    assert_eq!(ping(Duration::from_millis(300)), Err(ErrCode::ETIMEDOUT));
    assert!(hits("net.drop.from.1") >= 1);
    failpoint::clear("net.drop.from.1");
    assert_eq!(ping(T), Ok(()));
    // 流（快照下载）不能中间缺一帧：丢掉的那一帧让这个流以 ETIMEDOUT 结束（像链路卡住），而不是少一段接着往下收
    s.rpc.add_service(MSG_SNAPSHOT_FETCH, Arc::new(Streamer));
    let srv = s.client(key(0), Role::Server);
    failpoint::set("net.drop.from.1", "times(1,return(EIO))").unwrap();
    let mut st = srv.stream(&s.ep, frame(MSG_SNAPSHOT_FETCH, b""), T).unwrap();
    assert_eq!(st.next().err(), Some(ErrCode::ETIMEDOUT), "第一帧丢了：这个流结束");
    failpoint::clear("net.drop.from.1");
    let mut st = srv.stream(&s.ep, frame(MSG_SNAPSHOT_FETCH, b""), T).unwrap();
    let bodies: Vec<Vec<u8>> = std::iter::from_fn(|| st.next().unwrap()).map(|f| f.body).collect();
    assert_eq!(bodies, vec![vec![0], vec![1], vec![2]], "清掉之后流照常");

    // 丢一部分（有损的链路）：pct(50, …) 时大约一半的调用超时
    failpoint::set("net.drop.to.1", "pct(50,return(EIO))").unwrap();
    let lost = (0..40).filter(|_| ping(Duration::from_millis(150)) == Err(ErrCode::ETIMEDOUT)).count();
    failpoint::clear("net.drop.to.1");
    assert!((5..=35).contains(&lost), "pct(50)：40 次里丢了 {lost} 次");

    // net.delay = return(200)：每一帧入队之后至少 200 ms 才写出去——请求在客户端连接的写线程上、回复在服务端连接的写线程上，各晚一次；
    // 帧与帧之间不累加（模拟延迟，不是限流）：同时发 10 个 Ping，一共也只晚 400 ms 左右，不是 10 倍
    failpoint::set("net.delay", "return(200)").unwrap();
    let t0 = Instant::now();
    assert_eq!(ping(T), Ok(()));
    let slow = t0.elapsed();
    assert!(slow >= Duration::from_millis(400), "请求与回复各晚 200 ms：{slow:?}");
    let t0 = Instant::now();
    let calls: Vec<_> = (0..10).map(|_| c.start(&s.ep, Family::Control, frame(MSG_PING, b"x"), T).unwrap()).collect();
    assert!(calls.into_iter().all(|p| p.wait().and_then(check_reply).is_ok()));
    let ten = t0.elapsed();
    failpoint::clear("net.delay");
    assert!((400..800).contains(&ten.as_millis()), "10 个 Ping 同时发：各自晚 400 ms 左右、不累加：{ten:?}");
    let t0 = Instant::now();
    assert_eq!(ping(T), Ok(()));
    assert!(t0.elapsed() < Duration::from_millis(200), "清掉之后不再变慢：{:?}", t0.elapsed());

    dropped_head_reply_retry_stays_linearizable();
}

/// M15 审查复现场景（ADR-0046）：`net.drop.from.1` 丢掉 head 回复给写者 A 的 `Written`——A 的写已在链尾提交并被读到，随后写者 B
/// 写入 `BBBBBBBB` 并返回，A 的 `DataClient` 超时后带着同一个 `write_id` 重试：命中去重表直接回原版本号，chunk 里仍是 `BBBBBBBB`。
fn dropped_head_reply_retry_stays_linearizable() {
    use wmfs_core::buf::{Lane, MemPool, PoolConfig};
    use wmfs_core::client::data_client::DataClient;
    use wmfs_core::hooks::traits::Transport;
    use wmfs_core::meta::cluster::chains::SERVING;
    use wmfs_core::net::client::RpcClient;
    use wmfs_testkit::harness::craq::{Craq, id};

    let c = Craq::start(3, &[(1, SERVING), (2, SERVING), (3, SERVING)]);
    let pool = MemPool::new(&PoolConfig::small()).unwrap();
    let tr = vec![Arc::new(plugin_mem_transport::MemTransport::new()) as Arc<dyn Transport>];
    let rpc_a = RpcClient::new_with_role(pool.clone(), tr, key(2), Role::Client, net("mem")).unwrap();
    let map = c.map();
    let client_a = DataClient::new(rpc_a, Arc::new(move || map.clone()), false)
        .with_timeouts(Duration::from_millis(300), Duration::from_secs(5));
    let cap = c.cap();

    failpoint::set("net.drop.from.1", "times(1,return(EIO))").unwrap();
    let writer_a = std::thread::spawn(move || {
        let mut b = pool.get(8, Lane::Normal, Duration::from_secs(1)).unwrap();
        b.as_mut_slice().copy_from_slice(b"AAAAAAAA");
        client_a.write(&cap, id(0), vec![(0, 8)], vec![b.freeze().whole()], false)
    });
    let t0 = Instant::now();
    while c.read(0, 8).ok().as_deref() != Some(b"AAAAAAAA".as_slice()) {
        assert!(t0.elapsed() < Duration::from_secs(5), "A 的写应先在链上提交");
        std::thread::sleep(Duration::from_millis(5));
    }
    let v_b = c.write(0, 0, b"BBBBBBBB").unwrap();
    assert_eq!(c.read(0, 8).unwrap(), b"BBBBBBBB");
    let v_a = writer_a.join().unwrap().expect("A 重试应成功");
    failpoint::clear("net.drop.from.1");
    assert!(v_a < v_b, "A 重试命中去重，拿到原来的版本号 {v_a} < {v_b}");
    assert_eq!(c.read(0, 8).unwrap(), b"BBBBBBBB", "A 的重试不能覆盖 B 之后完成的写");
}
