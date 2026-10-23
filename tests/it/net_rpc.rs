//! RPC 组件测试（传输一致性）：同一套用例跑 tcp（127.0.0.1:0）、mem 与 rdma 三种传输。
//! 覆盖 roadmap M1 的验收标准：ping 1000 次、4 MiB bulk 往返 200 次 CRC 全对（另有一次 bulk 里夹着长度为 0 的段）、
//! 错误密钥被拒、client key 申请 server 角色被拒、角色权限表，以及 RpcClient 的 start / call_cb / stream / 超时、
//! Responder 被丢弃时回 EIO、池满回 BUSY；以及 M3 的单飞建连（ADR-0031 第 11 条：16 个线程同时第一次调用只建 1 条连接）。
//! rdma 的那一份需要 RDMA 设备（本机按决定不做实机测试）：`#[ignore = "需要 RDMA 设备"]`，scripts/accept.sh M2 里显示 SKIP；
//! 有设备时 `cargo test -p wmfs-testkit --test net_rpc -- --ignored` 真跑（流不走 rdma，3.3.3，那一项只在 tcp/mem 上跑）。
//! 发出的 bulk 与 recv_into 一律从客户端自己的池里取（rdma 只注册了它，harness 的 `client_pool`）。

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::{Duration, Instant};

use wmfs_core::api::metrics;
use wmfs_core::api::types::KeyFile;
use wmfs_core::base::ErrCode;
use wmfs_core::base::types::Role;
use wmfs_core::buf::Lane;
use wmfs_core::net::client::{Family, RpcClient, check_reply};
use wmfs_core::net::server::{Responder, Service};
use wmfs_core::proto::encode_body;
use wmfs_core::proto::frame::*;
use wmfs_core::proto::handshake::PingBody;
use wmfs_core::proto::peer::NodeOp;
use wmfs_testkit::harness::net::*;

/// 每个用例在 tcp 与 mem 上各跑一遍（rdma 的一份在各自的 #[ignore] 测试里）。
fn both(name: &str) -> Vec<TestServer> {
    vec![tcp(), mem(&format!("rpc-{name}"))]
}

fn call(c: &RpcClient, s: &TestServer, f: OutFrame) -> Result<InFrame, ErrCode> {
    c.call(&s.ep, Family::Data, f, T).and_then(check_reply)
}

/// rdma 的一份：没有设备时打印 SKIP 并直接返回（所以这些测试一律 #[ignore]）。
fn on_rdma(case: impl Fn(&TestServer)) {
    if let Some(s) = rdma() {
        case(&s);
    }
}

fn ping_case(s: &TestServer) {
    let (c, p) = s.client_pool(key(1), Role::Admin);
    for i in 0..1000u32 {
        let r = call(&c, s, frame(MSG_PING, &i.to_le_bytes())).unwrap();
        assert_eq!(r.body, encode_body(&PingBody { payload: i.to_le_bytes().to_vec() }), "{}", s.scheme);
    }
    let data = pattern(&p, 4 << 20);
    let want = crc32c::crc32c(data.as_slice());
    for _ in 0..200 {
        let r = call(&c, s, bulk_ping(&data, p.get(4 << 20, Lane::Normal, T).unwrap())).unwrap();
        let b = r.bulk.unwrap();
        assert_eq!((b.len(), crc32c::crc32c(b.as_slice()), r.hdr.bulk_crc32c), (4 << 20, want, want), "{}", s.scheme);
    }
    // bulk 里夹着长度为 0 的段（core 里合法：tcp 什么也不写，rdma 不为它生成描述符）：照样回显、CRC 对，连接不断
    let segs = vec![data.slice(0, 0), data.whole(), data.slice(4 << 20, 0)];
    let f = OutFrame { bulk: segs, ..bulk_ping(&data, p.get(4 << 20, Lane::Normal, T).unwrap()) };
    let b = call(&c, s, f).unwrap().bulk.unwrap();
    assert_eq!((b.len(), crc32c::crc32c(b.as_slice())), (4 << 20, want), "{}：bulk 带空段", s.scheme);
}

#[test]
fn ping_1000_and_4m_bulk_200_with_crc() {
    for s in both("ping") {
        ping_case(&s);
    }
}

#[test]
#[ignore = "需要 RDMA 设备"]
fn rdma_ping_1000_and_4m_bulk_200_with_crc() {
    on_rdma(ping_case);
}

#[test]
fn wrong_keys_and_role_claims_are_rejected() {
    for s in both("auth") {
        auth_case(&s);
    }
}

#[test]
#[ignore = "需要 RDMA 设备"]
fn rdma_wrong_keys_and_role_claims_are_rejected() {
    on_rdma(auth_case);
}

fn auth_case(s: &TestServer) {
    let ping = |k: KeyFile, r: Role| call(&s.client(k, r), s, frame(MSG_PING, b"x")).map(|_| ());
    assert_eq!(ping(key(2), Role::Client), Ok(()));
    assert_eq!(ping(key(0), Role::Server), Ok(()));
    assert_eq!(ping(KeyFile { key_id: 2, secret: [0x77; 32] }, Role::Client), Err(ErrCode::AUTH_FAILED), "错误密钥");
    assert_eq!(ping(key(2), Role::Server), Err(ErrCode::AUTH_FAILED), "client key 申请 server 角色被拒");
    assert_eq!(ping(key(1), Role::Client), Err(ErrCode::AUTH_FAILED), "Hello.role 必须等于 KEYMETA.role");
    assert_eq!(ping(key(0), Role::Admin), Err(ErrCode::AUTH_FAILED), "key 0 只能是 Server");
    assert_eq!(ping(key(3), Role::Client), Err(ErrCode::AUTH_FAILED), "集群不认识的 key");
}

#[test]
fn role_table_and_unknown_messages_do_not_disconnect() {
    for s in both("roles") {
        roles_case(&s);
    }
}

#[test]
#[ignore = "需要 RDMA 设备"]
fn rdma_role_table_and_unknown_messages_do_not_disconnect() {
    on_rdma(roles_case);
}

fn roles_case(s: &TestServer) {
    let denied = || metrics::counter("wmfs_rpc_denied_total", "", &[("role", "client"), ("family", "PeerCall")]).get();
    let c = s.client(key(2), Role::Client);
    let before = denied();
    for msg in [MSG_PEER_CALL, MSG_RAFT, MSG_SNAPSHOT_FETCH] {
        assert_eq!(call(&c, s, frame(msg, b"")).err(), Some(ErrCode::EACCES), "client 调 {msg:#06x} 应被拒");
    }
    assert!(denied() > before, "拒绝要计数");
    let mut join = frame(MSG_NODE_CALL, b"");
    join.body = encode_body(&NodeOp::SyncDone { target: 1, chain: 1, chain_ver: 1 });
    assert_eq!(call(&c, s, join).err(), Some(ErrCode::EACCES), "client 的 NodeCall 只能 GetClusterMap");
    let mut map = frame(MSG_NODE_CALL, b"");
    map.body = encode_body(&NodeOp::GetClusterMap { since_epoch: 0 });
    assert_eq!(call(&c, s, map).err(), Some(ErrCode::FEATURE_UNSUPPORTED), "允许，但 M1 还没有这个服务");
    assert_eq!(call(&c, s, frame(0x0777, b"")).err(), Some(ErrCode::FEATURE_UNSUPPORTED), "不认识的 msg_type");
    assert!(call(&c, s, frame(MSG_PING, b"still-connected")).is_ok(), "以上都不断开连接");
    let srv = s.client(key(0), Role::Server);
    assert_eq!(
        call(&srv, s, frame(MSG_PEER_CALL, b"")).err(),
        Some(ErrCode::FEATURE_UNSUPPORTED),
        "Server 可以调 PeerCall"
    );
}

/// 收下请求但一直不回（测超时）。
struct Hold(Mutex<Vec<Responder>>);
impl Service for Hold {
    fn call(&self, _f: InFrame, r: Responder) {
        self.0.lock().unwrap().push(r);
    }
}
/// 直接丢掉 Responder（测"没回复就被丢弃 → EIO"）。
struct DropIt;
impl Service for DropIt {
    fn call(&self, _f: InFrame, r: Responder) {
        drop(r);
    }
}
/// 回三个 StreamChunk，最后一个带 LAST_CHUNK。
struct Streamer;
impl Service for Streamer {
    fn call(&self, _f: InFrame, r: Responder) {
        let mut tx = r.into_stream();
        for i in 0..3u8 {
            tx.send(vec![i], vec![], i == 2).unwrap();
        }
    }
}

#[test]
fn start_call_cb_stream_timeout_and_dropped_responder() {
    for s in both("modes") {
        modes_case(&s, true);
    }
}

#[test]
#[ignore = "需要 RDMA 设备"]
fn rdma_start_call_cb_timeout_and_dropped_responder() {
    on_rdma(|s| modes_case(s, false)); // 流（SnapshotFetch）永远走 control_scheme，不走 rdma（3.3.3）
}

fn modes_case(s: &TestServer, stream: bool) {
    let c = s.client(key(1), Role::Admin);
    // start：64 路并发
    let pending: Vec<_> =
        (0..64u32).map(|i| c.start(&s.ep, Family::Data, frame(MSG_PING, &i.to_le_bytes()), T).unwrap()).collect();
    for (i, p) in pending.into_iter().enumerate() {
        let want = encode_body(&PingBody { payload: (i as u32).to_le_bytes().to_vec() });
        assert_eq!(check_reply(p.wait().unwrap()).unwrap().body, want);
    }
    // call_cb：回调恰好一次
    let hits = Arc::new(AtomicU32::new(0));
    let (tx, rx) = std::sync::mpsc::channel();
    let h = hits.clone();
    c.call_cb(
        &s.ep,
        Family::Data,
        frame(MSG_PING, b"cb"),
        T,
        Box::new(move |r| {
            h.fetch_add(1, Ordering::SeqCst);
            tx.send(r.is_ok()).unwrap();
        }),
    );
    assert!(rx.recv_timeout(T).unwrap());
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    // 超时：服务端一直不回 → rpc-timer 以 ETIMEDOUT 结束这个调用（约 100 ms 精度）
    let hold = Arc::new(Hold(Mutex::new(Vec::new())));
    s.rpc.add_service(MSG_META_CALL, hold.clone());
    let t0 = Instant::now();
    let r = c.call(&s.ep, Family::Control, frame(MSG_META_CALL, b""), Duration::from_millis(300));
    assert_eq!(r.err(), Some(ErrCode::ETIMEDOUT));
    assert!(t0.elapsed() >= Duration::from_millis(300) && t0.elapsed() < Duration::from_secs(3));
    hold.0.lock().unwrap().clear(); // 晚到的 EIO 回复被客户端忽略
    // Responder 没回复就被丢弃 → 客户端收到 EIO，服务端计数
    let dropped = || metrics::counter("wmfs_req_dropped_total", "", &[]).get();
    let before = dropped();
    s.rpc.add_service(MSG_META_CALL, Arc::new(DropIt));
    assert_eq!(call(&c, s, frame(MSG_META_CALL, b"")).err(), Some(ErrCode::EIO));
    assert!(dropped() > before);
    if !stream {
        return;
    }
    // stream：三帧，然后 None
    s.rpc.add_service(MSG_SNAPSHOT_FETCH, Arc::new(Streamer));
    let srv = s.client(key(0), Role::Server);
    let mut st = srv.stream(&s.ep, frame(MSG_SNAPSHOT_FETCH, b""), T).unwrap();
    let bodies: Vec<Vec<u8>> = std::iter::from_fn(|| st.next().unwrap()).map(|f| f.body).collect();
    assert_eq!(bodies, vec![vec![0], vec![1], vec![2]]);
    assert!(st.next().unwrap().is_none());
}

#[test]
fn full_pool_answers_busy_without_disconnecting() {
    for s in both("busy") {
        busy_case(&s);
    }
}

#[test]
#[ignore = "需要 RDMA 设备"]
fn rdma_full_pool_answers_busy_without_disconnecting() {
    on_rdma(busy_case);
}

fn busy_case(s: &TestServer) {
    let (c, p) = s.client_pool(key(1), Role::Admin);
    // 测试池的 M 档 4 槽里 1 个是复制专用：占住 3 个普通槽，服务端就收不下 1 MiB 的普通 bulk
    let hog: Vec<_> = (0..3).map(|_| s.pool.get(1 << 20, Lane::Normal, T).unwrap()).collect();
    let data = pattern(&p, 1 << 20);
    let r = call(&c, s, bulk_ping(&data, p.get(1 << 20, Lane::Normal, T).unwrap()));
    assert_eq!(r.err(), Some(ErrCode::BUSY), "{}：取不到缓冲 → bulk 读出丢弃、回 BUSY", s.scheme);
    drop(hog);
    assert!(call(&c, s, bulk_ping(&data, p.get(1 << 20, Lane::Normal, T).unwrap())).is_ok(), "连接还在，重试成功");
}

#[test]
fn data_family_never_silently_downgrades_for_clients() {
    let s = tcp();
    let mut net = net("tcp");
    net.data_scheme = "rdma".into();
    let t = || vec![transport("tcp")];
    let c = RpcClient::new_with_role(pool(), t(), key(2), Role::Client, net.clone()).unwrap();
    assert_eq!(call(&c, &s, frame(MSG_PING, b"")).err(), Some(ErrCode::FEATURE_UNSUPPORTED), "对端没公布 rdma：报错");
    assert!(c.call(&s.ep, Family::Control, frame(MSG_PING, b""), T).is_ok(), "控制族照常走 control_scheme");
    let fallback = || metrics::counter("wmfs_transport_fallback_total", "", &[("from", "rdma"), ("to", "tcp")]).get();
    let before = fallback();
    let srv = RpcClient::new_with_role(pool(), t(), key(0), Role::Server, net).unwrap();
    assert!(call(&srv, &s, frame(MSG_PING, b"")).is_ok(), "服务端之间退回 control_scheme");
    assert!(fallback() > before, "并计数");
}

#[test]
fn version_feature_role_and_cluster_negotiation() {
    use std::net::TcpStream;
    use wmfs_core::proto::features::FEAT_STRONG;
    let s = tcp();
    let try_hello = |edit: &dyn Fn(&mut wmfs_core::proto::handshake::Hello)| {
        let mut h = hello(&key(2), Role::Client);
        edit(&mut h);
        raw_hello(&mut TcpStream::connect(&s.addr).unwrap(), &h).map(|ch| ch.features)
    };
    assert_eq!(try_hello(&|_| {}), Ok(0), "合法的 Hello 得到 Challenge（features 取交集）");
    assert_eq!(try_hello(&|h| h.proto_min = 2), Err(ErrCode::PROTO_UNSUPPORTED), "版本区间里没有 1");
    assert_eq!(try_hello(&|h| h.required = FEAT_STRONG), Err(ErrCode::PROTO_UNSUPPORTED), "要求对方没有的特性");
    assert_eq!(try_hello(&|h| h.features = FEAT_STRONG), Ok(0), "本端多报的特性只是不在交集里");
    assert_eq!(try_hello(&|h| h.role = 9), Err(ErrCode::AUTH_FAILED), "不认识的角色值");
    assert_eq!(try_hello(&|h| h.cluster_id = [9; 16]), Err(ErrCode::AUTH_FAILED), "连错了集群");
    // 未完成认证的连接只允许握手帧：第一帧是 Ping → 直接断开（不回 ERROR）
    let mut raw = TcpStream::connect(&s.addr).unwrap();
    raw_send(&mut raw, FrameKind::Request, MSG_PING, 1, &encode_body(&PingBody { payload: vec![] })).unwrap();
    assert_eq!(raw_recv(&mut raw).err(), Some(ErrCode::ECONNRESET));
}

/// ADR-0031 第 11 条（M3 的 core 修复）：16 个线程同时对同一对端做第一次 call，只有一个线程建连、其余等它——
/// 客户端只建 1 次连接（tcp 与 mem 各一次），16 个调用全部成功；建连失败时（握手被拒）16 个调用拿到同一个错误，
/// 也只建了 1 次。客户端的每次建连先等 250 ms：16 个线程一定都在建连进行中到达（否则调度晚到的线程
/// 在失败的建连结束之后会合法地再建一次，结果取决于时机）。
/// 实现注记（M6）：判据从"服务端 accept 了几条连接"改为"这个客户端调了几次传输插件的 connect"——tcp 的计数服务端监听在
/// 127.0.0.1 的随机端口上，同一台机器上别的进程（并行跑的另一组测试、端口扫描）连进来也会被数到（M5 验证时偶发过一次 2 ≠ 1，
/// 只在 tcp 上；mem 是进程内的名字，别人连不进来）。服务端的计数仍然打印出来，只要求至少 1。
#[test]
fn first_calls_from_16_threads_share_one_connection() {
    for scheme in ["tcp", "mem"] {
        for (k, want) in [(key(2), Ok(())), (KeyFile { key_id: 2, secret: [0; 32] }, Err(ErrCode::AUTH_FAILED))] {
            let (s, accepted) = counting(scheme, &format!("rpc-single-flight-{}", want.is_ok()));
            let (c, dials) = slow_client(scheme, k, Role::Client, Duration::from_millis(250));
            let go = Arc::new(Barrier::new(16));
            let threads: Vec<_> = (0..16u8)
                .map(|i| {
                    let (c, ep, go) = (c.clone(), s.ep.clone(), go.clone());
                    std::thread::spawn(move || {
                        go.wait();
                        c.call(&ep, Family::Control, frame(MSG_PING, &[i]), T).and_then(check_reply).map(|_| ())
                    })
                })
                .collect();
            for t in threads {
                assert_eq!(t.join().unwrap(), want, "{scheme}：16 个调用的结果都一样");
            }
            let (n, seen) = (dials.load(Ordering::SeqCst), accepted.load(Ordering::SeqCst));
            println!("{scheme}（{want:?}）：客户端建连 {n} 次，服务端 accept {seen} 条");
            assert_eq!(n, 1, "{scheme}（{want:?}）：16 个线程同时第一次调用，客户端只建 1 次连接");
            assert!(seen >= 1, "{scheme}：服务端至少看到这一条");
        }
    }
}
