//! 连接故障的组件测试：发送队列满就断开；body / bulk 的 CRC 不对就断开；冒充的服务端（server_proof 不对）被客户端拒绝；
//! 对端在握手里没声明 feature 位时，要它的请求不发、回 `EOPNOTSUPP`（ADR-0043 第 3 条）；
//! OneWay 帧也过角色权限表；回复的 bulk 被丢弃时这次调用按 BUSY 结束；回复超过上限时只让这一个请求失败；
//! 流式回复中途被丢弃或显式报错时，客户端的流立即结束（design.md 3.2.11、3.3.1、3.3.2、3.3.3、3.3.8、3.6.3、3.6.4）。

use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::mpsc::{Sender, channel};
use std::time::{Duration, Instant};

use wmfs_core::api::metrics;
use wmfs_core::base::ErrCode;
use wmfs_core::base::config::NetConfig;
use wmfs_core::base::types::Role;
use wmfs_core::buf::Lane;
use wmfs_core::client::meta_client::{Caller, MetaClient, SessionInfo};
use wmfs_core::net::client::{Endpoint, Family, RpcClient, check_reply};
use wmfs_core::net::server::{Responder, Service};
use wmfs_core::proto::frame::{
    FLAG_HAS_BULK, FrameHeader, FrameKind, InFrame, MAX_BODY, MSG_META_CALL, MSG_NODE_CALL, MSG_PING, MSG_RAFT,
    MSG_SNAPSHOT_FETCH, OutFrame,
};
use wmfs_core::proto::handshake::{Hello, PingBody};
use wmfs_core::proto::meta::{MetaCall, MetaOp, MetaReply};
use wmfs_core::proto::peer::NodeReply;
use wmfs_core::proto::{decode_body, encode_body};
use wmfs_testkit::harness::net::*;

fn counter(name: &'static str, labels: &[(&'static str, &str)]) -> u64 {
    metrics::counter(name, "", labels).get()
}

#[test]
fn a_full_send_queue_drops_the_connection() {
    let before = counter("wmfs_conn_send_queue_full_total", &[]);
    let s = tcp_with(NetConfig { send_queue: 4, ..NetConfig::default() });
    let mut bad = TcpStream::connect(&s.addr).unwrap();
    raw_handshake(&mut bad, &key(2), Role::Client).unwrap();
    bad.set_write_timeout(Some(Duration::from_secs(10))).unwrap();
    // 一直发、从不读：服务端的写线程卡在 socket 上，回复在 4 帧的发送队列里排满 → 断开（远早于 5 s 的写超时）
    let big = encode_body(&PingBody { payload: vec![0xEE; 256 << 10] });
    for i in 0..400 {
        if raw_send(&mut bad, FrameKind::Request, MSG_PING, 10 + i, &big).is_err() {
            break; // 服务端已经断开
        }
    }
    let t0 = Instant::now();
    while counter("wmfs_conn_send_queue_full_total", &[]) == before {
        assert!(t0.elapsed() < Duration::from_secs(10), "发送队列满应该断开这条连接并计数");
        std::thread::sleep(Duration::from_millis(20));
    }
    let good = s.client(key(1), Role::Admin);
    assert!(good.call(&s.ep, Family::Data, frame(MSG_PING, b"ok"), T).is_ok(), "别的连接不受影响");
}

#[test]
fn bad_body_or_bulk_crc_drops_the_connection() {
    let s = tcp();
    for bulk in [false, true] {
        let before = counter("wmfs_conn_proto_errors_total", &[]);
        let mut raw = TcpStream::connect(&s.addr).unwrap();
        raw_handshake(&mut raw, &key(2), Role::Client).unwrap();
        let body = encode_body(&PingBody { payload: vec![1, 2, 3] });
        let data = vec![9u8; 1000];
        let mut h = FrameHeader::new(FrameKind::Request, MSG_PING, 5, 0);
        (h.body_len, h.body_crc32c) = (body.len() as u32, crc32c::crc32c(&body));
        if bulk {
            (h.flags, h.bulk_len, h.bulk_crc32c) = (FLAG_HAS_BULK, 1000, crc32c::crc32c(&data) ^ 1); // bulk 的 CRC 错
        } else {
            h.body_crc32c ^= 1; // body 的 CRC 错（帧头自己的 CRC 是对的）
        }
        raw_write(&mut raw, &h, &body, if bulk { &data } else { &[] }).unwrap();
        assert_eq!(raw_recv(&mut raw).err(), Some(ErrCode::ECONNRESET), "CRC 不对 → 断开，不回复（bulk = {bulk}）");
        assert!(counter("wmfs_conn_proto_errors_total", &[]) > before, "计数 wmfs_conn_proto_errors_total");
    }
}

#[test]
fn client_rejects_a_server_with_a_wrong_server_proof() {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap().to_string();
    let (k, fake_key) = (key(2), key(2));
    let fake = std::thread::spawn(move || {
        let (mut s, _) = l.accept().unwrap();
        let (h, body) = raw_recv(&mut s).unwrap();
        let hello: Hello = decode_body(&body).unwrap();
        let _ = fake_server_finish(&mut s, &fake_key, &hello, h.req_id, false);
        let _ = s.flush();
    });
    let c = client("tcp", k, Role::Client);
    let ep = Endpoint { node: 1, addrs: vec![("tcp".into(), addr)] };
    let r = c.call(&ep, Family::Data, frame(MSG_PING, b""), T);
    assert_eq!(r.err(), Some(ErrCode::AUTH_FAILED), "server_proof 不对 = 冒充的服务端（双向认证）");
    fake.join().unwrap();
}

/// 假的元数据服务端：握手时回 `Challenge.features = 0`（`fake_server_finish`：什么 feature 也不声明），之后 GetClusterMap 回一张空的
/// 集群图、SessionOpen 回会话、别的元数据请求回 Ok，并把收到的元数据请求的名字交给 `seen`。每条连接一个线程（本机别的程序也可能连上来）。
fn featureless_meta_server(l: TcpListener, seen: Sender<String>) {
    for s in l.incoming() {
        let (Ok(mut s), seen) = (s, seen.clone()) else { continue };
        std::thread::spawn(move || {
            let Ok((h, body)) = raw_recv(&mut s) else { return };
            let Ok(hello) = decode_body::<Hello>(&body) else { return };
            if fake_server_finish(&mut s, &key(2), &hello, h.req_id, true).is_err() {
                return;
            }
            while let Ok((h, body)) = raw_recv(&mut s) {
                let reply = match h.msg_type {
                    MSG_NODE_CALL => encode_body(&NodeReply::Map(Box::default())),
                    MSG_META_CALL => {
                        let Ok(call) = decode_body::<MetaCall>(&body) else { return };
                        let _ = seen.send(call.op.name().to_string());
                        match call.op {
                            MetaOp::SessionOpen { .. } => {
                                encode_body(&MetaReply::Session { lease_ms: 30_000, map_epoch: 0 })
                            }
                            _ => encode_body(&MetaReply::Ok),
                        }
                    }
                    _ => continue, // 保活 Ping：不用回
                };
                if raw_send(&mut s, FrameKind::Response, h.msg_type, h.req_id, &reply).is_err() {
                    return;
                }
            }
        });
    }
}

/// 对端在握手里没声明 `FEAT_LOCKS` / `FEAT_STRONG`：setlk（SetLock）、getlk（GetLock）、strong 的追加（AppendReserve）不发、回
/// `EOPNOTSUPP`；别的请求照常发（3.2.11 第 3 条：新的消息变体只能在对端声明支持之后发，ADR-0043 第 3 条）。
#[test]
fn lock_and_append_requests_are_not_sent_to_a_peer_without_the_feature() {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap().to_string();
    let (tx, rx) = channel();
    std::thread::spawn(move || featureless_meta_server(l, tx));
    let ep = Endpoint { node: 1, addrs: vec![("tcp".into(), addr)] };
    let meta = MetaClient::connect(client("tcp", key(2), Role::Client), &[ep], SessionInfo::default()).unwrap();
    let who = Caller { uid: 1000, gid: 1000, groups: vec![], pid: 1, job_id: 0 };
    let setlk = MetaOp::SetLock { ino: 18, owner: 1, pid: 1, start: 0, end: 9, kind: 2 };
    assert_eq!(meta.write(&who, setlk), Err(ErrCode::EOPNOTSUPP), "对端没声明 FEAT_LOCKS：setlk 回 EOPNOTSUPP");
    let getlk = MetaOp::GetLock { ino: 18, owner: 1, start: 0, end: 9, write: true };
    assert_eq!(meta.read(&who, getlk), Err(ErrCode::EOPNOTSUPP));
    let append = MetaOp::AppendReserve { ino: 18, epoch: 1, len: 10 };
    assert_eq!(meta.write(&who, append), Err(ErrCode::EOPNOTSUPP), "对端没声明 FEAT_STRONG");
    assert_eq!(meta.read(&who, MetaOp::GetAttr { ino: 18 }), Ok(MetaReply::Ok), "别的请求照常发");
    // 同一条连接上的请求按顺序处理：GetAttr 回来了，之前发出的都已经被看到
    assert_eq!(rx.try_iter().collect::<Vec<_>>(), ["SessionOpen", "GetAttr"], "要 feature 位的三个请求一个也没发出去");
}

#[test]
fn one_way_frames_are_checked_against_the_role_table_too() {
    let denied = || counter("wmfs_rpc_denied_total", &[("role", "admin"), ("family", "RaftMsg")]);
    let before = denied();
    let s = tcp();
    let mut raw = TcpStream::connect(&s.addr).unwrap();
    raw_handshake(&mut raw, &key(1), Role::Admin).unwrap();
    raw_send(&mut raw, FrameKind::OneWay, MSG_RAFT, 0, b"").unwrap();
    // OneWay 没法回 EACCES：只计数、丢弃；连接照常
    let ping = encode_body(&PingBody { payload: vec![1] });
    raw_send(&mut raw, FrameKind::Request, MSG_PING, 7, &ping).unwrap();
    let (h, body) = raw_recv(&mut raw).unwrap();
    assert_eq!((h.req_id, body), (7, ping), "下一个请求照常得到回复");
    assert_eq!(denied(), before + 1, "admin 发 RaftMsg：角色权限表拒绝并计数");
}

#[test]
fn a_reply_whose_bulk_was_dropped_fails_with_busy() {
    for s in [tcp(), mem("faults-busy")] {
        // 客户端自己的池：M 档 4 槽，1 个只给复制流量；data 占 1 个普通槽，hog 占掉另外 2 个
        let p = pool();
        let c = RpcClient::new_with_role(p.clone(), vec![transport(s.scheme)], key(1), Role::Admin, net(s.scheme));
        let c = c.unwrap();
        let data = pattern(&p, 1 << 20);
        let hog: Vec<_> = (0..2).map(|_| p.get(1 << 20, Lane::Normal, T).unwrap()).collect();
        // 不给 recv_into：回复的 bulk 只能从池里取，取不到 → 读出丢弃 → 这次调用按 BUSY 结束（可重试），不当成功
        let f = OutFrame { bulk: vec![data.whole()], ..frame(MSG_PING, b"no-recv-into") };
        assert_eq!(c.call(&s.ep, Family::Data, f, T).err(), Some(ErrCode::BUSY), "{}", s.scheme);
        drop(hog);
        let f = OutFrame { bulk: vec![data.whole()], ..frame(MSG_PING, b"again") };
        assert!(c.call(&s.ep, Family::Data, f, T).and_then(check_reply).is_ok(), "{}：有空槽之后成功", s.scheme);
    }
}

/// 回一个超过 1 MiB 的 body（超过协议上限）。
struct Huge;
impl Service for Huge {
    fn call(&self, _f: InFrame, r: Responder) {
        r.reply(vec![0; MAX_BODY as usize + 1], vec![]);
    }
}

#[test]
fn an_oversized_reply_fails_only_that_request() {
    for s in [tcp(), mem("faults-huge")] {
        s.rpc.add_service(MSG_META_CALL, Arc::new(Huge));
        let c = s.client(key(1), Role::Admin);
        let t0 = Instant::now();
        let r = c.call(&s.ep, Family::Control, frame(MSG_META_CALL, b""), T).and_then(check_reply);
        assert_eq!(r.err(), Some(ErrCode::FEATURE_UNSUPPORTED), "{}：回复太大 → 改回 FEATURE_UNSUPPORTED", s.scheme);
        assert!(t0.elapsed() < Duration::from_secs(2), "{}：不用等满超时", s.scheme);
        assert!(c.call(&s.ep, Family::Data, frame(MSG_PING, b"x"), T).is_ok(), "{}：连接还在", s.scheme);
    }
}

/// 发一帧之后：请求的 payload 是 "error" 就用 StreamTx::error 结束，否则直接丢掉 StreamTx（没发最后一帧）。
struct HalfStream;
impl Service for HalfStream {
    fn call(&self, f: InFrame, r: Responder) {
        let mut tx = r.into_stream();
        tx.send(vec![1], vec![], false).unwrap();
        if decode_body::<PingBody>(&f.body).is_ok_and(|p| p.payload == b"error") {
            tx.error(ErrCode::ENOSPC);
        }
    }
}

#[test]
fn a_dropped_or_failed_stream_ends_at_once() {
    let dropped = || counter("wmfs_req_dropped_total", &[]);
    let s = tcp();
    s.rpc.add_service(MSG_SNAPSHOT_FETCH, Arc::new(HalfStream));
    let c = s.client(key(0), Role::Server);
    let before = dropped();
    let t0 = Instant::now();
    // 流超时（相邻两帧之间）给 30 s：修好的话 EIO 立刻就到，远在超时之前
    let mut st = c.stream(&s.ep, frame(MSG_SNAPSHOT_FETCH, b"drop"), Duration::from_secs(30)).unwrap();
    assert_eq!(st.next().unwrap().map(|f| f.body), Some(vec![1]));
    assert_eq!(st.next().err(), Some(ErrCode::EIO), "没发最后一帧就丢掉 StreamTx → EIO");
    assert!(t0.elapsed() < Duration::from_secs(5), "不用等流的超时：{:?}", t0.elapsed());
    assert!(dropped() > before, "计数 wmfs_req_dropped_total");
    let mut st = c.stream(&s.ep, frame(MSG_SNAPSHOT_FETCH, b"error"), Duration::from_secs(30)).unwrap();
    assert_eq!(st.next().unwrap().map(|f| f.body), Some(vec![1]));
    assert_eq!(st.next().err(), Some(ErrCode::ENOSPC), "StreamTx::error 显式报错");
}
