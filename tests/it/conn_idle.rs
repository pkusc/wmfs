//! 连接的保活与读超时（design.md 3.3.3 的保活注记、4.1）。几个用例都要等十几秒，放在同一个测试进程里并行跑：
//! ① 认证后空闲 20 s 的连接靠客户端的保活 Ping 一直活着（不会被服务端 15 s 的读超时断开、再重新握手）；
//! ② 什么都不发的裸连接约 15 s 后被服务端断开（tcp 的读超时）；
//! ③ 握手完成之前客户端不发保活 Ping：服务端 6 s 后才回 Challenge，握手照样在 10 s 的期限内成功，不用重连。
//! （只有 ① 用到 shared-key 的认证计数，② ③ 都不经过它，所以可以并行。）

use std::io::{ErrorKind, Read};
use std::net::{TcpListener, TcpStream};
use std::time::{Duration, Instant};

use wmfs_core::api::metrics;
use wmfs_core::base::types::Role;
use wmfs_core::net::client::{Endpoint, Family, check_reply};
use wmfs_core::proto::decode_body;
use wmfs_core::proto::frame::{FrameKind, MSG_PING};
use wmfs_core::proto::handshake::Hello;
use wmfs_testkit::harness::net::*;

#[test]
fn idle_authenticated_connection_is_kept_alive() {
    let accepts = || metrics::counter("wmfs_shared_key_auth_total", "", &[("result", "accept")]).get();
    let s = tcp();
    let c = s.client(key(2), Role::Client);
    check_reply(c.call(&s.ep, Family::Data, frame(MSG_PING, b"a"), T).unwrap()).unwrap();
    let before = accepts();
    std::thread::sleep(Duration::from_secs(20));
    check_reply(c.call(&s.ep, Family::Data, frame(MSG_PING, b"b"), T).unwrap()).unwrap();
    assert_eq!(accepts(), before, "空闲 20 s 后不应重新握手（保活 Ping 让连接一直活着）");
}

#[test]
fn silent_raw_connection_is_dropped_after_15s() {
    let s = tcp();
    let mut raw = TcpStream::connect(&s.addr).unwrap();
    raw.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    let t0 = Instant::now();
    let r = raw.read(&mut [0u8; 16]);
    let waited = t0.elapsed();
    assert!(matches!(r, Ok(0)) || r.is_err(), "应该看到连接被断开：{r:?}");
    assert!(waited >= Duration::from_secs(14) && waited < Duration::from_secs(25), "约 15 s 后被断开：{waited:?}");
}

#[test]
fn no_keepalive_ping_before_the_handshake_completes() {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap().to_string();
    let k = key(2);
    let fake_key = k.clone();
    // 假服务端：读到 Hello 之后 6 s 才回 Challenge；这 6 s 里客户端不该再发任何东西
    let fake = std::thread::spawn(move || -> Result<(), String> {
        let (mut s, _) = l.accept().map_err(|e| e.to_string())?;
        let (h, body) = raw_recv(&mut s).map_err(|e| format!("读 Hello：{e}"))?;
        let hello: Hello = decode_body(&body).map_err(|e| format!("Hello 解不开：{e}"))?;
        s.set_read_timeout(Some(Duration::from_secs(6))).map_err(|e| e.to_string())?;
        match s.read(&mut [0u8; 1]) {
            Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {}
            other => return Err(format!("握手完成之前客户端又发了东西（保活 Ping？）：{other:?}")),
        }
        s.set_read_timeout(Some(Duration::from_secs(10))).map_err(|e| e.to_string())?;
        fake_server_finish(&mut s, &fake_key, &hello, h.req_id, true).map_err(|e| format!("握手：{e}"))?;
        let (h, body) = raw_recv(&mut s).map_err(|e| format!("读 Ping：{e}"))?;
        raw_send(&mut s, FrameKind::Response, MSG_PING, h.req_id, &body).map_err(|e| e.to_string())
    });
    let c = client("tcp", k, Role::Client);
    let ep = Endpoint { node: 1, addrs: vec![("tcp".into(), addr)] };
    let t0 = Instant::now();
    let r = c.call(&ep, Family::Data, frame(MSG_PING, b"slow"), T).and_then(check_reply);
    assert!(r.is_ok(), "握手应在 10 s 的期限内成功：{:?}（用了 {:?}）", r.err(), t0.elapsed());
    fake.join().unwrap().unwrap();
}
