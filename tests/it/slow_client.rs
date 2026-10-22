//! 慢客户端（roadmap M1 验收标准，design.md 附录 C 的 C-15）：一个握手之后**故意不读 socket** 的假客户端，
//! 不会卡住服务端——服务端写线程写不出去 5 s（`[net] write_timeout_ms`）后断开它、计数 `wmfs_conn_write_timeout_total`，
//! 期间别的客户端照常 ping（每个连接有自己的写线程与有界发送队列）。

use std::io::Read;
use std::net::TcpStream;
use std::time::{Duration, Instant};

use wmfs_core::api::metrics;
use wmfs_core::base::types::Role;
use wmfs_core::net::client::{Family, check_reply};
use wmfs_core::proto::encode_body;
use wmfs_core::proto::frame::{FrameKind, MSG_PING};
use wmfs_core::proto::handshake::PingBody;
use wmfs_testkit::harness::net::*;

#[test]
fn a_client_that_never_reads_is_disconnected_and_others_are_unaffected() {
    let timeouts = || metrics::counter("wmfs_conn_write_timeout_total", "", &[]).get();
    let before = timeouts();
    let s = tcp();
    // 假客户端：握手成功后发 200 个各带 256 KiB body 的 Ping，然后一个字节也不读。
    // 计时从发第一个 Ping 之前开始：服务端写回复（以及 5 s 的写超时）在发送的过程中就开始了，debug 构建里这 50 MiB
    // 要发 1 s 多，从发完才计时会把这段算漏
    let mut bad = TcpStream::connect(&s.addr).unwrap();
    raw_handshake(&mut bad, &key(2), Role::Client).unwrap();
    let big = encode_body(&PingBody { payload: vec![0xEE; 256 << 10] });
    let t0 = Instant::now();
    for i in 0..200 {
        raw_send(&mut bad, FrameKind::Request, MSG_PING, 10 + i, &big).unwrap();
    }
    // 好客户端：同时每 50 ms ping 一次，一直到服务端断开坏客户端为止（最多 30 s）
    let good = s.client(key(1), Role::Admin);
    let (mut ok, mut worst) = (0, Duration::ZERO);
    while timeouts() == before {
        assert!(t0.elapsed() < Duration::from_secs(30), "30 s 内服务端应该因写超时断开坏客户端");
        let t = Instant::now();
        check_reply(good.call(&s.ep, Family::Data, frame(MSG_PING, b"good"), T).unwrap()).unwrap();
        worst = worst.max(t.elapsed());
        ok += 1;
        std::thread::sleep(Duration::from_millis(50));
    }
    let waited = t0.elapsed();
    eprintln!("断开用了 {waited:?}，好客户端 ping {ok} 次，最慢 {worst:?}");
    assert!(waited >= Duration::from_secs(4), "写超时是 5 s，不应提前断开（用了 {waited:?}）");
    assert!(ok >= 20 && worst < Duration::from_secs(1), "好客户端不受影响：{ok} 次 ping，最慢 {worst:?}");
    // 坏客户端那头：读完内核里缓冲的回复之后看到连接已断开（EOF 或 reset），而不是永远挂着
    bad.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let mut sink = vec![0u8; 1 << 20];
    let closed = loop {
        match bad.read(&mut sink) {
            Ok(0) | Err(_) => break true,
            Ok(_) => continue,
        }
    };
    assert!(closed);
    eprintln!("读到断开用了 {:?}", t0.elapsed());
    assert!(good.call(&s.ep, Family::Data, frame(MSG_PING, b"after"), T).is_ok(), "之后照常服务");
}
