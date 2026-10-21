//! "帧超过该传输的上限只让这一个请求失败、不断开连接"（design.md 3.3.8、3.6.4）：mem 传输可以给帧头 + body 设一个
//! 很小的上限（模拟 RDMA 的 120 KiB）。请求太大 → 客户端的写线程发不出去，这个调用很快以 FEATURE_UNSUPPORTED 结束
//! （不是等满 10 s 超时、再被当成可重试的 ETIMEDOUT 一遍遍重发）；回复太大 → 服务端改回一个同 req_id 的 ERROR 帧。
//! 两种情况下连接都不断（只握手过一次）。单独一个测试进程：`ACCEPTS` 是全进程的计数。

mod common;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use common::{ACCEPTS, T, TestAuthn, client_on, frame, key, ping, server};
use plugin_mem_transport::MemTransport;
use wmfs_core::base::ErrCode;
use wmfs_core::base::types::Role;
use wmfs_core::net::client::{Family, check_reply};
use wmfs_core::net::server::{Responder, Service};
use wmfs_core::proto::frame::{InFrame, MSG_META_CALL};

/// 不管请求是什么，都回一个 4 KiB 的 body（超过服务端传输的 1 KiB 上限）。
struct BigReply;
impl Service for BigReply {
    fn call(&self, _f: InFrame, r: Responder) {
        r.reply(vec![7; 4096], vec![]);
    }
}

#[test]
fn oversized_frames_fail_only_that_request() {
    let (rpc, ep) = server(MemTransport::listening("mem-limit").with_frame_limit(1024), Arc::new(TestAuthn));
    rpc.add_service(MSG_META_CALL, Arc::new(BigReply));
    let c = client_on(MemTransport::new().with_frame_limit(1024), key(2), Role::Client);
    assert!(ping(&c, &ep, b"small").is_ok());
    // 请求太大：客户端写线程发不出去
    let t0 = Instant::now();
    assert_eq!(ping(&c, &ep, &[1; 2000]).err(), Some(ErrCode::FEATURE_UNSUPPORTED));
    assert!(t0.elapsed() < Duration::from_secs(2), "应该很快失败，而不是等满超时：{:?}", t0.elapsed());
    // 回复太大：服务端改回 ERROR(FEATURE_UNSUPPORTED)
    let t0 = Instant::now();
    let r = c.call(&ep, Family::Control, frame(MSG_META_CALL, b""), T).and_then(check_reply);
    assert_eq!(r.err(), Some(ErrCode::FEATURE_UNSUPPORTED));
    assert!(t0.elapsed() < Duration::from_secs(2), "{:?}", t0.elapsed());
    // 连接一直没断：之后照常 ping，而且只握手过一次
    assert!(ping(&c, &ep, b"after").is_ok());
    assert_eq!(ACCEPTS.load(Ordering::SeqCst), 1, "超限的帧不应让连接断开、重新握手");
}
