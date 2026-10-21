//! 不按规矩来的对端（改过的帧头，或用假网卡直接往接收槽里塞字节）：连接一律断开（`recv` 返回 `PROTO_UNSUPPORTED`），
//! 不发 READ、不取池槽、不 panic。覆盖 3.3.2（握手完成之前不做单边操作）、3.3.3（流帧与 OneWay 不走 rdma）与 3.6.4
//! 实现注记（M2）③④（描述符表要与 bulk_len 对得上；回复的 bulk 必须有对应的 recv_into）。

use wmfs_core::base::ErrCode;
use wmfs_core::buf::Lane;
use wmfs_core::proto::frame::{FrameHeader, FrameKind, MSG_PING};

use super::conn::{Z, authed, filled, out, pair};

#[test]
fn rdma_conn_no_one_sided_ops_before_authentication() {
    let q = pair();
    let d = filled(&q.cp, 1000, 2);
    let early = q.c.send(out(FrameKind::Request, 1, b"", vec![d.whole()], None));
    assert_eq!(early.err(), Some(ErrCode::FEATURE_UNSUPPORTED), "握手之前本端不发带描述符的帧");
    let p = pair();
    p.c.mark_authenticated(); // 只有客户端这头认证了：服务端在握手完成之前收到声明了 bulk 的帧
    let before = p.sp.stats()[1].free_normal;
    let data = filled(&p.cp, 1 << 20, 3);
    p.c.send(out(FrameKind::Request, 1, b"hello?", vec![data.whole()], None)).unwrap();
    assert_eq!(p.s.recv().err(), Some(ErrCode::PROTO_UNSUPPORTED), "断开");
    assert_eq!(p.nic.n(&p.nic.0.reads), 0, "没有发 READ");
    assert_eq!(p.sp.stats()[1].free_normal, before, "没有为它取池槽");
}

/// 服务端握手完成之前收到只带 REPLY_REMOTE（没有 bulk）的请求 → 断开。
#[test]
fn rdma_conn_reply_remote_before_authentication_disconnects() {
    let p = pair();
    p.c.mark_authenticated(); // 只有客户端这头认证了
    let into = p.cp.get(1 << 20, Lane::Normal, Z).unwrap();
    p.c.send(out(FrameKind::Request, 1, b"hello?", vec![], Some(into))).unwrap();
    assert_eq!(p.s.recv().err(), Some(ErrCode::PROTO_UNSUPPORTED), "断开");
}

/// 已认证的对端声明很小的 bulk_len、却给出很长的描述符 → 断开、不发 READ（否则 READ 会写穿相邻的池槽）。
#[test]
fn rdma_conn_descriptor_longer_than_bulk_len_disconnects() {
    let p = authed(pair());
    let data = filled(&p.cp, 1 << 20, 0x5a);
    let mut f = out(FrameKind::Request, 3, b"x", vec![data.whole()], None);
    f.hdr.bulk_len = 100; // 只分到一个小槽
    p.c.send(f).unwrap();
    assert_eq!(p.s.recv().err(), Some(ErrCode::PROTO_UNSUPPORTED));
    assert_eq!(p.nic.n(&p.nic.0.reads), 0, "没有发 READ");
}

/// 帧头声称的 body_len 比实际 SEND 的字节多 → 断开（不 panic）。
#[test]
fn rdma_conn_body_len_beyond_received_bytes_disconnects() {
    let p = authed(pair());
    let mut f = out(FrameKind::Request, 4, b"short", vec![], None);
    f.hdr.body_len = 4096;
    p.c.send(f).unwrap();
    assert_eq!(p.s.recv().err(), Some(ErrCode::PROTO_UNSUPPORTED));
}

/// 一帧编码好的字节（帧头 + body），用来直接塞进接收槽。
fn raw(kind: FrameKind, req_id: u64, bulk_len: u32) -> Vec<u8> {
    let mut h = FrameHeader::new(kind, MSG_PING, req_id, 0);
    h.bulk_len = bulk_len;
    h.encode().to_vec()
}

/// 回复帧声明了 bulk，客户端却没有为这个 req_id 给过 recv_into、或给的放不下（正常的对端做不到：没有 REPLY_REMOTE 就不会
/// WRITE，放不下就不 WRITE）→ 断开，不 panic。
#[test]
fn rdma_conn_reply_bulk_without_a_fitting_recv_into_disconnects() {
    let p = authed(pair());
    assert!(p.nic.inject(1, &raw(FrameKind::Response, 77, 100)), "客户端的 QP 是 qpn 1");
    assert_eq!(p.c.recv().err(), Some(ErrCode::PROTO_UNSUPPORTED), "没有 recv_into");
    let q = authed(pair());
    let small = q.cp.get(1000, Lane::Normal, Z).unwrap(); // S 档：容量 128 KiB
    q.c.send(out(FrameKind::Request, 8, b"", vec![], Some(small))).unwrap();
    q.s.recv().unwrap();
    assert!(q.nic.inject(1, &raw(FrameKind::Response, 8, 200 << 10)));
    assert_eq!(q.c.recv().err(), Some(ErrCode::PROTO_UNSUPPORTED), "recv_into 放不下");
}

/// 流帧与 OneWay 不走 rdma（3.3.3）：收到就断开（两侧都一样）。
#[test]
fn rdma_conn_stream_and_oneway_frames_disconnect() {
    let p = authed(pair());
    assert!(p.nic.inject(2, &raw(FrameKind::OneWay, 0, 0)), "服务端的 QP 是 qpn 2");
    assert_eq!(p.s.recv().err(), Some(ErrCode::PROTO_UNSUPPORTED), "OneWay");
    let q = authed(pair());
    assert!(q.nic.inject(1, &raw(FrameKind::StreamChunk, 5, 0)));
    assert_eq!(q.c.recv().err(), Some(ErrCode::PROTO_UNSUPPORTED), "流帧");
}

/// 收到的字节连一个帧头都不够、或帧头坏了 → 断开。
#[test]
fn rdma_conn_short_or_corrupt_frames_disconnect() {
    let p = authed(pair());
    assert!(p.nic.inject(2, &[0u8; 20]));
    assert_eq!(p.s.recv().err(), Some(ErrCode::PROTO_UNSUPPORTED), "不到 48 字节");
    let q = authed(pair());
    let mut bad = raw(FrameKind::Request, 1, 0);
    bad[10] ^= 1; // 帧头 CRC 不对
    assert!(q.nic.inject(2, &bad));
    assert_eq!(q.s.recv().err(), Some(ErrCode::PROTO_UNSUPPORTED), "帧头 CRC");
}
