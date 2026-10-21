//! tcp 插件的单元测试：manifest、帧收发（body + 多段 bulk、recv_into）、坏帧头、握手前不收 bulk、池满时丢弃 bulk、
//! PeerCall 用复制 lane、写超时。握手、Ping、fail point 重连重试、慢客户端等跨插件的测试在 wmfs-testkit
//! （tests/it/net_rpc.rs、slow_client.rs、failpoint_retry.rs、conn_idle.rs、conn_faults.rs）。

use std::sync::mpsc;

use wmfs_core::buf::{Lane, PoolConfig};
use wmfs_core::hooks::manifest::check_manifest_toml;
use wmfs_core::proto::frame::{MSG_PEER_CALL, MSG_PING};

use super::*;

fn pool(m_slots: u32) -> MemPool {
    MemPool::new(&PoolConfig { s_mb: 1, m_slots, l_slots: 1, repl_lane: false }).unwrap()
}

/// 连好一对连接（客户端，服务端），两头都已"认证"（握手由 core 做；这里直接告诉插件握手完成了）。
fn pair(t: &TcpTransport, sp: MemPool, cp: MemPool) -> (Box<dyn Conn>, Box<dyn Conn>) {
    let (addr, rx) = listen(t, sp);
    let c = t.connect(&addr, cp, Duration::from_secs(5)).unwrap();
    let s = rx.recv_timeout(Duration::from_secs(5)).unwrap();
    c.mark_authenticated();
    s.mark_authenticated();
    (c, s)
}

/// 在 127.0.0.1:0 上监听；返回 (传输, 地址, 收到的服务端连接)。
fn listen(t: &TcpTransport, p: MemPool) -> (String, mpsc::Receiver<Box<dyn Conn>>) {
    let (tx, rx) = mpsc::channel();
    let tx = Mutex::new(tx);
    let on = Arc::new(move |c: Box<dyn Conn>| tx.lock().unwrap().send(c).unwrap());
    (t.listen(p, on).unwrap().unwrap(), rx)
}

fn transport(write_timeout_ms: u32) -> TcpTransport {
    let cfg: PluginTable = "listen = \"127.0.0.1:0\"".parse().unwrap();
    TcpTransport::new(&cfg, write_timeout_ms).unwrap()
}

/// 像 core 的写线程那样填好帧头的长度与 CRC（Ping 帧）。
fn out(
    kind: FrameKind,
    req_id: u64,
    body: &[u8],
    bulk: Vec<wmfs_core::buf::BufSlice>,
    recv_into: Option<Buf>,
) -> OutFrame {
    out_msg(kind, MSG_PING, req_id, body, bulk, recv_into)
}

fn out_msg(
    kind: FrameKind,
    msg: u16,
    req_id: u64,
    body: &[u8],
    bulk: Vec<wmfs_core::buf::BufSlice>,
    recv_into: Option<Buf>,
) -> OutFrame {
    let mut hdr = FrameHeader::new(kind, msg, req_id, 7);
    hdr.body_len = body.len() as u32;
    hdr.body_crc32c = crc32c::crc32c(body);
    let mut crc = 0;
    for s in &bulk {
        hdr.bulk_len += s.len;
        crc = crc32c::crc32c_append(crc, s.as_slice());
    }
    hdr.bulk_crc32c = crc;
    OutFrame { hdr, body: body.to_vec(), bulk, recv_into }
}

fn filled(p: &MemPool, n: usize, byte: u8) -> wmfs_core::buf::FrozenBuf {
    let mut b = p.get(n, Lane::Normal, Duration::ZERO).unwrap();
    b.as_mut_slice().fill(byte);
    b.freeze()
}

#[test]
fn manifest_toml_matches_code() {
    check_manifest_toml(&MANIFEST, include_str!("manifest.toml")).unwrap();
    assert!(TcpTransport::new(&"listen = 1".parse().unwrap(), 5000).is_err());
    assert!(TcpTransport::new(&PluginTable::new(), 5000).is_ok(), "没写 listen 就用默认值");
}

#[test]
fn frames_with_bulk_and_recv_into_round_trip() {
    let (t, sp, cp) = (transport(5000), pool(4), pool(4));
    let (c, s) = pair(&t, sp.clone(), cp.clone());
    // 请求：body + 两段 bulk（来自同一个 FrozenBuf 的两个子区间），外加给回复用的 recv_into
    let data = filled(&cp, 300_000, 0xAB);
    let into = cp.get(1 << 20, Lane::Normal, Duration::ZERO).unwrap();
    let into_addr = into.addr();
    c.send(out(FrameKind::Request, 5, b"hello", vec![data.slice(0, 1000), data.slice(1000, 299_000)], Some(into)))
        .unwrap();
    let f = s.recv().unwrap();
    assert_eq!((f.hdr.req_id, f.body.as_slice(), f.bulk_dropped), (5, &b"hello"[..], false));
    let b = f.bulk.unwrap();
    assert_eq!(b.len(), 300_000);
    assert!(b.as_slice().iter().all(|x| *x == 0xAB));
    assert_eq!(crc32c::crc32c(b.as_slice()), f.hdr.bulk_crc32c);
    // 回复：bulk 读进请求方给的 recv_into（同一个缓冲）
    s.send(out(FrameKind::Response, 5, b"", vec![b.freeze().whole()], None)).unwrap();
    let r = c.recv().unwrap();
    let rb = r.bulk.unwrap();
    assert_eq!((rb.addr(), rb.len()), (into_addr, 300_000), "回复的 bulk 应读进 recv_into");
}

#[test]
fn bad_header_is_a_protocol_error() {
    let (t, sp) = (transport(5000), pool(2));
    let (addr, rx) = listen(&t, sp);
    let mut raw = TcpStream::connect(&addr).unwrap();
    let s = rx.recv_timeout(Duration::from_secs(5)).unwrap();
    raw.write_all(&[0x55; HEADER_LEN]).unwrap();
    assert_eq!(s.recv().err(), Some(ErrCode::PROTO_UNSUPPORTED));
    // 帧头合法但带了 tcp 不支持的单边描述符标志
    let mut f = out(FrameKind::Request, 1, b"", vec![], None);
    f.hdr.flags = FLAG_BULK_REMOTE;
    let mut raw2 = TcpStream::connect(&addr).unwrap();
    let s2 = rx.recv_timeout(Duration::from_secs(5)).unwrap();
    raw2.write_all(&f.hdr.encode()).unwrap();
    assert_eq!(s2.recv().err(), Some(ErrCode::PROTO_UNSUPPORTED));
}

#[test]
fn bulk_is_dropped_when_the_pool_is_full() {
    let (t, sp, cp) = (transport(5000), pool(1), pool(2));
    let (c, s) = pair(&t, sp.clone(), cp.clone());
    let hog = sp.get(1 << 20, Lane::Normal, Duration::ZERO).unwrap(); // 占住服务端唯一的 M 槽
    let data = filled(&cp, 1 << 20, 1);
    c.send(out(FrameKind::Request, 1, b"a", vec![data.whole()], None)).unwrap();
    c.send(out(FrameKind::Request, 2, b"b", vec![], None)).unwrap();
    let f = s.recv().unwrap();
    assert!(f.bulk_dropped && f.bulk.is_none(), "取不到缓冲：bulk 读出丢弃");
    let g = s.recv().unwrap();
    assert_eq!((g.hdr.req_id, g.body.as_slice()), (2, &b"b"[..]), "丢弃之后字节流仍然同步");
    drop(hog);
}

#[test]
fn unauthenticated_frames_with_bulk_are_rejected_before_taking_a_slot() {
    let (t, sp, cp) = (transport(5000), pool(2), pool(2));
    let (addr, rx) = listen(&t, sp.clone());
    let c = t.connect(&addr, cp.clone(), Duration::from_secs(5)).unwrap();
    let s = rx.recv_timeout(Duration::from_secs(5)).unwrap(); // 服务端这头还没认证
    let free = || sp.stats()[1].free_normal;
    let before = free();
    let data = filled(&cp, 1 << 20, 9);
    c.send(out(FrameKind::Request, 1, b"hello?", vec![data.whole()], None)).unwrap();
    assert_eq!(s.recv().err(), Some(ErrCode::PROTO_UNSUPPORTED), "握手帧不带 bulk：声明了 bulk 就断开");
    assert_eq!(free(), before, "没有为它从池里取槽");
}

#[test]
fn peer_call_bulk_uses_the_repl_lane() {
    // 服务端的池：M 档 4 槽，其中 1 个只给复制流量（Repl lane）
    let sp = MemPool::new(&PoolConfig { s_mb: 1, m_slots: 4, l_slots: 1, repl_lane: true }).unwrap();
    let (t, cp) = (transport(5000), pool(2));
    let (c, s) = pair(&t, sp.clone(), cp.clone());
    let hog: Vec<Buf> = (0..3).map(|_| sp.get(1 << 20, Lane::Normal, Duration::ZERO).unwrap()).collect();
    let data = filled(&cp, 1 << 20, 4);
    c.send(out_msg(FrameKind::Request, MSG_PING, 1, b"", vec![data.whole()], None)).unwrap();
    assert!(s.recv().unwrap().bulk_dropped, "普通槽用完了：Ping 的 bulk 被丢弃");
    c.send(out_msg(FrameKind::Request, MSG_PEER_CALL, 2, b"", vec![data.whole()], None)).unwrap();
    let f = s.recv().unwrap();
    assert!(!f.bulk_dropped && f.bulk.is_some_and(|b| b.len() == 1 << 20), "PeerCall 的 bulk 仍能取到复制槽（C-5）");
    drop(hog);
}

#[test]
fn write_times_out_when_the_peer_never_reads() {
    let (t, sp, cp) = (transport(300), pool(2), pool(2));
    let (addr, rx) = listen(&t, sp);
    let c = t.connect(&addr, cp.clone(), Duration::from_secs(5)).unwrap();
    let _s = rx.recv_timeout(Duration::from_secs(5)).unwrap(); // 服务端连接存在，但从不读
    let data = filled(&cp, 4 << 20, 2);
    let mut err = None;
    for i in 0..64 {
        if let Err(e) = c.send(out(FrameKind::Request, i, b"", vec![data.whole()], None)) {
            err = Some(e);
            break;
        }
    }
    assert_eq!(err, Some(ErrCode::ETIMEDOUT), "socket 缓冲写满后 300 ms 写超时");
}
