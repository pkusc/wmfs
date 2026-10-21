//! RdmaConn 本身（不经 core）：假网卡 + 回环 TCP 上的引导连接建一对连接，直接调 `Conn::send/recv`。
//! 覆盖：bulk 走 READ、回复 bulk 走 WRITE（多段按偏移依次写）；长度为 0 的段与 tcp 一样无害；帧头 + body 的 120 KiB 上限与
//! 16 段上限只让这一个帧失败；信用控制（第 17 个请求一直阻塞——超过写超时也不放弃、两个 req_id = 0 各占各的、close 能打断）；
//! 回复带 bulk 必须有够大的 REPLY_REMOTE（绝不越界 WRITE）；accept 侧不发带描述符的请求；池满时不发 READ；引导 TCP 断了
//! 连接作废；接收槽走 Normal lane；一个不说话的引导连接卡不住别人、max_qps 之外的引导连接被直接关掉。
//! 不按规矩来的对端在 peer.rs，拆除顺序在 teardown.rs（共用这里的工具）。

use std::net::TcpStream;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use wmfs_core::base::ErrCode;
use wmfs_core::buf::{Buf, BufSlice, FrozenBuf, Lane, MemPool, PoolConfig};
use wmfs_core::hooks::traits::{Conn, Transport};
use wmfs_core::proto::frame::{
    FLAG_BULK_REMOTE, FLAG_HAS_BULK, FLAG_REPLY_REMOTE, FrameHeader, FrameKind, MSG_PING, OutFrame,
};

use super::super::RdmaTransport;
use super::super::params::{MAX_FRAME, RdmaCfg};
use super::fake::FakeNic;

pub const T: Duration = Duration::from_secs(5);
pub const Z: Duration = Duration::ZERO;
/// 这里的传输的 `[net] write_timeout_ms`（等 SEND 完成的上限；假网卡当场完成，用不了这么久）。
pub const WRITE_TIMEOUT_MS: u32 = 500;

/// 测试用的池：S 8 MiB（64 槽，够 2 条连接各 17 个）、M 2 槽、L 1 槽。
pub fn pool(repl: bool) -> MemPool {
    MemPool::new(&PoolConfig { s_mb: 8, m_slots: 2, l_slots: 1, repl_lane: repl }).unwrap()
}

pub fn transport(nic: &FakeNic, listen: bool, max_qps: u32) -> RdmaTransport {
    let listen = listen.then(|| "127.0.0.1:0".parse().unwrap());
    let cfg = RdmaCfg { listen, device: None, gid_index: None, max_qps };
    RdmaTransport::with_provider(cfg, WRITE_TIMEOUT_MS, Arc::new(nic.clone())).unwrap()
}

/// 在 127.0.0.1:0 上监听（假网卡）；返回 (传输, 引导地址, 收到的服务端连接)。
pub fn listening(nic: &FakeNic, sp: &MemPool, max_qps: u32) -> (RdmaTransport, String, mpsc::Receiver<Box<dyn Conn>>) {
    let t = transport(nic, true, max_qps);
    t.register_region(&sp.region()).unwrap();
    let (tx, rx) = mpsc::channel();
    let tx = Mutex::new(tx);
    let on = Arc::new(move |c: Box<dyn Conn>| tx.lock().unwrap().send(c).unwrap());
    let addr = t.listen(sp.clone(), on).unwrap().unwrap();
    (t, addr, rx)
}

/// 一对连接：`c` 是 connect 侧（客户端，假网卡上的 qpn 1——它的 QP 先建），`s` 是 accept 侧（服务端，qpn 2）。
pub struct Pair {
    pub nic: FakeNic,
    pub c: Box<dyn Conn>,
    pub s: Box<dyn Conn>,
    pub cp: MemPool,
    pub sp: MemPool,
}

pub fn pair_with(sp: MemPool, cp: MemPool) -> Pair {
    let nic = FakeNic::new();
    let (_server, addr, rx) = listening(&nic, &sp, 2);
    let client = transport(&nic, false, 2);
    client.register_region(&cp.region()).unwrap();
    let c = client.connect(&addr, cp.clone(), T).unwrap();
    let s = rx.recv_timeout(T).unwrap();
    Pair { nic, c, s, cp, sp }
}

pub fn pair() -> Pair {
    pair_with(pool(false), pool(false))
}

/// 两头都"认证"了（握手由 core 做；这里直接告诉插件握手完成了）。
pub fn authed(p: Pair) -> Pair {
    p.c.mark_authenticated();
    p.s.mark_authenticated();
    p
}

/// 像 core 的写线程那样填好帧头的长度、CRC 与 HAS_BULK。
pub fn out(kind: FrameKind, req_id: u64, body: &[u8], bulk: Vec<BufSlice>, into: Option<Buf>) -> OutFrame {
    let mut hdr = FrameHeader::new(kind, MSG_PING, req_id, 7);
    (hdr.body_len, hdr.body_crc32c) = (body.len() as u32, crc32c::crc32c(body));
    let mut crc = 0;
    for s in &bulk {
        hdr.bulk_len += s.len;
        crc = crc32c::crc32c_append(crc, s.as_slice());
    }
    hdr.bulk_crc32c = crc;
    if hdr.bulk_len > 0 {
        hdr.flags |= FLAG_HAS_BULK;
    }
    OutFrame { hdr, body: body.to_vec(), bulk, recv_into: into }
}

pub fn req(id: u64) -> OutFrame {
    out(FrameKind::Request, id, b"", vec![], None)
}

pub fn filled(p: &MemPool, n: usize, byte: u8) -> FrozenBuf {
    let mut b = p.get(n, Lane::Normal, Z).unwrap();
    b.as_mut_slice().fill(byte);
    b.freeze()
}

#[test]
fn rdma_conn_bulk_is_read_and_reply_bulk_is_written() {
    let p = authed(pair());
    let data = filled(&p.cp, 300_000, 0xAB);
    let into = p.cp.get(1 << 20, Lane::Normal, Z).unwrap();
    let into_addr = into.addr();
    let two = vec![data.slice(0, 1000), data.slice(1000, 299_000)];
    p.c.send(out(FrameKind::Request, 5, b"hello", two, Some(into))).unwrap();
    let f = p.s.recv().unwrap();
    assert_eq!((f.hdr.req_id, f.body.as_slice(), f.bulk_dropped), (5, &b"hello"[..], false));
    assert_eq!(f.hdr.flags & (FLAG_BULK_REMOTE | FLAG_REPLY_REMOTE), FLAG_BULK_REMOTE | FLAG_REPLY_REMOTE);
    let b = f.bulk.unwrap();
    assert!(b.len() == 300_000 && b.as_slice().iter().all(|x| *x == 0xAB));
    assert_eq!(p.nic.n(&p.nic.0.reads), 2, "两段描述符 → 两个 READ，读进一个池缓冲");
    p.s.send(out(FrameKind::Response, 5, b"", vec![b.freeze().whole()], None)).unwrap();
    let r = p.c.recv().unwrap();
    let rb = r.bulk.unwrap();
    assert_eq!((rb.addr(), rb.len()), (into_addr, 300_000), "回复的 bulk 被 WRITE 进了 recv_into");
    assert!(rb.as_slice().iter().all(|x| *x == 0xAB));
    assert_eq!((p.nic.n(&p.nic.0.writes), p.nic.n(&p.nic.0.rnr), p.nic.n(&p.nic.0.violations)), (1, 0, 0));
    assert!(data.try_unfreeze().is_ok(), "回复到达之后，在途表放下了 BufSlice");
}

/// 回复的 bulk 有两段：WRITE 按偏移依次写进 recv_into（第二段接在第一段后面）。
#[test]
fn rdma_conn_two_segment_reply_is_written_in_order() {
    let p = authed(pair());
    let into = p.cp.get(1 << 20, Lane::Normal, Z).unwrap();
    p.c.send(out(FrameKind::Request, 6, b"", vec![], Some(into))).unwrap();
    p.s.recv().unwrap();
    let (a, b) = (filled(&p.sp, 1000, 1), filled(&p.sp, 2000, 2));
    p.s.send(out(FrameKind::Response, 6, b"", vec![a.whole(), b.whole()], None)).unwrap();
    let got = p.c.recv().unwrap().bulk.unwrap();
    assert_eq!((got.len(), p.nic.n(&p.nic.0.writes)), (3000, 2));
    assert!(got.as_slice()[..1000].iter().all(|x| *x == 1) && got.as_slice()[1000..].iter().all(|x| *x == 2));
}

/// 长度为 0 的 BufSlice 在 core 里是合法的，tcp 遇到它什么也不写：rdma 不为它生成描述符，连接照常（不断开）。
#[test]
fn rdma_conn_zero_length_segments_are_harmless_like_tcp() {
    let p = authed(pair());
    let data = filled(&p.cp, 1000, 0x11);
    p.c.send(out(FrameKind::Request, 9, b"z", vec![data.slice(0, 0), data.slice(0, 1000)], None)).unwrap();
    let f = p.s.recv().unwrap();
    assert_eq!((f.hdr.req_id, f.bulk.map(|b| b.len())), (9, Some(1000)), "空段不进描述符表，另一段照常 READ");
    p.c.send(out(FrameKind::Request, 10, b"z", vec![data.slice(1000, 0)], None)).unwrap();
    let f = p.s.recv().unwrap();
    assert_eq!(
        (f.hdr.req_id, f.hdr.flags & FLAG_BULK_REMOTE, f.bulk.is_none()),
        (10, 0, true),
        "全是空段：不带 BULK_REMOTE"
    );
    let segs: Vec<BufSlice> = (0..20).map(|_| data.slice(0, 0)).chain([data.slice(0, 10)]).collect();
    p.c.send(out(FrameKind::Request, 11, b"z", segs, None)).unwrap(); // 16 段的上限只数非空段
    assert_eq!(p.s.recv().unwrap().bulk.map(|b| b.len()), Some(10));
    assert_eq!((p.nic.n(&p.nic.0.reads), p.nic.n(&p.nic.0.violations)), (2, 0));
}

#[test]
fn rdma_conn_frame_limits_only_fail_that_frame() {
    let p = authed(pair());
    let body = vec![7u8; MAX_FRAME - 48];
    p.c.send(out(FrameKind::Request, 1, &body, vec![], None)).unwrap();
    assert_eq!(p.s.recv().unwrap().body.len(), MAX_FRAME - 48, "帧头 + body 正好 120 KiB 能发");
    let big = vec![7u8; MAX_FRAME - 48 + 1];
    let unsupported = Some(ErrCode::FEATURE_UNSUPPORTED);
    assert_eq!(p.c.send(out(FrameKind::Request, 2, &big, vec![], None)).err(), unsupported, "多 1 字节：不截断");
    let d = filled(&p.cp, 17, 1);
    let segs: Vec<BufSlice> = (0..17).map(|i| d.slice(i, 1)).collect();
    assert_eq!(p.c.send(out(FrameKind::Request, 3, b"", segs, None)).err(), unsupported, "bulk 超过 16 段");
    assert_eq!(p.c.send(out(FrameKind::StreamChunk, 4, b"", vec![], None)).err(), unsupported, "流不走 rdma");
    assert_eq!(p.c.send(out(FrameKind::OneWay, 0, b"", vec![], None)).err(), unsupported, "OneWay 不走 rdma");
    p.c.send(req(5)).unwrap();
    assert_eq!(p.s.recv().unwrap().hdr.req_id, 5, "以上都不断开连接");
}

/// 在途 16 个时第 17 个请求一直等信用——超过写超时也不放弃（"对端回得慢"不是"对端不读"，tcp 也不断开）。
#[test]
fn rdma_conn_credit_control_blocks_the_17th_request() {
    let p = Arc::new(authed(pair()));
    for i in 1..=14 {
        p.c.send(req(i)).unwrap();
    }
    p.c.send(req(0)).unwrap(); // 两个 req_id = 0 的保活：信用按帧计，各占各的
    p.c.send(req(0)).unwrap();
    let (tx, rx) = mpsc::channel();
    let p2 = p.clone();
    std::thread::spawn(move || tx.send(p2.c.send(req(17))).unwrap());
    let longer = Duration::from_millis(WRITE_TIMEOUT_MS as u64 + 500);
    assert!(rx.recv_timeout(longer).is_err(), "在途 16 个：第 17 个在等信用，过了写超时也还在等");
    let f = p.s.recv().unwrap();
    p.s.send(out(FrameKind::Response, f.hdr.req_id, b"", vec![], None)).unwrap();
    assert_eq!(p.c.recv().unwrap().hdr.req_id, 1, "客户端收到回复 → 还回一个信用");
    assert_eq!(rx.recv_timeout(T).unwrap(), Ok(()), "放行");
    assert_eq!(p.nic.n(&p.nic.0.rnr), 0, "从没触发 RNR");
}

#[test]
fn rdma_conn_close_wakes_a_sender_waiting_for_credit() {
    let p = Arc::new(authed(pair()));
    for i in 1..=16 {
        p.c.send(req(i)).unwrap();
    }
    let (tx, rx) = mpsc::channel();
    let p2 = p.clone();
    std::thread::spawn(move || tx.send(p2.c.send(req(17))).unwrap());
    std::thread::sleep(Duration::from_millis(100));
    p.c.close();
    assert_eq!(rx.recv_timeout(Duration::from_secs(1)).unwrap(), Err(ErrCode::ECONNRESET));
}

#[test]
fn rdma_conn_reply_bulk_needs_a_big_enough_reply_descriptor() {
    let p = authed(pair());
    let unsupported = Some(ErrCode::FEATURE_UNSUPPORTED);
    p.c.send(req(1)).unwrap(); // 没给 recv_into
    p.s.recv().unwrap();
    let b = filled(&p.sp, 1000, 5);
    assert_eq!(p.s.send(out(FrameKind::Response, 1, b"", vec![b.whole()], None)).err(), unsupported);
    p.s.send(out(FrameKind::Response, 1, b"err", vec![], None)).unwrap(); // core 改发的 ERROR 帧（这里用不带 bulk 的回复代替）
    assert_eq!(p.c.recv().unwrap().body, b"err");
    let small = p.cp.get(1000, Lane::Normal, Z).unwrap(); // S 档：容量 128 KiB
    p.c.send(out(FrameKind::Request, 2, b"", vec![], Some(small))).unwrap();
    p.s.recv().unwrap();
    let big = filled(&p.sp, 1 << 20, 6);
    assert_eq!(p.s.send(out(FrameKind::Response, 2, b"", vec![big.whole()], None)).err(), unsupported, "不越界 WRITE");
    assert_eq!(p.nic.n(&p.nic.0.writes), 0, "一次 WRITE 都没有");
}

/// 服务端（accept 侧）的 QP 不开放远程读写，所以它发的请求不能带描述符；不带的照常发。
#[test]
fn rdma_conn_accept_side_never_sends_descriptors() {
    let p = authed(pair());
    let d = filled(&p.sp, 1000, 3);
    let r = p.s.send(out(FrameKind::Request, 1, b"", vec![d.whole()], None));
    assert_eq!(r.err(), Some(ErrCode::FEATURE_UNSUPPORTED), "带 bulk");
    let into = p.sp.get(1000, Lane::Normal, Z).unwrap();
    let r = p.s.send(out(FrameKind::Request, 2, b"", vec![], Some(into)));
    assert_eq!(r.err(), Some(ErrCode::FEATURE_UNSUPPORTED), "带 recv_into");
    p.s.send(req(3)).unwrap();
    assert_eq!(p.c.recv().unwrap().hdr.req_id, 3, "不断开");
}

#[test]
fn rdma_conn_full_pool_drops_bulk_without_reading() {
    let p = authed(pair());
    let hog: Vec<Buf> = (0..2).map(|_| p.sp.get(1 << 20, Lane::Normal, Z).unwrap()).collect(); // 占住服务端的 M 档
    let data = filled(&p.cp, 1 << 20, 4);
    p.c.send(out(FrameKind::Request, 1, b"a", vec![data.whole()], None)).unwrap();
    let f = p.s.recv().unwrap();
    assert!(f.bulk_dropped && f.bulk.is_none(), "100 ms 取不到缓冲：交出 bulk_dropped（core 回 BUSY）");
    assert_eq!(p.nic.n(&p.nic.0.reads), 0, "没有发 READ");
    let data = match data.try_unfreeze() {
        Err(d) => d,
        Ok(_) => panic!("回复到达之前在途表还持有 BufSlice"),
    };
    p.s.send(out(FrameKind::Response, 1, b"busy", vec![], None)).unwrap();
    p.c.recv().unwrap();
    assert!(data.try_unfreeze().is_ok(), "对端回复之后才放下 BufSlice");
    drop(hog);
}

#[test]
fn rdma_conn_bootstrap_eof_invalidates_the_connection() {
    let p = authed(pair());
    let t0 = Instant::now();
    p.s.close(); // 引导 TCP 被关掉
    assert_eq!(p.c.recv().err(), Some(ErrCode::ECONNRESET));
    assert!(t0.elapsed() < Duration::from_secs(3), "最多 1 s 多一点就发现");
}

#[test]
fn rdma_conn_recv_slots_come_from_the_normal_lane() {
    let sp = pool(true);
    let before = sp.stats()[0];
    let p = pair_with(sp, pool(false));
    let after = p.sp.stats()[0];
    assert_eq!(after.free_repl, before.free_repl, "复制 lane 的保底一个没动");
    assert_eq!(before.free_normal - after.free_normal, 17, "16 个接收槽 + 1 个发送暂存");
}

#[test]
fn rdma_setup_silent_peer_does_not_block_and_max_qps_is_enforced() {
    let (nic, sp, cp) = (FakeNic::new(), pool(false), pool(false));
    let (server, addr, rx) = listening(&nic, &sp, 2);
    let client = transport(&nic, false, 4);
    client.register_region(&cp.region()).unwrap();
    let silent = TcpStream::connect(&addr).unwrap(); // 连上来却不说话
    let t0 = Instant::now();
    let _c1 = client.connect(&addr, cp.clone(), T).unwrap();
    assert!(t0.elapsed() < Duration::from_secs(2), "不被不说话的引导连接卡住");
    let _s1 = rx.recv_timeout(T).unwrap();
    assert!(client.connect(&addr, cp.clone(), T).is_err(), "max_qps = 2（含建连中的）：第三个引导连接被直接关掉");
    let t1 = Instant::now();
    while server.shared.live.count() != 1 && t1.elapsed() < Duration::from_secs(15) {
        std::thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(server.shared.live.count(), 1, "不说话的那个 5 s 后被放弃，名额归还");
    assert!(t1.elapsed() >= Duration::from_secs(3), "是交换超时（5 s）放弃的，不是马上");
    let _c2 = client.connect(&addr, cp.clone(), T).unwrap();
    drop(silent);
}
