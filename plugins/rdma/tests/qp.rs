//! QP 状态迁移的属性（qp_attr.rs，附录 C 的 C-10）与收帧的状态机（recv.rs 的 `frame_allowed`）：都是纯函数，不碰设备。
//! QP 属性用 sideway 自带的 getter 读回交给 modify_qp 的值逐项核对——verbs.rs 只是把它们原样交给 modify。

use sideway::ibverbs::AccessFlags;
use sideway::ibverbs::device_context::Mtu;
use sideway::ibverbs::queue_pair::QueuePairState;
use wmfs_core::proto::frame::{FLAG_BULK_REMOTE, FLAG_REPLY_REMOTE, FrameHeader, FrameKind, MSG_PING};

use super::super::params::qp_params;
use super::super::qp_attr;
use super::super::recv::frame_allowed;
use super::super::setup::RdmaSetup;

/// C-10：不能照抄 rc_pingpong 的 max_rd_atomic = 0（RDMA READ 会失败）；rq_psn 取对端的、sq_psn 取本端的；
/// INIT 时两侧都不开放远程访问，只有 connect 侧握手完成后（RTS → RTS）打开远程读写。
#[test]
fn rdma_qp_attributes_follow_c10_and_open_remote_access_only_after_auth() {
    let a = qp_attr::init(1);
    assert_eq!((a.state(), a.port(), a.pkey_index()), (QueuePairState::Init, 1, 0));
    assert!(a.access_flags().is_none(), "INIT 时不开放任何远程访问：{:?}", a.access_flags());
    for accept in [true, false] {
        let p = qp_params(accept);
        let remote = RdmaSetup { qpn: 0x00ab_cdef, psn: 0x12_3456, gid: [7; 16], mtu: 3 };
        let a = qp_attr::rtr(&p, 1, 3, 5, &remote);
        assert_eq!(a.state(), QueuePairState::ReadyToReceive);
        assert_eq!((a.dest_qp_num(), a.rq_psn()), (0x00ab_cdef, 0x12_3456), "rq_psn 取对端的 psn");
        assert_eq!(a.max_dest_read_atomic(), 16, "C-10：作为被读方同时接受 16 个 READ（示例是 0）");
        assert_eq!(a.min_rnr_timer(), 12);
        assert_eq!(a.path_mtu(), Mtu::Mtu1024, "两端 mtu 取较小者（3 = 1024）");
        let a = qp_attr::rts(&p, 0x65_4321);
        assert_eq!((a.state(), a.sq_psn()), (QueuePairState::ReadyToSend, 0x65_4321), "sq_psn 取本端的 psn");
        assert_eq!(a.max_read_atomic(), 16, "C-10：作为发起方同时发出 16 个 READ（示例是 0）");
        assert_eq!((a.timeout(), a.retry_cnt(), a.rnr_retry()), (14, 7, 7));
    }
    assert!(!qp_params(true).open_remote_after_auth, "accept 侧（服务端）永远不开放：不可信的客户端碰不到服务端内存");
    assert!(qp_params(false).open_remote_after_auth, "connect 侧握手完成后打开（对端要 READ/WRITE 它的缓冲）");
    let a = qp_attr::allow_remote();
    assert_eq!(a.state(), QueuePairState::ReadyToSend, "RTS → RTS：只改访问位");
    assert_eq!(a.access_flags(), AccessFlags::RemoteRead | AccessFlags::RemoteWrite);
    assert_eq!(qp_attr::error().state(), QueuePairState::Error);
}

fn hdr(kind: FrameKind, flags: u16, bulk_len: u32) -> FrameHeader {
    let mut h = FrameHeader::new(kind, MSG_PING, 1, 0);
    (h.flags, h.bulk_len) = (flags, bulk_len);
    h
}

/// 收帧的状态机（3.3.2、3.3.3、3.6.4）：false = 断开。参数是 (帧, 已认证, 本端是 accept 侧)。
#[test]
fn rdma_frame_state_machine() {
    use FrameKind::*;
    let (bulk, both) = (FLAG_BULK_REMOTE, FLAG_BULK_REMOTE | FLAG_REPLY_REMOTE);
    // 握手完成之前：不能声明 bulk，也不能带任何一种描述符
    assert!(frame_allowed(&hdr(Request, 0, 0), false, true), "握手帧");
    assert!(!frame_allowed(&hdr(Request, bulk, 100), false, true), "握手之前带 bulk");
    assert!(!frame_allowed(&hdr(Request, FLAG_REPLY_REMOTE, 0), false, true), "握手之前只带 REPLY_REMOTE");
    assert!(!frame_allowed(&hdr(Response, 0, 100), false, false), "握手之前的回复带 bulk");
    // 握手之后：只有 accept 侧收带描述符的请求；回复永远不带描述符
    assert!(frame_allowed(&hdr(Request, both, 100), true, true));
    assert!(!frame_allowed(&hdr(Request, bulk, 100), true, false), "connect 侧（客户端）不收带描述符的请求");
    assert!(!frame_allowed(&hdr(Request, 0, 100), true, false), "connect 侧不收带 bulk 的请求");
    assert!(frame_allowed(&hdr(Request, 0, 0), true, false), "不带 bulk 的请求照收（core 会忽略）");
    assert!(frame_allowed(&hdr(Response, 0, 100), true, false), "回复的 bulk 已被 WRITE 进 recv_into");
    assert!(!frame_allowed(&hdr(Response, FLAG_REPLY_REMOTE, 0), true, false), "回复不带描述符");
    // 流帧与 OneWay 不走 rdma（3.3.3）
    for side in [true, false] {
        assert!(!frame_allowed(&hdr(OneWay, 0, 0), true, side), "OneWay");
        assert!(!frame_allowed(&hdr(StreamChunk, 0, 0), true, side), "流帧");
    }
}
