//! QP 状态迁移的属性（design.md 4.1，附录 C 的 C-10）：纯函数，只填 sideway 的 `QueuePairAttribute`、不碰设备，所以单元
//! 测试能用 sideway 自带的 getter 逐项核对交给 modify_qp 的值；verbs.rs 只负责调用 modify。
//!
//! 照 sideway 的 examples/rc_pingpong.rs，但改了这几处：RTR/RTS 的 READ 深度都是 16（C-10：示例是 0，RDMA READ 会失败）；
//! INIT 不开放任何远程访问，只有 connect 侧在握手完成之后用 RTS → RTS 打开 RemoteRead | RemoteWrite（4.1 实现注记（M2）①；
//! QP 的访问位只有 REMOTE_* 有意义，LocalWrite 是 MR 的属性）；rq_psn 取对端的 psn、sq_psn 取本端的（与 libibverbs 的
//! C 版 rc_pingpong 相同；sideway 的示例两端都反过来用，也能通，但两端必须同一种约定）；min_rnr_timer 12、地址向量的
//! hop_limit 64（RoCE v2 走 IP 路由）。

use sideway::ibverbs::AccessFlags;
use sideway::ibverbs::address::{AddressHandleAttribute, Gid};
use sideway::ibverbs::device_context::Mtu;
use sideway::ibverbs::queue_pair::{QueuePairAttribute, QueuePairState};

use super::params::QpParams;
use super::setup::RdmaSetup;

/// RESET → INIT：pkey 0、本端端口，不开放任何远程访问。
pub fn init(port: u8) -> QueuePairAttribute {
    let mut a = QueuePairAttribute::new();
    a.setup_state(QueuePairState::Init).setup_pkey_index(0).setup_port(port).setup_access_flags(AccessFlags::none());
    a
}

/// INIT → RTR：对端的 qpn 与 psn、两端 mtu 的较小者（ibv_mtu 的值，双方都已校验为 1..=5）、READ 深度、地址向量
/// （RoCE v2：GRH 里写本端的 gid_index 与对端的 gid）。
pub fn rtr(p: &QpParams, port: u8, gid_index: u8, local_mtu: u8, remote: &RdmaSetup) -> QueuePairAttribute {
    let mut ah = AddressHandleAttribute::new();
    ah.setup_dest_lid(0).setup_port(port).setup_service_level(0);
    ah.setup_grh_src_gid_index(gid_index).setup_grh_dest_gid(&Gid { raw: remote.gid }).setup_grh_hop_limit(p.hop_limit);
    let mut a = QueuePairAttribute::new();
    a.setup_state(QueuePairState::ReadyToReceive)
        .setup_path_mtu(Mtu::from(local_mtu.min(remote.mtu) as u32))
        .setup_dest_qp_num(remote.qpn)
        .setup_rq_psn(remote.psn)
        .setup_max_dest_read_atomic(p.max_dest_rd_atomic)
        .setup_min_rnr_timer(p.min_rnr_timer)
        .setup_address_vector(&ah);
    a
}

/// RTR → RTS：本端的 psn、超时与重试次数、READ 深度。
pub fn rts(p: &QpParams, local_psn: u32) -> QueuePairAttribute {
    let mut a = QueuePairAttribute::new();
    a.setup_state(QueuePairState::ReadyToSend)
        .setup_sq_psn(local_psn)
        .setup_timeout(p.timeout)
        .setup_retry_cnt(p.retry_cnt)
        .setup_rnr_retry(p.rnr_retry)
        .setup_max_read_atomic(p.max_rd_atomic);
    a
}

/// RTS → RTS：只改访问位，打开远程读写（connect 侧握手完成之后；RTS → RTS 的可选属性里有 IBV_QP_ACCESS_FLAGS）。
pub fn allow_remote() -> QueuePairAttribute {
    let mut a = QueuePairAttribute::new();
    a.setup_state(QueuePairState::ReadyToSend).setup_access_flags(AccessFlags::RemoteRead | AccessFlags::RemoteWrite);
    a
}

/// 任意状态 → ERR：在途的工作请求全部以 flush 结束。
pub fn error() -> QueuePairAttribute {
    let mut a = QueuePairAttribute::new();
    a.setup_state(QueuePairState::Error);
    a
}
