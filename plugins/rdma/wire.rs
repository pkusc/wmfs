//! rdma 插件内部的两层接口：[`Provider`]（设备：打开、注册内存、建 QP）与 [`Wire`]（一个 RC QP 上的操作）。
//!
//! 真正的实现在 verbs.rs 与 device.rs（sideway）；单元测试用一块"假网卡"（tests/fake.rs：在进程内用内存拷贝模拟
//! SEND/RECV/READ/WRITE），所以和设备无关的逻辑——描述符表、信用控制、在途表、握手前禁止单边操作、拆除顺序、引导连接——
//! 没有设备也能测。
//!
//! 每个工作请求都带信号（signaled）；完成事件 [`Ev`] 按种类送进本连接的三个通道：RECV 给读线程、READ 给读线程
//! （等 READ 的时候不会把下一帧的 RECV 吞掉）、SEND 与 WRITE 给写线程。

use std::net::Ipv4Addr;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex};

use wmfs_core::base::ErrCode;
use wmfs_core::buf::RegionInfo;

use super::desc::Desc;
use super::gid::NoRoute;
use super::lock;
use super::setup::RdmaSetup;

/// 工作请求的种类（也写进 wr_id，完成事件靠它分给读线程或写线程）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op {
    Recv = 1,
    Read = 2,
    Send = 3,
    Write = 4,
}

impl Op {
    pub fn from_u8(v: u8) -> Option<Op> {
        match v {
            1 => Some(Op::Recv),
            2 => Some(Op::Read),
            3 => Some(Op::Send),
            4 => Some(Op::Write),
            _ => None,
        }
    }
}

/// 一个完成事件。`seq`：RECV 是接收槽号，其余是发起方给这一批工作请求编的号；`len`：RECV 收到的字节数。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Ev {
    pub op: Op,
    pub seq: u32,
    pub ok: bool,
    pub len: u32,
}

/// wr_id 的编码：高 32 位是连接号（每条连接一个、永不重复，拆掉之后晚到的完成事件找不到主人，直接丢弃），
/// 接着 8 位种类，低 24 位是 seq。
pub fn wr_id(conn: u32, op: Op, seq: u32) -> u64 {
    ((conn as u64) << 32) | ((op as u64) << 24) | (seq as u64 & 0x00ff_ffff)
}

/// [`wr_id`] 的逆：(连接号, 种类, seq)。
pub fn parse_wr_id(id: u64) -> (u32, Option<Op>, u32) {
    ((id >> 32) as u32, Op::from_u8((id >> 24) as u8), (id & 0x00ff_ffff) as u32)
}

/// 一个 RC QP 上的操作。post 只把工作请求交给网卡、立即返回；结果以 [`Ev`] 送回。所有地址都必须在已注册的内存里
/// （否则 `EINVAL`）。**Drop = 销毁 QP**：之后网卡不再碰这条连接的任何缓冲（conn.rs 靠这一点决定缓冲什么时候能还给池）。
pub trait Wire: Send + Sync {
    /// 本端 QP 的 qpn、psn、gid、mtu（在引导 TCP 上交给对端；不含 rkey）。
    fn local(&self) -> RdmaSetup;
    /// 覆盖 `addr..addr+len` 的那个 MR 的 rkey（写进给对端的描述符）；None = 这段内存没有注册过。
    fn rkey(&self, addr: u64, len: u64) -> Option<u32>;
    /// 把一个接收槽 post 到接收队列；`slot` 原样出现在完成事件的 seq 里。
    fn post_recv(&self, slot: u32, addr: u64, len: u32) -> Result<(), ErrCode>;
    /// SEND 本端 `addr..addr+len` 的字节。
    fn post_send(&self, seq: u32, addr: u64, len: u32) -> Result<(), ErrCode>;
    /// RDMA READ：把对端 `from` 描述的那段读进本端 `addr..addr+from.len`。
    fn post_read(&self, seq: u32, addr: u64, from: &Desc) -> Result<(), ErrCode>;
    /// RDMA WRITE：把本端 `addr..addr+len` 写到对端 `to.addr`（`to.rkey` 是对端给的）。
    fn post_write(&self, seq: u32, addr: u64, len: u32, to: &Desc) -> Result<(), ErrCode>;
    /// INIT → RTR → RTS（属性见 qp_attr.rs）。
    fn connect(&self, remote: &RdmaSetup) -> Result<(), ErrCode>;
    /// connect 侧握手完成之后打开远程读写（RTS → RTS 只改访问位，4.1 实现注记（M2））；accept 侧什么也不做。
    fn allow_remote(&self) -> Result<(), ErrCode>;
    /// QP → ERR：在途的工作请求（含已 post 的接收槽）全部以 flush 结束，等着它们的线程随即拿到出错的完成事件。
    fn fail(&self);
}

/// 一条连接的三个完成事件通道（接收端）。
pub struct Events {
    pub recvs: Receiver<Ev>,
    pub reads: Receiver<Ev>,
    pub sends: Receiver<Ev>,
}

/// 完成事件通道的发送端（放在完成队列轮询线程的路由表里）。
pub struct Sinks {
    recvs: Sender<Ev>,
    reads: Sender<Ev>,
    sends: Sender<Ev>,
}

impl Sinks {
    /// 按种类把完成事件交给读线程或写线程（对方已经不在了就丢掉）。
    pub fn deliver(&self, ev: Ev) {
        let tx = match ev.op {
            Op::Recv => &self.recvs,
            Op::Read => &self.reads,
            Op::Send | Op::Write => &self.sends,
        };
        let _ = tx.send(ev);
    }
}

/// 新建一组完成事件通道。
pub fn channels() -> (Sinks, Events) {
    let (a, recvs) = channel();
    let (b, reads) = channel();
    let (c, sends) = channel();
    (Sinks { recvs: a, reads: b, sends: c }, Events { recvs, reads, sends })
}

/// 设备一侧（verbs.rs 是真的；单元测试里是假网卡）。只有 `prepare` 与 `create_qp` 会打开设备。
pub trait Provider: Send + Sync {
    /// 记下一块要注册的内存（幂等）；设备还没打开时什么也不做，打开时一并注册（4.1 实现注记（M2）：推迟注册）。
    /// 不变量：一个传输在进程的生命周期里只服务同一个内存池（每个进程一个池）——池被丢弃后在同一地址新建的池会沿用
    /// 旧的注册（旧 MR 钉住的是旧的物理页），不支持这种用法。
    fn register(&self, region: RegionInfo) -> Result<(), ErrCode>;
    /// 不碰设备的预检：本机有没有能用于本端地址 `local` 的设备、端口与 GID（只读 sysfs）。
    fn route(&self, local: Ipv4Addr) -> Result<(), NoRoute>;
    /// 现在就打开 `local` 对应的设备（注册内存池、建完成队列、核对能力）：listen 一个具体地址时用，配置错了启动时就报出来。
    /// 错误是给人看的中文。
    fn prepare(&self, local: Ipv4Addr) -> Result<(), String>;
    /// 为一条新连接建 RC QP（已在 INIT 状态，可以 post 接收槽）。`local` 是引导 TCP 连接的本端 IPv4。
    fn create_qp(&self, local: Ipv4Addr, accept_side: bool) -> Result<(Box<dyn Wire>, Events), String>;
}

/// 活着的 QP 数（上限 max_qps：完成队列按它定大小，3.6.1 的接收槽校验也按它算）。建连中的也算。
#[derive(Clone, Default)]
pub struct LiveQps(Arc<Mutex<u32>>);

/// 占着一个 QP 名额；Drop 时归还（放在 RdmaConn 里，排在 QP 之后 Drop）。
pub struct LiveGuard(Arc<Mutex<u32>>);

impl LiveQps {
    /// 还没到 `max` 就占一个名额。
    pub fn try_take(&self, max: u32) -> Option<LiveGuard> {
        let mut n = lock(&self.0);
        if *n >= max {
            return None;
        }
        *n += 1;
        Some(LiveGuard(self.0.clone()))
    }
    pub fn count(&self) -> u32 {
        *lock(&self.0)
    }
}

impl Drop for LiveGuard {
    fn drop(&mut self) {
        *lock(&self.0) -= 1; // 有 LiveGuard 就一定先加过 1，不会减到 0 以下
    }
}
