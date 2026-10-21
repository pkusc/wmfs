//! 测试用的"假网卡"：在进程内用内存拷贝模拟 RC QP 的 SEND/RECV/READ/WRITE（同步执行：post 时就把完成事件送出去）。
//! 它也检查真网卡会检查的东西：本端缓冲必须在已注册的内存里；READ/WRITE 的远端地址必须在注册过的内存里、rkey 对，
//! 而且**对端 QP 开放了对应的远程访问位**（accept 侧从不开放，connect 侧握手完成后才开放）；对端没有 post 接收槽时
//! SEND 失败并计一次 RNR（信用控制保证不会发生）。QP 被销毁时调用测试给的钩子（检查"拆除之前缓冲不回池"）。
//!
//! 另有两个只给测试用的开关：`hold_one_sided` = READ/WRITE 的完成事件迟迟不报（数据照样拷，模拟"网卡还没做完"，测墓地）；
//! [`FakeNic::inject`] = 把任意字节直接放进某个 QP 的接收槽（模拟一个不按规矩来的对端）。

use std::collections::{HashMap, VecDeque};
use std::net::Ipv4Addr;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use wmfs_core::base::ErrCode;
use wmfs_core::buf::RegionInfo;

use super::super::desc::Desc;
use super::super::gid::NoRoute;
use super::super::lock;
use super::super::params::qp_params;
use super::super::setup::RdmaSetup;
use super::super::wire::{Ev, Events, Op, Provider, Sinks, Wire, channels};

type Hook = Box<dyn Fn() + Send>;

#[derive(Default)]
pub struct Nic {
    regions: Mutex<Vec<RegionInfo>>,
    qps: Mutex<HashMap<u32, Weak<Qp>>>,
    next_qpn: AtomicU32,
    pub reads: AtomicU64,
    pub writes: AtomicU64,
    pub sends: AtomicU64,
    pub rnr: AtomicU64,        // SEND 时对端没有接收槽
    pub violations: AtomicU64, // READ/WRITE 越权（没注册、rkey 不对、对端没开放远程访问）
    pub qps_made: AtomicU64,
    pub hold_one_sided: AtomicBool, // READ/WRITE 的完成事件不报（数据照样拷）
    pub on_destroy: Mutex<Option<Hook>>,
}

/// 假网卡（Clone 只增引用计数）。
#[derive(Clone, Default)]
pub struct FakeNic(pub Arc<Nic>);

pub struct Qp {
    qpn: u32,
    nic: Arc<Nic>,
    sinks: Sinks,
    st: Mutex<QpSt>,
}

#[derive(Default)]
struct QpSt {
    remote: Option<u32>,
    posted: VecDeque<(u32, u64, u32)>, // (槽号, 地址, 长度)
    error: bool,
    open_after_auth: bool, // qp_params 说握手完成后打开远程读写（只有 connect 侧）
    remote_access: bool,   // 现在开放了远程读写
}

/// `Box<dyn Wire>` 的那一个：Drop = 销毁 QP。
struct FakeQp(Arc<Qp>);

impl Drop for FakeQp {
    fn drop(&mut self) {
        lock(&self.0.nic.qps).remove(&self.0.qpn);
        if let Some(h) = lock(&self.0.nic.on_destroy).as_ref() {
            h();
        }
    }
}

impl FakeNic {
    pub fn new() -> FakeNic {
        FakeNic::default()
    }
    pub fn n(&self, c: &AtomicU64) -> u64 {
        c.load(Ordering::SeqCst)
    }
    /// 把 `bytes` 直接放进 QP `qpn` 的下一个接收槽并报一个 RECV 完成（qpn 从 1 起按建 QP 的先后编号：conn.rs 的 `pair()`
    /// 里客户端先建，是 1）。没有这个 QP、没有空槽或放不下 → false。
    pub fn inject(&self, qpn: u32, bytes: &[u8]) -> bool {
        let Some(qp) = lock(&self.0.qps).get(&qpn).and_then(Weak::upgrade) else { return false };
        let slot = {
            let mut st = lock(&qp.st);
            match st.posted.front() {
                Some((_, _, cap)) if bytes.len() <= *cap as usize => st.posted.pop_front(),
                _ => None,
            }
        };
        let Some((s, to, _)) = slot else { return false };
        copy(bytes.as_ptr() as u64, to, bytes.len() as u32);
        qp.sinks.deliver(Ev { op: Op::Recv, seq: s, ok: true, len: bytes.len() as u32 });
        true
    }
}

impl Nic {
    /// 覆盖 `addr..addr+len` 的已注册区域的 rkey。
    fn rkey(&self, addr: u64, len: u64) -> Option<u32> {
        let r = lock(&self.regions);
        r.iter().position(|x| addr >= x.base && addr + len <= x.base + x.len).map(|i| 0x1000 + i as u32)
    }
    /// 远端描述符是否合法：在 rkey 指的那块区域里。
    fn remote_ok(&self, d: &Desc) -> bool {
        let r = lock(&self.regions);
        let i = d.rkey.wrapping_sub(0x1000) as usize;
        r.get(i).is_some_and(|x| d.addr >= x.base && d.addr + d.len as u64 <= x.base + x.len)
    }
}

/// 拷贝 `len` 字节（两段地址都已核对过：在注册过的内存池里，或是调用方自己的切片）。
fn copy(src: u64, dst: u64, len: u32) {
    // SAFETY: 调用前已核对两段地址都在注册过的内存池里（inject 的来源是调用方传进来的切片），测试里的池比连接活得久；
    // 用 ptr::copy（允许重叠）。
    unsafe { std::ptr::copy(src as *const u8, dst as *mut u8, len as usize) }
}

impl Provider for FakeNic {
    fn register(&self, region: RegionInfo) -> Result<(), ErrCode> {
        let mut r = lock(&self.0.regions);
        if !r.contains(&region) {
            r.push(region);
        }
        Ok(())
    }
    fn route(&self, local: Ipv4Addr) -> Result<(), NoRoute> {
        if local.is_loopback() {
            return Ok(());
        }
        Err(NoRoute { why: format!("假网卡只认 127.0.0.1，不认 {local}"), link_down: false })
    }
    fn prepare(&self, local: Ipv4Addr) -> Result<(), String> {
        self.route(local).map_err(|e| e.why)
    }
    fn create_qp(&self, local: Ipv4Addr, accept_side: bool) -> Result<(Box<dyn Wire>, Events), String> {
        self.route(local).map_err(|e| e.why)?;
        let qpn = self.0.next_qpn.fetch_add(1, Ordering::SeqCst) + 1;
        let (sinks, events) = channels();
        let st = QpSt { open_after_auth: qp_params(accept_side).open_remote_after_auth, ..QpSt::default() };
        let qp = Arc::new(Qp { qpn, nic: self.0.clone(), sinks, st: Mutex::new(st) });
        lock(&self.0.qps).insert(qpn, Arc::downgrade(&qp));
        self.0.qps_made.fetch_add(1, Ordering::SeqCst);
        Ok((Box::new(FakeQp(qp)), events))
    }
}

impl FakeQp {
    fn peer(&self) -> Option<Arc<Qp>> {
        let remote = lock(&self.0.st).remote?;
        lock(&self.0.nic.qps).get(&remote).and_then(Weak::upgrade)
    }
    fn failed(&self) -> bool {
        lock(&self.0.st).error
    }
    fn set_error(&self) {
        lock(&self.0.st).error = true;
    }
    fn ev(&self, op: Op, seq: u32, ok: bool, len: u32) {
        self.0.sinks.deliver(Ev { op, seq, ok, len });
    }
    /// READ/WRITE 的共同部分：对端在、开放了远程访问、远端描述符合法。
    fn one_sided(&self, op: Op, seq: u32, local: u64, len: u32, remote: &Desc) -> Result<(), ErrCode> {
        if self.0.nic.rkey(local, len as u64).is_none() {
            return Err(ErrCode::EINVAL);
        }
        let ok = !self.failed()
            && self.peer().is_some_and(|p| lock(&p.st).remote_access)
            && self.0.nic.remote_ok(&Desc { len, ..*remote });
        if ok {
            let (src, dst) = if op == Op::Read { (remote.addr, local) } else { (local, remote.addr) };
            copy(src, dst, len);
            let c = if op == Op::Read { &self.0.nic.reads } else { &self.0.nic.writes };
            c.fetch_add(1, Ordering::SeqCst);
        } else if !self.failed() {
            self.0.nic.violations.fetch_add(1, Ordering::SeqCst);
            self.set_error();
        }
        if !self.0.nic.hold_one_sided.load(Ordering::SeqCst) {
            self.ev(op, seq, ok, len);
        }
        Ok(())
    }
}

impl Wire for FakeQp {
    fn local(&self) -> RdmaSetup {
        let mut gid = [0u8; 16];
        gid[10..].copy_from_slice(&[0xff, 0xff, 127, 0, 0, 1]);
        RdmaSetup { qpn: self.0.qpn, psn: 0x12_3456, gid, mtu: 5 }
    }
    fn rkey(&self, addr: u64, len: u64) -> Option<u32> {
        self.0.nic.rkey(addr, len)
    }
    fn post_recv(&self, slot: u32, addr: u64, len: u32) -> Result<(), ErrCode> {
        if self.0.nic.rkey(addr, len as u64).is_none() {
            return Err(ErrCode::EINVAL);
        }
        let mut st = lock(&self.0.st);
        if st.error {
            drop(st);
            self.ev(Op::Recv, slot, false, 0); // 进了 ERR 的 QP：新 post 的接收槽立刻 flush
        } else {
            st.posted.push_back((slot, addr, len));
        }
        Ok(())
    }
    fn post_send(&self, seq: u32, addr: u64, len: u32) -> Result<(), ErrCode> {
        if self.0.nic.rkey(addr, len as u64).is_none() {
            return Err(ErrCode::EINVAL);
        }
        let peer = self.peer().filter(|_| !self.failed());
        let Some(peer) = peer else {
            self.set_error();
            self.ev(Op::Send, seq, false, 0);
            return Ok(());
        };
        let slot = {
            let mut ps = lock(&peer.st);
            if ps.error { None } else { ps.posted.pop_front() }
        };
        match slot {
            Some((s, to, cap)) if len <= cap => {
                copy(addr, to, len);
                self.0.nic.sends.fetch_add(1, Ordering::SeqCst);
                peer.sinks.deliver(Ev { op: Op::Recv, seq: s, ok: true, len });
                self.ev(Op::Send, seq, true, len);
            }
            other => {
                if other.is_none() {
                    self.0.nic.rnr.fetch_add(1, Ordering::SeqCst);
                }
                self.set_error();
                self.ev(Op::Send, seq, false, 0);
            }
        }
        Ok(())
    }
    fn post_read(&self, seq: u32, addr: u64, from: &Desc) -> Result<(), ErrCode> {
        self.one_sided(Op::Read, seq, addr, from.len, from)
    }
    fn post_write(&self, seq: u32, addr: u64, len: u32, to: &Desc) -> Result<(), ErrCode> {
        self.one_sided(Op::Write, seq, addr, len, to)
    }
    fn connect(&self, remote: &RdmaSetup) -> Result<(), ErrCode> {
        lock(&self.0.st).remote = Some(remote.qpn);
        Ok(())
    }
    fn allow_remote(&self) -> Result<(), ErrCode> {
        let mut st = lock(&self.0.st);
        st.remote_access = st.open_after_auth; // accept 侧什么也不做（与 verbs.rs 相同）
        Ok(())
    }
    fn fail(&self) {
        let flushed: Vec<(u32, u64, u32)> = {
            let mut st = lock(&self.0.st);
            st.error = true;
            st.posted.drain(..).collect()
        };
        for (slot, _, _) in flushed {
            self.ev(Op::Recv, slot, false, 0);
        }
    }
}
