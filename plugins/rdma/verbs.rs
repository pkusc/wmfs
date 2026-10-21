//! 用 sideway（libibverbs 的封装）实现 wire.rs 的 `Provider` 与 `Wire`（design.md 4.1）：设备在 device.rs，QP 状态迁移的
//! 属性在 qp_attr.rs（纯函数，有单元测试）；这里是"选设备与 GID、建 QP、post 工作请求、改 QP 状态"。`unsafe` 只有
//! `setup_sge` 两处，各写了 `// SAFETY:`。本机没有可用 RDMA 链路，这个文件与 device.rs **未在本机验证**；和设备无关的
//! 逻辑都在别的文件里，由假网卡测试。
//!
//! - 设备推迟到第一次需要时才打开（listen 一个具体地址，或第一次 connect），打开过的设备记在 `devs` 里。
//! - 每连接一个 RC QP（发送队列 48、接收队列 16、各 1 个 SGE），建好就进 INIT；访问位按 `qp_params`（accept 侧不开放
//!   远程读写，connect 侧握手完成后 RTS → RTS 再打开）。

use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use sideway::ibverbs::queue_pair::{
    BasicQueuePair, PostSendGuard, QueuePair, QueuePairAttribute, SetScatterGatherEntry, WorkRequestFlags,
};
use wmfs_core::base::{ErrCode, log, rand};
use wmfs_core::buf::RegionInfo;

use super::desc::Desc;
use super::device::{self, Dev, Routes, why};
use super::gid::{NoRoute, Route, pick};
use super::lock;
use super::params::{MAX_SEND_WR, QpParams, RECV_SLOTS, RdmaCfg, psn, qp_params};
use super::qp_attr;
use super::setup::RdmaSetup;
use super::wire::{Events, Op, Provider, Wire, channels, wr_id};

/// 连接号（写进 wr_id 的高 32 位）：每条连接一个，永不重复。
static NEXT_CONN: AtomicU32 = AtomicU32::new(1);

pub struct Verbs {
    root: PathBuf, // sysfs（选设备与 GID，gid.rs）
    device: Option<String>,
    gid_index: Option<u8>,
    max_qps: u32,
    regions: Mutex<Vec<RegionInfo>>,
    devs: Mutex<HashMap<String, Arc<Dev>>>, // 打开过的设备
}

impl Verbs {
    pub fn new(c: &RdmaCfg, root: &Path) -> Verbs {
        Verbs {
            root: root.to_path_buf(),
            device: c.device.clone(),
            gid_index: c.gid_index,
            max_qps: c.max_qps,
            regions: Mutex::new(Vec::new()),
            devs: Mutex::new(HashMap::new()),
        }
    }

    /// 本端地址 `ip` 用哪块设备、哪个端口、哪个 GID（gid.rs；配置里的 device / gid_index 只在符合的项里找）。
    fn route_for(&self, ip: Ipv4Addr) -> Result<Route, NoRoute> {
        pick(&self.root, ip, self.device.as_deref(), self.gid_index)
    }

    /// 取（必要时打开）设备 `name`。
    fn open(&self, name: &str) -> Result<Arc<Dev>, String> {
        let mut devs = lock(&self.devs);
        if let Some(d) = devs.get(name) {
            return Ok(d.clone());
        }
        // 先拷一份再打开：之后登记的区域由 register 在设备进了 devs 之后补注册（它要等 devs 这把锁）
        let regions = lock(&self.regions).clone();
        let d = device::open(name, self.max_qps, &regions)?;
        devs.insert(name.to_string(), d.clone());
        Ok(d)
    }
}

impl Provider for Verbs {
    fn register(&self, r: RegionInfo) -> Result<(), ErrCode> {
        {
            let mut regs = lock(&self.regions);
            if regs.contains(&r) {
                return Ok(());
            }
            regs.push(r);
        }
        for d in lock(&self.devs).values() {
            d.reg(r).map_err(|m| {
                log::error("rdma", m);
                ErrCode::ENOMEM
            })?;
        }
        Ok(())
    }

    fn route(&self, local: Ipv4Addr) -> Result<(), NoRoute> {
        self.route_for(local).map(|_| ())
    }

    fn prepare(&self, local: Ipv4Addr) -> Result<(), String> {
        let route = self.route_for(local).map_err(|e| e.why)?;
        self.open(&route.device).map(|_| ())
    }

    fn create_qp(&self, local: Ipv4Addr, accept_side: bool) -> Result<(Box<dyn Wire>, Events), String> {
        let route = self.route_for(local).map_err(|e| e.why)?;
        let dev = self.open(&route.device)?;
        let mut qp = dev
            .pd
            .create_qp_builder()
            .setup_send_cq(dev.cq.clone())
            .setup_recv_cq(dev.cq.clone())
            .setup_max_send_wr(MAX_SEND_WR)
            .setup_max_recv_wr(RECV_SLOTS)
            .setup_max_send_sge(1)
            .setup_max_recv_sge(1)
            .build()
            .map_err(|e| format!("rdma：在 {} 上建 QP 失败：{}", dev.name, why(&e)))?;
        qp.modify(&qp_attr::init(route.port)).map_err(|e| format!("rdma：QP 进 INIT 失败：{}", why(&e)))?;
        let port = dev
            .ctx
            .query_port(route.port)
            .map_err(|e| format!("rdma：查询 {}/{} 失败：{}", dev.name, route.port, why(&e)))?;
        let mtu = port.active_mtu() as u32 as u8;
        let mut r = [0u8; 4];
        rand::fill(&mut r);
        let id = NEXT_CONN.fetch_add(1, Ordering::Relaxed);
        let (sinks, events) = channels();
        lock(&dev.routes).insert(id, sinks);
        let unroute = Unroute { routes: dev.routes.clone(), id };
        let params = qp_params(accept_side);
        let q = VerbsQp { qp: Mutex::new(qp), _unroute: unroute, dev, id, route, mtu, psn: psn(r), params };
        Ok((Box::new(q), events))
    }
}

/// Drop 时把连接号从路由表里摘掉（排在 QP 之后：QP 先销毁）。
struct Unroute {
    routes: Routes,
    id: u32,
}

impl Drop for Unroute {
    fn drop(&mut self) {
        lock(&self.routes).remove(&self.id);
    }
}

struct VerbsQp {
    qp: Mutex<BasicQueuePair>, // 第一个字段：Drop 时先销毁 QP（ibv_destroy_qp），之后网卡不再碰这条连接的缓冲
    _unroute: Unroute,
    dev: Arc<Dev>,
    id: u32,
    route: Route,
    mtu: u8,
    psn: u32,
    params: QpParams,
}

/// 发送队列上的一个工作请求：SEND，或对 `Desc` 描述的对端内存做 READ / WRITE。
#[derive(Clone, Copy)]
enum Kind<'a> {
    Send,
    Read(&'a Desc),
    Write(&'a Desc),
}

fn post_err(what: &str, e: &dyn std::error::Error) -> ErrCode {
    log::warn("rdma", format_args!("post {what} 失败：{}", why(e)));
    ErrCode::ECONNRESET
}

impl VerbsQp {
    fn lkey(&self, addr: u64, len: u64) -> Result<u32, ErrCode> {
        self.dev.mr(addr, len).map(|m| m.lkey()).ok_or(ErrCode::EINVAL)
    }

    /// SEND / READ / WRITE（都带信号）。本端缓冲是 `addr..addr+len`：SEND 与 WRITE 从这里读，READ 写进这里。
    fn post(&self, kind: Kind, seq: u32, addr: u64, len: u32) -> Result<(), ErrCode> {
        let lkey = self.lkey(addr, len as u64)?;
        let op = match kind {
            Kind::Send => Op::Send,
            Kind::Read(_) => Op::Read,
            Kind::Write(_) => Op::Write,
        };
        let mut qp = lock(&self.qp);
        let mut g = qp.start_post_send();
        let wr = g.construct_wr(wr_id(self.id, op, seq), WorkRequestFlags::Signaled);
        let h = match kind {
            Kind::Send => wr.setup_send(),
            Kind::Read(d) => wr.setup_read(d.rkey, d.addr),
            Kind::Write(d) => wr.setup_write(d.rkey, d.addr),
        };
        // SAFETY: [addr, addr+len) 在已注册的 MR 里（上面按地址查到了 lkey）；这段缓冲由 conn.rs 留到这个工作请求完成、
        // 或 QP 销毁之后才回池，所以网卡读写它的时候它一直有效。
        unsafe { h.setup_sge(lkey, addr, len) };
        g.post().map_err(|e| post_err(if op == Op::Send { "SEND" } else { "READ/WRITE" }, &e))
    }

    fn modify(&self, what: &str, a: &QueuePairAttribute) -> Result<(), ErrCode> {
        lock(&self.qp).modify(a).map_err(|e| {
            log::warn("rdma", format_args!("{}/{}：QP {what} 失败：{}", self.dev.name, self.route.port, why(&e)));
            ErrCode::ECONNRESET
        })
    }
}

impl Wire for VerbsQp {
    fn local(&self) -> RdmaSetup {
        RdmaSetup { qpn: lock(&self.qp).qp_number(), psn: self.psn, gid: self.route.gid, mtu: self.mtu }
    }

    fn rkey(&self, addr: u64, len: u64) -> Option<u32> {
        self.dev.mr(addr, len).map(|m| m.rkey())
    }

    fn post_recv(&self, slot: u32, addr: u64, len: u32) -> Result<(), ErrCode> {
        let lkey = self.lkey(addr, len as u64)?;
        let mut qp = lock(&self.qp);
        let mut g = qp.start_post_recv();
        let h = g.construct_wr(wr_id(self.id, Op::Recv, slot));
        // SAFETY: 接收槽在已注册的 MR 里（查到了 lkey），由连接持有到 QP 销毁之后，网卡往里写的时候它一直有效。
        unsafe { h.setup_sge(lkey, addr, len) };
        g.post().map_err(|e| post_err("RECV", &e))
    }

    fn post_send(&self, seq: u32, addr: u64, len: u32) -> Result<(), ErrCode> {
        self.post(Kind::Send, seq, addr, len)
    }

    fn post_read(&self, seq: u32, addr: u64, from: &Desc) -> Result<(), ErrCode> {
        self.post(Kind::Read(from), seq, addr, from.len)
    }

    fn post_write(&self, seq: u32, addr: u64, len: u32, to: &Desc) -> Result<(), ErrCode> {
        self.post(Kind::Write(to), seq, addr, len)
    }

    fn connect(&self, remote: &RdmaSetup) -> Result<(), ErrCode> {
        let p = &self.params;
        self.modify("进 RTR", &qp_attr::rtr(p, self.route.port, self.route.gid_index, self.mtu, remote))?;
        self.modify("进 RTS", &qp_attr::rts(p, self.psn))
    }

    fn allow_remote(&self) -> Result<(), ErrCode> {
        if !self.params.open_remote_after_auth {
            return Ok(()); // accept 侧（服务端）永远不开放
        }
        self.modify("RTS → RTS（打开远程读写）", &qp_attr::allow_remote())
    }

    fn fail(&self) {
        let _ = self.modify("进 ERR", &qp_attr::error());
    }
}

#[cfg(test)]
impl Verbs {
    /// 打开过几块设备（测试用：没设备的路径上必须一直是 0）。
    pub fn opened(&self) -> usize {
        lock(&self.devs).len()
    }
}
