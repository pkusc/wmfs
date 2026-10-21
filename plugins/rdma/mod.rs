//! rdma 传输插件（design.md 4.1、3.6.4，roadmap M2）：同一种帧跑在 RC QP 上；只有数据族（DataCall、PeerCall）与 Ping
//! 会用它（3.3.3），握手（3.3.2）由 core 在它交出的 `Conn` 上照常进行，与 tcp 完全相同。
//!
//! - 控制帧（帧头 + body ≤ 120 KiB）SEND 到对端预先 post 的接收槽（每连接 16 个 S 档槽位）；在途请求达到 16 个时 `send`
//!   阻塞写线程，直到有回复还回信用或连接关闭（信用控制，credit.rs），所以永不触发 RNR。
//! - bulk 用单边操作，**永远由接收请求的一方（accept 侧）发起**：请求带描述符表，accept 侧按顺序 RDMA READ 进自己的一个
//!   池缓冲；期望回复带 bulk 时请求附 `recv_into` 的描述符，accept 侧 RDMA WRITE 进去再 SEND 回复（conn.rs、recv.rs）。
//!   accept 侧的 QP 不开放远程读写，connect 侧握手完成之后才打开——不可信的客户端碰不到服务端内存由网卡保证。
//! - 建连在插件自己的引导 TCP 端口上交换 QP 信息（setup.rs）；设备、GID 按引导连接的本端 IPv4 自动选（gid.rs）。
//! - 设备推迟到第一次需要时才打开（verbs.rs、device.rs）：`[plugin.rdma] listen` 默认为空（不监听），`register_region`
//!   只记下区域，所以没有 RDMA 设备、没装 libibverbs 的机器上只用 tcp 照常工作。
//! - 调用 sideway 的只有三个文件：verbs.rs（QP 上的操作）、device.rs（打开设备、注册内存、轮询完成队列）、qp_attr.rs（QP 状态
//!   迁移的属性：纯函数，有单元测试）。非测试代码里的 `unsafe` 只在 verbs.rs 与 device.rs（每处写了 `// SAFETY:`）；测试用的
//!   假网卡 tests/fake.rs 另有一处内存拷贝。
//! - 子模块都是 rdma 私有的：里面写成 `pub` 的东西只在 rdma 插件内部可见（对外只有这个文件里的 `pub` 项）。
//!
//! 指标 `wmfs_rdma_ops_total{op=send|recv|read|write}`（构造时就注册，值从 0 起）；fail point `plugin.rdma.transport` 由
//! core 包在写线程的 `send` 外层（触发 = 连接作废）；收帧计时由这里做（3.7.4）。
//! 本机没有可用 RDMA 链路，**未在本机验证**（roadmap M2）；和设备无关的逻辑由 tests/ 里的"假网卡"测试。

use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use wmfs_core::api::metrics::{self, Counter};
use wmfs_core::base::config::PluginTable;
use wmfs_core::base::{ErrCode, log};
use wmfs_core::buf::{MemPool, RegionInfo};
use wmfs_core::hooks::manifest::{CORE_ABI_VERSION, Hook, Manifest};
use wmfs_core::hooks::pipeline::HookCall;
use wmfs_core::hooks::traits::{Conn, Transport};

mod conn;
mod credit;
mod desc;
mod device;
mod gid;
mod params;
mod qp_attr;
mod recv;
mod setup;
mod verbs;
mod wire;

/// 本机全部 RoCE v2 的 IPv4 映射 GID（只读 sysfs，不碰设备；与插件建连时选 GID 的规则相同）：testkit 用它判断本机有没有
/// 可用的 RDMA 地址。`wmfs doctor` 另有一份只用 std::fs 的副本（cli/src/doctor/rdma.rs 文件头写了原因）。
pub use gid::{Route, SYSFS, candidates};

use params::{RdmaCfg, check_recv_slots, parse_addr};
use wire::{LiveQps, Op, Provider};

pub static MANIFEST: Manifest = Manifest {
    name: "rdma",
    version: "0.1.0",
    hooks: &[Hook::Transport],
    altitude: 0,
    hot_path: true,
    config_schema: r#"listen = { type = "addr", default = "", doc = "RDMA 引导 TCP 的监听地址（IPv4:端口）；空 = 不监听" }
device = { type = "string", default = "", doc = "RDMA 设备名；空 = 按本端 IPv4 自动选" }
gid_index = { type = "int", default = -1, doc = "GID 表的下标；-1 = 自动选 RoCE v2 的 IPv4 映射 GID" }
max_qps = { type = "int", default = 64, doc = "最多同时几条 RDMA 连接（每条占 16 个 S 档接收槽）" }"#,
    abi_version: CORE_ABI_VERSION,
    attrs: &[],
    budget_ns: 100_000_000, // 100 ms：一次 send，或收到帧之后读完 body + bulk（3.7.4）
    fail_points: &["plugin.rdma.transport"],
};

/// 取锁；持锁的线程 panic 过也照样用（这里的锁只保护简单的表、计数与缓冲，不会留下改了一半的状态）。
pub(crate) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// 各种工作请求完成的次数（`wmfs_rdma_ops_total{op}`，design.md 7.3）。
pub(crate) struct Stats {
    ops: [Arc<Counter>; 4], // send、recv、read、write（下标见 `add`）
}

impl Stats {
    fn new() -> Stats {
        let c = |op: &'static str| metrics::counter("wmfs_rdma_ops_total", "rdma 完成的工作请求数", &[("op", op)]);
        Stats { ops: [c("send"), c("recv"), c("read"), c("write")] }
    }
    /// `op` 这种工作请求又完成了 `n` 个。
    pub(crate) fn add(&self, op: Op, n: u64) {
        let i = match op {
            Op::Send => 0,
            Op::Recv => 1,
            Op::Read => 2,
            Op::Write => 3,
        };
        self.ops[i].add(n);
    }
}

/// 一个 RdmaTransport 的各条连接共用的东西。
pub(crate) struct Shared {
    pub cfg: RdmaCfg,
    pub write_timeout_ms: u64, // 等一次 SEND（连同它前面的 WRITE）完成的上限
    pub provider: Arc<dyn Provider>,
    pub live: LiveQps,
    pub stats: Stats,
    pub recv_hook: HookCall, // 收帧一侧的计时（发帧一侧由 core 包）
}

#[derive(Clone)]
pub struct RdmaTransport {
    shared: Arc<Shared>,
}

impl RdmaTransport {
    /// `cfg` = node.toml 的 `[plugin.rdma]`（键已由 PluginSet::validate 按 config_schema 检查）；
    /// `write_timeout_ms` = `[net] write_timeout_ms`（等发送完成的上限；等信用不设上限，见 credit.rs）。不碰设备。
    pub fn new(cfg: &PluginTable, write_timeout_ms: u32) -> Result<RdmaTransport, String> {
        let c = RdmaCfg::parse(cfg)?;
        let p: Arc<dyn Provider> = Arc::new(verbs::Verbs::new(&c, std::path::Path::new(gid::SYSFS)));
        RdmaTransport::with_provider(c, write_timeout_ms, p)
    }

    pub(crate) fn with_provider(
        c: RdmaCfg,
        write_timeout_ms: u32,
        p: Arc<dyn Provider>,
    ) -> Result<RdmaTransport, String> {
        if write_timeout_ms == 0 {
            return Err("[net] write_timeout_ms 必须大于 0".into());
        }
        let shared = Shared {
            cfg: c,
            write_timeout_ms: write_timeout_ms as u64,
            provider: p,
            live: LiveQps::default(),
            stats: Stats::new(),
            recv_hook: HookCall::new(&MANIFEST, Hook::Transport),
        };
        Ok(RdmaTransport { shared: Arc::new(shared) })
    }
}

impl Transport for RdmaTransport {
    fn manifest(&self) -> &'static Manifest {
        &MANIFEST
    }
    fn scheme(&self) -> &'static str {
        "rdma"
    }
    /// 只记下区域（幂等）；设备打开时才对整块注册（4.1 实现注记（M2））。
    fn register_region(&self, region: &RegionInfo) -> Result<(), ErrCode> {
        self.shared.provider.register(*region)
    }
    /// `[plugin.rdma] listen` 为空（默认）→ 不监听、不碰设备。否则先做接收槽启动校验（3.6.1），监听具体地址时现在就打开
    /// 设备（配置错了启动时就报出来；原因写进日志，core 的报错里有"详情见日志"），再起 `accept-rdma` 线程。
    fn listen(
        &self,
        pool: MemPool,
        on_conn: Arc<dyn Fn(Box<dyn Conn>) + Send + Sync>,
    ) -> Result<Option<String>, ErrCode> {
        let Some(addr) = self.shared.cfg.listen else { return Ok(None) };
        let s = pool.stats().into_iter().find(|c| c.name == "S").map_or(0, |c| c.slots as u64 * c.slot_size as u64);
        let ip = *addr.ip();
        let ok = check_recv_slots(self.shared.cfg.max_qps, s)
            .and_then(|()| if ip.is_unspecified() { Ok(()) } else { self.shared.provider.prepare(ip) });
        if let Err(m) = ok {
            log::error("rdma", m);
            return Err(ErrCode::EINVAL);
        }
        let l = std::net::TcpListener::bind(addr).map_err(|e| {
            log::error("rdma", format_args!("监听 RDMA 引导端口 {addr} 失败：{e}"));
            ErrCode::from(e)
        })?;
        let bound = l.local_addr()?.to_string();
        setup::serve(self.shared.clone(), l, pool, on_conn)?;
        Ok(Some(bound))
    }
    /// `addr` = 对端的引导地址（IPv4:端口；写错 → `EINVAL`）。只建立"能收发帧"的通道，握手由 core 做。
    fn connect(&self, addr: &str, pool: MemPool, timeout: Duration) -> Result<Box<dyn Conn>, ErrCode> {
        let to = parse_addr(addr)?;
        Ok(Box::new(setup::dial(&self.shared, to, pool, timeout)?))
    }
}

#[cfg(test)]
mod tests;
