//! 一块打开了的 RDMA 设备（design.md 4.1；verbs.rs 用它）：打开设备、核对能力、PD、完成队列、内存注册、完成队列轮询线程。
//! 和 verbs.rs 一样**未在本机验证**（本机没有可用 RDMA 链路）。
//!
//! - 每设备一个 PD、一个完成队列（大小按 max_qps，params.rs 的 `cq_entries`）、一个轮询线程 `rdma-cq-<设备>`（忙轮询
//!   50 µs 后 sleep 50 µs），按 wr_id 里的连接号把完成事件分给对应的连接（路由表）。
//! - 打开时用 query_device 核对能力（`check_caps`）；已登记的内存区域每块注册一个 MR（LocalWrite | RemoteRead |
//!   RemoteWrite），之后登记的区域在设备已打开时当场注册。
//! - 关闭：传输被丢弃、这块设备上的连接全部拆除之后，轮询线程发现路由表只剩自己还拿着，就退出。sideway 用引用计数保证
//!   释放顺序：QP 与 MR 先没（它们各自拿着 PD、完成队列的引用），完成队列、PD、设备上下文随最后一个引用一起释放。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use sideway::ibverbs::AccessFlags;
use sideway::ibverbs::completion::{BasicCompletionQueue, WorkCompletionStatus};
use sideway::ibverbs::device::{DeviceInfo, DeviceList};
use sideway::ibverbs::device_context::DeviceContext;
use sideway::ibverbs::memory_region::MemoryRegion;
use sideway::ibverbs::protection_domain::ProtectionDomain;
use wmfs_core::base::log;
use wmfs_core::buf::RegionInfo;

use super::lock;
use super::params::{DevCaps, check_caps, cq_entries};
use super::wire::{Ev, Sinks, parse_wr_id};

/// 连接号 → 它的完成事件通道。
pub type Routes = Arc<Mutex<HashMap<u32, Sinks>>>;

pub struct Dev {
    pub name: String,
    pub ctx: Arc<DeviceContext>,
    pub pd: Arc<ProtectionDomain>,
    pub cq: Arc<BasicCompletionQueue>,
    mrs: Mutex<Vec<(RegionInfo, Arc<MemoryRegion>)>>,
    pub routes: Routes,
}

/// 错误的完整原因：sideway 的错误在 Display 里只写一句英文外层说明（例如 "failed to create queue pair"），真正的 errno
/// （EINVAL、ENOMEM、EPERM……）在 `source()` 里——把整条 source 链拼起来，日志里才看得到。
pub fn why(e: &dyn std::error::Error) -> String {
    let mut s = e.to_string();
    let mut cause = e.source();
    while let Some(c) = cause {
        s.push('：');
        s.push_str(&c.to_string());
        cause = c.source();
    }
    s
}

/// 打开设备 `name`、核对能力、建 PD 与完成队列，把 `regions` 每块注册一个 MR，起轮询线程。错误是给人看的中文。
pub fn open(name: &str, max_qps: u32, regions: &[RegionInfo]) -> Result<Arc<Dev>, String> {
    let list = DeviceList::new().map_err(|e| format!("rdma：列不出 RDMA 设备（{}；没装 libibverbs？）", why(&e)))?;
    let dev = list.iter().find(|d| d.name() == name).ok_or_else(|| format!("rdma：libibverbs 里没有设备 {name}"))?;
    let ctx = dev.open().map_err(|e| format!("rdma：打开设备 {name} 失败：{}", why(&e)))?;
    let a = ctx.query_device().map_err(|e| format!("rdma：查询设备 {name} 的能力失败：{}", why(&e)))?;
    let caps = DevCaps {
        max_qp: a.max_qp(),
        max_qp_wr: a.max_qp_wr(),
        max_cqe: a.max_cqe(),
        max_qp_rd_atom: a.max_qp_rd_atom(),
        max_qp_init_rd_atom: a.max_qp_init_rd_atom(),
    };
    check_caps(name, max_qps, &caps)?;
    let pd = ctx.alloc_pd().map_err(|e| format!("rdma：在 {name} 上分配 PD 失败：{}", why(&e)))?;
    let cq = ctx
        .create_cq_builder()
        .setup_cqe(cq_entries(max_qps))
        .build()
        .map_err(|e| format!("rdma：建完成队列失败：{}", why(&e)))?;
    let d = Dev { name: name.to_string(), ctx, pd, cq, mrs: Mutex::new(Vec::new()), routes: Arc::default() };
    for r in regions {
        d.reg(*r)?;
    }
    let (cq, routes) = (d.cq.clone(), d.routes.clone());
    std::thread::Builder::new()
        .name(format!("rdma-cq-{name}"))
        .spawn(move || poll(cq, routes))
        .map_err(|e| format!("rdma：起完成队列轮询线程失败：{e}"))?;
    log::info("rdma", format_args!("已打开 RDMA 设备 {name}"));
    Ok(Arc::new(d))
}

impl Dev {
    /// 对一块内存区域注册一个 MR（同一区域只注册一次）。
    pub fn reg(&self, r: RegionInfo) -> Result<(), String> {
        if lock(&self.mrs).iter().any(|(o, _)| *o == r) {
            return Ok(());
        }
        let acc = AccessFlags::LocalWrite | AccessFlags::RemoteRead | AccessFlags::RemoteWrite;
        // SAFETY: r 是 core 内存池（buf/pool.rs）一次 mmap 出来的整块内存，[base, base+len) 在池活着的时候一直有效，
        // 而交给网卡的每个 Buf 都持有池的引用、并由连接留到 QP 销毁之后（conn.rs）。注册会钉住这些页：池即使先被 munmap，
        // 网卡也只会碰到仍被钉住的旧页，不会碰到别的内存。（不变量：一个传输只服务同一个池，wire.rs 的 Provider::register；
        // 池被丢弃后在同一地址新建的池会沿用这个 MR，网卡读写的就是旧页而不是新池——不支持这种用法。）
        let mr = unsafe { self.pd.reg_mr(r.base as usize, r.len as usize, acc) }.map_err(|e| {
            let mib = r.len >> 20;
            format!(
                "rdma：在 {} 上注册内存池（{mib} MiB）失败：{}（memlock 上限不够？见 wmfs doctor）",
                self.name,
                why(&e)
            )
        })?;
        lock(&self.mrs).push((r, mr));
        Ok(())
    }

    /// 覆盖 `addr..addr+len` 的 MR（从新往旧找）。
    pub fn mr(&self, addr: u64, len: u64) -> Option<Arc<MemoryRegion>> {
        let mrs = lock(&self.mrs);
        let hit = mrs.iter().rev().find(|(r, _)| addr >= r.base && addr.saturating_add(len) <= r.base + r.len);
        hit.map(|(_, m)| m.clone())
    }
}

/// 完成队列轮询线程：有完成事件就一直取；空了先忙轮询 50 µs，再每 50 µs 看一次（1.2 线程表）。路由表只剩这个线程
/// 还拿着（设备与它上面的连接都没了）就退出，完成队列随之释放。
fn poll(cq: Arc<BasicCompletionQueue>, routes: Routes) {
    let (ok, flushed) = (WorkCompletionStatus::Success as u32, WorkCompletionStatus::WorkRequestFlushedError as u32);
    // 50 µs 的忙轮询窗口比 clock::mono_ms 的毫秒精度细，所以这里用 Instant；它不是超时，不需要 fail point 注入时钟。
    let mut last = Instant::now();
    loop {
        let mut got = 0;
        if let Ok(p) = cq.start_poll() {
            for wc in p {
                got += 1;
                let (conn, op, seq) = parse_wr_id(wc.wr_id());
                let st = wc.status();
                if st != ok && st != flushed {
                    log::warn(
                        "rdma",
                        format_args!("连接 {conn} 的工作请求出错（状态 {st}，厂商码 {}）", wc.vendor_err()),
                    );
                }
                let Some(op) = op else { continue };
                // 出错的完成事件里只有 wr_id 与 status 可信：byte_len 只在成功时被读线程用到（recv.rs 先看 ok）
                if let Some(s) = lock(&routes).get(&conn) {
                    s.deliver(Ev { op, seq, ok: st == ok, len: wc.byte_len() }); // 找不到 = 连接已拆：丢掉
                }
            }
        }
        if got > 0 {
            last = Instant::now();
        } else if last.elapsed() < Duration::from_micros(50) {
            std::hint::spin_loop();
        } else if Arc::strong_count(&routes) == 1 {
            return; // Dev 与每条连接的 Unroute 都拿着路由表：只剩这里 = 传输已丢弃、连接全部拆除
        } else {
            std::thread::sleep(Duration::from_micros(50));
        }
    }
}
