//! rdma 插件的常量、部署配置与几条纯计算（design.md 3.6.1、3.6.4、4.1）：接收槽启动校验、完成队列与发送队列的尺寸、
//! 设备能力核对、QP 状态迁移的参数（附录 C 的 C-10）、地址解析。都不碰设备，单元测试直接核对。

use std::net::{SocketAddr, SocketAddrV4, ToSocketAddrs};

use wmfs_core::base::ErrCode;
use wmfs_core::base::config::PluginTable;
use wmfs_core::buf::pool::S_SLOT;

/// 每条连接（每个 QP）的接收槽数（S 档，3.6.1），也是在途请求的信用数（3.6.4）。
pub const RECV_SLOTS: u32 = 16;
/// 一个请求最多带几段 bulk（描述符表 n ≤ 16，3.6.4）。tcp 没有这个限制。
pub const MAX_SEGS: usize = 16;
/// 帧头 + body 的上限 120 KiB（3.3.1、3.6.4）；描述符表另算（最多 1 + 16×16 + 16 = 273 字节），一定放得进 128 KiB 的接收槽。
pub const MAX_FRAME: usize = 120 << 10;
/// 发送队列深度：读线程一个请求最多 16 个 READ，写线程一个回复最多 16 个 WRITE 加 1 个 SEND，留足余量（16 + 2 × 16）。
pub const MAX_SEND_WR: u32 = 16 + 2 * 16;
/// 引导 TCP 上交换 QP 信息的读写超时（一个连上来却不说话的对端最多占用这么久）。
pub const SETUP_TIMEOUT_MS: u64 = 5000;
/// 15 s 收不到任何帧就判对端已死（与 tcp 的读超时相同；客户端空闲 5 s 会发保活 Ping，3.3.3）。
pub const READ_IDLE_MS: u64 = 15_000;
/// 读 bulk 时从池里取缓冲最多等多久（3.6.1）。
pub const BULK_WAIT_MS: u64 = 100;
/// 默认的 max_qps（3.6.1：服务端 64；wmfs dev 写成 32）。
pub const DEFAULT_MAX_QPS: u32 = 64;

/// `[plugin.rdma]`（node.toml；键已由 PluginSet::validate 按 config_schema 检查过，这里检查值）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RdmaCfg {
    pub listen: Option<SocketAddrV4>, // 引导 TCP 的监听地址；None = 不监听（客户端、只用 tcp 的节点）
    pub device: Option<String>,       // None = 按引导连接的本端 IPv4 自动选
    pub gid_index: Option<u8>,        // None = 自动选 RoCE v2 的 IPv4 映射 GID
    pub max_qps: u32,
}

impl RdmaCfg {
    /// 解析 `[plugin.rdma]`；没写的键取默认值（listen 空、device 空、gid_index -1、max_qps 64）。错误是给人看的中文。
    pub fn parse(t: &PluginTable) -> Result<RdmaCfg, String> {
        let text = |k: &str| -> Result<Option<String>, String> {
            match t.get(k) {
                None => Ok(None),
                Some(v) => {
                    let s = v.as_str().ok_or_else(|| format!("[plugin.rdma] {k} 应是字符串"))?;
                    Ok((!s.is_empty()).then(|| s.to_string()))
                }
            }
        };
        let int = |k: &str, default: i64, lo: i64, hi: i64| -> Result<i64, String> {
            let v = t.get(k).map_or(Some(default), |v| v.as_integer()).ok_or(format!("[plugin.rdma] {k} 应是整数"))?;
            if !(lo..=hi).contains(&v) {
                return Err(format!("[plugin.rdma] {k} = {v} 不在 {lo}..={hi} 里"));
            }
            Ok(v)
        };
        let listen = match text("listen")? {
            None => None,
            Some(s) => Some(parse_addr(&s).map_err(|_| {
                format!("[plugin.rdma] listen = {s:?} 应写成 IPv4:端口（RoCE v2 用 IPv4 映射的 GID），例如 \"10.77.0.1:19101\"")
            })?),
        };
        let gid = int("gid_index", -1, -1, 255)?;
        Ok(RdmaCfg {
            listen,
            device: text("device")?,
            gid_index: (gid >= 0).then_some(gid as u8),
            max_qps: int("max_qps", DEFAULT_MAX_QPS as i64, 1, 4096)? as u32,
        })
    }
}

/// 接收槽启动校验（3.6.1，只在监听的一侧做）：`2 × max_qps × 16 × 128 KiB ≤ S 档字节数`。不满足时的消息写明要调到多大。
pub fn check_recv_slots(max_qps: u32, s_bytes: u64) -> Result<(), String> {
    let need = max_qps as u64 * RECV_SLOTS as u64 * S_SLOT as u64;
    if 2 * need <= s_bytes {
        return Ok(());
    }
    Err(format!(
        "rdma：max_qps = {max_qps} 需要 {} MiB 接收槽，超过 S 档（{} MiB）的一半；需要把 S 档调到至少 {} MiB\
         （node.toml [mem] s_mb），或调小 [plugin.rdma] max_qps",
        need >> 20,
        s_bytes >> 20,
        (2 * need).div_ceil(1 << 20)
    ))
}

/// 每设备一个完成队列的大小：每个 QP 最多同时有 16 个接收完成与 MAX_SEND_WR 个发送完成，再留余量
/// （完成队列溢出是致命的：这个设备上的连接会一起断）。
pub fn cq_entries(max_qps: u32) -> u32 {
    max_qps * (RECV_SLOTS + MAX_SEND_WR) + 64
}

/// 设备能力里用得到的几项（query_device）。
#[derive(Clone, Copy, Debug)]
pub struct DevCaps {
    pub max_qp: i32,
    pub max_qp_wr: i32,
    pub max_cqe: i32,
    pub max_qp_rd_atom: i32,      // 作为被读方同时接受几个 READ
    pub max_qp_init_rd_atom: i32, // 作为发起方同时发出几个 READ
}

/// 核对设备能力够不够（不够就带中文原因拒绝，不在运行中才溢出）。
pub fn check_caps(dev: &str, max_qps: u32, c: &DevCaps) -> Result<(), String> {
    let need_cqe = cq_entries(max_qps) as i64;
    let rd = QP_RD_ATOMIC as i32;
    let why = if (c.max_qp as i64) < max_qps as i64 {
        format!("最多 {} 个 QP，[plugin.rdma] max_qps = {max_qps}", c.max_qp)
    } else if (c.max_cqe as i64) < need_cqe {
        format!("完成队列最多 {} 项，max_qps = {max_qps} 需要 {need_cqe} 项（调小 max_qps）", c.max_cqe)
    } else if (c.max_qp_wr as i64) < MAX_SEND_WR as i64 {
        format!("每个队列最多 {} 个工作请求，需要 {MAX_SEND_WR}", c.max_qp_wr)
    } else if c.max_qp_rd_atom < rd || c.max_qp_init_rd_atom < rd {
        format!("每个 QP 只支持 {}/{} 个并发 READ，需要 {rd}", c.max_qp_rd_atom, c.max_qp_init_rd_atom)
    } else {
        return Ok(());
    };
    Err(format!("rdma：设备 {dev} 能力不够：{why}"))
}

/// 一个 QP 同时进行的 READ 数（作为被读方与发起方都一样；描述符表最多 16 段。附录 C 的 C-10：不能照抄示例的 0）。
pub const QP_RD_ATOMIC: u8 = 16;

/// QP 状态迁移的参数（4.1：照 sideway 的 rc_pingpong，但改了 C-10 那几处，并把访问位按两侧分开；属性见 qp_attr.rs）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QpParams {
    pub open_remote_after_auth: bool, // 握手完成后（RTS → RTS）打开远程读写；INIT 时两侧都不开放
    pub max_dest_rd_atomic: u8,       // RTR
    pub max_rd_atomic: u8,            // RTS
    pub min_rnr_timer: u8,            // RTR：12 ≈ 0.64 ms（示例的 0 表示 655 ms）
    pub timeout: u8,                  // RTS：4.096 µs × 2^14 ≈ 67 ms
    pub retry_cnt: u8,                // RTS
    pub rnr_retry: u8,                // RTS：7 = 无限（信用控制保证不触发，只兜底）
    pub hop_limit: u8,                // 地址向量（RoCE v2 走 IP 路由）
}

/// 单边操作永远由接收请求的一方（accept 侧）发起，读写的是 connect 侧的内存（3.6.4）：所以 accept 侧的 QP 从不开放
/// 远程读写（"不可信的客户端碰不到服务端内存"由网卡保证）；connect 侧也只在握手完成之后才打开（4.1 实现注记（M2））。
pub fn qp_params(accept_side: bool) -> QpParams {
    QpParams {
        open_remote_after_auth: !accept_side,
        max_dest_rd_atomic: QP_RD_ATOMIC,
        max_rd_atomic: QP_RD_ATOMIC,
        min_rnr_timer: 12,
        timeout: 14,
        retry_cnt: 7,
        rnr_retry: 7,
        hop_limit: 64,
    }
}

/// `IP:端口`（只认 IPv4：RoCE v2 用 IPv4 映射的 GID）。写错 → `EINVAL`（RpcClient 不重试）。
pub fn parse_addr(s: &str) -> Result<SocketAddrV4, ErrCode> {
    let all = s.to_socket_addrs().map_err(|_| ErrCode::EINVAL)?;
    all.into_iter()
        .find_map(|a| match a {
            SocketAddr::V4(v4) => Some(v4),
            SocketAddr::V6(_) => None,
        })
        .ok_or(ErrCode::EINVAL)
}

/// QP 的起始包序号：随机数（core::base::rand，design.md 0.2 第 7 条）的低 24 位。
pub fn psn(random: [u8; 4]) -> u32 {
    u32::from_le_bytes(random) & 0x00ff_ffff
}
