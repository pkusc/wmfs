//! 数据面（design.md 3.2.2–3.2.5、3.4.2）。
//!
//! - [`format`]：chunk 文件的固定头与块表（3.2.4，M0）；[`superblock`]：target 超级块（3.2.3，M0）
//! - [`build`]：构建新版本（读改写与变换、头与块表，3.4.2.3 第 3 步）
//! - [`state`]：chunk 状态表（同一 chunk 的写排队、Corrupt 标记，3.4.2.2、3.2.5 R5）
//! - [`write`]：head 上的写与截断（写 pending；有后继就交给 forward.rs 转发，否则本地提交）
//! - [`read`]：打开已提交版本、逐块解码（读路径与读改写共用）、读
//! - [`service`]：DataCall 服务：QoS 入队、io 线程出队后校验 cap、分派（含 SyncChunks、DeleteEpoch）
//! - [`node_hb`]：target 心跳（租约、转发失败上报、SyncDone）与本节点的集群图视图
//! - [`chain`]：本节点眼里的链（前驱、后继、last serving）与 I5 的租约（M9）
//! - [`packed`]：packed 形式与增量转发的应用；[`forward`]：发转发、提交与确认（M9）
//! - [`replica`]：PeerCall 服务与 repl 线程（后继处理转发、确认、同步与删除的传播，M9）
//! - [`redrive`]：链变化时的前滚与 chain-timer（3.4.2.6，M9）；[`sync`]：重同步（3.4.2.8，M9）
//! - [`datakey`]：数据加密密钥的包裹、解包与内存里的缓存（M11）；[`repair`]：校验与修复一个 chunk 副本、上报，写路径要的修复（M11）
//! - [`slots`]：数据服务的排队位置（整个节点与每个 QoS 队列的上限）；[`latency`]：按存储类统计的延迟（M13 收尾，ADR-0044 第 5、6 条）
//!
//! 实现注记（M5）：单节点数据路径——链长 1，head 写完 pending 就本地提交（3.4.2.3 第 5 步"无后继"一支）。实现注记（M9）：多成员链
//! ——转发不占线程（`call_cb` + pending 表，回调只把结果 try_send 进 repl 队列）、逆向确认、前滚、重同步、target 租约与自我隔离（I5）。
//! 本模块里的 [`DataNode`] 是一个数据节点处理请求要用到的全部东西。

pub mod build;
pub mod chain;
pub mod datakey;
pub mod format;
pub mod forward;
pub mod latency;
pub mod node_hb;
pub mod packed;
pub mod read;
pub mod redrive;
pub mod repair;
pub mod replica;
pub mod service;
pub mod slots;
pub mod state;
pub mod superblock;
pub mod sync;
pub mod write;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use crossbeam_channel::Sender;
use wmfs_api::metrics::{self, Counter};

use crate::base::config::TargetConfig;
use crate::base::types::ChunkId;
use crate::base::{ErrCode, devinfo, lock, log};
use crate::buf::{Buf, MemPool};
use crate::hooks::pipeline::Pipeline;
use crate::hooks::traits::{ChunkStore, StoreStat, TargetStore, TransformKind, TransformPlugin};
use crate::net::client::RpcClient;
use chain::Lease;
use node_hb::MapHolder;
use replica::ReplJob;
use state::ChunkTable;
use superblock::TargetSuperblock;

/// 连续多少次本地提交失败就把 target 当作坏盘、从此只读（实现注记（M11），ADR-0040 第 10 条）。
pub const FAILED_COMMITS: u32 = 3;

/// 本节点的一个 target。实现注记（M11）：只读有两个来源——启动时超级块里不认识的 ro_compat 位（`ro_compat`），与运行中连续
/// [`FAILED_COMMITS`] 次本地提交失败（多半是盘坏了：`broken`，[`Target::note_commit`]）；两者都经心跳报 `read_only = true`，链管理器把它
/// 两步移出写路径（ADR-0041 第 13 条）。只读的 target 不写盘：运行中坏了的，写与截断回可重试的 `NOT_SERVING`（客户端拉新集群图、等它
/// 被移出；启动时就只读的回 `EROFS`）；当 head 时 DeleteEpoch 回 `EROFS`（两种都是；gc 下一轮再试，service.rs）；当后继收到的转发、
/// 同步与删除的传播回可重试的 `NOT_SERVING`（前驱稍后重发，replica.rs）；当 Syncing 成员时不同步（sync.rs，`EROFS`）。
pub struct Target {
    pub id: u32,
    pub path: String,
    pub store: Arc<dyn TargetStore>,
    pub ro_compat: bool,                 // 超级块里有不认识的 ro_compat 位（3.2.3）：只读服务
    broken: AtomicBool,                  // 运行中连续提交失败（M11）
    fail_streak: AtomicU32,              // 连续失败的次数
    pub capacity: u64,                   // node.toml 的 `[[target]] capacity`（0 = 不限）
    pub phys_devs: Vec<String>,          // 它所在的物理整盘（`base::devinfo`，建 Target 时解析一次；心跳上报）
    last_stat: Mutex<Option<StoreStat>>, // hb 线程最近一次的容量与已用（M14：`GET /api/v1/node` 读它，管理线程不碰盘）
    read_bytes: Arc<Counter>,
    write_bytes: Arc<Counter>,
    corrupt: Arc<Counter>,
}

impl Target {
    pub fn new(id: u32, path: &str, store: Arc<dyn TargetStore>, ro_compat: bool, capacity: u64) -> Target {
        let t = id.to_string();
        let l = [("target", t.as_str())];
        Target {
            id,
            path: path.to_string(),
            store,
            ro_compat,
            broken: AtomicBool::new(false),
            fail_streak: AtomicU32::new(0),
            capacity,
            phys_devs: devinfo::phys_devs(Path::new(path)),
            last_stat: Mutex::new(None),
            read_bytes: metrics::counter("wmfs_chunk_read_bytes_total", "读出的 chunk 明文字节数", &l),
            write_bytes: metrics::counter("wmfs_chunk_write_bytes_total", "写进 pending 文件的字节数", &l),
            corrupt: metrics::counter("wmfs_chunk_corrupt_total", "发现损坏（头 CRC 或块校验和不符）的次数", &l),
        }
    }

    /// 只读吗（见结构体的注释）。
    pub fn read_only(&self) -> bool {
        self.ro_compat || self.broken()
    }

    /// 运行中坏了（变成只读，不是启动时就只读）吗。
    pub fn broken(&self) -> bool {
        self.broken.load(Ordering::Relaxed)
    }

    /// 记一次本地提交的结果：连续 [`FAILED_COMMITS`] 次失败 → 从此只读（写一行错误日志）。
    pub fn note_commit(&self, ok: bool) {
        if ok {
            self.fail_streak.store(0, Ordering::Relaxed);
        } else if self.fail_streak.fetch_add(1, Ordering::Relaxed) + 1 >= FAILED_COMMITS
            && !self.broken.swap(true, Ordering::Relaxed)
        {
            log::error(
                "data",
                format_args!(
                    "target {} 连续 {FAILED_COMMITS} 次提交失败（盘坏了？）：从此只读，心跳上报，等链管理器把它移出写路径",
                    self.id
                ),
            );
        }
    }

    /// 容量与已用：chunk_store 报的容量（文件系统的大小）与 node.toml 的上限取小（防止测试写满系统盘，4.9）。
    /// 调用方经 [`DataNode::store`] 调它（3.7.4 的包装）；记下来给 [`Target::last_stat`]。
    pub fn stat(&self) -> Result<StoreStat, ErrCode> {
        let st = limit_capacity(self.store.statfs()?, self.capacity);
        *lock(&self.last_stat) = Some(st);
        Ok(st)
    }

    /// 最近一次 [`Target::stat`] 的结果（hb 线程每秒一次；还没有过 → None）：管理 API 的线程用它，不去碰可能挂死的盘（1.2 的线程表）。
    pub fn last_stat(&self) -> Option<StoreStat> {
        *lock(&self.last_stat)
    }
}

/// `st` 的容量与上限 `limit`（0 = 不限）取小。
pub fn limit_capacity(st: StoreStat, limit: u64) -> StoreStat {
    let capacity = if limit > 0 && (st.capacity == 0 || limit < st.capacity) { limit } else { st.capacity };
    StoreStat { capacity, used: st.used }
}

/// 一个数据节点处理请求要用到的全部东西（service 建好，write / read / 心跳用）。
/// 实现注记（M9）：复制要的东西——服务端之间的 RPC 客户端与 repl 队列（`replica::start` 交进来；组件测试里没有它们的节点只当链长 1）、
/// I5 的租约（连同本进程的 boot_id，hb 线程起来时打开；没打开 = 不核对）、正在同步的链在本轮本地提交过转发的 chunk（3.4.2.8 的 touched）、
/// 持续转发不到后继的起点（心跳上报，3.4.2.10）、等 hb 线程报给 leader 的 SyncDone，以及 CRAQ 的计数器。
pub struct DataNode {
    pub pipeline: Arc<Pipeline>,
    pub targets: BTreeMap<u32, Target>,
    pub chunks: ChunkTable,
    pub pool: MemPool,
    pub cluster_id: [u8; 16],
    pub cap_key: [u8; 32],
    pub map: Arc<MapHolder>,
    pub peer: OnceLock<Arc<RpcClient>>,
    pub repl: OnceLock<Sender<ReplJob>>,
    pub lease: Lease,
    pub touched: Mutex<HashMap<(u32, u32), HashSet<ChunkId>>>, // (链, 本节点的 target) → 本轮同步开始后本地提交过转发的 chunk
    pub fwd_fail: Mutex<HashMap<(u32, u32), u64>>,             // (链, 下游 target) → 持续转发失败的起点（墙钟 ms）
    pub sync_done: Mutex<Vec<(u32, u32, u64)>>, // (链, target, chain_ver)：同步完了，等 hb 线程报给 leader
    pub promoted: Mutex<HashMap<u32, (u64, bool)>>, // 链 → 本节点的成员刚成为 last serving：到这个时刻（mono_ms）之前读回 BUSY、整条链吗
    pub craq: Craq,
    pub keys: datakey::DataKeys, // 实现注记（M11）：解包过的数据密钥（只在内存里）
    pub repair_q: OnceLock<Sender<(u32, ChunkId)>>, // 实现注记（M11）：交给 repair 线程修的 (target, chunk)（repair.rs）
    pub latency: latency::ClassLatency,             // 按存储类统计的客户端请求延迟（心跳报 p99，ADR-0044 第 5 条）
}

/// CRAQ 的计数器（7.3 的"数据"一行）。
pub struct Craq {
    pub dirty_reads: Arc<Counter>,
    pub redrive: Arc<Counter>,
    pub need_full: Arc<Counter>,
    pub resync_bytes: Arc<Counter>,
}

impl Craq {
    fn new() -> Craq {
        let c = |n, h| metrics::counter(n, h, &[]);
        Craq {
            dirty_reads: c("wmfs_craq_dirty_reads_total", "有未提交版本、改问 last serving 的读（CHUNK_DIRTY）"),
            redrive: c("wmfs_craq_redrive_total", "重新驱动的 pending 版本（链变化或 3 s 未确认）"),
            need_full: c("wmfs_craq_need_full_total", "后继缺增量转发的基础版本、改发完整 chunk 的次数"),
            resync_bytes: c("wmfs_resync_bytes_total", "重同步时从 last serving 取回的字节数"),
        }
    }
}

impl DataNode {
    pub fn new(
        pipeline: Arc<Pipeline>,
        targets: Vec<Target>,
        pool: MemPool,
        cluster_id: [u8; 16],
        cap_key: [u8; 32],
        map: Arc<MapHolder>,
    ) -> Arc<DataNode> {
        let targets = targets.into_iter().map(|t| (t.id, t)).collect();
        Arc::new(DataNode {
            pipeline,
            targets,
            chunks: ChunkTable::new(),
            pool,
            cluster_id,
            cap_key,
            map,
            peer: OnceLock::new(),
            repl: OnceLock::new(),
            lease: Lease::default(),
            touched: Mutex::default(),
            fwd_fail: Mutex::default(),
            sync_done: Mutex::default(),
            promoted: Mutex::default(),
            craq: Craq::new(),
            keys: datakey::DataKeys::default(),
            repair_q: OnceLock::new(),
            latency: latency::ClassLatency::default(),
        })
    }

    /// 调 chunk_store 插件的一个方法（含读句柄的 `read_at`），经 3.7.4 的包装（fail point `plugin.<名字>.chunk_store`、计时、
    /// 预算）。管线里没有 chunk_store 插件（测试里直接给 TargetStore）时直接调用。
    pub fn store<T>(&self, f: impl FnOnce() -> Result<T, ErrCode>) -> Result<T, ErrCode> {
        match self.pipeline.chunk_store() {
            Some((_, h)) => h.call(f),
            None => f(),
        }
    }

    /// 经 3.7.4 的包装调一个 transform 插件（`kind` 类、算法编号 `alg`；没有这个算法的插件 → `FEATURE_UNSUPPORTED`）。
    pub fn transform(
        &self,
        kind: TransformKind,
        alg: u8,
        f: impl FnOnce(&Arc<dyn TransformPlugin>) -> Result<Buf, ErrCode>,
    ) -> Result<Buf, ErrCode> {
        let (p, h) = self.pipeline.transform_call(kind, alg).ok_or(ErrCode::FEATURE_UNSUPPORTED)?;
        h.call(|| f(p))
    }

    /// 发现损坏：标 Corrupt、计数、写错误日志（3.2.5 R5；单副本修不了，诚实报错）。
    pub fn report_corrupt(&self, t: &Target, id: &ChunkId, why: ErrCode) {
        self.chunks.mark_corrupt(id);
        t.corrupt.inc();
        log::error("data", format_args!("target {} 上的 chunk {id:?} 损坏（{why}）：标为 Corrupt，读一律拒绝", t.id));
    }
}

/// 打开 node.toml 里的每个 target（服务端启动时，3.2.3、3.2.5 R5）：读超级块（cluster_id 不符、不认识的 incompat 位 → 拒绝启动；
/// 不认识的 ro_compat 位 → 只读服务），交给 chunk_store 插件打开，清空 `p/`、`t/`。错误是给人看的中文。
/// 实现注记（M5）：这里的 `open_target`、`clear_temp` 不经 3.7.4 的包装（启动时只调一次；失败就拒绝启动，fail point 与计时都没有意义）。
pub fn open_targets(
    store: &Arc<dyn ChunkStore>,
    cfg: &[TargetConfig],
    cluster_id: &[u8; 16],
) -> Result<Vec<Target>, String> {
    let mut out = Vec::new();
    for t in cfg {
        let root = Path::new(&t.path);
        let sb_path = root.join("TARGET");
        let hint = "M5 之前建的开发集群没有超级块：wmfs dev down --purge 之后重新 wmfs dev up";
        let b = std::fs::read(&sb_path)
            .map_err(|e| format!("读 target 超级块 {} 失败：{e}（{hint}）", sb_path.display()))?;
        let sb =
            TargetSuperblock::decode(&b).map_err(|e| format!("target 超级块 {} 不能用：{e}", sb_path.display()))?;
        if sb.cluster_id != *cluster_id {
            return Err(format!("target {} 属于别的集群（超级块里的 cluster_id 不符）：插错盘了？", t.path));
        }
        let ro_compat = sb.features_ro_compat != 0; // 首发不认识任何 ro_compat 位
        let s = store.open_target(root, sb.target_id).map_err(|e| format!("打开 target {} 失败：{e}", t.path))?;
        s.clear_temp().map_err(|e| format!("清空 target {} 的 p/、t/ 失败：{e}", t.path))?;
        out.push(Target::new(sb.target_id, &t.path, s, ro_compat, t.capacity_bytes()?));
    }
    Ok(out)
}
