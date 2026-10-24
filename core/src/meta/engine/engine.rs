//! `MetaEngine` 的首发实现 embedded-kv（design.md 4.10、3.5.1、3.4.1）：raft-rs + redb，单飞批次执行器 + 读闸门。
//!
//! 因为"元数据 Raft"是契约层，代码在 core（这里与 `meta/raft/`）；注册表里以 `embedded-kv` 的名义登记为唯一的
//! meta_engine（manifest 见 [`MANIFEST`] 与 `plugins/embedded-kv/manifest.toml`）。fail point `plugin.embedded-kv.meta_engine`
//! 包在 `read/txn` 外层（4.10）。线程：`raft`（独占 RawNode 与 redb 写事务）、`meta-exec`（执行器）；读请求在调用者的
//! 线程上执行（meta-read 线程、后台任务、管理 API 线程）。这两个线程上的 panic（不变量破坏）一律写日志并 abort（fail-stop）。
//! 实现注记（M8）：给了服务端之间的 RPC 客户端（`EngineOpts.rpc`）就建元数据网络 [`RaftNet`]（raft-send、snap-recv、snap-serve，
//! `meta/raft/peers.rs`），服务端装配时把它登记到 RpcServer（[`EmbeddedEngine::net`]）；成员变更（cluster-mgr 的
//! `Membership`）经这里的 [`Members`] 实现提议 conf change、让出 leader、改节点记录。

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crossbeam_channel::{Sender, TrySendError, bounded};
use redb::Database;
use wmfs_api::metrics::{self, Histogram};
use wmfs_api::types::NodeAddrs;

use super::executor;
use super::txn::{EngineStatus, KvScan, MetaEngine, OpCtx, ReadFn, RoleInfo, TxnRequest};
use super::view::ReadView;
use crate::base::types::ReqCtx;
use crate::base::{ErrCode, log};
use crate::hooks::manifest::{CORE_ABI_VERSION, Hook, Manifest};
use crate::hooks::pipeline::{HookCall, Pipeline};
use crate::hooks::traits::KeyLookup;
use crate::meta::attrs::AttrResolver;
use crate::meta::cluster::map::MapCache;
use crate::meta::raft::driver::{Driver, DriverOpts, ProposeOutcome, Published, RaftInput};
use crate::meta::raft::peers::RaftNet;
use crate::meta::raft::redb_eio;
use crate::meta::raft::storage::{KV, read_global};
use crate::meta::schema::keys;
use crate::meta::schema::records::{
    ClusterInfoV1, ClusterSettingsV1, KeyMetaV1, NodeRecV1, PluginListV1, WriteBatchV1, decode_v1,
};
use crate::net::client::RpcClient;
use crate::proto::handshake::{SERVER_KEY, derive_secret};
use crate::proto::meta::MetaReply;
use crate::proto::peer::ClusterMap;

pub static MANIFEST: Manifest = Manifest {
    name: "embedded-kv",
    version: "0.1.0",
    hooks: &[Hook::MetaEngine],
    altitude: 0, // 唯一型钩子
    hot_path: false,
    config_schema: "",
    abi_version: CORE_ABI_VERSION,
    attrs: &[],
    budget_ns: 0,
    fail_points: &["plugin.embedded-kv.meta_engine"],
};

/// 读者等 ReadIndex 的上限（3.4.1.6 第 1 条）；执行器与 raft 线程的输入队列长度。
const READ_WAIT: Duration = Duration::from_secs(3);
const EXEC_QUEUE: usize = 1024;
const RAFT_QUEUE: usize = 4096;
const BATCH_OPS: &[f64] = &[1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0];
const BATCH_BYTES: &[f64] = &[256.0, 1024.0, 4096.0, 16384.0, 65536.0, 262144.0, 786432.0, 1048576.0, 2097152.0];

pub struct EngineOpts {
    pub node_id: u32,
    pub plugins: PluginListV1, // 本节点的插件清单（与已提交的 GLOBAL plugins 一致才就绪，3.7.2）
    pub pipeline: Arc<Pipeline>,
    pub cap_key: [u8; 32], // K_cap（由 cluster.key 派生，3.3.2）：闭包里签 cap 用（M5）
    // 实现注记（M8）：服务端之间的 RPC 客户端（服务端身份 key 0；None = 单节点的组件测试，raft 消息无处可发）、下载的快照文件放在
    // 哪（`<data_dir>/meta`）、`wmfs server --adopt-local-plugins`（3.7.2 第 5 条）
    pub rpc: Option<Arc<RpcClient>>,
    pub snap_dir: PathBuf,
    pub adopt_local_plugins: bool,
}

/// 执行器闭包与只读回调能用到的一切（[`OpCtx`] 的来源）。插件拿不到 redb 本身。
/// 集群图不放在这里：每个批次开始时、每个只读请求在 ReadIndex 之后各自从自己的读事务里取一份快照（`maps`，按 map_epoch 缓存）。
pub struct OpEnv {
    pub pipeline: Arc<Pipeline>,
    pub attrs: AttrResolver,
    pub maps: MapCache,
    pub cap_key: [u8; 32],
}

impl OpEnv {
    pub fn new(pipeline: Arc<Pipeline>, cap_key: [u8; 32]) -> OpEnv {
        OpEnv { pipeline, attrs: AttrResolver::default(), maps: MapCache::default(), cap_key }
    }

    /// 一个操作的上下文：`map` 是这个批次（或这个只读请求）的集群图快照，集群参数取自它。
    pub fn op_ctx<'a>(&'a self, req: &'a ReqCtx, map: &'a ClusterMap) -> OpCtx<'a> {
        OpCtx {
            req,
            pipeline: &self.pipeline,
            attrs: &self.attrs,
            map,
            settings: &map.settings,
            cap_key: &self.cap_key,
        }
    }
}

/// 执行器线程与引擎共用的部分。
pub(crate) struct EngineShared {
    pub db: Arc<Database>,
    pub raft_tx: Sender<RaftInput>,
    pub published: Arc<Published>,
    pub env: OpEnv,
    pub stopped: AtomicBool, // shutdown 之后为 true（cluster-mgr 线程据此退出）
    batch_ops: Arc<Histogram>,
    batch_bytes: Arc<Histogram>,
    commit_secs: Arc<Histogram>,
}

impl EngineShared {
    /// 提议一个批次并阻塞到 raft 线程给出结局（raft 线程退出 = 结果不确定）。
    pub fn propose_and_wait(&self, wb: &WriteBatchV1) -> ProposeOutcome {
        let (done, rx) = bounded(1);
        let input = RaftInput::Propose { data: wb.encode(), leader_term: wb.leader_term, done };
        if self.raft_tx.send(input).is_err() {
            return ProposeOutcome::Lost;
        }
        rx.recv().unwrap_or(ProposeOutcome::Lost)
    }

    /// 与读视图 `v` 一致的集群图快照（leader 取自当前角色）。
    pub fn map_for(&self, v: &ReadView) -> Result<Arc<ClusterMap>, ErrCode> {
        let leader = Some(self.published.role().leader_id).filter(|id| *id != 0);
        self.env.maps.get(v, leader)
    }

    pub fn observe_batch(&self, ops: usize, bytes: usize, took: Duration) {
        self.batch_ops.observe(ops as f64);
        self.batch_bytes.observe(bytes as f64);
        self.commit_secs.observe(took.as_secs_f64());
    }
}

pub struct EmbeddedEngine {
    shared: Arc<EngineShared>,
    exec_tx: Sender<TxnRequest>,
    hook: HookCall,
    gate_secs: Arc<Histogram>,
    raft: Mutex<Option<JoinHandle<()>>>,
    node_id: u32,
    plugins: PluginListV1,
    adopt_local_plugins: bool,
    net: Option<Arc<RaftNet>>,
}

impl EmbeddedEngine {
    /// 打开 `meta/meta.redb`（init 建的；不存在就报错，绝不自己新建：坑 P14）并启动 raft 与执行器线程。
    pub fn open(path: &Path, o: EngineOpts) -> Result<Arc<EmbeddedEngine>, String> {
        let db = Database::builder().open(path).map_err(|e| format!("打开元数据库 {} 失败：{e}", path.display()))?;
        EmbeddedEngine::start(db, o).map_err(|e| format!("{e}（元数据库 {}）", path.display()))
    }

    /// 在一个已打开的数据库上启动（测试用它接模拟掉电的后端）。GLOBAL `cluster` 与 `settings`（init 总会写，3.4.3）
    /// 读不到、不存在或解不开都拒绝启动：集群参数必须全集群一致（租约安全依赖它，3.2.8），不能悄悄换成默认值。
    pub fn start(db: Database, o: EngineOpts) -> Result<Arc<EmbeddedEngine>, String> {
        let db = Arc::new(db);
        let (snap_dir, adopt_local_plugins) = (o.snap_dir.clone(), o.adopt_local_plugins);
        let dopts = DriverOpts { node_id: o.node_id, plugins: o.plugins.clone(), snap_dir, adopt_local_plugins };
        let driver = Driver::open(db.clone(), dopts)?;
        let published = driver.published();
        let rtx = db.begin_read().map_err(|e| format!("redb 读事务失败：{e}"))?;
        let missing = |k: &str| format!("元数据库里没有 GLOBAL {k}（不是 wmfs init 建的元数据库？）");
        read_global::<ClusterInfoV1>(&rtx, keys::G_CLUSTER)?.ok_or_else(|| missing(keys::G_CLUSTER))?;
        read_global::<ClusterSettingsV1>(&rtx, keys::G_SETTINGS)?.ok_or_else(|| missing(keys::G_SETTINGS))?;
        drop(rtx);
        let (raft_tx, raft_rx) = bounded(RAFT_QUEUE);
        let net = o.rpc.map(|rpc| RaftNet::new(rpc, db.clone(), raft_tx.clone(), driver.pins(), o.snap_dir.clone()));
        let net_for_raft = net.clone();
        let raft = fail_stop_thread("raft", move || driver.run(raft_rx, net_for_raft))?;
        let h = |name, help, b| metrics::histogram(name, help, &[], b);
        let shared = Arc::new(EngineShared {
            db,
            raft_tx,
            published,
            env: OpEnv::new(o.pipeline, o.cap_key),
            stopped: AtomicBool::new(false),
            batch_ops: h("wmfs_meta_batch_ops", "每个批次的操作数", BATCH_OPS),
            batch_bytes: h("wmfs_meta_batch_bytes", "每个批次的写集编码字节数", BATCH_BYTES),
            commit_secs: h("wmfs_meta_commit_seconds", "批次从提议到应用的耗时", metrics::SECONDS_BUCKETS),
        });
        let (exec_tx, exec_rx) = bounded(EXEC_QUEUE);
        let s = shared.clone();
        fail_stop_thread("meta-exec", move || executor::run(exec_rx, s))?;
        let gate_secs = h("wmfs_meta_read_gate_seconds", "线性一致读等 ReadIndex 的耗时", metrics::SECONDS_BUCKETS);
        super::dedup::register_metrics();
        let hook = HookCall::new(&MANIFEST, Hook::MetaEngine);
        let raft = Mutex::new(Some(raft));
        let (node_id, plugins) = (o.node_id, o.plugins);
        Ok(Arc::new(EmbeddedEngine {
            shared,
            exec_tx,
            hook,
            gate_secs,
            raft,
            node_id,
            plugins,
            adopt_local_plugins,
            net,
        }))
    }

    /// 元数据网络（服务端装配时把 RaftMsg 与 SnapshotFetch 登记到 RpcServer；没有 RPC 客户端的组件测试是 None）。
    pub fn net(&self) -> Option<Arc<RaftNet>> {
        self.net.clone()
    }

    /// 本节点按 node.toml 装出来的插件清单（3.7.2）。
    pub fn local_plugins(&self) -> &PluginListV1 {
        &self.plugins
    }
    /// 是不是用 `wmfs server --adopt-local-plugins` 启动的（3.7.2 第 5 条的逃生口）。
    pub fn adopt_local_plugins(&self) -> bool {
        self.adopt_local_plugins
    }
    /// 已应用的集群插件清单与本地的不一致（cluster-mgr 看它，3.7.2）。
    pub fn plugins_mismatch(&self) -> bool {
        self.shared.published.plugins_mismatch()
    }

    /// 本地（不经 ReadIndex）读一个键的已应用值：认证用的 KEYMETA、health 里的 leader 地址这类"读到稍旧的也无妨"的地方用。
    pub fn local_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ErrCode> {
        local_get(&self.shared.db, key)
    }

    /// 某个节点的地址（来自已应用的 NodeRecV1；查不到 → None）。
    pub fn node_addrs(&self, node: u32) -> Option<NodeAddrs> {
        let b = self.local_get(&keys::node(node)).ok().flatten()?;
        let n = decode_v1::<NodeRecV1>(&b).ok()?;
        Some(NodeAddrs { node_id: node, addrs: n.addrs, mgmt_addr: n.mgmt_addr })
    }

    /// 读 KV 里 KEYMETA 的 key 表（服务端握手与管理 API 签名校验用，3.3.2）。
    pub fn keys(self: &Arc<Self>, master: [u8; 32]) -> KvKeys {
        KvKeys { master, eng: self.clone() }
    }

    /// 在本地（不经 ReadIndex）的已应用状态上跑 `f`：只给"读到稍旧的也无妨"的统计用（cluster-mgr 刷新 changelog 消费者的
    /// 落后条数，实现注记（M7））。
    pub fn local_scan(&self, f: &mut dyn FnMut(&dyn KvScan) -> Result<(), ErrCode>) -> Result<(), ErrCode> {
        let rtx = self.shared.db.begin_read().map_err(|e| redb_eio("本地读的读事务", e))?;
        f(&ReadView::new(&rtx)?)
    }

    /// 当前的集群图（本地读已应用的状态，不经 ReadIndex：cluster-mgr 写集群警告的日志用）。
    pub fn cluster_map(&self) -> Result<Arc<ClusterMap>, ErrCode> {
        let rtx = self.shared.db.begin_read().map_err(|e| redb_eio("本地读的读事务", e))?;
        self.shared.map_for(&ReadView::new(&rtx)?)
    }

    /// changelog 长轮询用（3.11，实现注记（M7））："写了 changelog 的批次"计数的当前值；先记下它再读一页，读完没有新记录就
    /// [`wait_changelog`](Self::wait_changelog)——中间提交的记录不会漏掉唤醒。
    pub fn changelog_seen(&self) -> u64 {
        self.shared.published.changelog_seen()
    }

    /// 等到有新的 changelog 记录提交、本节点失去 leader（计数不再是 `seen`，M8），或超时。
    pub fn wait_changelog(&self, seen: u64, timeout: Duration) -> u64 {
        self.shared.published.wait_changelog(seen, timeout)
    }

    /// 已经 `shutdown` 了吗（cluster-mgr 线程据此退出）。
    pub fn stopped(&self) -> bool {
        self.shared.stopped.load(Ordering::Relaxed)
    }

    /// 停止：raft 线程补一次 Immediate 提交、在途提议判 Lost，然后退出（之后的请求一律 NOT_LEADER）。
    pub fn shutdown(&self) {
        self.shared.stopped.store(true, Ordering::Relaxed);
        let _ = self.shared.raft_tx.send(RaftInput::Shutdown);
        let h = self.raft.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(h) = h {
            let _ = h.join();
        }
    }

    fn read_inner(&self, ctx: &ReqCtx, f: ReadFn) -> Result<MetaReply, ErrCode> {
        let (done, rx) = bounded(1);
        let t0 = Instant::now();
        self.shared.raft_tx.send(RaftInput::ReadIndex { done }).map_err(|_| ErrCode::NOT_LEADER)?;
        rx.recv_timeout(READ_WAIT).unwrap_or(Err(ErrCode::RETRY))?;
        self.gate_secs.observe(t0.elapsed().as_secs_f64());
        let rtx = self.shared.db.begin_read().map_err(|e| redb_eio("读事务", e))?;
        let view = ReadView::new(&rtx)?;
        let map = self.shared.map_for(&view)?;
        f(&view, &self.shared.env.op_ctx(ctx, &map))
    }
}

impl MetaEngine for EmbeddedEngine {
    fn manifest(&self) -> &'static Manifest {
        &MANIFEST
    }

    /// 线性一致读：先过读闸门（ReadIndex，确认 leader 身份且 applied ≥ read index），再开 redb 读事务回调 `f`。
    fn read(&self, ctx: &ReqCtx, f: ReadFn) -> Result<MetaReply, ErrCode> {
        let t0 = self.hook.enter()?; // fail point plugin.embedded-kv.meta_engine
        let r = self.read_inner(ctx, f);
        self.hook.exit(t0, true);
        r
    }

    /// 读写事务：投递给执行器后立即返回；队列满 → `BUSY`（可重试）。`req.done` 恰好被调用一次。
    fn txn(&self, req: TxnRequest) {
        let t0 = match self.hook.enter() {
            Ok(t) => t,
            Err(c) => return (req.done)(Err(c)),
        };
        match self.exec_tx.try_send(req) {
            Ok(()) => {}
            Err(TrySendError::Full(r)) => (r.done)(Err(ErrCode::BUSY)),
            Err(TrySendError::Disconnected(r)) => (r.done)(Err(ErrCode::NOT_LEADER)),
        }
        self.hook.exit(t0, true);
    }

    fn role(&self) -> RoleInfo {
        self.shared.published.role()
    }

    fn status(&self) -> EngineStatus {
        self.shared.published.status()
    }
}

/// 认证用的 key 表：KEYMETA 来自已应用的 KV（3.3.2；M1 的临时表 `init::InitKeys` 只留给组件测试），秘密由 cluster.key 派生。
/// key 0 固定是服务端之间用的（不在 KEYMETA 里）。被撤销的 key 照常返回元数据，由 authn 插件拒绝。
pub struct KvKeys {
    master: [u8; 32],
    eng: Arc<EmbeddedEngine>,
}

impl KeyLookup for KvKeys {
    fn secret(&self, key_id: u32) -> Option<[u8; 32]> {
        (key_id == SERVER_KEY || self.meta(key_id).is_some()).then(|| derive_secret(&self.master, key_id))
    }
    fn meta(&self, key_id: u32) -> Option<KeyMetaV1> {
        let b = self.eng.local_get(&keys::keymeta(key_id)).ok().flatten()?;
        decode_v1(&b).ok()
    }
}

fn local_get(db: &Database, key: &[u8]) -> Result<Option<Vec<u8>>, ErrCode> {
    let rtx = db.begin_read().map_err(|e| redb_eio("本地读的读事务", e))?;
    let t = rtx.open_table(KV).map_err(|e| redb_eio("本地读打开 kv 表", e))?;
    let v = t.get(key).map_err(|e| redb_eio("本地读", e))?;
    Ok(v.map(|g| g.value().to_vec()))
}

/// 起一个线程：线程里的 panic（不变量破坏）一律写日志并 abort（fail-stop，0.2 第 5 条）——不能让 raft 或执行器线程
/// 悄悄死掉、节点却还在应答。panic 的原因写进同一行日志（默认的 panic 输出在 stderr 上，前台运行时不在日志文件里）。
fn fail_stop_thread(name: &str, f: impl FnOnce() + Send + 'static) -> Result<JoinHandle<()>, String> {
    let n = name.to_string();
    std::thread::Builder::new()
        .name(n.clone())
        .spawn(move || {
            if let Err(p) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
                // panic 的消息是 String（带格式参数的 panic!）或 &str（字面量）
                let why = p.downcast_ref::<String>().map(String::as_str).or_else(|| p.downcast_ref::<&str>().copied());
                let why = why.unwrap_or("（原因不是字符串）");
                log::error("meta", format_args!("线程 {n} panic：{why}——按 fail-stop 规则立即退出"));
                std::process::abort();
            }
        })
        .map_err(|e| format!("起线程 {name} 失败：{e}"))
}

mod members;
#[cfg(test)]
mod tests;
