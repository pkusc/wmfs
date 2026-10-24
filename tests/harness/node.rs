//! 完整的单节点（design.md 9.5 的单节点版）：在临时目录里 `init_node` + `server::start`（插件与 `wmfs server` 默认装的一样：
//! tcp、shared-key、posix-mode、label-match、local-fs、crc32c、replica、fifo、gc），以及连到它的客户端（MetaClient + DataClient）
//! 和一个参考客户端（[`put`] / [`get`]：与 `wmfs fs put/get` 相同的协议）。端口由测试挑空闲的（init 把它们写进 node.toml 与
//! NodeRecV1，客户端按集群图连数据节点；所以不能像别的测试那样直接监听 127.0.0.1:0，见 design.md 9.5 的实现注记（M5））。
//! fail point 是进程级的：用到 fail point 的测试各自单独一个测试程序。

use std::collections::BTreeSet;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use wmfs_core::api::types::KeyFile;
use wmfs_core::base::config::{NodeConfig, TargetConfig};
use wmfs_core::base::types::{ChunkId, Ts};
use wmfs_core::base::{ErrCode, log};
use wmfs_core::buf::{Lane, MemPool, PoolConfig};
use wmfs_core::client::data_client::{DataClient, chunk_id};
use wmfs_core::client::files::FileClient;
use wmfs_core::client::meta_client::{Caller, MetaClient, MetaClientOpts, SessionInfo};
use wmfs_core::hooks::registry::PluginSet;
use wmfs_core::init::{InitOpts, Profile, init_node};
use wmfs_core::meta::engine::EmbeddedEngine;
use wmfs_core::meta::engine::txn::{TxnOp, txn_blocking};
use wmfs_core::meta::schema::keys;
use wmfs_core::meta::schema::kv;
use wmfs_core::meta::schema::records::{ClusterSettingsV1, F_INLINE, LayoutV1};
use wmfs_core::net::client::{Endpoint, RpcClient};
use wmfs_core::proto::meta::{MetaOp, MetaReply, O_RDONLY, OpenOut, SetAttrIn};
use wmfs_core::server::{self, Running};
use wmfs_plugins::{crc32c, fifo, gc, label_match, local_fs, posix_mode, replica, shared_key, tcp};

use super::meta::admin_ctx;

/// 一个当前空闲的本机端口（绑定 :0 再放掉：端口要在 init 时写进 node.toml 与 NodeRecV1）。同一个测试进程里并行的用例各自起节点：
/// 记下已经给出去的端口，不给第二次——内核可能把刚放掉的端口又分给下一次 bind(:0)，两个节点就撞在同一个端口上（EADDRINUSE）。
pub fn free_port() -> u16 {
    static GIVEN: Mutex<BTreeSet<u16>> = Mutex::new(BTreeSet::new());
    loop {
        let p = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
        if GIVEN.lock().unwrap_or_else(|e| e.into_inner()).insert(p) {
            return p;
        }
    }
}

/// 与 `cli/src/registry.rs` 默认装的同一组插件。
pub fn plugins(cfg: &NodeConfig) -> PluginSet {
    let mut s = PluginSet::new(&cfg.plugins.disable);
    s.transport(Arc::new(tcp::TcpTransport::new(&cfg.plugin_cfg("tcp"), cfg.net.write_timeout_ms).unwrap()));
    s.authn(Arc::new(shared_key::SharedKey::new()));
    s.authz(Arc::new(posix_mode::PosixMode::new()));
    s.placement(Arc::new(label_match::LabelMatch::new()));
    s.chunk_store(Arc::new(local_fs::LocalFs::new()));
    s.transform(Arc::new(crc32c::Crc32c::new()));
    s.codec(Arc::new(replica::Replica::new()));
    s.qos(Arc::new(fifo::Fifo::new()));
    s.bg_task(Box::new(gc::Gc::new()));
    s.meta_engine(&wmfs_core::meta::engine::MANIFEST);
    s.validate(&cfg.plugin).unwrap();
    s
}

/// 测试用的临时目录 `<tmp>/wmfs-<名字>-<pid>`：丢弃时（包括测试失败 panic 时）删掉，不在 /tmp 留下垃圾。
pub struct TempDir(pub PathBuf);

impl TempDir {
    pub fn new(name: &str) -> TempDir {
        TempDir(std::env::temp_dir().join(format!("wmfs-{name}-{}", std::process::id())))
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// 节点目录 `<dir>/n1`，target 在 `<dir>/n1/targets/1`。
pub fn node_dir(dir: &Path) -> PathBuf {
    dir.join("n1")
}

pub fn target_dir(dir: &Path) -> PathBuf {
    node_dir(dir).join("targets/1")
}

/// 在 `dir` 里初始化一个单节点（一个 target，小内存池，打开 debug_ops 与 fail point 接口）。
pub fn init(dir: &Path) {
    let _ = std::fs::remove_dir_all(dir);
    let cfg = NodeConfig::default();
    let o = InitOpts {
        data_dir: node_dir(dir),
        addr: "127.0.0.1".into(),
        rpc_port: free_port(),
        mgmt_port: free_port(),
        mem: PoolConfig { s_mb: 16, m_slots: 12, l_slots: 1, repl_lane: true },
        failpoints_api: true,
        debug_ops: true,
        host_id: Some("host-1".into()),
        plugins: plugins(&cfg).plugin_list(),
        targets: vec![TargetConfig {
            path: target_dir(dir).display().to_string(),
            labels: vec![],
            capacity: "0".into(),
        }],
        profile: Profile::Default,
    };
    init_node(&o).unwrap();
}

/// 启动（或重启：上一个进程已经不在了）`dir` 里的节点，等它成为就绪 leader。
pub fn start(dir: &Path) -> Running {
    let nd = node_dir(dir);
    let cfg = NodeConfig::parse(&std::fs::read_to_string(nd.join("node.toml")).unwrap()).unwrap();
    let run = server::start(&nd, &cfg, plugins(&cfg), "test", Default::default()).unwrap();
    if std::env::var_os("WMFS_LOG").is_none() {
        log::set_level(log::Level::Warn);
    }
    super::meta::wait_ready(&run.meta);
    run
}

/// 连到一个节点的客户端：MetaClient（已开会话）、DataClient、底下的 RpcClient。
pub struct Client {
    pub meta: Arc<MetaClient>,
    pub data: DataClient,
    pub rpc: Arc<RpcClient>,
}

/// 用 `<节点>/keys/<key>.key` 连上 `run`。
pub fn client(dir: &Path, run: &Running, key: &str) -> Client {
    connect(dir, run, key, PoolConfig { s_mb: 8, m_slots: 6, l_slots: 1, repl_lane: false })
}

/// 连到 `run` 的文件层（M6 的 `FileClient`，与 `wmfs mount` 里的一样，只是不经 FUSE；不带 cache_store），用 client key。
/// 内存池比 [`client`] 的大：写回缓冲按池的槽位计（3.6.1）。
pub fn file_client(dir: &Path, run: &Running) -> Arc<FileClient> {
    let c = connect(dir, run, "client", PoolConfig { s_mb: 16, m_slots: 30, l_slots: 2, repl_lane: false });
    FileClient::start(c.meta, c.data, None)
}

fn connect(dir: &Path, run: &Running, key: &str, pool: PoolConfig) -> Client {
    let ep = Endpoint { node: 1, addrs: run.addrs.clone() };
    connect_seeds(&node_dir(dir), &[ep], key, pool)
}

/// 用 `<节点目录>/keys/<key>.key` 连到 `seeds` 里的节点（MetaClient 跟随 leader；多节点的测试给全部节点，M8）。
pub fn connect_seeds(nd: &Path, seeds: &[Endpoint], key: &str, pool: PoolConfig) -> Client {
    let kf = KeyFile::parse(&std::fs::read_to_string(nd.join(format!("keys/{key}.key"))).unwrap()).unwrap();
    let pool = MemPool::new(&pool).unwrap();
    let t = tcp::TcpTransport::new(&Default::default(), 5000).unwrap();
    let rpc = RpcClient::new(pool, vec![Arc::new(t)], kf, Default::default()).unwrap();
    let info = SessionInfo { client_name: "test".into(), host: "h".into(), host_id: "h".into(), mount: "/m".into() };
    let opts = MetaClientOpts { rpc_timeout: Duration::from_secs(10), op_timeout: Duration::from_secs(30) };
    let meta = Arc::new(MetaClient::connect_with(rpc.clone(), seeds, info, opts).unwrap());
    let data = DataClient::for_meta(rpc.clone(), meta.clone(), Duration::from_secs(10), Duration::from_secs(30));
    Client { meta, data, rpc }
}

pub fn who(uid: u32, gid: u32) -> Caller {
    Caller { uid, gid, groups: vec![], pid: 1, job_id: 0 }
}

/// 经执行器改集群参数（例如把 `epoch_gc_grace` 调成 0），同批 `map_epoch += 1`（集群图按 map_epoch 缓存）。
pub fn set_settings(eng: &EmbeddedEngine, f: impl FnOnce(&mut ClusterSettingsV1) + Send + 'static) {
    let op: TxnOp = Box::new(move |v, _c| {
        let mut s: ClusterSettingsV1 = kv::get(v, &keys::global(keys::G_SETTINGS))?.ok_or(ErrCode::INTERNAL)?;
        f(&mut s);
        kv::put(v, &keys::global(keys::G_SETTINGS), &s);
        kv::bump_map_epoch(v)?;
        Ok(MetaReply::Ok)
    });
    txn_blocking(eng, admin_ctx(), op).unwrap();
}

/// 等 `f()` 为真，最多 `secs` 秒（每 50 ms 看一次）。
pub fn wait_for(secs: u64, mut f: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < deadline {
        if f() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    f()
}

pub fn opened(r: Result<MetaReply, ErrCode>) -> Result<OpenOut, ErrCode> {
    match r? {
        MetaReply::Opened(o) => Ok(o),
        other => panic!("期望 Opened，得到 {other:?}"),
    }
}

pub fn chunk_of(o: &OpenOut, idx: u32) -> ChunkId {
    let l: &LayoutV1 = o.layout.as_ref().unwrap();
    chunk_id(l, o.attr.ino, idx).unwrap()
}

/// 提交写入的长度（可带 inline 补丁）。
pub fn commit(
    c: &Client,
    who: &Caller,
    o: &OpenOut,
    new_size: u64,
    patches: Vec<(u64, Vec<u8>)>,
) -> Result<MetaReply, ErrCode> {
    let epoch = o.layout.as_ref().unwrap().epoch;
    let (mtime, inline_patches) = (Ts::default(), patches);
    let op = MetaOp::CommitWrite {
        ino: o.attr.ino,
        epoch,
        new_size,
        mtime,
        inline_patches,
        uninline_expect_change_id: None,
        want_quota: false,
        close: true,
    };
    c.meta.write(who, op)
}

/// 把 `data` 写进 chunk（从文件偏移 `off` 开始，可以跨 chunk）。
pub fn write_chunks(c: &Client, o: &OpenOut, off: u64, data: &[u8]) -> Result<(), ErrCode> {
    let cs = o.layout.as_ref().unwrap().chunk_size as u64;
    let mut pos = 0usize;
    while pos < data.len() {
        let at = off + pos as u64;
        let (idx, in_chunk) = (at / cs, at % cs);
        let n = ((cs - in_chunk) as usize).min(data.len() - pos);
        let mut b = c.rpc.pool().get(n, Lane::Normal, Duration::from_secs(5))?;
        b.as_mut_slice().copy_from_slice(&data[pos..pos + n]);
        let b = b.freeze();
        c.data.write(&o.cap, chunk_of(o, idx as u32), vec![(in_chunk as u32, n as u32)], vec![b.whole()], false)?;
        pos += n;
    }
    Ok(())
}

/// 参考客户端的 put（与 `wmfs fs put` 相同的协议）：建或打开 → 有内容就截断到 0 → 小于 inline_max 走 inline 补丁，否则（先
/// Uninline）按 chunk 写 → CommitWrite。返回最后一次的 OpenOut。
pub fn put(c: &Client, who: &Caller, parent: u64, name: &str, data: &[u8]) -> Result<OpenOut, ErrCode> {
    let op = MetaOp::Create { parent, name: name.as_bytes().to_vec(), mode: 0o644, umask: 0, excl: false };
    let mut o = opened(c.meta.write(who, op))?;
    if o.attr.size > 0 {
        let set =
            SetAttrIn { mode: None, uid: None, gid: None, size: Some(0), atime: None, mtime: None, epoch_seen: 0 };
        o = opened(c.meta.write(who, MetaOp::SetAttr { ino: o.attr.ino, set }))?;
    }
    let l = o.layout.clone().unwrap();
    if o.attr.flags & F_INLINE != 0 && data.len() <= l.inline_max as usize {
        commit(c, who, &o, data.len() as u64, vec![(0, data.to_vec())])?;
        return Ok(o);
    }
    if o.attr.flags & F_INLINE != 0 {
        o = opened(c.meta.read(who, MetaOp::Uninline { ino: o.attr.ino }))?;
    }
    write_chunks(c, &o, 0, data)?;
    commit(c, who, &o, data.len() as u64, vec![])?;
    Ok(o)
}

/// 参考客户端的 get（与 `wmfs fs get` 相同的协议）：Open（只读）→ inline 内容或逐个 chunk 读，按文件长度补 0。
pub fn get(c: &Client, who: &Caller, ino: u64) -> Result<Vec<u8>, ErrCode> {
    let o = opened(c.meta.read(who, MetaOp::Open { ino, flags: O_RDONLY }))?;
    let size = o.attr.size as usize;
    if o.attr.flags & F_INLINE != 0 {
        let mut d = o.inline.clone().unwrap_or_default();
        d.resize(size, 0);
        return Ok(d);
    }
    let l = o.layout.clone().unwrap();
    let cs = l.chunk_size as usize;
    let mut out = Vec::with_capacity(size);
    for idx in 0..size.div_ceil(cs) {
        let len = cs.min(size - idx * cs);
        let into = c.rpc.pool().get(len, Lane::Normal, Duration::from_secs(5))?;
        match c.data.read(&o.cap, &l, chunk_of(&o, idx as u32), 0, len as u32, into, None) {
            Ok((b, n)) => out.extend_from_slice(&b.as_slice()[..n as usize]),
            Err(ErrCode::CHUNK_NOT_FOUND) => {}
            Err(e) => return Err(e),
        }
        out.resize((idx * cs + len).min(size), 0);
    }
    Ok(out)
}
