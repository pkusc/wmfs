//! CRAQ 组件测试（design.md 9.3，M9）：一个进程里几个数据服务（DataService + PeerService + repl 线程 + chain-timer + 同步线程），经
//! mem 传输互连，集群图由测试直接下发（不需要元数据服务；链的变化由测试按链管理器的规则——`chain_plan` 的函数——算出来）。target
//! 是内存版的 [`MemTargetStore`]（可以模拟掉电）。没有 hb 线程，所以不核对 I5 的租约与 boot_id（3.4.2.2；要测 I5 的用例自己打开
//! `lease.enforce`，集群图里 target 的 boot_id 用 [`Craq::set_boot`] 给）；转发的期限、chain-timer 的 3 s 与生产代码相同。集群图里的
//! `target_lease` 是 1 s：新的 Syncing 成员第一轮同步之前等 0.8 × target_lease（sync.rs），组件测试里不必等 6.4 s。
//!
//! "重启"一个节点 = 让旧的数据服务看到一张没有任何链的集群图（它放弃全部 pending、之后什么也不服务），在掉电之后的存储上起一个新的
//! 数据服务（新的 mem 地址），集群图里这个节点的地址换成新的。

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use plugin_mem_transport::MemTransport;
use wmfs_core::base::types::{ChunkId, Role};
use wmfs_core::base::{ErrCode, clock};
use wmfs_core::buf::{BufSlice, Lane, MemPool, PoolConfig};
use wmfs_core::client::data_client::DataClient;
use wmfs_core::data::format::{CHUNK_HEADER_LEN, ChunkHeader, table_len_of};
use wmfs_core::data::node_hb::MapHolder;
use wmfs_core::data::service::DataService;
use wmfs_core::data::state::PendingWrite;
use wmfs_core::data::write::{self, JobKind, WriteJob};
use wmfs_core::data::{DataNode, Target, forward, redrive, replica};
use wmfs_core::hooks::pipeline::Pipeline;
use wmfs_core::hooks::registry::PluginSet;
use wmfs_core::hooks::traits::{TargetStore, Transport};
use wmfs_core::init::InitKeys;
use wmfs_core::meta::cluster::chains::SERVING;
use wmfs_core::meta::schema::records::{ChainRecV1, ClusterSettingsV1, NODE_ACTIVE, NodeRecV1, TargetRecV1};
use wmfs_core::net::client::{Endpoint, Family, RpcClient};
use wmfs_core::net::server::{RpcServer, ServerId};
use wmfs_core::proto::cap::{self, CapV1};
use wmfs_core::proto::data::{DataCall, DataOp, DataReply};
use wmfs_core::proto::frame::{FrameHeader, FrameKind, InFrame, MSG_DATA_CALL, MSG_PEER_CALL, OutFrame};
use wmfs_core::proto::peer::{ClusterMap, PeerOp, PeerReply};
use wmfs_core::proto::{decode_body, encode_body};
use wmfs_plugins::crc32c::Crc32c;
use wmfs_plugins::fifo::Fifo;
use wmfs_plugins::replica::Replica;
use wmfs_plugins::shared_key::SharedKey;

use super::data::{CLUSTER, EPOCH, INO, cap as data_cap};
use super::meta::CAP_KEY;
use super::net::{MASTER, key, net};

pub use super::mem_target::MemTargetStore;

/// 测试里每个数据节点的池（≈ 50 MiB：几个节点共用一个进程）。
const POOL: PoolConfig = PoolConfig { s_mb: 8, m_slots: 8, l_slots: 1, repl_lane: true };

/// 一个数据节点。
pub struct Member {
    pub node: Arc<DataNode>,
    pub store: MemTargetStore,
    pub addr: String,
}

pub struct Craq {
    pub members: Vec<Member>, // 下标 0 = 节点 / target 1
    map: Arc<Mutex<Arc<ClusterMap>>>,
    epoch: AtomicU64,
    boots: Mutex<BTreeMap<u32, u64>>, // 集群图里 target 的 boot_id（没给 = 0）
    pub client: DataClient,
    pub cs: u32, // chunk 大小（块 = min(cs, 64 KiB)）
}

/// 测试用的集群图里的 target 租约（见文件头）。
const LEASE_MS: u64 = 1000;

fn pipeline() -> Arc<Pipeline> {
    let mut s = PluginSet::new(&[]);
    s.transport(Arc::new(MemTransport::new()));
    s.authn(Arc::new(SharedKey::new()));
    s.transform(Arc::new(Crc32c::new()));
    s.qos(Arc::new(Fifo::new()));
    s.codec(Arc::new(Replica::new()));
    s.validate(&BTreeMap::new()).unwrap();
    Arc::new(Pipeline::new(s))
}

/// 一张只有链的集群图：节点 i 的 target 是 i，地址来自 `addrs`，boot_id 来自 `boots`。
fn map_of(epoch: u64, addrs: &[String], chains: &[(u32, ChainRecV1)], boots: &BTreeMap<u32, u64>) -> ClusterMap {
    let node = |i: usize| {
        let a = vec![("mem".to_string(), addrs[i].clone())];
        let (mgmt_addr, host_id, machine_id, hostname) = (String::new(), String::new(), String::new(), String::new());
        let rec = NodeRecV1 {
            addrs: a,
            mgmt_addr,
            host_id,
            machine_id,
            hostname,
            meta_role: 0,
            features: vec![],
            joined_ms: 0,
            state: NODE_ACTIVE,
        };
        (i as u32 + 1, rec)
    };
    let target = |i: usize| {
        let (path, labels, phys_devs) = (String::new(), vec![], vec![]);
        let rec = TargetRecV1 {
            node: i as u32 + 1,
            path,
            labels,
            phys_devs,
            capacity: 0,
            boot_id: boots.get(&(i as u32 + 1)).copied().unwrap_or(0),
            state: 1,
            last_up_ms: 0,
            read_only: false,
        };
        (i as u32 + 1, rec)
    };
    let settings = ClusterSettingsV1 { target_lease_ms: LEASE_MS, ..Default::default() };
    ClusterMap {
        map_epoch: epoch,
        cluster_id: CLUSTER,
        settings,
        nodes: (0..addrs.len()).map(node).collect(),
        targets: (0..addrs.len()).map(target).collect(),
        chains: chains.to_vec(),
        ..Default::default()
    }
}

/// 链 `id`：成员 (target, 状态)，版本 `ver`、incarnation。
pub fn chain(members: &[(u32, u8)], ver: u64, incarnation: u32) -> ChainRecV1 {
    let (last_serving, removed) = (None, vec![]);
    ChainRecV1 {
        ver,
        incarnation,
        class_id: 1,
        replicas: members.len() as u8,
        members: members.to_vec(),
        last_serving,
        removed,
    }
}

static NAMES: AtomicU64 = AtomicU64::new(1);

/// 在 `store` 上起一个数据节点（target id = `id`），监听一个新的 mem 名字。
fn member(id: u32, store: MemTargetStore) -> Member {
    let addr = format!("craq-{}-{}", std::process::id(), NAMES.fetch_add(1, Ordering::Relaxed));
    let (p, pool) = (pipeline(), MemPool::new(&POOL).unwrap());
    let t = Target::new(id, "/mem", Arc::new(store.clone()) as Arc<dyn TargetStore>, false, 0);
    let node = DataNode::new(p.clone(), vec![t], pool.clone(), CLUSTER, CAP_KEY, Arc::new(MapHolder::new()));
    let keys = Arc::new(InitKeys::new(MASTER, 0));
    let rpc = RpcServer::new(ServerId { cluster_id: CLUSTER, node_id: id }, p.clone(), keys, &net("mem"));
    let peer =
        RpcClient::new_with_role(pool.clone(), vec![Arc::new(MemTransport::new())], key(0), Role::Server, net("mem"))
            .unwrap();
    let svc = DataService::start(node.clone()).unwrap();
    rpc.add_service(MSG_DATA_CALL, svc.clone());
    rpc.add_service(MSG_PEER_CALL, replica::start(&node, peer, svc).unwrap());
    let tr = Arc::new(MemTransport::listening(&addr)) as Arc<dyn Transport>;
    rpc.listen(&tr, pool).unwrap();
    std::mem::forget(rpc); // 节点随测试进程一直在（mem 传输的监听没有关闭的办法）
    Member { node, store, addr }
}

impl Craq {
    /// `n` 个数据节点，一条链 1 = `members`，chunk 64 KiB（一个块）。
    pub fn start(n: usize, members: &[(u32, u8)]) -> Craq {
        Craq::start_with(n, members, CHUNK)
    }

    /// 同 [`Craq::start`]，chunk 大小 `cs`（大于 64 KiB 就是多个块：部分覆盖时没改动的块从已提交版本复制、截断时变成洞的块）。
    pub fn start_with(n: usize, members: &[(u32, u8)], cs: u32) -> Craq {
        let ms: Vec<Member> = (1..=n as u32).map(|i| member(i, MemTargetStore::new())).collect();
        let map = Arc::new(Mutex::new(Arc::new(ClusterMap::default())));
        let rpc = RpcClient::new_with_role(
            MemPool::new(&POOL).unwrap(),
            vec![Arc::new(MemTransport::new())],
            key(2),
            Role::Client,
            net("mem"),
        )
        .unwrap();
        let m2 = map.clone();
        let client = DataClient::new(rpc, Arc::new(move || m2.lock().unwrap().clone()), false)
            .with_timeouts(Duration::from_secs(5), Duration::from_secs(30));
        let (epoch, boots) = (AtomicU64::new(1), Mutex::default());
        let c = Craq { members: ms, map, epoch, boots, client, cs };
        c.set_chains(&[(1, chain(members, 1, 1))]);
        c
    }

    /// 之后下发的集群图里 target `t` 的 boot_id（I5 的用例）。
    pub fn set_boot(&self, t: u32, boot: u64) {
        self.boots.lock().unwrap().insert(t, boot);
    }

    pub fn map(&self) -> Arc<ClusterMap> {
        self.map.lock().unwrap().clone()
    }

    /// 下发新的链（像 hb 线程拉到新集群图：每个节点先按 3.4.2.6 处理 pending，再换上视图），客户端也换上。
    pub fn set_chains(&self, chains: &[(u32, ChainRecV1)]) {
        let m = self.new_map(chains);
        self.deliver(&m, &(1..=self.members.len() as u32).collect::<Vec<_>>());
        *self.map.lock().unwrap() = m;
    }

    /// 只把集群图 `m` 交给节点 `to`（集群图传播有先后：别的节点与客户端还在旧图上，客户端用 [`Craq::set_client_map`] 换）。
    pub fn deliver(&self, m: &Arc<ClusterMap>, to: &[u32]) {
        for n in to {
            redrive::switch(&self.members[*n as usize - 1].node, m.clone());
        }
    }

    pub fn set_client_map(&self, m: Arc<ClusterMap>) {
        *self.map.lock().unwrap() = m;
    }

    /// 一张新的集群图（epoch 加一；还没交给任何节点）。
    pub fn new_map(&self, chains: &[(u32, ChainRecV1)]) -> Arc<ClusterMap> {
        let e = self.epoch.fetch_add(1, Ordering::Relaxed) + 1;
        let addrs: Vec<String> = self.members.iter().map(|m| m.addr.clone()).collect();
        Arc::new(map_of(e, &addrs, chains, &self.boots.lock().unwrap()))
    }

    /// 节点 `i`（target i）掉电重启：旧的服务看到一张没有链的集群图（放弃 pending、不再服务），新的服务起在掉电之后的存储上。
    pub fn crash(&mut self, i: u32) {
        let old = &self.members[i as usize - 1];
        let dead = Arc::new(ClusterMap { map_epoch: u64::MAX, ..Default::default() });
        redrive::switch(&old.node, dead);
        let store = old.store.crash();
        store.clear_temp().unwrap(); // 启动时清空 p/、t/（R5）
        self.members[i as usize - 1] = member(i, store);
    }

    /// 文件 (INO, EPOCH) 的 chunk（链 1）的读写 cap（chunk 大小 `self.cs`）。
    pub fn cap(&self) -> Vec<u8> {
        cap::sign(&CAP_KEY, &data_cap(self.cs))
    }

    /// 同 [`Craq::cap`]，另带管理权限位（gc 发 DeleteEpoch 用的那种）。
    pub fn admin_cap(&self) -> Vec<u8> {
        cap::sign(&CAP_KEY, &CapV1 { rights: data_cap(self.cs).rights | cap::RIGHT_ADMIN, ..data_cap(self.cs) })
    }

    pub fn write(&self, idx: u32, off: u32, data: &[u8]) -> Result<u64, ErrCode> {
        let mut b = self.client.pool().get(data.len().max(1), Lane::Normal, Duration::from_secs(1)).unwrap();
        b.set_len(data.len());
        b.as_mut_slice().copy_from_slice(data);
        let s = b.freeze().whole();
        self.client.write(&self.cap(), id(idx), vec![(off, data.len() as u32)], vec![s], false)
    }

    pub fn truncate(&self, idx: u32, new_len: u32) -> Result<u64, ErrCode> {
        self.client.truncate(&self.cap(), id(idx), new_len)
    }

    pub fn read(&self, idx: u32, len: u32) -> Result<Vec<u8>, ErrCode> {
        let into = self.client.pool().get(len as usize, Lane::Normal, Duration::from_secs(1)).unwrap();
        let layout = wmfs_core::meta::schema::records::LayoutV1 {
            epoch: EPOCH,
            chunk_size: self.cs,
            block_size: self.cs.min(65536),
            chains: vec![1],
            replicas: 3,
            codec: 1,
            checksum: 1,
            compress: 0,
            compress_level: 0,
            encrypt: 0,
            encrypt_key: 0,
            inline_max: 0,
        };
        let (b, n) = self.client.read(&self.cap(), &layout, id(idx), 0, len, into, None)?;
        let mut v = b.as_slice()[..n as usize].to_vec();
        v.resize(len as usize, 0);
        Ok(v)
    }

    /// 不变式检查器（9.3）：静止后链 1 的全部 Serving 成员上，每个 chunk 的已提交版本逐字节相同（同版本即同内容，I6）。等最多
    /// `wait` 让 pending 都解决；返回各 chunk 的已提交版本。
    pub fn check(&self, wait: Duration) -> BTreeMap<ChunkId, u64> {
        let deadline = std::time::Instant::now() + wait;
        while self.members.iter().any(|m| !m.node.chunks.pendings(|_, _| true).is_empty())
            && std::time::Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(20));
        }
        let map = self.map();
        let c = &map.chains.iter().find(|(id, _)| *id == 1).unwrap().1;
        let serving: Vec<&Member> =
            c.members.iter().filter(|(_, s)| *s == SERVING).map(|(t, _)| &self.members[*t as usize - 1]).collect();
        let list = |m: &Member| m.store.list_chain(1, None, usize::MAX).unwrap();
        let mut out = BTreeMap::new();
        let first = serving[0];
        for m in &serving {
            assert_eq!(list(m), list(first), "Serving 成员上的 chunk 集合不同");
        }
        for id in list(first) {
            let bytes = first.store.committed_bytes(&id).unwrap();
            for m in &serving {
                assert!(m.store.committed_bytes(&id).unwrap() == bytes, "chunk {id:?} 在各 Serving 成员上不同");
            }
            out.insert(id, version_of(&bytes));
        }
        out
    }

    /// 节点 `n`（target n）上 chunk `idx` 的已提交版本号（不存在 → None）。
    pub fn committed_version(&self, n: u32, idx: u32) -> Option<u64> {
        self.members[n as usize - 1].store.committed_bytes(&id(idx)).map(|b| version_of(&b))
    }
}

/// 在节点 `n`（target n）上造一个 pending 版本（像 head 写完 pending、刚转发给 `succ`、在链版本 1 下）：返回版本号。
pub fn inject_pending(c: &Craq, n: u32, idx: u32, data: &[u8], succ: u32) -> u64 {
    let node = &c.members[n as usize - 1].node;
    let mut b = node.pool.get(data.len(), Lane::Normal, Duration::from_secs(1)).unwrap();
    b.as_mut_slice().copy_from_slice(data);
    let kind = JobKind::Write { extents: vec![(0, data.len() as u32)], sync: false };
    let job = WriteJob { chunk: id(idx), target: n, cap: data_cap(c.cs), kind, bulk: Some(b), reply: None };
    assert!(node.chunks.claim(&id(idx)));
    let p = write::prepare(node, &job).unwrap().unwrap();
    let (last_sent, upstream, successor) = (clock::mono_ms(), None, Some(succ));
    let pw = PendingWrite {
        version: p.version,
        target: n,
        upstream,
        successor,
        chain_ver: 1,
        last_sent,
        sync: false,
        clean: true,
        committed: false,
    };
    node.chunks.set_pending(&id(idx), pw);
    p.version
}

/// 以服务端身份（key 0）给节点 `to` 发一个 PeerCall：回复，或 (错误码, extra)。
pub fn peer_call(c: &Craq, to: u32, op: &PeerOp, bulk: Vec<BufSlice>) -> Result<PeerReply, (ErrCode, u64)> {
    let f = call(c, to, Role::Server, forward::peer_frame(op, bulk))?;
    Ok(decode_body(&f.body).unwrap())
}

/// 以客户端身份（key 2）直接给节点 `to` 发一个 DataCall（不经 DataClient 的换副本与重试）：回复，或 (错误码, extra)。
pub fn data_call(c: &Craq, to: u32, op: DataOp) -> Result<DataReply, (ErrCode, u64)> {
    let hdr = FrameHeader::new(FrameKind::Request, MSG_DATA_CALL, 0, 0);
    let body = encode_body(&DataCall { cap: c.cap(), op });
    let f = call(c, to, Role::Client, OutFrame { hdr, body, bulk: vec![], recv_into: None })?;
    Ok(decode_body(&f.body).unwrap())
}

fn call(c: &Craq, to: u32, role: Role, f: OutFrame) -> Result<InFrame, (ErrCode, u64)> {
    let tr = vec![Arc::new(MemTransport::new()) as Arc<dyn Transport>];
    let key = key(if role == Role::Server { 0 } else { 2 });
    let rpc = RpcClient::new_with_role(MemPool::new(&PoolConfig::small()).unwrap(), tr, key, role, net("mem")).unwrap();
    let ep = Endpoint { node: to, addrs: vec![("mem".into(), c.members[to as usize - 1].addr.clone())] };
    let r = rpc.call(&ep, Family::Data, f, Duration::from_secs(5)).unwrap();
    match r.error() {
        Some(e) => Err((ErrCode(e.code), e.extra)),
        None => Ok(r),
    }
}

/// chunk 文件（固定头与块表在最前面）里的版本号（3.2.4 固定头的 `chunk_version`）。
pub fn version_of(bytes: &[u8]) -> u64 {
    let fixed: &[u8; CHUNK_HEADER_LEN] = bytes[..CHUNK_HEADER_LEN].try_into().unwrap();
    let table = &bytes[CHUNK_HEADER_LEN..CHUNK_HEADER_LEN + table_len_of(fixed)];
    ChunkHeader::decode(fixed, table).unwrap().chunk_version
}

/// 组件测试用的 chunk 大小（64 KiB：一个块）。
pub const CHUNK: u32 = 64 << 10;

pub fn id(idx: u32) -> ChunkId {
    ChunkId { chain: 1, ino: INO, epoch: EPOCH, idx }
}
