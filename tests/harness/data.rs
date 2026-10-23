//! 数据路径的测试工具（design.md 9.1 的"chunk 格式"一行、9.3 的单节点部分）：不经 RPC，直接在一个 `DataNode` 上调用本体的
//! 写（`data::write::run`）与读（`data::read::read`）。target 可以是内存版的 [`MemTargetStore`]（模拟 fsync 与掉电），也可以是
//! 真实目录上的 local-fs。集群图里只有一条链 1（成员 target 1，Serving），管线装 crc32c、fifo、replica。

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use wmfs_core::base::ErrCode;
use wmfs_core::base::types::ChunkId;
use wmfs_core::buf::{Lane, MemPool, PoolConfig};
use wmfs_core::data::node_hb::MapHolder;
use wmfs_core::data::write::{JobKind, WriteJob};
use wmfs_core::data::{DataNode, Target, read, write};
use wmfs_core::hooks::pipeline::Pipeline;
use wmfs_core::hooks::registry::PluginSet;
use wmfs_core::hooks::traits::TargetStore;
use wmfs_core::meta::cluster::chains::SERVING;
use wmfs_core::meta::schema::records::ChainRecV1;
use wmfs_core::proto::cap::{CapV1, RIGHT_READ, RIGHT_WRITE};
use wmfs_core::proto::peer::ClusterMap;
use wmfs_plugins::crc32c::Crc32c;
use wmfs_plugins::fifo::Fifo;
use wmfs_plugins::replica::Replica;

pub use super::mem_target::MemTargetStore;
use super::meta::CAP_KEY;

pub const CLUSTER: [u8; 16] = [0xC1; 16];
pub const INO: u64 = 18;
pub const EPOCH: u32 = 1;
pub const CHAIN: u32 = 1;

/// 数据面的管线：crc32c（校验和）、fifo（QoS）、replica（codec）。
pub fn data_pipeline() -> Arc<Pipeline> {
    let mut s = PluginSet::new(&[]);
    s.transform(Arc::new(Crc32c::new()));
    s.qos(Arc::new(Fifo::new()));
    s.codec(Arc::new(Replica::new()));
    Arc::new(Pipeline::new(s))
}

/// 测试用的池（≈ 100 MiB，整个测试进程共用一个：建池要 MAP_POPULATE，每个用例都建一个太慢）。
pub fn data_pool() -> MemPool {
    static POOL: OnceLock<MemPool> = OnceLock::new();
    POOL.get_or_init(|| MemPool::new(&PoolConfig { s_mb: 16, m_slots: 16, l_slots: 1, repl_lane: false }).unwrap())
        .clone()
}

/// 只有一条链（链 1 = [target 1]，incarnation 由测试给）的集群图。
pub fn one_chain_map(incarnation: u32) -> ClusterMap {
    let rec = ChainRecV1 {
        ver: 1,
        incarnation,
        class_id: 1,
        replicas: 1,
        members: vec![(1, SERVING)],
        last_serving: None,
        removed: vec![],
    };
    ClusterMap { map_epoch: 1, cluster_id: CLUSTER, chains: vec![(CHAIN, rec)], ..ClusterMap::default() }
}

/// 一个 target（id 1）就是 `store` 的数据节点。
pub fn node_on(store: Arc<dyn TargetStore>, incarnation: u32) -> Arc<DataNode> {
    node_with(Target::new(1, "/mem", store, false, 0), incarnation)
}

/// 只有一个 target `t`（它的 id 必须是 1：链 1 的成员）的数据节点。
pub fn node_with(t: Target, incarnation: u32) -> Arc<DataNode> {
    let map = Arc::new(MapHolder::new());
    map.set(Arc::new(one_chain_map(incarnation)));
    DataNode::new(data_pipeline(), vec![t], data_pool(), CLUSTER, CAP_KEY, map)
}

/// 文件 (INO, EPOCH) 的一张读写 cap（crc32c、不压缩不加密，`chunk_size` 由测试给）。
pub fn cap(chunk_size: u32) -> CapV1 {
    CapV1 {
        ino: INO,
        epoch: EPOCH,
        rights: RIGHT_READ | RIGHT_WRITE,
        session: 0,
        tenant: 0,
        job_id: 0,
        chains: vec![CHAIN],
        class_id: 1,
        qos_id: 0,
        chunk_size,
        block_size: chunk_size.min(65536),
        checksum: 1,
        compress: 0,
        compress_level: 0,
        encrypt: 0,
        encrypt_key: 0,
        expires_ms: u64::MAX / 2,
    }
}

pub fn chunk(idx: u32) -> ChunkId {
    ChunkId { chain: CHAIN, ino: INO, epoch: EPOCH, idx }
}

fn job(node: &DataNode, cs: u32, idx: u32, kind: JobKind, data: &[u8]) -> WriteJob {
    let bulk = (!data.is_empty()).then(|| {
        let mut b = node.pool.get(data.len(), Lane::Normal, Duration::from_secs(1)).unwrap();
        b.as_mut_slice().copy_from_slice(data);
        b
    });
    WriteJob { chunk: chunk(idx), target: 1, cap: cap(cs), kind, bulk, reply: None }
}

/// 写一些区间（`data` = 各段依次拼接）；返回新版本号。测试是单线程的，所以直接调用 `write::run`（调用方独占这个 chunk）。
pub fn write_at(node: &DataNode, cs: u32, idx: u32, extents: &[(u32, u32)], data: &[u8]) -> Result<u64, ErrCode> {
    let j = job(node, cs, idx, JobKind::Write { extents: extents.to_vec(), sync: false }, data);
    let r = write::run(node, &j);
    node.chunks.finish(&j.chunk);
    r
}

/// 截断到 `new_len`；返回新版本号（chunk 不存在 → 0）。
pub fn truncate(node: &DataNode, cs: u32, idx: u32, new_len: u32) -> Result<u64, ErrCode> {
    let j = job(node, cs, idx, JobKind::Truncate { new_len }, &[]);
    let r = write::run(node, &j);
    node.chunks.finish(&j.chunk);
    r
}

/// 读 `[off, off + len)`，`logical_len` 之后补 0（客户端的做法）；返回 (内容, 版本号)。
pub fn read_at(node: &DataNode, idx: u32, off: u32, len: u32) -> Result<(Vec<u8>, u64), ErrCode> {
    let t = node.targets.get(&1).unwrap();
    let (b, n, v) = read::read(node, t, &chunk(idx), off, len)?;
    let mut out = b.as_slice()[..n as usize].to_vec();
    out.resize(len as usize, 0);
    Ok((out, v))
}

/// 让本体对这些 chunk fsync（客户端 fsync 时的 SyncChunks）。
pub fn sync(node: &DataNode, idxs: &[u32]) {
    let ids: Vec<ChunkId> = idxs.iter().map(|i| chunk(*i)).collect();
    node.targets.get(&1).unwrap().store.sync(&ids).unwrap();
}
