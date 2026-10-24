//! 不变式检查器（design.md 9.3："静止后所有 Serving 副本对每个 chunk 的已提交版本与内容哈希相同"，M9 验收在 2 GiB 用例之后跑）：
//! 取一张集群图（链、target 的路径），在本机直接读每条链每个 Serving 成员的 target 目录（开发集群的 target 都在本机，local-fs 的路径
//! 规则），比较每个 chunk 的已提交版本号与内容（packed 形式的 crc32c——与 FetchChunk、完整转发发的字节相同）。
//! "静止"：各 target 的 `p/` 里没有 pending 文件（等最多 30 s）。
//!
//! 对着一个跑着的开发集群：`#[ignore]`，由 `scripts/accept.d/m9.sh` 跑（环境变量 `WMFS_DEV_DIR`）。

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use wmfs_core::api::types::KeyFile;
use wmfs_core::base::types::ChunkId;
use wmfs_core::buf::{MemPool, PoolConfig};
use wmfs_core::client::meta_client::fetch_map;
use wmfs_core::data::packed;
use wmfs_core::hooks::traits::ChunkStore;
use wmfs_core::meta::cluster::chains::SERVING;
use wmfs_core::net::client::{Endpoint, RpcClient};
use wmfs_plugins::local_fs::LocalFs;
use wmfs_plugins::tcp::TcpTransport;

fn pending_files(root: &Path) -> usize {
    std::fs::read_dir(root.join("p")).map_or(0, |d| d.count())
}

#[test]
#[ignore = "要一个跑着的开发集群：由 scripts/accept.sh M9 跑"]
fn every_serving_replica_has_the_same_committed_versions_and_bytes() {
    let dir = PathBuf::from(std::env::var("WMFS_DEV_DIR").expect("WMFS_DEV_DIR"));
    let key = KeyFile::parse(&std::fs::read_to_string(dir.join("n1/keys/admin.key")).unwrap()).unwrap();
    let pool = MemPool::new(&PoolConfig::small()).unwrap();
    let t = TcpTransport::new(&Default::default(), 5000).unwrap();
    let rpc = RpcClient::new(pool.clone(), vec![std::sync::Arc::new(t)], key, Default::default()).unwrap();
    let seeds: Vec<Endpoint> =
        (1..=3).map(|n| Endpoint { node: n, addrs: vec![("tcp".into(), format!("127.0.0.1:19{n}00"))] }).collect();
    let map = fetch_map(rpc, &seeds, Duration::from_secs(30)).expect("取集群图");
    let path = |t: u32| PathBuf::from(&map.targets.iter().find(|(id, _)| *id == t).unwrap().1.path);
    let t0 = Instant::now();
    while map.targets.iter().any(|(id, _)| pending_files(&path(*id)) > 0) {
        assert!(t0.elapsed() < Duration::from_secs(30), "30 s 内还有 pending 文件：没有静止");
        std::thread::sleep(Duration::from_millis(100));
    }
    let (mut chunks, mut bytes) = (0u64, 0u64);
    for (cid, c) in &map.chains {
        let serving: Vec<u32> = c.members.iter().filter(|(_, s)| *s == SERVING).map(|(t, _)| *t).collect();
        let mut views: Vec<BTreeMap<ChunkId, (u64, u32)>> = Vec::new();
        for t in &serving {
            let store = LocalFs::new().open_target(&path(*t), *t).unwrap();
            let mut v = BTreeMap::new();
            for id in store.list_chain(*cid, None, usize::MAX).unwrap() {
                let mut rd = store.open_committed(&id).unwrap().expect("列出来的 chunk 打不开");
                let (b, h) = packed::pack(&pool, &mut |off, buf| rd.read_at(off, buf)).unwrap();
                bytes += b.len() as u64;
                v.insert(id, (h.chunk_version, crc32c::crc32c(b.as_slice())));
            }
            views.push(v);
        }
        for (i, v) in views.iter().enumerate().skip(1) {
            assert!(*v == views[0], "链 {cid}：target {} 与 target {} 的已提交版本或内容不同", serving[i], serving[0]);
        }
        chunks += views.first().map_or(0, |v| v.len() as u64);
    }
    println!("{{\"consistent\":true,\"chains\":{},\"chunks\":{chunks},\"bytes_compared\":{bytes}}}", map.chains.len());
}
