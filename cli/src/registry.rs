//! 插件注册表（design.md 3.7.5）：全仓库**唯一**"知道有哪些插件"的文件。core 只认 trait。
//!
//! 新增一个插件 = 新建插件目录（或独立 crate）+ 这里加一行 + `cli/Cargo.toml` 一个 feature，core 一行不改。
//! node.toml 只能启停插件（`[plugins] disable`）与给部署参数（`[plugin.<name>]`），不能改顺序。

#[cfg(feature = "local-fs")]
use std::path::{Path, PathBuf};
use std::sync::Arc;

use wmfs_core::base::config::NodeConfig;
#[cfg(feature = "local-fs")]
use wmfs_core::base::types::ChunkId;
use wmfs_core::hooks::registry::PluginSet;
use wmfs_core::hooks::traits::{CacheStore, CodecPlugin, Transport};
use wmfs_core::meta::schema::records::PluginListV1;

/// 按 node.toml 装出本节点的插件集合并校验（altitude、唯一性、abi_version、预算、fail point 声明、配置键）。
pub fn build_plugins(cfg: &NodeConfig) -> Result<PluginSet, String> {
    let mut s = PluginSet::new(&cfg.plugins.disable);
    #[cfg(feature = "tcp")]
    s.transport(Arc::new(wmfs_plugins::tcp::TcpTransport::new(&cfg.plugin_cfg("tcp"), cfg.net.write_timeout_ms)?));
    #[cfg(feature = "rdma")]
    s.transport(Arc::new(wmfs_plugins::rdma::RdmaTransport::new(&cfg.plugin_cfg("rdma"), cfg.net.write_timeout_ms)?));
    #[cfg(feature = "mem")]
    s.transport(Arc::new(plugin_mem_transport::MemTransport::new())); // 独立 crate 示例
    #[cfg(feature = "shared-key")]
    s.authn(Arc::new(wmfs_plugins::shared_key::SharedKey::new()));
    #[cfg(feature = "posix-mode")]
    s.authz(Arc::new(wmfs_plugins::posix_mode::PosixMode::new()));
    #[cfg(feature = "posix-acl")]
    s.authz(Arc::new(wmfs_plugins::posix_acl::PosixAcl::new()));
    #[cfg(feature = "path-caps")]
    s.authz(Arc::new(wmfs_plugins::path_caps::PathCaps::new()));
    #[cfg(feature = "quota")]
    s.quota(Arc::new(wmfs_plugins::quota::Quota::new()));
    #[cfg(feature = "label-match")]
    s.placement(Arc::new(wmfs_plugins::label_match::LabelMatch::new()));
    #[cfg(feature = "local-fs")]
    s.chunk_store(Arc::new(wmfs_plugins::local_fs::LocalFs::new()));
    #[cfg(feature = "crc32c")]
    s.transform(Arc::new(wmfs_plugins::crc32c::Crc32c::new()));
    #[cfg(feature = "xxh3")]
    s.transform(Arc::new(wmfs_plugins::xxh3::Xxh3::new()));
    #[cfg(feature = "blake3")]
    s.transform(Arc::new(wmfs_plugins::blake3::Blake3::new()));
    #[cfg(feature = "lz4")]
    s.transform(Arc::new(wmfs_plugins::lz4::Lz4::new()));
    #[cfg(feature = "zstd")]
    s.transform(Arc::new(wmfs_plugins::zstd::Zstd::new()));
    #[cfg(feature = "aes-gcm")]
    s.transform(Arc::new(wmfs_plugins::aes_gcm::AesGcm::new()));
    #[cfg(feature = "replica")]
    s.codec(Arc::new(wmfs_plugins::replica::Replica::new()));
    #[cfg(feature = "mclock")]
    s.qos(Arc::new(wmfs_plugins::mclock::Mclock::new()));
    // qos 只允许一个活跃（3.7.2）：fifo 是 mclock 被停用（minimal profile 的 disable = ["quota", "mclock"]）时的那一个（实现注记（M13））
    #[cfg(feature = "fifo")]
    if s.qos_plugins().is_empty() {
        s.qos(Arc::new(wmfs_plugins::fifo::Fifo::new()));
    }
    #[cfg(feature = "gc")]
    s.bg_task(Box::new(wmfs_plugins::gc::Gc::new()));
    #[cfg(feature = "gc")]
    s.bg_task(Box::new(wmfs_plugins::gc::GcLocal::new())); // gc 的数据节点部分（本地孤儿扫描，M11）
    #[cfg(feature = "reconciler")]
    s.bg_task(Box::new(wmfs_plugins::reconciler::Reconciler::new()));
    #[cfg(feature = "scrub")]
    s.bg_task(Box::new(wmfs_plugins::scrub::Scrub::new()));
    #[cfg(feature = "trash-expire")]
    s.bg_task(Box::new(wmfs_plugins::trash_expire::TrashExpire::new()));
    #[cfg(feature = "ttl")]
    s.bg_task(Box::new(wmfs_plugins::ttl::Ttl::new()));
    #[cfg(feature = "tiering")]
    s.bg_task(Box::new(wmfs_plugins::tiering::Tiering::new()));
    #[cfg(feature = "rebalance")]
    s.bg_task(Box::new(wmfs_plugins::rebalance::Rebalance::new()));
    s.meta_engine(&wmfs_core::meta::engine::MANIFEST); // embedded-kv：代码在 core（元数据 Raft 是契约，4.10）
    if !cfg.plugins.dylib.is_empty() {
        return Err("node.toml 的 [plugins] dylib：动态库插件 M14 才支持".into());
    }
    s.validate(&cfg.plugin)?;
    Ok(s)
}

/// `id` 的已提交版本在 local-fs target `root` 里的路径（`wmfs debug chunk-path` 等在本机直接找文件用；没编进 local-fs 就没有这几个
/// 子命令）。
#[cfg(feature = "local-fs")]
pub fn chunk_path(root: &Path, id: &ChunkId) -> PathBuf {
    wmfs_plugins::local_fs::committed_path(root, id)
}

/// `wmfs mount --cache` 的取值（design.md 4.11、7.2）：`memory[:SIZE]`（默认 512 MiB）→ cache-memory；`local-nvme:DIR[:SIZE]`（默认
/// 50 GiB，M13）→ cache-local-nvme；`none` → 不缓存。
pub fn cache_store(spec: &str) -> Result<Option<Arc<dyn CacheStore>>, String> {
    let (kind, arg) = spec.split_once(':').unwrap_or((spec, ""));
    match kind {
        "none" if arg.is_empty() => Ok(None),
        #[cfg(feature = "cache-memory")]
        "memory" => {
            use wmfs_plugins::cache_memory::{CacheMemory, DEFAULT_CAPACITY};
            validated(Arc::new(CacheMemory::new(size_or(arg, DEFAULT_CAPACITY)?)))
        }
        #[cfg(not(feature = "cache-memory"))]
        "memory" => Err("这个 wmfs 没有编进 cache-memory 插件：用 --cache none".into()),
        #[cfg(feature = "cache-local-nvme")]
        "local-nvme" if !arg.is_empty() => {
            use wmfs_plugins::cache_local_nvme::{DEFAULT_CAPACITY, LocalNvme};
            let (dir, sz) = dir_and_size(arg);
            validated(Arc::new(LocalNvme::new(std::path::Path::new(dir), size_or(sz, DEFAULT_CAPACITY)?)?))
        }
        #[cfg(not(feature = "cache-local-nvme"))]
        "local-nvme" => Err("这个 wmfs 没有编进 cache-local-nvme 插件：用 --cache memory 或 none".into()),
        _ => Err(format!("--cache 应写成 memory[:SIZE]、local-nvme:DIR[:SIZE] 或 none：{spec:?}")),
    }
}

/// 装进一个只有它的 PluginSet 校验 manifest（fail point 声明、预算），再从集合里交出去。
#[cfg(any(feature = "cache-memory", feature = "cache-local-nvme"))]
fn validated(p: Arc<dyn CacheStore>) -> Result<Option<Arc<dyn CacheStore>>, String> {
    let mut s = PluginSet::new(&[]);
    s.cache_store(p);
    s.validate(&Default::default())?;
    Ok(s.cache_store_plugin().cloned())
}

/// 缓存大小：空 = 默认值。
#[cfg(any(feature = "cache-memory", feature = "cache-local-nvme"))]
fn size_or(s: &str, default: u64) -> Result<u64, String> {
    if s.is_empty() { Ok(default) } else { wmfs_core::base::units::parse_size(s) }
}

/// `local-nvme:` 后面的 `DIR[:SIZE]`：最后一个冒号后面以数字开头才是 SIZE（目录名里可以有冒号）。
#[cfg(feature = "cache-local-nvme")]
fn dir_and_size(arg: &str) -> (&str, &str) {
    match arg.rsplit_once(':') {
        Some((dir, size)) if size.starts_with(|c: char| c.is_ascii_digit()) => (dir, size),
        _ => (arg, ""),
    }
}

/// 客户端读副本的顺序用的 codec 插件（replica 的 `read_order`，ADR-0040 第 10 条，M13；它的 manifest 由每个节点的 `build_plugins` 校验，
/// 调用照样经 3.7.4 的包装）。
pub fn read_codec() -> Option<Arc<dyn CodecPlugin>> {
    #[cfg(feature = "replica")]
    return Some(Arc::new(wmfs_plugins::replica::Replica::new()));
    #[cfg(not(feature = "replica"))]
    None
}

/// 一次性命令（`wmfs ping` 等）用的传输：同一组传输插件，只用来连别人、不监听。
pub fn client_transports() -> Result<Vec<Arc<dyn Transport>>, String> {
    build_plugins(&NodeConfig::default()).map(|s| s.transports().to_vec())
}

/// init 写进 GLOBAL plugins 的集群插件清单（3.4.3 第 3 步、3.7.2）= 按 init 写出的 node.toml 装出来的插件清单：init 写的
/// node.toml 只改了监听地址等部署参数与 profile 停用的插件（`disable`），清单里也不含传输插件（`plugin_list` 只列会影响策略的
/// 钩子），所以用默认配置加上 `disable` 算就是对的。`wmfs init` 与 `wmfs dev up` 都用它。
pub fn init_plugin_list(disable: &[String]) -> Result<PluginListV1, String> {
    let mut cfg = NodeConfig::default();
    cfg.plugins.disable = disable.to_vec();
    build_plugins(&cfg).map(|s| s.plugin_list())
}
