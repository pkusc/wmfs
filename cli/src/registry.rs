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
use wmfs_core::hooks::traits::{CacheStore, Transport};
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
    #[cfg(feature = "label-match")]
    s.placement(Arc::new(wmfs_plugins::label_match::LabelMatch::new()));
    #[cfg(feature = "local-fs")]
    s.chunk_store(Arc::new(wmfs_plugins::local_fs::LocalFs::new()));
    #[cfg(feature = "crc32c")]
    s.transform(Arc::new(wmfs_plugins::crc32c::Crc32c::new()));
    #[cfg(feature = "replica")]
    s.codec(Arc::new(wmfs_plugins::replica::Replica::new()));
    #[cfg(feature = "fifo")]
    s.qos(Arc::new(wmfs_plugins::fifo::Fifo::new()));
    #[cfg(feature = "gc")]
    s.bg_task(Box::new(wmfs_plugins::gc::Gc::new()));
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

/// `wmfs mount --cache` 的取值（design.md 4.11、7.2）：`memory[:SIZE]`（默认 512 MiB）→ cache-memory；`none` → 不缓存；
/// `local-nvme:DIR:SIZE` 在 M13。装进一个只有它的 PluginSet 校验 manifest（fail point 声明、预算），再从集合里交出去。
pub fn cache_store(spec: &str) -> Result<Option<Arc<dyn CacheStore>>, String> {
    let (kind, arg) = spec.split_once(':').unwrap_or((spec, ""));
    match kind {
        "none" if arg.is_empty() => Ok(None),
        #[cfg(feature = "cache-memory")]
        "memory" => {
            use wmfs_plugins::cache_memory::{CacheMemory, DEFAULT_CAPACITY};
            let size = if arg.is_empty() { DEFAULT_CAPACITY } else { wmfs_core::base::units::parse_size(arg)? };
            let mut s = PluginSet::new(&[]);
            s.cache_store(Arc::new(CacheMemory::new(size)));
            s.validate(&Default::default())?;
            Ok(s.cache_store_plugin().cloned())
        }
        #[cfg(not(feature = "cache-memory"))]
        "memory" => Err("这个 wmfs 没有编进 cache-memory 插件：用 --cache none".into()),
        "local-nvme" => Err("--cache local-nvme 在 M13 才有（现在可以用 memory[:SIZE] 或 none）".into()),
        _ => Err(format!("--cache 应写成 memory[:SIZE] 或 none：{spec:?}")),
    }
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
