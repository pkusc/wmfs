//! 插件注册表（design.md 3.7.5）：全仓库**唯一**"知道有哪些插件"的文件。core 只认 trait。
//!
//! 新增一个插件 = 新建插件目录（或独立 crate）+ 这里加一行 + `cli/Cargo.toml` 一个 feature，core 一行不改。
//! node.toml 只能启停插件（`[plugins] disable`）与给部署参数（`[plugin.<name>]`），不能改顺序。

use std::sync::Arc;

use wmfs_core::base::config::NodeConfig;
use wmfs_core::hooks::registry::PluginSet;
use wmfs_core::hooks::traits::Transport;

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
    if !cfg.plugins.dylib.is_empty() {
        return Err("node.toml 的 [plugins] dylib：动态库插件 M14 才支持".into());
    }
    s.validate(&cfg.plugin)?;
    Ok(s)
}

/// 一次性命令（`wmfs ping` 等）用的传输：同一组传输插件，只用来连别人、不监听。
pub fn client_transports() -> Result<Vec<Arc<dyn Transport>>, String> {
    build_plugins(&NodeConfig::default()).map(|s| s.transports().to_vec())
}
