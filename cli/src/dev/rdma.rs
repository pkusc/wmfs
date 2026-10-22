//! `wmfs dev up --rdma IP` 的声明式改写（design.md 7.2、3.6.1）：每次 up 都按这次的参数改写节点的 node.toml。

use std::net::Ipv4Addr;
use std::path::Path;

use wmfs_core::base::config::{NodeConfig, PluginTable};
use wmfs_core::base::sys;

use super::ports;

/// `up` 的 `--rdma`（声明式：每次 up 都按这次的参数改写 node.toml，返回它变了没有）。给了 IP → `[plugin.rdma] listen =
/// "IP:19n01"` 与 `max_qps = 32`（dev 的 S 档 128 MiB 只够 32 个 QP 的接收槽，design.md 3.6.1；不写的话插件取服务端默认的
/// 64，启动校验会让节点起不来），`[net] data_scheme = "rdma"`；没给 → 去掉 `[plugin.rdma]`，data_scheme 是 rdma 的改回 tcp
/// （`up --rdma` 起不来时，不带 --rdma 再 up 一次就恢复，不用 `down --purge`）。
pub fn set_rdma(nd: &Path, n: u32, ip: Option<Ipv4Addr>) -> Result<bool, String> {
    let p = nd.join("node.toml");
    let old = NodeConfig::parse(&std::fs::read_to_string(&p).map_err(|e| format!("读 {} 失败：{e}", p.display()))?)?;
    let mut cfg = old.clone();
    match ip {
        Some(ip) => {
            let t: PluginTable = format!("listen = \"{ip}:{}\"\nmax_qps = 32\n", ports(n).0 + 1)
                .parse()
                .map_err(|e| format!("[plugin.rdma] 编码失败：{e}"))?;
            cfg.plugin.insert("rdma".into(), t);
            cfg.net.data_scheme = "rdma".into();
        }
        None => {
            cfg.plugin.remove("rdma");
            if cfg.net.data_scheme == "rdma" {
                cfg.net.data_scheme = "tcp".into();
            }
        }
    }
    if cfg == old {
        return Ok(false);
    }
    sys::atomic_write(&p, cfg.to_toml().as_bytes()).map_err(|e| format!("写 {} 失败：{e}", p.display()))?;
    Ok(true)
}
