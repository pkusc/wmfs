//! `wmfs dev up --rdma IP` 的声明式改写（design.md 7.2、3.6.1）：每次 up 都按这次的参数改写节点的 node.toml。新节点在 init / join 时就带上
//! rdma（[`transport`]，M15）：rdma 地址才会写进 NodeRecV1.addrs、客户端 `--transport rdma` 才找得到；已有的节点事后才加 `--rdma` 时
//! 只改 node.toml，集群图里的地址不变（3.4.3 的 M15 注记）。

use std::net::Ipv4Addr;
use std::path::Path;

use wmfs_core::base::config::{NodeConfig, PluginTable};
use wmfs_core::base::sys;

use super::ports;

/// 开发节点 n 的 rdma：`[plugin.rdma] listen = "IP:19n01"` 与 `max_qps = 32`（dev 的 S 档 128 MiB 只够 32 个 QP 的接收槽，design.md
/// 3.6.1；不写的话插件取服务端默认的 64，启动校验会让节点起不来）。
pub fn transport(n: u32, ip: Ipv4Addr) -> Result<(String, PluginTable), String> {
    crate::node::rdma_transport(&ip.to_string(), ports(n).0, "max_qps = 32\n")
}

/// `up` 的 `--rdma`（声明式：每次 up 都按这次的参数改写 node.toml，返回它变了没有）。给了 IP → [`transport`] 的 `[plugin.rdma]` 与
/// `[net] data_scheme = "rdma"`；没给 → 去掉 `[plugin.rdma]`，data_scheme 是 rdma 的改回 tcp（`up --rdma` 起不来时，不带 --rdma 再 up
/// 一次就恢复，不用 `down --purge`）。
pub fn set_rdma(nd: &Path, n: u32, ip: Option<Ipv4Addr>) -> Result<bool, String> {
    let p = nd.join("node.toml");
    let old = NodeConfig::parse(&std::fs::read_to_string(&p).map_err(|e| format!("读 {} 失败：{e}", p.display()))?)?;
    let mut cfg = old.clone();
    match ip {
        Some(ip) => {
            let (scheme, t) = transport(n, ip)?;
            cfg.plugin.insert(scheme.clone(), t);
            cfg.net.data_scheme = scheme;
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
