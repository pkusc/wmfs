//! 找到元数据 leader 并建好 RPC 客户端（`wmfs debug kv`、`wmfs fs`、`wmfs dev up` 共用，design.md 7.2）。
//!
//! 依次问 `--cluster` / `WMFS_CLUSTER` 里每个管理地址的 `/api/v1/health`，直到有人说出 leader 的 rpc 地址（最多 30 s：
//! 刚启动的节点要先选出 leader）；自称 `candidate`（已当选、还没应用本任期的空条目）的节点再等一会儿，免得第一个请求碰上
//! `NOT_READY`、打出一串重试日志。30 s 后还是 `candidate`（选不出就绪的 leader，例如没有多数派）：报错里给出看日志的提示——插件清单
//! 不一致的节点 M8 起会以退出码 3 退出（见 server.log），不会一直停在 candidate。

use std::sync::Arc;
use std::time::Duration;

use wmfs_core::api::types::{Health, KeyFile, NodeAddrs};
use wmfs_core::base::clock;
use wmfs_core::base::config::NetConfig;
use wmfs_core::buf::{MemPool, PoolConfig};
use wmfs_core::net::client::{Endpoint, RpcClient};
use wmfs_mgr::api::ApiClient;

use crate::registry;

/// 找 leader 的总期限。
const FIND_TIMEOUT: Duration = Duration::from_secs(30);

/// 连到元数据 leader 的 RPC 客户端。
pub struct Leader {
    pub rpc: Arc<RpcClient>,
    pub ep: Endpoint,
    pub addrs: NodeAddrs, // leader 的 rpc 与管理地址
}

/// 问 health 找到 leader，建一个用 `key` 认证的 RPC 客户端（小内存池：CLI 不传大块数据）。
pub fn connect(cluster: &[String], key: KeyFile) -> Result<Leader, String> {
    let addrs = find_leader(cluster, &key)?;
    let pool = MemPool::new(&PoolConfig::small()).map_err(|e| format!("内存池创建失败：{e}"))?;
    let rpc = RpcClient::new(pool, registry::client_transports()?, key, NetConfig::default())
        .map_err(|e| format!("建 RPC 客户端失败：{e}"))?;
    let ep = Endpoint { node: addrs.node_id, addrs: addrs.addrs.clone() };
    Ok(Leader { rpc, ep, addrs })
}

/// 依次问每个管理地址的 health，直到找到 leader（见文件头）。实现注记（M8）：只信"自己说自己是就绪 leader"的节点；follower 说的
/// leader 先问一下它自己的 health（刚被 kill 的 leader 在 follower 眼里还会是 leader，直到选出新的）。
fn find_leader(cluster: &[String], key: &KeyFile) -> Result<NodeAddrs, String> {
    let deadline = clock::mono_ms() + FIND_TIMEOUT.as_millis() as u64;
    let health = |a: &str| ApiClient::new(vec![a.to_string()], key.clone()).get::<Health>("/api/v1/health");
    loop {
        let (mut last, mut candidate) = (String::new(), false);
        for a in cluster {
            match health(a) {
                Ok(Health { leader: Some(l), meta_role, .. }) if meta_role == "leader" => return Ok(l),
                Ok(Health { leader: Some(l), meta_role, .. }) if meta_role != "candidate" => {
                    if health(&l.mgmt_addr).is_ok_and(|h| h.meta_role == "leader") {
                        return Ok(l);
                    }
                    last = format!("{a} 说 leader 是 node{}，但它没有回答自己是就绪的 leader", l.node_id);
                }
                Ok(h) => {
                    candidate |= h.meta_role == "candidate";
                    last = format!("{a} 的元数据角色是 {}，还不知道 leader", h.meta_role);
                }
                Err(e) => last = format!("{a}：{e}"),
            }
        }
        if clock::mono_ms() > deadline {
            let fix = if candidate {
                "节点一直没有成为就绪的 leader：多数派的节点在运行吗？插件清单不一致的节点会以退出码 3 退出（见 server.log；\
                 旧里程碑建的开发集群请 wmfs dev down --purge 之后重新 wmfs dev up）"
            } else {
                "可以先 wmfs dev wait leader"
            };
            return Err(format!("{} s 内没有找到元数据 leader（{last}）；{fix}", FIND_TIMEOUT.as_secs()));
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}
