//! replica：codec 插件（design.md 4.7，altitude 610，codec 编号 1）——数据 ↔ 分片：链上每个成员存一份完整的数据。
//!
//! `shards(n)` = n 个 `Whole`；`read_order` = Serving 成员里同节点的优先，其余按 `hash(chunk)` 轮转（把读分散到各副本）。
//! 链式复制协议本身在本体（契约，3.4.2），codec 只决定"分几片、读哪个"。实现注记（M5）：单节点链长 1，数据节点写之前用
//! `shards` 核对"本成员存整块"；客户端按 `read_order` 选副本在 M9 有多副本时接入。指标 `wmfs_replica_read_orders_total`；
//! fail point `plugin.replica.codec`（本体包在调用外层）。

use std::sync::Arc;

use wmfs_core::api::metrics::{self, Counter};
use wmfs_core::base::types::ChunkId;
use wmfs_core::hooks::manifest::{CORE_ABI_VERSION, Hook, Manifest};
use wmfs_core::hooks::traits::{CODEC_REPLICA, CodecPlugin, ShardSpec};

pub static MANIFEST: Manifest = Manifest {
    name: "replica",
    version: "0.1.0",
    hooks: &[Hook::Codec],
    altitude: 610,
    hot_path: true,
    config_schema: "",
    abi_version: CORE_ABI_VERSION,
    attrs: &["replicas"],
    budget_ns: 2_000, // 3.7.4：codec 2 µs
    fail_points: &["plugin.replica.codec"],
};

pub struct Replica {
    orders: Arc<Counter>,
}

impl Replica {
    pub fn new() -> Replica {
        Replica {
            orders: metrics::counter("wmfs_replica_read_orders_total", "replica 排过的读副本顺序数", &[])
        }
    }
}

impl Default for Replica {
    fn default() -> Replica {
        Replica::new()
    }
}

impl CodecPlugin for Replica {
    fn manifest(&self) -> &'static Manifest {
        &MANIFEST
    }

    fn codec_id(&self) -> u8 {
        CODEC_REPLICA
    }

    fn shards(&self, chain_len: usize) -> Vec<ShardSpec> {
        vec![ShardSpec::Whole; chain_len]
    }

    fn read_order(&self, serving: &[(u32, u32)], prefer_node: Option<u32>, chunk: &ChunkId) -> Vec<u32> {
        self.orders.inc();
        let n = serving.len().max(1);
        let start = (chunk.ino as usize ^ chunk.idx as usize ^ chunk.epoch as usize) % n;
        let mut v: Vec<(u32, u32)> = serving.iter().cycle().skip(start).take(serving.len()).copied().collect();
        v.sort_by_key(|(_, node)| Some(*node) != prefer_node); // 稳定排序：同节点的在前，其余保持轮转后的顺序
        v.into_iter().map(|(t, _)| t).collect()
    }
}

#[cfg(test)]
mod tests;
