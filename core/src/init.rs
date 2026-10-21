//! `wmfs init` 的本地侧（design.md 3.4.3、3.2.2）：建数据目录、生成集群主密钥与 key 文件、写 NODE 与 node.toml。
//!
//! M1 只生成目录、密钥与 node.toml（roadmap M1）；元数据的初始状态（M3）、target 超级块（M5）随里程碑加入。
//! 只有 `cluster.key`（32 字节）是秘密根：key k 的秘密 = `HMAC(K_master, "wmfs-key-v1" ‖ k_be32)`（3.3.2），
//! 所以 key 文件可以随时从 cluster.key 重新算出来，节点之间从不传输秘密。
//!
//! 实现注记（M1）：还没有元数据 KV（M3），服务端认得的 key 就是 init 写进 KEYMETA 的初始值（[`InitKeys`]：
//! key 1 = admin、key 2 = 默认 client，tenant 0，uid 范围 0..=u32::MAX，不 squash）；M3/M4 起改为读复制状态里的 KEYMETA。
//! **NODE 最后写**，它是 init 的提交点：NODE 存在 = init 已经完成（`wmfs dev` 靠它判断"已初始化"）；init 中途失败或
//! 被杀时目录里没有 NODE，删掉重来即可。

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use wmfs_api::hex::{hex, unhex};
use wmfs_api::types::KeyFile;

use crate::base::config::NodeConfig;
use crate::base::types::Role;
use crate::base::{clock, rand, sys};
use crate::buf::PoolConfig;
use crate::hooks::traits::KeyLookup;
use crate::meta::schema::records::KeyMetaV1;
use crate::proto::handshake::derive_secret;

/// key 0 固定是服务端之间用的（角色 Server，不在 KEYMETA 里，3.3.2）。
pub const SERVER_KEY: u32 = 0;
/// init 生成的两把 key（3.2.7：key 1 = admin，key 2 = 默认 client）。
pub const ADMIN_KEY: u32 = 1;
pub const CLIENT_KEY: u32 = 2;

/// `<data_dir>/NODE`（TOML，atomic_write）：format = 1, cluster_id = "<32 位 hex>", node_id, created_ms。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NodeFile {
    pub format: u32,
    pub cluster_id: String,
    pub node_id: u32,
    pub created_ms: u64,
}

impl NodeFile {
    pub fn read(data_dir: &Path) -> Result<NodeFile, String> {
        let p = data_dir.join("NODE");
        let text = std::fs::read_to_string(&p)
            .map_err(|e| format!("读 {} 失败：{e}（这是 wmfs init 建的数据目录吗？）", p.display()))?;
        let n: NodeFile = toml::from_str(&text).map_err(|e| format!("{} 格式不对：{e}", p.display()))?;
        if n.format != 1 {
            return Err(format!("{} 的 format = {}，本版本只认识 1", p.display(), n.format));
        }
        n.cluster_id_bytes()?;
        Ok(n)
    }

    pub fn cluster_id_bytes(&self) -> Result<[u8; 16], String> {
        let mut b = [0u8; 16];
        if !unhex(&self.cluster_id, &mut b) {
            return Err(format!("NODE 里的 cluster_id 应是 32 位 hex：{:?}", self.cluster_id));
        }
        Ok(b)
    }
}

/// init 的参数（CLI 的 `wmfs init` 与 `wmfs dev up` 都用它）。
pub struct InitOpts {
    pub data_dir: PathBuf,
    pub addr: String, // 监听地址（不带端口），例如 127.0.0.1
    pub rpc_port: u16,
    pub mgmt_port: u16,
    pub mem: PoolConfig,
    pub failpoints_api: bool,
    pub host_id: Option<String>, // wmfs dev 用它给每个节点模拟不同的"主机"（1.1）
}

/// 建一个新的单节点数据目录。目录必须不存在或为空（防止覆盖已有集群，坑 P14）。
pub fn init_node(o: &InitOpts) -> Result<(), String> {
    let d = &o.data_dir;
    if std::fs::read_dir(d).is_ok_and(|mut it| it.next().is_some()) {
        return Err(format!("{} 不是空目录：拒绝初始化（防止覆盖已有集群）", d.display()));
    }
    for sub in ["keys", "logs", "run"] {
        std::fs::create_dir_all(d.join(sub)).map_err(|e| format!("建目录 {} 失败：{e}", d.join(sub).display()))?;
    }
    let (mut cluster_id, mut master) = ([0u8; 16], [0u8; 32]);
    rand::fill(&mut cluster_id);
    rand::fill(&mut master);
    let node = NodeFile { format: 1, cluster_id: hex(&cluster_id), node_id: 1, created_ms: clock::wall_ms() };
    let mut cfg = NodeConfig { failpoints_api: o.failpoints_api, mem: o.mem, ..NodeConfig::default() };
    cfg.mgmt.listen = format!("{}:{}", o.addr, o.mgmt_port);
    let tcp =
        toml::Table::from_iter([("listen".to_string(), toml::Value::String(format!("{}:{}", o.addr, o.rpc_port)))]);
    cfg.plugin.insert("tcp".into(), tcp);
    cfg.debug.host_id = o.host_id.clone();
    let node_toml = toml::to_string(&node).map_err(|e| format!("NODE 编码失败：{e}"))?;
    let key = |id| KeyFile { key_id: id, secret: derive_secret(&master, id) }.to_line();
    // NODE 放最后：它是提交点（见文件头），之前的文件都已经 atomic_write 落盘
    let files = [
        ("cluster.key", master.to_vec()),
        ("node.toml", cfg.to_toml().into_bytes()),
        ("keys/admin.key", key(ADMIN_KEY).into_bytes()),
        ("keys/client.key", key(CLIENT_KEY).into_bytes()),
        ("NODE", node_toml.into_bytes()),
    ];
    for (name, bytes) in files {
        sys::atomic_write(&d.join(name), &bytes).map_err(|e| format!("写 {} 失败：{e}", d.join(name).display()))?;
    }
    Ok(())
}

/// 读 `<data_dir>/cluster.key`（32 字节）。
pub fn read_master(data_dir: &Path) -> Result<[u8; 32], String> {
    let p = data_dir.join("cluster.key");
    let b = std::fs::read(&p).map_err(|e| format!("读 {} 失败：{e}", p.display()))?;
    b.try_into().map_err(|_| format!("{} 应正好 32 字节", p.display()))
}

/// init 写进 KEYMETA 的初始值（3.4.3 第 3 步）：key 1 = admin、key 2 = 默认 client。
pub fn initial_keymeta(created_ms: u64) -> Vec<(u32, KeyMetaV1)> {
    let k = |name: &str, role: Role| KeyMetaV1 {
        name: name.into(),
        role: role.as_u8(),
        tenant: 0,
        uid_ranges: vec![(0, u32::MAX)],
        root_squash: false,
        path_caps: vec![],
        revoked: false,
        created_ms,
    };
    vec![(ADMIN_KEY, k("admin", Role::Admin)), (CLIENT_KEY, k("client", Role::Client))]
}

/// M1 的 key 表：init 的初始 KEYMETA + 由 cluster.key 派生的秘密（见文件头的实现注记）。
pub struct InitKeys {
    master: [u8; 32],
    metas: Vec<(u32, KeyMetaV1)>,
}

impl InitKeys {
    pub fn new(master: [u8; 32], created_ms: u64) -> InitKeys {
        InitKeys { master, metas: initial_keymeta(created_ms) }
    }
}

impl KeyLookup for InitKeys {
    fn secret(&self, key_id: u32) -> Option<[u8; 32]> {
        let known = key_id == SERVER_KEY || self.metas.iter().any(|(k, _)| *k == key_id);
        known.then(|| derive_secret(&self.master, key_id))
    }
    fn meta(&self, key_id: u32) -> Option<KeyMetaV1> {
        self.metas.iter().find(|(k, _)| *k == key_id).map(|(_, m)| m.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_writes_files_and_refuses_non_empty_dir() {
        let d = crate::base::test_dir("init");
        let o = InitOpts {
            data_dir: d.join("n1"),
            addr: "127.0.0.1".into(),
            rpc_port: 19100,
            mgmt_port: 19180,
            mem: PoolConfig::dev_server(),
            failpoints_api: true,
            host_id: Some("host-1".into()),
        };
        init_node(&o).unwrap();
        let n = NodeFile::read(&o.data_dir).unwrap();
        assert_eq!((n.format, n.node_id), (1, 1));
        let cfg = NodeConfig::parse(&std::fs::read_to_string(o.data_dir.join("node.toml")).unwrap()).unwrap();
        assert_eq!(cfg.mgmt.listen, "127.0.0.1:19180");
        assert_eq!(cfg.plugin_cfg("tcp").get("listen").and_then(|v| v.as_str()), Some("127.0.0.1:19100"));
        assert!(cfg.failpoints_api && cfg.mem == PoolConfig::dev_server());
        let master = read_master(&o.data_dir).unwrap();
        let admin = KeyFile::parse(&std::fs::read_to_string(o.data_dir.join("keys/admin.key")).unwrap()).unwrap();
        assert_eq!(admin, KeyFile { key_id: 1, secret: derive_secret(&master, 1) });
        let keys = InitKeys::new(master, n.created_ms);
        assert_eq!(keys.secret(2), Some(derive_secret(&master, 2)));
        assert_eq!(keys.meta(2).map(|m| m.role), Some(Role::Client.as_u8()));
        assert!(keys.secret(0).is_some() && keys.meta(0).is_none(), "key 0 不在 KEYMETA 里");
        assert!(keys.secret(3).is_none() && keys.meta(3).is_none());
        assert!(init_node(&o).unwrap_err().contains("不是空目录"), "第二次 init 必须拒绝");
        std::fs::remove_dir_all(&d).unwrap();
    }
}
