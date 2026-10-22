//! `wmfs init` 的本地侧（design.md 3.4.3、3.2.2）：建数据目录、生成集群主密钥与 key 文件、写 node.toml、
//! 写元数据库 `meta/meta.redb` 的 index = 1 初始状态（M3），最后写 NODE。
//!
//! 只有 `cluster.key`（32 字节）是秘密根：key k 的秘密 = `HMAC(K_master, "wmfs-key-v1" ‖ k_be32)`（3.3.2），
//! 所以 key 文件可以随时从 cluster.key 重新算出来，节点之间从不传输秘密。
//!
//! 元数据库的初始状态（3.4.1.9）= `snap_meta = (1, 1)`、`hard_state = {term 1, commit 1}`、`conf_state = {voters: [1]}`、
//! `applied = (1, 1, 1)` 与初始 KV（[`initial_kv`]：根、/home、/scratch、default 类、节点 1、KEYMETA、GLOBAL 计数器……）；
//! 之后加入的节点一律先通过快照追上（M8），保证所有节点初始状态相同。服务端认得的 key 从 M3 起来自 KV 里的 KEYMETA
//! （`meta::engine::KvKeys`）；[`InitKeys`] 是 M1 的临时 key 表，现在只给不带元数据库的组件测试用。
//! **NODE 最后写**，它是 init 的提交点：NODE 存在 = init 已经完成（`wmfs dev` 靠它判断"已初始化"）；init 中途失败或
//! 被杀时目录里没有 NODE，删掉重来即可。实现注记（M3）：TARGET 超级块与 `--target` 在 M5，`--profile`/`--owner` 随用到它们
//! 的里程碑加入（M3 只写 default profile，根目录的属主是运行 init 的用户）。

mod initial_kv;

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

pub use initial_kv::{HOME_INO, InitialKv, SCRATCH_INO, initial_kv};
use serde::{Deserialize, Serialize};
use wmfs_api::hex::{hex, unhex};
use wmfs_api::types::KeyFile;

use crate::base::config::NodeConfig;
use crate::base::types::Role;
use crate::base::{clock, log, rand, sys};
use crate::buf::PoolConfig;
use crate::hooks::traits::KeyLookup;
use crate::meta::raft::storage;
use crate::meta::schema::records::{KeyMetaV1, PluginListV1};
use crate::proto::handshake::derive_secret;

/// key 号的约定（key 0 = 服务端之间、1 = admin、2 = 默认 client）是协议常量，定义在 `proto::handshake`，这里原样转出。
pub use crate::proto::handshake::{ADMIN_KEY, CLIENT_KEY, SERVER_KEY};

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
    pub debug_ops: bool,         // 允许 wmfs debug kv（wmfs dev 打开；wmfs init 默认关闭）
    pub host_id: Option<String>, // wmfs dev 用它给每个节点模拟不同的"主机"（1.1）
    pub plugins: PluginListV1,   // 本节点按 node.toml 装出来的插件清单：写进 GLOBAL plugins（3.4.3、3.7.2）
}

/// 建一个新的单节点数据目录。目录必须不存在或为空（防止覆盖已有集群，坑 P14）。
pub fn init_node(o: &InitOpts) -> Result<(), String> {
    let d = &o.data_dir;
    if std::fs::read_dir(d).is_ok_and(|mut it| it.next().is_some()) {
        return Err(format!("{} 不是空目录：拒绝初始化（防止覆盖已有集群）", d.display()));
    }
    for sub in ["keys", "logs", "run", "meta"] {
        std::fs::create_dir_all(d.join(sub)).map_err(|e| format!("建目录 {} 失败：{e}", d.join(sub).display()))?;
    }
    let (mut cluster_id, mut master) = ([0u8; 16], [0u8; 32]);
    rand::fill(&mut cluster_id);
    rand::fill(&mut master);
    let node = NodeFile { format: 1, cluster_id: hex(&cluster_id), node_id: 1, created_ms: clock::wall_ms() };
    let mut cfg =
        NodeConfig { failpoints_api: o.failpoints_api, debug_ops: o.debug_ops, mem: o.mem, ..NodeConfig::default() };
    cfg.mgmt.listen = format!("{}:{}", o.addr, o.mgmt_port);
    let tcp =
        toml::Table::from_iter([("listen".to_string(), toml::Value::String(format!("{}:{}", o.addr, o.rpc_port)))]);
    cfg.plugin.insert("tcp".into(), tcp);
    cfg.debug.host_id = o.host_id.clone();
    let node_toml = toml::to_string(&node).map_err(|e| format!("NODE 编码失败：{e}"))?;
    let key = |id| KeyFile { key_id: id, secret: derive_secret(&master, id) }.to_line();
    let write = |name: &str, bytes: &[u8]| {
        sys::atomic_write(&d.join(name), bytes).map_err(|e| format!("写 {} 失败：{e}", d.join(name).display()))
    };
    write("cluster.key", &master)?;
    write("node.toml", cfg.to_toml().as_bytes())?;
    write("keys/admin.key", key(ADMIN_KEY).as_bytes())?;
    write("keys/client.key", key(CLIENT_KEY).as_bytes())?;
    write_meta(d, o, &node)?;
    write("NODE", node_toml.as_bytes()) // NODE 放最后：它是提交点（见文件头），之前的文件都已经落盘
}

/// 写 `meta/meta.redb` 的 index = 1 初始状态（3.4.1.9、3.4.3 第 3 步）。根目录的属主 = 运行 init 的用户（本进程的有效
/// uid/gid；数据目录可以是别的用户事先建好的空目录，所以不取它的属主）。
fn write_meta(d: &Path, o: &InitOpts, node: &NodeFile) -> Result<(), String> {
    let path = d.join("meta/meta.redb");
    let read = |p: &str| match std::fs::read_to_string(p) {
        Ok(s) => s.trim().to_string(),
        Err(e) => {
            log::warn("init", format_args!("读不到 {p}（{e}）：NodeRecV1 里的这一项留空"));
            String::new()
        }
    };
    let machine_id = read("/etc/machine-id");
    let i = InitialKv {
        cluster_id: node.cluster_id_bytes()?,
        created_ms: node.created_ms,
        owner: sys::euid_egid(),
        addrs: vec![("tcp".into(), format!("{}:{}", o.addr, o.rpc_port))],
        mgmt_addr: format!("{}:{}", o.addr, o.mgmt_port),
        host_id: o.host_id.clone().unwrap_or_else(|| machine_id.clone()),
        machine_id,
        hostname: read("/proc/sys/kernel/hostname"),
        plugins: o.plugins.clone(),
    };
    let db = redb::Database::create(&path).map_err(|e| format!("建元数据库 {} 失败：{e}", path.display()))?;
    let private = std::fs::Permissions::from_mode(0o600); // 与 NODE、密钥文件一样只给本用户读写（3.2.1 的实现注记）
    std::fs::set_permissions(&path, private).map_err(|e| format!("设置 {} 的权限失败：{e}", path.display()))?;
    storage::bootstrap(&db, &[node.node_id as u64], &initial_kv(&i)).map_err(|e| format!("{}：{e}", path.display()))?;
    // redb 只 fsync 文件内容：新建的 meta.redb 在 meta/ 里的目录项要另外落盘，才能在之后写 NODE（提交点）。
    // meta/ 自己在数据目录里的目录项已经落盘了——写 cluster.key 的 atomic_write 在它建好之后 fsync 过数据目录
    let meta_dir = d.join("meta");
    sys::fsync_dir(&meta_dir).map_err(|e| format!("fsync 目录 {} 失败：{e}", meta_dir.display()))
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

/// M1 的临时 key 表：init 的初始 KEYMETA + 由 cluster.key 派生的秘密。M3 起服务端改读 KV 里的 KEYMETA
/// （`meta::engine::KvKeys`），这个表只给不带元数据库的组件测试（RPC、管理 API）用。
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
mod tests;
