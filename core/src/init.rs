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
//! 被杀时目录里没有 NODE，删掉重来即可。实现注记（M3）：`--profile`/`--owner` 随用到它们的里程碑加入（只写 default
//! profile，根目录的属主是运行 init 的用户）。实现注记（M6）：`--profile default|minimal`（[`Profile`]，3.4.3 的表）：minimal 的
//! default 类 replica = 1、不写 replicas 属性，node.toml 停用 quota 与 mclock；M9 加了 nas-safe（M12 起是完整的 `/home replicas=3,trash=7d,
//! audit=on`）。实现注记（M15）：数据族另走一个传输（hpc-rdma 的 rdma，[`InitOpts::data_transport`]）时，它的 `[plugin.<scheme>]` 与
//! `[net] data_scheme` 随 node.toml 一起写（NODE 之前），它的 listen 与 tcp 的一起写进 NodeRecV1(1).addrs 公布（[`Listen::addrs`]）。实现注记（M4）：`--target PATH[:label,label]` 写进 node.toml 的 `[[target]]`
//! 与初始 KV 的 TargetRecV1（id 1..=n，gen_chains 用它们生成链）；同一个路径给两次、或一个 target 在另一个里面 → 拒绝
//! （两个 target 的超级块与 chunk 目录会互相踩到）。实现注记（M5）：init 给每个 target 建目录、写 TARGET 超级块（3.2.3：
//! cluster_id、target_id = 1..=n、node_id 1，`atomic_write`，在 NODE 之前）；任何一个 target 目录里已经有超级块（属于别的集群）
//! → 拒绝初始化——在写任何文件之前就查完（与重复、嵌套的检查一起），所以被拒绝时什么也没写，改正参数就能重来。
//! `c/`、`p/`、`t/` 由 chunk_store 插件打开 target 时建。实现注记（M8）：`wmfs join` 的本地侧在 `init/join.rs`，与这里共用
//! `check_new_node`、`node_config`、`write_common`、`create_meta_db`、`write_superblocks`、`write_node`（同样的文件、同样的顺序，NODE 最后）。

mod initial_kv;
mod join;
mod targets;

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

pub use initial_kv::{HOME_INO, InitialKv, SCRATCH_INO, default_class, initial_kv};
pub use join::{JoinError, JoinOpts, join_node};
use serde::{Deserialize, Serialize};
pub use targets::parse_target;
use wmfs_api::hex::{hex, unhex};
use wmfs_api::types::KeyFile;

use crate::base::config::{NodeConfig, PluginTable, TargetConfig};
use crate::base::types::Role;
use crate::base::{clock, log, rand, sys};
use crate::buf::PoolConfig;
use crate::hooks::traits::KeyLookup;
use crate::meta::raft::storage;
use crate::meta::schema::records::{KeyMetaV1, PluginListV1};
use crate::proto::handshake::derive_secret;
use crate::proto::peer::ClusterMap;
use targets::{check_targets, target_rec, write_superblocks};

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

/// init 的部署模板（3.4.3 第 3 步的表）：default、minimal、nas-safe（`/home replicas=3,trash=7d,audit=on`）。hpc-rdma 不在这里：它的初始 KV
/// 与 default 只差 NodeRecV1(1) 多公布一个 rdma 地址——命令行按 default 建，把 `[plugin.rdma]` 经 [`InitOpts::data_transport`] 交给 init
/// （`cli/src/node.rs` 的 `parse_profile`；core 里除注释外不出现传输的名字，2.1）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Profile {
    Default,
    Minimal,
    NasSafe,
}

impl Profile {
    /// 按名字取模板；不认识 → None（给人看的合法名字只列在命令行的 `cli/src/node.rs` 的 `parse_profile`：它另认 hpc-rdma）。
    pub fn parse(s: &str) -> Option<Profile> {
        match s {
            "default" => Some(Profile::Default),
            "minimal" => Some(Profile::Minimal),
            "nas-safe" => Some(Profile::NasSafe),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Profile::Default => "default",
            Profile::Minimal => "minimal",
            Profile::NasSafe => "nas-safe",
        }
    }

    /// node.toml 里 `[plugins] disable` 的插件：minimal 停用 quota 与 mclock（于是 quota 属性无人消费、qos 用 fifo，9.7）。
    pub fn disabled_plugins(self) -> Vec<String> {
        match self {
            Profile::Default | Profile::NasSafe => vec![],
            Profile::Minimal => vec!["quota".into(), "mclock".into()],
        }
    }

    /// default 存储类的副本数：default、nas-safe 3（理念文档第 7 章）、minimal 1（单节点不出现降级警告）。
    pub fn class_replica(self) -> u8 {
        match self {
            Profile::Default | Profile::NasSafe => 3,
            Profile::Minimal => 1,
        }
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
    pub debug_ops: bool,            // 允许 wmfs debug kv（wmfs dev 打开；wmfs init 默认关闭）
    pub host_id: Option<String>,    // wmfs dev 用它给每个节点模拟不同的"主机"（1.1）
    pub plugins: PluginListV1,      // 本节点按 node.toml 装出来的插件清单：写进 GLOBAL plugins（3.4.3、3.7.2）
    pub targets: Vec<TargetConfig>, // `--target`：写进 node.toml 的 [[target]] 与初始 KV 的 TargetRecV1（M4）
    pub profile: Profile,           // 部署模板（M6）：初始 KV 与 node.toml 的 `[plugins] disable`
    /// 数据族另走的传输（M15，hpc-rdma）：(scheme, 它的 `[plugin.<scheme>]` 表，要有 `listen`)——写进 node.toml 并设 `[net] data_scheme`，
    /// listen 与 tcp 的一起写进 NodeRecV1(1).addrs（3.4.3 第 3 步）。None = 只有 tcp。core 不认识具体的传输（2.1）。
    pub data_transport: Option<(String, PluginTable)>,
}

/// 建一个新的单节点数据目录。目录必须不存在或为空（防止覆盖已有集群，坑 P14）。
pub fn init_node(o: &InitOpts) -> Result<(), String> {
    let d = &o.data_dir;
    check_new_node(d, &o.targets)?;
    let (mut cluster_id, mut master) = ([0u8; 16], [0u8; 32]);
    rand::fill(&mut cluster_id);
    rand::fill(&mut master);
    let node = NodeFile { format: 1, cluster_id: hex(&cluster_id), node_id: 1, created_ms: clock::wall_ms() };
    let net = Listen { addr: &o.addr, rpc_port: o.rpc_port, mgmt_port: o.mgmt_port, data: o.data_transport.as_ref() };
    let mut cfg = node_config(&net, &o.targets, o.host_id.clone());
    (cfg.failpoints_api, cfg.debug_ops, cfg.mem) = (o.failpoints_api, o.debug_ops, o.mem);
    cfg.plugins.disable = o.profile.disabled_plugins();
    write_common(d, &cfg, &master)?;
    write_meta(d, o, &node, net.addrs())?;
    let ids: Vec<u32> = (1..=o.targets.len() as u32).collect();
    write_superblocks(&o.targets, &ids, &node)?;
    write_node(d, &node) // NODE 放最后：它是提交点（见文件头），之前的文件都已经落盘
}

/// 监听地址（init 与 join 共用）。
pub(crate) struct Listen<'a> {
    pub addr: &'a str,
    pub rpc_port: u16,
    pub mgmt_port: u16,
    pub data: Option<&'a (String, PluginTable)>, // 数据族另走的传输（InitOpts::data_transport）
}

impl Listen<'_> {
    /// 写进 NodeRecV1.addrs 公布的地址（3.4.3 第 3 步）：tcp 的 `addr:rpc_port`，再加数据族另走的那个传输的 `listen`。实现注记（M3、M8、M15）：
    /// 取 node.toml 里的监听地址，不先起传输去问 `listen()`。
    pub(crate) fn addrs(&self) -> Vec<(String, String)> {
        let mut v = vec![("tcp".to_string(), format!("{}:{}", self.addr, self.rpc_port))];
        v.extend(self.data.and_then(|(s, t)| Some((s.clone(), t.get("listen")?.as_str()?.to_string()))));
        v
    }
}

/// 新节点的检查（init 与 join 共用）：数据目录必须不存在或为空（防止覆盖已有集群，坑 P14）；target 不能重复、嵌套或已有超级块。
pub(crate) fn check_new_node(d: &Path, targets: &[TargetConfig]) -> Result<(), String> {
    if std::fs::read_dir(d).is_ok_and(|mut it| it.next().is_some()) {
        return Err(format!("{} 不是空目录：拒绝初始化（防止覆盖已有集群）", d.display()));
    }
    check_targets(targets)
}

/// node.toml：监听地址（`[plugin.tcp] listen`、`[mgmt] listen`；数据族另走的传输：它的 `[plugin.<scheme>]` 与 `[net] data_scheme`）、
/// target、模拟的主机号；其余取默认值。
pub(crate) fn node_config(l: &Listen, targets: &[TargetConfig], host_id: Option<String>) -> NodeConfig {
    let mut cfg = NodeConfig::default();
    cfg.mgmt.listen = format!("{}:{}", l.addr, l.mgmt_port);
    let tcp =
        toml::Table::from_iter([("listen".to_string(), toml::Value::String(format!("{}:{}", l.addr, l.rpc_port)))]);
    cfg.plugin.insert("tcp".into(), tcp);
    if let Some((scheme, table)) = l.data {
        cfg.plugin.insert(scheme.clone(), table.clone());
        cfg.net.data_scheme = scheme.clone();
    }
    cfg.debug.host_id = host_id;
    cfg.targets = targets.to_vec();
    cfg
}

/// 建子目录，写 cluster.key、node.toml 与两把 key 文件（都在 NODE 之前）。
pub(crate) fn write_common(d: &Path, cfg: &NodeConfig, master: &[u8; 32]) -> Result<(), String> {
    for sub in ["keys", "logs", "run", "meta"] {
        std::fs::create_dir_all(d.join(sub)).map_err(|e| format!("建目录 {} 失败：{e}", d.join(sub).display()))?;
    }
    let key = |id| KeyFile { key_id: id, secret: derive_secret(master, id) }.to_line();
    write_file(d, "cluster.key", master)?;
    write_file(d, "node.toml", cfg.to_toml().as_bytes())?;
    write_file(d, "keys/admin.key", key(ADMIN_KEY).as_bytes())?;
    write_file(d, "keys/client.key", key(CLIENT_KEY).as_bytes())
}

fn write_file(d: &Path, name: &str, bytes: &[u8]) -> Result<(), String> {
    sys::atomic_write(&d.join(name), bytes).map_err(|e| format!("写 {} 失败：{e}", d.join(name).display()))
}

/// 写 NODE（init / join 的提交点，最后写）。
pub(crate) fn write_node(d: &Path, node: &NodeFile) -> Result<(), String> {
    let node_toml = toml::to_string(node).map_err(|e| format!("NODE 编码失败：{e}"))?;
    write_file(d, "NODE", node_toml.as_bytes())
}

/// 新建 `meta/meta.redb`（0600）。redb 只 fsync 文件内容：调用方写完初始状态之后还要 fsync `meta/` 目录。
pub(crate) fn create_meta_db(d: &Path) -> Result<redb::Database, String> {
    let path = d.join("meta/meta.redb");
    let db = redb::Database::create(&path).map_err(|e| format!("建元数据库 {} 失败：{e}", path.display()))?;
    let private = std::fs::Permissions::from_mode(0o600); // 与 NODE、密钥文件一样只给本用户读写（3.2.1 的实现注记）
    std::fs::set_permissions(&path, private).map_err(|e| format!("设置 {} 的权限失败：{e}", path.display()))?;
    Ok(db)
}

/// 写 `meta/meta.redb` 的 index = 1 初始状态（3.4.1.9、3.4.3 第 3 步）。根目录的属主 = 运行 init 的用户（本进程的有效
/// uid/gid；数据目录可以是别的用户事先建好的空目录，所以不取它的属主）。
fn write_meta(d: &Path, o: &InitOpts, node: &NodeFile, addrs: Vec<(String, String)>) -> Result<(), String> {
    let path = d.join("meta/meta.redb");
    let machine_id = read_fact("/etc/machine-id");
    let i = InitialKv {
        cluster_id: node.cluster_id_bytes()?,
        created_ms: node.created_ms,
        owner: sys::euid_egid(),
        addrs,
        mgmt_addr: format!("{}:{}", o.addr, o.mgmt_port),
        host_id: o.host_id.clone().unwrap_or_else(|| machine_id.clone()),
        machine_id,
        hostname: read_fact("/proc/sys/kernel/hostname"),
        plugins: o.plugins.clone(),
        targets: o.targets.iter().map(|t| target_rec(t, node.node_id, node.created_ms)).collect::<Result<_, _>>()?,
        profile: o.profile,
    };
    let db = create_meta_db(d)?;
    storage::bootstrap(&db, &[node.node_id as u64], &initial_kv(&i)).map_err(|e| format!("{}：{e}", path.display()))?;
    // redb 只 fsync 文件内容：新建的 meta.redb 在 meta/ 里的目录项要另外落盘，才能在之后写 NODE（提交点）。
    // meta/ 自己在数据目录里的目录项已经落盘了——写 cluster.key 的 atomic_write 在它建好之后 fsync 过数据目录
    let meta_dir = d.join("meta");
    sys::fsync_dir(&meta_dir).map_err(|e| format!("fsync 目录 {} 失败：{e}", meta_dir.display()))
}

/// 读一行本机信息（machine-id、hostname）；读不到写一行警告、留空。
pub(crate) fn read_fact(p: &str) -> String {
    match std::fs::read_to_string(p) {
        Ok(s) => s.trim().to_string(),
        Err(e) => {
            log::warn("init", format_args!("读不到 {p}（{e}）：NodeRecV1 里的这一项留空"));
            String::new()
        }
    }
}

/// init 写下的初始状态里的集群警告（3.4.3 第 4 步、ADR-0016）：例如单节点上 default 类声明 3 副本、只有 1 个故障域。
pub fn initial_warnings(o: &InitOpts) -> Vec<String> {
    let targets = o.targets.iter().filter_map(|t| target_rec(t, 1, 0).ok());
    let m = ClusterMap {
        targets: (1u32..).zip(targets).collect(),
        classes: vec![default_class(o.profile.class_replica())],
        ..ClusterMap::default()
    };
    crate::meta::cluster::map::warnings(&m)
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
