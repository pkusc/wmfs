//! init 写进 index = 1 初始状态的 KV（design.md 3.4.3 第 3 步、3.2.7 的"id 与计数器的初值"，default profile）。
//!
//! - 目录树：根 = inode 1（目录，mode 0755，owner = 运行 init 的用户）；`/home` = 16、`/scratch` = 17（mode 1777）。
//!   各自的 DENTRY 与 RSTAT（RSTAT 覆盖子树、不含自身：根的 dirs = 2）。
//! - default profile 的目录属性：`/home replicas=3`、`/scratch replicas=2`（理念文档第 7 章默认冗余）。
//! - 存储类 default（id 1，`label = "*"`，replica 3，priority normal，weight 1，无 QoS 限制）与 CLASS_NAME。
//! - 实现注记（M6）：minimal profile（3.4.3 的表）：default 类 replica = 1，不写 replicas 属性（单节点不出现降级警告）；其余相同。
//! - NodeRecV1(1)（voter、Active；地址来自 node.toml 的监听地址）；KEYMETA：key 1 = admin，key 2 = 默认 client。
//! - TargetRecV1：`--target` 给的每个 target 一条，id 1..=n（实现注记（M4）：node 1、状态 Up、`last_up_ms` = init 的时刻，
//!   `phys_devs` 空、`boot_id` 0——两者由 target 心跳在 M5 填上；容量取 `[[target]] capacity`，0 = 不限）。
//! - GLOBAL：`next_node = 2`、`next_target = n + 1`、`next_chain = 1`、`next_class = 2`、`next_key = 3`、
//!   `next_datakey = 1`、`next_changelog = 1`、`next_reconcile = 1`、`next_ino = 18`、`attr_epoch = map_epoch = 1`、
//!   `cluster`、`features`（空）、`settings`（默认值）、`plugins`（本节点按 node.toml 装出来的插件清单，3.7.2）。init 不写 changelog。
//!
//! 实现注记（M3）：规格没写的几个初值取最小方案（金样 `tests/golden/init_kv.golden` 钉住）：根的 `parent = 0`（"无"，与
//! "id 0 表示无"的约定一致）、`name` 为空；目录的 `nlink = 2 + 子目录数`、`size = 直接子项数`；三个目录的 `change_id = 1`；
//! 时间戳全部取 init 的时刻；`NodeRecV1.features` 为空、`host_id` 未指定时取 machine_id。

use crate::base::types::Ts;
use crate::init::{Profile, initial_keymeta};
use crate::meta::schema::keys::{self, FIRST_INO, ROOT_INO};
use crate::meta::schema::records::{
    AttrMapV1, ClassIdV1, ClusterInfoV1, ClusterSettingsV1, DentryV1, FeatureSetV1, InodeV1, KIND_DIR, NodeRecV1,
    PluginListV1, RStatV1, StorageClassV1, TargetRecV1, encode_v1,
};

/// `/home` 与 `/scratch` 是 init 分配的头两个普通 inode。
pub const HOME_INO: u64 = FIRST_INO;
pub const SCRATCH_INO: u64 = FIRST_INO + 1;

/// 初始 KV 需要的输入（都由 init 决定；测试与金样用固定值）。
pub struct InitialKv {
    pub cluster_id: [u8; 16],
    pub created_ms: u64,
    pub owner: (u32, u32), // 根目录的 uid、gid
    pub addrs: Vec<(String, String)>,
    pub mgmt_addr: String,
    pub host_id: String,
    pub machine_id: String,
    pub hostname: String,
    pub plugins: PluginListV1,
    pub targets: Vec<TargetRecV1>, // id 依次为 1..=n
    pub profile: Profile,
}

/// 按 key 排好序的 (key, value)。
pub fn initial_kv(i: &InitialKv) -> Vec<(Vec<u8>, Vec<u8>)> {
    let ts = Ts { secs: (i.created_ms / 1000) as i64, nanos: (i.created_ms % 1000) as u32 * 1_000_000 };
    let dir = |mode: u32, nlink: u32, size: u64, parent: u64, name: &[u8]| InodeV1 {
        kind: KIND_DIR,
        mode,
        uid: i.owner.0,
        gid: i.owner.1,
        nlink,
        size,
        atime: ts,
        mtime: ts,
        ctime: ts,
        btime: ts,
        rdev: 0,
        parent,
        name: name.to_vec(),
        extra_links: vec![],
        flags: 0,
        change_id: 1,
        class_id: 0,
        layout: None,
        pending_epoch: 0,
        max_epoch: 0,
        trash_expire_ms: 0,
        symlink: vec![],
    };
    let attr = |k: &str, v: &str| AttrMapV1 { entries: [(k.to_string(), v.to_string())].into_iter().collect() };
    let default_class = default_class(i.profile.class_replica());
    let node = NodeRecV1 {
        addrs: i.addrs.clone(),
        mgmt_addr: i.mgmt_addr.clone(),
        host_id: i.host_id.clone(),
        machine_id: i.machine_id.clone(),
        hostname: i.hostname.clone(),
        meta_role: 2, // voter
        features: vec![],
        joined_ms: i.created_ms,
        state: 1, // Active
    };
    let mut kv = vec![
        (keys::inode(ROOT_INO), encode_v1(&dir(0o755, 4, 2, 0, b""))),
        (keys::inode(HOME_INO), encode_v1(&dir(0o1777, 2, 0, ROOT_INO, b"home"))),
        (keys::inode(SCRATCH_INO), encode_v1(&dir(0o1777, 2, 0, ROOT_INO, b"scratch"))),
        (keys::dentry(ROOT_INO, b"home"), encode_v1(&DentryV1 { ino: HOME_INO, kind: KIND_DIR })),
        (keys::dentry(ROOT_INO, b"scratch"), encode_v1(&DentryV1 { ino: SCRATCH_INO, kind: KIND_DIR })),
        (keys::rstat(ROOT_INO), encode_v1(&RStatV1 { dirs: 2, ..Default::default() })),
        (keys::rstat(HOME_INO), encode_v1(&RStatV1::default())),
        (keys::rstat(SCRATCH_INO), encode_v1(&RStatV1::default())),
        (keys::class(1), encode_v1(&default_class)),
        (keys::class_name("default"), encode_v1(&ClassIdV1 { id: 1 })),
        (keys::node(1), encode_v1(&node)),
        (keys::global(keys::G_NEXT_INO), encode_v1(&(SCRATCH_INO + 1))),
        (keys::global(keys::G_NEXT_NODE), encode_v1(&2u32)),
        (keys::global(keys::G_NEXT_TARGET), encode_v1(&(i.targets.len() as u32 + 1))),
        (keys::global(keys::G_NEXT_CHAIN), encode_v1(&1u32)),
        (keys::global(keys::G_NEXT_CLASS), encode_v1(&2u16)),
        (keys::global(keys::G_NEXT_KEY), encode_v1(&3u32)),
        (keys::global(keys::G_NEXT_DATAKEY), encode_v1(&1u32)),
        (keys::global(keys::G_NEXT_CHANGELOG), encode_v1(&1u64)),
        (keys::global(keys::G_NEXT_RECONCILE), encode_v1(&1u64)),
        (keys::global(keys::G_ATTR_EPOCH), encode_v1(&1u64)),
        (keys::global(keys::G_MAP_EPOCH), encode_v1(&1u64)),
        (
            keys::global(keys::G_CLUSTER),
            encode_v1(&ClusterInfoV1 { cluster_id: i.cluster_id, created_ms: i.created_ms, format: 1 }),
        ),
        (keys::global(keys::G_FEATURES), encode_v1(&FeatureSetV1::default())),
        (keys::global(keys::G_SETTINGS), encode_v1(&ClusterSettingsV1::default())),
        (keys::global(keys::G_PLUGINS), encode_v1(&i.plugins)),
    ];
    if i.profile == Profile::Default {
        kv.push((keys::dirattr(HOME_INO), encode_v1(&attr("replicas", "3"))));
        kv.push((keys::dirattr(SCRATCH_INO), encode_v1(&attr("replicas", "2"))));
    }
    kv.extend(initial_keymeta(i.created_ms).into_iter().map(|(id, m)| (keys::keymeta(id), encode_v1(&m))));
    kv.extend(i.targets.iter().zip(1u32..).map(|(t, id)| (keys::target(id), encode_v1(t))));
    kv.sort();
    kv
}

/// init 预置的存储类 default（3.4.3、6.1）：id 1，`label = "*"` 匹配任何 target，`replica` 份（default profile 3、minimal 1），
/// priority normal，没有 QoS 限制。
pub fn default_class(replica: u8) -> StorageClassV1 {
    StorageClassV1 {
        id: 1,
        name: "default".into(),
        label: "*".into(),
        dedicated: false,
        replica,
        iops_min: 0,
        iops_max: 0,
        bw_min: 0,
        bw_max: 0,
        burst_ms: 0,
        weight: 1, // mClock 权重，缺省 1（6.1）
        latency_target_us: 0,
        priority: 1,       // normal
        failure_domain: 1, // node（首发只有这一种）
        locality: 0,       // any
        tier_to: None,
        tier_when: None,
    }
}
