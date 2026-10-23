//! init 的单元测试：写出的文件、拒绝非空目录、元数据库的 index = 1 初始状态（3.4.1.9、3.4.3）、`--target` 的解析与
//! 重复/嵌套的 target、init 的集群警告。

use super::*;
use crate::meta::raft::storage::{KV, RedbStorage, read_applied};
use crate::meta::schema::keys;
use crate::meta::schema::raft_state::Applied;
use crate::meta::schema::records::{InodeV1, NodeRecV1, TargetRecV1, decode_v1};

fn opts(d: &Path) -> InitOpts {
    InitOpts {
        data_dir: d.join("n1"),
        addr: "127.0.0.1".into(),
        rpc_port: 19100,
        mgmt_port: 19180,
        mem: PoolConfig::dev_server(),
        failpoints_api: true,
        debug_ops: true,
        host_id: Some("host-1".into()),
        plugins: PluginListV1 { plugins: vec!["authn:shared-key".into()] },
        targets: vec![TargetConfig {
            capacity: "20G".into(),
            ..parse_target(&format!("{}/t1:nvme,fast", d.display())).unwrap()
        }],
    }
}

#[test]
fn init_writes_files_and_refuses_non_empty_dir() {
    let d = crate::base::test_dir("init");
    let o = opts(&d);
    init_node(&o).unwrap();
    let n = NodeFile::read(&o.data_dir).unwrap();
    assert_eq!((n.format, n.node_id), (1, 1));
    let cfg = NodeConfig::parse(&std::fs::read_to_string(o.data_dir.join("node.toml")).unwrap()).unwrap();
    assert_eq!(cfg.mgmt.listen, "127.0.0.1:19180");
    assert_eq!(cfg.plugin_cfg("tcp").get("listen").and_then(|v| v.as_str()), Some("127.0.0.1:19100"));
    assert!(cfg.failpoints_api && cfg.debug_ops && cfg.mem == PoolConfig::dev_server());
    let master = read_master(&o.data_dir).unwrap();
    let admin = KeyFile::parse(&std::fs::read_to_string(o.data_dir.join("keys/admin.key")).unwrap()).unwrap();
    assert_eq!(admin, KeyFile { key_id: 1, secret: derive_secret(&master, 1) });
    let keys = InitKeys::new(master, n.created_ms);
    assert_eq!(keys.secret(2), Some(derive_secret(&master, 2)));
    assert_eq!(keys.meta(2).map(|m| m.role), Some(Role::Client.as_u8()));
    assert!(keys.secret(0).is_some() && keys.meta(0).is_none(), "key 0 不在 KEYMETA 里");
    assert!(keys.secret(3).is_none() && keys.meta(3).is_none());
    assert!(init_node(&o).unwrap_err().contains("不是空目录"), "第二次 init 必须拒绝");
    // M5：每个 target 一个超级块（target_id 1..=n、本集群的 cluster_id）
    let sb = TargetSuperblock::decode(&std::fs::read(d.join("t1/TARGET")).unwrap()).unwrap();
    assert_eq!((sb.target_id, sb.node_id, sb.cluster_id), (1, 1, n.cluster_id_bytes().unwrap()));
    let mut again = InitOpts { data_dir: d.join("n2"), ..opts(&d) };
    again.targets.insert(0, parse_target(&format!("{}/t0", d.display())).unwrap()); // 第 1 个是新的，第 2 个已经有超级块
    assert!(init_node(&again).unwrap_err().contains("已经有 target 超级块"), "target 已经属于一个集群");
    assert!(!d.join("n2").exists() && !d.join("t0").exists(), "被拒绝时什么也没写：改正参数就能重来");
    std::fs::remove_dir_all(&d).unwrap();
}

#[test]
fn init_writes_the_index_1_metadata_state() {
    let d = crate::base::test_dir("init-meta");
    let o = opts(&d);
    init_node(&o).unwrap();
    let db = std::sync::Arc::new(redb::Database::open(o.data_dir.join("meta/meta.redb")).unwrap());
    let s = RedbStorage::open(db.clone()).unwrap();
    assert_eq!((s.snap_index(), s.hard_state().commit, s.conf_state().voters.clone()), (1, 1, vec![1]));
    let rtx = db.begin_read().unwrap();
    assert_eq!(read_applied(&rtx), Ok(Applied { index: 1, term: 1, state_version: 1 }));
    let kv = rtx.open_table(KV).unwrap();
    let get = |k: Vec<u8>| kv.get(k.as_slice()).unwrap().map(|v| v.value().to_vec());
    let root: InodeV1 = decode_v1(&get(keys::inode(keys::ROOT_INO)).unwrap()).unwrap();
    let (uid, gid) = sys::euid_egid(); // 属主 = 运行 init 的用户（3.4.3 第 3 步）
    assert_eq!((root.mode, root.uid, root.gid, root.nlink, root.size), (0o755, uid, gid, 4, 2));
    let home: InodeV1 = decode_v1(&get(keys::inode(HOME_INO)).unwrap()).unwrap();
    assert_eq!((home.mode, home.parent, home.name.as_slice()), (0o1777, 1, b"home".as_slice()));
    let node: NodeRecV1 = decode_v1(&get(keys::node(1)).unwrap()).unwrap();
    assert_eq!(node.addrs, vec![("tcp".to_string(), "127.0.0.1:19100".to_string())]);
    assert_eq!((node.mgmt_addr.as_str(), node.host_id.as_str(), node.meta_role), ("127.0.0.1:19180", "host-1", 2));
    let plugins: PluginListV1 = decode_v1(&get(keys::global(keys::G_PLUGINS)).unwrap()).unwrap();
    assert_eq!(plugins, o.plugins, "GLOBAL plugins = 本节点的插件清单");
    let km: KeyMetaV1 = decode_v1(&get(keys::keymeta(2)).unwrap()).unwrap();
    assert_eq!(km, initial_keymeta(km.created_ms)[1].1, "KEYMETA：key 2 = 默认 client");
    assert!(get(keys::keymeta(0)).is_none() && get(keys::keymeta(3)).is_none());
    let next_ino: u64 = decode_v1(&get(keys::global(keys::G_NEXT_INO)).unwrap()).unwrap();
    assert_eq!(next_ino, 18);
    // M4：--target 写 TargetRecV1（id 1..=n）与 node.toml 的 [[target]]
    let t: TargetRecV1 = decode_v1(&get(keys::target(1)).unwrap()).unwrap();
    let want_path = d.join("t1").display().to_string();
    assert_eq!(
        (t.node, t.path.as_str(), t.labels.clone(), t.capacity, t.state),
        (1, want_path.as_str(), vec!["nvme".to_string(), "fast".to_string()], 20 << 30, 1)
    );
    assert!(get(keys::target(2)).is_none());
    let next_target: u32 = decode_v1(&get(keys::global(keys::G_NEXT_TARGET)).unwrap()).unwrap();
    assert_eq!(next_target, 2);
    let cfg = NodeConfig::parse(&std::fs::read_to_string(o.data_dir.join("node.toml")).unwrap()).unwrap();
    assert_eq!(cfg.targets, o.targets);
    std::fs::remove_dir_all(&d).unwrap();
}

#[test]
fn targets_on_the_command_line_and_the_initial_warnings() {
    let t = parse_target("/x/y:nvme,hdd-2").unwrap();
    assert_eq!((t.path.as_str(), t.labels.clone()), ("/x/y", vec!["nvme".to_string(), "hdd-2".to_string()]));
    assert_eq!(t.capacity, "0", "--target 的容量不限");
    assert!(parse_target("rel/dir").unwrap().path.starts_with('/'), "换成绝对路径");
    assert!(parse_target(":nvme").is_err(), "缺路径");
    assert!(parse_target("/x:NVMe").is_err(), "标签只能是小写字母、数字与 -");
    let d = crate::base::test_dir("init-warn");
    let mut o = opts(&d);
    let x = d.join("x").display().to_string();
    let twice = |a: &str, b: &str| {
        let t = |s: &str| parse_target(&format!("{x}{s}")).unwrap();
        InitOpts { targets: vec![t(a), t(b)], ..opts(&d) }
    };
    assert!(init_node(&twice("", "/")).unwrap_err().contains("给了两次"), "同一个路径给两次");
    assert!(init_node(&twice("/t1", "")).unwrap_err().contains("一个在另一个里面"), "互相嵌套");
    assert!(!o.data_dir.exists(), "拒绝时什么也没写");
    assert!(init_node(&twice("/t1", "/t10")).is_ok(), "x/t10 不在 x/t1 里面（按路径的各段比较）");
    let w = initial_warnings(&o);
    assert_eq!(
        w,
        vec!["default 类声明 3 副本，只有 1 个故障域：新文件实际为 1 副本（加入节点后自动补齐）".to_string()]
    );
    o.targets.clear();
    assert!(initial_warnings(&o)[0].contains("没有可用的 target"), "没有 target：新文件建不出来");
    std::fs::remove_dir_all(&d).unwrap();
}
