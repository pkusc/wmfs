//! init 的单元测试：写出的文件、拒绝非空目录、元数据库的 index = 1 初始状态（3.4.1.9、3.4.3）。

use super::*;
use crate::meta::raft::storage::{KV, RedbStorage, read_applied};
use crate::meta::schema::keys;
use crate::meta::schema::raft_state::Applied;
use crate::meta::schema::records::{InodeV1, NodeRecV1, decode_v1};

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
    std::fs::remove_dir_all(&d).unwrap();
}
