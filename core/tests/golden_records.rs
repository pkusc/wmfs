//! 金样：元数据 KV 的每种记录（design.md 3.2.8）、`raft_state` 表的值（3.2.6）、raft 条目负载 WriteBatchV1（3.2.9）、
//! 每种 key（3.2.7）；以及记录版本字节的检查。
//! 运行：`cargo test -p wmfs-core golden`；有意改格式后：`WMFS_GOLDEN_UPDATE=1 cargo test -p wmfs-core golden`。

mod common;

use std::collections::BTreeMap;

use common::*;
use wmfs_core::base::ErrCode;
use wmfs_core::meta::schema::raft_state::{Applied, MetaFormatV1, SnapMeta};
use wmfs_core::meta::schema::{keys, records::*};

#[test]
fn golden_records() {
    let mut g = Golden::new("records");
    g.rec("InodeV1.file", &inode_file());
    let mut link = inode_file();
    link.kind = KIND_SYMLINK;
    link.layout = None;
    link.symlink = b"../target".to_vec();
    link.size = 9;
    g.rec("InodeV1.symlink", &link);
    g.rec("DentryV1", &DentryV1 { ino: 0x1234_5678_9abc, kind: KIND_DIR });
    g.rec(
        "XattrsV1",
        &XattrsV1 {
            entries: vec![
                (b"user.k".to_vec(), b"hi".to_vec()),
                (b"system.posix_acl_access".to_vec(), vec![2, 0, 0, 0]),
            ],
        },
    );
    let mut attrs = BTreeMap::new();
    attrs.insert("chunk".to_string(), "4194304".to_string());
    attrs.insert("replicas".to_string(), "3".to_string());
    g.rec("AttrMapV1", &AttrMapV1 { entries: attrs });
    g.rec("RStatV1", &RStatV1 { bytes: 1 << 33, files: 12, dirs: 3, per_class: vec![(1, 1 << 33, 11), (2, 0, 1)] });
    g.rec("QuotaStateV1", &QuotaStateV1 { soft_since_ms: 1_792_454_400_123 });
    let l1 = LockRecV1 { session: 0xAA, owner: 0xBB, pid: 4242, start: 0, end: 99, write: true };
    let l2 = LockRecV1 { session: 0xCC, owner: 0xDD, pid: 4343, start: 100, end: LOCK_EOF, write: false };
    g.rec("LockTableV1", &LockTableV1 { locks: vec![l1, l2] });
    g.rec(
        "SessionRecV1",
        &SessionRecV1 {
            client_name: "wmfs-mount".into(),
            host: "spark".into(),
            host_id: "host-1".into(),
            mount: "/home/u/wmfs-mnt".into(),
            key_id: 2,
            tenant: 0,
            created_ms: 1_792_454_400_002,
            locked_inos: vec![18, 19],
            state: 2,
            evict_since_ms: 1_792_454_400_003,
            evict_map_epoch: 8,
        },
    );
    g.rec("DedupV1.ok", &DedupV1 { code: 0, reply: vec![15] });
    g.rec("DedupV1.err", &DedupV1 { code: 17, reply: vec![] });
    g.rec("GcRecV1", &GcRecV1 { due_ms: 1_792_454_460_000, chains: vec![11, 12], created_ms: 1_792_454_400_000 });
    g.rec(
        "ChangeRecV1",
        &ChangeRecV1 {
            time_ms: 1_792_454_400_004,
            op: 3,
            result: 0,
            trace_id: 0x7ACE,
            uid: 1000,
            gid: 1001,
            tenant: 2,
            session: 0x5E55,
            job_id: 77,
            ino: 18,
            parent: 16,
            name: b"a".to_vec(),
            new_parent: 17,
            new_name: b"b".to_vec(),
            size: 5,
            extra: vec![("key".into(), "chunk".into())],
            audited: true,
        },
    );
    g.rec("NodeRecV1", &node_rec());
    g.rec("TargetRecV1", &target_rec());
    g.rec("ChainRecV1", &chain_rec());
    g.rec("StorageClassV1", &storage_class());
    g.rec("ClassIdV1", &ClassIdV1 { id: 2 });
    g.rec(
        "TaskRecV1",
        &TaskRecV1 {
            state: 1,
            cursor: vec![1, 2, 3],
            pass: 4,
            done: 600,
            total_hint: 1000,
            backlog: 400,
            bytes_per_sec: 50 << 20,
            items_per_sec: 100,
            updated_ms: 1_792_454_400_005,
            last_error: "无".into(),
        },
    );
    g.rec("KeyMetaV1", &key_meta());
    g.rec("DataKeyV1", &data_key());
    g.rec("DataKeyIdV1", &DataKeyIdV1 { id: 3 });
    g.rec("QosSpecV1", &qos_spec());
    g.rec("ReconcileItemV1", &ReconcileItemV1 { dir: 16, reason: 2 });
    g.rec("CursorV1", &CursorV1 { seq: 1234, updated_ms: 1_792_454_400_006 });
    g.rec("PluginListV1", &plugin_list());
    g.rec("EvictedV1", &evicted());
    g.rec("ClusterInfoV1", &ClusterInfoV1 { cluster_id: [0xC1; 16], created_ms: 1_792_454_400_007, format: 1 });
    g.rec("FeatureSetV1", &feature_set());
    g.rec("ClusterSettingsV1.default", &ClusterSettingsV1::default());
    // 默认值里 repair_delay 与 rejoin_window 相等，另加一个七个字段互不相同的样本
    g.rec(
        "ClusterSettingsV1.custom",
        &ClusterSettingsV1 {
            session_lease_ms: 20_000,
            target_lease_ms: 6_000,
            cap_ttl_ms: 300_000,
            repair_delay_ms: 900_000,
            rejoin_window_ms: 1_200_000,
            epoch_gc_grace_ms: 30_000,
            bg_bytes_per_sec: 100 << 20,
        },
    );
    // raft_state 表（3.2.6）："format" 是带版本字节的记录，"snap_meta"、"applied" 是定长小端字节
    let init = MetaFormatV1 { version: 1, incompat: vec![] };
    let fmt = MetaFormatV1 { version: 1, incompat: vec!["meta.ec-layout".into()] };
    for (name, f) in [("raft_state.format.init", &init), ("raft_state.format", &fmt)] {
        assert_eq!(MetaFormatV1::decode(&f.encode()).as_ref(), Ok(f));
        g.add(name, f.encode());
    }
    let snap = SnapMeta { index: 10_000, term: 3 };
    assert_eq!(SnapMeta::decode(&snap.encode()), Ok(snap));
    g.add("raft_state.snap_meta", snap.encode().to_vec());
    let applied = Applied { index: 10_042, term: 4, state_version: 10_040 };
    assert_eq!(Applied::decode(&applied.encode()), Ok(applied));
    g.add("raft_state.applied", applied.encode().to_vec());
    // GLOBAL 计数器的 value 也是"版本字节 + postcard(u64/u32/u16)"
    g.rec("counter.u64.next_ino", &18u64);
    g.rec("counter.u32.next_key", &3u32);
    g.rec("counter.u16.next_class", &2u16);
    g.check();
}

#[test]
fn golden_write_batch() {
    let mut g = Golden::new("write_batch");
    let wb = WriteBatchV1 {
        expect_version: 41,
        leader_term: 2,
        ops: vec![
            KvOp::Put { key: keys::inode(18), value: encode_v1(&inode_file()) },
            KvOp::Delete { key: keys::dentry(16, b"old") },
        ],
        trace_ids: vec![0x7ACE, 0x7ACF],
    };
    let b = wb.encode();
    assert_eq!(b[0], 0x01, "raft 条目 data = [0x01] ‖ postcard(WriteBatchV1)");
    assert_eq!(WriteBatchV1::decode(&b), Ok(wb));
    g.add("WriteBatchV1", b);
    let ops = vec![
        ("Put", KvOp::Put { key: vec![0x2F, 1], value: vec![9] }),
        ("Delete", KvOp::Delete { key: vec![0x2F, 1] }),
    ];
    variants(&mut g, "KvOp", ops, 2);
    g.check();
}

#[test]
fn record_version_byte_and_length_are_checked() {
    // 版本纪律（3.2.11）的核心：V2（或任何不是 1 的版本字节）的记录绝不能被当成 V1 解开；尾部多余字节也算损坏
    let good = encode_v1(&DentryV1 { ino: 16, kind: KIND_DIR });
    assert_eq!(decode_v1::<DentryV1>(&good), Ok(DentryV1 { ino: 16, kind: KIND_DIR }));
    let mut v2 = good.clone();
    v2[0] = 2;
    assert_eq!(decode_v1::<DentryV1>(&v2), Err(ErrCode::INTERNAL), "版本字节 2");
    let mut longer = good.clone();
    longer.push(0);
    assert_eq!(decode_v1::<DentryV1>(&longer), Err(ErrCode::INTERNAL), "尾部多 1 字节");
    assert_eq!(decode_v1::<DentryV1>(&good[..good.len() - 1]), Err(ErrCode::INTERNAL), "少 1 字节");
    assert_eq!(decode_v1::<DentryV1>(&[REC_V1]), Err(ErrCode::INTERNAL), "只有版本字节");
    assert_eq!(decode_v1::<DentryV1>(&[]), Err(ErrCode::INTERNAL), "空");
    assert_eq!(WriteBatchV1::decode(&[2]), Err(ErrCode::INTERNAL), "raft 条目负载也一样");
}

#[test]
fn golden_keys() {
    let mut g = Golden::new("keys");
    g.add("INODE", keys::inode(0x0102_0304_0506_0708));
    g.add("DENTRY", keys::dentry(16, b"name"));
    g.add("XATTR", keys::xattr(18));
    g.add("INLINE", keys::inline(18));
    g.add("DIRATTR", keys::dirattr(16));
    g.add("RSTAT", keys::rstat(16));
    g.add("QUOTA_STATE", keys::quota_state(16, keys::QUOTA_CLASS_TOTAL));
    g.add("LOCKS", keys::locks(18));
    g.add("SESSION", keys::session(0x5E55));
    g.add("DEDUP", keys::dedup(0x5E55, 256));
    g.add("GC", keys::gc(keys::GC_EPOCH, 18, 3));
    g.add("CHANGELOG", keys::changelog(1));
    g.add("TRASH_INDEX", keys::trash_index(1_792_454_400_000, 18));
    g.add("NODE", keys::node(1));
    g.add("TARGET", keys::target(2));
    g.add("CHAIN", keys::chain(3));
    g.add("CLASS", keys::class(1));
    g.add("CLASS_NAME", keys::class_name("default"));
    g.add("TASK", keys::task("reconciler", "main"));
    g.add("KEYMETA", keys::keymeta(2));
    g.add("DATAKEY", keys::datakey(1));
    g.add("QOS", keys::qos(16));
    g.add("RECONCILE_Q", keys::reconcile(5));
    g.add("CURSOR", keys::cursor("audit-log"));
    g.add("EVICTED", keys::evicted(0x5E55));
    g.add("DATAKEY_NAME", keys::datakey_name("lab"));
    g.add("GLOBAL", keys::global(keys::G_NEXT_INO));
    g.add("DEBUG", keys::debug(b"hello"));
    g.check();
}
