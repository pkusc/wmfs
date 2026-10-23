//! 金样：帧头（design.md 3.3.1）、握手与 Ping（3.3.2）、MetaCall/MetaOp/MetaReply（3.3.4）、
//! DataCall/DataOp/DataReply/CapV1（3.3.5）、PeerOp/NodeOp/ClusterMap 与快照流消息（3.3.6、3.4.1.7）。
//! 每个枚举的**每个变体**一份金样，并检查第一个字节就是变体序号（postcard 的变体序号 = 声明顺序），
//! 还确认枚举没有更多的变体（`common::variants`）。

mod common;

use common::*;
use wmfs_core::proto::cap::CapV1;
use wmfs_core::proto::data::*;
use wmfs_core::proto::frame::*;
use wmfs_core::proto::handshake::*;
use wmfs_core::proto::meta::*;
use wmfs_core::proto::peer::*;

#[test]
fn golden_frame_headers() {
    let mut g = Golden::new("frame_header");
    let mut req = FrameHeader::new(FrameKind::Request, MSG_META_CALL, 1, 0x1122_3344_5566_7788);
    req.body_len = 200;
    req.body_crc32c = crc32c::crc32c(b"body");
    g.add("request.meta_call", req.encode().to_vec());
    let mut err = FrameHeader::new(FrameKind::Response, MSG_DATA_CALL, 2, 3);
    err.flags = FLAG_ERROR;
    err.span_id = 2;
    err.body_len = 30;
    g.add("response.error", err.encode().to_vec());
    let mut raft = FrameHeader::new(FrameKind::OneWay, MSG_RAFT, 0, 0);
    raft.flags = FLAG_HAS_BULK;
    raft.bulk_len = 1 << 20;
    raft.bulk_crc32c = 0xABCD_EF01;
    g.add("oneway.raft_bulk", raft.encode().to_vec());
    let mut last = FrameHeader::new(FrameKind::StreamChunk, MSG_SNAPSHOT_FETCH, 9, 9);
    last.flags = FLAG_LAST_CHUNK;
    g.add("stream.last_chunk", last.encode().to_vec());
    g.msg(
        "ErrorBody",
        &ErrorBody {
            code: 1000,
            msg: "不是 leader".into(),
            leader_hint: Some("127.0.0.1:19200".into()),
            retry_after_ms: 10,
            extra: 7,
        },
    );
    g.check();
}

#[test]
fn golden_handshake() {
    let mut g = Golden::new("handshake");
    g.msg(
        "Hello",
        &Hello {
            proto_min: 1,
            proto_max: 2,
            features: 0b1111,
            required: 0b0001,
            role: 2,
            cluster_id: [0xC1; 16],
            key_id: 7,
            client_nonce: [0x11; 16],
            software: "wmfs 0.1.0".into(),
        },
    );
    g.msg(
        "Challenge",
        &Challenge { proto: 1, features: 0b0101, cluster_id: [0xC1; 16], node_id: 1, server_nonce: [0x22; 16] },
    );
    g.msg("Auth", &Auth { proof: [0x33; 32] });
    g.msg("Welcome", &Welcome { key_id: 7, role: 2, tenant: 5, server_proof: [0x44; 32] });
    g.msg("PingBody", &PingBody { payload: b"ping".to_vec() });
    g.check();
}

fn meta_ops() -> Vec<(&'static str, MetaOp)> {
    let n = || b"f1".to_vec();
    vec![
        (
            "SessionOpen",
            MetaOp::SessionOpen {
                client_name: "c".into(),
                host: "h".into(),
                host_id: "hi".into(),
                mount: "/m".into(),
                version: "0.1.0".into(),
            },
        ),
        (
            "SessionHeartbeat",
            MetaOp::SessionHeartbeat { opened: vec![(18, true)], closed: vec![19], full: Some(vec![(20, false)]) },
        ),
        ("SessionClose", MetaOp::SessionClose),
        ("Lookup", MetaOp::Lookup { parent: 16, name: n() }),
        ("GetAttr", MetaOp::GetAttr { ino: 18 }),
        ("ReadDir", MetaOp::ReadDir { ino: 16, after: b"a".to_vec(), limit: 1024, plus: true }),
        ("ReadLink", MetaOp::ReadLink { ino: 19 }),
        ("GetXattr", MetaOp::GetXattr { ino: 18, name: b"user.k".to_vec() }),
        ("ListXattr", MetaOp::ListXattr { ino: 18 }),
        ("StatFs", MetaOp::StatFs),
        ("Open", MetaOp::Open { ino: 18, flags: 0o2 }),
        ("GetInline", MetaOp::GetInline { ino: 18 }),
        ("GetLock", MetaOp::GetLock { ino: 18, owner: 0xBB, start: 0, end: 99, write: true }),
        ("RenewCap", MetaOp::RenewCap { ino: 18, write: false }),
        ("ResolvePath", MetaOp::ResolvePath { path: b"/a/b".to_vec() }),
        ("Create", MetaOp::Create { parent: 16, name: n(), mode: 0o644, umask: 0o022, excl: true }),
        ("Mkdir", MetaOp::Mkdir { parent: 16, name: n(), mode: 0o755, umask: 0o027 }),
        ("Mknod", MetaOp::Mknod { parent: 16, name: n(), mode: 0o600, umask: 0o022, kind: 4, rdev: 0x0801 }),
        ("Symlink", MetaOp::Symlink { parent: 16, name: n(), target: b"../x".to_vec() }),
        ("Link", MetaOp::Link { ino: 18, new_parent: 17, new_name: b"f2".to_vec() }),
        ("Unlink", MetaOp::Unlink { parent: 16, name: n() }),
        ("Rmdir", MetaOp::Rmdir { parent: 16, name: b"d".to_vec() }),
        ("Rename", MetaOp::Rename { parent: 16, name: n(), new_parent: 17, new_name: b"g".to_vec(), flags: 1 }),
        (
            "SetAttr",
            MetaOp::SetAttr {
                ino: 18,
                set: SetAttrIn {
                    mode: Some(0o600),
                    uid: None,
                    gid: Some(1001),
                    size: Some(0),
                    atime: Some(TimeSet::Now),
                    mtime: Some(TimeSet::At(ts(9))),
                    epoch_seen: 3,
                },
            },
        ),
        ("SetXattr", MetaOp::SetXattr { ino: 18, name: b"user.k".to_vec(), value: b"v".to_vec(), flags: 1 }),
        ("RemoveXattr", MetaOp::RemoveXattr { ino: 18, name: b"user.k".to_vec() }),
        ("SetAtime", MetaOp::SetAtime { items: vec![(18, ts(10))] }),
        (
            "CommitWrite",
            MetaOp::CommitWrite {
                ino: 18,
                epoch: 3,
                new_size: 5,
                mtime: ts(11),
                inline_patches: vec![(0, b"hello".to_vec())],
                uninline_expect_change_id: Some(9),
                want_quota: true,
                close: false,
            },
        ),
        ("AppendReserve", MetaOp::AppendReserve { ino: 18, epoch: 3, len: 4096 }),
        ("Fallocate", MetaOp::Fallocate { ino: 18, offset: 0, len: 1 << 20, mode: 0 }),
        ("SetLock", MetaOp::SetLock { ino: 18, owner: 0xBB, pid: 4242, start: 0, end: u64::MAX >> 1, kind: 2 }),
        ("DebugPut", MetaOp::DebugPut { key: b"hello".to_vec(), value: b"world".to_vec() }),
        ("DebugGet", MetaOp::DebugGet { key: b"hello".to_vec() }),
        ("Uninline", MetaOp::Uninline { ino: 0x1_0000_0012 }), // M5 追加（ADR-0034）
        ("TruncateHandle", MetaOp::TruncateHandle { ino: 0x1_0000_0013, size: 6, epoch_seen: 3 }), // M6 追加（ADR-0036）
        ("ReopenHandle", MetaOp::ReopenHandle { ino: 0x1_0000_0014, write: true }), // M7 追加（ADR-0037 第 4、12 条）
    ]
}

#[test]
fn golden_meta() {
    let mut g = Golden::new("meta");
    let head = ReqHead { session: 0x5E55, seq: 7, uid: 1000, gid: 1001, groups: vec![27, 1002], pid: 4242, job_id: 88 };
    g.msg("MetaCall", &MetaCall { head, op: MetaOp::Lookup { parent: 1, name: b"home".to_vec() } });
    let ops = meta_ops();
    for (name, op) in &ops {
        assert_eq!(op.name(), *name, "MetaOp::name() 应等于变体名");
    }
    variants(&mut g, "MetaOp", ops, 36);
    assert_no_more_variants::<TimeSet>("TimeSet", 2); // TimeSet 的两个变体都出现在 SetAttr 的样本里
    let open = OpenOut {
        attr: attr_out(),
        layout: Some(layout()),
        cap: vec![1, 2, 3],
        inline: Some(b"hello".to_vec()),
        quota_end: u64::MAX,
        atime_due: true,
    };
    let dir = DirEntryOut { name: b"f1".to_vec(), ino: 18, kind: 1, attr: Some(attr_out()) };
    let replies = vec![
        ("Session", MetaReply::Session { lease_ms: 30_000, map_epoch: 9 }),
        ("Heartbeat", MetaReply::Heartbeat { lease_ms: 30_000, map_epoch: 9, need_full_open_set: true }),
        ("Entry", MetaReply::Entry { attr: attr_out() }),
        (
            "Dir",
            MetaReply::Dir {
                entries: vec![dir, DirEntryOut { name: b"d".to_vec(), ino: 19, kind: 2, attr: None }],
                eof: false,
            },
        ),
        ("Link", MetaReply::Link { target: b"../x".to_vec() }),
        ("Xattr", MetaReply::Xattr { value: b"v".to_vec() }),
        ("XattrNames", MetaReply::XattrNames { names: vec![b"user.k".to_vec()] }),
        (
            "StatFs",
            MetaReply::StatFs {
                bsize: 4096,
                blocks: 1 << 30,
                bfree: 1 << 29,
                files: 1 << 20,
                ffree: 1 << 19,
                namelen: 255,
            },
        ),
        ("Opened", MetaReply::Opened(open)),
        ("Inline", MetaReply::Inline { data: b"hello".to_vec(), change_id: 9 }),
        ("Lock", MetaReply::Lock { conflict: Some(LockOut { start: 0, end: 99, write: true, pid: 4343 }) }),
        ("Cap", MetaReply::Cap { cap: vec![4, 5, 6] }),
        ("Resolved", MetaReply::Resolved { ino: 18, kind: 1 }),
        ("Committed", MetaReply::Committed { attr: attr_out(), quota_end: 64 << 20 }),
        ("Offset", MetaReply::Offset { offset: 4096, attr: attr_out() }),
        ("Ok", MetaReply::Ok),
        ("DebugValue", MetaReply::DebugValue { value: Some(b"world".to_vec()) }),
    ];
    variants(&mut g, "MetaReply", replies, 17);
    g.check();
}

#[test]
fn golden_data() {
    let mut g = Golden::new("data");
    let c = chunk_id();
    g.msg(
        "DataCall",
        &DataCall { cap: vec![9; 20], op: DataOp::ReadChunk { chunk: c, chain_ver: 5, off: 0, len: 4096 } },
    );
    let ops = vec![
        ("ReadChunk", DataOp::ReadChunk { chunk: c, chain_ver: 5, off: 65536, len: 4096 }),
        ("WriteChunk", DataOp::WriteChunk { chunk: c, chain_ver: 5, extents: vec![(0, 100), (200, 50)], sync: true }),
        ("TruncateChunk", DataOp::TruncateChunk { chunk: c, chain_ver: 5, new_len: 6 }),
        ("SyncChunks", DataOp::SyncChunks { chain: 7, chain_ver: 5, chunks: vec![c] }),
        ("DeleteEpoch", DataOp::DeleteEpoch { chain: 7, chain_ver: 5, ino: 18, epoch: 2 }),
    ];
    for (name, op) in &ops {
        assert_eq!(op.name(), *name);
    }
    variants(&mut g, "DataOp", ops, 5);
    let replies = vec![
        ("Read", DataReply::Read { len: 4096, version: 1 << 40 }),
        ("Written", DataReply::Written { version: (1 << 40) + 1 }),
        ("Ok", DataReply::Ok),
        ("Deleted", DataReply::Deleted { files: 12 }),
    ];
    variants(&mut g, "DataReply", replies, 4);
    let cap = CapV1 {
        ino: 18,
        epoch: 4,
        rights: 0b011,
        session: 0x5E55,
        tenant: 2,
        job_id: 88,
        chains: vec![11, 12],
        class_id: 1,
        qos_id: 16,
        chunk_size: 4 << 20,
        block_size: 65536,
        checksum: 3,
        compress: 2,
        compress_level: 5,
        encrypt: 1,
        encrypt_key: 9,
        expires_ms: 1_792_455_000_000,
    };
    g.msg("CapV1", &cap);
    // M5：签好名的 cap = postcard(CapV1) ‖ HMAC-SHA256(K_cap, "WMFS-CAP-1" ‖ postcard)[0..16]（3.3.5），K_cap 取固定的测试值
    let signed = wmfs_core::proto::cap::sign(&[0x5A; 32], &cap);
    assert_eq!(wmfs_core::proto::cap::verify(&[0x5A; 32], &signed), Ok(cap.clone()));
    g.add("CapV1.signed", signed);
    g.check();
}

#[test]
fn golden_peer_and_node() {
    let mut g = Golden::new("peer");
    let c = chunk_id();
    let fw = ForwardWrite {
        chunk: c,
        chain_ver: 5,
        from_target: 1,
        base_version: 1 << 40,
        version: (1 << 40) + 1,
        full: false,
        header: vec![0xAB; 256],
        blocks: vec![(0, vec![0xCD; 72])],
        sync: true,
    };
    let ops = vec![
        ("ForwardWrite", PeerOp::ForwardWrite(fw)),
        ("ForwardSync", PeerOp::ForwardSync { chain: 7, chain_ver: 5, from_target: 1, chunks: vec![c] }),
        (
            "ForwardDeleteEpoch",
            PeerOp::ForwardDeleteEpoch { chain: 7, chain_ver: 5, from_target: 1, ino: 18, epoch: 2 },
        ),
        ("ListChunks", PeerOp::ListChunks { chain: 7, after: Some(c), limit: 1000 }),
        ("FetchChunk", PeerOp::FetchChunk { chunk: c }),
    ];
    for (name, op) in &ops {
        assert_eq!(op.name(), *name);
    }
    variants(&mut g, "PeerOp", ops, 5);
    let replies = vec![
        ("Ack", PeerReply::Ack { version: 1 << 40 }),
        ("NeedFull", PeerReply::NeedFull),
        ("Chunks", PeerReply::Chunks { items: vec![(c, 1 << 40)], more: true }),
        ("Fetched", PeerReply::Fetched { version: 1 << 40 }),
        ("Ok", PeerReply::Ok),
        ("Deleted", PeerReply::Deleted { files: 3 }),
    ];
    variants(&mut g, "PeerReply", replies, 6);
    let hb = NodeHeartbeat {
        node: 1,
        boot_id: 0xB007,
        host_id: "host-1".into(),
        machine_id: "m1".into(),
        map_epoch_seen: 9,
        targets: vec![TargetReport {
            target: 1,
            used: 1 << 30,
            capacity: 20 << 30,
            phys_devs: vec!["nvme0n1".into()],
            read_only: false,
        }],
        forward_failures: vec![(7, 2, 1_792_454_400_000)],
        features: vec!["proto.v1".into()],
        wall_ms: 1_792_454_400_100,
        qos_deficit: vec![(16, 25)],
    };
    let join = JoinReq {
        hostname: "spark".into(),
        host_id: "host-2".into(),
        machine_id: "m1".into(),
        addrs: vec![("tcp".into(), "127.0.0.1:19200".into())],
        mgmt_addr: "127.0.0.1:19280".into(),
        meta_role: 1,
        targets: vec![("/t".into(), vec!["nvme".into()], 20 << 30, vec!["nvme0n1".into()])],
        features: vec![],
        plugins: plugin_list().plugins,
    };
    let ops = vec![
        ("Heartbeat", NodeOp::Heartbeat(hb)),
        ("GetClusterMap", NodeOp::GetClusterMap { since_epoch: 8 }),
        ("Join", NodeOp::Join(join)),
        ("SyncDone", NodeOp::SyncDone { target: 3, chain: 7, chain_ver: 5 }),
        ("CheckEpochs", NodeOp::CheckEpochs { items: vec![(18, 2), (19, 1)] }),
        ("ReportCorrupt", NodeOp::ReportCorrupt { target: 1, chunk: c }),
    ];
    for (name, op) in &ops {
        assert_eq!(op.name(), *name);
    }
    variants(&mut g, "NodeOp", ops, 6);
    let replies = vec![
        ("HeartbeatOk", NodeReply::HeartbeatOk { lease_ms: 8000, map_epoch: 9, leader_wall_ms: 1_792_454_400_200 }),
        ("Map", NodeReply::Map(Box::new(cluster_map()))),
        ("Joined", NodeReply::Joined { node_id: 2, target_ids: vec![2], map: Box::new(cluster_map()) }),
        ("Ok", NodeReply::Ok),
        ("Epochs", NodeReply::Epochs { dead: vec![true, false] }),
    ];
    variants(&mut g, "NodeReply", replies, 5);
    g.msg("ClusterMap", &cluster_map());
    g.msg("SnapDesc", &SnapDesc { index: 1000, term: 3, source_node: 1 });
    g.msg("SnapshotFetch", &SnapshotFetch { index: 1000, term: 3 });
    g.msg(
        "SnapHeader",
        &SnapHeader { index: 1000, term: 3, state_version: 998, conf_state: vec![8, 1], pairs_total: 12345 },
    );
    g.msg("SnapTrailer", &SnapTrailer { pairs: 12345, crc32c: 0xDEAD_BEEF });
    g.check();
}

#[test]
fn golden_random_bodies_never_panic() {
    // 随机字节当消息体解码：只能得到错误，不能 panic（3.3.2：不认识的变体 → FEATURE_UNSUPPORTED，不断开）
    let mut r = wmfs_core::base::rand::XorShift64::new(2026);
    for _ in 0..20_000 {
        let mut b = vec![0u8; r.below(64) as usize];
        r.fill(&mut b);
        let _ = wmfs_core::proto::decode_body::<MetaCall>(&b);
        let _ = wmfs_core::proto::decode_body::<MetaReply>(&b);
        let _ = wmfs_core::proto::decode_body::<DataCall>(&b);
        let _ = wmfs_core::proto::decode_body::<PeerOp>(&b);
        let _ = wmfs_core::proto::decode_body::<NodeReply>(&b);
    }
    assert_eq!(
        wmfs_core::proto::decode_body::<MetaOp>(&[200]),
        Err(wmfs_core::base::ErrCode::FEATURE_UNSUPPORTED),
        "不认识的变体序号"
    );
    let mut ok = wmfs_core::proto::encode_body(&MetaOp::StatFs);
    ok.push(0);
    assert!(wmfs_core::proto::decode_body::<MetaOp>(&ok).is_err(), "多余的尾部字节也算错误");
}
