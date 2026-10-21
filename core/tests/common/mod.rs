//! 金样测试的公共部分：金样文件的读写与比对，以及各种记录的固定样本。
//!
//! 金样文件在仓库根目录的 `tests/golden/<名字>.golden`，每行 `<样本名> <hex>`，`#` 开头是注释。
//! 第一次生成或**有意**改了格式之后：`WMFS_GOLDEN_UPDATE=1 cargo test -p wmfs-core golden`，再看 `git diff tests/golden/`。
//! 格式一旦有数据在用就不能改：金样变了 = 线格式或磁盘格式变了，需要 ADR（design.md 3.2.11）。
//! 样本取值规则：同一个结构里**相邻的字段一定取不同的值**，其余字段也在取值范围允许时互不相同
//! （例如 codec 只能是 1、encrypt 只能是 0/1，这两个就只好相同），并让一部分整数超过 u32，
//! 这样字段顺序写错、两个字段对调、整数宽度变窄都会让金样不一致。
#![allow(dead_code)] // 每个测试文件只用到其中一部分

use std::fmt::Debug;
use std::path::PathBuf;

use serde::Serialize;
use serde::de::DeserializeOwned;
use wmfs_core::base::types::{ChunkId, Ts};
use wmfs_core::data::format::{CK_BLAKE3, CMP_ZSTD, ENC_AES256GCM};
use wmfs_core::meta::schema::records::*;
use wmfs_core::proto::meta::AttrOut;
use wmfs_core::proto::peer::ClusterMap;
use wmfs_core::proto::{decode_body, encode_body};

pub struct Golden {
    file: &'static str,
    items: Vec<(String, Vec<u8>)>,
}

fn golden_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../tests/golden")
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len() / 2).map(|i| u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).expect("金样文件里有非 hex 字符")).collect()
}

impl Golden {
    pub fn new(file: &'static str) -> Golden {
        Golden { file, items: Vec::new() }
    }

    /// 直接加一份字节（帧头、chunk 头这类手写布局）。
    pub fn add(&mut self, name: &str, bytes: Vec<u8>) {
        assert!(!self.items.iter().any(|(n, _)| n == name), "样本名重复：{name}");
        assert!(!name.contains(char::is_whitespace), "样本名不能含空白：{name}");
        self.items.push((name.to_string(), bytes));
    }

    /// 消息体（postcard）：顺便检查解码后与原值相等。
    pub fn msg<T: Serialize + DeserializeOwned + PartialEq + Debug>(&mut self, name: &str, v: &T) -> Vec<u8> {
        let b = encode_body(v);
        assert_eq!(&decode_body::<T>(&b).expect("消息解码失败"), v, "消息往返不一致：{name}");
        self.add(name, b.clone());
        b
    }

    /// 持久化记录（1 字节版本号 + postcard）：顺便检查解码后与原值相等。
    pub fn rec<T: Serialize + DeserializeOwned + PartialEq + Debug>(&mut self, name: &str, v: &T) {
        let b = encode_v1(v);
        assert_eq!(b[0], REC_V1, "记录必须以版本字节 1 开头：{name}");
        assert_eq!(&decode_v1::<T>(&b).expect("记录解码失败"), v, "记录往返不一致：{name}");
        self.add(name, b);
    }

    /// 与金样文件逐字节比对；设置了 WMFS_GOLDEN_UPDATE=1 时改为重写金样文件。
    pub fn check(self) {
        let path = golden_dir().join(format!("{}.golden", self.file));
        if std::env::var("WMFS_GOLDEN_UPDATE").as_deref() == Ok("1") {
            // 直接写 stderr 而不用 eprintln!：测试通过时 eprintln! 的输出会被测试框架吞掉，这条提醒必须让人看到
            let warn = format!(
                "!!! WMFS_GOLDEN_UPDATE=1：金样 {} 已被重写，这次没有比对。必须看 git diff tests/golden/ 确认只有预期的变化。\n",
                path.display()
            );
            let _ = std::io::Write::write_all(&mut std::io::stderr(), warn.as_bytes());
            let mut text = format!(
                "# wmfs 金样：{}（由 core/tests/golden_*.rs 生成，测试逐字节比对）\n\
                 # 每行：<样本名> <hex>。改动这里 = 改了线格式或磁盘格式，需要 ADR（design.md 3.2.11）。\n",
                self.file
            );
            for (n, b) in &self.items {
                text.push_str(&format!("{n} {}\n", hex(b)));
            }
            std::fs::write(&path, text).expect("写金样文件失败");
            return;
        }
        let text = std::fs::read_to_string(&path).unwrap_or_else(|_| {
            panic!(
                "金样文件 {} 不存在：第一次请运行 WMFS_GOLDEN_UPDATE=1 cargo test -p wmfs-core golden",
                path.display()
            )
        });
        let mut want: Vec<(String, Vec<u8>)> = Vec::new();
        for line in text.lines().filter(|l| !l.trim().is_empty() && !l.starts_with('#')) {
            let (n, h) = line.split_once(' ').unwrap_or((line, ""));
            want.push((n.to_string(), unhex(h.trim())));
        }
        let mut problems = Vec::new();
        for (n, got) in &self.items {
            match want.iter().find(|(w, _)| w == n) {
                None => problems.push(format!("{n}：金样里没有这个样本（新增的消息/记录？需要 ADR 后再更新金样）")),
                Some((_, exp)) if exp != got => {
                    let at = exp.iter().zip(got.iter()).position(|(a, b)| a != b).unwrap_or(exp.len().min(got.len()));
                    problems.push(format!(
                        "{n}：字节不一致（第一个不同在偏移 {at}；金样 {} 字节，现在 {} 字节）\n    金样：{}\n    现在：{}",
                        exp.len(),
                        got.len(),
                        hex(exp),
                        hex(got)
                    ));
                }
                Some(_) => {}
            }
        }
        for (n, _) in &want {
            if !self.items.iter().any(|(g, _)| g == n) {
                problems.push(format!("{n}：金样里有、代码里没有（删掉了某种消息/记录？）"));
            }
        }
        assert!(problems.is_empty(), "金样 {} 不一致：线格式或磁盘格式变了！\n{}", self.file, problems.join("\n"));
    }
}

/// 枚举的金样：样本按声明顺序排列，第 i 个样本的编码必须以字节 i 开头（postcard 的变体序号 = 声明顺序）；
/// 最后确认枚举没有第 `expect_count + 1` 个变体（[`assert_no_more_variants`]）。
pub fn variants<T: Serialize + DeserializeOwned + PartialEq + Debug>(
    g: &mut Golden,
    prefix: &str,
    items: Vec<(&str, T)>,
    expect_count: usize,
) {
    assert_eq!(items.len(), expect_count, "{prefix} 的样本数与期望的变体数不一致");
    for (i, (name, v)) in items.iter().enumerate() {
        let b = g.msg(&format!("{prefix}.{name}"), v);
        assert_eq!(b[0] as usize, i, "{prefix}.{name} 的变体序号应为 {i}");
    }
    assert_no_more_variants::<T>(prefix, expect_count);
}

/// 确认枚举 T 只有 `n` 个变体（序号 0..n）。原理：由整数、Vec、String、Option、bool、定长数组、结构体组成的变体
/// 都能从若干个 0 字节解出来，所以只要存在序号为 n 的变体，`[n, 0, 0, …]` 总有一个长度能解码成功。
/// 于是在枚举末尾加了变体却忘了给它加金样（和 ADR），这里会失败。
pub fn assert_no_more_variants<T: DeserializeOwned>(prefix: &str, n: usize) {
    assert!(n < 128, "变体序号按单字节 varint 处理");
    for len in 0..=256 {
        let mut b = vec![n as u8];
        b.resize(1 + len, 0);
        assert!(
            decode_body::<T>(&b).is_err(),
            "{prefix} 有第 {} 个变体（序号 {n}）却没有金样：给新变体加金样（并写 ADR）",
            n + 1
        );
    }
}

// ---------------- 固定样本 ----------------

pub fn ts(n: u32) -> Ts {
    Ts { secs: 1_792_454_400 + n as i64, nanos: 100_000 + n }
}

pub fn chunk_id() -> ChunkId {
    ChunkId { chain: 7, ino: 0x1_0000_0010, epoch: 2, idx: 5 }
}

pub fn layout() -> LayoutV1 {
    LayoutV1 {
        epoch: 5,
        chunk_size: 4 << 20,
        block_size: 65536,
        chains: vec![11, 12],
        replicas: 4,
        codec: 1,
        checksum: CK_BLAKE3,
        compress: CMP_ZSTD,
        compress_level: -2,
        encrypt: ENC_AES256GCM,
        encrypt_key: 9,
        inline_max: 4096,
    }
}

pub fn inode_file() -> InodeV1 {
    InodeV1 {
        kind: KIND_FILE,
        mode: 0o644,
        uid: 1000,
        gid: 1001,
        nlink: 2,
        size: 5 << 40, // 超过 u32：size 的宽度被改窄会被发现
        atime: ts(1),
        mtime: ts(2),
        ctime: ts(3),
        btime: ts(4),
        rdev: 0,
        parent: 16,
        name: b"data.bin".to_vec(),
        extra_links: vec![(17, b"link".to_vec())],
        flags: F_HAS_ACL,
        change_id: 42,
        class_id: 2,
        layout: Some(layout()),
        pending_epoch: 6,
        max_epoch: 7,
        trash_expire_ms: 0,
        symlink: vec![],
    }
}

pub fn attr_out() -> AttrOut {
    AttrOut {
        ino: 18,
        kind: KIND_FILE,
        mode: 0o640,
        uid: 1000,
        gid: 1001,
        nlink: 2,
        size: 1 << 33, // 超过 u32
        atime: ts(5),
        mtime: ts(6),
        ctime: ts(7),
        btime: ts(8),
        rdev: 3,
        change_id: 9,
        epoch: 4,
        consistency: 2,
        ttl_ms: 1000,
        flags: F_INLINE | F_HAS_ACL,
    }
}

pub fn node_rec() -> NodeRecV1 {
    NodeRecV1 {
        addrs: vec![("tcp".into(), "127.0.0.1:19100".into()), ("rdma".into(), "10.77.0.1:19101".into())],
        mgmt_addr: "127.0.0.1:19180".into(),
        host_id: "host-1".into(),
        machine_id: "7af66f30966a49b6886e00e2fce4b42f".into(),
        hostname: "spark".into(),
        meta_role: 2,
        features: vec!["proto.v1".into()],
        joined_ms: 1_792_454_400_000,
        state: 1,
    }
}

pub fn target_rec() -> TargetRecV1 {
    TargetRecV1 {
        node: 1,
        path: "/home/u/wmfs-dev/n1/targets/1".into(),
        labels: vec!["nvme".into()],
        phys_devs: vec!["nvme0n1".into()],
        capacity: 20 << 30,
        boot_id: 0xB007,
        state: 1,
        last_up_ms: 1_792_454_400_500,
        read_only: false,
    }
}

pub fn chain_rec() -> ChainRecV1 {
    ChainRecV1 {
        ver: 5,
        incarnation: 2,
        class_id: 1,
        replicas: 3,
        members: vec![(1, 1), (2, 1), (3, 2)],
        last_serving: Some(2),
        removed: vec![(4, 1_792_454_000_000)],
    }
}

pub fn storage_class() -> StorageClassV1 {
    StorageClassV1 {
        id: 1,
        name: "default".into(),
        label: "*".into(),
        dedicated: false,
        replica: 3,
        iops_min: 10,
        iops_max: 100,
        bw_min: 1 << 20,
        bw_max: 1 << 30,
        burst_ms: 30_000,
        weight: 5,
        latency_target_us: 500,
        priority: 2,
        failure_domain: 1,
        locality: 0,
        tier_to: Some("vhdd".into()),
        tier_when: Some("idle>30d".into()),
    }
}

pub fn key_meta() -> KeyMetaV1 {
    KeyMetaV1 {
        name: "admin".into(),
        role: 1,
        tenant: 0,
        uid_ranges: vec![(0, u32::MAX)],
        root_squash: false,
        path_caps: vec![(16, true)],
        revoked: false,
        created_ms: 1_792_454_400_001,
    }
}

pub fn data_key() -> DataKeyV1 {
    DataKeyV1 { name: "lab".into(), tenant: 3, wrapped: (0..60).collect() }
}

pub fn qos_spec() -> QosSpecV1 {
    QosSpecV1 { r: 100, w: 10, l: 1000, bw_max: 1 << 28, burst_ms: 5000 }
}

pub fn evicted() -> EvictedV1 {
    EvictedV1 { evicted_ms: 1_792_454_401_000, map_epoch: 77 }
}

pub fn plugin_list() -> PluginListV1 {
    PluginListV1 { plugins: vec!["authn:shared-key".into(), "authz:posix-mode".into(), "qos:fifo".into()] }
}

pub fn feature_set() -> FeatureSetV1 {
    FeatureSetV1 { compat: vec!["c1".into()], ro_compat: vec![], incompat: vec!["chunk.snapshots".into()] }
}

pub fn cluster_map() -> ClusterMap {
    ClusterMap {
        map_epoch: 9,
        cluster_id: [0xC1; 16],
        leader: Some(1),
        settings: ClusterSettingsV1::default(),
        features: feature_set(),
        nodes: vec![(1, node_rec())],
        targets: vec![(1, target_rec())],
        chains: vec![(1, chain_rec())],
        classes: vec![storage_class()],
        keys: vec![(1, key_meta())],
        datakeys: vec![(1, data_key())],
        qos: vec![(16, qos_spec())],
        evicted: vec![(0xE5, evicted())],
        plugins: plugin_list(),
        warnings: vec!["default 类声明 3 副本，只有 1 个故障域：新文件实际为 1 副本".into()],
    }
}
