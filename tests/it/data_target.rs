//! target 一级的检查（design.md 3.2.3、4.9、3.4.2.9 的 M5 注记；semantics.md 的容量上限），在真实目录上的 local-fs 里做：
//! `data::open_targets` 读超级块、打开 target（与服务端启动时一样），数据节点不经 RPC（`harness::data`）。
//! - 超级块里的 cluster_id 不是本集群的 → 拒绝打开（插错盘了）；
//! - 不认识的 ro_compat 位 → 只读服务：写与截断 `EROFS`，读照常；
//! - target 的已用到了 node.toml 的 `capacity` 上限 → 写 `ENOSPC`，截断照常。

use std::path::{Path, PathBuf};
use std::sync::Arc;

use wmfs_core::base::ErrCode;
use wmfs_core::base::config::TargetConfig;
use wmfs_core::data::superblock::TargetSuperblock;
use wmfs_core::data::{Target, open_targets};
use wmfs_core::hooks::traits::ChunkStore;
use wmfs_plugins::local_fs::LocalFs;
use wmfs_testkit::harness::data::{CLUSTER, node_with, read_at, truncate, write_at};

const CS: u32 = 256 << 10; // 4 个 64 KiB 的块

fn dir(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("wmfs-target-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// 在 `d` 里写 target 1 的超级块。
fn superblock(d: &Path, cluster_id: [u8; 16], ro_compat: u32) {
    let sb = TargetSuperblock {
        features_compat: 0,
        features_ro_compat: ro_compat,
        features_incompat: 0,
        cluster_id,
        target_id: 1,
        node_id: 1,
        created_ms: 0,
    };
    std::fs::write(d.join("TARGET"), sb.encode()).unwrap();
}

/// 像服务端启动时那样打开 `d`（容量上限 `capacity`，例如 "1M"）。
fn open(d: &Path, capacity: &str) -> Result<Target, String> {
    let cfg = [TargetConfig { path: d.display().to_string(), labels: vec![], capacity: capacity.into() }];
    let store: Arc<dyn ChunkStore> = Arc::new(LocalFs::new());
    open_targets(&store, &cfg, &CLUSTER).map(|mut v| v.remove(0))
}

#[test]
fn a_target_of_another_cluster_is_refused() {
    let d = dir("other");
    superblock(&d, [0xEE; 16], 0);
    let e = open(&d, "0").err().unwrap();
    assert!(e.contains("属于别的集群"), "{e}");
    superblock(&d, CLUSTER, 0);
    assert!(open(&d, "0").is_ok(), "本集群的照常打开");
    std::fs::remove_dir_all(&d).unwrap();
}

#[test]
fn an_unknown_ro_compat_bit_makes_the_target_read_only() {
    let d = dir("ro");
    superblock(&d, CLUSTER, 0);
    let t = open(&d, "0").unwrap();
    assert!(!t.read_only);
    write_at(&node_with(t, 1), CS, 0, &[(0, 100)], &[5; 100]).unwrap();
    superblock(&d, CLUSTER, 1); // ro_compat bit0：首发不认识任何 ro_compat 位
    let t = open(&d, "0").unwrap();
    assert!(t.read_only, "不认识的 ro_compat 位：只读服务");
    let node = node_with(t, 1);
    assert_eq!(write_at(&node, CS, 0, &[(0, 10)], &[1; 10]), Err(ErrCode::EROFS));
    assert_eq!(truncate(&node, CS, 0, 0), Err(ErrCode::EROFS));
    assert_eq!(read_at(&node, 0, 0, 100).unwrap().0, vec![5; 100], "读照常");
    std::fs::remove_dir_all(&d).unwrap();
}

#[test]
fn writes_stop_at_the_capacity_limit_but_truncates_still_work() {
    let d = dir("cap");
    superblock(&d, CLUSTER, 0);
    let t = open(&d, "1M").unwrap();
    assert_eq!(t.capacity, 1 << 20);
    let node = node_with(t, 1);
    let full = vec![1u8; CS as usize];
    let mut idx = 0;
    let e = loop {
        match write_at(&node, CS, idx, &[(0, CS)], &full) {
            Ok(_) => idx += 1,
            Err(e) => break e,
        }
        assert!(idx < 8, "写了 2 MiB 还没碰到 1 MiB 的上限");
    };
    assert_eq!(e, ErrCode::ENOSPC, "已用到了上限：写回 ENOSPC");
    assert!(idx as u64 * CS as u64 >= (1 << 20) - CS as u64, "到了上限才拒绝（已写 {idx} 个 chunk）");
    assert!(truncate(&node, CS, 0, 100).is_ok(), "截断照常（只会变小）");
    assert_eq!(read_at(&node, 0, 0, 200).unwrap().0[..100], [1u8; 100][..]);
    std::fs::remove_dir_all(&d).unwrap();
}

/// ADR-0038 第 5 条（M9）：调小租约时同一个批次在 GLOBAL `settings_prev` 里记下修改前的值（已经有就每项取较大的），调大不记；
/// 链管理器在（最近一次调小之后的）一个旧租约周期之后删掉它（授予方在这之前按 max(当前值, settings_prev) 判定过期）。
#[test]
fn shrinking_a_lease_records_the_previous_settings_until_one_old_lease_passes() {
    use wmfs_core::base::types::ReqCtx;
    use wmfs_core::meta::cluster::settings;
    use wmfs_core::meta::engine::txn::{TxnOp, txn_blocking};
    use wmfs_core::meta::schema::keys::{self, G_SETTINGS_PREV};
    use wmfs_core::meta::schema::records::{ClusterSettingsV1, decode_v1};
    use wmfs_core::proto::meta::MetaReply;
    use wmfs_testkit::harness::meta::get_raw;
    use wmfs_testkit::harness::node::{init, start, wait_for};
    let d = std::env::temp_dir().join(format!("wmfs-lease-prev-{}", std::process::id()));
    init(&d);
    let run = start(&d);
    let set = |name: &'static str, ms: u64| {
        let op: TxnOp = Box::new(move |v, c| settings::apply(v, c, &[(name.into(), ms)]).map(|_| MetaReply::Ok));
        txn_blocking(&*run.meta, ReqCtx::internal(), op).unwrap();
    };
    let prev = || {
        get_raw(&run.meta, &keys::global(G_SETTINGS_PREV)).unwrap().map(|b| decode_v1::<ClusterSettingsV1>(&b).unwrap())
    };
    set("session_lease", 45_000); // 调大：不记
    assert!(prev().is_none());
    set("target_lease", 4000); // 调小：记下修改前的值
    let p = prev().expect("调小时记下修改前的值");
    assert_eq!((p.target_lease_ms, p.session_lease_ms), (8000, 45_000));
    std::thread::sleep(std::time::Duration::from_secs(4));
    set("target_lease", 3000); // 再调小：每项取较大的；从这次修改起重新计一个旧租约周期
    assert_eq!(prev().unwrap().target_lease_ms, 8000);
    std::thread::sleep(std::time::Duration::from_secs(5));
    assert!(prev().is_some(), "第一次调小之后已过 9 s，但第二次之后还不到一个旧租约周期（8 s）：不删");
    assert!(wait_for(20, || prev().is_none()), "一个旧租约周期之后由链管理器删掉");
    run.stop();
    std::fs::remove_dir_all(&d).unwrap();
}
