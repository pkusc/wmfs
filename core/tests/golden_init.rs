//! 金样：init 写的 index = 1 初始状态（design.md 3.4.1.9、3.4.3 第 3 步、3.2.7 的"id 与计数器的初值"）——`raft_state` 的
//! 五个值与初始 KV 的每一个 (key, value)。固定输入（cluster_id、时间、属主、地址、插件清单）。规格没写、由 M3 按最小方案
//! 定下的初值（根的 parent = 0、目录的 nlink/size/change_id、default 类的 weight 1……，见 init/initial_kv.rs 的实现注记）
//! 也由它钉住：改了就是改了磁盘上的初始状态，需要 ADR。
//! 运行：`cargo test -p wmfs-core golden`；有意改动后：`WMFS_GOLDEN_UPDATE=1 cargo test -p wmfs-core golden`。

mod common;

use common::Golden;
use redb::{Database, ReadableTable};
use wmfs_core::api::hex::hex;
use wmfs_core::init::{InitialKv, initial_kv};
use wmfs_core::meta::raft::storage::{self, RAFT_STATE};
use wmfs_core::meta::schema::records::PluginListV1;

/// 固定输入。与 testkit 的 `tests/harness/meta.rs` 的 `sample_init()` 保持一致（core 用不了 testkit），改一处要改两处。
fn input() -> InitialKv {
    InitialKv {
        cluster_id: [0xc1; 16],
        created_ms: 1_792_454_400_123,
        owner: (1000, 1001),
        addrs: vec![("tcp".into(), "127.0.0.1:19100".into())],
        mgmt_addr: "127.0.0.1:19180".into(),
        host_id: "host-1".into(),
        machine_id: "0123456789abcdef0123456789abcdef".into(),
        hostname: "spark".into(),
        plugins: PluginListV1 { plugins: vec!["authn:shared-key".into()] },
    }
}

#[test]
fn golden_init_state() {
    let mut g = Golden::new("init_kv");
    let kv = initial_kv(&input());
    assert!(kv.windows(2).all(|w| w[0].0 < w[1].0), "按 key 排好序、不重复");
    let db = Database::builder().create_with_backend(redb::backends::InMemoryBackend::new()).unwrap();
    storage::bootstrap(&db, &[1], &kv).unwrap();
    let rtx = db.begin_read().unwrap();
    let st = rtx.open_table(RAFT_STATE).unwrap();
    for r in st.iter().unwrap() {
        let (k, v) = r.unwrap();
        g.add(&format!("raft_state.{}", k.value()), v.value().to_vec());
    }
    for (k, v) in kv {
        g.add(&format!("kv.{}", hex(&k)), v);
    }
    g.check();
}
