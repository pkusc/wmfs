//! 完整的元数据引擎（raft 线程 + 执行器线程，design.md 3.4.1、3.5.1）跑在模拟掉电的内存后端上：
//! 写、线性一致读、"单节点写入 → 掉电（kill -9 的严格版）→ 重启"不 fatal 且确认过的写都在、64 路并发写全部成功、
//! 服务端的 key 表读已应用的 KEYMETA、插件清单与集群清单不一致的 leader 不就绪（3.4.1.4 规则 3）、当不上 leader 的
//! 节点上读与写都回 NOT_LEADER（读一定先过读闸门，3.4.1.6）。

use std::time::Duration;

use raft::StateRole;
use wmfs_core::base::ErrCode;
use wmfs_core::base::types::Role;
use wmfs_core::hooks::traits::KeyLookup;
use wmfs_core::init::initial_keymeta;
use wmfs_core::meta::engine::txn::MetaEngine;
use wmfs_core::meta::schema::keys;
use wmfs_core::meta::schema::records::{KeyMetaV1, encode_v1};
use wmfs_core::proto::handshake::derive_secret;
use wmfs_core::proto::meta::MetaReply;
use wmfs_testkit::harness::mem_crash::MemCrashBackend;
use wmfs_testkit::harness::meta::{bootstrap, dkey, engine, get_raw, plugins, put_raw, sample_init, start_engine};

#[test]
fn writes_survive_power_loss_and_restart_is_not_fatal() {
    let b = bootstrap(&[1]);
    let eng = engine(&b, plugins());
    for i in 0..50u8 {
        assert_eq!(put_raw(&eng, dkey(&format!("k{i}")), vec![i]), Ok(MetaReply::Ok));
    }
    assert_eq!(get_raw(&eng, &dkey("k7")), Ok(Some(vec![7])));
    let after: MemCrashBackend = b.crash(); // 掉电：只留下 fsync 过的字节（引擎还在跑也无妨）
    eng.shutdown();
    let eng = engine(&after, plugins()); // 重启：不 fatal，campaign 后成为就绪 leader
    for i in 0..50u8 {
        assert_eq!(get_raw(&eng, &dkey(&format!("k{i}"))), Ok(Some(vec![i])), "确认过的第 {i} 个写在掉电后还在");
    }
    assert!(eng.status().applied > 50);
    eng.shutdown();
}

#[test]
fn concurrent_writers_all_succeed() {
    let eng = engine(&bootstrap(&[1]), plugins());
    let threads: Vec<_> = (0..64)
        .map(|t| {
            let eng = eng.clone();
            std::thread::spawn(move || {
                for i in 0..20 {
                    assert_eq!(put_raw(&eng, dkey(&format!("t{t}-{i}")), vec![t as u8, i]), Ok(MetaReply::Ok));
                }
            })
        })
        .collect();
    threads.into_iter().for_each(|t| t.join().unwrap());
    assert_eq!(get_raw(&eng, &dkey("t63-19")), Ok(Some(vec![63, 19])));
    let st = eng.status();
    assert!(st.applied < 64 * 20, "单飞批次自然攒批：1280 个写只用了 {} 个条目", st.applied);
    eng.shutdown();
}

#[test]
fn key_lookup_reads_keymeta_from_the_kv() {
    let eng = engine(&bootstrap(&[1]), plugins());
    let master = [9u8; 32];
    let keys = eng.keys(master);
    let created = sample_init().created_ms;
    assert_eq!(keys.meta(1), Some(initial_keymeta(created)[0].1.clone()), "key 1 = admin（init 写进 KV 的）");
    assert_eq!(keys.meta(1).map(|m| m.role), Some(Role::Admin.as_u8()));
    assert_eq!(keys.secret(2), Some(derive_secret(&master, 2)));
    assert!(keys.secret(0).is_some() && keys.meta(0).is_none(), "key 0 = 服务端之间，不在 KEYMETA 里");
    assert!(keys.secret(3).is_none() && keys.meta(3).is_none(), "KEYMETA 里没有的 key 不认");
    let revoked = KeyMetaV1 { revoked: true, ..initial_keymeta(created)[1].1.clone() };
    assert_eq!(put_raw(&eng, keys::keymeta(2), encode_v1(&revoked)), Ok(MetaReply::Ok));
    assert_eq!(keys.meta(2).map(|m| m.revoked), Some(true), "已应用的撤销立即对认证生效");
    eng.shutdown();
}

#[test]
fn a_leader_whose_plugins_differ_from_the_cluster_list_is_never_ready() {
    let mut mine = plugins();
    mine.plugins.push("authz:posix-mode".into()); // 本节点多装了一个授权插件
    let eng = start_engine(&bootstrap(&[1]), mine);
    std::thread::sleep(Duration::from_millis(300));
    let r = eng.role();
    assert!(r.role == StateRole::Leader && !r.ready, "当选了，但插件清单不一致就不就绪：{r:?}");
    assert_eq!(put_raw(&eng, dkey("x"), vec![1]), Err(ErrCode::NOT_READY), "执行器不建批次（3.4.1.4 规则 3）");
    eng.shutdown();
}

/// 线性一致读必须先过读闸门（3.4.1.6）：voters = [1, 2] 的节点 1 不会自己 campaign、永远当不上 leader——读与写都回
/// NOT_LEADER（单节点时读到的数据一样，测不出"跳过读闸门直接读本地"；到 M8 那就是被取代的旧 leader 读到旧数据）。
#[test]
fn a_node_that_is_not_leader_answers_not_leader_to_reads_and_writes() {
    let eng = start_engine(&bootstrap(&[1, 2]), plugins());
    assert_ne!(eng.role().role, StateRole::Leader, "节点 2 不存在，节点 1 当不上 leader");
    assert_eq!(get_raw(&eng, &dkey("x")), Err(ErrCode::NOT_LEADER), "读闸门：不是 leader → NOT_LEADER，不读本地");
    assert_eq!(put_raw(&eng, dkey("x"), vec![1]), Err(ErrCode::NOT_LEADER), "执行器：不是 leader → NOT_LEADER");
    eng.shutdown();
}
