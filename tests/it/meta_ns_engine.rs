//! 命名空间跑在完整的元数据引擎上（raft + 执行器线程，模拟掉电的内存后端）与真实的 RPC 上（tcp）：
//! ① 掉电重启之后命名空间完好（roadmap M4 的验收标准"重启后命名空间完好"）；② rename 对并发读者原子可见（"rename 原子性
//! 测试"：同一个读事务里两个名字恰好一个存在）；③ 会话 open / heartbeat / close 与去重（同一个 seq 重发得到原结果，不是
//! EEXIST），会话绑定打开它的 key；④ client key 的 uid 范围与 root_squash、admin 可以声明任意 uid，uid 范围不含本进程
//! uid 的 key 也开得了会话；⑤ 客户端跟随 NOT_LEADER 的 leader_hint，没有 hint 时换下一个认识的节点；⑥ 只有 server 角色
//! 拿到的集群图里有 keys。

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use wmfs_core::base::ErrCode;
use wmfs_core::base::types::{Principal, ReqCtx, Role, Ts};
use wmfs_core::hooks::traits::ConnIdentity;
use wmfs_core::meta::engine::EmbeddedEngine;
use wmfs_core::meta::engine::txn::{MetaEngine, txn_blocking};
use wmfs_core::meta::ns;
use wmfs_core::meta::schema::keys::{self, ROOT_INO};
use wmfs_core::meta::schema::records::{ChangeRecV1, KeyMetaV1, encode_v1};
use wmfs_core::net::client::{Endpoint, Family, check_reply};
use wmfs_core::proto::frame::{FrameHeader, FrameKind, MSG_META_CALL, MSG_NODE_CALL, OutFrame};
use wmfs_core::proto::meta::{MetaCall, MetaOp, MetaReply, ReqHead};
use wmfs_core::proto::peer::{ClusterMap, NodeOp, NodeReply};
use wmfs_core::proto::{decode_body, encode_body};
use wmfs_testkit::harness::meta::{bootstrap, engine, get_raw, plugins, put_raw, start_engine};
use wmfs_testkit::harness::meta_rpc::{caller, meta_client, meta_server};
use wmfs_testkit::harness::net::{T, client, key};

fn ctx(uid: u32) -> ReqCtx {
    let conn = Arc::new(ConnIdentity {
        key_id: 1,
        role: Role::Admin,
        tenant: 0,
        uid_ranges: vec![(0, u32::MAX)],
        root_squash: false,
        path_caps: vec![],
    });
    let who = Principal { uid, gid: uid, groups: vec![], tenant: 0, job_id: 0, conn };
    ReqCtx {
        who,
        session: 0,
        seq: 0,
        pid: 1,
        trace_id: 0,
        now: Ts { secs: 9, nanos: 0 },
        now_ms: 9000,
        internal: false,
    }
}

/// 经执行器做一个修改（seq 0：不经会话与去重，内部调用者的写法）。
fn write(eng: &EmbeddedEngine, op: MetaOp) -> Result<MetaReply, ErrCode> {
    txn_blocking(eng, ctx(1000), ns::txn_op(op).unwrap())
}

/// 线性一致读（先过读闸门）。
fn read(eng: &EmbeddedEngine, op: MetaOp) -> Result<MetaReply, ErrCode> {
    eng.read(&ctx(1000), &mut |v, c| ns::read(v, c, &op))
}

fn ino(r: Result<MetaReply, ErrCode>) -> u64 {
    match r {
        Ok(MetaReply::Entry { attr }) => attr.ino,
        Ok(MetaReply::Opened(o)) => o.attr.ino,
        Ok(MetaReply::Resolved { ino, .. }) => ino,
        other => panic!("意外的回复：{other:?}"),
    }
}

fn n(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

/// /a 下的全部目录项（名字、ino、种类、nlink）。
fn listing(eng: &EmbeddedEngine) -> Vec<(Vec<u8>, u64, u8, u32)> {
    let a = ino(read(eng, MetaOp::ResolvePath { path: n("/a") }));
    match read(eng, MetaOp::ReadDir { ino: a, after: vec![], limit: 100, plus: true }) {
        Ok(MetaReply::Dir { entries, eof: true }) => {
            entries.into_iter().map(|e| (e.name, e.ino, e.kind, e.attr.unwrap().nlink)).collect()
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn namespace_survives_power_loss() {
    let b = bootstrap(&[1]);
    let eng = engine(&b, plugins());
    let a = ino(write(&eng, MetaOp::Mkdir { parent: ROOT_INO, name: n("a"), mode: 0o755, umask: 0 }));
    let f = ino(write(&eng, MetaOp::Create { parent: a, name: n("f1"), mode: 0o644, umask: 0, excl: true }));
    write(&eng, MetaOp::Link { ino: f, new_parent: a, new_name: n("f2") }).unwrap();
    write(&eng, MetaOp::Rename { parent: a, name: n("f1"), new_parent: a, new_name: n("g"), flags: 0 }).unwrap();
    write(&eng, MetaOp::Symlink { parent: a, name: n("s"), target: n("g") }).unwrap();
    write(&eng, MetaOp::Mkdir { parent: a, name: n("d"), mode: 0o700, umask: 0 }).unwrap();
    let before = listing(&eng);
    assert_eq!(before.iter().map(|e| e.0.clone()).collect::<Vec<_>>(), vec![n("d"), n("f2"), n("g"), n("s")]);
    let after = b.crash(); // 掉电：只留下 fsync 过的字节
    eng.shutdown();
    let eng = engine(&after, plugins());
    assert_eq!(listing(&eng), before, "确认过的命名空间修改在掉电重启之后一个不少");
    assert_eq!(ino(read(&eng, MetaOp::ResolvePath { path: n("/a/f2") })), f);
    eng.shutdown();
}

#[test]
fn rename_is_atomic_for_concurrent_readers() {
    let eng = engine(&bootstrap(&[1]), plugins());
    let f = ino(write(&eng, MetaOp::Create { parent: ROOT_INO, name: n("x"), mode: 0o644, umask: 0, excl: true }));
    // 读者不停地读，每读一次把"看到的是不是 y"写进 saw_y；写者每做完一次 rename，等读者读到这次的结果再做下一次。
    // 这样读与 rename 一定交错、300 个中间状态都被检查过，不靠线程调度的运气：同一进程里别的测试在 mmap 内存池时，
    // 新线程可能几十 ms 才第一次跑起来，那时 300 次 rename 早就做完了，读者一次也没读到。
    let (done, saw_y) = (Arc::new(AtomicBool::new(false)), Arc::new(AtomicBool::new(false)));
    let (e2, d2, y2) = (eng.clone(), done.clone(), saw_y.clone());
    let reader = std::thread::spawn(move || {
        let mut seen = 0u64;
        while !d2.load(Ordering::Relaxed) {
            let mut found = Vec::new();
            e2.read(&ctx(1000), &mut |v, c| {
                found = ["x", "y"]
                    .iter()
                    .filter(|nm| ns::read(v, c, &MetaOp::Lookup { parent: ROOT_INO, name: n(nm) }).is_ok())
                    .map(|nm| nm.to_string())
                    .collect();
                Ok(MetaReply::Ok)
            })
            .unwrap();
            assert_eq!(found.len(), 1, "同一个读事务里 x、y 恰好一个存在：{found:?}");
            y2.store(found[0] == "y", Ordering::Relaxed);
            seen += 1;
        }
        seen
    });
    for i in 0..300 {
        let (from, to) = if i % 2 == 0 { ("x", "y") } else { ("y", "x") };
        write(
            &eng,
            MetaOp::Rename { parent: ROOT_INO, name: n(from), new_parent: ROOT_INO, new_name: n(to), flags: 0 },
        )
        .unwrap();
        // 读者 panic 退出了就不再等，下面 join 时报出它的错
        while saw_y.load(Ordering::Relaxed) != (to == "y") && !reader.is_finished() {
            std::thread::yield_now();
        }
    }
    done.store(true, Ordering::Relaxed);
    assert!(reader.join().unwrap() >= 300, "每次 rename 的结果都至少被一个读事务看到");
    assert_eq!(ino(read(&eng, MetaOp::ResolvePath { path: n("/x") })), f);
    eng.shutdown();
}

/// 一个原始的 MetaCall（自己选 session 与 seq），经 key `key_id` 的连接发出。
fn raw(ep: &Endpoint, key_id: u32, head: ReqHead, op: MetaOp) -> Result<MetaReply, ErrCode> {
    let role = if key_id == 1 { Role::Admin } else { Role::Client };
    let body = encode_body(&MetaCall { head, op });
    let f = OutFrame {
        hdr: FrameHeader::new(FrameKind::Request, MSG_META_CALL, 0, 0),
        body,
        bulk: vec![],
        recv_into: None,
    };
    decode_body(&client("tcp", key(key_id), role).call(ep, Family::Control, f, T).and_then(check_reply)?.body)
}

fn head(session: u64, seq: u64, uid: u32) -> ReqHead {
    ReqHead { session, seq, uid, gid: uid, groups: vec![], pid: 1, job_id: 0 }
}

#[test]
fn sessions_heartbeats_and_dedup_over_rpc() {
    let eng = engine(&bootstrap(&[1]), plugins());
    let s = meta_server(eng.clone(), None);
    let c = meta_client(std::slice::from_ref(&s.ep), 1, T);
    assert_eq!(c.map().map_epoch, 1, "connect 取了集群图");
    let mk = |nm: &str| MetaOp::Mkdir { parent: ROOT_INO, name: n(nm), mode: 0o755, umask: 0 };
    let first = c.write(&caller(1000, 1000), mk("d")).unwrap(); // seq 1
    let sid = c.session();
    assert_eq!(raw(&s.ep, 1, head(sid, 1, 1000), mk("d")), Ok(first), "同一个 seq 重发：去重表返回原结果");
    assert_eq!(raw(&s.ep, 1, head(sid, 2, 1000), mk("d")), Err(ErrCode::EEXIST), "新的 seq：照常执行");
    assert_eq!(raw(&s.ep, 1, head(sid, 0, 1000), mk("e")), Err(ErrCode::EINVAL), "修改型请求必须带 seq");
    match c.heartbeat(vec![(5, true)], vec![], None).unwrap() {
        MetaReply::Heartbeat { lease_ms, map_epoch, need_full_open_set } => {
            assert_eq!((lease_ms, map_epoch, need_full_open_set), (30_000, 1, false));
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(s.svc.live().open_files(sid), vec![(5, true)]);
    c.heartbeat(vec![], vec![5], None).unwrap();
    assert_eq!(s.svc.live().open_files(sid), vec![]);
    c.close();
    assert_eq!(raw(&s.ep, 1, head(sid, 3, 1000), mk("e")), Err(ErrCode::SESSION_EVICTED), "会话关闭之后");
    assert!(matches!(
        raw(&s.ep, 1, head(sid, 0, 1000), MetaOp::GetAttr { ino: ROOT_INO }),
        Ok(MetaReply::Entry { .. })
    ));
    let hb = MetaOp::SessionHeartbeat { opened: vec![], closed: vec![], full: None };
    assert_eq!(raw(&s.ep, 1, head(sid, 0, 1000), hb), Err(ErrCode::SESSION_EVICTED));
    let ops: Vec<&str> = (1..=4)
        .filter_map(|seq| get_raw(&eng, &keys::changelog(seq)).unwrap())
        .map(|b| {
            wmfs_core::api::ops::op_name(wmfs_core::meta::schema::records::decode_v1::<ChangeRecV1>(&b).unwrap().op)
        })
        .map(Option::unwrap)
        .collect();
    assert_eq!(ops, vec!["SessionOpen", "Mkdir", "SessionClose"]);
    eng.shutdown();
}

#[test]
fn a_session_belongs_to_the_key_that_opened_it() {
    let eng = engine(&bootstrap(&[1]), plugins());
    let s = meta_server(eng.clone(), None);
    let owner = meta_client(std::slice::from_ref(&s.ep), 1, T);
    let sid = owner.session();
    let mk = |nm: &str| MetaOp::Mkdir { parent: ROOT_INO, name: n(nm), mode: 0o755, umask: 0 };
    let hb = MetaOp::SessionHeartbeat { opened: vec![], closed: vec![], full: None };
    let reopen = MetaOp::SessionOpen {
        client_name: "x".into(),
        host: "h".into(),
        host_id: "h".into(),
        mount: "/".into(),
        version: "0".into(),
    };
    // key 2 拿着 key 1 的会话（session id 在线上是明文）：抢先用 seq 1、心跳、关闭、重新打开一律被拒
    assert_eq!(raw(&s.ep, 2, head(sid, 1, 1000), mk("intruder")), Err(ErrCode::EACCES), "修改");
    assert_eq!(raw(&s.ep, 2, head(sid, 0, 1000), hb), Err(ErrCode::EACCES), "心跳");
    assert_eq!(raw(&s.ep, 2, head(sid, 0, 1000), MetaOp::SessionClose), Err(ErrCode::EACCES), "关闭");
    assert_eq!(raw(&s.ep, 2, head(sid, 0, 1000), reopen), Err(ErrCode::EACCES), "重新打开");
    // 会话主人照常：它的 seq 1 执行的是它自己的 mkdir（没有被别人的请求占进去重表）
    assert!(matches!(owner.write(&caller(1000, 1000), mk("mine")), Ok(MetaReply::Entry { .. })));
    assert_eq!(read(&eng, MetaOp::Lookup { parent: ROOT_INO, name: n("intruder") }), Err(ErrCode::ENOENT));
    owner.close();
    eng.shutdown();
}

#[test]
fn client_keys_are_held_to_their_uid_range_and_root_squash() {
    let eng = engine(&bootstrap(&[1]), plugins());
    let km = |ranges: Vec<(u32, u32)>, squash| KeyMetaV1 {
        name: "k".into(),
        role: Role::Client.as_u8(),
        tenant: 0,
        uid_ranges: ranges,
        root_squash: squash,
        path_caps: vec![],
        revoked: false,
        created_ms: 0,
    };
    // 范围里没有本进程的 uid：会话操作不按 uid 范围检查（它们不代表任何用户），所以照样开得了会话
    put_raw(&eng, keys::keymeta(3), encode_v1(&km(vec![(5000, 5999)], false))).unwrap();
    put_raw(&eng, keys::keymeta(4), encode_v1(&km(vec![(0, 0)], true))).unwrap();
    let s = meta_server(eng.clone(), None);
    let mk = |nm: &str, parent| MetaOp::Mkdir { parent, name: n(nm), mode: 0o755, umask: 0 };
    let owner = |r: Result<MetaReply, ErrCode>| match r {
        Ok(MetaReply::Entry { attr }) => attr.uid,
        other => panic!("{other:?}"),
    };
    let c3 = meta_client(std::slice::from_ref(&s.ep), 3, T);
    assert_eq!(owner(c3.write(&caller(5500, 5500), mk("a", 17))), 5500, "范围内的 uid 被相信");
    assert_eq!(c3.write(&caller(7000, 7000), mk("b", 17)), Err(ErrCode::EACCES), "范围外的 uid 被拒绝");
    let c4 = meta_client(std::slice::from_ref(&s.ep), 4, T);
    assert_eq!(owner(c4.write(&caller(0, 0), mk("sq", 17))), 65534, "root_squash：root 映射成 nobody");
    assert_eq!(c4.write(&caller(0, 0), mk("r", ROOT_INO)), Err(ErrCode::EACCES), "squash 之后不再是 root");
    let admin = meta_client(std::slice::from_ref(&s.ep), 1, T);
    assert_eq!(owner(admin.write(&caller(7000, 7000), mk("adm", 17))), 7000, "admin 可以声明任意 uid（--as-uid）");
    c3.close();
    let ops: Vec<(String, u32)> = (1..=20)
        .filter_map(|seq| get_raw(&eng, &keys::changelog(seq)).unwrap())
        .map(|b| wmfs_core::meta::schema::records::decode_v1::<ChangeRecV1>(&b).unwrap())
        .filter(|r| r.session == c3.session())
        .map(|r| (wmfs_core::api::ops::op_name(r.op).unwrap().to_string(), r.uid))
        .collect();
    let want = [("SessionOpen", 65534), ("Mkdir", 5500), ("SessionClose", 65534)];
    assert_eq!(ops, want.map(|(o, u)| (o.to_string(), u)), "会话操作在 changelog 里记为 nobody");
    eng.shutdown();
}

/// 用 key `key_id`、以 `role` 发一次 GetClusterMap。
fn cluster_map(ep: &Endpoint, key_id: u32, role: Role) -> ClusterMap {
    let body = encode_body(&NodeOp::GetClusterMap { since_epoch: 0 });
    let hdr = FrameHeader::new(FrameKind::Request, MSG_NODE_CALL, 0, 0);
    let f = OutFrame { hdr, body, bulk: vec![], recv_into: None };
    let reply = client("tcp", key(key_id), role).call(ep, Family::Control, f, T).and_then(check_reply).unwrap();
    match decode_body::<NodeReply>(&reply.body).unwrap() {
        NodeReply::Map(m) => *m,
        other => panic!("GetClusterMap 的回复不对：{other:?}"),
    }
}

#[test]
fn only_servers_get_the_keys_in_the_cluster_map() {
    let eng = engine(&bootstrap(&[1]), plugins());
    let s = meta_server(eng.clone(), None);
    for (key_id, role) in [(1, Role::Admin), (2, Role::Client)] {
        let m = cluster_map(&s.ep, key_id, role);
        assert!(m.keys.is_empty() && m.datakeys.is_empty(), "{role:?} 拿到的集群图不含 keys、datakeys");
        assert_eq!(m.nodes.iter().map(|(id, _)| *id).collect::<Vec<_>>(), vec![1], "别的都在");
    }
    let m = cluster_map(&s.ep, 0, Role::Server);
    assert_eq!(m.keys.iter().map(|(id, _)| *id).collect::<Vec<_>>(), vec![1, 2], "server 角色拿到 KEYMETA");
    eng.shutdown();
}

#[test]
fn client_follows_the_leader_hint_and_rotates_without_one() {
    let leader_eng = engine(&bootstrap(&[1]), plugins());
    let leader = meta_server(leader_eng.clone(), None);
    let follower_eng = start_engine(&bootstrap(&[1, 2]), plugins()); // 两个 voter、另一个不存在：永远当不上 leader
    let with_hint = meta_server(follower_eng.clone(), Some(leader.addr()));
    let c = meta_client(std::slice::from_ref(&with_hint.ep), 1, Duration::from_secs(5));
    c.write(&caller(1000, 1000), MetaOp::Mkdir { parent: ROOT_INO, name: n("z"), mode: 0o755, umask: 0 }).unwrap();
    let zr = leader_eng.read(&ctx(1000), &mut |v, cx| ns::read(v, cx, &MetaOp::ResolvePath { path: n("/z") }));
    assert!(matches!(zr, Ok(MetaReply::Resolved { .. })), "跟随 hint 写到了 leader 上");
    let no_hint = meta_server(follower_eng.clone(), None);
    let c = meta_client(&[no_hint.ep.clone(), leader.ep.clone()], 1, Duration::from_secs(5));
    c.write(&caller(1000, 1000), MetaOp::Mkdir { parent: ROOT_INO, name: n("w"), mode: 0o755, umask: 0 }).unwrap();
    assert!(
        leader_eng
            .read(&ctx(1000), &mut |v, cx| ns::read(v, cx, &MetaOp::Lookup { parent: ROOT_INO, name: n("w") }))
            .is_ok()
    );
    follower_eng.shutdown();
    leader_eng.shutdown();
}
