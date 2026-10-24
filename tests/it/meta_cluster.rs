//! 元数据三副本（roadmap M8，design.md 3.4.1.7–3.4.1.9、3.7.2、7.1）：同一个进程里的 3 节点集群（`harness::cluster`）。
//! - 附录 A 的坑 P12：3 节点下提交 900 KiB 写集的批次成功（RaftMsg 的 protobuf 走 bulk，3.4.1.4），每个副本都应用了；
//! - 插件清单不同的节点 join 被拒绝并列出差异；
//! - 管理 API：follower 回 409 带 leader 的管理地址（只给 follower 的地址 ApiClient 照样成功），changelog 长轮询的等待者在本节点
//!   失去 leader 时立即返回（409/503，ADR-0038 第 2 条），不挂到超时；
//! - SIGTERM 的路径（`Running::stop`）先把 leader 让出去（不到一个选举超时就换好），客户端跟随新的 leader；剩下的副本状态相同；
//! - `wmfs node promote / remove`：第 4 个节点（只做数据节点）被指定为 voter 后先以 learner 加入、追上后提升；移除一个 voter 之后
//!   它离开配置、记录改成 none；
//! - 审查发现的几处（M8）：只做数据的节点（不在 raft 配置里）也找得到 leader——心跳正常、以它为 head 的链写得进去；两个节点 drain、
//!   唯一能接替的节点停了时，drain 中的 leader 不把 leader 让给死掉的节点，写照常成功；join 的重试（回复丢了、本地侧再发一次）拿回
//!   原来的节点号，不建幽灵节点。

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::{Duration, Instant};

use wmfs_core::api::auth::{auth_header, sign_input};
use wmfs_core::api::hex::hex;
use wmfs_core::api::types::{ChangelogPage, ClusterReply, KeyFile, NodeOpReply};
use wmfs_core::base::config::{NetConfig, NodeConfig};
use wmfs_core::base::{ErrCode, clock};
use wmfs_core::buf::{MemPool, PoolConfig};
use wmfs_core::init::read_master;
use wmfs_core::meta::engine::txn::MetaEngine;
use wmfs_core::meta::raft::membership::Members;
use wmfs_core::meta::schema::keys::{self, ROOT_INO};
use wmfs_core::meta::schema::records::{TargetRecV1, decode_v1};
use wmfs_core::net::client::{Endpoint, Family, RpcClient};
use wmfs_core::proto::frame::{FrameHeader, FrameKind, MSG_NODE_CALL, OutFrame};
use wmfs_core::proto::handshake::{SERVER_KEY, derive_secret, hmac_sha256};
use wmfs_core::proto::meta::{MetaOp, MetaReply};
use wmfs_core::proto::peer::{JOIN_META_NONE, JoinReq, NodeOp, NodeReply};
use wmfs_core::proto::{decode_body, encode_body};
use wmfs_mgr::api::ApiClient;
use wmfs_plugins::tcp::TcpTransport;
use wmfs_testkit::harness::cluster::Cluster;
use wmfs_testkit::harness::node::{get, plugins, put, who};

fn admin(c: &Cluster) -> KeyFile {
    KeyFile::parse(&std::fs::read_to_string(c.node_dir(1).join("keys/admin.key")).unwrap()).unwrap()
}

fn mkdir(cl: &wmfs_testkit::harness::node::Client, name: &str) -> Result<MetaReply, ErrCode> {
    let op = MetaOp::Mkdir { parent: ROOT_INO, name: name.as_bytes().to_vec(), mode: 0o755, umask: 0 };
    cl.meta.write(&who(1000, 1000), op)
}

#[test]
fn p12_a_900k_write_set_commits_on_three_nodes() {
    let c = Cluster::start("p12", 3);
    let cl = c.client("admin");
    let big = vec![0x5au8; 900 << 10];
    let op = MetaOp::DebugPut { key: b"big".to_vec(), value: big.clone() };
    assert_eq!(
        cl.meta.read(&who(0, 0), op),
        Ok(MetaReply::Ok),
        "900 KiB 的写集：一个 raft 条目，MsgAppend 远超过 body 的 1 MiB 上限之外的部分走 bulk"
    );
    // 两个并发的 600 KiB：多半落进同一个批次（单个条目约 1.2 MiB，超过帧 body 的 1 MiB 上限）
    let (a, b) = (c.client("admin"), c.client("admin"));
    let t = std::thread::spawn(move || {
        a.meta.read(&who(0, 0), MetaOp::DebugPut { key: b"a".to_vec(), value: vec![1; 600 << 10] })
    });
    assert_eq!(
        b.meta.read(&who(0, 0), MetaOp::DebugPut { key: b"b".to_vec(), value: vec![2; 600 << 10] }),
        Ok(MetaReply::Ok)
    );
    assert_eq!(t.join().unwrap(), Ok(MetaReply::Ok));
    c.wait_caught_up();
    for i in 1..=3 {
        let got = c.run(i).meta.local_get(&keys::debug(b"big")).unwrap();
        assert_eq!(got.as_deref(), Some(&big[..]), "node{i} 应用了 900 KiB 的批次");
        assert_eq!(c.run(i).meta.local_get(&keys::debug(b"b")).unwrap().map(|v| v.len()), Some(600 << 10));
    }
    let hashes: Vec<String> = (1..=3).map(|i| c.run(i).meta.meta_hash().unwrap().hash).collect();
    assert!(hashes.windows(2).all(|w| w[0] == w[1]), "各副本的状态相同：{hashes:?}");
}

#[test]
fn a_node_with_different_plugins_is_rejected_with_the_difference() {
    let mut c = Cluster::start("plugins", 1);
    let e = c.join(2, &["posix-mode"]).unwrap_err();
    assert!(e.rejected && e.code == ErrCode::EINVAL, "{e:?}");
    assert!(e.msg.contains("authz:posix-mode"), "列出差异：{}", e.msg);
    assert!(!c.node_dir(2).join("NODE").exists(), "被拒绝时没有建出节点");
    let st = c.run(1).meta.status();
    assert_eq!((st.voters.len(), st.learners.len()), (1, 0));
    c.join(2, &[]).expect("插件清单一致：照常加入");
    let deadline = Instant::now() + Duration::from_secs(20);
    while c.run(1).meta.status().learners != [2] {
        assert!(Instant::now() < deadline, "第 2 个节点应以 learner 加入（2 节点集群保持 1 voter + 1 learner）");
        std::thread::sleep(Duration::from_millis(20));
    }
    let r: ClusterReply = ApiClient::new(vec![c.run(1).mgmt_addr.clone()], admin(&c)).get("/api/v1/cluster").unwrap();
    assert!(r.warnings.iter().any(|w| w.contains("没有高可用")), "2 节点：警告元数据没有高可用：{:?}", r.warnings);
}

/// 一个带签名的 GET（不跟随 409，看原样的状态码）。返回 (状态码, body)。
fn raw_get(addr: &str, key: &KeyFile, path: &str) -> (u16, String) {
    const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    let ts = clock::wall_ms();
    let sig = hex(&hmac_sha256(&key.secret, &[sign_input("GET", path, ts, EMPTY_SHA256).as_bytes()]));
    let req = format!(
        "GET {path} HTTP/1.1\r\nHost: t\r\nConnection: close\r\nAuthorization: {}\r\n\r\n",
        auth_header(key.key_id, ts, &sig)
    );
    let mut s = TcpStream::connect(addr).unwrap();
    s.write_all(req.as_bytes()).unwrap();
    let mut resp = String::new();
    s.read_to_string(&mut resp).unwrap();
    (resp[9..12].parse().unwrap(), resp.split_once("\r\n\r\n").map(|(_, b)| b.to_string()).unwrap_or_default())
}

#[test]
fn followers_redirect_and_long_polls_end_when_leadership_is_lost() {
    let c = Cluster::start("mgmt", 3);
    let key = admin(&c);
    let l = c.wait_leader();
    let f = (1..=3).find(|i| *i != l).unwrap();
    let (s, b) = raw_get(&c.run(f).mgmt_addr, &key, "/api/v1/cluster");
    assert!(s == 409 && b.contains(&c.run(l).mgmt_addr), "follower 回 409 带 leader 的管理地址：{s} {b}");
    let r: ClusterReply = ApiClient::new(vec![c.run(f).mgmt_addr.clone()], key.clone()).get("/api/v1/cluster").unwrap();
    assert_eq!((r.leader as usize, r.voters.len()), (l, 3), "只给 follower 的地址：ApiClient 跟随 409");
    let page: ChangelogPage =
        ApiClient::new(vec![c.run(l).mgmt_addr.clone()], key.clone()).get("/api/v1/changelog?limit=1").unwrap();
    let (addr, k2) = (c.run(l).mgmt_addr.clone(), key.clone());
    let path = format!("/api/v1/changelog?after={}&wait_ms=20000", page.last_seq);
    let poll = std::thread::spawn(move || {
        let t0 = Instant::now();
        (raw_get(&addr, &k2, &path), t0.elapsed())
    });
    std::thread::sleep(Duration::from_millis(500)); // 长轮询已经挂上
    c.run(l).meta.transfer_leader(f as u32);
    let ((s, b), took) = poll.join().unwrap();
    // ADR-0038 第 2 条：409 一定带 leader 的管理地址（ApiClient 只跟随带 leader 的 409），不知道 leader 就是 503 not_leader
    let redirect = s == 409 && b.contains(&format!("http://{}", c.run(f).mgmt_addr));
    assert!(redirect || (s == 503 && b.contains("not_leader")), "失去 leader：409 带新 leader 的地址或 503：{s} {b}");
    assert!(took < Duration::from_secs(5), "立即返回，不挂到 wait_ms 到期：{took:?}");
}

/// 同一个进程里的节点停掉之后不能再起（它的元数据库还被别的线程开着），重启与追平在多进程的 `scripts/accept.d/m8.sh` 里测。
#[test]
fn stopping_the_leader_hands_over_and_clients_follow() {
    let mut c = Cluster::start("failover", 3);
    let cl = c.client("client");
    mkdir(&cl, "before").unwrap();
    let l = c.wait_leader();
    let t0 = Instant::now();
    c.stop(l); // SIGTERM 的路径：先 transfer_leader（最多 3 s）
    let nl = c.wait_leader();
    assert_ne!(nl, l, "换了 leader");
    // 不让位的话 follower 至少要等一个选举超时（≥ 1 s）才发起选举；让位只要几十毫秒
    assert!(t0.elapsed() < Duration::from_millis(900), "优雅退出时先把 leader 让出去：{:?}", t0.elapsed());
    for i in 0..20 {
        mkdir(&cl, &format!("after-{i}")).unwrap(); // 客户端跟随新的 leader（同一个 seq 重试）
    }
    c.wait_caught_up();
    let hashes: Vec<String> = c.nodes.iter().flatten().map(|r| r.meta.meta_hash().unwrap().hash).collect();
    assert!(hashes.len() == 2 && hashes[0] == hashes[1], "剩下的两个副本状态相同：{hashes:?}");
    let r = cl.meta.read(&who(1000, 1000), MetaOp::ReadDir { ino: ROOT_INO, after: vec![], limit: 1024, plus: false });
    let Ok(MetaReply::Dir { entries, .. }) = r else { panic!("{r:?}") };
    assert_eq!(entries.iter().filter(|e| e.name.starts_with(b"after-")).count(), 20);
}

#[test]
fn promote_adds_a_learner_then_a_voter_and_remove_takes_one_out() {
    let mut c = Cluster::start("members", 3);
    c.join(4, &[]).unwrap();
    let api = ApiClient::new(vec![c.run(1).mgmt_addr.clone()], admin(&c));
    let r: ClusterReply = api.get("/api/v1/cluster").unwrap();
    let n4 = r.nodes.iter().find(|n| n.node_id == 4).unwrap();
    assert_eq!(
        (n4.member.as_str(), n4.member_wanted.as_str()),
        ("none", "none"),
        "已有 3 个元数据成员：第 4 个只做数据节点"
    );
    let p: NodeOpReply =
        api.post("/api/v1/nodes/4/promote", &std::collections::BTreeMap::<String, String>::new()).unwrap();
    assert_eq!(p.node.member_wanted, "voter");
    c.wait_voters(4);
    let rm: NodeOpReply =
        api.post("/api/v1/nodes/3/remove", &std::collections::BTreeMap::<String, String>::new()).unwrap();
    assert_eq!(rm.node.state, "removed");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let r: ClusterReply = api.get("/api/v1/cluster").unwrap();
        let n3 = r.nodes.iter().find(|n| n.node_id == 3).unwrap();
        if r.voters == [1, 2, 4] && n3.member == "none" && n3.member_wanted == "none" {
            break;
        }
        assert!(Instant::now() < deadline, "节点 3 应离开配置：{r:?}");
        std::thread::sleep(Duration::from_millis(50));
    }
    let e = api
        .post::<_, NodeOpReply>("/api/v1/nodes/3/drain", &std::collections::BTreeMap::<String, String>::new())
        .unwrap_err();
    assert_eq!(e.code, "einval", "已经移除的节点：{e}");
}

/// 只做数据的节点（第 4 个：已有 3 个元数据成员）不在 raft 配置里、收不到 raft 消息，raft 不知道 leader：它的心跳线程按认识的节点与
/// leader_hint 找到 leader（`node_hb.rs`），target 的 boot_id 记上了、拿到了集群图，以它为 head 的链也写得进去。
#[test]
fn a_data_only_node_finds_the_leader_and_serves_its_chains() {
    let c = Cluster::start("data-only", 4);
    let l = c.wait_leader();
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let t: TargetRecV1 = decode_v1(&c.run(l).meta.local_get(&keys::target(4)).unwrap().unwrap()).unwrap();
        if t.boot_id != 0 {
            break; // node4 的 target 心跳到了 leader（同一轮里它也拉到了集群图）
        }
        assert!(Instant::now() < deadline, "node4 的 target 心跳 20 s 内没到 leader：{t:?}");
        std::thread::sleep(Duration::from_millis(50));
    }
    let cl = c.client("admin");
    let me = who(1000, 1000);
    let data = vec![0x4du8; 1 << 20];
    for i in 0..8 {
        let o = put(&cl, &me, ROOT_INO, &format!("big{i}"), &data).unwrap(); // 4 条链各有一个 head：总有文件落在 node4 上
        assert!(get(&cl, &me, o.attr.ino).unwrap() == data, "big{i} 读回来不对");
    }
}

/// 两个 follower 被 drain、唯一没在 drain 的 leader 停了：新 leader（被 drain 的）找不到活着的接替者，就不让位（以前它每秒把 leader
/// 让给死掉的节点，raft-rs 转让期间丢掉所有提议，写一直失败）。
#[test]
fn a_draining_leader_does_not_hand_over_to_a_dead_node() {
    let mut c = Cluster::start("drain", 3);
    let l = c.wait_leader();
    let api = ApiClient::new(vec![c.run(l).mgmt_addr.clone()], admin(&c));
    let no_body = std::collections::BTreeMap::<String, String>::new();
    for f in (1..=3).filter(|i| *i != l) {
        let _: NodeOpReply = api.post(&format!("/api/v1/nodes/{f}/drain"), &no_body).unwrap();
    }
    let cl = c.client("client");
    mkdir(&cl, "before").unwrap();
    c.stop(l); // 唯一能接替的就是它自己，没有人可以让：照常重新选举
    let nl = c.wait_leader();
    assert_ne!(nl, l);
    std::thread::sleep(Duration::from_millis(1500)); // 等新 leader 的 cluster-mgr 至少做过一轮成员变更（每 1 s 一次）
    let t0 = Instant::now();
    for i in 0..10 {
        mkdir(&cl, &format!("after-{i}")).unwrap();
    }
    assert!(t0.elapsed() < Duration::from_secs(5), "drain 中的 leader 留在原位，写照常成功：{:?}", t0.elapsed());
    assert_eq!(c.leader(), Some(nl), "没有活着的接替者：不让位");
}

/// 用服务端身份（key 0）发一次 `NodeOp::Join`：(节点号, target 号)，或错误码。
fn send_join(c: &Cluster, req: &JoinReq) -> Result<(u32, Vec<u32>), ErrCode> {
    let key = KeyFile { key_id: SERVER_KEY, secret: derive_secret(&read_master(&c.node_dir(1)).unwrap(), SERVER_KEY) };
    let tcp: Arc<dyn wmfs_core::hooks::traits::Transport> =
        Arc::new(TcpTransport::new(&Default::default(), 5000).unwrap());
    let rpc =
        RpcClient::new(MemPool::new(&PoolConfig::small()).unwrap(), vec![tcp], key, NetConfig::default()).unwrap();
    let ep = Endpoint { node: 1, addrs: c.run(1).addrs.clone() };
    let hdr = FrameHeader::new(FrameKind::Request, MSG_NODE_CALL, 0, 0);
    let f = OutFrame { hdr, body: encode_body(&NodeOp::Join(req.clone())), bulk: vec![], recv_into: None };
    let r = rpc.call(&ep, Family::Control, f, Duration::from_secs(10))?;
    if let Some(e) = r.error() {
        return Err(ErrCode(e.code));
    }
    match decode_body::<NodeReply>(&r.body)? {
        NodeReply::Joined { node_id, target_ids, .. } => Ok((node_id, target_ids)),
        other => panic!("Join 的回复不对：{other:?}"),
    }
}

/// join 的重试（审查发现）：批次已经提交、`Joined` 回复丢了，本地侧按 3.3.8 重试——同一个请求再发一次拿回原来的节点号与 target 号，
/// 不建第二个节点；地址相同而别的不同（例如丢了数据目录、换了主机名的旧节点）照旧 `EEXIST`。
#[test]
fn a_retried_join_gets_its_node_back() {
    let c = Cluster::start("rejoin", 1);
    let req = JoinReq {
        hostname: "h9".into(),
        host_id: "host-9".into(),
        machine_id: "m9".into(),
        addrs: vec![("tcp".into(), "127.0.0.1:1".into())], // 这个节点不会真的起来：只看 leader 怎么记账
        mgmt_addr: "127.0.0.1:2".into(),
        meta_role: JOIN_META_NONE,
        targets: vec![("/nonexistent/wmfs-t9".into(), vec![], 0, vec![])],
        features: vec![],
        plugins: plugins(&NodeConfig::default()).plugin_list().plugins,
    };
    let first = send_join(&c, &req).unwrap();
    assert_eq!(send_join(&c, &req), Ok(first.clone()), "同一个请求重试：回原来的节点号与 target 号");
    let other = JoinReq { hostname: "h10".into(), ..req.clone() };
    assert_eq!(send_join(&c, &other), Err(ErrCode::EEXIST), "地址相同、主机名不同：不是重试");
    let r: ClusterReply = ApiClient::new(vec![c.run(1).mgmt_addr.clone()], admin(&c)).get("/api/v1/cluster").unwrap();
    assert_eq!(r.nodes.len(), 2, "只多了一个节点：{:?}", r.nodes);
}
