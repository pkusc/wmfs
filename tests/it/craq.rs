//! CRAQ 组件测试（design.md 9.3）：3 个数据服务经 mem 传输互连，集群图由测试下发（链的变化按链管理器的规则——`chain_plan` 的函数）。
//! 每个用例最后跑不变式检查器：静止后所有 Serving 副本的已提交版本与内容逐字节相同（I6）。
//! - 写到链上每个成员（增量转发：部分覆盖时没改动的块从已提交版本复制）；
//! - 新成员以 Syncing 追加：它缺基础版本 → NeedFull → 完整转发；同步从 last serving 拉差异、删掉源没有的 chunk、交 SyncDone；
//!   之后变 Serving；
//! - 唯一幸存者重启（incarnation + 1，附录 C 的 C-8）与整链重启（选按链序最后一个原 Serving 成员）：重启前没 fsync 的提交丢了，
//!   回来的成员从幸存者重新同步，内容与幸存者相同（没有 incarnation 时它们会把新写当成"同版本的重复"吞掉）。

use std::sync::Arc;
use std::time::{Duration, Instant};

use plugin_mem_transport::MemTransport;
use wmfs_core::api::metrics;
use wmfs_core::base::types::Role;
use wmfs_core::base::{ErrCode, lock};
use wmfs_core::buf::Lane;
use wmfs_core::data::write::{self, JobKind, WriteJob};
use wmfs_core::hooks::traits::Transport;
use wmfs_core::meta::cluster::chain_plan::sole;
use wmfs_core::meta::cluster::chains::{SERVING, SYNCING};
use wmfs_core::net::client::{Family, RpcClient};
use wmfs_core::proto::frame::MSG_PING;
use wmfs_core::proto::peer::{ForwardWrite, PeerOp, PeerReply};
use wmfs_testkit::harness::craq::{CHUNK, Craq, chain, id, inject_pending, peer_call};
use wmfs_testkit::harness::{data, net};

const S: u8 = SERVING;
const Y: u8 = SYNCING;

fn bytes(seed: u8, n: usize) -> Vec<u8> {
    (0..n).map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed)).collect()
}

/// 等节点 `n` 的同步线程交出 (链 1, target n, chain_ver) 的 SyncDone（真集群里由 hb 线程报给 leader）。
fn wait_sync_done(c: &Craq, n: u32, ver: u64) {
    let t0 = Instant::now();
    while !lock(&c.members[n as usize - 1].node.sync_done).contains(&(1, n, ver)) {
        assert!(t0.elapsed() < Duration::from_secs(20), "node{n} 20 s 内没有同步完");
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn writes_reach_every_member_and_partial_overwrites_copy_unchanged_blocks() {
    let c = Craq::start(3, &[(1, S), (2, S), (3, S)]);
    for i in 0..4 {
        c.write(i, 0, &bytes(i as u8, CHUNK as usize)).unwrap();
    }
    c.write(1, 1000, b"patch").unwrap(); // 增量：只改了这一块，其余从已提交版本复制
    let mut want = bytes(1, CHUNK as usize);
    want[1000..1005].copy_from_slice(b"patch");
    assert_eq!(c.read(1, CHUNK).unwrap(), want);
    let v = c.check(Duration::from_secs(5));
    let base = 1u64 << 40; // V = max(committed + 1, incarnation << 40)，incarnation 从 1 起（3.4.2.2）
    assert_eq!((v.len(), v[&id(1)], v[&id(0)]), (4, base + 1, base), "每个成员上同版本同内容");
}

#[test]
fn a_new_member_syncs_from_the_last_serving_member_and_needs_full_forwards() {
    let c = Craq::start(3, &[(1, S), (2, S)]);
    for i in 0..6 {
        c.write(i, 0, &bytes(i as u8, CHUNK as usize)).unwrap();
    }
    c.members[2].store.put_committed(&id(9), b"stale".to_vec()); // 源没有的 chunk：同步要删掉它
    c.set_chains(&[(1, chain(&[(1, S), (2, S), (3, Y)], 2, 1))]);
    for i in 0..3 {
        c.write(i, 5, b"during-sync").unwrap(); // 同步期间的写：新成员缺基础版本 → NeedFull → 完整转发
    }
    c.write(10, 0, b"new chunk").unwrap();
    wait_sync_done(&c, 3, 2);
    assert!(c.members[0].node.craq.need_full.get() > 0 || c.members[1].node.craq.need_full.get() > 0, "走过 NeedFull");
    c.set_chains(&[(1, chain(&[(1, S), (2, S), (3, S)], 3, 1))]);
    let v = c.check(Duration::from_secs(5));
    assert!(!v.contains_key(&id(9)), "源没有的 chunk 被删掉了");
    assert_eq!(v.len(), 7);
    let mut want = bytes(2, CHUNK as usize);
    want[5..16].copy_from_slice(b"during-sync");
    assert_eq!(c.read(2, CHUNK).unwrap(), want);
}

/// 附录 C 的 C-8：唯一幸存者重启时丢了一个没 fsync 的提交；incarnation 加一之后新版本号 ≥ incarnation << 40，比别的成员手里的旧
/// 版本都大——它们重新同步时取回幸存者的内容，而不是把新写当成"同版本的重复"吞掉。
#[test]
fn a_restarted_sole_survivor_starts_a_new_incarnation_and_peers_resync() {
    let mut c = Craq::start(3, &[(1, S), (2, S), (3, S)]);
    c.write(0, 0, b"A-fsynced").unwrap();
    c.client.sync(&c.cap(), 1, vec![id(0)]).unwrap(); // SyncChunks 沿链传播：每个成员都落盘
    c.write(0, 0, b"B-not-synced").unwrap(); // v2：在每个成员上提交了，但没落盘
    c.set_chains(&[(1, chain(&[(1, S)], 2, 1))]); // 2、3 离线被移除：1 是唯一的成员
    c.crash(1); // 1 重启：丢了 v2
    let mut ch = chain(&[(1, S)], 2, 1);
    sole(&mut ch, 1); // 链管理器：唯一幸存者 → incarnation += 1
    ch.ver = 3;
    c.set_chains(&[(1, ch)]);
    let v = c.write(0, 0, b"C-new").unwrap();
    assert!(v >= 2 << 40, "新 incarnation 里的版本号：{v:#x}");
    c.set_chains(&[(1, chain(&[(1, S), (2, Y), (3, Y)], 4, 2))]); // 2、3 回来：以 Syncing 追加
    wait_sync_done(&c, 2, 4);
    c.set_chains(&[(1, chain(&[(1, S), (2, S), (3, Y)], 5, 2))]);
    wait_sync_done(&c, 3, 5);
    c.set_chains(&[(1, chain(&[(1, S), (2, S), (3, S)], 6, 2))]);
    c.check(Duration::from_secs(5));
    assert_eq!(&c.read(0, 12).unwrap()[..], b"C-newnced\0\0\0" as &[u8], "幸存者的内容（A 上改了 5 个字节；B 丢了）");
}

/// 整链同时重启（3.4.2.7）：都丢了没 fsync 的提交；幸存者是按链序最后一个原 Serving 成员，别的从它重新同步。
#[test]
fn a_whole_chain_restart_keeps_the_last_member_and_the_rest_resync() {
    let mut c = Craq::start(3, &[(1, S), (2, S), (3, S)]);
    for i in 0..3 {
        c.write(i, 0, &bytes(i as u8, CHUNK as usize)).unwrap();
    }
    c.client.sync(&c.cap(), 1, (0..3).map(id).collect()).unwrap();
    c.write(0, 0, b"lost on power failure").unwrap();
    for n in 1..=3 {
        c.crash(n);
    }
    let mut ch = chain(&[(1, S), (2, S), (3, S)], 1, 1);
    sole(&mut ch, 3); // 链管理器：按链序最后一个原 Serving 成员
    ch.ver = 2;
    assert_eq!(ch.members, vec![(3, S), (1, Y), (2, Y)]);
    c.set_chains(&[(1, ch)]);
    assert_eq!(
        c.read(0, CHUNK).unwrap(),
        bytes(0, CHUNK as usize),
        "没 fsync 的提交在整链掉电时丢了（semantics.md 2.5）"
    );
    c.write(1, 0, b"after restart").unwrap();
    wait_sync_done(&c, 1, 2);
    c.set_chains(&[(1, chain(&[(3, S), (1, S), (2, Y)], 3, 2))]);
    wait_sync_done(&c, 2, 3);
    c.set_chains(&[(1, chain(&[(3, S), (1, S), (2, S)], 4, 2))]);
    let v = c.check(Duration::from_secs(5));
    assert!(v[&id(1)] >= 2 << 40);
}

/// 3.4.2.6 的四条前滚规则（链变化时、切换视图之前）。
#[test]
fn chain_changes_roll_pending_versions_forward() {
    let c = Craq::start(4, &[(1, S), (2, S), (3, S)]);
    // 规则 1：被移出链 → 放弃 pending（删 pending 文件）
    inject_pending(&c, 2, 0, b"r1", 3);
    c.set_chains(&[(1, chain(&[(1, S), (3, S)], 2, 1))]);
    assert!(!c.members[1].node.chunks.has_dirty(&id(0)) && c.members[1].store.pending_count() == 0);
    assert_eq!(c.committed_version(2, 0), None);
    // 规则 3：成了尾 → 提交并确认
    c.set_chains(&[(1, chain(&[(1, S), (2, S), (3, S)], 3, 1))]);
    let v = inject_pending(&c, 2, 1, b"r3", 3);
    c.set_chains(&[(1, chain(&[(1, S), (2, S)], 4, 1))]);
    assert_eq!(c.committed_version(2, 1), Some(v));
    assert!(c.members[1].node.chunks.pendings(|_, _| true).is_empty());
    // 规则 2：成了新的 last serving、旧 L 不在了 → 立即本地提交（后继 target 4 在这张图里找不到地址：还在等它确认）
    c.set_chains(&[(1, chain(&[(1, S), (2, S), (3, S)], 5, 1))]);
    let v = inject_pending(&c, 2, 2, b"r2", 3);
    c.set_chains(&[(1, chain(&[(1, S), (2, S), (9, Y)], 6, 1))]);
    assert_eq!(c.committed_version(2, 2), Some(v), "本地已提交（暂时违反 I2，后面只有 Syncing 成员）");
    assert_eq!(c.members[1].node.chunks.with_pending(&id(2), None, |p| p.committed), Some(true), "仍等后面的成员确认");
    assert!(!c.members[1].node.chunks.has_dirty(&id(2)), "读不再重定向：L 返回它");
    // 规则 4：后继变了 → 完整重发给新后继（target 4），它确认之后提交
    c.set_chains(&[(1, chain(&[(1, S), (2, S), (3, S)], 7, 1))]);
    let v = inject_pending(&c, 2, 3, b"r4", 3);
    c.set_chains(&[(1, chain(&[(1, S), (2, S), (4, S)], 8, 1))]);
    let t0 = Instant::now();
    while c.committed_version(2, 3) != Some(v) {
        assert!(t0.elapsed() < Duration::from_secs(2), "规则 4：不等 chain-timer");
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(c.committed_version(4, 3), Some(v), "新后继拿到了完整的版本");
}

/// 3.4.2.4 的后继处理表：直接给中间的成员发 ForwardWrite（I4 的前驱与链版本检查、重复、缺基础版本）。
#[test]
fn the_successor_table() {
    let c = Craq::start(3, &[(1, S), (2, S), (3, Y)]);
    let head = &c.members[0].node;
    let mut b = head.pool.get(10, Lane::Normal, Duration::from_secs(1)).unwrap();
    b.as_mut_slice().copy_from_slice(b"0123456789");
    let kind = JobKind::Write { extents: vec![(0, 10)], sync: false };
    let job = WriteJob { chunk: id(0), target: 1, cap: data::cap(CHUNK), kind, bulk: Some(b), reply: None };
    let p = write::prepare(head, &job).unwrap().unwrap();
    let (header, blocks) = p.nv.forward_parts();
    let bulk = p.nv.into_bulk();
    let fw = |from: u32, ver: u64, base: u64| ForwardWrite {
        chunk: id(0),
        chain_ver: ver,
        from_target: from,
        base_version: base,
        version: p.version,
        full: false,
        header: header.clone(),
        blocks: blocks.clone(),
        sync: false,
    };
    let send = |to: u32, f: ForwardWrite| peer_call(&c, to, &PeerOp::ForwardWrite(f), bulk.clone());
    assert_eq!(send(2, fw(3, 1, 0)), Err((ErrCode::STALE_CHAIN, 1)), "不是当前前驱（I4）");
    assert_eq!(send(2, fw(1, 7, 0)).unwrap_err().0, ErrCode::STALE_CHAIN, "链版本不一致");
    assert_eq!(send(2, fw(1, 1, 99)), Err((ErrCode::INTERNAL, 0)), "Serving 成员缺基础版本：不变量破坏");
    assert_eq!(send(3, fw(2, 1, 99)), Ok(PeerReply::NeedFull), "Syncing 成员缺基础版本：要完整的");
    assert_eq!(send(2, fw(1, 1, 0)), Ok(PeerReply::Ack { version: p.version }), "正常：写 pending、转发给尾、确认");
    assert_eq!(send(2, fw(1, 1, 0)), Ok(PeerReply::Ack { version: p.version }), "重复：committed ≥ V");
    assert_eq!(c.committed_version(3, 0), Some(p.version), "尾（Syncing 也算）先提交");
}

/// 3.4.2.8 的 M9 注记 ①：同步源列出 chunk 时，有 pending 的报 pending 的版本，只有 pending 的也列出——那个写可能已经在后面的
/// Syncing 成员上提交了，同步不能把它换回旧的已提交版本、也不能把新 chunk 当成"源没有的"删掉。
#[test]
fn the_sync_source_lists_pending_versions() {
    let c = Craq::start(3, &[(1, S), (2, S), (3, Y)]);
    c.write(0, 0, b"old").unwrap();
    let v1 = inject_pending(&c, 2, 0, b"new", 3); // L = target 2 还在等尾确认 v1
    let v2 = inject_pending(&c, 2, 5, b"fresh", 3); // 只有 pending 的新 chunk
    assert!(v1 > c.committed_version(2, 0).unwrap());
    let op = PeerOp::ListChunks { chain: 1, after: None, limit: 1000 };
    let r = peer_call(&c, 2, &op, vec![]);
    assert_eq!(r, Ok(PeerReply::Chunks { items: vec![(id(0), v1), (id(5), v2)], more: false }));
}

/// 服务端之间按 data_scheme 选传输（3.3.3）：对端没有公布 data_scheme（例如要 rdma、对端只有 mem/tcp）→ 退回 control_scheme、计数
/// `wmfs_transport_fallback_total` 并写一行警告；客户端不静默降级（`FEATURE_UNSUPPORTED`）。
#[test]
fn servers_fall_back_to_the_control_scheme_with_a_warning() {
    let s = net::mem(&format!("fallback-{}", std::process::id()));
    let mut cfg = net::net("mem");
    cfg.data_scheme = "rdma".into();
    let mk = |role| {
        let t = vec![Arc::new(MemTransport::new()) as Arc<dyn Transport>];
        let key = net::key(if role == Role::Server { 0 } else { 2 });
        RpcClient::new_with_role(net::pool(), t, key, role, cfg.clone()).unwrap()
    };
    let r = mk(Role::Server).call(&s.ep, Family::Data, net::frame(MSG_PING, b"x"), net::T);
    assert!(r.is_ok(), "服务端之间：退回 mem（control_scheme）");
    let fb = metrics::counter(
        "wmfs_transport_fallback_total",
        "服务端之间数据族退回 control_scheme 的次数",
        &[("from", "rdma"), ("to", "mem")],
    );
    assert!(fb.get() >= 1);
    let r = mk(Role::Client).call(&s.ep, Family::Data, net::frame(MSG_PING, b"x"), net::T);
    assert_eq!(r.err(), Some(ErrCode::FEATURE_UNSUPPORTED), "客户端：报错，不静默降级");
}
