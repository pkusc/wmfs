//! M12 的复制锁表与 AppendReserve（design.md 3.5.3、3.5.5、3.4.4 第 3 步），在完整节点上经 MetaClient 直接发请求（两个客户端 = 两个会话）：
//! - 冲突（区间相交、至少一个写、所有者不同）→ `EAGAIN`；同一个会话里不同的 owner 也冲突；读读共享；降级、解锁之后别人拿得到；GetLock 回
//!   第一个冲突的锁，"到文件尾"的 end 是 `LOCK_EOF`（= 内核的 OFFSET_MAX）；会话记录的 `locked_inos` 跟着变；
//! - SessionClose 释放这个会话的全部锁；驱逐：第一阶段之后锁还在，数据节点确认、第二阶段之后才释放；
//! - AppendReserve：并发的两个客户端分到的区间不重叠、首尾相接；epoch 不对 → `STALE_LAYOUT`；配额精确（`EDQUOT` 时长度不变）；
//! - `audit` 打开时，被拒绝的修改经执行器也写一条 changelog（result = 错误码）。

use std::sync::Arc;
use std::thread;

use wmfs_core::base::ErrCode;
use wmfs_core::buf::PoolConfig;
use wmfs_core::meta::evict;
use wmfs_core::meta::locks::{LOCK_READ, LOCK_UNLOCK, LOCK_WRITE};
use wmfs_core::meta::schema::keys::{self, T_CHANGELOG};
use wmfs_core::meta::schema::kv;
use wmfs_core::meta::schema::records::{ChangeRecV1, LOCK_EOF, SessionRecV1, decode_v1};
use wmfs_core::net::client::Endpoint;
use wmfs_core::proto::meta::{LockOut, MetaOp, MetaReply};
use wmfs_core::server::Running;
use wmfs_mgr::api::ApiClient;
use wmfs_testkit::harness::bg::{api, set};
use wmfs_testkit::harness::node::{
    Client, TempDir, client, connect_silent, init, node_dir, opened, put, start, wait_for, who,
};

const HOME: u64 = 16;

fn node(name: &str) -> (TempDir, Running, ApiClient) {
    let d = TempDir::new(&format!("locks-{name}"));
    init(&d.0);
    let run = start(&d.0);
    let a = api(&d.0, &run.mgmt_addr);
    (d, run, a)
}

fn lock(c: &Client, ino: u64, owner: u64, range: (u64, u64), kind: u8) -> Result<MetaReply, ErrCode> {
    let op = MetaOp::SetLock { ino, owner, pid: 40 + owner as u32, start: range.0, end: range.1, kind };
    c.meta.write(&who(1000, 1000), op)
}

fn getlk(c: &Client, ino: u64, range: (u64, u64), write: bool) -> Option<LockOut> {
    let op = MetaOp::GetLock { ino, owner: 99, start: range.0, end: range.1, write };
    match c.meta.read(&who(1000, 1000), op).unwrap() {
        MetaReply::Lock { conflict } => conflict,
        r => panic!("{r:?}"),
    }
}

fn locked_inos(run: &Running, c: &Client) -> Vec<u64> {
    let s: Option<SessionRecV1> =
        run.meta.local_get(&keys::session(c.meta.session())).unwrap().map(|b| decode_v1(&b).unwrap());
    s.map_or(vec![], |s| s.locked_inos)
}

#[test]
fn conflicts_sharing_getlk_and_session_close() {
    let (d, run, _api) = node("basic");
    let (a, b) = (client(&d.0, &run, "client"), client(&d.0, &run, "client"));
    let ino = put(&a, &who(1000, 1000), HOME, "f", b"x").unwrap().attr.ino;
    assert_eq!(lock(&a, ino, 1, (0, u64::MAX), LOCK_WRITE), Ok(MetaReply::Ok), "≥ i64::MAX 的 end 归一为到文件尾");
    assert_eq!(lock(&b, ino, 1, (10, 20), LOCK_READ), Err(ErrCode::EAGAIN), "别的会话：冲突");
    assert_eq!(lock(&a, ino, 2, (10, 20), LOCK_READ), Err(ErrCode::EAGAIN), "同一个会话里别的 owner 也冲突");
    let want = LockOut { start: 0, end: LOCK_EOF, write: true, pid: 41 };
    assert_eq!(getlk(&b, ino, (0, 0), false), Some(want), "F_GETLK 看得到到文件尾（l_len = 0）的冲突锁");
    assert_eq!(locked_inos(&run, &a), vec![ino], "会话记录记着它持有锁的 inode");
    // 降成读锁：读读共享；写锁仍冲突
    assert_eq!(lock(&a, ino, 1, (0, LOCK_EOF), LOCK_READ), Ok(MetaReply::Ok));
    assert_eq!(lock(&b, ino, 1, (10, 20), LOCK_READ), Ok(MetaReply::Ok));
    assert_eq!(lock(&b, ino, 1, (10, 20), LOCK_WRITE), Err(ErrCode::EAGAIN));
    assert_eq!(getlk(&b, ino, (0, 5), true).map(|l| l.write), Some(false));
    // 解锁之后别人升级得了；locked_inos 跟着清掉
    assert_eq!(lock(&a, ino, 1, (0, LOCK_EOF), LOCK_UNLOCK), Ok(MetaReply::Ok));
    assert_eq!(locked_inos(&run, &a), Vec::<u64>::new());
    assert_eq!(lock(&b, ino, 1, (10, 20), LOCK_WRITE), Ok(MetaReply::Ok));
    assert_eq!(lock(&a, ino, 1, (0, 9), LOCK_WRITE), Ok(MetaReply::Ok), "不相交的区间");
    // SessionClose 释放 b 的全部锁
    b.meta.close();
    assert_eq!(lock(&a, ino, 1, (0, LOCK_EOF), LOCK_WRITE), Ok(MetaReply::Ok));
    assert_eq!(lock(&a, ino, 1, (0, 5), 7), Err(ErrCode::EINVAL), "不认识的 kind");
    assert_eq!(lock(&a, ino, 1, (6, 5), LOCK_READ), Err(ErrCode::EINVAL), "start > end");
}

/// 驱逐（3.4.4）：失联的客户端持有写锁；第一阶段之后锁还在（数据节点还没确认），第二阶段之后才释放、别人拿得到。
#[test]
fn an_evicted_session_keeps_its_locks_until_phase_two() {
    let (d, run, _api) = node("evict");
    let a = client(&d.0, &run, "client");
    let ep = Endpoint { node: 1, addrs: run.addrs.clone() };
    let lost = connect_silent(&node_dir(&d.0), &[ep], "client", PoolConfig::small());
    let ino = put(&a, &who(1000, 1000), HOME, "f", b"x").unwrap().attr.ino;
    assert_eq!(lock(&lost, ino, 5, (0, LOCK_EOF), LOCK_WRITE), Ok(MetaReply::Ok));
    let sid = lost.meta.session();
    evict::evict(&*run.meta, sid, "测试：手动驱逐".into()).unwrap();
    assert_eq!(lock(&a, ino, 1, (0, LOCK_EOF), LOCK_WRITE), Err(ErrCode::EAGAIN), "第一阶段之后锁还在");
    assert!(wait_for(20, || lock(&a, ino, 1, (0, LOCK_EOF), LOCK_WRITE).is_ok()), "第二阶段释放之后拿得到");
    assert!(run.meta.local_get(&keys::session(sid)).unwrap().is_none(), "会话记录已删");
}

#[test]
fn append_reserve_hands_out_disjoint_ranges_and_checks_quota() {
    let (d, run, api) = node("append");
    let me = who(1000, 1000);
    let a = Arc::new(client(&d.0, &run, "client"));
    let mk = MetaOp::Mkdir { parent: HOME, name: b"ap".to_vec(), mode: 0o777, umask: 0 };
    a.meta.write(&me, mk).unwrap();
    set(&api, "/home/ap", "inline=0,quota=1M,quantum=1M").unwrap();
    let ap = match a.meta.read(&me, MetaOp::ResolvePath { path: b"/home/ap".to_vec() }).unwrap() {
        MetaReply::Resolved { ino, .. } => ino,
        r => panic!("{r:?}"),
    };
    let o = opened(
        a.meta.write(&me, MetaOp::Create { parent: ap, name: b"log".to_vec(), mode: 0o644, umask: 0, excl: true }),
    )
    .unwrap();
    let (ino, epoch) = (o.attr.ino, o.layout.unwrap().epoch);
    let reserve = move |c: &Client, len: u64, epoch: u32| match c
        .meta
        .write(&who(1000, 1000), MetaOp::AppendReserve { ino, epoch, len })
    {
        Ok(MetaReply::Offset { offset, attr }) => Ok((offset, attr.size)),
        Ok(r) => panic!("{r:?}"),
        Err(e) => Err(e),
    };
    assert_eq!(reserve(&a, 10, epoch), Ok((0, 10)));
    assert_eq!(reserve(&a, 5, epoch + 1), Err(ErrCode::STALE_LAYOUT), "epoch 不对");
    // 两个客户端并发地各分 50 次 7 字节：分到的区间不重叠、首尾相接
    let b = Arc::new(client(&d.0, &run, "client"));
    let go =
        |c: Arc<Client>| thread::spawn(move || (0..50).map(|_| reserve(&c, 7, epoch).unwrap().0).collect::<Vec<u64>>());
    let (ta, tb) = (go(a.clone()), go(b.clone()));
    let mut all: Vec<u64> = ta.join().unwrap().into_iter().chain(tb.join().unwrap()).collect();
    all.sort();
    assert_eq!(all, (0..100).map(|i| 10 + 7 * i).collect::<Vec<u64>>());
    // 配额精确：越过 1 MiB 的分配被拒绝，长度不变
    assert_eq!(reserve(&a, 1 << 20, epoch), Err(ErrCode::EDQUOT));
    assert_eq!(reserve(&a, 1, epoch), Ok((710, 711)), "拒绝的那次没改长度");
}

#[test]
fn rejected_changes_are_audited_through_the_executor() {
    let (d, run, api) = node("audit");
    let a = client(&d.0, &run, "client");
    set(&api, "/home", "audit=on").unwrap();
    let op = MetaOp::Unlink { parent: HOME, name: b"nope".to_vec() };
    assert_eq!(a.meta.write(&who(1000, 1000), op), Err(ErrCode::ENOENT));
    let mut last = None;
    run.meta
        .local_scan(&mut |v| {
            last = kv::scan_tag(v, T_CHANGELOG)?.pop().map(|(_, b)| decode_v1::<ChangeRecV1>(&b).unwrap());
            Ok(())
        })
        .unwrap();
    let e = last.unwrap();
    assert_eq!((e.result, e.audited, e.parent, e.name.as_slice()), (ErrCode::ENOENT.0, true, HOME, &b"nope"[..]));
}
