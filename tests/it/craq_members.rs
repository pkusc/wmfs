//! 链成员变更时的读窗口（ADR-0041 第 13–15 条，design.md 3.4.2.5、3.4.2.10）：活着的成员两步移出（先降为 Syncing、它换上新图之后才
//! 移出）、新成为 last serving 的成员一个租约内对被占着的 chunk 回 `BUSY`（在规则 2 之前登记）、非 L 成员提交的那一下也重定向给 L。
//! 集群图由测试手工下发（`harness::craq`：只交给一部分节点 = 别的节点换图慢一拍），链的变化照 `chain_plan` 的规则写；组件测试里 target
//! 租约是 1 s。读一律直接问某个节点（不经 DataClient 的换副本与重定向），回的是已提交版本号：版本号大 = 新。不用 fail point。

use std::time::{Duration, Instant};

use wmfs_core::base::{ErrCode, clock};
use wmfs_core::meta::cluster::chains::{SERVING, SYNCING};
use wmfs_core::proto::data::{DataOp, DataReply};
use wmfs_testkit::harness::craq::{Craq, MemTargetStore, chain, data_call, id, inject_pending};

const S: u8 = SERVING;
const Y: u8 = SYNCING;

/// 直接问节点 `n` 读 chunk `idx`（链版本 `ver`）：回的已提交版本号，或错误码。
fn at(c: &Craq, n: u32, idx: u32, ver: u64) -> Result<u64, ErrCode> {
    match data_call(c, n, DataOp::ReadChunk { chunk: id(idx), chain_ver: ver, off: 0, len: 2 }) {
        Ok(DataReply::Read { version, .. }) => Ok(version),
        Ok(_) => Err(ErrCode::INTERNAL),
        Err((e, _)) => Err(e),
    }
}

/// 不论断言成败都放行拦截点：停在那里的线程不放行的话，`thread::scope` 一直等它，测试会挂住而不是失败。
struct Release<'a>(&'a MemTargetStore);

impl Drop for Release<'_> {
    fn drop(&mut self) {
        self.0.release();
    }
}

/// 等 `f()` 成立（最多 `secs` 秒）。
fn wait(what: &str, secs: u64, mut f: impl FnMut() -> bool) {
    let t0 = Instant::now();
    while !f() {
        assert!(t0.elapsed() < Duration::from_secs(secs), "{secs} s 内没等到：{what}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// 第 13 条：活着的中间成员两步移出。第一步的图 [1, 3, 2(Y)] 只交给了 1、3：t2 还在旧图上，按新图转发给它的写确认不了；这期间各处读到的
/// 都不新于已确认的写（t2 按旧图回它自己的已提交版本）。t2 换上新图之后写确认、它不再服务读；第二步移出它。
#[test]
fn a_live_middle_member_leaves_in_two_steps() {
    let c = Craq::start(3, &[(1, S), (2, S), (3, S)]);
    let v1 = c.write(0, 0, b"v1").unwrap();
    let e1 = c.new_map(&[(1, chain(&[(1, S), (3, S), (2, Y)], 2, 1))]);
    c.deliver(&e1, &[1, 3]);
    c.set_client_map(e1.clone());
    std::thread::scope(|sc| {
        let w = sc.spawn(|| c.write(0, 0, b"v2"));
        let t0 = Instant::now();
        while t0.elapsed() < Duration::from_millis(1500) {
            for (n, ver) in [(1, 2), (3, 2), (2, 1)] {
                let r = at(&c, n, 0, ver);
                assert!(r == Ok(v1) || r == Err(ErrCode::CHUNK_DIRTY), "t{n} 读到 {r:?}（已确认的是 {v1:#x}）");
            }
            assert!(!w.is_finished(), "t2 还在旧图上：写确认不了");
            std::thread::sleep(Duration::from_millis(20));
        }
        c.deliver(&e1, &[2]);
        let v2 = w.join().unwrap().unwrap();
        assert!(v2 > v1);
        assert_eq!((at(&c, 1, 0, 2), at(&c, 3, 0, 2)), (Ok(v2), Ok(v2)));
        assert_eq!(at(&c, 2, 0, 2), Err(ErrCode::NOT_SERVING), "t2 已是 Syncing：不服务读");
    });
    c.set_chains(&[(1, chain(&[(1, S), (3, S)], 3, 1))]); // 第二步：t2 报过新图之后移出
    let v3 = c.write(0, 0, b"v3").unwrap();
    assert_eq!((at(&c, 1, 0, 3), at(&c, 3, 0, 3)), (Ok(v3), Ok(v3)));
    c.check(Duration::from_secs(5));
}

/// 第 13、14 条：活着的链尾（L）两步移出，而 head 上有一个转发丢了的 pending（β）。原地降为 Syncing 的图只交给 head：head 成了新 L，
/// 规则 2 把 v2 在本地提交了——一个租约内它对这个 chunk 回 BUSY、不回 v2（旧 L 还在旧图上、回 v1）；干净的 chunk 照常读。旧 L 换上
/// 新图、确认 v2 之后 head 照常回 v2；第二步移出旧 L。
#[test]
fn a_live_tail_leaves_in_two_steps_and_the_new_l_holds_back_pending_chunks() {
    let c = Craq::start(2, &[(1, S), (2, S)]);
    let v1 = c.write(0, 0, b"v1").unwrap();
    let clean = c.write(1, 0, b"c1").unwrap();
    let v2 = inject_pending(&c, 1, 0, b"v2", 2); // head 写好 pending、"转发给了 t2"——转发丢了
    let e1 = c.new_map(&[(1, chain(&[(1, S), (2, Y)], 2, 1))]);
    c.deliver(&e1, &[1]);
    assert_eq!(c.committed_version(1, 0), Some(v2), "规则 2：新 L 在本地提交了 v2");
    assert_eq!(at(&c, 1, 0, 2), Err(ErrCode::BUSY), "新 L：有 pending 的 chunk 一个租约内回 BUSY（不回 v2）");
    assert_eq!(at(&c, 1, 1, 2), Ok(clean), "干净的 chunk 照常");
    assert_eq!(at(&c, 2, 0, 1), Ok(v1), "旧 L 还在旧图上：回 v1——新 L 没回过 v2，不是先新后旧");
    c.deliver(&e1, &[2]);
    c.set_client_map(e1);
    wait("旧 L 确认 v2、head 的 pending 解决", 5, || c.members[0].node.chunks.pendings(|_, _| true).is_empty());
    assert_eq!(at(&c, 1, 0, 2), Ok(v2), "pending 解决了：照常读到 v2");
    assert_eq!(c.committed_version(2, 0), Some(v2));
    assert_eq!(at(&c, 2, 0, 2), Err(ErrCode::NOT_SERVING), "旧 L 已是 Syncing：不服务读");
    c.set_chains(&[(1, chain(&[(1, S)], 3, 1))]);
    assert_eq!(&c.read(0, 2).unwrap(), b"v2");
}

/// 第 14 条的登记顺序：规则 2 在换图途中（切换视图之前）就把在途的 pending 本地提交，这时按旧图来的读（本节点在旧图里还不是 L、chunk
/// 也不再算脏）会读到旧 L 还没有的版本——窗口要在规则 2 之前登记。拦截点让换图停在第二个 chunk 的本地提交上。
#[test]
fn reads_on_the_old_map_in_the_middle_of_a_switch_see_busy_after_rule_2() {
    let c = Craq::start(2, &[(1, S), (2, S)]);
    for i in 0..4 {
        c.write(i, 0, b"v1").unwrap();
        inject_pending(&c, 1, i, b"v2", 2); // 转发都丢了
    }
    let order = c.members[0].node.chunks.pendings(|_, _| true); // 换图时依次处理的顺序
    let (first, second) = (order[0].idx, order[1].idx);
    let store = c.members[0].store.clone();
    store.hold_commit(Some(id(second)));
    let e1 = c.new_map(&[(1, chain(&[(1, S), (2, Y)], 2, 1))]);
    std::thread::scope(|sc| {
        let _release = Release(&store);
        let h = sc.spawn(|| c.deliver(&e1, &[1]));
        assert!(store.wait_held(Duration::from_secs(3)), "换图停在第二个 chunk 的本地提交上");
        let old = c.committed_version(2, first).unwrap();
        assert!(c.committed_version(1, first).unwrap() > old, "第一个已被规则 2 在本地提交");
        assert_eq!(at(&c, 1, first, 1), Err(ErrCode::BUSY), "按旧图读它：BUSY（不回旧 L 还没有的版本）");
        assert_eq!(at(&c, 1, second, 1), Err(ErrCode::BUSY), "正在提交的那个也是");
        assert_eq!(at(&c, 2, first, 1), Ok(old), "旧 L 回它的已提交版本");
        store.release();
        h.join().unwrap();
    });
    c.deliver(&e1, &[2]);
    c.set_client_map(e1);
    wait("pending 都解决", 5, || c.members[0].node.chunks.pendings(|_, _| true).is_empty());
}

/// 第 15 条：非 L 成员从取走 pending 到提交的 rename 完成之间，状态表里已没有 pending，已提交文件却还是旧版本，而 L 早已提交、可能已回给
/// 读者——这时读回 CHUNK_DIRTY 重定向给 L（以前回旧版本：L 回过 v2 之后它还回 v1）。拦截点让 head 停在提交上（像很慢的 fdatasync）。
#[test]
fn a_member_that_is_not_l_redirects_reads_while_it_commits() {
    let c = Craq::start(2, &[(1, S), (2, S)]);
    let v1 = c.write(0, 0, b"v1").unwrap();
    let store = c.members[0].store.clone();
    store.hold_commit(None);
    std::thread::scope(|sc| {
        let _release = Release(&store);
        let w = sc.spawn(|| c.write(0, 0, b"v2"));
        assert!(store.wait_held(Duration::from_secs(3)), "head 收到确认、停在提交上");
        let v2 = c.committed_version(2, 0).unwrap();
        assert!(v2 > v1 && at(&c, 2, 0, 1) == Ok(v2), "L 已提交 v2");
        let r = data_call(&c, 1, DataOp::ReadChunk { chunk: id(0), chain_ver: 1, off: 0, len: 2 }).err();
        assert_eq!(r, Some((ErrCode::CHUNK_DIRTY, 2)), "head 正在提交：改问 L");
        store.release();
        assert_eq!(w.join().unwrap(), Ok(v2));
    });
    assert_eq!(at(&c, 1, 0, 1), Ok(c.committed_version(2, 0).unwrap()));
}

/// 第 14 条的租约边界：新 L 的 BUSY 窗口是一个 target 租约（这里 1 s），旧 L 的租约（最后一次续租的发送时刻 + 0.8 × 租约）在窗口结束之前
/// 就到期、自我隔离——新 L 回 v2 的时候，旧 L 只回 FENCED。旧 L 若在换图之后续了租（回复带新图的 map_epoch），换上新图之前也是 FENCED（I5）。
#[test]
fn the_new_l_waits_until_the_old_l_has_fenced_itself() {
    let c = Craq::start(2, &[(1, S), (2, S)]);
    let v1 = c.write(0, 0, b"v1").unwrap();
    c.set_boot(2, 77);
    c.set_chains(&[(1, chain(&[(1, S), (2, S)], 1, 1))]); // 集群图里 t2 的 boot_id = 77（链不变）
    let old_l = &c.members[1].node;
    old_l.lease.enforce(77);
    old_l.lease.renew(clock::mono_ms(), 1000, old_l.map.epoch()); // 旧 L 在换图之前最后一次心跳成功
    let v2 = inject_pending(&c, 1, 0, b"v2", 2);
    let e1 = c.new_map(&[(1, chain(&[(1, S), (2, Y)], 2, 1))]);
    c.deliver(&e1, &[1]);
    let t0 = Instant::now();
    loop {
        let (new_l, old) = (at(&c, 1, 0, 2), at(&c, 2, 0, 1));
        assert!(old == Ok(v1) || old == Err(ErrCode::FENCED), "旧 L 读到 {old:?}");
        if new_l == Ok(v2) {
            assert_eq!(old, Err(ErrCode::FENCED), "{:?}：新 L 回 v2 之前旧 L 已自我隔离", t0.elapsed());
            break;
        }
        assert_eq!(new_l, Err(ErrCode::BUSY), "窗口里新 L 对这个 chunk 回 BUSY");
        assert!(t0.elapsed() < Duration::from_secs(3), "一个租约之后新 L 照常服务读");
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(t0.elapsed() >= Duration::from_millis(900), "窗口不短于租约：{:?}", t0.elapsed());
    assert_eq!(at(&c, 2, 0, 1), Err(ErrCode::FENCED));
    old_l.lease.renew(clock::mono_ms(), 1000, e1.map_epoch);
    assert_eq!(at(&c, 2, 0, 1), Err(ErrCode::FENCED), "续上了租、回复里的图比本地的新：换上之前不服务");
}
