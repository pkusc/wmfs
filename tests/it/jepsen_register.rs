//! Jepsen-lite 初版（design.md 9.5 的检查器 ⑦，M9）：寄存器负载由 testkit 直接用 `DataClient` 读写**单个 chunk** 的前 8 字节（绕过
//! FUSE 与各级缓存），3 个客户端线程并发读写，同时一个"捣乱"线程随机 `kill -9` 再起来、或 `SIGSTOP` 一会儿（偶尔 9–12 s，比 target
//! 租约还久）再 `SIGCONT` 某个节点（`wmfs dev kill|start|pause|resume`：只碰本开发集群自己的进程），或者（M10，ADR-0040 第 11 条）
//! "提升"：别的节点换图慢一拍（fail point `chain.map.delay`）时重启一个节点，它以 Syncing 回链、同步完被提升为 last serving。
//! 记下每个操作的调用与返回时刻，用 Wing-Gong 回溯检查线性一致（`harness::linear`）。失败时打印种子（`WMFS_JEPSEN_SEED=<种子>`
//! 复现捣乱的顺序）。M15：寄存器负载挪进 `harness::register`、捣乱的命令挪进 `harness::dev`（chaos 的用例 `tests/it/chaos.rs` 共用）；
//! 客户端除重试外不得看到错误（发布门槛"所有故障注入测试中客户端除重试外无错误"：失败的操作数必须是 0）。M15 审查时发现的"回复丢了或 head
//! 宕机后重试覆盖更新写"已按 ADR-0046 第 1 条通过沿链数据写去重表修补（`data::dedup`、`craq_edges`、`failpoint_net` 回归覆盖）；本用例
//! 保持 M9 的 200 个操作（约 8 s、随机的一两次捣乱）以控制阶段回归时长，完整多故障场景由 `tests/it/chaos.rs` 覆盖。
//!
//! M10 审查（ADR-0041 第 13、14 条）另有一个用例：`wmfs node drain` 持有寄存器所在链的 last serving 的节点，同时在那个节点上设
//! `chain.map.delay` 拖住它换图——链管理器把这个活着的 L 先原地降为 Syncing、等它装上新图再移出，新 L 一个租约内对被占着的 chunk 回
//! BUSY。三个线程各用自己的客户端，其中一个只读、一直先问要被移出的那个节点（别的线程换了图它不跟着换），检查器照样判定线性一致——
//! 把链管理器临时改回一步移出时，这个用例实测判为不线性一致。drain 不能撤销，所以它单独一个用例、由验收最后跑（之后集群里那个节点不再
//! 有链）。
//!
//! M12：再加 strong 目录里单文件当寄存器的 FUSE 版本（9.5："M12 起再加"）：经两个挂载点 pwrite / pread，`scripts/accept.d/m12.sh` 跑。
//!
//! 对着一个跑着的 3 节点开发集群（固定端口）：`#[ignore]`，由 `scripts/accept.d/m9.sh`、`m10.sh` 在 `wmfs dev up --nodes 3` 之后跑
//! （环境变量 `WMFS_DEV_DIR`，`PATH` 里要有 `wmfs`）。检查器本身的单元测试照常跑。

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use wmfs_core::base::rand::XorShift64;
use wmfs_testkit::harness::dev::Dev;
use wmfs_testkit::harness::linear::{Kind, Op, linearizable};
use wmfs_testkit::harness::register::{now_us, seed, start};

#[test]
fn the_checker_accepts_linearizable_and_rejects_stale_reads() {
    let op = |call, ret, kind| Op { call, ret: Some(ret), kind };
    let ok =
        [op(0, 10, Kind::Write(1)), op(5, 15, Kind::Read(1)), op(20, 30, Kind::Read(1)), op(25, 40, Kind::Write(2))];
    assert!(linearizable(&ok, 0));
    let concurrent = [op(0, 10, Kind::Write(1)), op(1, 2, Kind::Read(0)), op(3, 4, Kind::Read(1))];
    assert!(linearizable(&concurrent, 0), "写还没返回：先读到旧值、再读到新值都可以");
    let stale = [op(0, 10, Kind::Write(1)), op(11, 12, Kind::Read(1)), op(13, 14, Kind::Read(0))];
    assert!(!linearizable(&stale, 0), "先新后旧：不线性一致");
    let lost = [op(0, 10, Kind::Write(1)), op(11, 12, Kind::Read(0))];
    assert!(!linearizable(&lost, 0), "写返回之后还读到旧值");
    let unknown = [Op { call: 0, ret: None, kind: Kind::Write(7) }, op(5, 6, Kind::Read(0)), op(8, 9, Kind::Read(7))];
    assert!(linearizable(&unknown, 0), "结果未知的写可以在任何时候生效");
    let never = [Op { call: 0, ret: None, kind: Kind::Write(7) }, op(5, 6, Kind::Read(0))];
    assert!(linearizable(&never, 0), "也可以从没发生");
}

#[test]
#[ignore = "要一个跑着的 3 节点开发集群：由 scripts/accept.sh M9 跑"]
fn a_register_on_one_chunk_stays_linearizable_while_nodes_die_and_freeze() {
    let (dev, seed) = (Dev::from_env(), seed());
    let reg = start(&dev.dir, seed, 200, false, 50); // 见文件头：运行长度照旧
    let faults = nemesis(&dev, seed, true, &|| reg.running());
    assert_eq!(reg.check(seed, &faults, 50), 0, "种子 {seed}：客户端看到了错误（除重试外不该有）");
}

/// 捣乱（见文件头）：直到 60 s 或 `go()` 为假，每隔 1.5–4 s 随机挑一个节点 kill 再起来、（`promote` 时）"提升"、或 pause 一会儿再 resume。
fn nemesis(dev: &Dev, seed: u64, promote: bool, go: &dyn Fn() -> bool) -> Vec<String> {
    let (mut r, t0) = (XorShift64::new(seed), Instant::now());
    let mut faults = Vec::new();
    while go() && t0.elapsed() < Duration::from_secs(60) {
        std::thread::sleep(Duration::from_millis(1500 + r.below(2500)));
        let n = 1 + r.below(3) as u32;
        let what = if promote { r.below(3) } else { [0, 2][r.below(2) as usize] }; // 0 kill、1 提升、2 pause
        if what == 0 {
            faults.push(format!("kill {n}"));
            dev.kill(n);
            std::thread::sleep(Duration::from_millis(500 + r.below(1500)));
            dev.start(n);
        } else if what == 1 {
            // M10（ADR-0040 第 11 条）的"提升"：别的节点换图慢一拍（fail point chain.map.delay：三成的心跳之前睡 2.5 s，租约照样有效），
            // 这个节点重启 → 以 Syncing 回链、同步完被提升为 last serving——旧的 L 还在旧图上的那一刻也进检查
            faults.push(format!("promote {n}"));
            let others: Vec<u32> = (1..=3).filter(|m| *m != n).collect();
            for m in &others {
                assert!(dev.failpoint(*m, "chain.map.delay", Some("pct(30,sleep(2500))")));
            }
            dev.kill(n);
            dev.start(n);
            let _ = dev.wmfs(&["dev", "wait", "chains-serving", "--timeout", "60"]);
            for m in &others {
                let _ = dev.failpoint(*m, "chain.map.delay", None);
            }
        } else {
            // 偶尔停得比 target 租约（8 s）还久：它先自我隔离（6.4 s，I5）、再被移出链，恢复之后以 Syncing 回来——这些路径也进检查
            let ms = if r.below(4) == 0 { 9000 + r.below(3000) } else { 1000 + r.below(3000) };
            faults.push(format!("pause {n} {ms} ms"));
            dev.pause(n);
            std::thread::sleep(Duration::from_millis(ms));
            dev.resume(n);
        }
    }
    faults
}

/// 见文件头：drain 持有寄存器所在链 L 的节点、同时拖住它换图（每次心跳之前睡 2.5 s，租约 6.4 s 照样续得上），链管理器两步移出这个活着
/// 的 L（ADR-0041 第 13 条）。等到这条链上没有那个节点的成员、再跑一会儿，检查线性一致。
#[test]
#[ignore = "要一个跑着的 3 节点开发集群，会 drain 掉一个节点（撤销不了）：由 scripts/accept.sh M10 最后跑"]
fn a_register_stays_linearizable_while_the_node_with_its_last_serving_is_drained() {
    let (dev, seed) = (Dev::from_env(), seed());
    let reg = start(&dev.dir, seed, 600, true, 50); // 足够跑到移出之后（检查完就停）
    let n = reg.l_node.expect("寄存器所在链的 L 所在的节点");
    let map = reg.c.meta.refresh_map().unwrap();
    let c0 = &map.chains.iter().find(|(id, _)| *id == reg.chunk.chain).expect("寄存器所在的链").1;
    let faults = vec![format!("链 {} = {:?}：drain 持有 L 的节点 {n}，它换图慢 2.5 s", reg.chunk.chain, c0.members)];
    println!("{}", faults[0]);
    std::thread::sleep(Duration::from_millis(1500)); // 先有一些操作
    assert!(dev.failpoint(n, "chain.map.delay", Some("sleep(2500)")));
    let cluster = "127.0.0.1:19180,127.0.0.1:19280,127.0.0.1:19380";
    let key = dev.dir.join("n1/keys/admin.key").display().to_string();
    let drain = ["node", "drain", &n.to_string(), "--cluster", cluster, "--key-file", &key];
    assert!(dev.wmfs(&drain), "wmfs node drain {n} 失败");
    let t0 = Instant::now();
    let gone = || {
        let Ok(m) = reg.c.meta.refresh_map() else { return false }; // 换 leader 途中（drain 的节点也许正是 leader）：下次再看
        let c = m.chains.iter().find(|(id, _)| *id == reg.chunk.chain).map(|(_, c)| c.members.clone());
        let on_n = |t: &u32| m.targets.iter().any(|(id, r)| id == t && r.node == n);
        c.is_some_and(|ms| !ms.iter().any(|(t, _)| on_n(t)))
    };
    while !gone() {
        assert!(t0.elapsed() < Duration::from_secs(90), "90 s 内节点 {n} 还在链 {} 上", reg.chunk.chain);
        std::thread::sleep(Duration::from_millis(100));
    }
    println!("{:?} 之后节点 {n} 已移出链 {}", t0.elapsed(), reg.chunk.chain);
    assert!(dev.failpoint(n, "chain.map.delay", None));
    let t1 = Instant::now();
    while reg.running() && t1.elapsed() < Duration::from_secs(3) {
        std::thread::sleep(Duration::from_millis(100)); // 移出之后再跑一会儿
    }
    reg.check(seed, &faults, 50);
}

/// M12（design.md 9.5 的检查器 ⑦ 的 FUSE 版本）：strong 目录里的一个文件当寄存器——3 个线程经两个挂载点（环境变量 `WMFS_JEPSEN_FILES` =
/// 两个挂载上这个文件的路径，逗号分隔）pwrite / pread 它的前 8 字节（strong：不缓存、读前线性一致地取长度、写同步写链并同步提交），捣乱同上
/// （kill、pause；不做"提升"）。失败的写结果未知、失败的读不进历史（与 DataClient 的版本相同）。
#[test]
#[ignore = "要一个跑着的 3 节点开发集群与两个挂载点：由 scripts/accept.sh M12 跑"]
fn a_strong_file_through_two_mounts_stays_linearizable_while_nodes_die_and_freeze() {
    use std::os::unix::fs::FileExt;
    // 先起 Dev、再打开挂载点上的文件：它跑捣乱命令的 shell 不能继承这些文件（harness::dev 的文件头）
    let (dev, seed) = (Dev::from_env(), seed());
    let files: Vec<PathBuf> =
        std::env::var("WMFS_JEPSEN_FILES").expect("WMFS_JEPSEN_FILES").split(',').map(PathBuf::from).collect();
    std::fs::write(&files[0], [0u8; 8]).unwrap();
    let (history, stop, t0) = (Arc::new(Mutex::new(Vec::new())), Arc::new(AtomicBool::new(false)), Instant::now());
    let workers: Vec<JoinHandle<()>> = (0..3u64)
        .map(|w| {
            let (path, history, stop) = (files[w as usize % files.len()].clone(), history.clone(), stop.clone());
            std::thread::spawn(move || {
                let f = std::fs::OpenOptions::new().read(true).write(true).open(&path).unwrap();
                let (mut r, mut n) = (XorShift64::new(seed ^ (w + 1)), 0u64);
                while !stop.load(Ordering::Relaxed) {
                    let call = now_us(t0);
                    let (kind, ok) = if r.below(2) == 0 {
                        n += 1;
                        let v = ((w + 1) << 32) | n;
                        (Kind::Write(v), f.write_at(&v.to_le_bytes(), 0).is_ok())
                    } else {
                        let mut b = [0u8; 8];
                        let ok = f.read_at(&mut b, 0).is_ok_and(|k| k == 8);
                        (Kind::Read(u64::from_le_bytes(b)), ok)
                    };
                    history.lock().unwrap().push(Op { call, ret: ok.then(|| now_us(t0)), kind });
                    std::thread::sleep(Duration::from_millis(50 + r.below(150)));
                }
            })
        })
        .collect();
    let faults = nemesis(&dev, seed, false, &|| history.lock().unwrap().len() < 300);
    stop.store(true, Ordering::Relaxed);
    for w in workers {
        w.join().unwrap();
    }
    let h = history.lock().unwrap().clone();
    let ok = h.iter().filter(|o| o.ret.is_some()).count();
    println!("历史：{} 个操作（{ok} 个确定的结果），捣乱：{faults:?}", h.len());
    assert!(ok >= 50, "种子 {seed}：成功的操作太少（{ok}）");
    assert!(linearizable(&h, 0), "种子 {seed}：经 FUSE 的 strong 寄存器的历史不是线性一致的：{h:?}");
    assert_eq!(h.len(), ok, "种子 {seed}：经 FUSE 的读写看到了错误（除重试外不该有）");
}
