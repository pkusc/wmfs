//! Jepsen-lite 初版（design.md 9.5 的检查器 ⑦，M9）：寄存器负载由 testkit 直接用 `DataClient` 读写**单个 chunk** 的前 8 字节（绕过
//! FUSE 与各级缓存），3 个客户端线程并发读写，同时一个"捣乱"线程随机 `kill -9` 再起来、或 `SIGSTOP` 一会儿（偶尔 9–12 s，比 target
//! 租约还久）再 `SIGCONT` 某个节点（`wmfs dev kill|start|pause|resume`：只碰本开发集群自己的进程）。记下每个操作的调用与返回时刻，用 Wing-Gong 回溯检查线性一致
//! （`harness::linear`）。失败时打印种子（`WMFS_JEPSEN_SEED=<种子>` 复现捣乱的顺序）。
//!
//! 对着一个跑着的 3 节点开发集群（固定端口）：`#[ignore]`，由 `scripts/accept.d/m9.sh` 在 `wmfs dev up --nodes 3` 之后跑
//! （环境变量 `WMFS_DEV_DIR`，`PATH` 里要有 `wmfs`）。检查器本身的单元测试照常跑。

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use wmfs_core::base::ErrCode;
use wmfs_core::base::rand::{XorShift64, random_u64};
use wmfs_core::buf::{Lane, PoolConfig};
use wmfs_core::client::data_client::chunk_id;
use wmfs_core::net::client::Endpoint;
use wmfs_core::proto::meta::MetaOp;
use wmfs_testkit::harness::linear::{Kind, Op, linearizable};
use wmfs_testkit::harness::node::{connect_seeds, opened, who};

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

fn wmfs(dir: &Path, args: &[&str]) -> bool {
    let o = Command::new("wmfs").args(args).env("WMFS_DEV_DIR", dir).output();
    o.is_ok_and(|o| o.status.success())
}

fn now_us(t0: Instant) -> u64 {
    t0.elapsed().as_micros() as u64
}

#[test]
#[ignore = "要一个跑着的 3 节点开发集群：由 scripts/accept.sh M9 跑"]
fn a_register_on_one_chunk_stays_linearizable_while_nodes_die_and_freeze() {
    let dir = PathBuf::from(std::env::var("WMFS_DEV_DIR").expect("WMFS_DEV_DIR"));
    let seed = std::env::var("WMFS_JEPSEN_SEED").ok().and_then(|s| s.parse().ok()).unwrap_or_else(random_u64);
    println!("Jepsen-lite 种子 {seed}（WMFS_JEPSEN_SEED={seed} 复现捣乱的顺序）");
    let seeds: Vec<Endpoint> =
        (1..=3).map(|n| Endpoint { node: n, addrs: vec![("tcp".into(), format!("127.0.0.1:19{n}00"))] }).collect();
    let c = Arc::new(connect_seeds(&dir.join("n1"), &seeds, "admin", PoolConfig::small()));
    let me = who(0, 0);
    let name = format!("jepsen-{}", std::process::id());
    let create = MetaOp::Create { parent: 1, name: name.into_bytes(), mode: 0o644, umask: 0, excl: true };
    let o = opened(c.meta.write(&me, create)).unwrap();
    let layout = o.layout.clone().unwrap();
    let chunk = chunk_id(&layout, o.attr.ino, 0).unwrap();
    let (cap, t0) = (o.cap.clone(), Instant::now());
    let history: Arc<Mutex<Vec<Op>>> = Arc::default();
    let (stop, count) = (Arc::new(AtomicBool::new(false)), Arc::new(AtomicUsize::new(0)));
    let workers: Vec<_> = (0..3u64)
        .map(|w| {
            let (c, cap, layout, history, stop, count) =
                (c.clone(), cap.clone(), layout.clone(), history.clone(), stop.clone(), count.clone());
            std::thread::spawn(move || {
                let mut r = XorShift64::new(seed ^ (w + 1));
                let mut n = 0u64;
                while !stop.load(Ordering::Relaxed) && count.fetch_add(1, Ordering::Relaxed) < 200 {
                    let call = now_us(t0);
                    let (kind, res) = if r.below(2) == 0 {
                        n += 1;
                        let v = ((w + 1) << 32) | n;
                        let mut b = c.data.pool().get(8, Lane::Normal, Duration::from_secs(1)).unwrap();
                        b.as_mut_slice()[..8].copy_from_slice(&v.to_le_bytes());
                        b.set_len(8);
                        (
                            Kind::Write(v),
                            c.data.write(&cap, chunk, vec![(0, 8)], vec![b.freeze().whole()], false).map(|_| v),
                        )
                    } else {
                        let into = c.data.pool().get(8, Lane::Normal, Duration::from_secs(1)).unwrap();
                        let v = match c.data.read(&cap, &layout, chunk, 0, 8, into, None) {
                            Ok((b, len)) => {
                                let mut x = [0u8; 8];
                                x[..len as usize].copy_from_slice(&b.as_slice()[..len as usize]);
                                Ok(u64::from_le_bytes(x))
                            }
                            Err(ErrCode::CHUNK_NOT_FOUND) => Ok(0),
                            Err(e) => Err(e),
                        };
                        (Kind::Read(v.unwrap_or(0)), v)
                    };
                    let ret = res.is_ok().then(|| now_us(t0));
                    history.lock().unwrap().push(Op { call, ret, kind });
                    std::thread::sleep(Duration::from_millis(50 + r.below(150)));
                }
            })
        })
        .collect();
    let mut r = XorShift64::new(seed);
    let mut faults = Vec::new();
    while count.load(Ordering::Relaxed) < 200 && t0.elapsed() < Duration::from_secs(60) {
        std::thread::sleep(Duration::from_millis(1500 + r.below(2500)));
        let n = (1 + r.below(3)).to_string();
        if r.below(2) == 0 {
            faults.push(format!("kill {n}"));
            assert!(wmfs(&dir, &["dev", "kill", &n]));
            std::thread::sleep(Duration::from_millis(500 + r.below(1500)));
            assert!(wmfs(&dir, &["dev", "start", &n]), "node{n} 起不来");
        } else {
            // 偶尔停得比 target 租约（8 s）还久：它先自我隔离（6.4 s，I5）、再被移出链，恢复之后以 Syncing 回来——这些路径也进检查
            let ms = if r.below(4) == 0 { 9000 + r.below(3000) } else { 1000 + r.below(3000) };
            faults.push(format!("pause {n} {ms} ms"));
            assert!(wmfs(&dir, &["dev", "pause", &n]));
            std::thread::sleep(Duration::from_millis(ms));
            assert!(wmfs(&dir, &["dev", "resume", &n]));
        }
    }
    stop.store(true, Ordering::Relaxed);
    for w in workers {
        w.join().unwrap();
    }
    let h = history.lock().unwrap().clone();
    let ok = h.iter().filter(|o| o.ret.is_some()).count();
    println!("历史：{} 个操作（{ok} 个确定的结果），捣乱：{faults:?}", h.len());
    assert!(ok >= 50, "种子 {seed}：成功的操作太少（{ok}）");
    assert!(linearizable(&h, 0), "种子 {seed}：寄存器的历史不是线性一致的：{h:?}");
}
