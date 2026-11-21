//! Jepsen-lite 的寄存器负载（design.md 9.5 的检查器 ⑦；M9 写在 `tests/it/jepsen_register.rs` 里，M15 挪到这里给 chaos 共用）：
//! 3 个客户端线程用 `DataClient` 直接读写**一个 chunk** 的前 8 字节（绕过 FUSE 与各级缓存；写的值各不相同），记下每个操作的调用与
//! 返回时刻，用 Wing-Gong 回溯检查线性一致（[`super::linear`]）。失败的操作结果未知：写可能生效也可能没生效，读不进历史——
//! [`Register::check`] 的返回值数的就是它们（DataClient 自己按 3.3.8 重试到期限，所以"客户端除重试外无错误"= 它是 0）。
//!
//! 对着一个跑着的 3 节点开发集群（固定端口 19n00；种子与 admin key 取自 `dir`）。

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use wmfs_core::base::ErrCode;
use wmfs_core::base::rand::{XorShift64, random_u64};
use wmfs_core::base::types::ChunkId;
use wmfs_core::buf::{Lane, PoolConfig};
use wmfs_core::client::data_client::chunk_id;
use wmfs_core::meta::cluster::chains::SERVING;
use wmfs_core::proto::meta::MetaOp;
use wmfs_core::proto::peer::ClusterMap;

use super::dev::dev_seeds;
use super::linear::{Kind, Op, linearizable};
use super::node::{Client, connect_seeds, opened, who};

pub fn now_us(t0: Instant) -> u64 {
    t0.elapsed().as_micros() as u64
}

/// 捣乱的种子：`WMFS_JEPSEN_SEED` 或随机（打印出来，失败时用它复现捣乱的顺序；线程调度本身不可复现）。
pub fn seed() -> u64 {
    let seed = std::env::var("WMFS_JEPSEN_SEED").ok().and_then(|s| s.parse().ok()).unwrap_or_else(random_u64);
    println!("Jepsen-lite 种子 {seed}（WMFS_JEPSEN_SEED={seed} 复现捣乱的顺序）");
    seed
}

/// 跑着的寄存器负载：3 个客户端线程一共最多 `max` 个操作，或者直到 `stop`。`l_node` 见 [`start`]。
pub struct Register {
    pub c: Arc<Client>,
    pub chunk: ChunkId,
    pub l_node: Option<u32>,
    history: Arc<Mutex<Vec<Op>>>,
    stop: Arc<AtomicBool>,
    count: Arc<AtomicUsize>,
    max: usize,
    workers: Vec<JoinHandle<()>>,
}

/// 连上开发集群 `dir`、建一个新文件，起 3 个读写它第一个 chunk 的线程（每个操作之后歇 `pace_ms` 到 4 倍 `pace_ms`）。
/// `at_l`：第 0 个线程只读、优先问这个 chunk 所在链的 L 所在的节点（[`Register::l_node`]），三个线程各用自己的客户端（各自的会话
/// 与集群图）——别的线程换了图、它不跟着换：一步移出活着的 L 时读到旧值的正是这样的读者（它不写，也就不会因为写回 STALE_CHAIN 而
/// 先换上新图）。
pub fn start(dir: &Path, seed: u64, max: usize, at_l: bool, pace_ms: u64) -> Register {
    let seeds = dev_seeds();
    let connect = || Arc::new(connect_seeds(&dir.join("n1"), &seeds, "admin", PoolConfig::small()));
    let c = connect();
    let clients: Vec<Arc<Client>> = (0..3).map(|_| if at_l { connect() } else { c.clone() }).collect();
    let me = who(0, 0);
    let name = format!("jepsen-{}-{}", std::process::id(), random_u64() % 1000);
    let create = MetaOp::Create { parent: 1, name: name.into_bytes(), mode: 0o644, umask: 0, excl: true };
    let o = opened(c.meta.write(&me, create)).unwrap();
    let layout = o.layout.clone().unwrap();
    let chunk = chunk_id(&layout, o.attr.ino, 0).unwrap();
    let l_node = if at_l { l_node(&c.meta.refresh_map().unwrap(), chunk.chain) } else { None };
    let (cap, t0) = (o.cap.clone(), Instant::now());
    let history: Arc<Mutex<Vec<Op>>> = Arc::default();
    let (stop, count) = (Arc::new(AtomicBool::new(false)), Arc::new(AtomicUsize::new(0)));
    let workers = (0..3u64)
        .map(|w| {
            let (c, cap, layout, history, stop, count) = (
                clients[w as usize].clone(),
                cap.clone(),
                layout.clone(),
                history.clone(),
                stop.clone(),
                count.clone(),
            );
            let (prefer, reader) = if w == 0 { (l_node, l_node.is_some()) } else { (None, false) };
            std::thread::spawn(move || {
                let mut r = XorShift64::new(seed ^ (w + 1));
                let mut n = 0u64;
                while !stop.load(Ordering::Relaxed) && count.fetch_add(1, Ordering::Relaxed) < max {
                    let call = now_us(t0);
                    let (kind, res) = if r.below(2) == 0 && !reader {
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
                        let v = match c.data.read(&cap, &layout, chunk, 0, 8, into, prefer) {
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
                    if let Err(e) = &res {
                        println!("寄存器操作 {kind:?} 失败（客户端看到了错误）：{e}");
                    }
                    let ret = res.is_ok().then(|| now_us(t0));
                    history.lock().unwrap().push(Op { call, ret, kind });
                    std::thread::sleep(Duration::from_millis(pace_ms + r.below(3 * pace_ms)));
                }
            })
        })
        .collect();
    Register { c, chunk, l_node, history, stop, count, max, workers }
}

/// 集群图里链 `chain` 的 last serving 所在的节点。
fn l_node(m: &ClusterMap, chain: u32) -> Option<u32> {
    let c = &m.chains.iter().find(|(id, _)| *id == chain)?.1;
    let l = c.members.iter().rev().find(|(_, s)| *s == SERVING)?.0;
    m.targets.iter().find(|(id, _)| *id == l).map(|(_, r)| r.node)
}

impl Register {
    pub fn running(&self) -> bool {
        self.count.load(Ordering::Relaxed) < self.max
    }

    /// 停下负载、检查历史线性一致（至少 `min_ok` 个确定的结果）；返回失败的操作数（客户端看到了错误的）。
    pub fn check(self, seed: u64, faults: &[String], min_ok: usize) -> usize {
        self.stop.store(true, Ordering::Relaxed);
        for w in self.workers {
            w.join().unwrap();
        }
        let h = self.history.lock().unwrap().clone();
        let ok = h.iter().filter(|o| o.ret.is_some()).count();
        println!("历史：{} 个操作（{ok} 个确定的结果），捣乱：{faults:?}", h.len());
        assert!(ok >= min_ok, "种子 {seed}：成功的操作太少（{ok}）");
        assert!(linearizable(&h, 0), "种子 {seed}：寄存器的历史不是线性一致的：{h:?}");
        h.len() - ok
    }
}
