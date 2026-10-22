//! 确定性 raft 模拟（design.md 9.2）的单节点部分：一个 `RawNode<RedbStorage>` + Driver（直接调用，不起线程、不依赖真实
//! 时间），存储是模拟掉电的 `MemCrashBackend`。每个种子随机地：提议批次、提议 expect_version 不对的批次、tick、读闸门、
//! 在随机的 sync 处"掉电"（之后照常跑一会儿）、崩溃重启。检查：
//! ② 每个被确认的写在任何崩溃序列后都还在——重启后的 KV 一定等于"确认历史"里的某个状态，而且不早于掉电点之前最后
//!    一个确认（确认时的 sync 计数 ≤ 掉电点 ⇒ 它的日志条目已落盘）；
//! ③ 单 key 线性一致：读闸门放行后读到的就是最新确认的状态（单节点、顺序历史）；
//! ④ expect_version 作废的批次从未被部分应用；
//! 以及规则 M1（任何可见状态都满足 applied ≤ hs.commit）、每次重启都不 fatal（坑 P9）。
//! 种子数：普通 `cargo test` 跑 200 个（约 10 s，全工作区的测试别太慢）；`scripts/ci.sh` 与 `scripts/accept.sh M3` 按 9.2
//! "每次提交跑 1000 个随机种子"用 `WMFS_SIM_SEEDS=1000`（约 1 分钟）。失败时消息里有种子与步数，便于复现。

use std::collections::BTreeMap;

use wmfs_core::base::rand::XorShift64;
use wmfs_core::meta::raft::driver::ProposeOutcome;
use wmfs_core::meta::schema::keys::T_DEBUG;
use wmfs_core::meta::schema::records::KvOp;
use wmfs_testkit::harness::meta::{Sim, bootstrap, dkey};

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

const STEPS: u32 = 150;

fn random_ops(r: &mut XorShift64) -> Vec<KvOp> {
    (0..1 + r.below(4))
        .map(|_| {
            let key = dkey(&format!("k{}", r.below(8)));
            if r.below(5) == 0 {
                KvOp::Delete { key }
            } else {
                let mut value = vec![0u8; 1 + r.below(20) as usize];
                r.fill(&mut value);
                KvOp::Put { key, value }
            }
        })
        .collect()
}

fn apply(m: &mut Model, ops: &[KvOp]) {
    for op in ops {
        match op {
            KvOp::Put { key, value } => m.insert(key.clone(), value.clone()),
            KvOp::Delete { key } => m.remove(key),
        };
    }
}

/// 一次确认：确认之后的状态，以及确认时后端的 sync 计数。
struct Confirmed {
    state: Model,
    sync: u64,
}

fn run_seed(seed: u64) {
    let mut r = XorShift64::new(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
    let mut s = Sim::start(bootstrap(&[1]), 1);
    s.until_ready();
    let mut hist = vec![Confirmed { state: Model::new(), sync: 0 }];
    for step in 0..STEPS {
        let at = format!("种子 {seed} 第 {step} 步");
        let model = &hist[hist.len() - 1].state;
        match r.below(100) {
            0..=49 => {
                let ops = random_ops(&mut r);
                assert_eq!(
                    s.write(ops.clone()),
                    ProposeOutcome::Applied { ok: true },
                    "{at}：就绪 leader 的批次应当成功"
                );
                let mut m = model.clone();
                apply(&mut m, &ops);
                hist.push(Confirmed { state: m, sync: s.backend.syncs() });
            }
            50..=57 => {
                let sv = s.driver.applied().state_version;
                let wrong = if r.below(2) == 0 { sv + 1 + r.below(5) } else { sv.saturating_sub(1 + r.below(3)) };
                if wrong != sv {
                    let rx = s.propose(random_ops(&mut r), Some(wrong));
                    s.driver.process_ready();
                    assert_eq!(rx.try_recv(), Ok(ProposeOutcome::Applied { ok: false }), "{at}：过期批次应整批作废");
                    assert_eq!(&s.kv(T_DEBUG), model, "{at}：作废的批次一个 op 也不能应用（④）");
                }
            }
            58..=69 => {
                for _ in 0..1 + r.below(12) {
                    s.driver.tick();
                }
                s.driver.process_ready();
            }
            70..=77 => {
                assert_eq!(s.read_index(), Ok(()), "{at}：单节点的读闸门立即放行");
                assert_eq!(&s.kv(T_DEBUG), model, "{at}：读到的必须是最新确认的状态（③）");
            }
            78..=86 => {
                if s.backend.frozen_at().is_none() {
                    s.backend.freeze_after(r.below(4)); // 在之后第 0..3 次 sync 处掉电，进程照常往下跑
                }
            }
            _ => {
                if r.below(3) == 0 {
                    let _ = s.propose(random_ops(&mut r), None); // 还没落盘就掉电：这个提议必须消失
                }
                s = restart(s, &mut hist, &at);
            }
        }
        let (hs, a, _) = s.raft_state();
        assert!(a.index <= hs.commit, "{at}：规则 M1 被破坏：applied {} > hs.commit {}", a.index, hs.commit);
    }
    let s = restart(s, &mut hist, &format!("种子 {seed} 结束时"));
    assert!(s.driver.is_ready(), "种子 {seed}：重启后应是就绪 leader");
}

/// 掉电重启，然后核对：重启后的 KV = 确认历史里的某个状态，且不早于掉电点之前最后一个确认（②）。
fn restart(s: Sim, hist: &mut Vec<Confirmed>, at: &str) -> Sim {
    let cut = s.backend.frozen_at().unwrap_or(u64::MAX);
    let must = hist.iter().filter(|c| c.sync <= cut).count(); // hist[0] 是初始状态，sync = 0
    let mut s = s.crash();
    s.until_ready(); // 重启不 fatal（坑 P9）：applied 来自盘上，且 ≤ hs.commit
    let got = s.kv(T_DEBUG);
    let pos = (must - 1..hist.len()).rev().find(|&i| hist[i].state == got);
    let Some(pos) = pos else {
        panic!(
            "{at}：重启后的状态不是任何一个确认过的状态，或丢了掉电点之前确认的写（至少应有 {} 个确认）：{got:?}",
            must - 1
        )
    };
    hist.truncate(pos + 1); // 掉电点之后的确认"没发生过"
    hist.iter_mut().for_each(|c| c.sync = 0); // 留下来的都已落盘（新后端从 0 计 sync）
    s
}

#[test]
fn single_node_random_power_loss_never_loses_a_confirmed_write() {
    let seeds: u64 = std::env::var("WMFS_SIM_SEEDS").ok().and_then(|v| v.parse().ok()).unwrap_or(200);
    let t0 = std::time::Instant::now();
    for seed in 1..=seeds {
        run_seed(seed);
    }
    println!("{seeds} 个种子 × {STEPS} 步，用时 {:.1} s", t0.elapsed().as_secs_f64());
}

/// 掉电点之后被"确认"的写：进程其实已经没了，所以重启后它们可以不在——但掉电点之前确认的一个都不能少。
#[test]
fn writes_confirmed_before_the_power_cut_survive_and_later_ones_may_vanish() {
    let mut s = Sim::start(bootstrap(&[1]), 1);
    s.until_ready();
    let put = |k: &str| vec![KvOp::Put { key: dkey(k), value: k.as_bytes().to_vec() }];
    assert_eq!(s.write(put("a")), ProposeOutcome::Applied { ok: true });
    s.backend.freeze_after(0);
    assert_eq!(s.write(put("b")), ProposeOutcome::Applied { ok: true });
    assert_eq!(s.read_index(), Ok(()));
    assert_eq!(s.kv(T_DEBUG).len(), 2, "进程看到的状态里两个都在");
    let mut s = s.crash();
    s.until_ready();
    let got = s.kv(T_DEBUG);
    assert!(got.contains_key(&dkey("a")) && !got.contains_key(&dkey("b")), "{got:?}");
    assert_eq!(s.read_index(), Ok(()), "重启后照常当选，读闸门放行");
}
