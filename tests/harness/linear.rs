//! 寄存器历史的线性一致性检查（design.md 9.2 ③、9.5 ⑦ 的 Wing-Gong：回溯 + 记忆）。历史小（≤ 200 个操作），直接回溯即可。
//!
//! 每个操作有调用时刻与返回时刻；结果未知的写（超时、报错）**可能生效也可能没有**：返回时刻当作无穷大、可以不线性化；结果未知的读
//! 不进历史。一步能线性化的操作 = 调用时刻不晚于所有还没线性化、已返回的操作里最早的返回时刻（在它之前返回的操作必须排在它前面）。

use std::collections::HashSet;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Read(u64),
    Write(u64),
}

#[derive(Clone, Copy, Debug)]
pub struct Op {
    pub call: u64,
    pub ret: Option<u64>, // None = 结果未知
    pub kind: Kind,
}

/// 历史 `ops`（寄存器初值 `init`）是不是线性一致的。
pub fn linearizable(ops: &[Op], init: u64) -> bool {
    let ops: Vec<Op> = ops.iter().filter(|o| o.ret.is_some() || matches!(o.kind, Kind::Write(_))).copied().collect();
    let mut done = vec![false; ops.len()];
    let mut seen = HashSet::new();
    search(&ops, &mut done, init, &mut seen)
}

fn search(ops: &[Op], done: &mut Vec<bool>, val: u64, seen: &mut HashSet<(Vec<bool>, u64)>) -> bool {
    if ops.iter().zip(done.iter()).all(|(o, d)| *d || o.ret.is_none()) {
        return true; // 已返回的都线性化了（剩下的结果未知的写可以当作没发生）
    }
    if !seen.insert((done.clone(), val)) {
        return false;
    }
    let horizon =
        ops.iter().zip(done.iter()).filter(|(_, d)| !**d).filter_map(|(o, _)| o.ret).min().unwrap_or(u64::MAX);
    for i in 0..ops.len() {
        if done[i] || ops[i].call > horizon {
            continue;
        }
        let next = match ops[i].kind {
            Kind::Read(v) if v != val => continue,
            Kind::Read(_) => val,
            Kind::Write(v) => v,
        };
        done[i] = true;
        if search(ops, done, next, seen) {
            return true;
        }
        done[i] = false;
    }
    false
}
