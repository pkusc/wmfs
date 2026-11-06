//! 后台任务的控制与进度存在哪里（design.md 3.10、7.1 的任务端点；实现注记（M11））。
//!
//! - **集群级的记录** `TaskRecV1`（KV 键 `task(kind, "")`）：控制字段由管理 API 写——`state`（1 Running / 2 Paused；`wmfs task
//!   pause|resume|run`）、`bytes_per_sec`/`items_per_sec`（`wmfs task set`；字节/秒 0 = 用任务的默认值，对象/秒 0 = 不限，limiter.rs）；
//!   `MetaLeader` 任务的进度（游标、`pass`、`done`、`backlog`、`updated_ms`、`last_error`，以及一轮做完时的 `state = 3 Done`）由 leader
//!   上的调度器写。两边都在执行器闭包里读-改-写，只改自己那一半（[`save_leader`]、[`apply_action`]），谁也不会冲掉谁。
//! - **`EveryDataNode` 任务**（scrub、gc-local）的进度是本节点的：存在 `<data_dir>/run/task-<kind>.cursor`（`atomic_write`，内容是
//!   1 字节版本号 + postcard([`LocalTaskV1`])）。它们的控制字段照样读集群级的那条记录，但读的是本节点**已应用**的 KV（只做数据的节点
//!   没有 → 按默认值：运行、默认限速，`wmfs task pause|set|run` 都到不了它们）。`wmfs task run` 对它们 = 集群记录的 `pass` 加一（"运行
//!   请求号"），每个节点看到比自己记下的 [`LocalTaskV1::run_seen`] 大就马上从头开始新的一轮。
//! - **给人看的进度**（[`TaskBoard`]）：调度器每一步之后把本节点每个任务的样子（含最近的速率）放在这里，管理 API 原样回出去
//!   （leader 任务看 leader 的，节点任务看各节点的 `GET /api/v1/node`）；另记着每个任务自己的默认限速（没设限速时显示它）。

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::api::types::{STATE_DONE, STATE_PAUSED, STATE_RUNNING, TaskInfo};
use crate::base::types::ReqCtx;
use crate::base::{ErrCode, lock, log, sys};
use crate::meta::engine::txn::{KvTxn, MetaEngine, TxnOp, txn_blocking};
use crate::meta::schema::keys;
use crate::meta::schema::kv;
use crate::meta::schema::records::{TaskRecV1, decode_v1, encode_v1};
use crate::proto::meta::MetaReply;

/// TaskRecV1.state（3.2.8）。
pub const RUNNING: u8 = 1;
pub const PAUSED: u8 = 2;
pub const DONE: u8 = 3;

/// 本地任务的游标文件（见文件头）：进度，与最近一次照办的运行请求号。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct LocalTaskV1 {
    pub rec: TaskRecV1,
    pub run_seen: u64,
}

/// 管理 API 用的控制动作。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    Pause,
    Resume,
    Run { every_node: bool },
    Set { bps: Option<u64>, ips: Option<u64> },
}

pub fn new_rec() -> TaskRecV1 {
    TaskRecV1 {
        state: RUNNING,
        cursor: vec![],
        pass: 0,
        done: 0,
        total_hint: 0,
        backlog: 0,
        bytes_per_sec: 0,
        items_per_sec: 0,
        updated_ms: 0,
        last_error: String::new(),
    }
}

/// 状态的名字（管理 API 与 CLI：`TaskInfo::state` 的取值）。
pub fn state_name(s: u8) -> &'static str {
    match s {
        PAUSED => STATE_PAUSED,
        DONE => STATE_DONE,
        _ => STATE_RUNNING,
    }
}

/// 管理 API 的控制动作（执行器闭包里）：只改控制字段，回改完的记录。
pub fn apply_action(v: &mut dyn KvTxn, kind: &str, a: Action) -> Result<TaskRecV1, ErrCode> {
    let key = keys::task(kind, "");
    let mut r = kv::get::<TaskRecV1>(v, &key)?.unwrap_or_else(new_rec);
    match a {
        Action::Pause => r.state = PAUSED,
        Action::Resume => r.state = RUNNING,
        Action::Run { every_node } => {
            r.state = RUNNING;
            if every_node {
                r.pass += 1; // 节点任务的运行请求号（见文件头）；leader 任务看 state 从 Done 变回 Running
            }
        }
        Action::Set { bps, ips } => {
            r.bytes_per_sec = bps.unwrap_or(r.bytes_per_sec);
            r.items_per_sec = ips.unwrap_or(r.items_per_sec);
        }
    }
    kv::put(v, &key, &r);
    Ok(r)
}

/// [`load_leader`] 读到的：任务的记录（没有 = 第一次跑：默认值）与节点级限速（集群参数 `bg_bytes_per_sec`）。
pub struct Loaded {
    pub rec: TaskRecV1,
    pub node_bps: u64,
}

/// 线性一致地读一个 leader 任务的记录与节点级限速。
pub fn load_leader(eng: &dyn MetaEngine, kind: &str) -> Result<Loaded, ErrCode> {
    let key = keys::task(kind, "");
    let mut out = Loaded { rec: new_rec(), node_bps: 0 };
    // 这个闭包只用来在 ReadIndex 之后的视图上读记录，它的返回值丢弃
    eng.read(&ReqCtx::internal(), &mut |v, c| {
        out.rec = kv::get::<TaskRecV1>(v, &key)?.unwrap_or_else(new_rec);
        out.node_bps = c.settings.bg_bytes_per_sec;
        Ok(MetaReply::Ok)
    })?;
    Ok(out)
}

/// 把 leader 任务的进度写回（一个内部批次）：控制字段保留库里的（管理 API 可能刚改过）；库里是 Paused 就保持 Paused。
pub fn save_leader(eng: &dyn MetaEngine, kind: &str, mine: &TaskRecV1) -> Result<(), ErrCode> {
    let (key, mine) = (keys::task(kind, ""), mine.clone());
    let op: TxnOp = Box::new(move |v, _c| {
        let stored = kv::get::<TaskRecV1>(v, &key)?.unwrap_or_else(new_rec);
        let state = if stored.state == PAUSED { PAUSED } else { mine.state };
        let rec = TaskRecV1 { state, bytes_per_sec: stored.bytes_per_sec, items_per_sec: stored.items_per_sec, ..mine };
        kv::put(v, &key, &rec);
        Ok(MetaReply::Ok)
    });
    txn_blocking(eng, ReqCtx::internal(), op).map(|_| ())
}

fn local_path(dir: &Path, kind: &str) -> std::path::PathBuf {
    dir.join(format!("task-{kind}.cursor"))
}

/// 读本地任务的游标文件（没有、读不出 → None：从头开始，scrub 与本地孤儿扫描从头再做一遍是安全的；读不出、解不开写一行警告）。
pub fn load_local(dir: &Path, kind: &str) -> Option<LocalTaskV1> {
    let p = local_path(dir, kind);
    let r = match std::fs::read(&p) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
        Err(e) => Err(e.to_string()),
        Ok(b) => decode_v1(&b).map_err(|e| e.to_string()),
    };
    r.inspect_err(|e| log::warn("bg", format_args!("任务游标文件 {} 用不了：{e}（这个任务从头开始）", p.display())))
        .ok()
}

pub fn save_local(dir: &Path, kind: &str, l: &LocalTaskV1) -> Result<(), ErrCode> {
    sys::atomic_write(&local_path(dir, kind), &encode_v1(l))
}

/// 本节点上每个任务给人看的样子与默认限速（见文件头）。
#[derive(Default)]
pub struct TaskBoard {
    m: Mutex<BTreeMap<String, TaskInfo>>,
    defaults: Mutex<BTreeMap<String, u64>>,
}

impl TaskBoard {
    pub fn put(&self, t: TaskInfo) {
        lock(&self.m).insert(t.kind.clone(), t);
    }

    /// 任务自己的默认限速（字节/秒，`BgTask::default_rate`；0 = 只受节点级上限）。
    pub fn set_default_bw(&self, kind: &str, bps: u64) {
        lock(&self.defaults).insert(kind.into(), bps);
    }

    pub fn default_bw(&self, kind: &str) -> u64 {
        lock(&self.defaults).get(kind).copied().unwrap_or(0)
    }

    pub fn all(&self) -> Vec<TaskInfo> {
        lock(&self.m).values().cloned().collect()
    }

    pub fn get(&self, kind: &str) -> Option<TaskInfo> {
        lock(&self.m).get(kind).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 本地游标文件（1 字节版本号 + postcard；字节金样在 tests/golden/records.golden 的 LocalTaskV1）。
    #[test]
    fn local_cursor_file_round_trips() {
        let dir = std::env::temp_dir().join(format!("wmfs-bg-progress-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let rec = TaskRecV1 { cursor: vec![1, 2], pass: 3, done: 4, updated_ms: 5, ..new_rec() };
        let l = LocalTaskV1 { rec, run_seen: 7 };
        save_local(&dir, "scrub", &l).unwrap();
        assert_eq!(load_local(&dir, "scrub"), Some(l));
        assert_eq!(load_local(&dir, "gc-local"), None, "没有文件：从头开始");
        std::fs::write(dir.join("task-gc-local.cursor"), b"junk").unwrap();
        assert_eq!(load_local(&dir, "gc-local"), None, "解不开：从头开始");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
