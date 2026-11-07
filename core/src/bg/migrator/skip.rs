//! 迁移原语里"因写打开被跳过"的记账（design.md 3.10 第 1 步，实现注记（M11））：第一次被跳过的时刻、24 h 的告警、指标
//! `wmfs_migration_skipped_open_files`。从 `migrator.rs` 拆出来（每个文件 ≤ 400 行，M13）。

use std::collections::HashMap;

use wmfs_api::failpoint::fail_point_ms;

use super::Migrator;
use crate::base::types::ReqCtx;
use crate::base::{clock, lock, log};
use crate::meta::changelog::{self, Change};
use crate::meta::engine::txn::{MetaEngine, TxnOp, txn_blocking};
use crate::meta::sessions::Live;
use crate::proto::meta::MetaReply;

/// 跳过多久之后告警（3.10："某个文件被跳过超过 24 h"）。
const OPEN_ALERT_MS: u64 = 24 * 3600 * 1000;

/// 本任期里因写打开被跳过的文件（换 leader 后重新计时）；`shown` 是已经加进指标的个数（[`Migrator::show`]）。
#[derive(Default)]
pub(super) struct Skipped {
    term: u64,
    files: HashMap<u64, SkipInfo>,
    shown: usize,
}

#[derive(Clone, Copy)]
struct SkipInfo {
    since_ms: u64, // 第一次被跳过的时刻（mono_ms）
    alerted: bool, // 告警过了吗
}

impl Migrator {
    /// 已经不再写打开着的（关了、删了）跳过记录清掉（协调器每一轮开头调用，见文件头第 1 步）。
    pub fn prune(&self, live: &Live) {
        let mut s = lock(&self.skipped);
        s.files.retain(|ino, _| live.is_write_open(*ino));
        self.show(&mut s);
    }

    /// 不再是 leader：跳过记录全部清掉（指标归 0；新 leader 自己重新计）。
    pub fn forget_all(&self) {
        let mut s = lock(&self.skipped);
        (s.term, s.files) = (0, HashMap::new());
        self.show(&mut s);
    }

    pub(super) fn forget(&self, ino: u64) {
        let mut s = lock(&self.skipped);
        if s.files.remove(&ino).is_some() {
            self.show(&mut s);
        }
    }

    /// 把跳过的个数报进指标。指标是进程级的：一个进程里跑着几个节点时（测试），各自 `set` 会互相盖掉（谁最后写算谁的），所以
    /// 只加减自己的变化量，指标是本进程各节点之和（生产上一个进程一个节点：就是它自己的个数）。
    fn show(&self, s: &mut Skipped) {
        self.skipped_gauge.add(s.files.len() as f64 - s.shown as f64);
        s.shown = s.files.len();
    }

    /// 记下"因写打开被跳过"（见文件头第 1 步）；到了告警的时候写一次 `PolicyViolation`。
    pub(super) fn note_skip(&self, eng: &dyn MetaEngine, ino: u64) {
        let (term, now) = (eng.role().term, clock::mono_ms());
        let alert = fail_point_ms("bg.migrate.open_alert_ms", OPEN_ALERT_MS);
        let mut s = lock(&self.skipped);
        if s.term != term {
            (s.term, s.files) = (term, HashMap::new()); // 换了 leader：重新计时
        }
        let e = s.files.entry(ino).or_insert(SkipInfo { since_ms: now, alerted: false });
        let (since, due) = (e.since_ms, !e.alerted && now.saturating_sub(e.since_ms) >= alert);
        e.alerted |= due;
        self.show(&mut s);
        drop(s);
        if due {
            let extra = vec![
                ("kind".into(), "migration_blocked_by_open".into()),
                ("skipped_ms".into(), now.saturating_sub(since).to_string()),
            ];
            let op: TxnOp = Box::new(move |v, c| {
                changelog::append(v, c, Change { op: "PolicyViolation", ino, extra, ..Default::default() })?;
                Ok(MetaReply::Ok)
            });
            if let Err(e) = txn_blocking(eng, ReqCtx::internal(), op) {
                log::warn("bg", format_args!("inode {ino} 一直写打开、迁移被跳过：告警事件没写成（{e}）"));
            }
        }
    }
}
