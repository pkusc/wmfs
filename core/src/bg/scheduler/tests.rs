//! 调度器的单元测试：leader 任务所在的节点不再是就绪的 leader 时，积压与进度的 gauge 归零（M14 审查：告警 reconciler_backlog 看任一节点
//! 的样本，旧 leader 停在旧值上会让它永远 firing）。

use std::time::Duration;

use super::*;
use crate::hooks::manifest::{CORE_ABI_VERSION, Manifest};

static MANIFEST: Manifest = Manifest {
    name: "sched-test",
    version: "0.1.0",
    hooks: &[Hook::BgTask],
    altitude: 0,
    hot_path: false,
    config_schema: "",
    abi_version: CORE_ABI_VERSION,
    attrs: &[],
    budget_ns: 0,
    fail_points: &["plugin.sched-test.bg_task"],
};

/// 一个只在 leader 上跑的假任务（kind 独有：进程级的 gauge 不与别的测试共用）。
struct OnLeader;

impl BgTask for OnLeader {
    fn manifest(&self) -> &'static Manifest {
        &MANIFEST
    }
    fn kind(&self) -> &'static str {
        "sched-test-leader"
    }
    fn runs_on(&self) -> RunOn {
        RunOn::MetaLeader
    }
    fn interval(&self) -> Duration {
        Duration::from_secs(60)
    }
    fn step(&mut self, _ctx: &mut TaskCtx, _cursor: &[u8]) -> Result<StepResult, ErrCode> {
        Err(ErrCode::EIO)
    }
}

#[test]
fn losing_leadership_zeroes_the_backlog_and_progress_gauges() {
    let mut s = slot(Box::new(OnLeader));
    s.backlog.set(5.0);
    s.ratio.as_ref().unwrap().set(0.4);
    s.term = 3;
    not_leader(&mut s, &Migrator::new());
    let g = |name| metrics::gauge(name, "", &[("task", "sched-test-leader")]).get();
    assert_eq!((g("wmfs_task_backlog"), g("wmfs_task_progress_ratio"), s.term), (0.0, 0.0, 0));
}
