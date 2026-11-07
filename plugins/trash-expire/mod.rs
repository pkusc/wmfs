//! trash-expire：后台任务插件（design.md 4.12 的 trash-expire 一行、第 5 章"回收站细节"，bg_task；实现注记（M12））——回收站里到期的文件
//! 真正删掉。只在元数据 leader 上跑，每轮结束隔 10 s。
//!
//! - 一步：范围扫描 TRASH_INDEX（键 = `0F expire_ms ino`，按到期时间排好）里 `expire_ms ≤ now` 的最多 100 项（3.10："≤ 100 个对象"），每一项
//!   一个内部批次（`ns::trash::expire`：文件还在回收站里、到期时间就是这条才删——进普通的孤儿 / GC 流程，打开着它的人照常读写到关闭，
//!   changelog `TrashExpire`；否则只删掉这条过时的索引）。删过的索引不会再被扫到，所以不用游标；每一项幂等。按索引找，与回收站的目录结构
//!   无关：各用户子目录（`.wmfs-trash/<uid>/`，ADR-0043 第 1 条）里的都扫得到。
//! - 积压只表示这一步之后还有没有已经到期的（0 或 1；只多扫一行，不数真正的个数）。指标 `wmfs_trash_expired_total`（只数真删的，过时的
//!   索引不算）；fail point `plugin.trash-expire.bg_task`（本体包在每一步外层）。

use std::sync::Arc;
use std::time::Duration;

use wmfs_core::api::metrics::{self, Counter};
use wmfs_core::base::ErrCode;
use wmfs_core::base::types::ReqCtx;
use wmfs_core::bg::task::{BgTask, RunOn, StepResult, TaskCtx};
use wmfs_core::hooks::manifest::{CORE_ABI_VERSION, Hook, Manifest};
use wmfs_core::meta::engine::txn::txn_value;
use wmfs_core::meta::ns::trash;
use wmfs_core::meta::schema::keys::{self, T_TRASH_INDEX, parse_trash_index};
use wmfs_core::proto::meta::MetaReply;

pub static MANIFEST: Manifest = Manifest {
    name: "trash-expire",
    version: "0.1.0",
    hooks: &[Hook::BgTask],
    altitude: 0, // bg_task 不排序
    hot_path: false,
    config_schema: "",
    abi_version: CORE_ABI_VERSION,
    attrs: &["trash"],
    budget_ns: 0,
    fail_points: &["plugin.trash-expire.bg_task"],
};

const PAGE: usize = 100;

pub struct TrashExpire {
    expired: Arc<Counter>,
}

impl TrashExpire {
    pub fn new() -> TrashExpire {
        TrashExpire {
            expired: metrics::counter("wmfs_trash_expired_total", "回收站里到期、真正删掉的文件数", &[])
        }
    }
}

impl Default for TrashExpire {
    fn default() -> TrashExpire {
        TrashExpire::new()
    }
}

impl BgTask for TrashExpire {
    fn manifest(&self) -> &'static Manifest {
        &MANIFEST
    }

    fn kind(&self) -> &'static str {
        "trash-expire"
    }

    fn runs_on(&self) -> RunOn {
        RunOn::MetaLeader
    }

    fn interval(&self) -> Duration {
        Duration::from_secs(10)
    }

    fn step(&mut self, ctx: &mut TaskCtx, _cursor: &[u8]) -> Result<StepResult, ErrCode> {
        let mut due = vec![];
        ctx.meta.read(&ReqCtx::internal(), &mut |v, _c| {
            let (lo, _) = keys::tag_range(T_TRASH_INDEX);
            let rows = v.scan(&lo, &keys::trash_index(ctx.now_ms.saturating_add(1), 0), PAGE + 1)?; // expire_ms ≤ now
            due = rows.iter().filter_map(|(k, _)| parse_trash_index(k)).collect();
            Ok(MetaReply::Ok)
        })?;
        let n = due.len().min(PAGE);
        for &(expire_ms, ino) in &due[..n] {
            if txn_value(ctx.meta, ReqCtx::internal(), move |v, c| trash::expire(v, c, expire_ms, ino))? {
                self.expired.inc();
            }
        }
        let backlog = (due.len() - n) as u64;
        Ok(StepResult { cursor: vec![], items: n as u64, bytes: 0, backlog, pass_done: backlog == 0 })
    }
}

#[cfg(test)]
mod tests;
