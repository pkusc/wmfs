//! ttl：后台任务插件（design.md 4.12 的 ttl 一行、第 5 章的 `ttl` 属性，bg_task；实现注记（M12））——设了 `ttl` 的子树里，mtime 早于
//! `now − ttl` 的文件被删掉（`trash` 生效就进回收站；changelog `Unlink` 带 `reason=ttl`，ADR-0029）。只在元数据 leader 上跑，每轮结束隔 60 s。
//!
//! - 一轮：先扫 DIRATTR 找出显式设了 `ttl`（不是 off）的目录（"规则根"，按 ino 排好），逐个子树 DFS（`bg::walker`）；每个不是目录、不在
//!   回收站里、生效的 ttl 不是 0、mtime 已经过期的名字 → 一个内部批次（`ns::trash::ttl`：在批次里按当前状态再判一遍，期间被改写、改名的
//!   不删）。嵌套的规则根会被走两遍（每一项幂等，最小方案）。
//! - 一步最多 100 个删除、或 1 s（3.10，`bg::task::StepBudget`；在一页里删到一半也停：游标退回这一页的开头，下一步重走这一页——每一项幂等，
//!   删掉的不会再出现）；
//!   游标 = (规则根, DFS 游标) 的 postcard，换 leader 后接着走（解不开就从头开始）。积压 = 这一轮还没走完的规则根个数。指标
//!   `wmfs_ttl_deleted_total`（只数真删的：批次里再判时已经不过期的不算）；fail point `plugin.ttl.bg_task`（本体包在每一步外层）。

use std::sync::Arc;
use std::time::Duration;

use wmfs_core::api::metrics::{self, Counter};
use wmfs_core::base::ErrCode;
use wmfs_core::base::types::ReqCtx;
use wmfs_core::bg::task::{BgTask, RunOn, StepBudget, StepResult, TaskCtx};
use wmfs_core::hooks::manifest::{CORE_ABI_VERSION, Hook, Manifest};
use wmfs_core::meta::engine::txn::txn_value;
use wmfs_core::meta::ns::trash::{self, ttl_due};
use wmfs_core::meta::schema::keys::T_DIRATTR;
use wmfs_core::meta::schema::kv;
use wmfs_core::meta::schema::records::{AttrMapV1, decode_v1};
use wmfs_core::proto::meta::MetaReply;
use wmfs_core::proto::{decode_body, encode_body};

pub static MANIFEST: Manifest = Manifest {
    name: "ttl",
    version: "0.1.0",
    hooks: &[Hook::BgTask],
    altitude: 0, // bg_task 不排序
    hot_path: false,
    config_schema: "",
    abi_version: CORE_ABI_VERSION,
    attrs: &["ttl"],
    budget_ns: 0,
    fail_points: &["plugin.ttl.bg_task"],
};

pub struct Ttl {
    deleted: Arc<Counter>,
}

impl Ttl {
    pub fn new() -> Ttl {
        Ttl { deleted: metrics::counter("wmfs_ttl_deleted_total", "ttl 任务删掉的过期文件数", &[]) }
    }
}

impl Default for Ttl {
    fn default() -> Ttl {
        Ttl::new()
    }
}

/// 显式设了 `ttl`（不是 off）的目录，按 ino 排好（见文件头）。
fn roots(ctx: &TaskCtx) -> Result<Vec<u64>, ErrCode> {
    let mut out = vec![];
    ctx.meta.read(&ReqCtx::internal(), &mut |v, _c| {
        for (k, b) in kv::scan_tag(v, T_DIRATTR)? {
            let m: AttrMapV1 = decode_v1(&b)?;
            if m.entries.get("ttl").is_some_and(|x| x != "off") {
                out.push(kv::key_id(&k));
            }
        }
        Ok(MetaReply::Ok)
    })?;
    Ok(out)
}

impl BgTask for Ttl {
    fn manifest(&self) -> &'static Manifest {
        &MANIFEST
    }

    fn kind(&self) -> &'static str {
        "ttl"
    }

    fn runs_on(&self) -> RunOn {
        RunOn::MetaLeader
    }

    fn interval(&self) -> Duration {
        Duration::from_secs(60)
    }

    fn step(&mut self, ctx: &mut TaskCtx, cursor: &[u8]) -> Result<StepResult, ErrCode> {
        let (mut root, mut dfs): (u64, Vec<u8>) = decode_body(cursor).unwrap_or_default();
        let (roots, mut b) = (roots(ctx)?, StepBudget::new(ctx));
        let paused = |root: u64, dfs: Vec<u8>, items: u64| -> Result<StepResult, ErrCode> {
            let backlog = roots.iter().filter(|r| **r >= root).count() as u64;
            Ok(StepResult { cursor: encode_body(&(root, dfs)), items, bytes: 0, backlog, pass_done: false })
        };
        while let Some(&r) = roots.iter().find(|r| **r >= root) {
            if r != root {
                (root, dfs) = (r, vec![]);
            }
            let page = ctx.walker.subtree(ctx.meta, root, &dfs)?;
            for w in page.items.iter().filter(|w| ttl_due(&w.inode, w.eff.ttl_ms, ctx.now_ms)) {
                let (parent, name, ino) = (w.parent, w.name.clone(), w.ino);
                if txn_value(ctx.meta, ReqCtx::internal(), move |v, c| trash::ttl(v, c, parent, &name, ino))? {
                    self.deleted.inc();
                }
                b.add(0);
                if !b.more() {
                    return paused(root, dfs, b.items); // 这一页删到一半：下一步从这一页的开头重走（见文件头）
                }
            }
            (root, dfs) = if page.done { (root + 1, vec![]) } else { (root, page.cursor) };
            if !b.more() {
                return paused(root, dfs, b.items);
            }
        }
        Ok(StepResult { cursor: vec![], items: b.items, bytes: 0, backlog: 0, pass_done: true })
    }
}

#[cfg(test)]
mod tests;
