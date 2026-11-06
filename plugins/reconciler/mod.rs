//! reconciler（协调器，design.md 3.10、4.12，bg_task）：后台任务框架的第一个实例——不断比较每个文件的"声明"（它所在目录的生效属性）
//! 与"实际"（inode 里的存储类与 layout），不一致就用迁移原语把它换到新布局（`bg::migrator`）。只在元数据 leader 上跑，每轮结束隔 1 s。
//!
//! - **一轮** = 把 RECONCILE_Q（`wmfs set`/`unset` 了 class、replicas、compress、encrypt，或目录 rename 时写的一条 `{dir, reason}`）清空：
//!   按 seq 顺序取一项，子树 DFS 走一遍它的目录（`bg::walker`），对每个与声明不同的普通文件 `migrate`（还留着别人的迁移意图——
//!   `pending_epoch ≠ 0`、不是 inline——的也交给它：声明一致了也要放弃那个意图，ADR-0042 第 10 条）；走完、而且一个都没被跳过 → 删掉这
//!   一项。有文件因为一直写打开（或迁移途中被写、或这个文件现在迁不了）而没迁成 → 这一项留着、过一会儿再重走（10 s 起，每次没迁成就翻倍，
//!   最多 10 min：一个一直开着的日志文件不会让它把整棵子树每 10 s 重走一遍；只在内存里记，换 leader 之后马上重走），算进积压。队列空了
//!   （剩下的都在等）→ 这一轮做完。一轮开头先让迁移原语把已经不再写打开着的"跳过"记录清掉（`Migrator::prune`）。
//! - **全量扫描**：本任期就绪之后每 24 h 一次（测试用 fail point `reconciler.full_every_ms` 的 `return(<毫秒>)` 缩短）按 ino 扫全部 inode
//!   （`Walker::flat`）：与声明不同的文件照样迁移（漏网的、队列丢了的都在这里补上）；每个目录顺带做一次递归计数的在线自检
//!   （`meta::rstat::selfcheck`：偏差 → 一个批次修正 + changelog `RStatDrift`，3.9）。
//! - **一步**（3.10："≤ 100 个对象 / ≤ 64 MiB / ≤ 1 s"）：迁移满 100 个文件、或搬的字节到了大约一秒的限速量（`wmfs task set reconciler bw=…`，
//!   没设就是 64 MiB；`RateLimiter::step_bytes`）、或花了 1 s 就停；游标记着做到哪（见 [`Cursor`]），断点续跑。中途停下的那一页下一步从头
//!   再看一遍（已经迁移好的文件一比较就跳过）。每一步都幂等：迁移原语自己是 CAS（3.10），删队列项是点删。
//! - 积压 = 队列里的项数（每项是一个目录）。指标 `wmfs_reconciler_files_migrated_total`、`wmfs_reconciler_rstat_fixed_total`（另有框架的
//!   `wmfs_task_*` 与迁移原语的 `wmfs_migrations_total`、`wmfs_migration_skipped_open_files`）；fail point `plugin.reconciler.bg_task`（本体包在
//!   每一步外层）。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use wmfs_core::api::failpoint::fail_point_ms;
use wmfs_core::api::metrics::{self, Counter};
use wmfs_core::base::types::ReqCtx;
use wmfs_core::base::{ErrCode, clock};
use wmfs_core::bg::migrator::Migrated;
use wmfs_core::bg::status::differs;
use wmfs_core::bg::task::{BgTask, RunOn, StepResult, TaskCtx};
use wmfs_core::bg::walker::Walked;
use wmfs_core::hooks::manifest::{CORE_ABI_VERSION, Hook, Manifest};
use wmfs_core::meta::engine::txn::{TxnOp, txn_blocking};
use wmfs_core::meta::rstat;
use wmfs_core::meta::schema::keys::{self, T_RECONCILE_Q};
use wmfs_core::meta::schema::kv;
use wmfs_core::meta::schema::records::{F_INLINE, KIND_DIR, KIND_FILE, ReconcileItemV1, decode_v1};
use wmfs_core::proto::meta::MetaReply;
use wmfs_core::proto::{decode_body, encode_body};

pub static MANIFEST: Manifest = Manifest {
    name: "reconciler",
    version: "0.1.0",
    hooks: &[Hook::BgTask],
    altitude: 0, // bg_task 不排序
    hot_path: false,
    config_schema: "",
    abi_version: CORE_ABI_VERSION,
    // 协调器也迁移 replicas，但 replicas 的消费者（属性注册表 3.8）是放置它的 replica / label-match：没有它们，replicas 本身就设不了
    attrs: &["class", "compress", "encrypt"],
    budget_ns: 0,
    fail_points: &["plugin.reconciler.bg_task"],
};

const FULL_EVERY_MS: u64 = 24 * 3600 * 1000;
const HOLD_MS: u64 = 10_000;
const HOLD_MAX_MS: u64 = 600_000;
const MAX_ITEMS: u64 = 100;
const MAX_BYTES: u64 = 64 << 20;
const MAX_STEP_MS: u64 = 1000;
const FLAT_PAGE: usize = 100;

/// 游标（插件私有的字节，postcard）：`Start` = 一轮的开头；`Walk` = 正在走队列项 seq（跳过过没有、DFS 游标）；`Full` = 全量扫描到了
/// ino 之后。解不开就从头开始（每一步都幂等）。
#[derive(Serialize, Deserialize, Default, Clone, Debug, PartialEq, Eq)]
enum Cursor {
    #[default]
    Start,
    Walk {
        seq: u64,
        skipped: bool,
        dfs: Vec<u8>,
    },
    Full {
        after: u64,
    },
}

/// 一步的额度（见文件头）。
struct Budget {
    t0: u64,
    items: u64,
    bytes: u64,
    max_bytes: u64,
}

impl Budget {
    fn more(&self) -> bool {
        self.items < MAX_ITEMS && self.bytes < self.max_bytes && clock::mono_ms().saturating_sub(self.t0) < MAX_STEP_MS
    }
}

pub struct Reconciler {
    term: u64,
    full_from: u64, // mono_ms：上一次全量扫描做完（或本任期开始）的时刻，过了间隔就做下一次
    hold: HashMap<u64, (u64, u64)>, // 队列项 seq → (到这个时刻（mono_ms）之前不再重走, 这次等了多久)
    migrated: Arc<Counter>,
    fixed: Arc<Counter>,
}

impl Default for Reconciler {
    fn default() -> Reconciler {
        Reconciler::new()
    }
}

/// 队列里的全部项（seq 升序，最多 1000 个：一次 set 只写一条，项数很少）。
fn queue(ctx: &TaskCtx) -> Result<Vec<(u64, ReconcileItemV1)>, ErrCode> {
    let mut out = vec![];
    ctx.meta.read(&ReqCtx::internal(), &mut |v, _c| {
        let (lo, hi) = keys::tag_range(T_RECONCILE_Q);
        let rows = v.scan(&lo, &hi, 1000)?;
        out = rows.iter().map(|(k, b)| Ok((kv::key_id(k), decode_v1(b)?))).collect::<Result<_, ErrCode>>()?;
        Ok(MetaReply::Ok)
    })?;
    Ok(out)
}

/// 队列项 seq 的目录（已经删了 → None）。
fn item_dir(ctx: &TaskCtx, seq: u64) -> Result<Option<u64>, ErrCode> {
    let mut dir = None;
    ctx.meta.read(&ReqCtx::internal(), &mut |v, _c| {
        dir = kv::get::<ReconcileItemV1>(v, &keys::reconcile(seq))?.map(|i| i.dir);
        Ok(MetaReply::Ok)
    })?;
    Ok(dir)
}

/// 走完了一个队列项：点删它（还是这一项才删）。
fn drop_item(ctx: &TaskCtx, seq: u64) -> Result<(), ErrCode> {
    let op: TxnOp = Box::new(move |v, _c| {
        v.delete(&keys::reconcile(seq));
        Ok(MetaReply::Ok)
    });
    txn_blocking(ctx.meta, ReqCtx::internal(), op).map(|_| ())
}

impl Reconciler {
    pub fn new() -> Reconciler {
        Reconciler {
            term: 0,
            full_from: 0,
            hold: HashMap::new(),
            migrated: metrics::counter("wmfs_reconciler_files_migrated_total", "协调器迁移成的文件数", &[]),
            fixed: metrics::counter("wmfs_reconciler_rstat_fixed_total", "协调器修正过的递归计数（RStatDrift）", &[]),
        }
    }

    /// 一个普通文件与声明不同（或留着别人的迁移意图，见文件头）就交给迁移原语；回 false = 没迁成（写打开、期间被写、现在迁不了），
    /// 这一项要留着。
    fn reconcile_file(&self, ctx: &TaskCtx, w: &Walked, b: &mut Budget) -> Result<bool, ErrCode> {
        let i = &w.inode;
        let stale = i.pending_epoch != 0 && i.flags & F_INLINE == 0;
        if i.kind != KIND_FILE || i.nlink == 0 || !(stale || differs(i, &w.eff)) {
            return Ok(true);
        }
        match ctx.migrate.migrate(ctx, w.ino, &w.eff)? {
            Migrated::Done { bytes } => {
                (b.items, b.bytes) = (b.items + 1, b.bytes + bytes);
                self.migrated.inc();
                Ok(true)
            }
            Migrated::Same => Ok(true),
            Migrated::SkippedOpen | Migrated::Retry => Ok(false),
        }
    }

    /// 走队列项 `seq` 的子树（见文件头）。
    fn walk_item(
        &mut self,
        ctx: &TaskCtx,
        seq: u64,
        mut skipped: bool,
        dfs: Vec<u8>,
        b: &mut Budget,
    ) -> Result<Cursor, ErrCode> {
        let Some(dir) = item_dir(ctx, seq)? else { return Ok(Cursor::Start) }; // 已经删了
        let page = ctx.walker.subtree(ctx.meta, dir, &dfs)?;
        for w in &page.items {
            if !b.more() {
                return Ok(Cursor::Walk { seq, skipped, dfs }); // 额度用完：下一步把这一页再看一遍
            }
            skipped |= !self.reconcile_file(ctx, w, b)?;
        }
        if !page.done {
            return Ok(Cursor::Walk { seq, skipped, dfs: page.cursor });
        }
        if skipped {
            let wait = self.hold.get(&seq).map_or(HOLD_MS, |(_, w)| (w * 2).min(HOLD_MAX_MS)); // 有文件没迁成：等一会儿再重走
            self.hold.insert(seq, (clock::mono_ms() + wait, wait));
        } else {
            drop_item(ctx, seq)?;
            self.hold.remove(&seq);
        }
        Ok(Cursor::Start)
    }

    /// 全量扫描的一批（见文件头）。
    fn full_scan_batch(&mut self, ctx: &TaskCtx, after: u64, b: &mut Budget) -> Result<Option<Cursor>, ErrCode> {
        let page = ctx.walker.flat(ctx.meta, after, FLAT_PAGE)?;
        let mut at = after;
        for w in &page.items {
            if !b.more() {
                return Ok(Some(Cursor::Full { after: at }));
            }
            if w.inode.kind == KIND_DIR && rstat::selfcheck(ctx.meta, w.ino)? {
                self.fixed.inc();
            }
            self.reconcile_file(ctx, w, b)?; // 没迁成的等下一次全量扫描（或它所在目录下一次进队列）
            at = w.ino;
        }
        if page.done {
            self.full_from = clock::mono_ms();
            return Ok(None);
        }
        Ok(Some(Cursor::Full { after: page.cursor }))
    }
}

impl BgTask for Reconciler {
    fn manifest(&self) -> &'static Manifest {
        &MANIFEST
    }

    fn kind(&self) -> &'static str {
        "reconciler"
    }

    fn runs_on(&self) -> RunOn {
        RunOn::MetaLeader
    }

    fn interval(&self) -> Duration {
        Duration::from_secs(1)
    }

    fn step(&mut self, ctx: &mut TaskCtx, cursor: &[u8]) -> Result<StepResult, ErrCode> {
        let (term, now) = (ctx.meta.role().term, clock::mono_ms());
        if term != self.term {
            (self.term, self.full_from) = (term, now); // 换了 leader：从本任期就绪算起
            self.hold.clear();
        }
        let mut b = Budget { t0: now, items: 0, bytes: 0, max_bytes: ctx.limiter.step_bytes(MAX_BYTES) };
        let q = queue(ctx)?;
        self.hold.retain(|seq, _| q.iter().any(|(s, _)| s == seq)); // 已经删了的项不必再记
        let pick = q.iter().map(|(seq, _)| *seq).find(|seq| self.hold.get(seq).is_none_or(|(until, _)| *until <= now));
        let next = match decode_body(cursor).unwrap_or_default() {
            Cursor::Walk { seq, skipped, dfs } => Some(self.walk_item(ctx, seq, skipped, dfs, &mut b)?),
            Cursor::Full { after } => self.full_scan_batch(ctx, after, &mut b)?,
            Cursor::Start => {
                ctx.migrate.prune(ctx.live); // 一轮（或一项）的开头（见文件头）
                match pick {
                    Some(seq) => Some(self.walk_item(ctx, seq, false, vec![], &mut b)?),
                    None if now
                        >= self.full_from.saturating_add(fail_point_ms("reconciler.full_every_ms", FULL_EVERY_MS)) =>
                    {
                        self.full_scan_batch(ctx, 0, &mut b)?
                    }
                    None => None, // 队列空了（或剩下的都在等）：这一轮做完
                }
            }
        };
        let pass_done = next.is_none();
        Ok(StepResult {
            cursor: next.filter(|c| *c != Cursor::Start).map(|c| encode_body(&c)).unwrap_or_default(),
            items: b.items,
            bytes: b.bytes,
            backlog: q.len() as u64,
            pass_done,
        })
    }
}

#[cfg(test)]
mod tests;
