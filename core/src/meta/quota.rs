//! 配额的本体一侧（design.md 3.9 的 quantum 额度模型、4.4，ADR-0026）：目录属性 `quota` / `quota.<类>` 的解析与显示、一个 inode
//! 的配额 scope、精确检查、CommitWrite 的护栏、授予的写入上限 `quota_end`、软限的起点与事件、`wmfs usage` 的配额部分。判定本身
//! （软 / 硬 / 宽限 / 文件数）在 quota 插件里（纯函数），这里给它准备输入、按结果写回状态。
//!
//! - **scope**：一个文件的配额 = 它主父目录到根的每个 `quota` 与 `quota.<它的类>`（Stacked，`inherit=false` 挡不住）；`used` = 那个目录
//!   的 RSTAT（常量时间）。孤儿（nlink = 0，已从 RSTAT 扣掉）不计：没有 scope、`quota_end = u64::MAX`（semantics.md 第 5 节"删除后仍
//!   打开的文件不计"）。
//! - **额度**：Open（写）、Create、改长度的 SetAttr、CommitWrite 的回复带 `quota_end = size + min(各 scope 的 quantum, 插件给的
//!   max_growth)`（[`end`]）。实现注记（M10）：每次 CommitWrite 都按当前的 scope 重新算（不只是 `want_quota` 的那次）——属性刚设上配额时，
//!   已经打开着的文件下一次提交就进入额度模式；`want_quota` 的提交见下面的护栏。
//! - **精确检查**（[`admit`]、[`admit_size`]、[`admit_move`]）：create 的文件数 + 1、SetAttr/TruncateHandle/Fallocate 扩展长度、跨 scope
//!   的 rename（新祖先链一侧，按被覆盖的目标扣掉之后、对调之后的净增量）。插件拒绝 → `EDQUOT`。入口都收 inode，孤儿在这里判（不计）。
//! - **护栏**（[`commit_growth`]，防不守协议的客户端）：CommitWrite 增长 Δ，插件拒绝时只在 Δ ≤ quantum、且每个 scope 都不超过
//!   `hard + 16 × quantum` 时接受（守协议的客户端最多超出 quantum × 写者数）；否则 `EDQUOT`，长度不更新。`want_quota` 的提交是
//!   客户端专门来要额度（`ensure_quota`，稀疏写提交到这次写的末尾），增长没有已授予的额度撑着：插件拒绝就 `EDQUOT`（ADR-0041 第 8 条 ⑤）。
//! - **配额范围**（[`same_range`]，ADR-0041 第 1 条）：一个文件的硬链接都在同一个配额范围（管着它的 scope 的集合）里——link 进、
//!   有多个链接的文件 rename 进范围不同的目录 → `EXDEV`（同 XFS 的项目配额；否则删掉主链接时计数搬过去，不过准入检查）。
//! - **软限**：插件回 SoftWarn（这次越过了软限、还没在计时）→ 对这次越过软限、还没在计时的**每个** scope（同一目录上的 `quota`
//!   与 `quota.<类>` 各算各的）记 `QUOTA_STATE(dir, class).soft_since_ms = now`、写 changelog `PolicyViolation`（extra：
//!   kind=quota_soft、dir、class、used、soft、grace）。用量回到软限以内时计时作废（读 scope 时当作 0），下一次越过重新计时。
//!   `wmfs set` 设上一个配额键时按这次的值重新判（3.8 变更通知 ⑤，[`note_set`]）：超了软限、还没在计时 → 从现在开始计时；设置之前
//!   就超了旧的软限（正在计时）→ 保留起点（重放同一份配置不把宽限期一再推后，ADR-0041 第 8 条 ②）；没超（含 `off`）→ 删掉起点。
//!   unset 与删掉目录时一并删掉（[`forget`]）——很久以前的起点不会管新的配额。
//!
//! 规范化的存储形式：`off`，或 `<hard 字节>[,soft=<字节>][,grace=<毫秒>][,files=<n>][,quantum=<字节>]`（只存写了的子键；没写 grace 时
//! 宽限 7 天、没写 quantum 时 64 MiB——缺省值只在这里，不在插件配置里）。指标 `wmfs_quota_grants_total`（授予了有限额度的次数，7.3）。

mod usage;
pub use usage::{over, quotas_of, semantics, usage};

use std::time::Duration;

use wmfs_api::metrics;

use crate::base::units::{fmt_duration, fmt_size, parse_duration, parse_size};
use crate::base::{ErrCode, log};
use crate::hooks::traits::{QuotaDecision, QuotaInput, QuotaScope};
use crate::meta::attr_registry::Parsed;
use crate::meta::attrs::QuotaDecl;
use crate::meta::changelog::{self, Change};
use crate::meta::engine::txn::{KvRead, KvTxn, OpCtx};
use crate::meta::ns::common::{Node, is_file};
use crate::meta::rstat::{self, Delta};
use crate::meta::schema::keys::{self, QUOTA_CLASS_TOTAL};
use crate::meta::schema::kv;
use crate::meta::schema::records::{AttrMapV1, ClassIdV1, QuotaStateV1, RStatV1};
use crate::obs::stages::Stage;

const DEFAULT_GRACE_MS: u64 = 7 * 86_400_000;
const DEFAULT_QUANTUM: u64 = 64 << 20;
const GUARD_QUANTA: u64 = 16; // 护栏：任何客户端最多超出 16 × quantum（3.9 第 4 条）

/// `quota=…` / `quota.<类>=…` 的解析与规范化（见文件头）；错误是给人看的原因（属性注册表映射成 `EINVAL`）。
pub fn parse(v: &str, subs: &[(String, String)]) -> Result<String, String> {
    if v == "off" {
        return if subs.is_empty() { Ok("off".into()) } else { Err("off 不能带子键".into()) };
    }
    let hard = parse_size(v)?;
    if hard == 0 {
        return Err("硬限应大于 0（不要配额就写 off）".into());
    }
    let mut out = hard.to_string();
    let (mut soft, mut grace) = (0, false);
    for (k, x) in subs {
        // 每个子键一个分支：解析并校验
        let n = match k.as_str() {
            "soft" => {
                soft = parse_size(x)?;
                if soft > hard {
                    return Err(format!("soft={x} 大于硬限"));
                }
                soft
            }
            "grace" => {
                grace = true;
                parse_duration(x)?.as_millis() as u64
            }
            "files" => x.parse::<u64>().map_err(|_| format!("files={x}：应是文件数"))?,
            "quantum" => {
                let q = parse_size(x)?;
                if !((1 << 20)..=(1 << 30)).contains(&q) {
                    return Err("quantum 应在 1M–1G 之间".into());
                }
                q
            }
            _ => return Err(format!("quota 没有子键 {k}")), // 属性注册表只会交来上面四个
        };
        if out.contains(&format!(",{k}=")) {
            return Err(format!("{k} 写了两次"));
        }
        out += &format!(",{k}={n}");
    }
    if grace && soft == 0 {
        return Err("grace 要与 soft 一起写".into());
    }
    Ok(out)
}

/// 规范化的形式 → 配额声明（`off` 或解不开 → None）。
pub fn decl(norm: &str, class: Option<u16>) -> Option<QuotaDecl> {
    let mut it = norm.split(',');
    let hard = it.next()?.parse().ok()?;
    let mut q = QuotaDecl { class, hard, soft: 0, grace_ms: DEFAULT_GRACE_MS, files: 0, quantum: DEFAULT_QUANTUM };
    for kv in it {
        let (k, v) = kv.split_once('=')?;
        let n: u64 = v.parse().ok()?;
        match k {
            "soft" => q.soft = n,
            "grace" => q.grace_ms = n,
            "files" => q.files = n,
            "quantum" => q.quantum = n,
            _ => return None,
        }
    }
    Some(q)
}

/// 存储形式 → 显示形式（`104857600,soft=83886080,grace=60000` → `100M,soft=80M,grace=1m`）：`wmfs set` 的回复与 changelog 用，
/// 只写用户写了的子键、按写的顺序。`wmfs get` 用 [`decl_text`]（缺省的宽限期与 quantum 也写出来）。
pub fn show(norm: &str) -> String {
    let mut parts = norm.split(',');
    let first = parts.next().unwrap_or_default();
    let mut s = first.parse().map(fmt_size).unwrap_or_else(|_| first.to_string());
    for kv in parts {
        let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
        let shown = match (k, v.parse::<u64>()) {
            ("grace", Ok(ms)) => fmt_duration(Duration::from_millis(ms)),
            ("files", _) | (_, Err(_)) => v.to_string(),
            (_, Ok(n)) => fmt_size(n),
        };
        s += &format!(",{k}={shown}");
    }
    s
}

/// 一个 scope 在它管的那部分里的 (字节, 文件数)：总量配额 = 子树全部（文件数含子目录）；按类的 = 这个类的。
pub fn used_of(class: Option<u16>, r: &RStatV1) -> (u64, u64) {
    match class {
        None => (r.bytes, r.files + r.dirs),
        Some(k) => r.per_class.iter().find(|(c, ..)| *c == k).map_or((0, 0), |(_, b, f)| (*b, *f)),
    }
}

/// 目录 `dir` 到根的配额 scope：`class` = Some(类) 只要总量的与这个类的（文件），None 要全部（rename 搬一棵子树）。实现注记（M14 收尾，
/// ADR-0045 第 9 条）：这里与 [`ask`] 的时间记作"quota"、之前的记作"执行"（慢日志的各阶段耗时）。
fn scopes(v: &dyn KvRead, c: &OpCtx, dir: u64, class: Option<u16>) -> Result<Vec<QuotaScope>, ErrCode> {
    c.mark(Stage::Exec);
    let e = c.attrs.resolve(v, dir)?;
    let mut out = Vec::new();
    for (d, q) in e.quotas.iter().filter(|(_, q)| class.is_none() || q.class.is_none() || q.class == class) {
        let used = rstat::get(v, *d)?;
        let since = kv::get::<QuotaStateV1>(v, &state_key(*d, q.class))?.map_or(0, |s| s.soft_since_ms);
        let over_soft = q.soft > 0 && used_of(q.class, &used).0 > q.soft; // 回到软限以内：计时作废（见文件头）
        out.push(scope(*d, q, used, if over_soft { since } else { 0 }));
    }
    c.mark(Stage::Quota);
    Ok(out)
}

fn scope(dir: u64, q: &QuotaDecl, used: RStatV1, soft_since_ms: u64) -> QuotaScope {
    QuotaScope {
        dir,
        class: q.class,
        hard_bytes: q.hard,
        soft_bytes: q.soft,
        grace_ms: q.grace_ms,
        hard_files: q.files,
        quantum: q.quantum,
        used,
        soft_since_ms,
    }
}

fn state_key(dir: u64, class: Option<u16>) -> Vec<u8> {
    keys::quota_state(dir, class.unwrap_or(QUOTA_CLASS_TOTAL))
}

/// 配额键（`quota` / `quota.<类>`）管的类：None = 总量；类不存在 → `ENOENT`。
fn key_class(v: &dyn KvRead, key: &str) -> Result<Option<u16>, ErrCode> {
    match key.strip_prefix("quota.") {
        Some(name) => Ok(Some(kv::get::<ClassIdV1>(v, &keys::class_name(name))?.ok_or(ErrCode::ENOENT)?.id)),
        None => Ok(None),
    }
}

fn ask(c: &OpCtx, ss: &[QuotaScope], class: u16, bytes: i64, files: i64) -> QuotaDecision {
    c.mark(Stage::Exec);
    let input = QuotaInput {
        who: &c.req.who,
        scopes: ss,
        delta_bytes: bytes,
        delta_files: files,
        class_id: class,
        now_ms: c.req.now_ms,
    };
    let d = c.pipeline.quota(&input);
    c.mark(Stage::Quota);
    d
}

/// 软限的计时开始（见文件头）：QUOTA_STATE 记下起点，changelog `PolicyViolation`。
fn soft_warn(v: &mut dyn KvTxn, c: &OpCtx, s: &QuotaScope) -> Result<(), ErrCode> {
    kv::put(v, &state_key(s.dir, s.class), &QuotaStateV1 { soft_since_ms: c.req.now_ms });
    let extra = vec![
        ("kind".into(), "quota_soft".into()),
        ("dir".into(), s.dir.to_string()),
        ("class".into(), s.class.map_or(String::new(), |k| k.to_string())),
        ("used".into(), used_of(s.class, &s.used).0.to_string()),
        ("soft".into(), s.soft_bytes.to_string()),
        ("grace".into(), fmt_duration(Duration::from_millis(s.grace_ms))),
    ];
    log::info("quota", format_args!("目录 {} 超过软限 {}：宽限期开始计时", s.dir, fmt_size(s.soft_bytes)));
    changelog::append(v, c, Change { op: "PolicyViolation", ino: s.dir, extra, ..Default::default() })
}

/// 软限：这次增长 `grow` 让它越过软限、还没在计时的每个 scope 开始计时（见文件头）。
fn warn_crossed(v: &mut dyn KvTxn, c: &OpCtx, ss: &[QuotaScope], grow: i64) -> Result<(), ErrCode> {
    for s in ss {
        let after = used_of(s.class, &s.used).0.saturating_add_signed(grow);
        if s.soft_bytes > 0 && s.soft_since_ms == 0 && after > s.soft_bytes {
            soft_warn(v, c, s)?;
        }
    }
    Ok(())
}

/// 插件的判定 → 结果：Deny → 它的码；SoftWarn → 越过软限的 scope 开始计时（插件只报第一个，这里每个都记）。
fn settle(v: &mut dyn KvTxn, c: &OpCtx, ss: &[QuotaScope], d: QuotaDecision, grow: i64) -> Result<(), ErrCode> {
    match d {
        QuotaDecision::Deny { code, .. } => Err(code),
        QuotaDecision::SoftWarn { .. } => warn_crossed(v, c, ss, grow),
        QuotaDecision::Allow { .. } => Ok(()),
    }
}

/// 精确检查（见文件头）：inode `n`（新建的、或要变长的）在它的主父目录下增加 `bytes` 字节、`files` 个文件；孤儿（nlink = 0）不计。
/// 按 `n` 的存储类（目录、符号链接、设备节点是 0：只有总量配额管它们）。
pub fn admit(v: &mut dyn KvTxn, c: &OpCtx, n: &Node, bytes: i64, files: i64) -> Result<(), ErrCode> {
    if n.rec.nlink == 0 || (bytes <= 0 && files <= 0) {
        return Ok(());
    }
    let class = n.rec.class_id;
    let ss = scopes(v, c, n.rec.parent, Some(class))?;
    if ss.is_empty() {
        return Ok(());
    }
    let d = ask(c, &ss, class, bytes, files);
    settle(v, c, &ss, d, bytes)
}

/// 普通文件 `n` 要变长到 `new_size`（SetAttr/TruncateHandle/Fallocate）：精确检查多出来的那段（不变长 → 不查）。
pub fn admit_size(v: &mut dyn KvTxn, c: &OpCtx, n: &Node, new_size: u64) -> Result<(), ErrCode> {
    match new_size.checked_sub(n.rec.size) {
        Some(grow) if grow > 0 => admit(v, c, n, grow as i64, 0),
        _ => Ok(()),
    }
}

/// rename 把一份计数 `d` 从 `from` 搬到 `to`：新祖先链一侧（`from` 的祖先里没有的）每个 scope 按它管的那部分精确检查（`d` 可以是
/// 净增量：RENAME_EXCHANGE 两份计数一进一出，变少的部分不查）。
pub fn admit_move(v: &mut dyn KvTxn, c: &OpCtx, from: u64, to: u64, d: &Delta) -> Result<(), ErrCode> {
    let old = c.attrs.ancestors(v, from)?;
    for s in scopes(v, c, to, None)?.into_iter().filter(|s| !old.contains(&s.dir)) {
        let (bytes, files, class) = match s.class {
            None => (d.bytes, d.files + d.dirs, 0),
            Some(k) => d.per_class.iter().find(|(x, ..)| *x == k).map_or((0, 0, k), |(_, b, f)| (*b, *f, k)),
        };
        if bytes > 0 || files > 0 {
            let one = std::slice::from_ref(&s);
            let r = ask(c, one, class, bytes, files);
            settle(v, c, one, r, bytes)?;
        }
    }
    Ok(())
}

/// 硬链接不跨配额范围（见文件头）：文件 `n` 的一个链接要进目录 `dir`（link；有多个链接时的 rename）——`dir` 与它主父目录的配额范围
/// 不同 → `EXDEV`；两边都没有配额（空集）算同一个范围。scope 按"近的在前"排（`attrs.rs` 的 `compute`），集合相同时次序也相同。
pub fn same_range(v: &dyn KvRead, c: &OpCtx, n: &Node, dir: u64) -> Result<(), ErrCode> {
    let range = |d| -> Result<Vec<(u64, Option<u16>)>, ErrCode> {
        Ok(scopes(v, c, d, Some(n.rec.class_id))?.iter().map(|s| (s.dir, s.class)).collect())
    };
    if range(n.rec.parent)? == range(dir)? { Ok(()) } else { Err(ErrCode::EXDEV) }
}

/// CommitWrite 让文件 `n` 长了 `grow` 字节：插件放行就照常；插件拒绝时按护栏判（见文件头）。孤儿不计。
pub fn commit_growth(v: &mut dyn KvTxn, c: &OpCtx, n: &Node, grow: u64, want_quota: bool) -> Result<(), ErrCode> {
    if n.rec.nlink == 0 || (grow == 0 && !want_quota) {
        return Ok(());
    }
    let class = n.rec.class_id;
    let ss = scopes(v, c, n.rec.parent, Some(class))?;
    if ss.is_empty() {
        return Ok(());
    }
    if grow == 0 {
        // 客户端用完了额度来要（长度不变）：问插件"再多 1 字节行不行"——一点余量也没有时插件判 Deny 并计入
        // wmfs_quota_denied_total（告警 quota_denied）；回复的 quota_end = 长度，客户端随后报 EDQUOT（实现注记（M14 审查））
        let _ = ask(c, &ss, class, 1, 0);
        return Ok(());
    }
    let d = ask(c, &ss, class, grow as i64, 0);
    if !matches!(d, QuotaDecision::Deny { .. }) {
        return settle(v, c, &ss, d, grow as i64);
    }
    for s in &ss {
        let after = used_of(s.class, &s.used).0.saturating_add(grow);
        let past_guard = after > s.hard_bytes.saturating_add(GUARD_QUANTA * s.quantum);
        if want_quota || past_guard || (grow > s.quantum && after > s.hard_bytes) {
            return Err(ErrCode::EDQUOT); // 专门来要额度（见文件头）、不守协议的客户端，或单次增长超过 quantum 且超了硬限
        }
    }
    warn_crossed(v, c, &ss, grow as i64)
}

/// 这次授予的写入上限（`OpenOut.quota_end`、`Committed.quota_end`，见文件头）；没有 scope（含孤儿与非普通文件）→ `u64::MAX`。
pub fn end(v: &dyn KvRead, c: &OpCtx, n: &Node) -> Result<u64, ErrCode> {
    if !is_file(n) || n.rec.nlink == 0 {
        return Ok(u64::MAX);
    }
    let ss = scopes(v, c, n.rec.parent, Some(n.rec.class_id))?;
    let Some(quantum) = ss.iter().map(|s| s.quantum).min() else { return Ok(u64::MAX) };
    let grant = match ask(c, &ss, n.rec.class_id, 0, 0) {
        QuotaDecision::Allow { max_growth } | QuotaDecision::SoftWarn { max_growth, .. } => max_growth.min(quantum),
        QuotaDecision::Deny { .. } => 0,
    };
    metrics::counter("wmfs_quota_grants_total", "授予了有限写入额度的次数（受配额管着的写打开与提交）", &[]).inc();
    Ok(n.rec.size.saturating_add(grant))
}

/// `wmfs set` 要在目录 `dir` 上设 `items`（3.8 变更通知 ⑤，规则见文件头）：在写回 DIRATTR 之前调用（读到设置之前的值：正在计时
/// 吗）；不经解析缓存，只看这次设置的配额键。
pub fn note_set(v: &mut dyn KvTxn, c: &OpCtx, dir: u64, items: &[Parsed]) -> Result<(), ErrCode> {
    let before = kv::get::<AttrMapV1>(v, &keys::dirattr(dir))?.map(|m| m.entries).unwrap_or_default();
    for p in items.iter().filter(|p| matches!(p.spec.key, "quota" | "quota.*")) {
        let class = key_class(v, &p.key)?; // 类的存在已由 attr_ops 检查过
        let (used, key) = (rstat::get(v, dir)?, state_key(dir, class));
        let over_soft = |norm: &str| decl(norm, class).filter(|q| q.soft > 0 && used_of(class, &used).0 > q.soft);
        let timing =
            before.get(&p.key).and_then(|o| over_soft(o)).is_some() && kv::get::<QuotaStateV1>(v, &key)?.is_some();
        match over_soft(&p.norm) {
            Some(_) if timing => {} // 重放同一份配置不把宽限期一再推后（ADR-0041 第 8 条 ②）
            Some(q) => soft_warn(v, c, &scope(dir, &q, used, 0))?,
            None => v.delete(&key),
        }
    }
    Ok(())
}

/// 目录 `dir` 上的配额键 `key` 没有了（`wmfs unset`、删掉目录）：它的软限起点一并删掉（见文件头）。不是配额键、类已经不在了 →
/// 什么也不做。
pub fn forget(v: &mut dyn KvTxn, dir: u64, key: &str) -> Result<(), ErrCode> {
    if key != "quota" && !key.starts_with("quota.") {
        return Ok(());
    }
    match key_class(v, key) {
        Ok(class) => v.delete(&state_key(dir, class)),
        Err(ErrCode::ENOENT) => {}
        Err(e) => return Err(e),
    }
    Ok(())
}

/// 一个配额声明的显示形式（缺省的宽限期与 quantum 也写出来：`wmfs get` 看得到实际生效的值；`quota.<类>` 前面带类名）。`wmfs set`
/// 的回复用 [`show`]（只写用户写了的）。
pub fn decl_text(q: &QuotaDecl, class: Option<&str>) -> String {
    let mut s = fmt_size(q.hard);
    if q.soft > 0 {
        s += &format!(",soft={}", fmt_size(q.soft));
    }
    if q.files > 0 {
        s += &format!(",files={}", q.files);
    }
    s += &format!(",grace={},quantum={}", fmt_duration(Duration::from_millis(q.grace_ms)), fmt_size(q.quantum));
    class.map_or(s.clone(), |k| format!("{k}:{s}"))
}
