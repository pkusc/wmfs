//! changelog 写入（design.md 3.11、3.2.8 的 `ChangeRecV1`、键空间 0x0E；操作码见 ADR-0029 与 `wmfs_api::ops`）。
//!
//! 每个成功的修改都按顺序记一条事件（seq 从 GLOBAL `next_changelog` 取号，从 1 开始），与修改在同一个操作里提交
//! （审计、webhook、dashboard 都从这里读，3.11 的 changelog 流在 M7）。不写 changelog 的修改：SessionHeartbeat、SetAtime、
//! SetLock、DebugPut、游标提交（ADR-0029 第 5 条）。
//!
//! 实现注记（M7）：changelog 流（3.11）的读取一侧——[`page`]（`after` 之后最多 `limit` 条，转成对外的 `ChangeEvent`）、消费者游标
//! [`set_cursor`] / [`get_cursor`]（CURSOR 0x1A；游标提交不写 changelog，ADR-0029 第 5 条）、消费者落后条数的指标 [`refresh_lag`]。
//! 长轮询的等待与唤醒在 `mgmt/changelog.rs` 与 raft 线程的 `after_apply`（写了 0x0E 的批次应用之后唤醒等待者）。修剪（保留期）在
//! M11 的 gc，所以现在 `410 Gone` 只在记录被删掉时才会出现。
//!
//! 实现注记（M4）：成功的修改由各操作在自己的闭包末尾调用 [`append`]（只有闭包知道 inode 号、名字与长度；写在闭包里，
//! 它就与修改原子地一起提交、一起回滚）。3.4.1.4 的 `run_one` 里"被拒绝的修改在 audited 时也写"随 `audit` 属性在 M12 加入。

use wmfs_api::metrics;

use crate::api::ops::{op_code, op_name};
use crate::api::types::{ChangeEvent, ChangelogPage, CursorReply};
use crate::base::{ErrCode, Failed};
use crate::meta::engine::txn::{KvRead, KvScan, KvTxn, OpCtx};
use crate::meta::schema::keys::{self, G_NEXT_CHANGELOG, T_CHANGELOG, T_CURSOR};
use crate::meta::schema::kv;
use crate::meta::schema::records::{ChangeRecV1, CursorV1, decode_v1};

/// 一页最多多少条（3.11：`limit=1000`）。
pub const PAGE_MAX: u32 = 1000;

/// 一个事件里随操作而变的部分（其余的——时间、身份、会话、trace_id——取自请求上下文）。
#[derive(Default)]
pub struct Change<'a> {
    pub op: &'static str, // 操作名（ADR-0029 的表；命名空间与会话操作的名字就是 MetaOp 的变体名）
    pub ino: u64,
    pub parent: u64,
    pub name: &'a [u8],
    pub new_parent: u64,
    pub new_name: &'a [u8],
    pub size: u64,
    pub extra: Vec<(String, String)>, // 实现注记（M5）：例如 SetSize 的 via=CommitWrite（ADR-0029 的码表注释）
}

/// 追加一条成功的事件。操作名不在 ADR-0029 的表里 = 程序写错了（不变量破坏）。
pub fn append(v: &mut dyn KvTxn, c: &OpCtx, ch: Change) -> Result<(), ErrCode> {
    let op = op_code(ch.op).unwrap_or_else(|| panic!("不变量破坏：changelog 操作名 {} 不在码表里", ch.op));
    let seq = kv::next_id(v, G_NEXT_CHANGELOG)?;
    let (r, who) = (c.req, &c.req.who);
    let rec = ChangeRecV1 {
        time_ms: r.now_ms,
        op,
        result: 0,
        trace_id: r.trace_id,
        uid: who.uid,
        gid: who.gid,
        tenant: who.tenant,
        session: r.session,
        job_id: who.job_id,
        ino: ch.ino,
        parent: ch.parent,
        name: ch.name.to_vec(),
        new_parent: ch.new_parent,
        new_name: ch.new_name.to_vec(),
        size: ch.size,
        extra: ch.extra,
        audited: false, // audit 属性在 M12
    };
    kv::put(v, &keys::changelog(seq), &rec);
    Ok(())
}

/// 一条记录 → 对外的 JSON 形态（`op` 用名字，不认识的数显示 `op#<数>`；名字按 UTF-8 显示，ADR-0029 第 3 条）。
pub fn to_event(seq: u64, r: ChangeRecV1) -> ChangeEvent {
    let op = op_name(r.op).map_or(format!("op#{}", r.op), str::to_string);
    let text = |b: &[u8]| String::from_utf8_lossy(b).into_owned();
    ChangeEvent {
        seq,
        time_ms: r.time_ms,
        op,
        result: r.result,
        trace_id: r.trace_id,
        uid: r.uid,
        gid: r.gid,
        tenant: r.tenant,
        session: r.session,
        job_id: r.job_id,
        ino: r.ino,
        parent: r.parent,
        name: text(&r.name),
        new_parent: r.new_parent,
        new_name: text(&r.new_name),
        size: r.size,
        extra: r.extra,
        audited: r.audited,
    }
}

/// 最新的 seq（0 = 还没有记录）。
pub fn last_seq(v: &dyn KvRead) -> Result<u64, ErrCode> {
    Ok(kv::get::<u64>(v, &keys::global(G_NEXT_CHANGELOG))?.ok_or(ErrCode::INTERNAL)? - 1)
}

/// [`page`] 的结果。
pub enum Page {
    Events(ChangelogPage),
    /// `after` 之后的一段已被修剪（410，缺口显式）：`oldest` 是最早还在的 seq。
    Gone {
        oldest: u64,
    },
}

/// `after` 之后最多 `limit`（≤ [`PAGE_MAX`]）条。
pub fn page(v: &dyn KvScan, after: u64, limit: u32) -> Result<Page, ErrCode> {
    let last = last_seq(v)?;
    let (start, end) = (keys::changelog(after.saturating_add(1)), keys::changelog(u64::MAX));
    let rows = v.scan(&start, &end, limit.clamp(1, PAGE_MAX) as usize)?;
    let first = rows.first().map(|(k, _)| kv::key_id(k));
    if after < last && first != Some(after + 1) {
        let (lo, hi) = keys::tag_range(T_CHANGELOG);
        let oldest = v.scan(&lo, &hi, 1)?.first().map_or(last + 1, |(k, _)| kv::key_id(k));
        if oldest > after + 1 {
            return Ok(Page::Gone { oldest });
        }
    }
    let mut events = Vec::with_capacity(rows.len());
    for (k, b) in rows {
        events.push(to_event(kv::key_id(&k), decode_v1(&b)?));
    }
    let next = events.last().map_or(after, |e| e.seq);
    Ok(Page::Events(ChangelogPage { events, next, last_seq: last }))
}

/// 提交消费者游标（执行器闭包里）：`seq` 不能超过最新的 seq（`EINVAL`，带原因）。不写 changelog（ADR-0029 第 5 条）。
pub fn set_cursor(v: &mut dyn KvTxn, c: &OpCtx, name: &str, seq: u64) -> Result<CursorReply, Failed> {
    let last = last_seq(v)?;
    if seq > last {
        return Err(Failed::new(ErrCode::EINVAL, format!("游标 {seq} 不能超过最新的 seq {last}")));
    }
    let rec = CursorV1 { seq, updated_ms: c.req.now_ms };
    kv::put(v, &keys::cursor(name), &rec);
    Ok(CursorReply { name: name.into(), seq, updated_ms: rec.updated_ms })
}

/// 读一个游标（没有 → None）。
pub fn get_cursor(v: &dyn KvRead, name: &str) -> Result<Option<CursorReply>, ErrCode> {
    let r = kv::get::<CursorV1>(v, &keys::cursor(name))?;
    Ok(r.map(|r| CursorReply { name: name.into(), seq: r.seq, updated_ms: r.updated_ms }))
}

/// 按（本节点已应用的）状态刷新每个消费者的落后条数 `wmfs_changelog_consumer_lag{name}` = 最新 seq − 游标。cluster-mgr 每 5 s
/// 调一次（实现注记（M7）：只在提交游标时算的话，卡住不动的消费者——正是这个指标要发现的——会一直显示提交那一刻的值）。
pub fn refresh_lag(v: &dyn KvScan) -> Result<(), ErrCode> {
    let last = last_seq(v)?;
    for (k, b) in kv::scan_tag(v, T_CURSOR)? {
        let name = String::from_utf8_lossy(&k[1..]).into_owned(); // 键 = 0x1A + 游标名
        let c: CursorV1 = decode_v1(&b)?;
        let g = metrics::gauge("wmfs_changelog_consumer_lag", "changelog 消费者落后的条数", &[("name", &name)]);
        g.set(last.saturating_sub(c.seq) as f64);
    }
    Ok(())
}
