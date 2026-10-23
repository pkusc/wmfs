//! changelog 写入（design.md 3.11、3.2.8 的 `ChangeRecV1`、键空间 0x0E；操作码见 ADR-0029 与 `wmfs_api::ops`）。
//!
//! 每个成功的修改都按顺序记一条事件（seq 从 GLOBAL `next_changelog` 取号，从 1 开始），与修改在同一个操作里提交
//! （审计、webhook、dashboard 都从这里读，3.11 的 changelog 流在 M7）。不写 changelog 的修改：SessionHeartbeat、SetAtime、
//! SetLock、DebugPut、游标提交（ADR-0029 第 5 条）。
//!
//! 实现注记（M4）：成功的修改由各操作在自己的闭包末尾调用 [`append`]（只有闭包知道 inode 号、名字与长度；写在闭包里，
//! 它就与修改原子地一起提交、一起回滚）。3.4.1.4 的 `run_one` 里"被拒绝的修改在 audited 时也写"随 `audit` 属性在 M12 加入。

use crate::base::ErrCode;
use crate::meta::engine::txn::{KvTxn, OpCtx};
use crate::meta::schema::keys::{self, G_NEXT_CHANGELOG};
use crate::meta::schema::kv;
use crate::meta::schema::records::ChangeRecV1;

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
    let op =
        crate::api::ops::op_code(ch.op).unwrap_or_else(|| panic!("不变量破坏：changelog 操作名 {} 不在码表里", ch.op));
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
