//! 数据节点报给元数据 leader 的两件事（design.md 3.3.6 的 `NodeOp`；实现注记（M11））。
//!
//! - **`CheckEpochs { items: [(ino, epoch)] }`**（数据节点的本地孤儿扫描，4.12 gc）：线性一致地读出每个 inode，回 `Epochs { dead }`。
//!   死了 = inode 已经不在了，或这个 epoch 不是它当前 layout 的、也不是正在迁移的 `pending_epoch`（3.10 第 2 步："孤儿扫描把
//!   pending_epoch 视为存活"）。比 `max_epoch` 还大的 epoch 不会是这个 inode 写出来的：不认识就当活着（宁可不删）。还有
//!   GC 记录（kind 2、3）的 epoch 也当活着：它归 leader 的 gc 删——要等宽限期，迁移换下来的还要等没人打开着这个文件（3.5.6 的
//!   M11 注记，ADR-0042 第 5 条）；本地孤儿扫描只清 leader 的 gc 已经不管的残留。一次最多 [`CHECK_EPOCHS_MAX`] 项。
//! - **`ReportCorrupt { target, chunk }`**（scrub 与读写路径的修复，`data/repair.rs` 的文件头）：`target ≠ 0` = 这个 target
//!   上的坏副本已经从别的副本修好了 → changelog `ChecksumRepaired`；`target = 0` = 一个好副本也没有 → `DataLossDetected`（同一个 chunk
//!   在本 leader 上只报一次：各个副本的 scrub 都会发现它）。事件的 `ino` 是文件，`extra` 写 `chain`/`epoch`/`idx`（修好的另写
//!   `target`）。

use std::collections::HashSet;
use std::sync::Mutex;

use crate::base::types::{ChunkId, ReqCtx};
use crate::base::{ErrCode, lock, log};
use crate::meta::changelog::{self, Change};
use crate::meta::engine::txn::{MetaEngine, TxnOp, txn_blocking};
use crate::meta::schema::keys;
use crate::meta::schema::kv;
use crate::meta::schema::records::InodeV1;
use crate::proto::meta::MetaReply;

/// 一次 CheckEpochs 最多问多少项。
pub const CHECK_EPOCHS_MAX: usize = 1000;

/// 本 leader 上报过 DataLossDetected 的 chunk（只在内存里：换 leader 之后可能再报一次）。
pub type Lost = Mutex<HashSet<ChunkId>>;

/// `(ino, epoch)` 死了没有（见文件头）。
pub fn dead(i: Option<&InodeV1>, epoch: u32) -> bool {
    match i {
        None => true,
        Some(i) => {
            let current = i.layout.as_ref().is_some_and(|l| l.epoch == epoch);
            epoch <= i.max_epoch && !current && i.pending_epoch != epoch
        }
    }
}

pub fn check_epochs(eng: &dyn MetaEngine, ctx: &ReqCtx, items: &[(u64, u32)]) -> Result<Vec<bool>, ErrCode> {
    if items.len() > CHECK_EPOCHS_MAX {
        return Err(ErrCode::EINVAL);
    }
    let mut out = vec![];
    // 这个闭包只用来在 ReadIndex 之后的视图上取出每一项的结论，它的返回值丢弃
    eng.read(ctx, &mut |v, _c| {
        out.clear();
        for (ino, epoch) in items {
            // leader 的 gc 还管着它（见文件头）
            let tracked = v.get(&keys::gc(keys::GC_EPOCH, *ino, *epoch))?.is_some()
                || v.get(&keys::gc(keys::GC_MOVED, *ino, *epoch))?.is_some();
            out.push(!tracked && dead(kv::get::<InodeV1>(v, &keys::inode(*ino))?.as_ref(), *epoch));
        }
        Ok(MetaReply::Ok)
    })?;
    Ok(out)
}

pub fn report(eng: &dyn MetaEngine, lost: &Lost, target: u32, chunk: ChunkId) -> Result<(), ErrCode> {
    if target == 0 && !lock(lost).insert(chunk) {
        return Ok(()); // 报过了
    }
    let op_name = if target == 0 { "DataLossDetected" } else { "ChecksumRepaired" };
    let mut extra = vec![
        ("chain".into(), chunk.chain.to_string()),
        ("epoch".into(), chunk.epoch.to_string()),
        ("idx".into(), chunk.idx.to_string()),
    ];
    if target != 0 {
        extra.push(("target".into(), target.to_string()));
    }
    log::warn("meta", format_args!("{op_name}：inode {} 的 chunk {chunk:?}（target {target}）", chunk.ino));
    let op: TxnOp = Box::new(move |v, c| {
        changelog::append(v, c, Change { op: op_name, ino: chunk.ino, extra, ..Default::default() })?;
        Ok(MetaReply::Ok)
    });
    let r = txn_blocking(eng, ReqCtx::internal(), op).map(|_| ());
    if r.is_err() && target == 0 {
        lock(lost).remove(&chunk); // 没写成：下次再报
    }
    r
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::schema::records::LayoutV1;

    #[test]
    fn epochs_that_are_current_or_being_migrated_are_alive() {
        let layout = LayoutV1 { epoch: 3, chains: vec![1], ..Default::default() };
        let i =
            InodeV1 { kind: 1, nlink: 1, layout: Some(layout), pending_epoch: 4, max_epoch: 5, ..Default::default() };
        assert!(!dead(Some(&i), 3), "当前 layout 的 epoch 是活的");
        assert!(dead(None, 1), "inode 已经不在");
        assert!(!dead(Some(&i), 4), "正在迁移的 pending_epoch 是活的");
        assert!(!dead(Some(&i), 9), "比 max_epoch 还大：不认识，当活着");
        assert!(dead(Some(&i), 2), "旧 epoch");
    }
}
