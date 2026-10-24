//! 服务端把 inline 小文件转成 chunk（ADR-0034、design.md 3.3.4 的 `Uninline`）：只有写权限的人也能把小文件写大，`GetInline`
//! 仍只给能读的人。在 leader 上、处理这个请求的 meta-read 线程里做（不占执行器与 raft 线程）：
//!
//! 1. 线性一致地读出 inode（记下 change_id c）和 INLINE 内容；调用者要有写权限；已经不是 inline → 直接回成功（幂等）。
//! 2. **意图**（实现注记（M8），ADR-0035 第 11 条）：在执行器里提交一个新 epoch——`max_epoch += 1`、`pending_epoch = max_epoch`（CAS：
//!    change_id 仍是 c、仍是 inline）；别人留下的 `pending_epoch ≠ 0`（旧 leader 没做完的、或崩溃留下的）不复用，记为垃圾（GC 记录）。
//! 3. 用 leader 自己签的写 cap（session 0）把内容写进**这个新 epoch** 的 chunk 0（链与其余 layout 参数不变）、截断到内容长度、
//!    `SyncChunks`——**清掉 INLINE 之前数据必须已持久**。
//! 4. **切换**（CAS）：change_id 仍是 c、`pending_epoch` 仍是这次的 → `layout.epoch = pending_epoch`、清 `pending_epoch`、清 F_INLINE 与
//!    INLINE（旧 epoch 记为垃圾）。回给客户端的是新 layout（新 epoch）的 `Opened`，客户端之后的写直接进新 epoch。
//! 5. CAS 失败（期间有人改了 inline 内容，或换过意图）→ 从第 1 步重做，最多 3 次，仍失败回 `RETRY`（客户端按 3.3.8 重试）。
//!
//! **空内容**（新建之后第一次写大：`wmfs fs put`、`cp` 的常见路径）没有东西要搬：不提意图、不碰数据面，直接第 4 步（epoch 不变，
//! 别人留下的意图照样记为垃圾）。旧 leader 上迟到的空内容 uninline 也什么都不写，没有要挡的；而当前 epoch 下面不会有 chunk——新建
//! 与截断到 0 都给一个新的 epoch，M8 起非空内容只写进意图的 epoch。所以新建的大文件仍是 epoch 1（与 M5 相同）。
//!
//! 为什么这样就安全（ADR-0035 第 11 条）：换 leader 之后旧 leader 上落后的 uninline 只会把旧内容写进**它自己那个**意图的 epoch——新
//! leader 上的 uninline 已经把那个意图记为垃圾、换了新的，切换的 CAS 要求 `pending_epoch` 仍是自己的，所以只有一次尝试的 epoch 能成为
//! 文件的 layout，而且它的数据在 CAS 之前已经写好、落盘；迟到的写落进作废的 epoch，没人读，由 gc 清理（gc 删过之后才落下的极少数
//! chunk 文件由 M11 数据节点的本地孤儿扫描清理）。代价：每次 uninline 多一次提交。
//!
//! 内容与大小都没变、只换了存放位置，所以不写 changelog。崩溃在意图之后、切换之前：INLINE 还在，读照旧读 INLINE；意图的 epoch 下面写了
//! 一半的 chunk 没人读，下一次 Uninline 把它记为垃圾、换新的 epoch（截断到 0 之后文件又是 inline 的，照样由下一次 Uninline 记为垃圾；
//! 文件在那之前被删掉的话，这个最多 64 KiB 的 chunk 不在任何 GC 记录里，由 M11 数据节点的本地孤儿扫描清理）。fail point：
//! `meta.uninline.before_write`（意图已提交、写 chunk 之前，M8 新增：测试用它把"旧 leader 卡在写 chunk 之前"拉长）、
//! `meta.uninline.before_commit`（写完 chunk、切换之前）。3.10 迁移原语第 3、4 步的"把 INLINE 内容按 L′ 写进 chunk"与这里共用
//! [`write_inline`]（它按调用方给的 layout 写，迁移给 L′，这里给"新 epoch 的 L"，ADR-0034 第 5 条，M11）。
//!
//! 实现注记（M5）：同一个 inode 同时最多一个服务端 Uninline（[`InFlight`]，leader 内存里）、全部加起来最多 4 个（meta-read 线程的一半，
//! 原因见 service.rs 的文件头），超出都回 `BUSY`（客户端退避重试，重试时多半已经不是 inline，直接幂等成功）。这个内存互斥只管当前
//! leader；换 leader 之后由上面的 epoch 意图挡住旧 leader（M8）。

use std::collections::HashSet;
use std::sync::Mutex;

use wmfs_api::failpoint::fail_point;

use crate::base::types::ReqCtx;
use crate::base::{ErrCode, lock};
use crate::buf::{Lane, POOL_WAIT};
use crate::client::data_client::{DataClient, chunk_id};
use crate::meta::engine::txn::{MetaEngine, TxnOp, txn_blocking};
use crate::meta::ns::file_ops::{self, UninlineTodo};
use crate::meta::schema::records::LayoutV1;
use crate::proto::meta::{MetaReply, OpenOut};

/// CAS 失败后最多重做几次。
const ATTEMPTS: u32 = 3;

/// 正在做的 Uninline（见文件头的实现注记）：只在进出时短暂持锁，不跨网络持锁（1.2 规则 2）。
pub struct InFlight {
    inos: Mutex<HashSet<u64>>,
    max: usize,
}

impl InFlight {
    /// 同时最多 `max` 个。
    pub fn new(max: usize) -> InFlight {
        InFlight { inos: Mutex::new(HashSet::new()), max }
    }

    /// 登记 `ino`：它已经在做、或已经有 `max` 个在做 → `BUSY`。做完（不论结果）一定 [`InFlight::end`]。
    pub fn begin(&self, ino: u64) -> Result<(), ErrCode> {
        let mut s = lock(&self.inos);
        if s.len() >= self.max || !s.insert(ino) {
            return Err(ErrCode::BUSY);
        }
        Ok(())
    }

    pub fn end(&self, ino: u64) {
        lock(&self.inos).remove(&ino);
    }
}

/// 处理一个 `Uninline { ino }`（见文件头）；回复 `Opened(OpenOut)`。调用方已经 [`InFlight::begin`] 过。
pub fn run(eng: &dyn MetaEngine, data: &DataClient, ctx: &ReqCtx, ino: u64) -> Result<MetaReply, ErrCode> {
    for _ in 0..ATTEMPTS {
        let mut got: Option<(OpenOut, Option<UninlineTodo>)> = None;
        // 这个闭包只用来在 ReadIndex 之后的视图上取出 inode 的情况，它的返回值丢弃
        eng.read(ctx, &mut |v, c| {
            got = Some(file_ops::uninline_prepare(v, c, ino)?);
            Ok(MetaReply::Ok)
        })?;
        let (open, todo) = got.ok_or(ErrCode::INTERNAL)?;
        let Some(todo) = todo else { return Ok(MetaReply::Opened(open)) }; // 已经不是 inline：幂等
        let cid = todo.change_id;
        // 意图的 epoch：空内容没有东西要搬 → None（不碰数据面、不换 epoch，见文件头）；否则先提交意图、按新 epoch 写好数据 → Some
        let intent = if todo.content.is_empty() {
            None
        } else {
            let o = match commit(eng, ctx, Box::new(move |v, c| file_ops::uninline_intent(v, c, ino, cid))) {
                Ok(MetaReply::Opened(o)) => o,
                Ok(_) => return Err(ErrCode::INTERNAL),
                Err(ErrCode::STALE_LAYOUT) => continue, // 期间有人改了 inline 内容（或已经转过了）：从头再来
                Err(code) => return Err(code),
            };
            let layout = o.layout.ok_or(ErrCode::INTERNAL)?; // 按新 epoch 写的 layout
            fail_point("meta.uninline.before_write").map_err(ErrCode)?;
            write_inline(data, &o.cap, &layout, ino, &todo.content)?;
            fail_point("meta.uninline.before_commit").map_err(ErrCode)?;
            Some(layout.epoch)
        };
        match commit(eng, ctx, Box::new(move |v, c| file_ops::uninline_switch(v, c, ino, cid, intent))) {
            Ok(r @ MetaReply::Opened(_)) => return Ok(r),
            Ok(_) => return Err(ErrCode::INTERNAL),
            Err(ErrCode::STALE_LAYOUT) => continue, // CAS 失败：从头再来（下一次的意图把这次的 epoch 记为垃圾）
            Err(code) => return Err(code),
        }
    }
    Err(ErrCode::RETRY)
}

/// 意图与切换都是 seq 0 的批次：它们本身是 CAS、幂等，不进去重表（ADR-0034 第 2 条）。
fn commit(eng: &dyn MetaEngine, ctx: &ReqCtx, op: TxnOp) -> Result<MetaReply, ErrCode> {
    let mut c = ctx.clone();
    c.seq = 0;
    txn_blocking(eng, c, op)
}

/// 把 inline 内容按 `layout`（uninline：意图的新 epoch；迁移：L′）写进 chunk 0，并截断到内容长度、fdatasync（内容 ≤ 64 KiB ≤
/// chunk_size，只占 chunk 0）。
pub fn write_inline(data: &DataClient, cap: &[u8], layout: &LayoutV1, ino: u64, content: &[u8]) -> Result<(), ErrCode> {
    let chunk = chunk_id(layout, ino, 0)?;
    let len = u32::try_from(content.len()).ok().filter(|n| *n <= layout.chunk_size).ok_or(ErrCode::EINVAL)?;
    if len > 0 {
        let mut b = data.pool().get(content.len(), Lane::Normal, POOL_WAIT)?;
        b.as_mut_slice().copy_from_slice(content);
        let b = b.freeze();
        data.write(cap, chunk, vec![(0, len)], vec![b.whole()], false)?;
    }
    data.truncate(cap, chunk, len)?;
    data.sync(cap, chunk.chain, vec![chunk])
}
