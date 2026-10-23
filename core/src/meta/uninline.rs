//! 服务端把 inline 小文件转成 chunk（ADR-0034、design.md 3.3.4 的 `Uninline`）：只有写权限的人也能把小文件写大，`GetInline`
//! 仍只给能读的人。在 leader 上、处理这个请求的 meta-read 线程里做（不占执行器与 raft 线程）：
//!
//! 1. 线性一致地读出 inode（记下 change_id c）和 INLINE 内容；调用者要有写权限；已经不是 inline → 直接回成功（幂等）。
//! 2. 用 leader 自己签的写 cap（session 0）按文件当前的 layout（同一个 epoch、create 时分好的链）把内容写进 chunk 0、再截断到内容
//!    长度（上一次没做完的 uninline 可能在 chunk 里留下更长的旧内容）、`SyncChunks`——**清掉 INLINE 之前数据必须已持久**。
//! 3. 在执行器里提交 `CommitWrite { uninline_expect_change_id: Some(c) }`（CAS：change_id 仍是 c 才清除 F_INLINE 与 INLINE）。
//! 4. CAS 失败（期间有人改了 inline 内容）→ 从第 1 步重做，最多 3 次，仍失败回 `RETRY`（客户端按 3.3.8 重试）。
//!
//! 内容与大小都没变、只换了存放位置，所以不写 changelog。崩溃在第 2、3 步之间：INLINE 还在，读照旧读 INLINE；chunk 里写了一半的
//! 内容没人读，下一次 Uninline 会覆盖它，文件删掉或截断到 0 时由 gc 按 epoch 清理。fail point `meta.uninline.before_commit`
//! （3.11 的命名习惯）埋在第 2、3 步之间，测试用它模拟"写完 chunk、提交之前崩溃"。
//! 3.10 迁移原语第 3、4 步的"把 INLINE 内容按 L′ 写进 chunk"与这里共用 [`write_inline`]（ADR-0034 第 5 条，M11）。
//!
//! 实现注记（M5）：同一个 inode 同时最多一个服务端 Uninline（[`InFlight`]，leader 内存里）——否则落后的那个可能在别人 CAS 成功、
//! 客户端已经往 chunk 0 写了新数据并 CommitWrite 之后，才把旧的 inline 内容写进 chunk 0、再截掉新数据（它自己的 CAS 失败后照常回
//! 成功），已确认的写就丢了。全部加起来最多 4 个（meta-read 线程的一半，原因见 service.rs 的文件头）。超出都回 `BUSY`：客户端退避
//! 重试，重试时多半已经不是 inline，直接幂等成功。换 leader 之后（M8）旧 leader 上还没做完的 Uninline 不受这里约束，要数据面的
//! fencing（ADR-0035 第 11 条）。

use std::collections::HashSet;
use std::sync::Mutex;

use wmfs_api::failpoint::fail_point;

use crate::base::types::ReqCtx;
use crate::base::{ErrCode, lock};
use crate::buf::{Lane, POOL_WAIT};
use crate::client::data_client::{DataClient, chunk_id};
use crate::meta::engine::txn::{MetaEngine, TxnOp, txn_blocking};
use crate::meta::ns::file_ops::{self, CommitIn, UninlineTodo};
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
        let layout = open.layout.clone().ok_or(ErrCode::INTERNAL)?;
        write_inline(data, &todo.cap, &layout, ino, &todo.content)?;
        fail_point("meta.uninline.before_commit").map_err(ErrCode)?;
        let w =
            CommitIn { ino, epoch: layout.epoch, new_size: 0, patches: vec![], expect_change_id: Some(todo.change_id) };
        let op: TxnOp = Box::new(move |v, c| file_ops::commit_write(v, c, &w));
        let mut c = ctx.clone();
        c.seq = 0; // CAS 本身幂等：不进去重表（ADR-0034 第 2 条）
        match txn_blocking(eng, c, op) {
            Ok(MetaReply::Committed { attr, .. }) => return Ok(MetaReply::Opened(OpenOut { attr, ..open })),
            Ok(_) => return Err(ErrCode::INTERNAL),
            Err(ErrCode::STALE_LAYOUT) => continue, // CAS 失败：期间有人改了 inline 内容，从头再来
            Err(e) => return Err(e),
        }
    }
    Err(ErrCode::RETRY)
}

/// 把 inline 内容按 `layout`（当前 epoch）写进 chunk 0，并截断到内容长度、fdatasync（内容 ≤ 64 KiB ≤ chunk_size，只占 chunk 0）。
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
