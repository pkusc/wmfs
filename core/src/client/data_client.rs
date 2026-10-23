//! `DataClient`：FUSE 层、CLI、后台任务用；选副本、CHUNK_DIRTY 重定向、换副本、按 3.3.8 重试（design.md 3.12、8.3）。
//!
//! - 写类请求（WriteChunk、TruncateChunk、SyncChunks、DeleteEpoch）发给链的 head（第一个 Serving 成员）；读发给一个 Serving 成员
//!   （`prefer_node` 上的优先），回 `CHUNK_DIRTY` 就改问 `extra` 指的那个 target，回 `CHECKSUM_MISMATCH`/`CORRUPT` 就换下一个副本——
//!   全部副本都坏了才 `EIO`（单副本时就是 `EIO`：诚实报错，不返回错误的数据）。
//! - 链、target、节点地址都来自集群图（`map()`）；集群图里还没有这条链、或者回了 `STALE_CHAIN`/`NOT_SERVING`，就调用 `refresh`
//!   （调用方给的"拉一张新集群图"，C6）再重试。可重试的错误（1000–1007、1019、`ECONNRESET`、`ETIMEDOUT`）按 3.3.8 退避（10 ms 起、
//!   ×2、上限 1 s），直到总期限（默认 120 s）；`CHUNK_NOT_FOUND` 原样交给调用方（C5：先 GetAttr 确认 epoch 没变，再当空洞读 0）。
//!   数据请求天然幂等（同一 extents 写两次结果相同，3.3.8），所以重试不需要 seq。不用 `net::retry::retry`：重定向与换副本要
//!   立即重发、不退避（重定向同样受总期限约束，换副本最多换链长那么多次），只有可重试的错误才退避。
//!
//! 实现注记（M5）：`new` 的 `background` 参数不用（后台请求由 cap 的管理权限位标明，`proto/cap.rs` 的注记）；读的 `into` 只在第一次
//! 尝试时当 `recv_into` 用，重试时从 RpcClient 的池里另取一个（所以返回的缓冲不一定是 `into`——3.12 的签名本来就返回缓冲）；副本先按链序
//! 选（codec 的 `read_order` 在 M9 有多副本时接入）。新增（只增不改）：[`DataClient::with_refresh`]、[`DataClient::with_timeouts`]、
//! [`DataClient::for_meta`]、[`DataClient::pool`]、[`chunk_id`]。实现注记（M6）：DataClient 也给服务端用（Uninline、后台任务），
//! 所以它自己取接收缓冲时仍用服务端的 `buf::POOL_WAIT`（100 ms）；客户端文件层取写回、读、同步写的缓冲用 `client::CLIENT_POOL_WAIT`。

use std::sync::Arc;
use std::time::Duration;

use super::meta_client::MetaClient;
use crate::base::types::ChunkId;
use crate::base::{ErrCode, clock, rand};
use crate::buf::{Buf, BufSlice, Lane, MemPool, POOL_WAIT};
use crate::meta::cluster::chains::SERVING;
use crate::meta::schema::records::LayoutV1;
use crate::net::client::{Endpoint, Family, RpcClient};
use crate::net::retry::{BACKOFF_MAX_MS, BACKOFF_START_MS, retryable};
use crate::proto::data::{DataCall, DataOp, DataReply};
use crate::proto::frame::{FrameHeader, FrameKind, InFrame, MSG_DATA_CALL, OutFrame};
use crate::proto::peer::ClusterMap;
use crate::proto::{decode_body, encode_body};

pub struct DataClient {
    rpc: Arc<RpcClient>,
    map: Arc<dyn Fn() -> Arc<ClusterMap> + Send + Sync>,
    refresh: Option<Arc<dyn Fn() + Send + Sync>>,
    rpc_timeout: Duration,
    op_timeout: Duration,
}

/// 发给谁：链的 head，或某个 Serving 成员（读：`prefer` = 优先这个节点上的；`into` = 第一次尝试的接收缓冲，`len` = 重试时
/// 另取的接收缓冲多大）。
enum To {
    Head,
    Read { prefer: Option<u32>, into: Option<Buf>, len: usize },
}

/// 文件第 `idx` 个 chunk 的身份：链 = `layout.chains[idx % chains.len()]`（3.2.4，在 (ino, epoch) 的生命周期内固定）。
/// layout 没有链 → `EINVAL`。
pub fn chunk_id(layout: &LayoutV1, ino: u64, idx: u32) -> Result<ChunkId, ErrCode> {
    let k = layout.chains.len();
    if k == 0 {
        return Err(ErrCode::EINVAL);
    }
    Ok(ChunkId { chain: layout.chains[idx as usize % k], ino, epoch: layout.epoch, idx })
}

impl DataClient {
    pub fn new(
        rpc: Arc<RpcClient>,
        map: Arc<dyn Fn() -> Arc<ClusterMap> + Send + Sync>,
        background: bool,
    ) -> DataClient {
        let _ = background; // 见文件头的实现注记
        let (rpc_timeout, op_timeout) = (Duration::from_secs(10), Duration::from_secs(120));
        DataClient { rpc, map, refresh: None, rpc_timeout, op_timeout }
    }

    /// 用 MetaClient 的集群图建一个 DataClient（CLI、FUSE、测试共用）：集群图过期时让 MetaClient 拉一张新的（C6）。
    pub fn for_meta(
        rpc: Arc<RpcClient>,
        meta: Arc<MetaClient>,
        rpc_timeout: Duration,
        op_timeout: Duration,
    ) -> DataClient {
        let map_src = meta.clone();
        let refresher = meta;
        DataClient::new(rpc, Arc::new(move || map_src.map()), false)
            .with_refresh(Arc::new(move || {
                let _ = refresher.refresh_map(); // 拉不到就算了：下一次重试再拉
            }))
            .with_timeouts(rpc_timeout, op_timeout)
    }

    /// 集群图过期（还没有这条链、`STALE_CHAIN`、`NOT_SERVING`）时调用 `f` 拉一张新的（C6）。
    pub fn with_refresh(mut self, f: Arc<dyn Fn() + Send + Sync>) -> DataClient {
        self.refresh = Some(f);
        self
    }

    /// 单次 RPC 超时（默认 10 s）与连同重试的总期限（默认 120 s，挂载参数 `--op-timeout`）。
    pub fn with_timeouts(mut self, rpc_timeout: Duration, op_timeout: Duration) -> DataClient {
        (self.rpc_timeout, self.op_timeout) = (rpc_timeout, op_timeout);
        self
    }

    /// 底下那个 RpcClient 的内存池（发数据前从这里取缓冲）。
    pub fn pool(&self) -> &MemPool {
        self.rpc.pool()
    }

    /// 返回 (缓冲, 实际长度)：实际长度可能小于 `len`（chunk 的 `logical_len` 之后），其余由调用方补 0。
    #[allow(clippy::too_many_arguments)] // 签名按 design.md 3.12
    pub fn read(
        &self,
        cap: &[u8],
        layout: &LayoutV1,
        chunk: ChunkId,
        off: u32,
        len: u32,
        into: Buf,
        prefer_node: Option<u32>,
    ) -> Result<(Buf, u32 /*实际长度*/), ErrCode> {
        let _ = layout; // 多副本、纠删码时选分片用（M9 起）
        let op = |ver| DataOp::ReadChunk { chunk, chain_ver: ver, off, len };
        let to = To::Read { prefer: prefer_node, into: Some(into), len: len as usize };
        let f = self.exchange(cap, chunk.chain, &op, &[], to)?;
        let n = match decode_body(&f.body)? {
            DataReply::Read { len, .. } => len,
            _ => return Err(ErrCode::INTERNAL),
        };
        let buf = match f.bulk {
            Some(b) => b,
            None => {
                // 读到 0 字节时回复不带 bulk，而 `into` 已经交给了 RpcClient：另取一个空缓冲来满足返回类型
                let mut b = self.rpc.pool().get(1, Lane::Normal, POOL_WAIT)?;
                b.set_len(0);
                b
            }
        };
        if buf.len() < n as usize {
            return Err(ErrCode::INTERNAL);
        }
        Ok((buf, n))
    }

    /// 返回 version
    pub fn write(
        &self,
        cap: &[u8],
        chunk: ChunkId,
        extents: Vec<(u32, u32)>,
        data: Vec<BufSlice>,
        sync: bool,
    ) -> Result<u64 /*version*/, ErrCode> {
        let op = |ver| DataOp::WriteChunk { chunk, chain_ver: ver, extents: extents.clone(), sync };
        written(&self.exchange(cap, chunk.chain, &op, &data, To::Head)?)
    }

    pub fn truncate(&self, cap: &[u8], chunk: ChunkId, new_len: u32) -> Result<u64, ErrCode> {
        let op = |ver| DataOp::TruncateChunk { chunk, chain_ver: ver, new_len };
        written(&self.exchange(cap, chunk.chain, &op, &[], To::Head)?)
    }

    pub fn sync(&self, cap: &[u8], chain: u32, chunks: Vec<ChunkId>) -> Result<(), ErrCode> {
        let op = |ver| DataOp::SyncChunks { chain, chain_ver: ver, chunks: chunks.clone() };
        match decode_body(&self.exchange(cap, chain, &op, &[], To::Head)?.body)? {
            DataReply::Ok => Ok(()),
            _ => Err(ErrCode::INTERNAL),
        }
    }

    /// 只给 gc（管理权限 cap）
    pub fn delete_epoch(&self, cap: &[u8], chain: u32, ino: u64, epoch: u32) -> Result<u64, ErrCode> {
        let op = |ver| DataOp::DeleteEpoch { chain, chain_ver: ver, ino, epoch };
        match decode_body(&self.exchange(cap, chain, &op, &[], To::Head)?.body)? {
            DataReply::Deleted { files } => Ok(files),
            _ => Err(ErrCode::INTERNAL),
        }
    }

    /// 发一个数据请求并按 3.3.8 重试（见文件头）；`op(chain_ver)` 生成这一次的请求。
    fn exchange(
        &self,
        cap: &[u8],
        chain: u32,
        op: &dyn Fn(u64) -> DataOp,
        bulk: &[BufSlice],
        mut to: To,
    ) -> Result<InFrame, ErrCode> {
        let deadline = clock::mono_ms().saturating_add(self.op_timeout.as_millis() as u64);
        let trace_id = rand::random_u64();
        let (mut backoff, mut skip, mut redirect) = (BACKOFF_START_MS, Vec::new(), None);
        loop {
            let map = (self.map)();
            let code = match pick(&map, chain, &to, &skip, redirect.take()) {
                Err(e) => e,
                Ok((ver, target, ep)) => {
                    let recv_into = match &mut to {
                        To::Read { into, len, .. } => {
                            into.take().or_else(|| self.rpc.pool().get((*len).max(1), Lane::Normal, POOL_WAIT).ok())
                        }
                        To::Head => None,
                    };
                    let body = encode_body(&DataCall { cap: cap.to_vec(), op: op(ver) });
                    let hdr = FrameHeader::new(FrameKind::Request, MSG_DATA_CALL, 0, trace_id);
                    let f = OutFrame { hdr, body, bulk: bulk.to_vec(), recv_into };
                    match self.rpc.call(&ep, Family::Data, f, self.rpc_timeout) {
                        Ok(f) => match f.error() {
                            None => return Ok(f),
                            // 重定向也受总期限约束（过了期限就按下面的可重试错误处理：到期返回 CHUNK_DIRTY）
                            Some(e) if e.code == ErrCode::CHUNK_DIRTY.0 && clock::mono_ms() < deadline => {
                                redirect = Some(e.extra as u32); // 改问 last serving（CRAQ，3.4.2.5）
                                continue;
                            }
                            // 换副本最多换链长那么多次（都坏了 pick 回 EIO）
                            Some(e) if matches!(to, To::Read { .. }) && is_bad_replica(ErrCode(e.code)) => {
                                skip.push(target); // 这个副本坏了：换下一个
                                continue;
                            }
                            Some(e) => ErrCode(e.code),
                        },
                        Err(e) => e,
                    }
                }
            };
            if (code == ErrCode::STALE_CHAIN || code == ErrCode::NOT_SERVING)
                && let Some(f) = &self.refresh
            {
                f();
            }
            if !retryable(code) || clock::mono_ms().saturating_add(backoff) >= deadline {
                return Err(code);
            }
            std::thread::sleep(Duration::from_millis(backoff));
            backoff = (backoff * 2).min(BACKOFF_MAX_MS);
        }
    }
}

fn is_bad_replica(c: ErrCode) -> bool {
    c == ErrCode::CHECKSUM_MISMATCH || c == ErrCode::CORRUPT
}

fn written(f: &InFrame) -> Result<u64, ErrCode> {
    match decode_body(&f.body)? {
        DataReply::Written { version } => Ok(version),
        _ => Err(ErrCode::INTERNAL),
    }
}

/// 这一次发给谁：(chain_ver, target, 它所在节点的地址)。读的副本都试过、都坏了 → `EIO`；集群图里没有这条链、没有 Serving
/// 成员或找不到地址 → `NOT_SERVING`（拉新集群图后重试）。
fn pick(
    map: &ClusterMap,
    chain: u32,
    to: &To,
    skip: &[u32],
    redirect: Option<u32>,
) -> Result<(u64, u32, Endpoint), ErrCode> {
    let (_, c) = map.chains.iter().find(|(id, _)| *id == chain).ok_or(ErrCode::NOT_SERVING)?;
    let node_of = |t: u32| map.targets.iter().find(|(id, _)| *id == t).map(|(_, r)| r.node);
    let mut serving: Vec<u32> = c.members.iter().filter(|(_, s)| *s == SERVING).map(|(t, _)| *t).collect();
    let target = match (to, redirect) {
        (To::Head, _) => *serving.first().ok_or(ErrCode::NOT_SERVING)?,
        (To::Read { .. }, Some(t)) => t,
        (To::Read { prefer, .. }, None) => {
            serving.retain(|t| !skip.contains(t));
            if let Some(n) = prefer {
                serving.sort_by_key(|t| node_of(*t) != Some(*n)); // 稳定排序：同节点的在前，其余保持链序
            }
            *serving.first().ok_or(if skip.is_empty() { ErrCode::NOT_SERVING } else { ErrCode::EIO })?
        }
    };
    let node = node_of(target).ok_or(ErrCode::NOT_SERVING)?;
    let addrs =
        map.nodes.iter().find(|(id, _)| *id == node).map(|(_, n)| n.addrs.clone()).ok_or(ErrCode::NOT_SERVING)?;
    Ok((c.ver, target, Endpoint { node, addrs }))
}
