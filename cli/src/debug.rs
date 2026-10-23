//! `wmfs debug kv put|get|bench`（design.md 7.2、3.2.7 的调试键空间 0x2F）：经 MetaCall 的 `DebugPut` / `DebugGet` 读写
//! 元数据 KV。服务端 node.toml 要有 `debug_ops = true`（`wmfs dev` 默认打开），且只接受 admin key。
//!
//! 先问 `--cluster` / `WMFS_CLUSTER` 里的管理地址的 `/api/v1/health` 找到元数据 leader 的 rpc 地址（最多找 30 s，`leader.rs`），
//! 再用 `RpcClient` 发请求；可重试的错误（leader 还没就绪、连接断开……）按 3.3.8 退避重试，总期限 30 s。
//! `bench`：用 `RpcClient::start` 保持 64 路并发写 `bench-<序号>`（值 32 字节），打印写入速率与平均批大小——后者取 leader 的
//! `/metrics` 里 `wmfs_meta_batch_ops` 直方图在压测前后的 `_sum`/`_count` 增量（单飞批次在负载高时自然攒批，3.4.1.4）；
//! 压测期间 leader 换过（term 变了，计数器从 0 重新开始）就不报批大小。可重试的失败按 3.3.8 退避：故障时一起失败的
//! 那一批在途请求只退避一次，退避期间不发新请求。
//!
//! - [`chunk`]：`wmfs debug chunk-path / chunk-header / corrupt-chunk`（M5；只在编进 local-fs 插件时有）

#[cfg(feature = "local-fs")]
pub mod chunk;

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::Serialize;
use wmfs_core::base::{ErrCode, clock};
use wmfs_core::net::client::{Endpoint, Family, PendingCall, RpcClient, check_reply};
use wmfs_core::net::retry::{BACKOFF_MAX_MS, BACKOFF_START_MS, retry, retryable};
use wmfs_core::proto::frame::{FrameHeader, FrameKind, InFrame, MSG_META_CALL, OutFrame};
use wmfs_core::proto::meta::{MetaCall, MetaOp, MetaReply, ReqHead};
use wmfs_core::proto::{decode_body, encode_body};
use wmfs_mgr::api::ApiClient;

use crate::{cluster_addrs, fail, leader, load_key, out};

#[derive(clap::Subcommand)]
pub enum KvCmd {
    /// 写一个调试键，例如 wmfs debug kv put hello world
    Put {
        key: String,
        value: String,
        #[command(flatten)]
        c: KvArgs,
    },
    /// 读一个调试键（线性一致读）；不存在时退出码 1
    Get {
        key: String,
        #[command(flatten)]
        c: KvArgs,
    },
    /// 写入压测：保持 64 路并发写 --n 条 bench-<序号>
    Bench {
        #[arg(long, default_value_t = 100_000)]
        n: u64,
        #[command(flatten)]
        c: KvArgs,
    },
}

#[derive(clap::Args)]
pub struct KvArgs {
    /// 集群的管理地址，逗号分隔（默认 $WMFS_CLUSTER）
    #[arg(long)]
    cluster: Option<String>,
    /// 管理员密钥（默认 $WMFS_KEY_FILE，否则 ~/.config/wmfs/admin.key）
    #[arg(long)]
    key_file: Option<PathBuf>,
    /// 输出 JSON
    #[arg(long)]
    json: bool,
}

/// 单次 RPC 超时（3.3.8）、一条命令连同重试的总期限、压测的总期限。
const RPC_TIMEOUT: Duration = Duration::from_secs(10);
const OP_TIMEOUT: Duration = Duration::from_secs(30);
const BENCH_LIMIT_MS: u64 = 600_000;
/// 压测保持多少路并发（design.md 7.2）、每个值多少字节。
const CONCURRENCY: usize = 64;
const VALUE_SIZE: usize = 32;

/// 连到元数据 leader 的客户端。
struct Kv {
    rpc: Arc<RpcClient>,
    ep: Endpoint,
    api: ApiClient, // leader 的管理地址（bench 读 /metrics）
}

#[derive(Serialize)]
struct Bench {
    n: u64,
    concurrency: usize,
    ok: u64,
    retries: u64,
    elapsed_ms: u64,
    ops_per_sec: f64,
    batches: u64,
    avg_batch: f64,
}

pub fn run(cmd: KvCmd) -> i32 {
    let (c, r) = match cmd {
        KvCmd::Put { key, value, c } => {
            let r = connect(&c).and_then(|kv| put(&kv, key.into_bytes(), value.into_bytes()));
            (c, r.map(|()| (0, serde_json::json!({"ok": true}), "ok".to_string())))
        }
        KvCmd::Get { key, c } => {
            let r = connect(&c).and_then(|kv| get(&kv, &key));
            (c, r)
        }
        KvCmd::Bench { n, c } => {
            let r = connect(&c).and_then(|kv| bench(&kv, n));
            (c, r.map(|b| (0, serde_json::to_value(&b).unwrap_or_default(), describe(&b))))
        }
    };
    match r {
        Ok((code, json, text)) => {
            let w = out(if c.json { json.to_string() } else { text });
            if w != 0 { w } else { code }
        }
        Err(e) => fail(e),
    }
}

fn connect(c: &KvArgs) -> Result<Kv, String> {
    let key = load_key(&c.key_file)?;
    let l = leader::connect(&cluster_addrs(&c.cluster)?, key.clone())?;
    let api = ApiClient::new(vec![l.addrs.mgmt_addr.clone()], key);
    Ok(Kv { rpc: l.rpc, ep: l.ep, api })
}

fn frame(op: MetaOp) -> OutFrame {
    let head = ReqHead { session: 0, seq: 0, uid: 0, gid: 0, groups: vec![], pid: std::process::id(), job_id: 0 };
    let body = encode_body(&MetaCall { head, op });
    OutFrame { hdr: FrameHeader::new(FrameKind::Request, MSG_META_CALL, 0, 0), body, bulk: vec![], recv_into: None }
}

fn reply(f: InFrame) -> Result<MetaReply, ErrCode> {
    decode_body(&check_reply(f)?.body)
}

/// 发一个调试请求，按 3.3.8 重试（总期限 30 s）。
fn call(kv: &Kv, op: MetaOp) -> Result<MetaReply, String> {
    let what = format!("debug kv {}", op.name());
    retry(&what, OP_TIMEOUT, |_| reply(kv.rpc.call(&kv.ep, Family::Control, frame(op.clone()), RPC_TIMEOUT)?))
        .map_err(|e| why(&what, e))
}

fn why(what: &str, e: ErrCode) -> String {
    let hint = match e {
        ErrCode::EACCES => "：需要 admin key，且服务端 node.toml 里 debug_ops = true（wmfs dev 默认打开）",
        ErrCode::NOT_LEADER | ErrCode::NOT_READY => "：还没有就绪的元数据 leader（wmfs dev wait leader）",
        ErrCode::ECONNRESET | ErrCode::ETIMEDOUT => "：连不上元数据 leader（节点在运行吗？wmfs dev status）",
        _ => "",
    };
    format!("{what} 失败：{e}{hint}")
}

fn put(kv: &Kv, key: Vec<u8>, value: Vec<u8>) -> Result<(), String> {
    match call(kv, MetaOp::DebugPut { key, value })? {
        MetaReply::Ok => Ok(()),
        other => Err(format!("DebugPut 的回复不对：{other:?}")),
    }
}

/// 返回 (退出码, JSON, 给人看的文字)：键不存在时退出码 1。
fn get(kv: &Kv, key: &str) -> Result<(i32, serde_json::Value, String), String> {
    match call(kv, MetaOp::DebugGet { key: key.as_bytes().to_vec() })? {
        MetaReply::DebugValue { value: Some(v) } => {
            let v = String::from_utf8_lossy(&v).into_owned();
            Ok((0, serde_json::json!({"key": key, "found": true, "value": v}), v))
        }
        MetaReply::DebugValue { value: None } => {
            Ok((1, serde_json::json!({"key": key, "found": false, "value": null}), format!("（没有这个键：{key}）")))
        }
        other => Err(format!("DebugGet 的回复不对：{other:?}")),
    }
}

fn bench(kv: &Kv, n: u64) -> Result<Bench, String> {
    let before = batch_stats(&kv.api);
    let t0 = Instant::now();
    let deadline = clock::mono_ms() + BENCH_LIMIT_MS;
    let mut next = 0u64; // 下一个还没发过的序号
    let mut ok = 0u64;
    let mut retries = 0u64;
    let mut backoff = 0u64; // 当前的退避（ms）；成功一次就清零
    let mut backoff_since = 0u64; // 最近一次加大退避的时刻（mono_ms）
    let mut redo: VecDeque<u64> = VecDeque::new(); // 要重发的序号
    let mut inflight: VecDeque<(u64, u64, PendingCall)> = VecDeque::new(); // (序号, 发出的时刻, 调用)
    while ok < n {
        let resume_at = backoff_since + backoff; // 退避期间不发新请求
        let now = clock::mono_ms();
        if now < resume_at {
            std::thread::sleep(Duration::from_millis(resume_at - now));
        }
        while inflight.len() < CONCURRENCY {
            let i = match redo.pop_front() {
                Some(i) => i,
                None if next < n => {
                    next += 1;
                    next - 1
                }
                None => break,
            };
            let mut value = format!("v{i}-").into_bytes();
            value.resize(VALUE_SIZE.max(value.len()), b'.');
            let f = frame(MetaOp::DebugPut { key: format!("bench-{i:08}").into_bytes(), value });
            let pc = kv.rpc.start(&kv.ep, Family::Control, f, RPC_TIMEOUT).map_err(|e| why("debug kv bench", e))?;
            inflight.push_back((i, clock::mono_ms(), pc));
        }
        let Some((i, sent, pc)) = inflight.pop_front() else { break };
        match pc.wait().and_then(reply) {
            Ok(MetaReply::Ok) => {
                ok += 1;
                backoff = 0;
            }
            Ok(other) => return Err(format!("bench-{i:08} 的回复不对：{other:?}")),
            Err(e) if retryable(e) && clock::mono_ms() < deadline => {
                retries += 1;
                redo.push_back(i);
                // 只有上次加大退避之后才发出的请求失败了，才说明故障还在、再加倍：一起失败的那一批只退避一次
                if sent >= backoff_since {
                    backoff = (backoff * 2).clamp(BACKOFF_START_MS, BACKOFF_MAX_MS);
                    backoff_since = clock::mono_ms();
                }
            }
            Err(e) => return Err(why(&format!("写 bench-{i:08}"), e)),
        }
    }
    let secs = t0.elapsed().as_secs_f64();
    let after = batch_stats(&kv.api);
    let mut batches = 0;
    let mut avg_batch = 0.0;
    if let (Some(b), Some(a)) = (before, after)
        && a.term == b.term
        && a.count > b.count
    {
        batches = (a.count - b.count) as u64;
        avg_batch = (a.sum - b.sum) / (a.count - b.count);
    }
    let ops_per_sec = if secs > 0.0 { ok as f64 / secs } else { 0.0 };
    let elapsed_ms = (secs * 1000.0) as u64;
    Ok(Bench { n, concurrency: CONCURRENCY, ok, retries, elapsed_ms, ops_per_sec, batches, avg_batch })
}

/// leader 的 `/metrics` 里批次直方图 `wmfs_meta_batch_ops` 的 `_sum`、`_count`，以及当时的 raft term。
struct BatchStats {
    sum: f64,
    count: f64,
    term: f64,
}

/// 读不到就是 None（只影响"平均批大小"一项）。
fn batch_stats(api: &ApiClient) -> Option<BatchStats> {
    let text: String = api.get("/metrics").ok()?; // /metrics 是 Prometheus 文本，ApiClient 把它原样交成字符串
    let find = |name: &str| text.lines().find_map(|l| l.strip_prefix(name)?.trim().parse::<f64>().ok());
    Some(BatchStats {
        sum: find("wmfs_meta_batch_ops_sum ")?,
        count: find("wmfs_meta_batch_ops_count ")?,
        term: find("wmfs_raft_term ")?,
    })
}

fn describe(b: &Bench) -> String {
    let batches = if b.batches > 0 {
        format!("{} 个批次，平均每批 {:.1} 个操作", b.batches, b.avg_batch)
    } else {
        "批大小统计不可用（压测期间 leader 重启或换过，或读不到 leader 的 /metrics）".to_string()
    };
    format!(
        "写入 {} 条，用时 {:.1} s，{:.0} 条/秒；{batches}（{} 路并发，重试 {} 次）",
        b.ok,
        b.elapsed_ms as f64 / 1000.0,
        b.ops_per_sec,
        b.concurrency,
        b.retries
    )
}
