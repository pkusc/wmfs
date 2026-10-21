//! `wmfs ping <scheme://地址> [--count N] [--bulk 4M] [--key-file F]`：传输自检（design.md 3.3.3 的 Ping、7.2）。
//!
//! 用 CLI 的小内存池（`PoolConfig::small()`）连上服务端、完成 3.3.2 握手，发 N 次 Ping（可带 bulk），核对回显
//! （payload 与 bulk 逐字节；bulk 的 CRC 由 core 收帧时校验），打印成功次数、p50/p99 延迟与吞吐。每次 Ping 按 3.3.8 重试
//! （连接断开 → 重连重试，带退避与 30 s 总期限）；遇到不可重试的错误（例如 `AUTH_FAILED`）立即停下。
//! 实现注记（M1）：`--role` 指定 Hello 里声明的角色（默认按 key 号推断：0 server、1 admin、其余 client），
//! 用来演示"client key 申请 server 角色被拒"；`--json` 输出给 scripts/accept.sh 解析。

use std::path::PathBuf;
use std::time::{Duration, Instant};

use serde::Serialize;
use wmfs_core::base::config::NetConfig;
use wmfs_core::base::types::Role;
use wmfs_core::base::{ErrCode, rand, units};
use wmfs_core::buf::pool::M_SLOT;
use wmfs_core::buf::{FrozenBuf, Lane, MemPool, PoolConfig};
use wmfs_core::net::client::{Endpoint, Family, RpcClient, check_reply, default_role};
use wmfs_core::net::retry::retry;
use wmfs_core::proto::encode_body;
use wmfs_core::proto::frame::{FrameHeader, FrameKind, InFrame, MAX_BULK, MSG_PING, OutFrame};
use wmfs_core::proto::handshake::PingBody;

use crate::{fail, load_key, registry, write_stdout};

#[derive(clap::Args)]
pub struct PingArgs {
    /// 目标：<scheme>://<地址>，例如 tcp://127.0.0.1:19100
    target: String,
    /// Ping 几次
    #[arg(long, default_value_t = 10)]
    count: u32,
    /// 每次带多大的 bulk（例如 4M；不写 = 不带）
    #[arg(long)]
    bulk: Option<String>,
    /// 密钥文件（默认 $WMFS_KEY_FILE，否则 ~/.config/wmfs/admin.key）
    #[arg(long)]
    key_file: Option<PathBuf>,
    /// 握手时声明的角色（默认按 key 号推断：0 server、1 admin、其余 client）
    #[arg(long, value_parser = ["admin", "client", "server"])]
    role: Option<String>,
    /// 输出 JSON
    #[arg(long)]
    json: bool,
}

/// 单次 RPC 超时（3.3.8）与每次 Ping 连同重试的总期限。
const RPC_TIMEOUT: Duration = Duration::from_secs(10);
const OP_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Serialize, Default)]
struct Report {
    target: String,
    count: u32,
    ok: u32,     // 成功且回显一致
    failed: u32, // 0 或 1（第一次失败就停下）
    bulk_bytes: u64,
    retries: u32,
    p50_us: u64,
    p99_us: u64,
    elapsed_ms: u64,
    gb_per_s: f64, // bulk 收发合计
    error: Option<String>,
}

pub fn run(a: PingArgs) -> i32 {
    match ping(&a) {
        Ok(r) => report(&a, r),
        Err(e) => fail(e),
    }
}

fn ping(a: &PingArgs) -> Result<Report, String> {
    let (scheme, addr) = a
        .target
        .split_once("://")
        .ok_or_else(|| format!("目标应写成 <scheme>://<地址>，例如 tcp://127.0.0.1:19100：{}", a.target))?;
    let bulk = a.bulk.as_deref().map(units::parse_size).transpose()?.unwrap_or(0);
    if bulk > MAX_BULK as u64 {
        return Err(format!("--bulk 最大 {}", units::fmt_size(MAX_BULK as u64)));
    }
    let key = load_key(&a.key_file)?;
    let role = match a.role.as_deref() {
        Some(r) => Role::from_name(r).ok_or_else(|| format!("--role 只能是 admin、client、server：{r}"))?,
        None => default_role(key.key_id),
    };
    let transports = registry::client_transports()?;
    if !transports.iter().any(|t| t.scheme() == scheme) {
        let have: Vec<&str> = transports.iter().map(|t| t.scheme()).collect();
        return Err(format!("本二进制没有 {scheme} 传输插件（有：{}）", have.join("、")));
    }
    // 发出的数据与回复各占一个槽：CLI 的小池 L 档只有 1 个槽，带超过 M 档的 bulk 时多要一个
    let l_slots = if bulk > M_SLOT as u64 { 2 } else { 1 };
    let pool =
        MemPool::new(&PoolConfig { l_slots, ..PoolConfig::small() }).map_err(|e| format!("内存池创建失败：{e}"))?;
    let net = NetConfig { control_scheme: scheme.into(), data_scheme: scheme.into(), ..NetConfig::default() };
    let rpc = RpcClient::new_with_role(pool.clone(), transports, key, role, net).map_err(|e| e.to_string())?;
    let ep = Endpoint { node: 0, addrs: vec![(scheme.to_string(), addr.to_string())] };
    let data = (bulk > 0).then(|| random_buf(&pool, bulk as usize)).transpose()?;
    let mut r = Report { target: a.target.clone(), count: a.count, bulk_bytes: bulk, ..Report::default() };
    let (t0, mut lat) = (Instant::now(), Vec::new());
    for i in 0..a.count {
        let res = retry(&format!("ping {}", a.target), OP_TIMEOUT, |attempt| {
            r.retries += (attempt > 0) as u32;
            let t = Instant::now();
            let f = check_reply(rpc.call(&ep, Family::Data, frame(&pool, i, data.as_ref())?, RPC_TIMEOUT)?)?;
            Ok((f, t.elapsed()))
        });
        match res {
            Ok((f, d)) if echoed(&f, i, data.as_ref()) => {
                r.ok += 1;
                lat.push(d);
            }
            Ok(_) => return Err(format!("第 {} 次 Ping 的回显与发出的不一致（数据被改了？）", i + 1)),
            Err(e) => {
                r.failed = 1;
                r.error = Some(e.to_string());
                break;
            }
        }
    }
    let secs = t0.elapsed().as_secs_f64();
    lat.sort();
    let pick =
        |q: usize| lat.get((lat.len() * q / 100).min(lat.len().saturating_sub(1))).map_or(0, |d| d.as_micros() as u64);
    (r.p50_us, r.p99_us, r.elapsed_ms) = (pick(50), pick(99), (secs * 1000.0) as u64);
    r.gb_per_s = if secs > 0.0 { 2.0 * bulk as f64 * r.ok as f64 / secs / 1e9 } else { 0.0 };
    Ok(r)
}

fn random_buf(pool: &MemPool, n: usize) -> Result<FrozenBuf, String> {
    let mut b = pool.get(n, Lane::Normal, Duration::from_secs(1)).map_err(|e| format!("取缓冲失败：{e}"))?;
    rand::fill(b.as_mut_slice());
    Ok(b.freeze())
}

/// 第 i 次的 Ping：payload = i；带 bulk 时附上整段数据，并给回复的 bulk 预先分配一个缓冲（recv_into）。
fn frame(pool: &MemPool, i: u32, data: Option<&FrozenBuf>) -> Result<OutFrame, ErrCode> {
    let body = encode_body(&PingBody { payload: i.to_le_bytes().to_vec() });
    let hdr = FrameHeader::new(FrameKind::Request, MSG_PING, 0, 0);
    let recv_into = data.map(|d| pool.get(d.as_slice().len(), Lane::Normal, Duration::from_secs(1))).transpose()?;
    Ok(OutFrame { hdr, body, bulk: data.map(|d| vec![d.whole()]).unwrap_or_default(), recv_into })
}

/// 回显逐字节一致（payload 与 bulk）。bulk 的 CRC 由 core 收帧时按帧头校验过（不对就断开重连），这里只比内容。
fn echoed(f: &InFrame, i: u32, data: Option<&FrozenBuf>) -> bool {
    let body_ok = f.body == encode_body(&PingBody { payload: i.to_le_bytes().to_vec() });
    let bulk_ok = match (data, &f.bulk) {
        (None, None) => true,
        (Some(d), Some(b)) => b.as_slice() == d.as_slice(),
        _ => false,
    };
    body_ok && bulk_ok
}

fn report(a: &PingArgs, r: Report) -> i32 {
    let code = if r.failed == 0 && r.ok == r.count { 0 } else { 1 };
    let text = if a.json {
        serde_json::to_string(&r).unwrap_or_default()
    } else if let Some(e) = &r.error {
        let why = match e.as_str() {
            "AUTH_FAILED" => "认证被拒：密钥不对、集群不认识这把 key，或声明的角色与 key 不符",
            "ECONNRESET" => "连接断开：服务端没在运行，或地址不对",
            "ETIMEDOUT" => "超时",
            "FEATURE_UNSUPPORTED" => "对端不支持：例如节点没有这种传输，或帧超过了传输的上限",
            _ => "失败",
        };
        format!("ping {}：{e}（{why}）——{}/{} ok", r.target, r.ok, r.count)
    } else if r.bulk_bytes > 0 {
        let size = units::fmt_size(r.bulk_bytes);
        format!(
            "ping {}：bulk crc ok {}/{}（每次 {size}），p50 {}，p99 {}，吞吐 {:.2} GB/s（收发合计；重试 {} 次）",
            r.target,
            r.ok,
            r.count,
            us(r.p50_us),
            us(r.p99_us),
            r.gb_per_s,
            r.retries
        )
    } else {
        format!(
            "ping {}：{}/{} ok，p50 {}，p99 {}（重试 {} 次）",
            r.target,
            r.ok,
            r.count,
            us(r.p50_us),
            us(r.p99_us),
            r.retries
        )
    };
    match write_stdout(&format!("{text}\n")) {
        Ok(()) => code,
        Err(c) => c,
    }
}

fn us(v: u64) -> String {
    if v >= 1000 { format!("{:.2} ms", v as f64 / 1000.0) } else { format!("{v} µs") }
}

#[cfg(test)]
mod tests {
    use wmfs_core::proto::frame::FrameHeader;

    use super::*;

    /// 一个 Ping 的回复：payload = i，bulk = `bulk`（拷进池缓冲）。
    fn reply(pool: &MemPool, i: u32, bulk: Option<&[u8]>) -> InFrame {
        let body = encode_body(&PingBody { payload: i.to_le_bytes().to_vec() });
        let bulk = bulk.map(|b| {
            let mut buf = pool.get(b.len(), Lane::Normal, Duration::ZERO).unwrap();
            buf.as_mut_slice().copy_from_slice(b);
            buf
        });
        let hdr = FrameHeader::new(FrameKind::Response, MSG_PING, 1, 0);
        InFrame { hdr, body, bulk, bulk_dropped: false }
    }

    #[test]
    fn echo_must_match_byte_for_byte() {
        let pool = MemPool::new(&PoolConfig { s_mb: 1, m_slots: 1, l_slots: 1, repl_lane: false }).unwrap();
        let data = random_buf(&pool, 1000).unwrap();
        let mut changed = data.as_slice().to_vec();
        changed[500] ^= 1;
        assert!(echoed(&reply(&pool, 7, Some(data.as_slice())), 7, Some(&data)));
        assert!(echoed(&reply(&pool, 7, None), 7, None));
        assert!(!echoed(&reply(&pool, 8, Some(data.as_slice())), 7, Some(&data)), "payload 不一样");
        assert!(!echoed(&reply(&pool, 7, Some(&changed)), 7, Some(&data)), "bulk 改了一个比特");
        assert!(!echoed(&reply(&pool, 7, Some(&data.as_slice()[..999])), 7, Some(&data)), "bulk 短了");
        assert!(!echoed(&reply(&pool, 7, None), 7, Some(&data)), "少了 bulk");
        assert!(!echoed(&reply(&pool, 7, Some(data.as_slice())), 7, None), "多了 bulk");
    }
}
