//! 按本端 IPv4 地址选 RDMA 设备、端口与 GID（design.md 4.1 实现注记（M2）③）：只读 sysfs（`/sys/class/infiniband`），
//! 不打开设备，所以没有设备的机器上也能测（单元测试在临时目录里造一棵 sysfs 树）。
//!
//! RoCE v2 要用 IPv4 映射的 GID（`::ffff:a.b.c.d`，类型 "RoCE v2"）。一次性命令（`wmfs ping rdma://…`）没有 node.toml，
//! 所以 `[plugin.rdma] device`、`gid_index` 都可以省略：取引导 TCP 连接本端的 IPv4，找"RoCE v2、IPv4 映射、地址等于它、
//! 端口 ACTIVE"的那一项；写了 device / gid_index 就只在符合的项里找。找不到时的错误里列出全部候选，并分清两种情况：
//! 本机根本没有这个地址的 GID（配置错了），还是 GID 在、只是端口链路不是 ACTIVE（可能只是暂时断链）。

use std::net::Ipv4Addr;
use std::path::Path;

/// 默认的 sysfs 根目录。
pub const SYSFS: &str = "/sys/class/infiniband";

/// 一个 RoCE v2 的 IPv4 映射 GID：在哪块设备、哪个端口、表里第几项。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Route {
    pub device: String,
    pub port: u8,
    pub gid_index: u8,
    pub gid: [u8; 16],
    pub ip: Ipv4Addr,
    pub active: bool, // 端口链路 ACTIVE
}

/// 选不出 GID 的原因。
#[derive(Debug)]
pub struct NoRoute {
    pub why: String,     // 给人看的中文（含全部候选）
    pub link_down: bool, // 有对应的 GID，只是端口链路不是 ACTIVE（可能只是暂时断链；本机根本没有对应的 GID 时是 false）
}

/// "0000:0000:0000:0000:0000:ffff:0a4d:0001" → 16 字节。
pub fn parse_gid(s: &str) -> Option<[u8; 16]> {
    let groups: Vec<&str> = s.trim().split(':').collect();
    if groups.len() != 8 {
        return None;
    }
    let mut g = [0u8; 16];
    for (i, x) in groups.iter().enumerate() {
        let v = u16::from_str_radix(x, 16).ok()?;
        g[2 * i..2 * i + 2].copy_from_slice(&v.to_be_bytes());
    }
    Some(g)
}

/// IPv4 映射的 GID（前 10 字节 0、接着 ff ff）→ IPv4。
pub fn gid_ipv4(g: &[u8; 16]) -> Option<Ipv4Addr> {
    (g[..10].iter().all(|b| *b == 0) && g[10] == 0xff && g[11] == 0xff)
        .then(|| Ipv4Addr::new(g[12], g[13], g[14], g[15]))
}

/// 目录里的条目名（排好序）；读不了 = 空。
fn names(p: &Path) -> Vec<String> {
    let mut v: Vec<String> = std::fs::read_dir(p)
        .map(|it| it.flatten().map(|e| e.file_name().to_string_lossy().into_owned()).collect())
        .unwrap_or_default();
    v.sort();
    v
}

/// 列出 `root`（平时是 [`SYSFS`]）下全部 RoCE v2 的 IPv4 映射 GID。空表项读 `gid_attrs/types/<i>` 会报 EINVAL，按未使用处理。
pub fn candidates(root: &Path) -> Vec<Route> {
    let mut out = Vec::new();
    for dev in names(root) {
        let ports = root.join(&dev).join("ports");
        for port in names(&ports) {
            let Ok(pn) = port.parse::<u8>() else { continue };
            let p = ports.join(&port);
            let active = std::fs::read_to_string(p.join("state")).is_ok_and(|s| s.contains("ACTIVE"));
            let mut idx: Vec<u8> = names(&p.join("gids")).iter().filter_map(|n| n.parse().ok()).collect();
            idx.sort();
            for i in idx {
                let Some(gid) = std::fs::read_to_string(p.join(format!("gids/{i}"))).ok().and_then(|s| parse_gid(&s))
                else {
                    continue;
                };
                let Some(ip) = gid_ipv4(&gid) else { continue };
                let ty = std::fs::read_to_string(p.join(format!("gid_attrs/types/{i}"))).unwrap_or_default();
                if ty.trim() == "RoCE v2" {
                    out.push(Route { device: dev.clone(), port: pn, gid_index: i, gid, ip, active });
                }
            }
        }
    }
    out
}

/// 给本端地址 `ip` 选一项（可选地限定设备与 gid_index）。找不到、或找到的端口都不是 ACTIVE → [`NoRoute`]。
pub fn pick(root: &Path, ip: Ipv4Addr, device: Option<&str>, gid_index: Option<u8>) -> Result<Route, NoRoute> {
    let all = candidates(root);
    let want =
        |r: &&Route| r.ip == ip && device.is_none_or(|d| r.device == d) && gid_index.is_none_or(|g| r.gid_index == g);
    let matched: Vec<&Route> = all.iter().filter(want).collect();
    if let Some(r) = matched.iter().find(|r| r.active) {
        return Ok((*r).clone());
    }
    let mut filter = String::new();
    if let Some(d) = device {
        filter += &format!("、设备 {d}");
    }
    if let Some(g) = gid_index {
        filter += &format!("、gid_index {g}");
    }
    let list: Vec<String> = all
        .iter()
        .map(|r| {
            let state = if r.active { "ACTIVE" } else { "DOWN" };
            format!("{}/{} gid_index {}（{}，{state}）", r.device, r.port, r.gid_index, r.ip)
        })
        .collect();
    let list = if list.is_empty() { "本机没有任何 RoCE v2 的 IPv4 GID".to_string() } else { list.join("；") };
    let link_down = !matched.is_empty();
    let why = if link_down { "对应的端口链路不是 ACTIVE" } else { "没有对应的 RoCE v2 GID" };
    Err(NoRoute { why: format!("rdma：本端地址 {ip}{filter} {why}（候选：{list}）"), link_down })
}
