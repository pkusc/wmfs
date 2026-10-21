//! `wmfs doctor` 的 RDMA 一项（design.md 4.1 实现注记（M0、M2））：只用 std::fs 读 sysfs（`/sys/class/infiniband`），
//! 不打开设备、不调用 libibverbs。
//!
//! - 识别 rxe（Soft-RoCE）设备：名字以 rxe 开头，或者有 `parent` 文件却没有 `device` 链接（真网卡有 `device` 链接）。
//! - 列出 RoCE v2 的 IPv4 映射 GID 候选：每个端口读 `gids/<i>` 与 `gid_attrs/types/<i>`（空表项读 types 会报 EINVAL，
//!   按未使用处理），写成 `gid_index = i（IPv4 a.b.c.d，<设备>/<端口>）`；JSON 里另有机读的 `data.gids`
//!   （`scripts/accept.sh M2` 用它找本机可用的 RDMA 地址）。
//! - 规则与 rdma 插件建连时选 GID 的规则相同（plugins/rdma/gid.rs，testkit 直接用插件的 `candidates`）。这里**故意保留一份
//!   只用 std::fs 的副本**：体检必须在不带 rdma 插件的构建里也能用（`cargo build --no-default-features --features
//!   tcp,shared-key`，例如没有 cmake/libclang 的机器），而且它还要数所有端口（包括没有 IPv4 GID 的）、认出 rxe——插件不需要
//!   这些。改了选 GID 的规则（例如加 IPv6）要两处一起改。
//! - 有 ACTIVE 端口 → ✓；否则 !，标题与 roadmap M2 的预期一致：「RDMA：4 个 RoCE 端口链路 DOWN，无可用设备（本机不做
//!   RDMA 实机测试）」。不提示任何 sudo 操作。

use std::net::Ipv4Addr;
use std::path::Path;

use super::parse::{list_dir, name_of};
use super::{Check, ok, read, warn};

/// sysfs 里 RDMA 设备的目录。
pub const SYSFS: &str = "/sys/class/infiniband";

const NO_TEST: &str = "无可用设备（本机不做 RDMA 实机测试）";
const SKIP: &str = "需要 RDMA 设备的检查会显示 SKIP";

/// 一个 RoCE v2 的 IPv4 映射 GID。
struct Gid {
    device: String,
    port: String,
    index: u32,
    ip: Ipv4Addr,
    active: bool,
}

/// "0000:0000:0000:0000:0000:ffff:0a4d:0001" → 10.77.0.1（不是 IPv4 映射的 GID → None）。
fn mapped_ipv4(s: &str) -> Option<Ipv4Addr> {
    let g: Vec<u16> = s.trim().split(':').map(|x| u16::from_str_radix(x, 16)).collect::<Result<_, _>>().ok()?;
    if g.len() != 8 || g[..5].iter().any(|x| *x != 0) || g[5] != 0xffff {
        return None;
    }
    let [a, b] = g[6].to_be_bytes();
    let [c, d] = g[7].to_be_bytes();
    Some(Ipv4Addr::new(a, b, c, d))
}

/// 一个端口的 RoCE v2 IPv4 GID：(下标, 地址)。
fn port_gids(port: &Path) -> Vec<(u32, Ipv4Addr)> {
    let mut idx: Vec<u32> = list_dir(&port.join("gids")).iter().filter_map(|p| name_of(p).parse().ok()).collect();
    idx.sort();
    idx.into_iter()
        .filter_map(|i| {
            let ip = mapped_ipv4(&read(port.join(format!("gids/{i}"))))?;
            (read(port.join(format!("gid_attrs/types/{i}"))).trim() == "RoCE v2").then_some((i, ip))
        })
        .collect()
}

/// rxe（Soft-RoCE）：名字以 rxe 开头，或者有 parent 文件却没有 device 链接。
fn is_rxe(dev: &Path) -> bool {
    name_of(dev).starts_with("rxe")
        || (dev.join("parent").exists() && std::fs::symlink_metadata(dev.join("device")).is_err())
}

/// 在 `root`（平时是 [`SYSFS`]）下做 RDMA 一项的体检。
pub fn rdma_at(root: &Path) -> Check {
    let (mut devices, mut rxe, mut gids) = (Vec::new(), Vec::new(), Vec::new());
    let (mut ports, mut active, mut roce) = (0, 0, 0);
    for dev in list_dir(root) {
        let name = name_of(&dev);
        if is_rxe(&dev) {
            rxe.push(name.clone());
        }
        for port in list_dir(&dev.join("ports")) {
            ports += 1;
            let up = read(port.join("state")).contains("ACTIVE");
            if up {
                active += 1;
            }
            if read(port.join("link_layer")).trim() == "Ethernet" {
                roce += 1;
            }
            for (index, ip) in port_gids(&port) {
                gids.push(Gid { device: name.clone(), port: name_of(&port), index, ip, active: up });
            }
        }
        devices.push(name);
    }
    let list: Vec<String> =
        gids.iter().map(|g| format!("gid_index = {}（IPv4 {}，{}/{}）", g.index, g.ip, g.device, g.port)).collect();
    let cands = if list.is_empty() {
        "没有 RoCE v2 的 IPv4 GID（网口配上 IPv4 地址后才会出现）".to_string()
    } else {
        format!("RoCE v2 的 IPv4 GID 候选：{}", list.join("；"))
    };
    let data = serde_json::json!({
        "rxe": rxe,
        "gids": gids.iter().map(|g| serde_json::json!({
            "device": g.device, "port": g.port, "gid_index": g.index, "ipv4": g.ip.to_string(), "active": g.active,
        })).collect::<Vec<_>>(),
    });
    let mut c = if active > 0 {
        let soft =
            if rxe.is_empty() { String::new() } else { format!("（含 Soft-RoCE 设备 {}）", rxe.join("、")) };
        ok("rdma", format!("RDMA：{active} 个端口链路 ACTIVE{soft}"), format!("{}；{cands}", devices.join("、")))
    } else if ports == 0 {
        warn("rdma", format!("RDMA：{NO_TEST}"), SKIP)
    } else {
        let kind = if roce == ports { "RoCE" } else { "RDMA" };
        warn(
            "rdma",
            format!("RDMA：{ports} 个 {kind} 端口链路 DOWN，{NO_TEST}"),
            format!("{SKIP}（{}）；{cands}", devices.join("、")),
        )
    };
    c.data = Some(data);
    c
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::doctor::Status;

    /// 在临时目录里造一棵 sysfs 树：`files` 是 (相对路径, 内容)。
    fn tree(name: &str, files: &[(&str, &str)]) -> std::path::PathBuf {
        let root = std::env::temp_dir().join(format!("wmfs-doctor-rdma-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        for (p, s) in files {
            let f = root.join(p);
            std::fs::create_dir_all(f.parent().unwrap()).unwrap();
            std::fs::write(f, s).unwrap();
        }
        root
    }

    #[test]
    fn soft_roce_with_an_ipv4_gid_is_ok() {
        let root = tree(
            "rxe",
            &[
                ("rxe0/parent", "enp1s0\n"),
                ("rxe0/ports/1/state", "4: ACTIVE\n"),
                ("rxe0/ports/1/link_layer", "Ethernet\n"),
                ("rxe0/ports/1/gids/0", "fe80:0000:0000:0000:0000:0000:0000:0001\n"),
                ("rxe0/ports/1/gid_attrs/types/0", "IB/RoCE v1\n"),
                ("rxe0/ports/1/gids/1", "0000:0000:0000:0000:0000:ffff:0a4d:0001\n"),
                ("rxe0/ports/1/gid_attrs/types/1", "RoCE v2\n"),
                ("rxe0/ports/1/gids/2", "0000:0000:0000:0000:0000:0000:0000:0000\n"), // 空表项：没有 types 文件
            ],
        );
        let c = rdma_at(&root);
        std::fs::remove_dir_all(&root).unwrap();
        assert_eq!(c.status, Status::Ok);
        assert!(c.title.contains("rxe0"), "{c:?}");
        assert!(c.detail.contains("gid_index = 1（IPv4 10.77.0.1，rxe0/1）"), "{c:?}");
        let g = &c.data.as_ref().unwrap()["gids"];
        assert_eq!(g.as_array().unwrap().len(), 1);
        assert_eq!((g[0]["ipv4"].as_str(), g[0]["active"].as_bool()), (Some("10.77.0.1"), Some(true)));
    }

    #[test]
    fn four_mlx5_ports_down_is_a_warning_without_sudo() {
        let mut files = Vec::new();
        let names = ["rocep1s0f0", "rocep1s0f1", "roceP2p1s0f0", "roceP2p1s0f1"];
        let paths: Vec<(String, String)> = names
            .iter()
            .flat_map(|n| {
                [
                    (format!("{n}/ports/1/state"), "1: DOWN\n".to_string()),
                    (format!("{n}/ports/1/link_layer"), "Ethernet\n".to_string()),
                    (format!("{n}/device/vendor"), "0x15b3\n".to_string()), // 真网卡：有 device
                ]
            })
            .collect();
        for (p, s) in &paths {
            files.push((p.as_str(), s.as_str()));
        }
        let root = tree("mlx5", &files);
        let c = rdma_at(&root);
        std::fs::remove_dir_all(&root).unwrap();
        assert_eq!(c.status, Status::Warn);
        assert_eq!(c.title, "RDMA：4 个 RoCE 端口链路 DOWN，无可用设备（本机不做 RDMA 实机测试）");
        assert!(!c.title.contains("sudo") && !c.detail.contains("sudo"));
        assert!(c.data.as_ref().unwrap()["rxe"].as_array().unwrap().is_empty(), "有 device 的不是 rxe");
        let none = rdma_at(Path::new("/no/such/sysfs"));
        assert!(none.status == Status::Warn && none.title.contains("本机不做 RDMA 实机测试"));
    }
}
