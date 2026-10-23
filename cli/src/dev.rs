//! `wmfs dev …`：本机开发集群（design.md 7.2，roadmap §0 第 3 条），基础版（M1）：up/down/env/status/kill/start/logs。
//!
//! 开发目录默认 `~/wmfs-dev`（`--dir` 或 `WMFS_DEV_DIR` 可改），第 n 个节点的数据在 `<dir>/n<n>`，端口 rpc `19n00`、
//! http `19n80`、RDMA 引导 `19n01`（只在 `up --rdma IP` 时监听）；node.toml 打开 `failpoints_api`，内存池用 `PoolConfig::dev_server()`。
//! `up` 起节点后等它就绪才返回——`/api/v1/health` 有响应，并且 pid 文件里就是刚起的这个进程（超时打印日志路径与最后一行
//! 并以非 0 退出），所以演示与验收不需要 sleep。开发目录一律换成规范的绝对路径（同一个目录换一种写法也认得出来）。
//! 节点在自己的进程组里后台运行（终端里按 Ctrl-C 不会带走它），stdout/stderr 追加到 `<节点>/logs/server.log`。
//! 安全：只对 pid 文件里、且命令行确实是"wmfs server --data-dir <这个节点>"的进程发信号；`down --purge` 只删除
//! 默认的 `~/wmfs-dev` 或带 `.wmfs-dev` 标记的目录。实现注记（M1）：多节点要 `wmfs join`（M8），所以 M1 的 `up` 只支持 `--nodes 1`。
//! M3：开发节点打开 `debug_ops`（`wmfs debug kv`）；`wmfs dev wait <条件>`（[`wait`]，M3 只有 leader）。
//! M4：每个节点默认一个 target（`<节点目录>/targets/1`，容量上限 20 GiB，design.md 12 节第 13 条）；`up` 起好节点后取一次
//! 集群图，把集群警告打印出来（ADR-0016：例如"default 类声明 3 副本，只有 1 个故障域……"）。取不到集群图（节点没有成为
//! 就绪的 leader，多半是旧里程碑建的开发集群、插件清单不一致）→ 打印原因与修法（`wmfs dev down --purge`）并以非 0 退出。
//! `up --rdma` 改写 node.toml 的部分在 `dev/rdma.rs`。
//! M6：`dev mount/umount/client-pid` 与 `down` 先清理挂载（`dev/mount.rs`）；`up --profile minimal`（design.md 9.7、3.4.3 的部署
//! 模板）：初始化新节点时按 `wmfs init --profile minimal` 建（default 类 1 副本、不写 replicas 属性、node.toml 停用 quota 与
//! mclock）；已有的开发集群是按别的 profile 建的 → 报错并提示 `wmfs dev down --purge`（profile 写在初始 KV 里，不能事后改写）。
//! `env`/`status` 拆到 `dev/info.rs`；后台起进程、等就绪、认 pid、等退出的几步与 `dev mount` 共用（`dev/proc.rs`）。

use std::ffi::OsStr;
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use wmfs_core::api::types::{Health, KeyFile};
use wmfs_core::base::config::{NodeConfig, TargetConfig};
use wmfs_core::base::sys;
use wmfs_core::buf::PoolConfig;
use wmfs_core::client::meta_client::fetch_map;
use wmfs_core::init::{InitOpts, NodeFile, Profile, init_node};
use wmfs_mgr::api::ApiClient;

use crate::{fail, home, out};

mod info;
mod mount;
mod proc;
mod rdma;
mod wait;

use proc::{live_pid, spawn_logged, wait_exit, wait_ready};
use rdma::set_rdma;

#[derive(clap::Subcommand)]
pub enum DevCmd {
    /// 起开发集群并等它就绪（M1 只支持 1 个节点）
    Up {
        #[arg(long, default_value_t = 1)]
        nodes: u32,
        /// 节点同时在这个 IPv4 上监听 RDMA（引导端口 19n01），数据族走 rdma（要有 RoCE v2 设备，见 wmfs doctor）。
        /// 每次 up 都按这次的参数改写 node.toml：不带 --rdma 就改回只用 tcp；节点已在运行时要先 wmfs dev down 再 up 才生效
        #[arg(long, value_name = "IP")]
        rdma: Option<Ipv4Addr>,
        /// 按某个 profile 的部署模板建新节点（design.md 9.7、3.4.3）：default 或 minimal（1 节点、TCP、default 类 1 副本、停用 quota 与
        /// mclock）；nas-safe、hpc-rdma 在 M9 起。已有的开发集群是按别的 profile 建的 → 报错（先 wmfs dev down --purge）
        #[arg(long, default_value = "default")]
        profile: String,
    },
    /// 先卸载全部开发挂载点，再停掉全部开发节点（SIGTERM，5 s 后 SIGKILL）；--purge 再删除开发目录
    Down {
        #[arg(long)]
        purge: bool,
    },
    /// 打印 export 语句（用法：eval "$(wmfs dev env)"）
    Env,
    /// 各节点是否在运行、是否健康
    Status {
        #[arg(long)]
        json: bool,
    },
    /// kill -9 第 n 个节点
    Kill { n: u32 },
    /// 启动第 n 个节点（已初始化过的）并等它就绪
    Start { n: u32 },
    /// 打印第 n 个节点的日志
    Logs { n: u32 },
    /// 后台挂载一个客户端（wmfs mount，用 node1 的 client key），等它挂上；会打印集群警告
    Mount {
        mountpoint: PathBuf,
        /// client key（默认 <开发目录>/n1/keys/client.key）
        #[arg(long)]
        key_file: Option<PathBuf>,
        /// 块缓存：memory[:SIZE] 或 none（传给 wmfs mount --cache）
        #[arg(long)]
        cache: Option<String>,
    },
    /// 卸载 wmfs dev mount 挂上的挂载点，等挂载进程写完数据退出
    Umount { mountpoint: PathBuf },
    /// 打印某个开发挂载点的挂载进程号
    ClientPid { mountpoint: PathBuf },
    /// 等某个条件成立（满足立即返回 0；超时返回 1 并打印当前状态）。M3 只有 leader（有就绪的元数据 leader）
    Wait {
        /// 条件（及其参数），例如 leader
        #[arg(required = true, num_args = 1..)]
        what: Vec<String>,
        /// 最多等多少秒
        #[arg(long, default_value_t = 120)]
        timeout: u64,
    },
}

const MARKER: &str = ".wmfs-dev";
const READY_TIMEOUT_MS: u64 = 30_000;

pub fn run(cmd: DevCmd, dir: Option<PathBuf>) -> i32 {
    let d =
        dir.or_else(|| std::env::var_os("WMFS_DEV_DIR").map(PathBuf::from)).unwrap_or_else(|| home().join("wmfs-dev"));
    // 换成规范的绝对路径（去掉 ./、..、多余的斜杠，解开符号链接）：server 的命令行里记的就是它，之后靠它认出
    // "这是我们起的进程"，所以同一个目录换一种写法也认得出来。目录还不存在时 up 先建出来，才能规范化
    let d = std::path::absolute(&d).unwrap_or(d);
    if matches!(cmd, DevCmd::Up { .. }) {
        let _ = std::fs::create_dir_all(&d); // 建不出来的话 up 里会报出原因
    }
    let d = std::fs::canonicalize(&d).unwrap_or(d);
    let r = match cmd {
        DevCmd::Up { nodes, rdma, profile } => Profile::parse(&profile).and_then(|p| up(&d, nodes, rdma, p)),
        DevCmd::Down { purge } => down(&d, purge),
        DevCmd::Env => info::env(&d),
        DevCmd::Status { json } => info::status(&d, json),
        DevCmd::Kill { n } => kill(&d, n),
        DevCmd::Start { n } => start(&d, n).map(|line| vec![line]),
        DevCmd::Logs { n } => std::fs::read_to_string(log_path(&d, n))
            .map(|t| vec![t])
            .map_err(|e| format!("读 {} 失败：{e}", log_path(&d, n).display())),
        DevCmd::Wait { what, timeout } => wait::wait(&d, &what, timeout),
        DevCmd::Mount { mountpoint, key_file, cache } => mount::mount(&d, &mountpoint, &key_file, &cache),
        DevCmd::Umount { mountpoint } => mount::umount(&d, &mountpoint),
        DevCmd::ClientPid { mountpoint } => mount::client_pid(&d, &mountpoint),
    };
    match r {
        Ok(lines) => out(lines.iter().map(|l| l.trim_end_matches('\n')).collect::<Vec<_>>().join("\n")),
        Err(e) => fail(e),
    }
}

fn node_dir(d: &Path, n: u32) -> PathBuf {
    d.join(format!("n{n}"))
}
fn log_path(d: &Path, n: u32) -> PathBuf {
    node_dir(d, n).join("logs/server.log")
}
/// 第 n 个节点的 (rpc, http) 端口：19n00、19n80。
fn ports(n: u32) -> (u16, u16) {
    let base = 19_000 + 100 * n as u16;
    (base, base + 80)
}

/// 已初始化的节点号（有 NODE 文件的 n<k> 目录），升序。
fn nodes(d: &Path) -> Vec<u32> {
    let mut v: Vec<u32> = std::fs::read_dir(d)
        .map(|it| it.flatten().filter_map(|e| e.file_name().to_str()?.strip_prefix('n')?.parse().ok()).collect())
        .unwrap_or_default();
    v.retain(|n| node_dir(d, *n).join("NODE").exists());
    v.sort();
    v
}

fn mgmt_addr(d: &Path, n: u32) -> Result<String, String> {
    let p = node_dir(d, n).join("node.toml");
    let t = std::fs::read_to_string(&p).map_err(|e| format!("读 {} 失败：{e}", p.display()))?;
    Ok(NodeConfig::parse(&t)?.mgmt.listen)
}

/// 正在运行的 server 进程号：pid 文件里的进程还活着，且命令行确实是这个节点的 `wmfs server`（防止 pid 被别的进程复用）。
fn running_pid(d: &Path, n: u32) -> Option<i32> {
    let nd = node_dir(d, n);
    live_pid(&nd.join("run/server.pid"), "server", &nd)
}

/// 第 n 个节点的 admin key（`<节点目录>/keys/admin.key`）。
fn admin_key(d: &Path, n: u32) -> Result<KeyFile, String> {
    let p = node_dir(d, n).join("keys/admin.key");
    KeyFile::parse(&std::fs::read_to_string(&p).map_err(|e| format!("读 {} 失败：{e}", p.display()))?)
}

/// 这个节点的 health；端口上回答的是别的集群（例如另一个开发目录的节点占着这个端口）也算失败。
fn health(d: &Path, n: u32) -> Result<Health, String> {
    let nd = node_dir(d, n);
    let key = admin_key(d, n)?;
    let h: Health = ApiClient::new(vec![mgmt_addr(d, n)?], key).get("/api/v1/health").map_err(|e| e.to_string())?;
    let mine = NodeFile::read(&nd)?.cluster_id;
    if h.cluster_id != mine {
        return Err(format!("{} 上是别的集群（{}），不是 {}", h.node.mgmt_addr, h.cluster_id, nd.display()));
    }
    Ok(h)
}

fn up(d: &Path, nodes: u32, rdma: Option<Ipv4Addr>, profile: Profile) -> Result<Vec<String>, String> {
    if nodes != 1 {
        return Err("M1 的 wmfs dev up 只支持 --nodes 1（多节点要 wmfs join，M8 起）".into());
    }
    std::fs::create_dir_all(d).map_err(|e| format!("建目录 {} 失败：{e}", d.display()))?;
    let marker = d.join(MARKER);
    sys::atomic_write(&marker, "wmfs dev 的开发目录（wmfs dev down --purge 只删除带这个标记的目录）\n".as_bytes())
        .map_err(|e| format!("写 {} 失败：{e}", marker.display()))?;
    let mut lines = Vec::new();
    for n in 1..=nodes {
        let nd = node_dir(d, n);
        if nd.join("NODE").exists() {
            check_profile(&nd, profile)?;
        } else {
            let (rpc_port, mgmt_port) = ports(n);
            let o = InitOpts {
                data_dir: nd.clone(),
                addr: "127.0.0.1".into(),
                rpc_port,
                mgmt_port,
                mem: PoolConfig::dev_server(),
                failpoints_api: true,
                debug_ops: true, // wmfs debug kv（M3 演示）
                host_id: Some(format!("host-{n}")),
                plugins: crate::registry::init_plugin_list(&profile.disabled_plugins())?,
                targets: vec![TargetConfig {
                    path: nd.join("targets/1").display().to_string(),
                    labels: vec![],
                    capacity: "20G".into(),
                }],
                profile,
            };
            init_node(&o)?;
        }
        if set_rdma(&nd, n, rdma)? && running_pid(d, n).is_some() {
            lines.push(format!(
                "node{n} 已在运行：node.toml 里的 RDMA 设置改了，要先 wmfs dev down、再 wmfs dev up 才生效"
            ));
        }
        lines.push(start(d, n)?);
        // 取不到集群图 = 节点没有成为就绪的 leader（例如旧里程碑建的开发集群插件清单不一致）：大声失败，别让后面的命令白等
        let ws = warnings(d, n).map_err(|e| {
            let log = log_path(d, n).display().to_string();
            format!("{}\nnode{n} 起来了，但没能取到集群图：{e}（日志 {log}）", lines.join("\n"))
        })?;
        lines.extend(ws.iter().map(|w| format!("警告：{w}")));
    }
    Ok(lines)
}

/// 已有的节点是按 `want` 这个 profile 建的吗（看 node.toml 的 `[plugins] disable`：profile 的初始 KV 写进了元数据库，不能事后
/// 改写）？不是 → 报错并给出修法。
fn check_profile(nd: &Path, want: Profile) -> Result<(), String> {
    let p = nd.join("node.toml");
    let cfg = NodeConfig::parse(&std::fs::read_to_string(&p).map_err(|e| format!("读 {} 失败：{e}", p.display()))?)?;
    if cfg.plugins.disable == want.disabled_plugins() {
        return Ok(());
    }
    let was = if cfg.plugins.disable == Profile::Minimal.disabled_plugins() { "minimal" } else { "default" };
    Err(format!(
        "{} 是按 --profile {was} 建的开发集群，这次要的是 --profile {}：先 wmfs dev down --purge，再 wmfs dev up",
        nd.display(),
        want.name()
    ))
}

/// 集群警告（ADR-0016：`wmfs dev up` 的输出里也要看得到）：用这个节点的 admin key 取一次集群图（等 leader 最多 30 s）。
fn warnings(d: &Path, n: u32) -> Result<Vec<String>, String> {
    let l = crate::leader::connect(&[mgmt_addr(d, n)?], admin_key(d, n)?)?;
    let m = fetch_map(l.rpc, &[l.ep], Duration::from_secs(30)).map_err(|e| format!("取集群图失败：{e}"))?;
    Ok(m.warnings.clone())
}

/// 启动第 n 个节点（已在运行就只报告），等它就绪：health 有响应，并且 pid 文件里就是这个进程
/// （只看 health 的话，紧接着的 `dev status/down` 有可能还看不到 pid 文件）。
fn start(d: &Path, n: u32) -> Result<String, String> {
    let nd = node_dir(d, n);
    if !nd.join("NODE").exists() {
        return Err(format!("{} 还没有初始化：先 wmfs dev up", nd.display()));
    }
    if running_pid(d, n).is_some() {
        return Ok(format!("node{n} 已在运行（{}）", describe(&health(d, n).unwrap_or_default())));
    }
    let args = [OsStr::new("server"), OsStr::new("--data-dir"), nd.as_os_str()];
    let mut child = spawn_logged(&args, &log_path(d, n))?;
    let pid = child.id() as i32;
    let ready = || health(d, n).is_ok() && running_pid(d, n) == Some(pid);
    wait_ready(&mut child, READY_TIMEOUT_MS, &log_path(d, n), &format!("node{n}"), ready)?;
    Ok(format!("node{n} started（{}）", describe(&health(d, n).unwrap_or_default())))
}

fn describe(h: &Health) -> String {
    let rpc: Vec<&str> = h.node.addrs.iter().map(|(_, a)| a.as_str()).collect();
    format!("rpc {}，http {}", rpc.join(","), h.node.mgmt_addr)
}

/// 给第 n 个节点发 `sig`，等它退出（最多 `wait_ms`）；返回它是否已经不在了。
fn signal_and_wait(d: &Path, n: u32, sig: i32, wait_ms: u64) -> bool {
    let Some(pid) = running_pid(d, n) else { return true };
    let _ = sys::send_signal(pid, sig);
    wait_exit(pid, wait_ms)
}

fn kill(d: &Path, n: u32) -> Result<Vec<String>, String> {
    if running_pid(d, n).is_none() {
        return Ok(vec![format!("node{n} 没有在运行")]);
    }
    if !signal_and_wait(d, n, sys::SIGKILL, 3000) {
        return Err(format!("node{n} 收到 SIGKILL 后 3 s 仍在"));
    }
    let _ = std::fs::remove_file(node_dir(d, n).join("run/server.pid"));
    Ok(vec![format!("node{n} 已被 kill -9")])
}

fn down(d: &Path, purge: bool) -> Result<Vec<String>, String> {
    if !d.exists() {
        return Ok(vec![format!("没有开发集群（{} 不存在）", d.display())]);
    }
    let mut lines = mount::stop_all(d); // 先挂载、后节点：节点停了以后挂载点上的命令会卡到 --op-timeout（7.2 的清理顺序）
    for n in nodes(d) {
        let Some(pid) = running_pid(d, n) else { continue };
        let _ = sys::send_signal(pid, sys::SIGCONT); // 被冻住（SIGSTOP）的进程收到 SIGTERM 也不会退出：先让它继续运行
        if !signal_and_wait(d, n, sys::SIGTERM, 5000) {
            if !signal_and_wait(d, n, sys::SIGKILL, 3000) {
                return Err(format!("停不掉 node{n}"));
            }
            let _ = std::fs::remove_file(node_dir(d, n).join("run/server.pid")); // 被 SIGKILL 的进程来不及删自己的 pid 文件
        }
        lines.push(format!("node{n} 已停止"));
    }
    if purge {
        // 默认的 ~/wmfs-dev 是 wmfs 自己的地方（验收日志也在里面）；别的目录必须带 wmfs dev 留下的标记，防止删错
        if !d.join(MARKER).exists() && d != home().join("wmfs-dev") {
            return Err(format!("{} 没有 {MARKER} 标记，不像 wmfs dev 建的目录：拒绝删除", d.display()));
        }
        std::fs::remove_dir_all(d).map_err(|e| format!("删除 {} 失败：{e}", d.display()))?;
        lines.push(format!("已删除 {}", d.display()));
    }
    if lines.is_empty() {
        lines.push("没有在运行的开发节点".into());
    }
    Ok(lines)
}

#[cfg(test)]
mod tests;
