//! `wmfs dev …`：本机开发集群（design.md 7.2，roadmap §0 第 3 条），基础版（M1）：up/down/env/status/kill/start/logs。
//!
//! 开发目录默认 `~/wmfs-dev`（`--dir` 或 `WMFS_DEV_DIR` 可改），第 n 个节点的数据在 `<dir>/n<n>`，端口 rpc `19n00`、
//! http `19n80`（RDMA 引导 `19n01` 留给 M2）；node.toml 打开 `failpoints_api`，内存池用 `PoolConfig::dev_server()`。
//! `up` 起节点后等它就绪才返回——`/api/v1/health` 有响应，并且 pid 文件里就是刚起的这个进程（超时打印日志路径与最后一行
//! 并以非 0 退出），所以演示与验收不需要 sleep。开发目录一律换成规范的绝对路径（同一个目录换一种写法也认得出来）。
//! 节点在自己的进程组里后台运行（终端里按 Ctrl-C 不会带走它），stdout/stderr 追加到 `<节点>/logs/server.log`。
//! 安全：只对 pid 文件里、且命令行确实是"wmfs server --data-dir <这个节点>"的进程发信号；`down --purge` 只删除
//! 默认的 `~/wmfs-dev` 或带 `.wmfs-dev` 标记的目录。实现注记（M1）：多节点要 `wmfs join`（M8），所以 M1 的 `up` 只支持 `--nodes 1`。

use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use serde::Serialize;
use wmfs_core::api::types::{Health, KeyFile};
use wmfs_core::base::config::NodeConfig;
use wmfs_core::base::{clock, sys};
use wmfs_core::buf::PoolConfig;
use wmfs_core::init::{InitOpts, NodeFile, init_node};
use wmfs_mgr::api::ApiClient;

use crate::{fail, home, out};

#[derive(clap::Subcommand)]
pub enum DevCmd {
    /// 起开发集群并等它就绪（M1 只支持 1 个节点）
    Up {
        #[arg(long, default_value_t = 1)]
        nodes: u32,
    },
    /// 停掉全部开发节点（SIGTERM，5 s 后 SIGKILL）；--purge 再删除开发目录
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
        DevCmd::Up { nodes } => up(&d, nodes),
        DevCmd::Down { purge } => down(&d, purge),
        DevCmd::Env => env(&d),
        DevCmd::Status { json } => status(&d, json),
        DevCmd::Kill { n } => kill(&d, n),
        DevCmd::Start { n } => start(&d, n).map(|line| vec![line]),
        DevCmd::Logs { n } => std::fs::read_to_string(log_path(&d, n))
            .map(|t| vec![t])
            .map_err(|e| format!("读 {} 失败：{e}", log_path(&d, n).display())),
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
    let pid: i32 = std::fs::read_to_string(nd.join("run/server.pid")).ok()?.trim().parse().ok()?;
    let cmdline = std::fs::read(format!("/proc/{pid}/cmdline")).ok()?;
    let args: Vec<&[u8]> = cmdline.split(|b| *b == 0).collect();
    let ours = args.contains(&&b"server"[..]) && args.contains(&nd.as_os_str().as_encoded_bytes());
    (ours && sys::process_alive(pid)).then_some(pid)
}

/// 这个节点的 health；端口上回答的是别的集群（例如另一个开发目录的节点占着这个端口）也算失败。
fn health(d: &Path, n: u32) -> Result<Health, String> {
    let nd = node_dir(d, n);
    let key = KeyFile::parse(&std::fs::read_to_string(nd.join("keys/admin.key")).map_err(|e| e.to_string())?)?;
    let h: Health = ApiClient::new(vec![mgmt_addr(d, n)?], key).get("/api/v1/health").map_err(|e| e.to_string())?;
    let mine = NodeFile::read(&nd)?.cluster_id;
    if h.cluster_id != mine {
        return Err(format!("{} 上是别的集群（{}），不是 {}", h.node.mgmt_addr, h.cluster_id, nd.display()));
    }
    Ok(h)
}

fn up(d: &Path, nodes: u32) -> Result<Vec<String>, String> {
    if nodes != 1 {
        return Err("M1 的 wmfs dev up 只支持 --nodes 1（多节点要 wmfs join，M8 起）".into());
    }
    std::fs::create_dir_all(d).map_err(|e| format!("建目录 {} 失败：{e}", d.display()))?;
    sys::atomic_write(
        &d.join(MARKER),
        "wmfs dev 的开发目录（wmfs dev down --purge 只删除带这个标记的目录）\n".as_bytes(),
    )
    .map_err(|e| e.to_string())?;
    let mut lines = Vec::new();
    for n in 1..=nodes {
        let nd = node_dir(d, n);
        if !nd.join("NODE").exists() {
            let (rpc_port, mgmt_port) = ports(n);
            let host_id = Some(format!("host-{n}"));
            let o = InitOpts {
                data_dir: nd,
                addr: "127.0.0.1".into(),
                rpc_port,
                mgmt_port,
                mem: PoolConfig::dev_server(),
                failpoints_api: true,
                host_id,
            };
            init_node(&o)?;
        }
        lines.push(start(d, n)?);
    }
    Ok(lines)
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
    let log = std::fs::OpenOptions::new().create(true).append(true).open(log_path(d, n)).map_err(|e| e.to_string())?;
    let exe = std::env::current_exe().map_err(|e| format!("找不到 wmfs 可执行文件：{e}"))?;
    let mut child = Command::new(exe)
        .arg("server")
        .arg("--data-dir")
        .arg(&nd)
        .stdin(Stdio::null())
        .stdout(log.try_clone().map_err(|e| e.to_string())?)
        .stderr(log)
        .process_group(0) // 自己的进程组：终端里的 Ctrl-C 不会带走它
        .spawn()
        .map_err(|e| format!("启动 node{n} 失败：{e}"))?;
    let deadline = clock::mono_ms() + READY_TIMEOUT_MS;
    loop {
        if let Ok(h) = health(d, n)
            && running_pid(d, n) == Some(child.id() as i32)
        {
            return Ok(format!("node{n} started（{}）", describe(&h)));
        }
        // 用 try_wait 而不是 kill(pid, 0)：退出了但还没被回收的子进程（僵尸）在 kill(pid, 0) 看来仍然"活着"
        let exited = child.try_wait().ok().flatten().is_some();
        if exited || clock::mono_ms() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            let why = if exited {
                "启动后退出了".to_string()
            } else {
                format!("没能在 {} s 内就绪（已停止它）", READY_TIMEOUT_MS / 1000)
            };
            let log = log_path(d, n);
            let last = std::fs::read_to_string(&log).ok().and_then(|t| t.lines().last().map(str::to_string));
            return Err(format!("node{n} {why}，请看日志 {}（最后一行：{}）", log.display(), last.unwrap_or_default()));
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn describe(h: &Health) -> String {
    let rpc: Vec<&str> = h.node.addrs.iter().map(|(_, a)| a.as_str()).collect();
    format!("rpc {}，http {}", rpc.join(","), h.node.mgmt_addr)
}

/// 给第 n 个节点发 `sig`，等它退出（最多 `wait_ms`）；返回它是否已经不在了。
fn signal_and_wait(d: &Path, n: u32, sig: i32, wait_ms: u64) -> bool {
    let Some(pid) = running_pid(d, n) else { return true };
    let _ = sys::send_signal(pid, sig);
    let deadline = clock::mono_ms() + wait_ms;
    while clock::mono_ms() < deadline {
        if !sys::process_alive(pid) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    !sys::process_alive(pid)
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
    let mut lines = Vec::new();
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

fn env(d: &Path) -> Result<Vec<String>, String> {
    let ns = nodes(d);
    let first = ns.first().ok_or_else(|| format!("{} 里还没有开发集群：先 wmfs dev up", d.display()))?;
    let mgmt: Result<Vec<String>, String> = ns.iter().map(|n| mgmt_addr(d, *n)).collect();
    let q = |p: &Path| format!("'{}'", p.display().to_string().replace('\'', r"'\''"));
    Ok(vec![
        format!("export WMFS_CLUSTER={}", mgmt?.join(",")),
        format!("export WMFS_KEY_FILE={}", q(&node_dir(d, *first).join("keys/admin.key"))),
        format!("export WMFS_MGR_DIR={}", q(&d.join("mgr"))),
        format!("export WMFS_DEV_DIR={}", q(d)),
    ])
}

#[derive(Serialize)]
struct NodeStatus {
    node: u32,
    running: bool,
    pid: Option<i32>,
    healthy: bool,
    rpc: Vec<(String, String)>,
    mgmt: String,
    log: String,
}

fn status(d: &Path, json: bool) -> Result<Vec<String>, String> {
    let mut v = Vec::new();
    for n in nodes(d) {
        let (pid, h) = (running_pid(d, n), health(d, n).ok());
        let mgmt = mgmt_addr(d, n).unwrap_or_default();
        let rpc = h.as_ref().map(|h| h.node.addrs.clone()).unwrap_or_default();
        let log = log_path(d, n).display().to_string();
        v.push(NodeStatus { node: n, running: pid.is_some(), pid, healthy: h.is_some(), rpc, mgmt, log });
    }
    if json {
        let j = serde_json::json!({"dir": d.display().to_string(), "nodes": v});
        return Ok(vec![j.to_string()]);
    }
    if v.is_empty() {
        return Ok(vec![format!("{} 里没有开发节点（wmfs dev up 起一个）", d.display())]);
    }
    let state = |s: &NodeStatus| match (s.running, s.healthy) {
        (true, true) => "运行中".to_string(),
        (true, false) => "运行中（health 没有响应）".to_string(),
        _ => "已停止".to_string(),
    };
    Ok(v.iter().map(|s| format!("node{}：{}，http {}，日志 {}", s.node, state(s), s.mgmt, s.log)).collect())
}

#[cfg(test)]
mod tests {
    #[test]
    fn dev_ports_follow_the_19n00_scheme() {
        assert_eq!(super::ports(1), (19100, 19180));
        assert_eq!(super::ports(3), (19300, 19380));
    }
}
