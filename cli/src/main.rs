//! wmfs：唯一的二进制（design.md 1.1、7.2，ADR-0006）。
//!
//! M1 的子命令：`doctor`、`init`（只生成目录、密钥与 node.toml）、`server`、`ping`、`failpoint set/clear/list`、
//! `dev up/down/env/status/kill/start/logs`（基础版）、`debug gen-key`。其余子命令随里程碑加入。
//! 插件注册表 `registry.rs` 是全仓库唯一"知道有哪些插件"的文件。

mod dev;
mod doctor;
mod failpoint;
mod node;
mod ping;
mod registry;

use std::io::Write;
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use wmfs_core::api::types::KeyFile;
use wmfs_core::base::sys;

/// `wmfs --version` 的输出（clap 会在前面加 "wmfs "）：软件版本 + 线上协议主版本 + 静态插件 ABI + 动态库 C ABI。
/// 单元测试保证它与 core 里的常量一致。
const VERSION: &str = "0.1.0 (proto 1, core-abi 1, c-abi 1)";

#[derive(Parser)]
#[command(name = "wmfs", version = VERSION, about = "wmfs：给小集群用的分布式文件系统（一个二进制、三台机器）")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// 环境体检：这台机器能测什么（逐项 ✓ 可以 / ! 能用但有限制 / ✗ 缺失）
    Doctor {
        /// 输出 JSON（scripts/accept.sh 只解析这个）
        #[arg(long)]
        json: bool,
    },
    /// 初始化一个新的单节点集群（M1：只生成目录、密钥与 node.toml），然后在前台运行它
    Init(node::InitArgs),
    /// 在前台运行一个已初始化的节点（SIGTERM / Ctrl-C 退出）
    Server {
        /// 节点数据目录（wmfs init 建的）
        #[arg(long)]
        data_dir: PathBuf,
    },
    /// 传输自检：握手认证 + Ping（可带 bulk），打印成功次数、延迟与吞吐
    Ping(ping::PingArgs),
    /// fail point（故障注入点）：经带签名的管理 API 设置、清除、列出
    Failpoint {
        #[command(subcommand)]
        cmd: failpoint::FailpointCmd,
    },
    /// 本机开发集群（数据在 ~/wmfs-dev；第 n 个节点 rpc 19n00、http 19n80）
    Dev {
        #[command(subcommand)]
        cmd: dev::DevCmd,
        /// 开发目录（默认 $WMFS_DEV_DIR，否则 ~/wmfs-dev）
        #[arg(long, global = true)]
        dir: Option<PathBuf>,
    },
    /// 调试工具
    Debug {
        #[command(subcommand)]
        cmd: DebugCmd,
    },
}

#[derive(Subcommand)]
enum DebugCmd {
    /// 生成一把集群不认识的 key（key 2 + 随机秘密），演示认证失败
    GenKey {
        /// 写到哪个文件（权限 0600）
        #[arg(long)]
        out: PathBuf,
    },
}

fn main() {
    let cli = Cli::parse();
    // 前台运行 server 的命令：在创建任何线程之前屏蔽 SIGTERM/SIGINT，最后由 main 线程 sigwait（design.md 0.2 第 4 条）
    let serving = matches!(&cli.cmd, Cmd::Server { .. }) || matches!(&cli.cmd, Cmd::Init(a) if !a.no_start);
    if serving && let Err(e) = sys::block_signals(&[sys::SIGTERM, sys::SIGINT]) {
        std::process::exit(fail(format!("屏蔽信号失败：{e}")));
    }
    let code = match cli.cmd {
        Cmd::Doctor { json } => doctor::run(json),
        Cmd::Init(a) => node::init(a),
        Cmd::Server { data_dir } => node::server(&data_dir),
        Cmd::Ping(a) => ping::run(a),
        Cmd::Failpoint { cmd } => failpoint::run(cmd),
        Cmd::Dev { cmd, dir } => dev::run(cmd, dir),
        Cmd::Debug { cmd: DebugCmd::GenKey { out } } => node::gen_key(&out),
    };
    std::process::exit(code);
}

/// 把一条命令的全部输出一次写到标准输出（各子命令共用）。不用 println!：它写失败时会 panic。
/// 读的一方提前退出（`wmfs doctor | head -1`、`| true`）不算错误；其他写失败（例如 `> /dev/full`）
/// 打印原因并返回 `Err(1)`（调用方把它当退出码）。
fn write_stdout(text: &str) -> Result<(), i32> {
    let mut out = std::io::stdout().lock();
    match out.write_all(text.as_bytes()).and_then(|()| out.flush()) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => Ok(()),
        Err(e) => {
            let _ = writeln!(std::io::stderr(), "写标准输出失败：{e}");
            Err(1)
        }
    }
}

/// 输出一行到标准输出，返回退出码 0（写失败时 1）。
fn out(line: impl std::fmt::Display) -> i32 {
    match write_stdout(&format!("{line}\n")) {
        Ok(()) => 0,
        Err(code) => code,
    }
}

/// 把错误写到标准错误，返回退出码 1。
fn fail(msg: impl std::fmt::Display) -> i32 {
    let _ = writeln!(std::io::stderr(), "{msg}");
    1
}

fn home() -> PathBuf {
    PathBuf::from(std::env::var_os("HOME").unwrap_or_else(|| ".".into()))
}

/// 管理员密钥：`--key-file F`，否则环境变量 `WMFS_KEY_FILE`，否则 `~/.config/wmfs/admin.key`（design.md 7.2）。
fn load_key(arg: &Option<PathBuf>) -> Result<KeyFile, String> {
    let p = arg
        .clone()
        .or_else(|| std::env::var_os("WMFS_KEY_FILE").map(PathBuf::from))
        .unwrap_or_else(|| home().join(".config/wmfs/admin.key"));
    let text = std::fs::read_to_string(&p).map_err(|e| {
        format!("读密钥文件 {} 失败：{e}（用 --key-file 指定，或先 eval \"$(wmfs dev env)\"）", p.display())
    })?;
    KeyFile::parse(&text).map_err(|e| format!("密钥文件 {} 格式不对：{e}", p.display()))
}

/// 集群的管理地址：`--cluster a,b`，否则环境变量 `WMFS_CLUSTER`（逗号分隔的多个地址）。
fn cluster_addrs(arg: &Option<String>) -> Result<Vec<String>, String> {
    let s = arg.clone().or_else(|| std::env::var("WMFS_CLUSTER").ok()).unwrap_or_default();
    let v: Vec<String> = s.split(',').map(str::trim).filter(|x| !x.is_empty()).map(String::from).collect();
    if v.is_empty() {
        return Err("不知道集群的管理地址：用 --cluster 指定，或先 eval \"$(wmfs dev env)\"".into());
    }
    Ok(v)
}

#[cfg(test)]
mod tests {
    use wmfs_core::hooks::manifest::{C_ABI_VERSIONS, CORE_ABI_VERSION};
    use wmfs_core::proto::frame::PROTO_MAJOR;

    #[test]
    fn version_string_matches_constants() {
        let c_abi: Vec<String> = C_ABI_VERSIONS.iter().map(|v| v.to_string()).collect();
        let want = format!(
            "{} (proto {}, core-abi {}, c-abi {})",
            env!("CARGO_PKG_VERSION"),
            PROTO_MAJOR,
            CORE_ABI_VERSION,
            c_abi.join(",")
        );
        assert_eq!(super::VERSION, want, "改了协议/ABI 版本号时同步改 VERSION");
    }

    #[test]
    fn cli_definition_is_valid() {
        use clap::CommandFactory;
        super::Cli::command().debug_assert();
    }

    #[test]
    fn registry_builds_the_default_plugin_set() {
        let s = super::registry::build_plugins(&Default::default()).unwrap();
        assert_eq!(s.plugin_list().plugins, vec!["authn:shared-key"]);
        let t = super::registry::client_transports().unwrap();
        assert_eq!(t.iter().map(|t| t.scheme()).collect::<Vec<_>>(), vec!["tcp"]);
    }
}
