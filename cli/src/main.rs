//! wmfs：唯一的二进制（design.md 1.1、7.2，ADR-0006）。
//!
//! M1 的子命令：`doctor`、`init`、`server`、`ping`、`failpoint set/clear/list`、`dev up/down/env/status/kill/start/logs`
//! （基础版）、`debug gen-key`；M3 加了 `dev wait`（leader）与 `debug kv put/get/bench`，`init` 写元数据的初始状态；
//! M4 加了 `fs`（元数据命令与 `--as-uid`）、`init --target`。M5 加了 `fs put/get/cat/truncate` 与
//! `debug chunk-path/chunk-header/corrupt-chunk`（按 local-fs 的路径规则找 chunk 文件：没编进 local-fs 插件就没有这三个子命令）。
//! M6 加了 `mount`/`umount`（FUSE，`fuse/`）与 `dev mount/umount/client-pid`。M7 加了策略命令 `set/get/unset/usage/status`
//! （`attr.rs`，挂载点路径的换算在 `pathmap.rs`）、`changelog tail`（`changelog.rs`）、`cluster set`（`cluster.rs`）与
//! `debug rstat-check`。M8 加了 `join`、`server --adopt-local-plugins`、`node list|drain|remove|promote`（`node_admin.rs`）、
//! `cluster plugins show|add|remove`、`dev up --nodes N`、`dev leader`、`debug meta-hash`。M9 加了 `chain list|show|force-serving`
//! （`chain.rs`）、`dev restart-all`、`dev pause|resume`、`dev wait chains-serving`、`dev up --profile nas-safe`。
//! 其余子命令随里程碑加入。插件注册表 `registry.rs` 是全仓库唯一"知道有哪些插件"的文件。

mod attr;
mod chain;
mod changelog;
mod cluster;
mod debug;
mod dev;
mod doctor;
mod failpoint;
mod fs;
mod fuse;
mod leader;
mod node;
mod node_admin;
mod pathmap;
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
    /// 初始化一个新的单节点集群（目录、密钥、node.toml、元数据的初始状态），然后在前台运行它
    Init(node::InitArgs),
    /// 加入一个已有的集群（带着本节点的插件清单，不一致就被拒绝），然后在前台运行它
    Join(node::JoinArgs),
    /// 在前台运行一个已初始化的节点（SIGTERM / Ctrl-C 退出）
    Server {
        /// 节点数据目录（wmfs init / wmfs join 建的）
        #[arg(long)]
        data_dir: PathBuf,
        /// 逃生口：成为就绪 leader 时把集群插件清单改成本节点的（某个插件再也装不上时用，design.md 3.7.2）
        #[arg(long)]
        adopt_local_plugins: bool,
    },
    /// 节点运维：list（--json）、drain、remove、promote
    Node {
        #[command(subcommand)]
        cmd: node_admin::NodeCmd,
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
    /// 不经 FUSE 的调试客户端：直接发元数据与数据请求（ls、stat、mkdir、rmdir、rm、mv、ln、touch、put、get、cat、truncate）
    Fs(fs::FsArgs),
    /// 把 wmfs 挂成一个目录（前台运行；SIGTERM / Ctrl-C 写出全部数据后卸载）
    Mount(fuse::MountArgs),
    /// 卸载一个 wmfs 挂载点（fusermount3 -u）
    Umount { mountpoint: PathBuf },
    /// 设置目录属性，例如 wmfs set ~/wmfs-mnt/proj chunk=1M,stripe=2（服务端切分、解析；打印规范化的结果与警告）
    Set(attr::SetArgs),
    /// 生效属性、来源与生效方式，最后一行是 semantics:（-r：另列出子树里设置了属性的目录）
    Get(attr::GetArgs),
    /// 删掉目录上的一个属性
    Unset(attr::UnsetArgs),
    /// 目录的递归用量：字节数、文件数、子目录数（常量时间）
    Usage(attr::PathArgs),
    /// 声明 vs 实际；第一行起是集群警告（不给路径 = 整个文件系统）
    Status(attr::StatusArgs),
    /// changelog 流（tail）
    Changelog {
        #[command(subcommand)]
        cmd: changelog::ChangelogCmd,
    },
    /// 集群管理（set：集群运行参数；plugins show|add|remove：集群插件清单）
    Cluster {
        #[command(subcommand)]
        cmd: cluster::ClusterCmd,
    },
    /// 数据链（list、show、force-serving）
    Chain {
        #[command(subcommand)]
        cmd: chain::ChainCmd,
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
    /// 离线检查递归用量计数（节点要先停掉）：从全部 inode 重新算一遍，与每个目录的 RSTAT 比较
    RstatCheck {
        /// 节点数据目录（例如 ~/wmfs-dev/n1）
        data_dir: PathBuf,
        #[arg(long)]
        json: bool,
    },
    /// 各节点已应用的元数据状态的哈希（等到各节点追平再比较；不一致时退出码 1）
    MetaHash {
        #[command(flatten)]
        api: attr::ApiArgs,
    },
    /// 读写元数据 KV 的调试键空间（服务端要开 debug_ops，只接受 admin key）
    Kv {
        #[command(subcommand)]
        cmd: debug::KvCmd,
    },
    /// 某个文件第 idx 个 chunk 的已提交版本在本机上的路径（--node 选链上哪个节点的副本，默认 head）
    #[cfg(feature = "local-fs")]
    ChunkPath(debug::chunk::Where),
    /// 解析一个 chunk 文件的固定头与块表
    #[cfg(feature = "local-fs")]
    ChunkHeader {
        file: PathBuf,
        #[arg(long)]
        json: bool,
    },
    /// 人为制造一个坏块：按块表找到第 b 块的存储字节，翻转其中一个字节（演示与验收用）
    #[cfg(feature = "local-fs")]
    CorruptChunk {
        #[command(flatten)]
        at: debug::chunk::Where,
        /// 第几块（从 0 起）
        #[arg(long, default_value_t = 0)]
        block: u32,
    },
}

fn main() {
    let cli = Cli::parse();
    // 前台运行 server 的命令：在创建任何线程之前屏蔽 SIGTERM/SIGINT，最后由 main 线程 sigwait（design.md 0.2 第 4 条）
    let serving = matches!(&cli.cmd, Cmd::Server { .. } | Cmd::Mount(_))
        || matches!(&cli.cmd, Cmd::Init(a) if !a.no_start)
        || matches!(&cli.cmd, Cmd::Join(a) if !a.no_start);
    if serving && let Err(e) = sys::block_signals(&[sys::SIGTERM, sys::SIGINT]) {
        std::process::exit(fail(format!("屏蔽信号失败：{e}")));
    }
    let code = match cli.cmd {
        Cmd::Doctor { json } => doctor::run(json),
        Cmd::Init(a) => node::init(a),
        Cmd::Join(a) => node::join(a),
        Cmd::Server { data_dir, adopt_local_plugins } => {
            node::server(&data_dir, wmfs_core::server::StartOpts { adopt_local_plugins })
        }
        Cmd::Node { cmd } => node_admin::run_node(cmd),
        Cmd::Ping(a) => ping::run(a),
        Cmd::Failpoint { cmd } => failpoint::run(cmd),
        Cmd::Dev { cmd, dir } => dev::run(cmd, dir),
        Cmd::Fs(a) => fs::run(a),
        Cmd::Mount(a) => fuse::run(a),
        Cmd::Umount { mountpoint } => fuse::umount(&mountpoint),
        Cmd::Set(a) => attr::set(a),
        Cmd::Get(a) => attr::get(a),
        Cmd::Unset(a) => attr::unset(a),
        Cmd::Usage(a) => attr::usage(a),
        Cmd::Status(a) => attr::status(a),
        Cmd::Changelog { cmd } => changelog::run_changelog(cmd),
        Cmd::Cluster { cmd } => cluster::run_cluster(cmd),
        Cmd::Chain { cmd } => chain::run_chain(cmd),
        Cmd::Debug { cmd: DebugCmd::RstatCheck { data_dir, json } } => debug::rstat_check(&data_dir, json),
        Cmd::Debug { cmd: DebugCmd::GenKey { out } } => node::gen_key(&out),
        Cmd::Debug { cmd: DebugCmd::Kv { cmd } } => debug::run(cmd),
        Cmd::Debug { cmd: DebugCmd::MetaHash { api } } => debug::meta_hash::meta_hash(&api),
        #[cfg(feature = "local-fs")]
        Cmd::Debug { cmd: DebugCmd::ChunkPath(w) } => debug::chunk::chunk_path(&w),
        #[cfg(feature = "local-fs")]
        Cmd::Debug { cmd: DebugCmd::ChunkHeader { file, json } } => debug::chunk::chunk_header(&file, json),
        #[cfg(feature = "local-fs")]
        Cmd::Debug { cmd: DebugCmd::CorruptChunk { at, block } } => debug::chunk::corrupt(&at, block),
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
        let want = [
            "authn:shared-key",
            "authz:posix-mode",
            "bg_task:gc",
            "codec:replica",
            "placement:label-match",
            "qos:fifo",
            "transform:crc32c",
        ];
        assert_eq!(s.plugin_list().plugins, want, "chunk_store（local-fs）与传输插件不进集群插件清单（3.7.2）");
        assert!(s.chunk_store_plugin().is_some());
        let t = super::registry::client_transports().unwrap();
        assert_eq!(
            t.iter().map(|t| t.scheme()).collect::<Vec<_>>(),
            vec!["tcp", "rdma"],
            "rdma 默认编进来（不监听、不碰设备）"
        );
    }
}
