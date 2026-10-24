//! `wmfs init`、`wmfs join`、`wmfs server`、`wmfs debug gen-key`（design.md 3.4.3、7.2）。
//!
//! 实现注记（M8）：`wmfs join <任一节点的 rpc 地址> --cluster-key F --data-dir D [--addr A] [--rpc-port P] [--mgmt-port M]
//! [--meta auto|none] [--target PATH[:labels]]… [--disable 插件]… [--no-start]`：按本节点的 node.toml（`--disable` 写进
//! `[plugins] disable`）装出插件清单，向集群发 Join；插件清单与集群不一致被拒绝时打印差异并以**退出码 3** 结束（与运行中的节点发现
//! 不一致时退出的码相同，3.7.2）。成功后像 init 一样前台运行 server。`wmfs server --adopt-local-plugins` 是 3.7.2 第 5 条的逃生口。

use std::path::{Path, PathBuf};

use wmfs_core::api::types::KeyFile;
use wmfs_core::base::config::NodeConfig;
use wmfs_core::base::{ErrCode, rand, sys};
use wmfs_core::buf::PoolConfig;
use wmfs_core::init::{
    CLIENT_KEY, InitOpts, JoinOpts, NodeFile, Profile, init_node, initial_warnings, join_node, parse_target,
};
use wmfs_core::meta::cluster::plugins::EXIT_MISMATCH;
use wmfs_core::server::StartOpts;

use crate::{fail, home, out, registry};

#[derive(clap::Args)]
pub struct InitArgs {
    /// 节点数据目录（必须不存在或为空）
    #[arg(long)]
    data_dir: PathBuf,
    /// 监听地址（rpc 与管理 API 都用它）
    #[arg(long, default_value = "127.0.0.1")]
    addr: String,
    /// rpc 端口（客户端与其他节点连它）
    #[arg(long, default_value_t = 9300)]
    rpc_port: u16,
    /// 管理 API（HTTP）端口
    #[arg(long, default_value_t = 9380)]
    mgmt_port: u16,
    /// 一个 target（一块盘或一个目录）：PATH[:标签,标签]，可以给多次；不给就没有地方放文件数据（新建文件报 ENOSPC）
    #[arg(long, value_name = "PATH[:label,label]")]
    target: Vec<String>,
    /// 部署模板（design.md 3.4.3）：default（/home replicas=3、/scratch replicas=2）或 minimal（default 类 1 副本、不写 replicas
    /// 属性、停用 quota 与 mclock）；nas-safe、hpc-rdma 在 M9 起
    #[arg(long, default_value = "default")]
    profile: String,
    /// 只初始化，不在前台运行 server
    #[arg(long)]
    pub no_start: bool,
}

#[derive(clap::Args)]
pub struct JoinArgs {
    /// 集群里任一节点的 rpc 地址（host:port；不是 leader 也行，会跟随）
    seed: String,
    /// 集群主密钥（从已有节点的数据目录复制过来的 cluster.key；不通过网络传输秘密）
    #[arg(long)]
    cluster_key: PathBuf,
    /// 本节点的数据目录（必须不存在或为空）
    #[arg(long)]
    data_dir: PathBuf,
    /// 监听地址（rpc 与管理 API 都用它）
    #[arg(long, default_value = "127.0.0.1")]
    addr: String,
    /// rpc 端口（客户端与其他节点连它）
    #[arg(long, default_value_t = 9300)]
    rpc_port: u16,
    /// 管理 API（HTTP）端口
    #[arg(long, default_value_t = 9380)]
    mgmt_port: u16,
    /// auto：现有元数据成员不到 3 个时以 learner 加入、追上之后提升；none：只做数据节点
    #[arg(long, default_value = "auto")]
    meta: String,
    /// 一个 target（一块盘或一个目录）：PATH[:标签,标签]，可以给多次；不给就没有地方放文件数据
    #[arg(long, value_name = "PATH[:label,label]")]
    target: Vec<String>,
    /// 停用一个插件（写进 node.toml 的 [plugins] disable；插件清单与集群不一致会被拒绝加入）
    #[arg(long)]
    disable: Vec<String>,
    /// 只加入，不在前台运行 server
    #[arg(long)]
    pub no_start: bool,
}

/// `wmfs join`（见文件头）。
pub fn join(a: JoinArgs) -> i32 {
    let targets = match a.target.iter().map(|t| parse_target(t)).collect::<Result<Vec<_>, _>>() {
        Ok(t) => t,
        Err(e) => return fail(e),
    };
    let master: [u8; 32] = match std::fs::read(&a.cluster_key).map(|b| b.try_into()) {
        Ok(Ok(m)) => m,
        _ => return fail(format!("读 {} 失败：应是 32 字节的 cluster.key", a.cluster_key.display())),
    };
    let meta_auto = match a.meta.as_str() {
        "auto" => true,
        "none" => false,
        m => return fail(format!("--meta {m:?}：只能是 auto 或 none")),
    };
    let plugins = match registry::init_plugin_list(&a.disable) {
        Ok(p) => p,
        Err(e) => return fail(e),
    };
    let transports = match registry::client_transports() {
        Ok(t) => t,
        Err(e) => return fail(e),
    };
    let o = JoinOpts {
        data_dir: a.data_dir.clone(),
        seed: a.seed,
        master,
        addr: a.addr,
        rpc_port: a.rpc_port,
        mgmt_port: a.mgmt_port,
        mem: PoolConfig::server(),
        failpoints_api: false,
        debug_ops: false,
        host_id: None,
        plugins,
        disable: a.disable,
        targets,
        meta_auto,
        transports,
    };
    match join_node(&o) {
        Ok(id) => {
            let code = out(format!("已加入集群：{}（node {id}）", a.data_dir.display()));
            if code != 0 || a.no_start { code } else { server(&a.data_dir, StartOpts::default()) }
        }
        Err(e) if e.rejected && e.code == ErrCode::EINVAL => {
            let _ = fail(format!("wmfs join：{}", e.msg));
            EXIT_MISMATCH
        }
        Err(e) => fail(format!("wmfs join 失败：{}（{}）", e.msg, e.code)),
    }
}

/// `wmfs init`：建数据目录、生成集群主密钥与 key 文件（并复制到 ~/.config/wmfs/）、写 node.toml，然后前台运行 server。
pub fn init(a: InitArgs) -> i32 {
    let targets = match a.target.iter().map(|t| parse_target(t)).collect::<Result<Vec<_>, _>>() {
        Ok(t) => t,
        Err(e) => return fail(e),
    };
    let profile = match Profile::parse(&a.profile) {
        Ok(p) => p,
        Err(e) => return fail(e),
    };
    let o = InitOpts {
        data_dir: a.data_dir.clone(),
        addr: a.addr,
        rpc_port: a.rpc_port,
        mgmt_port: a.mgmt_port,
        mem: PoolConfig::server(),
        failpoints_api: false,
        debug_ops: false,
        host_id: None,
        plugins: match registry::init_plugin_list(&profile.disabled_plugins()) {
            Ok(p) => p,
            Err(e) => return fail(e),
        },
        targets,
        profile,
    };
    if let Err(e) = init_node(&o) {
        return fail(e);
    }
    let d = &a.data_dir;
    let cluster = NodeFile::read(d).map(|n| n.cluster_id).unwrap_or_default();
    let mut lines = vec![format!("已初始化 {}（cluster {cluster}，node 1）", d.display())];
    for name in ["admin.key", "client.key"] {
        lines.push(copy_key(&d.join("keys").join(name), &home().join(".config/wmfs").join(name)));
    }
    // 声明在当前集群无法满足时大声警告（3.4.3 第 4 步、ADR-0016）
    lines.extend(initial_warnings(&o).iter().map(|w| format!("警告：{w}")));
    let code = out(lines.join("\n"));
    if code != 0 || a.no_start {
        return code;
    }
    server(d, StartOpts::default())
}

/// 把 init 生成的 key 文件复制到 ~/.config/wmfs/（0600）。那里已经有**别的**内容时不覆盖（可能属于另一个集群）。
fn copy_key(from: &Path, to: &Path) -> String {
    let Ok(bytes) = std::fs::read(from) else { return format!("！读不到 {}", from.display()) };
    match std::fs::read(to) {
        Ok(old) if old == bytes => return format!("{} 已是这把密钥", to.display()),
        Ok(_) => {
            return format!(
                "！{} 已存在（另一个集群的？），没有覆盖；请用 --key-file {}",
                to.display(),
                from.display()
            );
        }
        Err(_) => {}
    }
    let dir = to.parent().unwrap_or(Path::new("."));
    match std::fs::create_dir_all(dir).map_err(ErrCode::from).and_then(|()| sys::atomic_write(to, &bytes)) {
        Ok(()) => format!("密钥已复制到 {}", to.display()),
        Err(e) => format!("！复制密钥到 {} 失败：{e}", to.display()),
    }
}

/// `wmfs server --data-dir D [--adopt-local-plugins]`：前台运行，直到 SIGTERM / SIGINT（main 已经屏蔽了这两个信号）。
pub fn server(data_dir: &Path, opts: StartOpts) -> i32 {
    let toml = data_dir.join("node.toml");
    let cfg = match std::fs::read_to_string(&toml)
        .map_err(|e| format!("读 {} 失败：{e}", toml.display()))
        .and_then(|t| NodeConfig::parse(&t))
    {
        Ok(c) => c,
        Err(e) => return fail(e),
    };
    let start = |p| wmfs_core::server::start(data_dir, &cfg, p, crate::VERSION, opts);
    let running = match registry::build_plugins(&cfg).and_then(start) {
        Ok(r) => r,
        Err(e) => return fail(format!("node 启动失败：{e}")),
    };
    let rpc: Vec<String> = running.addrs.iter().map(|(_, a)| a.clone()).collect();
    out(format!(
        "node{} started（rpc {}，http {}；日志 {}）",
        running.node_id,
        rpc.join(","),
        running.mgmt_addr,
        data_dir.join("logs/server.log").display()
    ));
    let sig = sys::wait_signals(&[sys::SIGTERM, sys::SIGINT]);
    running.stop();
    if let Err(e) = sig {
        return fail(format!("等待信号失败：{e}"));
    }
    0
}

/// `wmfs debug gen-key --out F`：key 2（默认 client 的号）+ 随机秘密——号是对的，秘密不对，于是演示的是"证明不对"被拒。
pub fn gen_key(path: &Path) -> i32 {
    let mut secret = [0u8; 32];
    rand::fill(&mut secret);
    let k = KeyFile { key_id: CLIENT_KEY, secret };
    match sys::atomic_write(path, k.to_line().as_bytes()) {
        Ok(()) => {
            out(format!("已生成 {}（key {CLIENT_KEY}，随机秘密：集群不认识它，用它连接会被拒绝）", path.display()))
        }
        Err(e) => fail(format!("写 {} 失败：{e}", path.display())),
    }
}
