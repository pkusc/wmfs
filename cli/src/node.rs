//! `wmfs init`、`wmfs join`、`wmfs server`、`wmfs debug gen-key`（design.md 3.4.3、7.2）。
//!
//! 实现注记（M8）：`wmfs join <任一节点的 rpc 地址> --cluster-key F --data-dir D [--addr A] [--rpc-port P] [--mgmt-port M]
//! [--meta auto|none] [--target PATH[:labels]]… [--disable 插件]… [--dylib F]… [--no-start]`：按本节点的 node.toml（`--disable` 写进
//! `[plugins] disable`，M14 的 `--dylib` 写进 `[plugins] dylib`）装出插件清单，向集群发 Join；插件清单与集群不一致被拒绝时打印差异并以**退出码 3** 结束（与运行中的节点发现
//! 不一致时退出的码相同，3.7.2）。成功后像 init 一样前台运行 server。`wmfs server --adopt-local-plugins` 是 3.7.2 第 5 条的逃生口。
//!
//! 实现注记（M15）：`wmfs init --profile hpc-rdma` 按 default 的模板建，另让文件数据走 rdma（[`rdma_transport`]：init 在 NODE 之前把
//! `[plugin.rdma]` 与 `[net] data_scheme` 写进 node.toml，rdma 地址与 tcp 的一起公布进 NodeRecV1）；hpc-rdma 集群新加入的节点用
//! `wmfs join --rdma`（规格的 join 没有这个参数，最小的加法）。

use std::path::{Path, PathBuf};

use wmfs_core::api::types::KeyFile;
use wmfs_core::base::config::{NodeConfig, PluginTable};
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
    /// 部署模板（design.md 3.4.3）：default（/home replicas=3、/scratch replicas=2）、minimal（default 类 1 副本、不写 replicas
    /// 属性、停用 quota 与 mclock）、nas-safe（/home replicas=3,trash=7d,audit=on）或 hpc-rdma（default 再加 RDMA：文件数据的读写
    /// 走 rdma、元数据照旧走 tcp；rdma 监听 <--addr>:<rpc 端口 + 1>，--addr 要是本机 RoCE 网口的 IPv4；首发未验证）
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
    /// 加载一个动态库插件（.so，写进 node.toml 的 [plugins] dylib；集群插件清单里有它时必须带上，否则被拒绝加入）
    #[arg(long)]
    dylib: Vec<String>,
    /// hpc-rdma 集群的节点：文件数据的读写走 rdma、元数据照旧走 tcp（同 wmfs init --profile hpc-rdma：rdma 监听 <--addr>:<rpc 端口 + 1>，
    /// --addr 要是本机 RoCE 网口的 IPv4）
    #[arg(long)]
    rdma: bool,
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
    let plugins = match registry::init_plugin_list(&a.disable, &a.dylib) {
        Ok(p) => p,
        Err(e) => return fail(e),
    };
    let transports = match registry::client_transports() {
        Ok(t) => t,
        Err(e) => return fail(e),
    };
    let data_transport = match a.rdma.then(|| rdma_transport(&a.addr, a.rpc_port, "")).transpose() {
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
        dylib: a.dylib,
        targets,
        meta_auto,
        transports,
        data_transport,
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
    let o = match init_opts(&a) {
        Ok(o) => o,
        Err(e) => return fail(e),
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

/// `wmfs init` 的参数 → [`InitOpts`]。
fn init_opts(a: &InitArgs) -> Result<InitOpts, String> {
    let targets = a.target.iter().map(|t| parse_target(t)).collect::<Result<Vec<_>, _>>()?;
    let (profile, hpc_rdma) = parse_profile(&a.profile)?;
    let data_transport = if hpc_rdma { Some(rdma_transport(&a.addr, a.rpc_port, "")?) } else { None };
    Ok(InitOpts {
        data_dir: a.data_dir.clone(),
        addr: a.addr.clone(),
        rpc_port: a.rpc_port,
        mgmt_port: a.mgmt_port,
        mem: PoolConfig::server(),
        failpoints_api: false,
        debug_ops: false,
        host_id: None,
        plugins: registry::init_plugin_list(&profile.disabled_plugins(), &[])?,
        targets,
        profile,
        data_transport,
    })
}

/// `--profile` 的解析（合法名字的列表只写在这里）：hpc-rdma（3.4.3）按 default 的模板建——core 的部署模板里没有它（core 除注释外不出现
/// 传输的名字，2.1）——另让文件数据走 rdma（[`rdma_transport`]）。返回 (模板, 是否 hpc-rdma)。
pub fn parse_profile(s: &str) -> Result<(Profile, bool), String> {
    if s == "hpc-rdma" {
        return Ok((Profile::Default, true));
    }
    let p = Profile::parse(s).ok_or_else(|| format!("--profile {s:?}：只有 default、minimal、nas-safe、hpc-rdma"))?;
    Ok((p, false))
}

/// 文件数据走 rdma（hpc-rdma，3.4.3）：`[plugin.rdma] listen = "<addr>:<rpc 端口 + 1>"` 再加 `extra`（附加的 TOML 行，`wmfs dev` 给
/// `max_qps = 32`）。交给 init / join：写进 node.toml、设 `[net] data_scheme = "rdma"`（元数据照旧走 tcp）、公布进 NodeRecV1.addrs。
pub fn rdma_transport(addr: &str, rpc_port: u16, extra: &str) -> Result<(String, PluginTable), String> {
    let t = format!("listen = \"{addr}:{}\"\n{extra}", u32::from(rpc_port) + 1);
    Ok(("rdma".into(), t.parse().map_err(|e| format!("[plugin.rdma] 编码失败：{e}"))?))
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

#[cfg(test)]
mod tests {
    use super::*;
    use wmfs_core::meta::raft::storage::KV;
    use wmfs_core::meta::schema::keys;
    use wmfs_core::meta::schema::records::{NodeRecV1, decode_v1};

    /// M15：hpc-rdma = default 的模板 + 文件数据走 rdma（3.4.3）：init 在 NODE 之前把 `[plugin.rdma]` 与 data_scheme 写进 node.toml，
    /// rdma 地址与 tcp 的一起写进初始 KV 的 NodeRecV1(1).addrs（客户端 `--transport rdma` 靠它找地址）；不认识的 profile 列出全部四个。
    /// 只写文件，不碰 RDMA 设备。
    #[test]
    fn hpc_rdma_is_default_plus_rdma_published_at_init() {
        assert_eq!(parse_profile("nas-safe"), Ok((Profile::NasSafe, false)));
        assert!(parse_profile("bogus").unwrap_err().contains("hpc-rdma"));
        let d = std::env::temp_dir().join(format!("wmfs-hpc-rdma-{}", std::process::id()));
        let (addr, data_dir, profile) = ("10.77.0.1".to_string(), d.join("n1"), "hpc-rdma".to_string());
        let a = InitArgs { data_dir, addr, rpc_port: 9300, mgmt_port: 9380, target: vec![], profile, no_start: true };
        let o = init_opts(&a).unwrap();
        assert_eq!(o.profile, Profile::Default, "初始 KV 按 default 的模板");
        init_node(&o).unwrap();
        let cfg = NodeConfig::parse(&std::fs::read_to_string(d.join("n1/node.toml")).unwrap()).unwrap();
        assert_eq!(cfg.plugin_cfg("rdma").get("listen").and_then(|v| v.as_str()), Some("10.77.0.1:9301"));
        let schemes = (cfg.net.control_scheme.as_str(), cfg.net.data_scheme.as_str());
        assert_eq!(schemes, ("tcp", "rdma"), "元数据照旧走 tcp");
        crate::registry::build_plugins(&cfg).unwrap(); // rdma 插件按它的 config_schema 认这个键
        let db = redb::Database::open(d.join("n1/meta/meta.redb")).unwrap();
        let rtx = db.begin_read().unwrap();
        let kv = rtx.open_table(KV).unwrap();
        let node: NodeRecV1 = decode_v1(kv.get(keys::node(1).as_slice()).unwrap().unwrap().value()).unwrap();
        let want = [("tcp", "10.77.0.1:9300"), ("rdma", "10.77.0.1:9301")].map(|(s, a)| (s.to_string(), a.to_string()));
        assert_eq!(node.addrs, want, "公布 rdma 地址：mount --transport rdma 找得到");
        std::fs::remove_dir_all(&d).unwrap();
    }
}
