//! `wmfs mount` / `wmfs umount`（design.md 8.1、7.2）：把 wmfs 挂成一个普通目录（fuser 0.18，经 `fusermount3` 以普通用户挂载）。
//!
//! `wmfs mount <rpc 地址[,地址…]> <挂载点>`：挂载点上已经挂着 wmfs → 直接报错；依次试这些地址（`host:port` 或
//! `tcp://host:port`），用 client key 握手 → GetClusterMap + SessionOpen（`MetaClient::connect`，跟随 leader）→ 检查 `--transport`
//! 的 data_scheme 每个数据节点都公布了（没有就报错并列出缺的节点）→ 打印集群警告（`ClusterMap.warnings`，例如"……新文件实际为
//! 1 副本"）→ 挂载（16 个工作线程、clone_fd，`FSName = wmfs:<cluster_id 前 8 位 hex>`、`Subtype = wmfs`、`DefaultPermissions`；
//! 不开 allow_other）→ 打印"已挂载"。会话开了之后挂载失败也先 SessionClose 再退出（M10 之前会话不会被驱逐，不关就一直留在
//! leader 上）。之后前台运行：SIGTERM / SIGINT → 把全部脏数据写出去 → 卸载 → 关会话、退出；挂载点被别人卸载（`wmfs umount`、
//! `fusermount3 -u`）→ 会话结束 → 同样写出脏数据、关会话、退出。
//! 信号（0.2 第 4 条）：main 在创建任何线程之前屏蔽 SIGTERM/SIGINT，这里最后 `sys::wait_signals`；会话线程结束时给自己发一个
//! SIGTERM 叫醒 main。
//! `wmfs umount <挂载点>` = `fusermount3 -u <挂载点>`（挂载进程看到会话结束后自己退出）。
//!
//! [`WmfsFs`]（fuser 的 `Filesystem` 实现在 `fs.rs`，稍长的回调在 `ops.rs`，纯函数在 `util.rs`）：
//! - **身份**：uid/gid/pid 来自内核的请求；附加组读 `/proc/<pid>/status` 的 `Groups:`（按 pid 缓存 1 s），作业 id 读
//!   `/proc/<pid>/environ` 里 `--job-id-var` 指定的变量（默认依次试 `SLURM_JOB_ID`、`PBS_JOBID`、`WMFS_JOB_ID`，按 pid 缓存
//!   10 s，读不到为 0；取开头的数字）。客户端声明的 uid 只在它的 key 允许的范围内被相信（服务端检查，ADR-0009）。
//! - **TTL**（C1）：entry/attr = min(`AttrOut.ttl_ms`, `--max-attr-ttl`)。

mod fs;
mod ops;
mod util;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use fuser::{Config, FileAttr, INodeNo, MountOption, Notifier, Session};
use wmfs_core::api::hex::hex;
use wmfs_core::api::types::KeyFile;
use wmfs_core::base::config::NetConfig;
use wmfs_core::base::units::parse_duration;
use wmfs_core::base::{ErrCode, clock, lock, log, sys};
use wmfs_core::buf::{MemPool, PoolConfig};
use wmfs_core::client::data_client::DataClient;
use wmfs_core::client::files::FileClient;
use wmfs_core::client::meta_client::{Caller, MetaClient, MetaClientOpts, SessionInfo};
use wmfs_core::meta::ns::common::MODE_MASK;
use wmfs_core::net::client::{Endpoint, RpcClient};
use wmfs_core::proto::meta::AttrOut;
use wmfs_core::proto::peer::ClusterMap;

use crate::doctor::parse::parse_mountinfo;
use crate::{fail, home, out, registry};
use util::{file_type, job_in, systime};

#[derive(clap::Args)]
pub struct MountArgs {
    /// 元数据节点的 rpc 地址，逗号分隔（host:port 或 tcp://host:port；会跟随 leader）
    addrs: String,
    /// 挂载点（一个已存在的空目录）
    mountpoint: PathBuf,
    /// client key（默认 ~/.config/wmfs/client.key）
    #[arg(long)]
    key_file: Option<PathBuf>,
    /// 内核属性/目录项缓存时长的上限（只能把目录属性 attr_ttl 调小）
    #[arg(long, default_value = "10s")]
    max_attr_ttl: String,
    /// 块缓存：memory[:SIZE]（默认 512M）或 none
    #[arg(long, default_value = "memory")]
    cache: String,
    /// 数据请求走哪种传输（每个数据节点都要公布它）
    #[arg(long, default_value = "tcp")]
    transport: String,
    /// 一个操作连同重试的总期限，超过返回 EIO（类似 NFS hard 挂载）
    #[arg(long, default_value = "120s")]
    op_timeout: String,
    /// 作业 id 取自哪个环境变量（默认依次试 SLURM_JOB_ID、PBS_JOBID、WMFS_JOB_ID）
    #[arg(long)]
    job_id_var: Option<String>,
    /// 日志写到这个文件（默认写 stderr）
    #[arg(long)]
    log: Option<PathBuf>,
}

pub fn run(a: MountArgs) -> i32 {
    match mount(a) {
        Ok(()) => 0,
        Err(e) => fail(format!("wmfs mount：{e}")),
    }
}

fn mount(a: MountArgs) -> Result<(), String> {
    if let Some(p) = &a.log {
        log::set_file(p).map_err(|e| format!("打开日志 {} 失败：{e}", p.display()))?;
    }
    let mp = std::fs::canonicalize(&a.mountpoint).map_err(|e| format!("挂载点 {}：{e}", a.mountpoint.display()))?;
    let text = std::fs::read_to_string("/proc/self/mountinfo").unwrap_or_default();
    if parse_mountinfo(&text).iter().any(|(m, t)| *m == mp.display().to_string() && t == "fuse.wmfs") {
        return Err(format!("{} 上已经挂着 wmfs（先 wmfs umount {}）", mp.display(), mp.display()));
    }
    let kp = a.key_file.clone().unwrap_or_else(|| home().join(".config/wmfs/client.key"));
    let text = std::fs::read_to_string(&kp).map_err(|e| format!("读密钥文件 {} 失败：{e}", kp.display()))?;
    let key = KeyFile::parse(&text).map_err(|e| format!("密钥文件 {} 格式不对：{e}", kp.display()))?;
    let max_ttl = parse_duration(&a.max_attr_ttl)?;
    let op_timeout = parse_duration(&a.op_timeout)?;
    let net = NetConfig { data_scheme: a.transport.clone(), ..NetConfig::default() };
    let seeds = endpoints(&a.addrs, &net.control_scheme)?;
    let pool = MemPool::new(&PoolConfig::client()).map_err(|e| format!("内存池创建失败：{e}"))?;
    let rpc = RpcClient::new(pool, registry::client_transports()?, key, net)
        .map_err(|e| format!("建 RPC 客户端失败（--transport {}）：{e}", a.transport))?;
    let host = std::fs::read_to_string("/proc/sys/kernel/hostname").unwrap_or_default().trim().to_string();
    let info =
        SessionInfo { client_name: "wmfs-mount".into(), host, host_id: String::new(), mount: mp.display().to_string() };
    let opts = MetaClientOpts { rpc_timeout: Duration::from_secs(10), op_timeout };
    let meta = match MetaClient::connect_with(rpc.clone(), &seeds, info, opts) {
        Ok(m) => Arc::new(m),
        Err(e @ (ErrCode::ECONNRESET | ErrCode::ETIMEDOUT)) => {
            let why = "节点没在运行，或者地址、端口写错了？";
            return Err(format!("连接集群失败：{e}（{} 在 --op-timeout 内都连不上：{why}）", a.addrs));
        }
        Err(e) => return Err(format!("连接集群失败：{e}")),
    };
    let r = serve(&a, &mp, rpc, meta.clone(), max_ttl, op_timeout);
    meta.close(); // 不论成败：会话开了就要关（M10 之前不会被驱逐）
    r
}

/// 会话开好之后：检查传输、挂载、前台运行到信号或被卸载（见文件头）。
fn serve(
    a: &MountArgs,
    mp: &Path,
    rpc: Arc<RpcClient>,
    meta: Arc<MetaClient>,
    max_ttl: Duration,
    op_timeout: Duration,
) -> Result<(), String> {
    let map = meta.map();
    check_data_scheme(&map, &a.transport)?;
    let mut lines: Vec<String> = map.warnings.iter().map(|w| format!("警告：{w}")).collect();
    let data = DataClient::for_meta(rpc, meta.clone(), Duration::from_secs(10), op_timeout);
    let fc = FileClient::start(meta.clone(), data, registry::cache_store(&a.cache)?);
    let fs = WmfsFs::new(fc.clone(), max_ttl, a.job_id_var.clone());
    let slot = fs.notifier.clone();
    let mut cfg = Config::default();
    cfg.n_threads = Some(16);
    cfg.clone_fd = true;
    let fsname = format!("wmfs:{}", &hex(&map.cluster_id)[..8]); // FSName 里不能有逗号（fusermount 用逗号分隔选项）
    cfg.mount_options =
        vec![MountOption::FSName(fsname), MountOption::Subtype("wmfs".into()), MountOption::DefaultPermissions];
    let mut session = Session::new(fs, mp, &cfg).map_err(|e| format!("挂载 {} 失败：{e}", mp.display()))?;
    let _ = slot.set(session.notifier());
    let mut unmounter = session.unmount_callable();
    lines.push(format!("已挂载 {}（会话 {:x}）", mp.display(), meta.session()));
    let _ = out(lines.join("\n"));
    let t = std::thread::Builder::new()
        .name("fuse-session".into())
        .spawn(move || {
            if let Err(e) = session.run() {
                log::error("fuse", format_args!("会话结束：{e}"));
            }
            let _ = sys::send_signal(std::process::id() as i32, sys::SIGTERM); // 叫醒 main（它在 sigwait）
        })
        .map_err(|e| format!("起会话线程失败：{e}"))?;
    let _ = sys::wait_signals(&[sys::SIGTERM, sys::SIGINT]);
    let bad = fc.flush_all();
    if bad > 0 {
        log::error("fuse", format_args!("退出前有 {bad} 个文件的数据没能写出去"));
    }
    let _ = unmounter.unmount(); // 已经被别人卸载了就什么也不做
    let _ = t.join();
    log::info("fuse", format_args!("已卸载 {}", mp.display()));
    Ok(())
}

/// `a,b,c`（`host:port` 或 `scheme://host:port`）→ Endpoint 列表。
fn endpoints(s: &str, scheme: &str) -> Result<Vec<Endpoint>, String> {
    let v: Vec<Endpoint> = s
        .split(',')
        .map(str::trim)
        .filter(|x| !x.is_empty())
        .map(|x| {
            let (sch, addr) = x.split_once("://").unwrap_or((scheme, x));
            Endpoint { node: 0, addrs: vec![(sch.to_string(), addr.to_string())] }
        })
        .collect();
    if v.is_empty() {
        return Err("没有给出 rpc 地址（例如 127.0.0.1:19100）".into());
    }
    Ok(v)
}

/// 每个数据节点（有 target 的节点）都公布了 `scheme` 的地址；缺的列出来（8.1：不静默降级）。
fn check_data_scheme(map: &ClusterMap, scheme: &str) -> Result<(), String> {
    let mut missing: Vec<u32> = map
        .targets
        .iter()
        .map(|(_, t)| t.node)
        .filter(|n| !map.nodes.iter().any(|(id, r)| id == n && r.addrs.iter().any(|(s, _)| s == scheme)))
        .collect();
    missing.sort();
    missing.dedup();
    if missing.is_empty() {
        return Ok(());
    }
    Err(format!("这些数据节点没有公布 {scheme} 地址：{missing:?}（--transport 换一种，或给它们配上 {scheme}）"))
}

/// `wmfs umount <挂载点>` = `fusermount3 -u`。
pub fn umount(mp: &Path) -> i32 {
    match std::process::Command::new("fusermount3").arg("-u").arg(mp).status() {
        Ok(s) if s.success() => 0,
        Ok(s) => fail(format!("fusermount3 -u {} 失败（{s}）", mp.display())),
        Err(e) => fail(format!("运行 fusermount3 失败：{e}")),
    }
}

/// 默认依次尝试的作业 id 环境变量（8.7）。
const JOB_VARS: [&str; 3] = ["SLURM_JOB_ID", "PBS_JOBID", "WMFS_JOB_ID"];
/// 附加组、作业 id 的缓存最多记多少个 pid（超过就整个清掉：只是缓存）。
const PID_CACHE_MAX: usize = 4096;

/// 挂载上的文件系统对象（fuser 的 `Filesystem` 实现在 `fs.rs`）。
pub struct WmfsFs {
    pub fc: Arc<FileClient>,
    pub notifier: Arc<OnceLock<Notifier>>, // Session::new 拿走 fs 之后才有 notifier，事后塞进来（8.1）
    max_ttl_ms: u64,
    job_var: Option<String>,
    groups: Mutex<HashMap<u32, (u64, Vec<u32>)>>, // pid → (过期时刻, 附加组)
    jobs: Mutex<HashMap<u32, (u64, u64)>>,        // pid → (过期时刻, 作业 id)
}

impl WmfsFs {
    pub fn new(fc: Arc<FileClient>, max_ttl: Duration, job_var: Option<String>) -> WmfsFs {
        WmfsFs {
            fc,
            notifier: Arc::new(OnceLock::new()),
            max_ttl_ms: max_ttl.as_millis() as u64,
            job_var,
            groups: Mutex::default(),
            jobs: Mutex::default(),
        }
    }

    /// 发请求的人（见文件头）。
    pub fn who(&self, req: &fuser::Request) -> Caller {
        let (uid, gid, pid) = (req.uid(), req.gid(), req.pid());
        Caller { uid, gid, groups: self.groups_of(pid), pid, job_id: self.job_of(pid) }
    }

    fn groups_of(&self, pid: u32) -> Vec<u32> {
        let now = clock::mono_ms();
        if let Some((_, g)) = lock(&self.groups).get(&pid).filter(|(t, _)| *t > now) {
            return g.clone();
        }
        let status = std::fs::read_to_string(format!("/proc/{pid}/status")).unwrap_or_default();
        let line = status.lines().find_map(|l| l.strip_prefix("Groups:")).unwrap_or("");
        let g: Vec<u32> = line.split_whitespace().filter_map(|x| x.parse().ok()).collect();
        let mut m = lock(&self.groups);
        if m.len() > PID_CACHE_MAX {
            m.clear();
        }
        m.insert(pid, (now + 1000, g.clone()));
        g
    }

    fn job_of(&self, pid: u32) -> u64 {
        let now = clock::mono_ms();
        if let Some((_, j)) = lock(&self.jobs).get(&pid).filter(|(t, _)| *t > now) {
            return *j;
        }
        let env = std::fs::read(format!("/proc/{pid}/environ")).unwrap_or_default();
        let vars: Vec<&str> = match &self.job_var {
            Some(v) => vec![v.as_str()],
            None => JOB_VARS.to_vec(),
        };
        let job = vars.iter().find_map(|v| job_in(&env, v)).unwrap_or(0);
        let mut m = lock(&self.jobs);
        if m.len() > PID_CACHE_MAX {
            m.clear();
        }
        m.insert(pid, (now + 10_000, job));
        job
    }

    /// 回给内核的属性与它的 TTL（C1）；本挂载还有没提交的写时长度用本地的（`FileClient::fix_attr`）。
    pub fn fattr(&self, mut a: AttrOut) -> (Duration, FileAttr) {
        self.fc.fix_attr(&mut a);
        let ttl = Duration::from_millis((a.ttl_ms as u64).min(self.max_ttl_ms));
        let fa = FileAttr {
            ino: INodeNo(a.ino),
            size: a.size,
            blocks: a.size.div_ceil(512),
            atime: systime(a.atime),
            mtime: systime(a.mtime),
            ctime: systime(a.ctime),
            crtime: systime(a.btime),
            kind: file_type(a.kind),
            perm: (a.mode & MODE_MASK) as u16,
            nlink: a.nlink,
            uid: a.uid,
            gid: a.gid,
            rdev: a.rdev,
            blksize: 1 << 20, // = max_write：cp 等按它挑每次读写的大小
            flags: 0,
        };
        (ttl, fa)
    }

    /// 根目录的虚拟 xattr 的值：集群图里各节点的管理地址，逗号分隔。
    pub fn mgmt_addrs(&self) -> Vec<u8> {
        let m = self.fc.meta.map();
        let v: Vec<String> = m.nodes.iter().map(|(_, n)| n.mgmt_addr.clone()).filter(|a| !a.is_empty()).collect();
        v.join(",").into_bytes()
    }
}
