//! `wmfs doctor`：环境体检——告诉你这台机器能测什么（roadmap M0、design.md 6.2、9.6、附录 B）。
//!
//! 每一项：✓ 可以；! 能用但有限制或提示（相关测试会显示 SKIP 或降级）；✗ 缺失（相关测试无法运行）。
//! 只读系统信息（/proc、/sys、/etc、环境变量），不改任何系统配置，也不提示任何 sudo 操作。
//! 有 ✗ 时退出码为 1，否则为 0。`--json` 输出给 scripts/accept.sh 解析。解析文本的纯函数在 [`parse`]。

pub(crate) mod parse;
mod rdma;

use std::ffi::OsStr;
use std::fmt::Write as _;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Duration;

use serde::Serialize;
use wmfs_core::base::{clock, devinfo, sys};
use wmfs_core::buf::PoolConfig;

use parse::{fuse_conf_allows_other, list_dir, name_of, nvme_ctrl, parse_kernel, parse_mountinfo, uid_from_status};

#[derive(Serialize, Clone, Copy, PartialEq, Eq, Debug)]
#[serde(rename_all = "lowercase")]
enum Status {
    Ok,
    Warn,
    Fail,
}

#[derive(Serialize, Debug)]
struct Check {
    id: &'static str,
    status: Status,
    title: String,  // 一行结论（给人看，也进 JSON）
    detail: String, // 补充说明：影响什么、下一步怎么办
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<serde_json::Value>, // 机读的补充数据（目前只有 rdma 一项：GID 候选，accept.sh 用）
}

fn mk(id: &'static str, status: Status, title: impl Into<String>, detail: impl Into<String>) -> Check {
    Check { id, status, title: title.into(), detail: detail.into(), data: None }
}
fn ok(id: &'static str, title: impl Into<String>, detail: impl Into<String>) -> Check {
    mk(id, Status::Ok, title, detail)
}
fn warn(id: &'static str, title: impl Into<String>, detail: impl Into<String>) -> Check {
    mk(id, Status::Warn, title, detail)
}
fn fail(id: &'static str, title: impl Into<String>, detail: impl Into<String>) -> Check {
    mk(id, Status::Fail, title, detail)
}

#[derive(Serialize)]
struct Report {
    version: String,
    ok: bool, // 没有 ✗
    checks: Vec<Check>,
}

pub fn run(json: bool) -> i32 {
    let mounts = parse_mountinfo(&read("/proc/self/mountinfo"));
    let root_devs = devinfo::phys_devs(Path::new("/"));
    let checks = vec![
        kernel(),
        fuse_dev(Path::new("/dev/fuse")),
        fusermount(&std::env::var_os("PATH").unwrap_or_default()),
        memlock(),
        rootfs(&mounts),
        rootdisk(&root_devs),
        proxy(),
        rdma_check(),
        cgroup_io(),
        fdp(&root_devs),
        docker(),
        allow_other(),
        stale_mounts(&mounts),
    ];
    let code = exit_code(&checks);
    let report = Report { version: format!("wmfs {}", crate::VERSION), ok: code == 0, checks };
    let text = if json { serde_json::to_string_pretty(&report).unwrap_or_default() + "\n" } else { render(&report) };
    match crate::write_stdout(&text) {
        Ok(()) => code,
        Err(c) => c,
    }
}

/// 有任何一项 ✗ → 1；只有 ✓ 与 ! → 0。
fn exit_code(checks: &[Check]) -> i32 {
    if checks.iter().any(|c| c.status == Status::Fail) { 1 } else { 0 }
}

/// 给人看的检查表。
fn render(report: &Report) -> String {
    let count = |s: Status| report.checks.iter().filter(|c| c.status == s).count();
    let mut out = format!("{} 环境体检（✓ 可以 / ! 能用但有限制或提示 / ✗ 缺失）\n", report.version);
    for c in &report.checks {
        let mark = match c.status {
            Status::Ok => "✓",
            Status::Warn => "!",
            Status::Fail => "✗",
        };
        let _ = writeln!(out, "  {mark} {}", c.title);
        if !c.detail.is_empty() {
            let _ = writeln!(out, "      {}", c.detail);
        }
    }
    let (n_ok, n_warn, n_fail) = (count(Status::Ok), count(Status::Warn), count(Status::Fail));
    let _ = writeln!(out, "汇总：✓ {n_ok} 项，! {n_warn} 项，✗ {n_fail} 项。");
    out
}

/// 读一个系统文件；读不到 = 空字符串（各项检查自己决定空内容意味着什么）。
fn read(path: impl AsRef<Path>) -> String {
    std::fs::read_to_string(path).unwrap_or_default()
}

fn kernel() -> Check {
    let rel = read("/proc/sys/kernel/osrelease").trim().to_string();
    match parse_kernel(&rel) {
        Some((a, b)) if (a, b) >= (5, 10) => ok("kernel", format!("内核 {a}.{b}"), rel),
        Some((a, b)) => warn("kernel", format!("内核 {a}.{b} 较旧"), "建议 ≥ 5.10，FUSE 的部分能力可能不可用"),
        None => warn("kernel", "内核版本未知", format!("无法解析 /proc/sys/kernel/osrelease：{rel:?}")),
    }
}

/// `dev` 平时是 /dev/fuse（测试时传一个不存在的路径，检查 ✗ 分支）。
fn fuse_dev(dev: &Path) -> Check {
    match std::fs::OpenOptions::new().read(true).write(true).open(dev) {
        Ok(_) => ok("fuse_dev", format!("{} 可读写", dev.display()), "FUSE 挂载（M6 起）需要它"),
        Err(e) => fail("fuse_dev", format!("{} 不可用", dev.display()), format!("{e}：没有它就不能挂载（M6 起需要）")),
    }
}

/// 在 `path_var`（平时是环境变量 PATH）里找 fusermount3。
fn fusermount(path_var: &OsStr) -> Check {
    use std::os::unix::fs::PermissionsExt;
    let found = std::env::split_paths(path_var).map(|d| d.join("fusermount3")).find(|f| f.is_file());
    match found {
        None => fail("fusermount3", "找不到 fusermount3", "普通用户无法挂载 FUSE（Ubuntu 的 fuse3 包提供它）"),
        Some(f) => match std::fs::metadata(&f).map(|m| m.permissions().mode() & 0o4000 != 0) {
            Ok(true) => ok("fusermount3", "fusermount3", format!("{}，带 setuid 位，普通用户可以挂载", f.display())),
            _ => fail("fusermount3", "fusermount3 没有 setuid 位", format!("{}：普通用户无法挂载", f.display())),
        },
    }
}

fn memlock() -> Check {
    // RDMA 要把整个服务端内存池注册（锁定）进网卡（design.md 3.6.1）
    let need = PoolConfig::server().total_bytes();
    match sys::memlock_limit() {
        Ok(None) => ok("memlock", "memlock 不限", "RDMA 注册整个内存池够用"),
        Ok(Some(b)) if b >= need => ok(
            "memlock",
            format!("memlock ≈{} GiB", (b as f64 / (1u64 << 30) as f64).round()),
            format!("RDMA 注册整个服务端内存池（{} MiB）够用", need >> 20),
        ),
        Ok(Some(b)) => warn(
            "memlock",
            format!("memlock 只有 {} MiB", b >> 20),
            format!("只影响 RDMA：注册服务端内存池需要 {} MiB", need >> 20),
        ),
        Err(e) => warn("memlock", "memlock 读取失败", e.to_string()),
    }
}

fn rootfs(mounts: &[(String, String)]) -> Check {
    let fs = mounts.iter().rev().find(|(mp, _)| mp == "/").map(|(_, t)| t.as_str()).unwrap_or("未知");
    match fs {
        "xfs" => ok("rootfs", "根分区 xfs", "推荐的文件系统（ADR-0008）"),
        "ext4" => {
            warn("rootfs", "根分区 ext4", "提示：XFS 更推荐；ext4 也满足 rename 原子替换，可以正常使用（ADR-0008）")
        }
        other => warn("rootfs", format!("根分区 {other}"), "只在 ext4 与 XFS 上测试过"),
    }
}

fn rootdisk(devs: &[String]) -> Check {
    if devs.is_empty() {
        warn(
            "rootdisk",
            "根分区所在物理盘：unknown",
            "解析不出物理盘（loop 或虚拟文件系统）：隔离承诺一律按共享设备显示",
        )
    } else {
        let d = devs.join("、");
        ok("rootdisk", format!("根分区所在物理盘 {d}"), "隔离承诺按物理盘计算；同一块盘上的 target 只承诺份额（6.3）")
    }
}

fn proxy() -> Check {
    let set: Vec<&str> = ["http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY"]
        .into_iter()
        .filter(|k| std::env::var(k).is_ok_and(|v| !v.is_empty()))
        .collect();
    if set.is_empty() {
        return ok("proxy", "没有设置 http_proxy", "");
    }
    let np = std::env::var("no_proxy").or_else(|_| std::env::var("NO_PROXY")).unwrap_or_default();
    let tail = if np.contains("127.0.0.1") {
        "；no_proxy 已包含 127.0.0.1"
    } else {
        "；或按 roadmap §0 设置 no_proxy"
    };
    warn(
        "proxy",
        "http_proxy 已设置（wmfs 访问本机时已自动绕过代理）",
        format!("{}；自己用 curl 访问 127.0.0.1 时请加 --noproxy '*'{tail}", set.join("、")),
    )
}

/// RDMA 一项：识别 rxe 设备、列出 RoCE v2 的 IPv4 GID 候选（doctor/rdma.rs）。
fn rdma_check() -> Check {
    rdma::rdma_at(Path::new(rdma::SYSFS))
}

fn cgroup_io() -> Check {
    let uid = uid_from_status(&read("/proc/self/status")).unwrap_or(0);
    let path = format!("/sys/fs/cgroup/user.slice/user-{uid}.slice/user@{uid}.service/cgroup.subtree_control");
    match std::fs::read_to_string(&path) {
        Ok(s) if s.split_whitespace().any(|c| c == "io") => {
            ok("cgroup_io", "cgroup io 已委派给用户", "首发仍不做设备出口隔离（ADR-0011）")
        }
        Ok(s) => warn(
            "cgroup_io",
            "cgroup io 未委派给用户（设备出口隔离需要 root）",
            format!("当前委派：{}；首发不做 cgroup io 隔离（ADR-0011）", s.trim()),
        ),
        Err(_) => warn("cgroup_io", "无法确定 cgroup io 的委派情况", format!("读不到 {path}")),
    }
}

fn fdp(devs: &[String]) -> Check {
    let Some(d) = devs.first() else {
        return warn("fdp", "FDP：未知", "根分区的物理盘解析不出来");
    };
    let ns = nvme_ctrl(d)
        .map(|c| {
            let n =
                list_dir(Path::new("/sys/block")).iter().filter(|p| name_of(p).starts_with(&format!("{c}n"))).count();
            format!("，控制器 {c} 有 {n} 个 namespace")
        })
        .unwrap_or_default();
    match read(format!("/sys/block/{d}/queue/max_write_streams")).trim().parse::<u64>() {
        Ok(0) => {
            warn("fdp", "FDP：不支持", format!("{d} 的 max_write_streams = 0{ns}；硬件隔离只做整盘独占（ADR-0011）"))
        }
        Ok(k) => ok("fdp", format!("FDP/写流：支持（max_write_streams = {k}）"), format!("{d}{ns}")),
        Err(_) => warn("fdp", "FDP：未知", format!("{d} 没有 max_write_streams（内核较老或不是 NVMe）")),
    }
}

fn docker() -> Check {
    let mut cmd = Command::new("docker");
    cmd.args(["version", "--format", "{{.Server.Version}}"]);
    match run_with_timeout(cmd, Duration::from_secs(5)) {
        Some(v) if !v.is_empty() => ok("docker", "docker 可用", format!("服务端 {v}；M6 起用它在容器里跑外部测试套件")),
        _ => warn("docker", "docker 不可用", "M6 起的外部测试套件（pjdfstest、fio、fsx）需要它"),
    }
}

fn allow_other() -> Check {
    if fuse_conf_allows_other(&read("/etc/fuse.conf")) {
        ok("allow_other", "user_allow_other 已开启", "其他用户可以访问你的 FUSE 挂载")
    } else {
        warn(
            "allow_other",
            "user_allow_other 未开启（用 --as-uid 演示多用户）",
            "多用户权限演示用 wmfs fs --as-uid（M4 起）与容器，不需要改 /etc/fuse.conf",
        )
    }
}

fn stale_mounts(mounts: &[(String, String)]) -> Check {
    let left: Vec<&str> = mounts.iter().filter(|(_, t)| t == "fuse.wmfs").map(|(m, _)| m.as_str()).collect();
    if left.is_empty() {
        return ok("stale_mounts", "没有残留的 wmfs 挂载", "");
    }
    warn(
        "stale_mounts",
        format!("有 {} 个残留的 wmfs 挂载：{}", left.len(), left.join("、")),
        "清理：fusermount3 -uz <挂载点>（M6 起也可以用 wmfs dev down --purge）",
    )
}

/// 运行一个命令，最多等 `limit`（按 `clock::mono_ms` 计时，design.md 0.2 第 6 条）；成功退出时返回去掉首尾空白的 stdout。
fn run_with_timeout(mut cmd: Command, limit: Duration) -> Option<String> {
    let mut child = cmd.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::null()).spawn().ok()?;
    let deadline = clock::mono_ms() + limit.as_millis() as u64;
    loop {
        match child.try_wait() {
            Ok(Some(_)) => break,
            Ok(None) if clock::mono_ms() < deadline => std::thread::sleep(Duration::from_millis(20)),
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
        }
    }
    let out = child.wait_with_output().ok()?;
    out.status.success().then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
}

#[cfg(test)]
mod tests;
