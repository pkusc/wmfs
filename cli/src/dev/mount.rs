//! `wmfs dev mount/umount/client-pid` 与 `wmfs dev down` 的挂载清理（design.md 7.2，roadmap M6）。
//!
//! - `dev mount <挂载点> [--key-file F] [--cache …]`：后台起一个 `wmfs mount <全部节点的 rpc 地址> <挂载点> --key-file <n1 的
//!   client.key>`（自己的进程组；stdout/stderr 与 `--log` 都写进 `<开发目录>/mounts/<名字>/mount.log`），记下挂载点与进程号，等到
//!   `/proc/self/mountinfo` 里出现这个 `fuse.wmfs` 挂载、**并且**日志里已经有完整的"已挂载 …"一行才返回（见 [`announced`]；进程
//!   退出或 30 s 超时 → 还挂着就 lazy 卸载、打印日志路径与最后一行、非 0 退出；实现注记（M7，ADR-0037 第 14 条）：这时只删记录里的
//!   pid 与挂载点，mount.log 留着给人看——`dev down` 时随记录目录一起删），然后把它打印的集群警告与"已挂载"转出来。
//!   记录目录的名字 = 挂载点的最后一段（重名时加 `-2`、`-3`……）；按挂载点找记录时扫一遍 `mounts/`，比对各自的 `mountpoint`
//!   文件。实现注记（M6）：7.2 的 `--node n`（客户端冒充那个节点所在的"主机"）随 M13 的 locality。
//! - `dev umount <挂载点>`：`fusermount3 -u`，等挂载进程退出（它写出全部脏数据后自己退出），删掉记录。
//! - `dev client-pid <挂载点>`：打印挂载进程的 pid（M10 用 `kill -STOP` 冻住它演示驱逐）。
//! - `dev down` 先清理挂载、再停节点（否则节点停了以后挂载点上的命令会卡到 `--op-timeout`）：对每个开发挂载进程依次 SIGCONT
//!   （被冻住的也能处理 SIGTERM）→ SIGTERM（它写出脏数据、自己卸载）→ 3 s 后仍在就 SIGKILL → 还挂着的（被 SIGKILL 的、卡住的）
//!   `fusermount3 -uz`（lazy 卸载：冻住或崩溃的挂载点上普通卸载会 EBUSY 或卡住）→ 删掉记录（7.2 的"安全清理顺序"）。
//!   只对记录里的、命令行确实是"wmfs mount … <这个挂载点>"的进程发信号（`dev/proc.rs`）。

use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::Command;

use wmfs_core::base::{clock, sys};

use super::proc::{live_pid, spawn_logged, wait_exit, wait_ready};
use super::{health, node_dir, nodes};
use crate::doctor::parse::parse_mountinfo;

const MOUNT_TIMEOUT_MS: u64 = 30_000;

/// 这个路径上有没有 fuse.wmfs 挂载。
pub fn mounted(mp: &Path) -> bool {
    let text = std::fs::read_to_string("/proc/self/mountinfo").unwrap_or_default();
    let want = mp.display().to_string();
    parse_mountinfo(&text).iter().any(|m| m.point == want && m.fstype == "fuse.wmfs")
}

/// 全部挂载记录：(记录目录, 记下的挂载点)。
fn records(d: &Path) -> Vec<(PathBuf, PathBuf)> {
    let dirs: Vec<PathBuf> =
        std::fs::read_dir(d.join("mounts")).map(|it| it.flatten().map(|e| e.path()).collect()).unwrap_or_default();
    dirs.into_iter()
        .map(|rec| {
            let mp = PathBuf::from(std::fs::read_to_string(rec.join("mountpoint")).unwrap_or_default());
            (rec, mp)
        })
        .collect()
}

/// 给挂载点 `mp` 新建一个记录目录：名字 = 挂载点的最后一段，已经有同名的就加 `-2`、`-3`……
fn new_record(d: &Path, mp: &Path) -> Result<PathBuf, String> {
    let base = mp.file_name().map(|s| s.to_string_lossy().into_owned()).unwrap_or_else(|| "root".into());
    let mut rec = d.join("mounts").join(&base);
    let mut k = 2;
    while rec.exists() {
        rec = d.join("mounts").join(format!("{base}-{k}"));
        k += 1;
    }
    std::fs::create_dir_all(&rec).map_err(|e| format!("建 {} 失败：{e}", rec.display()))?;
    Ok(rec)
}

/// 记录里的挂载进程还活着，且命令行确实是"wmfs mount … <挂载点>"。
fn mount_pid(rec: &Path, mp: &Path) -> Option<i32> {
    live_pid(&rec.join("pid"), "mount", mp)
}

fn canon(mp: &Path) -> Result<PathBuf, String> {
    std::fs::canonicalize(mp).map_err(|e| format!("挂载点 {}：{e}（先 mkdir -p）", mp.display()))
}

fn write_file(p: &Path, text: &str) -> Result<(), String> {
    sys::atomic_write(p, text.as_bytes()).map_err(|e| format!("写 {} 失败：{e}", p.display()))
}

pub fn mount(d: &Path, mp: &Path, key_file: &Option<PathBuf>, cache: &Option<String>) -> Result<Vec<String>, String> {
    let mp = canon(mp)?;
    if mounted(&mp) {
        return Err(format!("{} 上已经挂着 wmfs（先 wmfs dev umount）", mp.display()));
    }
    let mut addrs = Vec::new();
    for n in nodes(d) {
        let h = health(d, n).map_err(|e| format!("node{n} 没有响应（{e}）：先 wmfs dev up"))?;
        addrs.extend(h.node.addrs.iter().map(|(s, a)| format!("{s}://{a}")));
    }
    if addrs.is_empty() {
        return Err(format!("{} 里还没有开发集群：先 wmfs dev up", d.display()));
    }
    for (rec, _) in records(d).into_iter().filter(|(_, m)| *m == mp) {
        if let Some(pid) = mount_pid(&rec, &mp) {
            return Err(format!("{} 的挂载进程 {pid} 还在（先 wmfs dev umount）", mp.display()));
        }
        let _ = std::fs::remove_dir_all(rec); // 同一个挂载点上一次没清理掉的记录（挂载进程已经不在了）
    }
    let rec = new_record(d, &mp)?;
    let log_path = rec.join("mount.log");
    let key = key_file.clone().unwrap_or_else(|| node_dir(d, 1).join("keys/client.key"));
    let joined = addrs.join(",");
    let mut args: Vec<&OsStr> =
        vec![OsStr::new("mount"), OsStr::new(&joined), mp.as_os_str(), OsStr::new("--key-file"), key.as_os_str()];
    if let Some(c) = cache {
        args.extend([OsStr::new("--cache"), OsStr::new(c)]);
    }
    let mut child = spawn_logged(&args, &log_path)?;
    write_file(&rec.join("mountpoint"), &mp.display().to_string())?;
    write_file(&rec.join("pid"), &child.id().to_string())?;
    let read_log = || std::fs::read_to_string(&log_path).unwrap_or_default();
    let ready = || mounted(&mp) && announced(&read_log());
    if let Err(e) = wait_ready(&mut child, MOUNT_TIMEOUT_MS, &log_path, "wmfs mount", ready) {
        lazy_umount(&mp); // 挂上了却没等到"已挂载"（挂载进程已被结束）：不留下没有记录、没人管的挂载点
        // 只删 pid 与挂载点记录，mount.log 留着：错误信息里指着它（ADR-0037 第 14 条）；wmfs dev down 时随记录目录一起删
        let _ = std::fs::remove_file(rec.join("pid"));
        let _ = std::fs::remove_file(rec.join("mountpoint"));
        return Err(e);
    }
    let text = read_log();
    Ok(text.lines().filter(|l| l.starts_with("警告") || l.starts_with("已挂载")).map(String::from).collect())
}

/// 挂载进程已经把"已挂载 …"这一行完整写进了日志。只看 mountinfo 不够：挂载在 `Session::new` 里就出现在 mountinfo 里，
/// 集群警告与"已挂载"是紧接着才一次写出来的——只等 mountinfo，偶尔会读到还空着的日志，什么也转不出来。
pub fn announced(log: &str) -> bool {
    log.split_inclusive('\n').any(|l| l.starts_with("已挂载") && l.ends_with('\n'))
}

/// 还挂着就 lazy 卸载（`fusermount3 -uz`：冻住或崩溃的挂载点上普通卸载会 EBUSY 或卡住）；卸不掉也不报错。
fn lazy_umount(mp: &Path) {
    if mounted(mp) {
        let _ = Command::new("fusermount3").arg("-uz").arg(mp).status();
    }
}

/// 找到挂载点的记录（没有 → 错误）。
fn find(d: &Path, mp: &Path) -> Result<(PathBuf, PathBuf), String> {
    let mp = canon(mp)?;
    match records(d).into_iter().find(|(_, m)| *m == mp) {
        Some((rec, _)) => Ok((rec, mp)),
        None => Err(format!("{} 不是 wmfs dev mount 挂上的（{} 里没有记录）", mp.display(), d.display())),
    }
}

pub fn umount(d: &Path, mp: &Path) -> Result<Vec<String>, String> {
    let (rec, mp) = find(d, mp)?;
    if mounted(&mp) {
        let st = Command::new("fusermount3")
            .arg("-u")
            .arg(&mp)
            .status()
            .map_err(|e| format!("运行 fusermount3 失败：{e}"))?;
        if !st.success() {
            return Err(format!("fusermount3 -u {} 失败（有进程还在用它？）", mp.display()));
        }
    }
    if let Some(pid) = mount_pid(&rec, &mp)
        && !wait_exit(pid, 10_000)
    {
        let log = rec.join("mount.log");
        return Err(format!("已卸载 {}，但挂载进程 {pid} 10 s 后还没退出（日志 {}）", mp.display(), log.display()));
    }
    let _ = std::fs::remove_dir_all(&rec);
    Ok(vec![format!("已卸载 {}", mp.display())])
}

pub fn client_pid(d: &Path, mp: &Path) -> Result<Vec<String>, String> {
    let (rec, mp) = find(d, mp)?;
    let pid = mount_pid(&rec, &mp).ok_or_else(|| format!("{} 的挂载进程已经不在了", mp.display()))?;
    Ok(vec![pid.to_string()])
}

/// `dev down` 的第一步：按"安全清理顺序"清理全部开发挂载（见文件头）。返回给人看的行。
pub fn stop_all(d: &Path) -> Vec<String> {
    let recs = records(d);
    let mut pids = Vec::new();
    for (rec, mp) in &recs {
        if let Some(pid) = mount_pid(rec, mp) {
            let _ = sys::send_signal(pid, sys::SIGCONT);
            let _ = sys::send_signal(pid, sys::SIGTERM);
            pids.push(pid);
        }
    }
    let deadline = clock::mono_ms() + 3000;
    for p in &pids {
        if !wait_exit(*p, deadline.saturating_sub(clock::mono_ms())) {
            let _ = sys::send_signal(*p, sys::SIGKILL);
        }
    }
    let mut lines = Vec::new();
    for (rec, mp) in &recs {
        let _ = std::fs::remove_dir_all(rec);
        if !mp.as_os_str().is_empty() {
            lazy_umount(mp); // 没有挂载点的是挂载失败留下的日志（见 mount）
            lines.push(format!("已卸载 {}", mp.display()));
        }
    }
    lines
}
