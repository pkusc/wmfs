//! `wmfs dev mgr`（design.md 7.2、7.4）：后台起一个 `wmfs mgr`（全部开发节点的管理地址、node1 的 admin key、监听 [`MGR_LISTEN`]、
//! 目录 `<开发目录>/mgr`——就是 `wmfs dev env` 导出的 `WMFS_MGR_DIR`），输出追加到 `<开发目录>/mgr/mgr.log`；等它就绪（带 token 问
//! `GET /alerts` 有回复）才返回，最后一行打印带 token 的 dashboard 网址。已经在运行就只打印网址。`wmfs dev up --profile nas-safe` 在集群
//! 起好之后自动调用它（9.7：完整的 nas-safe 含 mgr 与审计日志）；`wmfs dev down` 先停它（SIGTERM，5 s 后仍在就 SIGKILL）。
//! 实现注记（M14）：端口 19090 与开发节点的任何 19n00 / 19n01 / 19n80（n = 1..9）都不冲突（`wmfs mgr` 自己的缺省是 127.0.0.1:9500，
//! 那是生产的写法）。只认 pid 文件里、命令行确实是"wmfs mgr … --dir <这个目录>"的进程（`proc::live_pid`）。同时有两个 `wmfs dev mgr`
//! 时只有一个 mgr 起得来（端口被占用）：另一个看到本集群的 mgr 已经在运行，就照常打印网址、返回成功。

use std::ffi::OsStr;
use std::path::{Path, PathBuf};

use wmfs_core::base::sys;
use wmfs_mgr::alert::{AlertsReply, fetch};

use super::proc::{live_pid, spawn_logged, wait_exit, wait_ready};
use super::{mgmt_addr, node_dir, nodes};

/// `wmfs dev mgr` 起的 mgr 监听的地址（见文件头）。
pub const MGR_LISTEN: &str = "127.0.0.1:19090";

fn dir(d: &Path) -> PathBuf {
    d.join("mgr")
}

fn running(d: &Path) -> Option<i32> {
    live_pid(&dir(d).join("mgr.pid"), "mgr", &dir(d))
}

/// mgr 的 dashboard token（`<目录>/token`）。
fn token(d: &Path) -> Result<String, String> {
    let p = dir(d).join("token");
    std::fs::read_to_string(&p).map(|t| t.trim().to_string()).map_err(|e| format!("读 {} 失败：{e}", p.display()))
}

/// mgr 的 `GET /alerts`（`wmfs dev wait alert` 用；只要 dashboard token）。
pub fn alerts(d: &Path) -> Result<AlertsReply, String> {
    fetch(&format!("http://{MGR_LISTEN}"), &token(d)?)
}

pub fn start(d: &Path) -> Result<Vec<String>, String> {
    let (md, log) = (dir(d), dir(d).join("mgr.log"));
    if running(d).is_none() {
        std::fs::create_dir_all(&md).map_err(|e| format!("建目录 {} 失败：{e}", md.display()))?;
        let addrs: Vec<String> = nodes(d).iter().filter_map(|n| mgmt_addr(d, *n).ok()).collect();
        let (addrs, key) = (addrs.join(","), node_dir(d, 1).join("keys/admin.key"));
        let args: [&OsStr; 9] = [
            "mgr".as_ref(),
            "--cluster".as_ref(),
            addrs.as_ref(),
            "--listen".as_ref(),
            MGR_LISTEN.as_ref(),
            "--dir".as_ref(),
            md.as_os_str(),
            "--key-file".as_ref(),
            key.as_os_str(),
        ];
        let mut child = spawn_logged(&args, &log)?;
        let pid = child.id() as i32;
        let started = wait_ready(&mut child, 30_000, &log, "mgr", || running(d) == Some(pid) && alerts(d).is_ok());
        let other = running(d).is_some_and(|p| p != pid); // 同时起了两个：另一个起的 mgr 在运行（见文件头），照样能用
        if !other {
            started?;
        }
    }
    let t = token(d)?;
    Ok(vec![format!("mgr 在运行（日志 {}）", log.display()), format!("dashboard：http://{MGR_LISTEN}/?token={t}")])
}

/// 停掉 `wmfs dev mgr` 起的 mgr（没有在运行 → None）。
pub fn stop(d: &Path) -> Option<String> {
    let pid = running(d)?;
    let _ = sys::send_signal(pid, sys::SIGTERM);
    if !wait_exit(pid, 5000) {
        let _ = sys::send_signal(pid, sys::SIGKILL);
        wait_exit(pid, 3000);
    }
    let _ = std::fs::remove_file(dir(d).join("mgr.pid"));
    Some("mgr 已停止".into())
}
