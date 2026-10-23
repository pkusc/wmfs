//! `wmfs dev` 管的后台进程（design.md 7.2）：节点（`wmfs server`）与开发挂载（`wmfs mount`）共用的几步——在自己的进程组里后台起
//! 一个 `wmfs` 子命令、等它就绪、认出"这是我们起的进程"、等它退出。
//!
//! 安全：只认 pid 文件里、命令行确实是"wmfs <子命令> … <这个路径>"的进程（防止 pid 被别的进程复用之后误发信号）。

use std::ffi::OsStr;
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use wmfs_core::base::{clock, sys};

/// 在自己的进程组里后台起 `wmfs <args>`（终端里按 Ctrl-C 不会带走它），stdout/stderr 追加到 `log`。
pub fn spawn_logged(args: &[&OsStr], log: &Path) -> Result<Child, String> {
    let open = || std::fs::OpenOptions::new().create(true).append(true).open(log);
    let err = |e: std::io::Error| format!("打开日志 {} 失败：{e}", log.display());
    let (out, errf) = (open().map_err(err)?, open().map_err(err)?);
    let exe = std::env::current_exe().map_err(|e| format!("找不到 wmfs 可执行文件：{e}"))?;
    let what = args.first().map(|a| a.to_string_lossy().into_owned()).unwrap_or_default();
    Command::new(exe)
        .args(args)
        .stdin(Stdio::null())
        .stdout(out)
        .stderr(errf)
        .process_group(0)
        .spawn()
        .map_err(|e| format!("启动 wmfs {what} 失败：{e}"))
}

/// 等 `ready()` 成立（每 50 ms 看一次）。子进程先退出了、或过了 `timeout_ms` → 结束它，报错（带日志路径与日志的最后一行）。
pub fn wait_ready(
    child: &mut Child,
    timeout_ms: u64,
    log: &Path,
    what: &str,
    mut ready: impl FnMut() -> bool,
) -> Result<(), String> {
    let deadline = clock::mono_ms() + timeout_ms;
    loop {
        if ready() {
            return Ok(());
        }
        // 用 try_wait 而不是 kill(pid, 0)：退出了但还没被回收的子进程（僵尸）在 kill(pid, 0) 看来仍然"活着"
        let exited = child.try_wait().ok().flatten().is_some();
        if exited || clock::mono_ms() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            let why = if exited {
                "启动后退出了".to_string()
            } else {
                format!("没能在 {} s 内就绪（已停止它）", timeout_ms / 1000)
            };
            let text = std::fs::read_to_string(log).unwrap_or_default();
            let last = text.lines().last().unwrap_or_default();
            return Err(format!("{what} {why}，请看日志 {}（最后一行：{last}）", log.display()));
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// `pid_file` 里记的进程还活着，且命令行里有子命令 `sub`（"server" / "mount"）与路径 `path`。
pub fn live_pid(pid_file: &Path, sub: &str, path: &Path) -> Option<i32> {
    let pid: i32 = std::fs::read_to_string(pid_file).ok()?.trim().parse().ok()?;
    let cmdline = std::fs::read(format!("/proc/{pid}/cmdline")).ok()?;
    let args: Vec<&[u8]> = cmdline.split(|b| *b == 0).collect();
    let ours = args.contains(&sub.as_bytes()) && args.contains(&path.as_os_str().as_encoded_bytes());
    (ours && sys::process_alive(pid)).then_some(pid)
}

/// 等进程 `pid` 退出，最多 `ms` 毫秒；返回它是否已经不在了。
pub fn wait_exit(pid: i32, ms: u64) -> bool {
    let deadline = clock::mono_ms() + ms;
    while sys::process_alive(pid) && clock::mono_ms() < deadline {
        std::thread::sleep(Duration::from_millis(50));
    }
    !sys::process_alive(pid)
}
