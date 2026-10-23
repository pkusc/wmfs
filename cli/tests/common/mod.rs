//! FUSE 端到端测试的公共部分（design.md 9.5 的单节点版 + 真的挂载）：在临时目录里起一个完整节点（wmfs-testkit 的
//! `harness::node`：同一个测试进程里 `init_node` + `server::start`，端口随机），再用真的 `wmfs mount` 子进程挂上它
//! （`CARGO_BIN_EXE_wmfs`，cargo 保证它是最新的）。每个挂载点在测试自己的临时目录里；`Mount` 被丢弃时（包括测试失败 panic 时）
//! 一定卸载（`fusermount3 -u`，不行再 `-uz`）并结束挂载进程——不留挂载、不留进程。
//! 实现注记（M6）：FUSE 测试放在 cli 包的 `tests/` 里（不是 wmfs-testkit），因为只有二进制所在的包能拿到 `CARGO_BIN_EXE_wmfs`。
#![allow(dead_code)] // 每个测试文件只用到其中一部分

use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use wmfs_core::proto::meta::{AttrOut, MetaOp, MetaReply};
use wmfs_core::server::Running;
use wmfs_testkit::harness::node::{client, init, node_dir, start, who};

/// 一个完整节点。丢弃时（包括测试失败 panic 时）停掉服务端、删掉目录（挂载点在目录里：`Mount` 要先丢弃，它们后声明、先丢弃）。
pub struct Node {
    pub dir: PathBuf,
    pub run: Option<Running>,
}

impl Node {
    pub fn start(name: &str) -> Node {
        let dir = std::env::temp_dir().join(format!("wmfs-fuse-{name}-{}", std::process::id()));
        let mut n = Node { dir, run: None }; // 先有 Node：起不来（panic）时 Drop 也会删掉目录
        init(&n.dir);
        n.run = Some(start(&n.dir));
        n
    }

    pub fn rpc(&self) -> String {
        let (s, a) = &self.run.as_ref().unwrap().addrs[0];
        format!("{s}://{a}")
    }

    /// 挂上一个客户端（`env`：给挂载进程的环境变量，例如 `WMFS_FAILPOINTS`、`WMFS_LOG`）。
    pub fn mount(&self, name: &str, env: &[(&str, &str)]) -> Mount {
        let mp = self.dir.join(name);
        std::fs::create_dir_all(&mp).unwrap();
        let log = self.dir.join(format!("{name}.log"));
        let key = node_dir(&self.dir).join("keys/client.key");
        let err = File::create(self.dir.join(format!("{name}.stderr"))).unwrap();
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_wmfs"));
        cmd.arg("mount").arg(self.rpc()).arg(&mp).arg("--key-file").arg(&key).arg("--log").arg(&log);
        cmd.args(["--cache", "memory:64M", "--op-timeout", "30s"]).env_remove("WMFS_FAILPOINTS");
        cmd.envs(env.iter().copied()).stdin(Stdio::null()).stdout(Stdio::null()).stderr(err);
        let mut child = cmd.spawn().unwrap();
        let mp = std::fs::canonicalize(&mp).unwrap();
        let t0 = Instant::now();
        while !mounted(&mp) {
            if child.try_wait().unwrap().is_some() || t0.elapsed() > Duration::from_secs(30) {
                let _ = child.kill();
                let text = std::fs::read_to_string(self.dir.join(format!("{name}.stderr"))).unwrap_or_default();
                panic!("wmfs mount 没有挂上 {}：{text}", mp.display());
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        Mount { mp, log, child: Some(child) }
    }

    /// 元数据里这个文件的属性（按文件系统内的路径找；用 admin key、uid 0）。
    pub fn meta_attr(&self, path: &str) -> AttrOut {
        let c = client(&self.dir, self.run.as_ref().unwrap(), "admin");
        let me = who(0, 0);
        let MetaReply::Resolved { ino, .. } = c.meta.read(&me, MetaOp::ResolvePath { path: path.into() }).unwrap()
        else {
            panic!("找不到 {path}")
        };
        let MetaReply::Entry { attr } = c.meta.read(&me, MetaOp::GetAttr { ino }).unwrap() else { panic!() };
        c.meta.close();
        attr
    }

    /// 停掉节点、删掉目录（挂载要先卸掉）。
    pub fn stop(self) {
        drop(self);
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        if let Some(r) = self.run.take() {
            r.stop();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// 一个挂载点（挂载进程是子进程）。
pub struct Mount {
    pub mp: PathBuf,
    pub log: PathBuf,
    child: Option<Child>,
}

impl Mount {
    pub fn p(&self, rel: &str) -> PathBuf {
        self.mp.join(rel)
    }

    pub fn pid(&self) -> u32 {
        self.child.as_ref().unwrap().id()
    }

    /// 挂载进程的日志（`--log`）。
    pub fn log_text(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }

    /// 正常卸载（`wmfs umount`），等挂载进程写完数据退出。卸载回 EBUSY 时过一会儿再试：同一个测试进程里别的用例的线程在 fork
    /// 子进程，子进程在 exec 之前的一瞬间还拿着从这里继承来的 fd（例如刚打开的文件），这期间挂载点算"忙"。
    pub fn unmount(mut self) {
        let umount = || Command::new(env!("CARGO_BIN_EXE_wmfs")).arg("umount").arg(&self.mp).status().unwrap();
        assert!(wait_for(10, || umount().success()), "wmfs umount 失败");
        let mut c = self.child.take().unwrap();
        let t0 = Instant::now();
        while c.try_wait().unwrap().is_none() {
            assert!(t0.elapsed() < Duration::from_secs(30), "卸载之后挂载进程 30 s 还没退出");
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Drop for Mount {
    fn drop(&mut self) {
        if mounted(&self.mp) {
            let ok = Command::new("fusermount3").arg("-u").arg(&self.mp).status().is_ok_and(|s| s.success());
            if !ok {
                let _ = Command::new("fusermount3").arg("-uz").arg(&self.mp).status();
            }
        }
        if let Some(mut c) = self.child.take() {
            let t0 = Instant::now();
            while c.try_wait().ok().flatten().is_none() && t0.elapsed() < Duration::from_secs(10) {
                std::thread::sleep(Duration::from_millis(20));
            }
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

/// /proc/self/mountinfo 里这个路径上有没有 fuse.wmfs 挂载。
pub fn mounted(mp: &Path) -> bool {
    let want = mp.display().to_string();
    std::fs::read_to_string("/proc/self/mountinfo").unwrap_or_default().lines().any(|l| {
        let (left, right) = l.split_once(" - ").unwrap_or(("", ""));
        left.split(' ').nth(4) == Some(want.as_str()) && right.starts_with("fuse.wmfs ")
    })
}

/// 在 `dir` 里跑一条 shell 命令，返回 (退出码, stdout+stderr)。
pub fn sh(dir: &Path, cmd: &str) -> (i32, String) {
    let o = Command::new("sh").arg("-c").arg(cmd).current_dir(dir).output().unwrap();
    let mut s = String::from_utf8_lossy(&o.stdout).into_owned();
    s.push_str(&String::from_utf8_lossy(&o.stderr));
    (o.status.code().unwrap_or(-1), s)
}

/// 确定性的伪随机字节。
pub fn bytes(seed: u64, n: usize) -> Vec<u8> {
    let mut v = vec![0u8; n];
    wmfs_core::base::rand::XorShift64::new(seed).fill(&mut v);
    v
}

/// 读整个文件（pread，不经过 std 的缓冲）。
pub fn read_all(p: &Path) -> Vec<u8> {
    let f = File::open(p).unwrap();
    let n = f.metadata().unwrap().len() as usize;
    let mut v = vec![0u8; n];
    f.read_exact_at(&mut v, 0).unwrap();
    v
}

/// 等 `f()` 为真，最多 `secs` 秒。
pub fn wait_for(secs: u64, mut f: impl FnMut() -> bool) -> bool {
    let t0 = Instant::now();
    while t0.elapsed() < Duration::from_secs(secs) {
        if f() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    f()
}
