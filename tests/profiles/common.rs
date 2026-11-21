//! profile 测试共用的部分（design.md 9.5 的工作负载检查器 ②–⑤ 与开发集群的小工具；M15 定稿时从 nas_safe.rs 拆出来，minimal.rs、
//! hpc_rdma.rs 也用，每个 profile 只用其中一部分）。每个检查器都经两个挂载点（两个客户端）做，`a`、`b` 是两个挂载上的同一个目录；
//! `dev_dir` 是开发集群的目录（`WMFS_DEV_DIR`），`fs_path` 是文件系统里的路径（例如 `/home/q`）。

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// 跑一条 `wmfs …`（开发集群 `dev_dir`、它的 `nodes` 个节点、node1 的 admin key），返回是否成功与 stdout + stderr。
pub fn run(dev_dir: &Path, nodes: u32, args: &[&str]) -> (bool, String) {
    let cluster: Vec<String> = (1..=nodes).map(|n| format!("127.0.0.1:19{n}80")).collect();
    let o = Command::new(env!("CARGO_BIN_EXE_wmfs"))
        .args(args)
        .env("WMFS_DEV_DIR", dev_dir)
        .env("WMFS_CLUSTER", cluster.join(","))
        .env("WMFS_KEY_FILE", dev_dir.join("n1/keys/admin.key"))
        .env("no_proxy", "127.0.0.1,localhost")
        .output()
        .unwrap();
    (o.status.success(), format!("{}{}", String::from_utf8_lossy(&o.stdout), String::from_utf8_lossy(&o.stderr)))
}

/// 同 [`run`]，失败就断言失败；返回 stdout + stderr。
pub fn wmfs(dev_dir: &Path, nodes: u32, args: &[&str]) -> String {
    let (ok, text) = run(dev_dir, nodes, args);
    assert!(ok, "wmfs {args:?} 失败：{text}");
    text
}

/// 无论成败，结束时停掉这个开发集群（先卸载挂载点、停 mgr，再停节点）并删掉目录。
pub struct Cluster(pub PathBuf);

impl Drop for Cluster {
    fn drop(&mut self) {
        let _ = Command::new(env!("CARGO_BIN_EXE_wmfs"))
            .args(["dev", "down", "--purge"])
            .env("WMFS_DEV_DIR", &self.0)
            .status();
    }
}

/// ② rename 原子性（strong 目录 `a`/`b`）：一个挂载上把文件在 x、y 两个名字之间来回 rename，另一个挂载一直列这个目录——每次都恰好看到
/// 其中一个名字（不会两个都在、也不会一个都没有）。
pub fn rename_is_atomic(a: &Path, b: &Path) {
    std::fs::write(a.join("x"), b"r").unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let t = {
        let (a, stop) = (a.to_path_buf(), stop.clone());
        std::thread::spawn(move || {
            let mut n = 0;
            while !stop.load(Ordering::Relaxed) {
                let (from, to) = if n % 2 == 0 { ("x", "y") } else { ("y", "x") };
                std::fs::rename(a.join(from), a.join(to)).unwrap();
                n += 1;
            }
            n
        })
    };
    let t0 = Instant::now();
    let mut looks = 0;
    while t0.elapsed() < Duration::from_secs(4) {
        let names: Vec<String> =
            std::fs::read_dir(b).unwrap().map(|e| e.unwrap().file_name().to_string_lossy().into_owned()).collect();
        let seen = names.iter().filter(|n| *n == "x" || *n == "y").count();
        assert_eq!(seen, 1, "② 另一个挂载应恰好看到 x、y 之一：{names:?}");
        looks += 1;
    }
    stop.store(true, Ordering::Relaxed);
    println!("② rename 原子：{} 次 rename、{looks} 次列目录，每次恰好一个名字", t.join().unwrap());
}

/// ③ strong 追加：两个挂载各自以 O_APPEND 追加 `n` 行 `(写者, 序号)`（每行一次 write）——结果正好 2n 行，没有重叠、没有丢失，
/// 每个写者的行按序号排列。
pub fn strong_append(a: &Path, b: &Path, n: usize) {
    let path = |m: &Path| m.join("log");
    std::fs::File::create(path(a)).unwrap();
    let ts: Vec<_> = [("A", path(a)), ("B", path(b))]
        .into_iter()
        .map(|(w, p)| {
            std::thread::spawn(move || {
                let mut f = std::fs::OpenOptions::new().append(true).open(p).unwrap();
                for i in 0..n {
                    f.write_all(format!("{w} {i:06}\n").as_bytes()).unwrap();
                }
            })
        })
        .collect();
    ts.into_iter().for_each(|t| t.join().unwrap());
    let text = std::fs::read_to_string(path(b)).unwrap();
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 2 * n, "③ 两个挂载各追加 {n} 行");
    for w in ["A", "B"] {
        let seq: Vec<usize> =
            lines.iter().filter(|l| l.starts_with(w)).map(|l| l[2..].parse().expect("③ 行是完整的")).collect();
        assert_eq!(seq, (0..n).collect::<Vec<_>>(), "③ 写者 {w} 的行按序号、不丢不重");
    }
    println!("③ strong 追加：{} 行，不丢不乱", lines.len());
}

/// ④ 锁互斥（9.5）：两个挂载上各一个进程，fd 一直开着，在 F_SETLKW（`fcntl` 的字节范围锁；`cli/tests/lock_counter.py`，与
/// `scripts/accept.d/m12.sh` 共用）的保护下把同一个计数器各加 `n` 次——结果恰好 2n（锁是集群范围的，也是同步点：拿到锁时丢掉本地缓存，
/// 放锁之前先提交写入）。
pub fn lock_counter(a: &Path, b: &Path, n: u64) {
    std::fs::write(a.join("counter"), b"0").unwrap();
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/lock_counter.py");
    let run = |m: &Path| {
        let mut c = Command::new("timeout");
        c.args(["900", "python3"]).arg(&script).arg(m.join("counter")).args(["fcntl", &n.to_string()]);
        c.spawn().unwrap()
    };
    let (mut x, mut y) = (run(a), run(b));
    assert!(x.wait().unwrap().success() && y.wait().unwrap().success(), "④ 两边都在时限内做完（没有死锁）");
    let got = std::fs::read_to_string(b.join("counter")).unwrap();
    assert_eq!(got, (2 * n).to_string(), "④ 两个挂载各加 {n} 次");
    println!("④ 锁互斥（F_SETLKW）：计数器 = {got}");
}

/// ⑤ 配额：`fs_path` 设 `quota=<hard_mib>M,quantum=1M`，`writers` 个写者（一半在挂载 `a`、一半在 `b`）同时往里写，后台每 100 ms
/// 采样一次递归用量——用量永远不超过 hard + quantum × 写者数；每个写者最后都收到 EDQUOT。
pub fn quota_bound(dev_dir: &Path, nodes: u32, a: &Path, b: &Path, fs_path: &str, hard_mib: u64, writers: usize) {
    wmfs(dev_dir, nodes, &["set", fs_path, &format!("quota={hard_mib}M,quantum=1M"), "--json"]);
    let bound = (hard_mib + writers as u64) << 20;
    let (stop, peak) = (Arc::new(AtomicBool::new(false)), Arc::new(Mutex::new(0u64)));
    let sampler = {
        let (dev_dir, fs_path, stop, peak) = (dev_dir.to_path_buf(), fs_path.to_string(), stop.clone(), peak.clone());
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let u: serde_json::Value =
                    serde_json::from_str(&wmfs(&dev_dir, nodes, &["usage", &fs_path, "--json"])).unwrap();
                let used = u["quotas"][0]["used_bytes"].as_u64().unwrap();
                let mut p = peak.lock().unwrap();
                *p = (*p).max(used);
                drop(p);
                std::thread::sleep(Duration::from_millis(100));
            }
        })
    };
    let ws: Vec<_> = (0..writers)
        .map(|i| {
            let p = if i % 2 == 0 { a } else { b }.join(format!("w{i}"));
            std::thread::spawn(move || {
                let mut f = std::fs::File::create(p).unwrap();
                let block = vec![7u8; 1 << 20];
                for _ in 0..2 * hard_mib {
                    if let Err(e) = f.write_all(&block).and_then(|_| f.sync_all()) {
                        return e.raw_os_error();
                    }
                }
                None
            })
        })
        .collect();
    let codes: Vec<Option<i32>> = ws.into_iter().map(|t| t.join().unwrap()).collect();
    stop.store(true, Ordering::Relaxed);
    sampler.join().unwrap();
    assert!(codes.iter().all(|c| *c == Some(libc::EDQUOT)), "⑤ 每个写者最后都收到 EDQUOT：{codes:?}");
    let peak = *peak.lock().unwrap();
    assert!(peak <= bound, "⑤ 用量的峰值 {peak} 超过了 hard + quantum × 写者数 = {bound}");
    println!("⑤ 配额：{writers} 个写者，用量峰值 {} MiB ≤ {} MiB", peak >> 20, bound >> 20);
}
