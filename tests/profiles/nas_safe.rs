//! nas-safe profile 的阶段版（design.md 9.7：M9–M11 = 3 节点、replicas=3，只设置已经有消费者的属性——不设 trash、audit）：
//! 按 `wmfs dev up --nodes 3 --profile nas-safe` 起 3 个节点（`/home replicas=3`），`wmfs dev mount` 挂上一个客户端，然后跑 9.5 里
//! 这个阶段已有的检查器：
//! - ⑥ 单点故障无错误：写入中**依次** `kill -9` 每个节点（含元数据 leader），客户端一个错误也没有（只是等一会儿）；节点回来之后
//!   `wmfs dev wait chains-serving`（先 Syncing、后 Serving）；
//! - ① 已确认写不丢：每个文件写完都 fsync，全部 kill 做完之后逐一读回校验（经挂载点，并另用 `wmfs fs get` 绕过页缓存）；
//! - ⑦ Jepsen-lite（寄存器）在 `tests/it/jepsen_register.rs`，`scripts/profiles.sh nas-safe` 一起跑。
//!
//! 用开发集群的固定端口，所以是 `#[ignore]`：只由 `scripts/profiles.sh nas-safe` 跑。

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn wmfs(dir: &Path, args: &[&str]) -> String {
    let o = Command::new(env!("CARGO_BIN_EXE_wmfs"))
        .args(args)
        .env("WMFS_DEV_DIR", dir)
        .env("WMFS_CLUSTER", "127.0.0.1:19180,127.0.0.1:19280,127.0.0.1:19380")
        .env("WMFS_KEY_FILE", dir.join("n1/keys/admin.key"))
        .env("no_proxy", "127.0.0.1,localhost")
        .output()
        .unwrap();
    let text = format!("{}{}", String::from_utf8_lossy(&o.stdout), String::from_utf8_lossy(&o.stderr));
    assert!(o.status.success(), "wmfs {args:?} 失败：{text}");
    text
}

/// 无论成败，结束时停掉这个开发集群（先卸载挂载点）并删掉目录。
struct Cluster(PathBuf);

impl Drop for Cluster {
    fn drop(&mut self) {
        let _ = Command::new(env!("CARGO_BIN_EXE_wmfs"))
            .args(["dev", "down", "--purge"])
            .env("WMFS_DEV_DIR", &self.0)
            .status();
    }
}

fn content(i: usize) -> Vec<u8> {
    (0..(3 << 20) + i * 4099).map(|j| (j.wrapping_mul(7) ^ i) as u8).collect()
}

#[test]
#[ignore = "用开发集群的固定端口：由 scripts/profiles.sh nas-safe 跑"]
fn nas_safe_staged_profile() {
    let dir = std::env::var_os("WMFS_PROFILE_DIR") // scripts/profiles.sh 给的目录（超时被杀时它来兜底清理）
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join(format!("wmfs-profile-nas-safe-{}", std::process::id())));
    let c = Cluster(dir.clone());
    wmfs(&dir, &["dev", "up", "--nodes", "3", "--profile", "nas-safe"]);
    let get = wmfs(&dir, &["get", "/home", "replicas"]);
    assert!(get.contains("replicas=3") && get.contains("/home"), "nas-safe：/home replicas=3：{get}");
    let mp = dir.join("mnt");
    std::fs::create_dir_all(&mp).unwrap();
    wmfs(&dir, &["dev", "mount", mp.to_str().unwrap()]);
    let home = mp.join("home");
    let (stop, errors, written) =
        (Arc::new(AtomicBool::new(false)), Arc::new(Mutex::new(Vec::new())), Arc::new(Mutex::new(0)));
    let w = {
        let (home, stop, errors, written) = (home.clone(), stop.clone(), errors.clone(), written.clone());
        std::thread::spawn(move || {
            let mut i = 0;
            while !stop.load(Ordering::Relaxed) {
                let r = std::fs::File::create(home.join(format!("f{i}"))).and_then(|mut f| {
                    f.write_all(&content(i))?;
                    f.sync_all() // ① 已确认：fsync 返回之后
                });
                match r {
                    Ok(()) => *written.lock().unwrap() = i + 1,
                    Err(e) => errors.lock().unwrap().push(format!("f{i}: {e}")),
                }
                i += 1;
            }
        })
    };
    let at_least = |n: usize| {
        let t0 = std::time::Instant::now();
        while *written.lock().unwrap() < n {
            assert!(t0.elapsed() < Duration::from_secs(120), "写不动了：{:?}", errors.lock().unwrap());
            std::thread::sleep(Duration::from_millis(50));
        }
    };
    let now = || *written.lock().unwrap(); // 取了就放锁：拿着锁调 at_least 会和它自己、和写线程互相等
    for n in ["1", "2", "3"] {
        at_least(now() + 3);
        wmfs(&dir, &["dev", "kill", n]); // ⑥ 写入中 kill -9（node1 多半是元数据 leader）
        at_least(now() + 3); // 节点不在的时候照样写得进去
        wmfs(&dir, &["dev", "start", n]);
        wmfs(&dir, &["dev", "wait", "leader", "--timeout", "60"]);
        wmfs(&dir, &["dev", "wait", "chains-serving", "--timeout", "120"]);
    }
    stop.store(true, Ordering::Relaxed);
    w.join().unwrap();
    assert!(errors.lock().unwrap().is_empty(), "⑥ 客户端一个错误也没有：{:?}", errors.lock().unwrap());
    let n = *written.lock().unwrap();
    for i in 0..n {
        assert!(std::fs::read(home.join(format!("f{i}"))).unwrap() == content(i), "① f{i} 读回来不对");
    }
    let back = dir.join("back");
    wmfs(&dir, &["fs", "get", &format!("/home/f{}", n - 1), back.to_str().unwrap()]); // 绕过页缓存
    assert!(std::fs::read(&back).unwrap() == content(n - 1));
    let status = wmfs(&dir, &["status", "/home", "--json"]);
    assert!(status.contains("\"short_files\":0"), "全部节点回来、同步完：副本差距归零：{status}");
    wmfs(&dir, &["dev", "umount", mp.to_str().unwrap()]);
    println!("nas-safe 阶段版：写了 {n} 个 fsync 过的文件，依次 kill 了 3 个节点，0 个错误");
    drop(c);
}
