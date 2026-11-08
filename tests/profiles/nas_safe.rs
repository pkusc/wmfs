//! nas-safe profile（design.md 9.7：M9–M11 的阶段版 = 3 节点、replicas=3，只设置已经有消费者的属性；M12 起 `/home` 加上
//! `trash=7d`、`audit=on`；M14 起是完整版：`wmfs dev up --profile nas-safe` 在集群起好之后起一个 mgr，审计日志由它的 audit-log 写）：
//! 按 `wmfs dev up --nodes 3 --profile nas-safe` 起 3 个节点（`/home replicas=3,trash=7d,audit=on`），`wmfs dev mount` 挂上一个客户端，
//! 然后跑 9.5 里这个阶段已有的检查器（M12：另有回收站恢复——删掉的文件在 `.wmfs-trash/<uid>/` 里、mv 回来内容不变——与审计标记）：
//! - ⑥ 单点故障无错误：写入中**依次** `kill -9` 每个节点（含元数据 leader），客户端一个错误也没有（只是等一会儿）；节点回来之后
//!   `wmfs dev wait chains-serving`（先 Syncing、后 Serving）；
//! - ① 已确认写不丢：每个文件写完都 fsync，全部 kill 做完之后逐一读回校验（经挂载点，并另用 `wmfs fs get` 绕过页缓存）；
//! - ⑦ Jepsen-lite（寄存器）在 `tests/it/jepsen_register.rs`，`scripts/profiles.sh nas-safe` 一起跑；
//! - M14：审计日志在 mgr 重启后仍完整——mgr 被 kill -9、停着的时候写的文件，重启之后补进审计日志；按 seq 去重之后每个文件的 Create
//!   恰好一条（至少一次投递：同一条可能写了两遍，seq 相同）。
//!
//! 用开发集群的固定端口，所以是 `#[ignore]`：只由 `scripts/profiles.sh nas-safe` 跑。

use std::io::Write;
use std::os::unix::fs::MetadataExt;
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
    for (k, v) in [("trash", "trash=7d"), ("audit", "audit=on")] {
        let got = wmfs(&dir, &["get", "/home", k]);
        assert!(got.contains(v), "nas-safe 阶段版（M12）：/home {v}：{got}");
    }
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
    // M12：回收站恢复（rm 进 .wmfs-trash/<文件属主的 uid>/、mv 回来内容不变，ADR-0043 第 1 条）与审计标记（/home 的修改都带 audited）
    let uid = std::fs::metadata(home.join("f0")).unwrap().uid();
    std::fs::remove_file(home.join("f0")).unwrap();
    let t = home.join(".wmfs-trash").join(uid.to_string());
    let name =
        std::fs::read_dir(&t).unwrap().map(|e| e.unwrap().file_name()).find(|n| n.to_string_lossy().ends_with("-f0"));
    std::fs::rename(t.join(name.expect("f0 在回收站里")), home.join("f0")).unwrap();
    assert!(std::fs::read(home.join("f0")).unwrap() == content(0), "从回收站 mv 回来，内容不变");
    // 审计标记：M14 起 mgr 的 audit-log 消费这些记录、写进审计日志（只写 audited 的），被所有消费者消费过的 changelog 记录很快会被 gc
    // 修剪（3.11 的保留策略），所以在审计日志里找：rm f0 的那条 Unlink（带 trash，进了回收站）
    wmfs(&dir, &["dev", "wait", "audit-caught-up", "--timeout", "60"]);
    let trashed = |v: &serde_json::Value| {
        v["op"] == "Unlink" && v["name"] == "f0" && v["extra"].as_array().unwrap().iter().any(|kv| kv[0] == "trash")
    };
    assert!(audit_events(&dir).iter().any(trashed), "审计日志里有 rm f0（进回收站）的 Unlink");
    audit_survives_a_mgr_restart(&dir, &home, n);
    wmfs(&dir, &["dev", "umount", mp.to_str().unwrap()]);
    println!("nas-safe：写了 {n} 个 fsync 过的文件，依次 kill 了 3 个节点，0 个错误；审计日志在 mgr 重启后完整");
    drop(c);
}

/// 审计日志（mgr 的 audit-log 写的全部文件）里的每一行（M14）；只有 audited 的记录才会写进去。
fn audit_events(dir: &Path) -> Vec<serde_json::Value> {
    let mut v = Vec::new();
    for e in std::fs::read_dir(dir.join("mgr/audit")).unwrap() {
        for l in std::fs::read_to_string(e.unwrap().path()).unwrap().lines() {
            let x: serde_json::Value = serde_json::from_str(l).unwrap();
            assert_eq!(x["audited"], true, "审计日志里只有 audited 的记录：{l}");
            v.push(x);
        }
    }
    v
}

/// 审计日志里 Create 的 (seq, 名字)，按 seq 去重（M14）。
fn audit_creates(dir: &Path) -> std::collections::BTreeMap<u64, String> {
    let creates = audit_events(dir).into_iter().filter(|v| v["op"] == "Create");
    creates.map(|v| (v["seq"].as_u64().unwrap(), v["name"].as_str().unwrap().to_string())).collect()
}

/// M14：kill -9 掉 mgr（先核对 /proc/<pid>/cmdline 确实是本集群的 wmfs mgr）、它停着的时候建几个文件、`wmfs dev mgr` 重启它，等审计追上：
/// f0..f{n-1} 与 g0..g4 的 Create 按 seq 去重之后都恰好一条。
fn audit_survives_a_mgr_restart(dir: &Path, home: &Path, n: usize) {
    wmfs(dir, &["dev", "wait", "audit-caught-up", "--timeout", "60"]);
    let pid = std::fs::read_to_string(dir.join("mgr/mgr.pid")).unwrap().trim().to_string();
    let cmdline = std::fs::read(format!("/proc/{pid}/cmdline")).unwrap();
    assert!(String::from_utf8_lossy(&cmdline).contains(&dir.join("mgr").display().to_string()), "是本集群的 mgr");
    assert!(Command::new("kill").args(["-9", &pid]).status().unwrap().success());
    for i in 0..5 {
        std::fs::write(home.join(format!("g{i}")), b"x").unwrap();
    }
    wmfs(dir, &["dev", "mgr"]);
    wmfs(dir, &["dev", "wait", "audit-caught-up", "--timeout", "60"]);
    let creates = audit_creates(dir);
    let want: Vec<String> = (0..n).map(|i| format!("f{i}")).chain((0..5).map(|i| format!("g{i}"))).collect();
    for name in &want {
        let count = creates.values().filter(|x| *x == name).count();
        assert_eq!(count, 1, "{name} 的 Create 按 seq 去重之后恰好一条：{creates:?}");
    }
}
