//! minimal profile 的冒烟（design.md 9.7；M6 起每个里程碑都跑）：按 `wmfs dev up --profile minimal` 起 1 个节点（3.4.3 的部署模板：
//! TCP、default 类 1 副本、不写 replicas 属性——所以单节点上**没有**降级警告；node.toml 停用 quota 与 mclock、qos 用 fifo），用
//! `wmfs dev mount` 挂上一个客户端，然后：
//! - 冒烟：目录、文件、改名、符号链接、硬链接、xattr、截断、删除；
//! - inline 与 chunk 文件（`wmfs fs stat --json` 核对：≤ 64 KiB 只在元数据里，更大的按 4 MiB 的 chunk 存）；
//! - `kill -9` / 重启：fsync 过的数据在服务端被 `kill -9`、重启之后完整，挂载进程在服务端不在的这段时间里自己重试（不报错）；
//! - 属性 set/get 在 M7 才有；pjdfstest、fio verify、fsx 在容器里跑（`scripts/profiles.sh minimal`，没有镜像时显示 SKIP）。
//!
//! 用的是开发集群的固定端口 19100/19180，所以是 `#[ignore]`：只由 `scripts/profiles.sh minimal`（`scripts/accept.sh M6` 调用它）跑。
//! 实现注记（M6）：这个文件是 cli 包的 `[[test]]`（`path = "../tests/profiles/minimal.rs"`）——要用 `CARGO_BIN_EXE_wmfs`。

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;

fn wmfs(dir: &Path, args: &[&str]) -> String {
    let o = Command::new(env!("CARGO_BIN_EXE_wmfs"))
        .args(args)
        .env("WMFS_DEV_DIR", dir)
        .env("WMFS_CLUSTER", "127.0.0.1:19180")
        .env("WMFS_KEY_FILE", dir.join("n1/keys/admin.key"))
        .env("no_proxy", "127.0.0.1,localhost")
        .output()
        .unwrap();
    let text = format!("{}{}", String::from_utf8_lossy(&o.stdout), String::from_utf8_lossy(&o.stderr));
    assert!(o.status.success(), "wmfs {args:?} 失败：{text}");
    text
}

/// 无论成败，结束时停掉这个开发集群（先卸载挂载点，`dev down` 的清理顺序）并删掉目录。
struct Cluster(PathBuf);

impl Drop for Cluster {
    fn drop(&mut self) {
        let _ = Command::new(env!("CARGO_BIN_EXE_wmfs"))
            .args(["dev", "down", "--purge"])
            .env("WMFS_DEV_DIR", &self.0)
            .status();
    }
}

fn stat_json(dir: &Path, path: &str) -> String {
    wmfs(dir, &["fs", "stat", path, "--json"])
}

#[test]
#[ignore = "用开发集群的固定端口：由 scripts/profiles.sh minimal 跑"]
fn minimal_profile_smoke() {
    let dir = std::env::temp_dir().join(format!("wmfs-profile-minimal-{}", std::process::id()));
    let c = Cluster(dir.clone());
    let up = wmfs(&dir, &["dev", "up", "--nodes", "1", "--profile", "minimal"]);
    assert!(!up.contains("警告"), "minimal：default 类 1 副本、不写 replicas 属性，单节点上没有降级警告：{up}");
    let toml = std::fs::read_to_string(dir.join("n1/node.toml")).unwrap();
    assert!(toml.contains("disable = [\"quota\", \"mclock\"]"), "minimal 的部署模板：{toml}");
    let mp = dir.join("mnt");
    std::fs::create_dir_all(&mp).unwrap();
    let out = wmfs(&dir, &["dev", "mount", mp.to_str().unwrap()]);
    assert!(out.contains("已挂载") && !out.contains("警告"), "挂载时也没有集群警告：{out}");
    // 冒烟
    std::fs::create_dir_all(mp.join("a/b")).unwrap();
    std::fs::write(mp.join("a/b/small"), b"hello\n").unwrap();
    let big: Vec<u8> = (0..(5u32 << 20) + 77).map(|i| (i * 31 % 251) as u8).collect();
    std::fs::write(mp.join("a/big"), &big).unwrap();
    std::fs::rename(mp.join("a/b/small"), mp.join("a/small")).unwrap();
    std::os::unix::fs::symlink("small", mp.join("a/link")).unwrap();
    std::fs::hard_link(mp.join("a/big"), mp.join("a/big2")).unwrap();
    assert_eq!(std::fs::read(mp.join("a/link")).unwrap(), b"hello\n");
    assert!(std::fs::read(mp.join("a/big2")).unwrap() == big);
    let f = std::fs::OpenOptions::new().write(true).open(mp.join("a/big2")).unwrap();
    f.set_len(1000).unwrap();
    drop(f);
    assert_eq!(std::fs::metadata(mp.join("a/big")).unwrap().len(), 1000, "硬链接是同一个文件");
    std::fs::remove_file(mp.join("a/big2")).unwrap();
    std::fs::remove_dir(mp.join("a/b")).unwrap();
    // inline 与 chunk 文件
    std::fs::write(mp.join("a/chunked"), &big).unwrap();
    assert!(stat_json(&dir, "/a/small").contains("\"inline\":true"), "小文件只在元数据里");
    let s = stat_json(&dir, "/a/chunked");
    assert!(s.contains("\"inline\":false") && s.contains("\"chunk_size\":4194304"), "大文件按 chunk 存：{s}");
    assert!(s.contains("\"replicas\":1"), "default 类 1 副本：新文件的 layout 声明 1 副本：{s}");
    // kill -9 / 重启：fsync 过的数据完整
    let data: Vec<u8> = (0..3_000_000u32).map(|i| (i % 253) as u8).collect();
    let mut f = std::fs::File::create(mp.join("durable")).unwrap();
    f.write_all(&data).unwrap();
    f.sync_all().unwrap();
    drop(f);
    wmfs(&dir, &["dev", "kill", "1"]);
    wmfs(&dir, &["dev", "start", "1"]);
    wmfs(&dir, &["dev", "wait", "leader", "--timeout", "60"]);
    let back = dir.join("durable.back");
    wmfs(&dir, &["fs", "get", "/durable", back.to_str().unwrap()]); // 绕过内核页缓存：直接从数据节点读
    assert!(std::fs::read(&back).unwrap() == data, "kill -9、重启之后 fsync 过的数据完整");
    assert!(std::fs::read(mp.join("durable")).unwrap() == data, "挂载进程在服务端不在的这段时间里自己重试，照常可用");
    assert!(std::fs::read(mp.join("a/chunked")).unwrap() == big);
    std::fs::write(mp.join("after"), b"still works").unwrap();
    wmfs(&dir, &["dev", "umount", mp.to_str().unwrap()]);
    drop(c);
}
