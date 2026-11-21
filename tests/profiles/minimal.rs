//! minimal profile 的冒烟（design.md 9.7；M6 起每个里程碑都跑）：按 `wmfs dev up --profile minimal` 起 1 个节点（3.4.3 的部署模板：
//! TCP、default 类 1 副本、不写 replicas 属性——所以单节点上**没有**降级警告；node.toml 停用 quota 与 mclock、qos 用 fifo），用
//! `wmfs dev mount` 挂上一个客户端，然后：
//! - 冒烟：目录、文件、改名、符号链接、硬链接、xattr、截断、删除；
//! - inline 与 chunk 文件（`wmfs fs stat --json` 核对：≤ 64 KiB 只在元数据里，更大的按 4 MiB 的 chunk 存）；
//! - `kill -9` / 重启：fsync 过的数据在服务端被 `kill -9`、重启之后完整，挂载进程在服务端不在的这段时间里自己重试（不报错）；
//! - quota、qos 属性被拒绝（没有插件消费：minimal 停用了 quota 与 mclock，不静默忽略）；
//! - 属性（M7）：在挂载点下的目录上 `wmfs set chunk=1M,replicas=1`（没有警告：只有 1 个故障域、声明也是 1 副本），新文件按 1 MiB 的
//!   chunk 存，`wmfs get` 的来源与 `semantics:` 一行（replicas 声明 1/实际 1），`wmfs usage` 的字节数等于写进去的；
//! - pjdfstest、fio verify、fsx 在容器里跑（`scripts/profiles.sh minimal`，没有镜像时显示 SKIP）。
//!
//! 用的是开发集群的固定端口 19100/19180，所以是 `#[ignore]`：只由 `scripts/profiles.sh minimal`（`scripts/accept.sh M6` 调用它）跑。
//! 实现注记（M6）：这个文件是 cli 包的 `[[test]]`（`path = "../tests/profiles/minimal.rs"`）——要用 `CARGO_BIN_EXE_wmfs`。

#[allow(dead_code)] // 与 nas_safe.rs、hpc_rdma.rs 共用：每个 profile 只用其中一部分
mod common;

use std::io::Write;
use std::path::Path;
use std::process::Command;

use common::Cluster;

/// 跑一条 `wmfs …`（1 个节点的开发集群、node1 的 admin key），失败就断言失败。
fn wmfs(dir: &Path, args: &[&str]) -> String {
    common::wmfs(dir, 1, args)
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
    let py = "import os; os.setxattr('a/big','user.k',b'hi'); print(os.getxattr('a/big','user.k'), os.listxattr('a/big')); \
              os.removexattr('a/big','user.k'); print(os.listxattr('a/big'))";
    let o = Command::new("python3").args(["-c", py]).current_dir(&mp).output().unwrap();
    let out = String::from_utf8_lossy(&o.stdout);
    assert_eq!((o.status.success(), out.trim()), (true, "b'hi' ['user.k']\n[]"), "user.* xattr 的 set/get/list/remove");
    std::fs::remove_file(mp.join("a/big2")).unwrap();
    std::fs::remove_dir(mp.join("a/b")).unwrap();
    // inline 与 chunk 文件
    std::fs::write(mp.join("a/chunked"), &big).unwrap();
    assert!(stat_json(&dir, "/a/small").contains("\"inline\":true"), "小文件只在元数据里");
    let s = stat_json(&dir, "/a/chunked");
    assert!(s.contains("\"inline\":false") && s.contains("\"chunk_size\":4194304"), "大文件按 chunk 存：{s}");
    assert!(s.contains("\"replicas\":1"), "default 类 1 副本：新文件的 layout 声明 1 副本：{s}");
    // 目录属性（M7）：挂载点下的路径由 CLI 换算
    std::fs::create_dir(mp.join("attrs")).unwrap();
    let d = mp.join("attrs");
    let set = wmfs(&dir, &["set", d.to_str().unwrap(), "chunk=1M,replicas=1", "--json"]);
    let only_reminder = set.contains("\"warnings\":[\"replicas 是")
        && set.contains("已有文件由协调器在后台转换")
        && !set.contains("故障域");
    assert!(only_reminder, "1 个故障域、声明 1 副本：只有\"已有文件由协调器转换\"的提醒：{set}");
    std::fs::write(d.join("f"), &big).unwrap();
    assert!(stat_json(&dir, "/attrs/f").contains("\"chunk_size\":1048576"), "新文件按目录属性的 chunk");
    let get = wmfs(&dir, &["get", d.to_str().unwrap()]);
    assert!(get.contains("chunk=1M") && get.contains("来自 /attrs") && get.contains("replicas:声明1/实际1"), "{get}");
    let usage = wmfs(&dir, &["usage", d.to_str().unwrap(), "--json"]);
    assert!(usage.contains(&format!("\"bytes\":{}", big.len())), "递归用量：{usage}");
    // 停用了 quota 与 mclock：quota、qos 属性没有插件消费——设置时被拒绝，而不是被静默忽略（9.7、semantics.md 第 4 节）
    for kv in ["quota=1G", "qos=r=10,w=1,l=100"] {
        let (ok, out) = common::run(&dir, 1, &["set", d.to_str().unwrap(), kv, "--json"]);
        assert!(!ok && out.contains("\"error\""), "{kv} 应被拒绝：{out}");
    }
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
