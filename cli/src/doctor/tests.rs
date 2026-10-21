//! `wmfs doctor` 的单元测试（单独放一个文件，给 doctor.rs 留出将来加检查项的空间）。

use super::*;
use std::time::Instant;

#[test]
fn mount_based_checks() {
    let m = parse_mountinfo("29 1 259:2 / / rw - ext4 /dev/nvme0n1p2 rw\n90 29 0:50 / /home/u/m rw - fuse.wmfs w rw\n");
    assert_eq!(stale_mounts(&m).status, Status::Warn);
    assert_eq!(rootfs(&m).title, "根分区 ext4");
    assert_eq!(stale_mounts(&m[..1]).status, Status::Ok);
}

#[test]
fn missing_pieces_are_failures_and_exit_code_is_1() {
    let no_fuse = fuse_dev(Path::new("/no/such/fuse"));
    assert_eq!(no_fuse.status, Status::Fail);
    assert_eq!(fusermount(OsStr::new("")).status, Status::Fail, "PATH 为空");
    assert_eq!(fusermount(OsStr::new("/no/such/dir")).status, Status::Fail);
    // 有 fusermount3 但没有 setuid 位
    let dir = std::env::temp_dir().join(format!("wmfs-doctor-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("fusermount3"), b"#!/bin/sh\n").unwrap();
    let c = fusermount(dir.as_os_str());
    std::fs::remove_dir_all(&dir).unwrap();
    assert!(c.status == Status::Fail && c.title.contains("setuid"), "{c:?}");
    assert_eq!(exit_code(&[ok("a", "x", ""), warn("b", "y", "")]), 0, "只有 ✓ 与 ! 时退出码为 0");
    assert_eq!(exit_code(&[ok("a", "x", ""), no_fuse]), 1, "有 ✗ 时退出码为 1");
    let report = Report { version: "v".into(), ok: false, checks: vec![fuse_dev(Path::new("/no/such/fuse"))] };
    let text = render(&report);
    assert!(text.contains("✗ /no/such/fuse 不可用") && text.contains("✗ 1 项"), "{text}");
}

#[test]
fn checks_never_suggest_sudo() {
    // 人类的决定：doctor 不提示任何 sudo 操作（RDMA 一项只说明本机不做实机测试）
    let checks = [
        kernel(),
        fuse_dev(Path::new("/dev/fuse")),
        fusermount(&std::env::var_os("PATH").unwrap_or_default()),
        memlock(),
        proxy(),
        rdma_check(),
        cgroup_io(),
        allow_other(),
    ];
    for c in checks {
        assert!(!c.title.contains("sudo") && !c.detail.contains("sudo"), "{c:?}");
    }
}

#[test]
fn timeout_runner() {
    assert_eq!(run_with_timeout(Command::new("true"), Duration::from_secs(5)), Some(String::new()));
    let mut sleep = Command::new("sleep");
    sleep.arg("5");
    let t = Instant::now();
    assert_eq!(run_with_timeout(sleep, Duration::from_millis(100)), None);
    assert!(t.elapsed() < Duration::from_secs(2));
    assert_eq!(run_with_timeout(Command::new("/no/such/cmd"), Duration::from_secs(1)), None);
}
