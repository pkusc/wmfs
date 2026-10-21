//! 把目录所在的块设备解析成"物理整盘或 NVMe namespace"（design.md 6.2 第 3 层、6.3、`TargetRecV1.phys_devs`）。
//!
//! 只读 sysfs（纯 std::fs）：目录的 st_dev → `/sys/dev/block/<maj>:<min>`；是分区 → 取父设备；
//! 是 dm/md/LVM（有 `slaves/`）→ 沿 slaves 递归到底；loop 或解析失败 → unknown（空列表），一律按共享设备对待。
//! 系统据此**计算**实际的隔离承诺（同一块 NVMe 的两个分区、同一个 PV 上的两个 LV 都算共用）。

use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

/// 目录 `path` 所在的物理整盘/namespace（例如 `["nvme0n1"]`），升序去重；空 = unknown。
pub fn phys_devs(path: &Path) -> Vec<String> {
    match std::fs::metadata(path) {
        Ok(m) => resolve(Path::new("/sys"), dev_major(m.dev()), dev_minor(m.dev())),
        Err(_) => Vec::new(),
    }
}

/// 按 Linux 的 dev_t 编码取主设备号（与 glibc 的 major() 相同）。
pub fn dev_major(dev: u64) -> u32 {
    (((dev >> 32) & 0xffff_f000) | ((dev >> 8) & 0x0000_0fff)) as u32
}

/// 按 Linux 的 dev_t 编码取次设备号（与 glibc 的 minor() 相同）。
pub fn dev_minor(dev: u64) -> u32 {
    (((dev >> 12) & 0xffff_ff00) | (dev & 0x0000_00ff)) as u32
}

/// 在 `sys_root`（平时是 `/sys`，测试时是一个假的目录树）下解析主/次设备号。
/// 任何一个叶子解析不出来（loop、读不到）→ 整体 unknown，不给出"部分正确"的答案。
pub fn resolve(sys_root: &Path, major: u32, minor: u32) -> Vec<String> {
    let mut out = Vec::new();
    let ok = match std::fs::canonicalize(sys_root.join(format!("dev/block/{major}:{minor}"))) {
        Ok(dev) => walk(&dev, 0, &mut out),
        Err(_) => false,
    };
    if !ok {
        return Vec::new();
    }
    out.sort();
    out.dedup();
    out
}

fn walk(dev: &Path, depth: u32, out: &mut Vec<String>) -> bool {
    let Some(name) = dev.file_name().and_then(|n| n.to_str()) else {
        return false;
    };
    if depth > 8 || name.starts_with("loop") {
        return false; // 异常的环；或 loop 设备：背后的文件在哪块盘上不可知
    }
    let disk: PathBuf = if dev.join("partition").exists() {
        match dev.parent() {
            Some(p) => p.to_path_buf(), // 分区 → 它所在的整盘
            None => return false,
        }
    } else {
        dev.to_path_buf()
    };
    let slaves: Vec<PathBuf> = match std::fs::read_dir(disk.join("slaves")) {
        Ok(rd) => rd.filter_map(|e| e.ok()).filter_map(|e| std::fs::canonicalize(e.path()).ok()).collect(),
        Err(_) => Vec::new(),
    };
    if slaves.is_empty() {
        match disk.file_name().and_then(|n| n.to_str()) {
            Some(n) => {
                out.push(n.to_string());
                true
            }
            None => false,
        }
    } else {
        slaves.iter().all(|s| walk(s, depth + 1, out))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;

    /// 造一个假的 sysfs：nvme0n1（分区 p2、p3）、建在 p3 上的 dm-0、loop0、建在 loop0 上的 dm-1。
    fn fake_sys() -> PathBuf {
        let r = crate::base::test_dir("devinfo");
        let mk = |p: &str| std::fs::create_dir_all(r.join(p)).unwrap();
        mk("devices/pci/nvme0n1/nvme0n1p2");
        mk("devices/pci/nvme0n1/nvme0n1p3");
        std::fs::write(r.join("devices/pci/nvme0n1/nvme0n1p2/partition"), "2").unwrap();
        std::fs::write(r.join("devices/pci/nvme0n1/nvme0n1p3/partition"), "3").unwrap();
        mk("devices/virtual/dm-0/slaves");
        symlink("../../../pci/nvme0n1/nvme0n1p3", r.join("devices/virtual/dm-0/slaves/nvme0n1p3")).unwrap();
        mk("devices/virtual/loop0");
        mk("devices/virtual/dm-1/slaves");
        symlink("../../loop0", r.join("devices/virtual/dm-1/slaves/loop0")).unwrap();
        mk("dev/block");
        for (num, target) in [
            ("259:0", "devices/pci/nvme0n1"),
            ("259:2", "devices/pci/nvme0n1/nvme0n1p2"),
            ("253:0", "devices/virtual/dm-0"),
            ("7:0", "devices/virtual/loop0"),
            ("253:1", "devices/virtual/dm-1"),
        ] {
            symlink(format!("../../{target}"), r.join("dev/block").join(num)).unwrap();
        }
        r
    }

    #[test]
    fn resolves_partitions_dm_and_loop() {
        let r = fake_sys();
        assert_eq!(resolve(&r, 259, 0), vec!["nvme0n1"], "整盘就是它自己");
        assert_eq!(resolve(&r, 259, 2), vec!["nvme0n1"], "分区 → 整盘");
        assert_eq!(resolve(&r, 253, 0), vec!["nvme0n1"], "dm → slaves → 分区 → 整盘");
        assert!(resolve(&r, 7, 0).is_empty(), "loop → unknown");
        assert!(resolve(&r, 253, 1).is_empty(), "建在 loop 上的 dm → unknown");
        assert!(resolve(&r, 0, 42).is_empty(), "虚拟文件系统（tmpfs/overlay）→ unknown");
        std::fs::remove_dir_all(&r).unwrap();
    }

    #[test]
    fn dev_number_split() {
        // glibc 的 makedev：主设备号低 12 位在 8..20，高位在 44..；次设备号低 8 位在 0..8，高位在 20..44
        let makedev =
            |ma: u64, mi: u64| (mi & 0xff) | ((ma & 0xfff) << 8) | ((mi & !0xff) << 12) | ((ma & !0xfff) << 32);
        for (ma, mi) in [(259u32, 2u32), (8, 1), (4095, 255), (4096, 256), (0x12345, 0xabcdef)] {
            let dev = makedev(ma as u64, mi as u64);
            assert_eq!((dev_major(dev), dev_minor(dev)), (ma, mi));
        }
        let _ = phys_devs(Path::new("/")); // 本机应是 ["nvme0n1"]；在容器里可能是 unknown，只要求不 panic
    }
}
