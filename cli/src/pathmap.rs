//! CLI 的路径换算（design.md 7.2，ADR-0037 第 10 条）：`wmfs set/get/unset/usage/status <path>` 的 `<path>` 可以是本机 wmfs 挂载点
//! 下的路径——读 `/proc/self/mountinfo`，按挂载来源 `wmfs:<cluster_id 前 8 位 hex>` 认出 wmfs 挂载（容器里以 root 直接挂载时类型是
//! `fuse`、不是 `fuse.wmfs`，所以只看来源前缀与类型以 `fuse` 开头），取最长的那个前缀换成文件系统内的路径，再读挂载根目录的虚拟
//! xattr `user.wmfs.mgmt`（客户端本地应答：逗号分隔的管理地址）得到集群地址；也可以直接写文件系统内的绝对路径，集群地址取
//! `--cluster` 或 `WMFS_CLUSTER`。`--cluster` 写了就总是用它。

use std::path::{Path, PathBuf};

use wmfs_core::base::sys;

use crate::cluster_addrs;
use crate::doctor::parse::{MountLine, parse_mountinfo};

/// 换算的结果：文件系统内的路径与集群的管理地址。
#[derive(Debug, PartialEq, Eq)]
pub struct Target {
    pub path: String,
    pub cluster: Vec<String>,
}

/// mountinfo 里的 wmfs 挂载点（见文件头）。
fn wmfs_mounts(mountinfo: &str) -> Vec<String> {
    let wmfs = |m: &MountLine| m.fstype.starts_with("fuse") && m.source.starts_with("wmfs:");
    parse_mountinfo(mountinfo).into_iter().filter(wmfs).map(|m| m.point).collect()
}

/// `abs` 落在哪个挂载点下（取最长的）：(挂载点, 文件系统内的路径)。
fn split_mount(abs: &str, mounts: &[String]) -> Option<(String, String)> {
    let under = |m: &&String| abs == m.as_str() || m.as_str() == "/" || abs.starts_with(&format!("{m}/"));
    let m = mounts.iter().filter(under).max_by_key(|m| m.len())?;
    let rest = abs[m.len()..].trim_start_matches('/');
    Some((m.clone(), format!("/{rest}")))
}

/// 规范的绝对路径：存在就 canonicalize；不存在就规范化它的父目录再接上最后一段。
fn absolute(arg: &str) -> PathBuf {
    let p = Path::new(arg);
    if let Ok(c) = std::fs::canonicalize(p) {
        return c;
    }
    match (p.parent().and_then(|d| std::fs::canonicalize(d).ok()), p.file_name()) {
        (Some(d), Some(n)) => d.join(n),
        _ => std::path::absolute(p).unwrap_or_else(|_| p.to_path_buf()),
    }
}

/// 换算 `<path>`（见文件头）。
pub fn resolve(arg: &str, cluster: &Option<String>) -> Result<Target, String> {
    let mounts = wmfs_mounts(&std::fs::read_to_string("/proc/self/mountinfo").unwrap_or_default());
    let abs = absolute(arg).display().to_string();
    if let Some((mp, path)) = split_mount(&abs, &mounts) {
        let cluster = match cluster {
            Some(_) => cluster_addrs(cluster)?,
            None => match sys::getxattr(Path::new(&mp), "user.wmfs.mgmt") {
                Ok(v) if !v.is_empty() => String::from_utf8_lossy(&v).split(',').map(String::from).collect(),
                _ => cluster_addrs(&None).map_err(|e| format!("读不到挂载点 {mp} 的 user.wmfs.mgmt：{e}"))?,
            },
        };
        return Ok(Target { path, cluster });
    }
    if !arg.starts_with('/') {
        return Err(format!("{arg} 不在 wmfs 挂载点下，也不是文件系统内的绝对路径（以 / 开头）"));
    }
    Ok(Target { path: arg.to_string(), cluster: cluster_addrs(cluster)? })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mounts_are_found_by_source_and_paths_are_mapped() {
        let mi = "29 1 259:2 / / rw - ext4 /dev/nvme0n1p2 rw\n\
                  90 29 0:50 / /home/u/wmfs-mnt rw,nosuid - fuse.wmfs wmfs:c1c1c1c1 rw,user_id=1000\n\
                  91 29 0:51 / /mnt/in\\040ctr rw - fuse wmfs:c1c1c1c1 rw\n\
                  92 29 0:52 / /home/u/other rw - fuse.sshfs host:/x rw\n\
                  93 90 0:53 / /home/u/wmfs-mnt/nested rw - fuse.wmfs wmfs:d2d2d2d2 rw\n";
        let m = wmfs_mounts(mi);
        assert_eq!(
            m,
            vec!["/home/u/wmfs-mnt", "/mnt/in ctr", "/home/u/wmfs-mnt/nested"],
            "按来源认，容器里的类型是 fuse"
        );
        let s = |p: &str| split_mount(p, &m);
        assert_eq!(s("/home/u/wmfs-mnt/proj/a"), Some(("/home/u/wmfs-mnt".into(), "/proj/a".into())));
        assert_eq!(s("/home/u/wmfs-mnt"), Some(("/home/u/wmfs-mnt".into(), "/".into())));
        assert_eq!(s("/home/u/wmfs-mnt/nested/x"), Some(("/home/u/wmfs-mnt/nested".into(), "/x".into())), "取最长的");
        assert_eq!(s("/home/u/wmfs-mntx/a"), None, "按整段比较");
        assert_eq!(s("/mnt/in ctr/p"), Some(("/mnt/in ctr".into(), "/p".into())));
        assert_eq!(s("/home/u/other/a"), None);
    }
}
