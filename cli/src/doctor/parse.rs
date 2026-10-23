//! `wmfs doctor` 用的纯函数：解析 /proc、/sys、/etc 里的文本，列目录（都有单元测试，不依赖本机环境）。

use std::path::{Path, PathBuf};

/// 列出目录里的条目（排好序）；目录不存在或读不了 = 空列表。
pub fn list_dir(p: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> =
        std::fs::read_dir(p).map(|rd| rd.filter_map(|e| e.ok()).map(|e| e.path()).collect()).unwrap_or_default();
    v.sort();
    v
}

/// 路径的最后一段（例如 "/sys/class/infiniband/mlx5_0" → "mlx5_0"）。
pub fn name_of(p: &Path) -> String {
    p.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default()
}

/// "6.17.0-1029-nvidia" → (6, 17)
pub fn parse_kernel(rel: &str) -> Option<(u32, u32)> {
    let mut it = rel.split(|c: char| !c.is_ascii_digit()).filter(|s| !s.is_empty());
    Some((it.next()?.parse().ok()?, it.next()?.parse().ok()?))
}

/// /proc/self/mountinfo 的一行里用得到的部分。
#[derive(Debug, PartialEq, Eq)]
pub struct MountLine {
    pub point: String,  // 挂载点（八进制转义已经解回来）
    pub fstype: String, // 例如 ext4、fuse.wmfs，容器里以 root 直接挂载的 wmfs 是 fuse
    pub source: String, // 挂载来源，例如 /dev/nvme0n1p2、wmfs:c1c1c1c1（没有就是空）
}

/// /proc/self/mountinfo → 每个挂载一项。挂载点里的空格、制表符、换行、反斜杠在 mountinfo 里写成 `\040`、`\011`、`\012`、`\134`，
/// 这里解回来。
pub fn parse_mountinfo(text: &str) -> Vec<MountLine> {
    text.lines()
        .filter_map(|l| {
            let (left, right) = l.split_once(" - ")?;
            let point = unescape_octal(left.split(' ').nth(4)?);
            let mut r = right.split(' ');
            let fstype = r.next()?.to_string();
            let source = r.next().unwrap_or("").to_string();
            Some(MountLine { point, fstype, source })
        })
        .collect()
}

/// 把 `\ooo`（三位八进制）解成那个字节，其余原样。
fn unescape_octal(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        let octal = b[i] == b'\\' && i + 4 <= b.len() && b[i + 1..i + 4].iter().all(|c| (b'0'..=b'7').contains(c));
        if octal {
            let v = b[i + 1..i + 4].iter().fold(0u32, |v, c| v * 8 + (c - b'0') as u32);
            out.push(v as u8);
            i += 4;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// /proc/self/status 的 "Uid:" 行的第一个数（真实 uid）。
pub fn uid_from_status(text: &str) -> Option<u32> {
    text.lines().find_map(|l| l.strip_prefix("Uid:")).and_then(|r| r.split_whitespace().next()?.parse().ok())
}

/// /etc/fuse.conf 里有没有未注释的 user_allow_other。
pub fn fuse_conf_allows_other(text: &str) -> bool {
    text.lines().map(str::trim).any(|l| l == "user_allow_other")
}

/// "nvme0n1" → "nvme0"（NVMe 控制器名）；不是 NVMe namespace 返回 None。
pub fn nvme_ctrl(dev: &str) -> Option<String> {
    let rest = dev.strip_prefix("nvme")?;
    let n = rest.find('n')?;
    Some(format!("nvme{}", &rest[..n]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_parsers() {
        assert_eq!(parse_kernel("6.17.0-1029-nvidia"), Some((6, 17)));
        assert_eq!(parse_kernel("garbage"), None);
        let mi = "29 1 259:2 / / rw,relatime shared:1 - ext4 /dev/nvme0n1p2 rw\n\
                  90 29 0:50 / /home/u/wmfs-mnt rw,nosuid shared:2 - fuse.wmfs wmfs:c1c1c1c1 rw,user_id=1000\n";
        let m = parse_mountinfo(mi);
        assert_eq!((m[0].point.as_str(), m[0].fstype.as_str()), ("/", "ext4"));
        assert_eq!((m[1].fstype.as_str(), m[1].source.as_str()), ("fuse.wmfs", "wmfs:c1c1c1c1"));
        assert_eq!(uid_from_status("Name:\tx\nUid:\t1000\t1000\t1000\t1000\n"), Some(1000));
        assert!(!fuse_conf_allows_other("# user_allow_other\n"));
        assert!(fuse_conf_allows_other("mount_max = 1000\n user_allow_other \n"));
        assert_eq!(nvme_ctrl("nvme0n1").as_deref(), Some("nvme0"));
        assert_eq!(nvme_ctrl("nvme12n3").as_deref(), Some("nvme12"));
        assert_eq!(nvme_ctrl("sda"), None);
        assert!(list_dir(Path::new("/no/such/dir")).is_empty());
        assert_eq!(name_of(Path::new("/sys/class/infiniband/mlx5_0")), "mlx5_0");
    }
}
