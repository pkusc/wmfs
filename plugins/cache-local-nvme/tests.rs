//! cache-local-nvme 的单元测试：放进去取得出、change_id 不同查不到、按总字节 LRU 淘汰（文件也删了）、invalidate_ino、挂载时只清空本插件的文件；
//! 目录 0700、块文件 0600，目录权限不对拒绝；一个目录同时只给一个挂载用。

use std::os::unix::fs::PermissionsExt;

use wmfs_core::hooks::manifest::check_manifest_toml;

use super::*;

/// 目录里这个插件的块文件个数（不算 `.lock`）。
fn blocks(d: &Path) -> usize {
    std::fs::read_dir(d).unwrap().flatten().filter(|e| e.file_name().to_str().is_some_and(ours)).count()
}

fn mode(p: &Path) -> u32 {
    std::fs::metadata(p).unwrap().permissions().mode() & 0o777
}

fn key(ino: u64, change_id: u64, block: u64) -> CacheKey {
    CacheKey { ino, change_id, block }
}

fn dir(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("wmfs-test-local-nvme-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

#[test]
fn manifest_toml_matches_code() {
    check_manifest_toml(&MANIFEST, include_str!("manifest.toml")).unwrap();
}

#[test]
fn put_get_lru_and_invalidate() {
    let d = dir("lru");
    let c = LocalNvme::new(&d, 3000).unwrap();
    let mut buf = vec![0u8; 2000];
    c.put(&key(1, 1, 0), &[1u8; 1000]);
    c.put(&key(1, 1, 1), &[2u8; 1000]);
    assert_eq!(c.get(&key(1, 1, 0), &mut buf), Some(1000));
    assert!(buf[..1000].iter().all(|b| *b == 1));
    assert_eq!(c.get(&key(1, 2, 0), &mut buf), None, "change_id 变了：查不到");
    c.put(&key(2, 1, 0), &[3u8; 1000]);
    c.put(&key(3, 1, 0), &[4u8; 1000]); // 超过 3000：淘汰最久没用的 (1,1,1)
    assert_eq!(c.get(&key(1, 1, 1), &mut buf), None, "最久没用的被淘汰");
    assert_eq!(c.get(&key(1, 1, 0), &mut buf), Some(1000), "刚用过的还在");
    assert_eq!(blocks(&d), 3, "淘汰的块的文件也删了");
    assert_eq!((mode(&d), mode(&c.path(&key(1, 1, 0)))), (0o700, 0o600), "缓存的是明文：只有属主能读");
    c.put(&key(9, 9, 9), &vec![0u8; 4000]); // 比容量还大：不放
    assert_eq!(c.get(&key(9, 9, 9), &mut vec![0u8; 4000]), None);
    c.invalidate_ino(1);
    assert_eq!(c.get(&key(1, 1, 0), &mut buf), None);
    std::fs::remove_dir_all(&d).unwrap();
}

#[test]
fn mount_clears_only_our_files() {
    let d = dir("clear");
    let c = LocalNvme::new(&d, 1 << 20).unwrap();
    c.put(&key(5, 6, 7), b"cached");
    std::fs::write(d.join("not-ours.txt"), b"keep me").unwrap();
    drop(c);
    let c = LocalNvme::new(&d, 1 << 20).unwrap();
    assert_eq!(c.get(&key(5, 6, 7), &mut [0u8; 16]), None, "重新挂载：缓存清空（不做崩溃恢复）");
    let mut left: Vec<String> =
        std::fs::read_dir(&d).unwrap().flatten().map(|e| e.file_name().into_string().unwrap()).collect();
    left.sort();
    assert_eq!(left, vec![".lock", "not-ours.txt"], "别的文件不碰");
    std::fs::remove_dir_all(&d).unwrap();
}

#[test]
fn one_mount_per_directory_and_only_a_private_one() {
    let d = dir("lock");
    let c = LocalNvme::new(&d, 1 << 20).unwrap();
    let e = LocalNvme::new(&d, 1 << 20).err().unwrap();
    assert!(e.contains("另一个挂载"), "同一个目录的第二个挂载被拒绝：{e}");
    drop(c);
    drop(LocalNvme::new(&d, 1 << 20).unwrap()); // 前一个卸载了：可以
    std::fs::set_permissions(&d, std::fs::Permissions::from_mode(0o755)).unwrap();
    let e = LocalNvme::new(&d, 1 << 20).err().unwrap();
    assert!(e.contains("chmod 700"), "别人能读的目录：拒绝（缓存的是明文）：{e}");
    std::fs::remove_dir_all(&d).unwrap();
}
