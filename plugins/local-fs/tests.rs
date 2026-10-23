//! local-fs 的单元测试：manifest、写 pending → 提交 → 读（含从旧版本复制未改动的块）、读句柄钉住旧版本、abort、sync、按 epoch
//! 删除与计数、列出链上的 chunk 与 epoch 目录、启动清理、已用的增减。fail point 的用例在 testkit（进程级开关，单独的测试程序）。

use wmfs_core::hooks::manifest::check_manifest_toml;

use super::*;

fn dir(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("wmfs-localfs-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn id(idx: u32) -> ChunkId {
    ChunkId { chain: 3, ino: 0x1234, epoch: 2, idx }
}

fn read_all(t: &dyn TargetStore, c: &ChunkId, n: usize) -> Option<Vec<u8>> {
    let mut b = vec![0u8; n];
    t.read_at(c, 0, &mut b).unwrap().map(|k| b[..k].to_vec())
}

#[test]
fn manifest_toml_matches_code() {
    check_manifest_toml(&MANIFEST, include_str!("manifest.toml")).unwrap();
}

#[test]
fn write_commit_read_copy_and_pinned_readers() {
    let d = dir("rw");
    let t = LocalFs::new().open_target(&d, 1).unwrap();
    assert_eq!(read_all(&*t, &id(0), 16), None, "还没有已提交版本");
    t.write_pending(&id(0), &[(0, b"abcd"), (8, b"ijkl")], &[]).unwrap();
    assert_eq!(read_all(&*t, &id(0), 16), None, "pending 不可见");
    t.commit_pending(&id(0), false).unwrap();
    assert_eq!(read_all(&*t, &id(0), 16).unwrap(), b"abcd\0\0\0\0ijkl");
    assert!(committed_path(&d, &id(0)).ends_with("c/00000003/34/0000000000001234-00000002/00000000"));
    let mut pinned = t.open_committed(&id(0)).unwrap().unwrap();
    // 新版本：改第 0..4 字节，8..12 从旧版本复制
    t.write_pending(&id(0), &[(0, b"WXYZ")], &[(8, 4)]).unwrap();
    t.commit_pending(&id(0), true).unwrap();
    assert_eq!(read_all(&*t, &id(0), 16).unwrap(), b"WXYZ\0\0\0\0ijkl");
    let mut old = [0u8; 4];
    pinned.read_at(0, &mut old).unwrap();
    assert_eq!(&old, b"abcd", "rename 之后，之前打开的句柄仍指向原版本");
    t.write_pending(&id(0), &[(0, b"zzzz")], &[]).unwrap();
    t.abort_pending(&id(0)).unwrap();
    assert_eq!(read_all(&*t, &id(0), 4).unwrap(), b"WXYZ", "abort 不影响已提交版本");
    t.sync(&[id(0), id(9)]).unwrap(); // 不存在的 chunk 跳过
    std::fs::remove_dir_all(&d).unwrap();
}

#[test]
fn lists_deletes_counts_and_clears_temp() {
    let d = dir("ls");
    let t = LocalFs::new().open_target(&d, 1).unwrap();
    for i in [2, 0, 1] {
        t.write_pending(&id(i), &[(0, &[7u8; 5000])], &[]).unwrap();
        t.commit_pending(&id(i), false).unwrap();
    }
    let other = ChunkId { epoch: 3, ..id(0) };
    t.install(&other, &[(0, b"x")]).unwrap();
    assert_eq!(t.list_chain(3, None, 10).unwrap(), vec![id(0), id(1), id(2), other]);
    assert_eq!(t.list_chain(3, Some(&id(1)), 1).unwrap(), vec![id(2)]);
    let ep: Vec<_> = t.list_epochs(None, 10).unwrap().iter().map(|e| (e.0, e.1, e.2)).collect();
    assert_eq!(ep, vec![(3, 0x1234, 2), (3, 0x1234, 3)]);
    let used = t.statfs().unwrap().used;
    assert!(used >= 3 * 4096, "已用跟着提交增加：{used}");
    assert_eq!(t.delete_epoch(3, 0x1234, 2).unwrap(), 3);
    assert_eq!(t.delete_epoch(3, 0x1234, 2).unwrap(), 0, "再删一次：没有了（幂等）");
    assert!(t.statfs().unwrap().used < used, "删除之后已用减少");
    std::fs::write(d.join("p/junk.p"), b"half").unwrap();
    std::fs::write(d.join("t/junk.t"), b"half").unwrap();
    t.clear_temp().unwrap();
    assert_eq!(std::fs::read_dir(d.join("p")).unwrap().count() + std::fs::read_dir(d.join("t")).unwrap().count(), 0);
    let reopened = LocalFs::new().open_target(&d, 1).unwrap();
    assert_eq!(reopened.statfs().unwrap().used, t.statfs().unwrap().used, "重新打开时遍历得到同样的已用");
    t.delete_chain_dir(3).unwrap();
    assert!(t.list_chain(3, None, 10).unwrap().is_empty());
    std::fs::remove_dir_all(&d).unwrap();
}
