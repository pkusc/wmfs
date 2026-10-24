//! M8 第一步（ADR-0038 第 7 条）：读句柄碰上 layout 变化时，重新取 layout 用 `ReopenHandle { write: false }`（以前是 `Open(O_RDONLY)`，
//! 按当前权限）。在完整节点上直接用两个 `FileClient`（= 两个挂载，不经 FUSE、没有内核页缓存）：
//! - 属主只读打开自己的 0600 文件，`chmod 0200` 之后另一个挂载（同一属主）把它截断到 0、写入新内容并 close：读句柄照常读到新内容
//!   （以前 `EACCES`）；
//! - 非属主只读打开 0644 文件，属主 `chmod o-r` 之后碰上 layout 变化 → `EACCES`（与 `Open` 只读的规则相同）。
//!
//! 读句柄怎样"碰上 layout 变化"：旧 epoch 的 chunk 被 gc 删掉（`epoch_gc_grace = 0`）之后再读 → `CHUNK_NOT_FOUND` → C5 先 GetAttr，
//! 发现 epoch 变了 → 重新取 layout（大文件一段）；新内容写成 inline 时，读句柄手上的属性比 inline 副本新 → 重新取 inline 内容（小文件一段）。

use std::path::Path;

use wmfs_core::base::ErrCode;
use wmfs_core::base::types::ChunkId;
use wmfs_core::client::files::FileClient;
use wmfs_core::meta::schema::keys::ROOT_INO;
use wmfs_core::proto::meta::{MetaOp, O_RDONLY, O_RDWR, O_WRONLY, SetAttrIn};
use wmfs_plugins::local_fs::committed_path;
use wmfs_testkit::harness::node::{TempDir, file_client, init, set_settings, start, target_dir, wait_for, who};

fn chmod(c: &FileClient, ino: u64, mode: u32) {
    let set = SetAttrIn { mode: Some(mode), uid: None, gid: None, size: None, atime: None, mtime: None, epoch_seen: 0 };
    c.meta.write(&who(1000, 1000), MetaOp::SetAttr { ino, set }).unwrap();
}

/// 属主经只写句柄把 `data` 写进去并 close。
fn write_wronly(c: &FileClient, ino: u64, data: &[u8]) {
    let me = who(1000, 1000);
    let w = c.open(&me, ino, O_WRONLY).unwrap();
    c.write(&me, &c.handle(w.fh).unwrap(), 0, data).unwrap();
    c.flush(w.fh).unwrap();
    c.release(w.fh);
}

/// 本挂载记着的 chunk 0（当前 layout 的 epoch）。
fn chunk0(c: &FileClient, ino: u64) -> ChunkId {
    let l = c.inode(ino).unwrap().st.lock().unwrap().layout.clone().unwrap();
    ChunkId { chain: l.chains[0], ino, epoch: l.epoch, idx: 0 }
}

/// 一个属主写好的 200 000 字节的文件（chunk 存储），`reader` 只读打开它，读一次；返回 (ino, 读句柄)。
fn opened_by(a: &FileClient, b: &FileClient, name: &str, mode: u32, reader_uid: u32) -> (u64, u64) {
    let owner = who(1000, 1000);
    let o = b.create(&owner, ROOT_INO, name.as_bytes(), mode, 0, O_RDWR).unwrap();
    b.write(&owner, &b.handle(o.fh).unwrap(), 0, &vec![b'o'; 200_000]).unwrap();
    b.flush(o.fh).unwrap();
    b.release(o.fh);
    let reader = who(reader_uid, reader_uid);
    let r = a.open(&reader, o.attr.ino, O_RDONLY).unwrap();
    assert_eq!(a.read(&reader, &a.handle(r.fh).unwrap(), 0, 4).unwrap(), b"oooo");
    (o.attr.ino, r.fh)
}

/// 另一个挂载（属主）截断到 0 并写入 `new`；等旧 epoch 的 chunk 被 gc 删掉（读句柄再读就一定碰上 layout 变化）。
fn rewrite(b: &FileClient, d: &Path, ino: u64, old: ChunkId, new: &[u8]) {
    b.set_size(&who(1000, 1000), ino, 0, None).unwrap(); // 按路径截断：Open(O_WRONLY)，属主有写权限
    write_wronly(b, ino, new);
    let gone = wait_for(15, || !committed_path(&target_dir(d), &old).exists());
    assert!(gone, "旧 epoch 的 chunk 应在宽限期 0 之后被 gc 删掉");
}

#[test]
fn read_handles_reopen_by_handle_after_a_layout_change() {
    let d = TempDir::new("reopen-read");
    init(&d.0);
    let run = start(&d.0);
    set_settings(&run.meta, |s| s.epoch_gc_grace_ms = 0);
    let (a, b) = (file_client(&d.0, &run), file_client(&d.0, &run));
    let owner = who(1000, 1000);

    // 属主：0600 → 只读打开 → chmod 0200 → 另一个挂载截断到 0 并写入 150 000 字节（新 epoch 的 chunk）
    let (ino, fh) = opened_by(&a, &b, "own-big", 0o600, 1000);
    let old = chunk0(&a, ino);
    chmod(&b, ino, 0o200);
    rewrite(&b, &d.0, ino, old, &vec![b'n'; 150_000]);
    let got = a.read(&owner, &a.handle(fh).unwrap(), 0, 150_000).unwrap();
    assert!(got.len() == 150_000 && got.iter().all(|x| *x == b'n'), "属主的读句柄照常读到新内容（以前 EACCES）");
    a.release(fh);

    // 属主：新内容是 inline 的小文件——读句柄先碰上 layout 变化（旧 chunk 没了），重新取回的是 inline 内容
    let (ino, fh) = opened_by(&a, &b, "own-small", 0o600, 1000);
    let old = chunk0(&a, ino);
    chmod(&b, ino, 0o200);
    rewrite(&b, &d.0, ino, old, b"tiny new content");
    let got = a.read(&owner, &a.handle(fh).unwrap(), 0, 100).unwrap();
    assert_eq!(got, b"tiny new content", "重新取回的 inline 内容也经句柄（属主不看权限位）");
    a.release(fh);

    // 非属主：0644 → 只读打开 → 属主 chmod o-r（0640）→ 截断并重写 → 读句柄碰上 layout 变化 → EACCES
    let (ino, fh) = opened_by(&a, &b, "other", 0o644, 2000);
    let old = chunk0(&a, ino);
    chmod(&b, ino, 0o640);
    rewrite(&b, &d.0, ino, old, &vec![b'n'; 150_000]);
    let other = who(2000, 2000);
    assert_eq!(
        a.read(&other, &a.handle(fh).unwrap(), 0, 10).err(),
        Some(ErrCode::EACCES),
        "其他用户与 Open 只读的规则相同"
    );
    a.release(fh);
    run.stop();
}
