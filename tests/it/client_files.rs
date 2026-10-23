//! 客户端文件层（`core/src/client/`，M6 审查后补的回归用例）：在完整节点上直接用 `FileClient`（与 `wmfs mount` 里的同一个，只是
//! 不经 FUSE），两个 FileClient = 两个挂载。
//! - 截断的范围按线性一致的最新长度算（别的挂载把文件写长之后，按路径 / 经句柄截短再扩长，读到 0，不会冒出旧数据）；
//! - 按路径截断先按当前权限检查：`EACCES` 原样返回，数据一点没动；
//! - 截断到 0 之后的 fsync 不再提交（不改 mtime 与 change_id）；
//! - `STALE_LAYOUT` 之后的重新 Open 不把别人的修改记成"见过"（下一次 open 照样失效，C2）；
//! - 并发提交时本挂载的属性只前进；读已提交的数据不会读成 0；
//! - inline 副本带着版本：只写打开回来的属性比副本新时，读重新取内容。

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use wmfs_core::base::ErrCode;
use wmfs_core::client::files::FileClient;
use wmfs_core::meta::schema::keys::ROOT_INO;
use wmfs_core::proto::meta::{AttrOut, MetaOp, MetaReply, O_RDONLY, O_RDWR, O_WRONLY, SetAttrIn};
use wmfs_core::server::Running;
use wmfs_testkit::harness::node::{TempDir, client, file_client, get, init, start, who};

fn node(name: &str) -> (TempDir, Running) {
    let d = TempDir::new(&format!("files-{name}"));
    init(&d.0);
    let run = start(&d.0);
    (d, run)
}

fn attr(c: &FileClient, ino: u64) -> AttrOut {
    match c.meta.read(&who(1000, 1000), MetaOp::GetAttr { ino }).unwrap() {
        MetaReply::Entry { attr } => attr,
        r => panic!("{r:?}"),
    }
}

/// 写 `n` 字节的 `b`、close（文件在本挂载里的状态留着，就像 FUSE 的 close 之后、内核 forget 之前）。
fn write_close(c: &FileClient, ino: u64, off: u64, n: usize, b: u8) {
    let me = who(1000, 1000);
    let o = c.open(&me, ino, O_RDWR).unwrap();
    c.write(&me, &c.handle(o.fh).unwrap(), off, &vec![b; n]).unwrap();
    c.flush(o.fh).unwrap();
    c.release(o.fh);
}

/// A 写 10 MiB 后 close；B 追加到 20 MiB；A 截到 1 MiB（按路径与经句柄各一次）；B 扩到 20 MiB → [1 MiB, 20 MiB) 读到 0。
#[test]
fn truncate_covers_what_another_mount_appended() {
    let (d, run) = node("stale-size");
    let (a, b) = (file_client(&d.0, &run), file_client(&d.0, &run));
    let me = who(1000, 1000);
    for by_handle in [false, true] {
        let name = format!("f{by_handle}");
        let o = a.create(&me, ROOT_INO, name.as_bytes(), 0o644, 0, O_RDWR).unwrap();
        let ino = o.attr.ino;
        a.write(&me, &a.handle(o.fh).unwrap(), 0, &vec![b'a'; 10 << 20]).unwrap();
        a.flush(o.fh).unwrap();
        let fh = if by_handle {
            Some(o.fh) // 经写句柄截断（ftruncate）：句柄一直开着
        } else {
            a.release(o.fh); // 按路径截断（truncate(2)）：已经 close，本挂载里的状态还留着
            None
        };
        write_close(&b, ino, 10 << 20, 10 << 20, b'b');
        assert_eq!(attr(&a, ino).size, 20 << 20);
        a.set_size(&me, ino, 1 << 20, fh).unwrap();
        assert_eq!(attr(&a, ino).size, 1 << 20);
        let unsynced = a.inode(ino).unwrap().st.lock().unwrap().unsynced.clone();
        assert!((0..5u32).all(|i| unsynced.contains(&i)), "截断过的 chunk 等着 fsync 落盘：{unsynced:?}");
        if by_handle {
            a.release(o.fh);
        }
        b.set_size(&me, ino, 20 << 20, None).unwrap();
        let got = get(&client(&d.0, &run, "client"), &me, ino).unwrap();
        assert_eq!(got.len(), 20 << 20);
        assert!(got[..1 << 20].iter().all(|x| *x == b'a'));
        let bad = got[1 << 20..].iter().filter(|x| **x != 0).count();
        assert_eq!(bad, 0, "经句柄 = {by_handle}：截断点之后的旧数据冒出来了");
    }
    run.stop();
}

/// 按路径截断属主自己的 0444 文件（别的挂载刚 chmod 过，本挂载还留着以前写打开时的写 cap）→ `EACCES`，数据一点没动。
#[test]
fn path_truncate_without_write_permission_touches_nothing() {
    let (d, run) = node("path-eacces");
    let (a, b) = (file_client(&d.0, &run), file_client(&d.0, &run));
    let me = who(1000, 1000);
    let o = a.create(&me, ROOT_INO, b"ro", 0o644, 0, O_RDWR).unwrap();
    let ino = o.attr.ino;
    a.write(&me, &a.handle(o.fh).unwrap(), 0, &vec![b'x'; 6 << 20]).unwrap();
    a.flush(o.fh).unwrap();
    a.release(o.fh);
    let set =
        SetAttrIn { mode: Some(0o444), uid: None, gid: None, size: None, atime: None, mtime: None, epoch_seen: 0 };
    b.meta.write(&me, MetaOp::SetAttr { ino, set }).unwrap();
    assert_eq!(a.set_size(&me, ino, 10, None).err(), Some(ErrCode::EACCES), "按路径截断照常按当前权限检查");
    let got = get(&client(&d.0, &run, "client"), &me, ino).unwrap();
    assert!(got.len() == 6 << 20 && got.iter().all(|x| *x == b'x'), "被拒绝的截断没有动 chunk 里的数据");
    run.stop();
}

/// 写 5 MiB 的 chunk 文件 → 截到 0 → 连续两次 fsync：文件没变，mtime 与 change_id 都不变。
#[test]
fn fsync_after_truncate_to_zero_does_not_commit() {
    let (d, run) = node("fsync");
    let a = file_client(&d.0, &run);
    let me = who(1000, 1000);
    let o = a.create(&me, ROOT_INO, b"g", 0o644, 0, O_RDWR).unwrap();
    let ino = o.attr.ino;
    a.write(&me, &a.handle(o.fh).unwrap(), 0, &vec![b'x'; 5 << 20]).unwrap();
    a.fsync(o.fh).unwrap();
    a.set_size(&me, ino, 0, Some(o.fh)).unwrap();
    let before = attr(&a, ino);
    std::thread::sleep(Duration::from_millis(1100));
    a.fsync(o.fh).unwrap();
    a.fsync(o.fh).unwrap();
    let after = attr(&a, ino);
    a.release(o.fh);
    assert_eq!((after.change_id, after.mtime), (before.change_id, before.mtime), "fsync 改了文件");
    run.stop();
}

/// C2：A 有脏数据时 B 把文件截到 0；A close 时 STALE_LAYOUT → 重新 Open → 重写；A 下一次 open 不能回 keep_cache。
#[test]
fn reopen_after_stale_layout_does_not_mark_a_foreign_change_as_seen() {
    let (d, run) = node("seen");
    let (a, b) = (file_client(&d.0, &run), file_client(&d.0, &run));
    let me = who(1000, 1000);
    let o = a.create(&me, ROOT_INO, b"h", 0o644, 0, O_RDWR).unwrap();
    let ino = o.attr.ino;
    a.release(o.fh);
    write_close(&a, ino, 0, 8 << 20, b'x');
    let o2 = a.open(&me, ino, O_RDWR).unwrap();
    a.write(&me, &a.handle(o2.fh).unwrap(), (8 << 20) - 100, &[b'y'; 100]).unwrap();
    let ob = b.open(&me, ino, O_RDWR).unwrap();
    b.set_size(&me, ino, 0, Some(ob.fh)).unwrap();
    b.release(ob.fh);
    a.flush(o2.fh).unwrap();
    a.release(o2.fh);
    let o3 = a.open(&me, ino, O_RDONLY).unwrap();
    a.release(o3.fh);
    assert!(!o3.keep_cache, "别的挂载改过文件：下一次 open 不能保留页缓存");
    let got = get(&client(&d.0, &run, "client"), &me, ino).unwrap();
    assert!(got.len() == 8 << 20 && got[(8 << 20) - 100..].iter().all(|x| *x == b'y'), "A 的数据按新 layout 重写了");
    assert!(got[..(8 << 20) - 100].iter().all(|x| *x == 0));
    run.stop();
}

/// 4 个后台写线程并发提交：本挂载的属性（change_id、长度）只前进（一个线程每 50 µs 看一次）；同时另一个线程用读句柄读已经提交
/// 的数据，不会读成 0。
#[test]
fn attrs_only_move_forward_while_the_writers_commit() {
    let (d, run) = node("forward");
    let a = file_client(&d.0, &run);
    let me = who(1000, 1000);
    for round in 0..4u8 {
        let o = a.create(&me, ROOT_INO, format!("r{round}").as_bytes(), 0o644, 0, O_RDWR).unwrap();
        let (h, ino) = (a.handle(o.fh).unwrap(), o.attr.ino);
        let r = a.open(&me, ino, O_RDONLY).unwrap();
        let hr = a.handle(r.fh).unwrap();
        let inode = a.inode(ino).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let (s1, i1) = (stop.clone(), inode.clone());
        let watcher = std::thread::spawn(move || {
            let (mut last, mut bad) = ((0u64, 0u64), Vec::new());
            while !s1.load(Ordering::Relaxed) {
                let cur = {
                    let st = i1.st.lock().unwrap();
                    (st.attr.change_id, st.attr.size)
                };
                if cur.0 < last.0 || cur.1 < last.1 {
                    bad.push(format!("属性往回走了：{last:?} → {cur:?}"));
                }
                last = cur;
                std::thread::sleep(Duration::from_micros(50));
            }
            bad
        });
        let (s2, a2, me2) = (stop.clone(), a.clone(), me.clone());
        let reader = std::thread::spawn(move || {
            let (mut high, mut bad) = (0u64, Vec::new());
            while !s2.load(Ordering::Relaxed) {
                high = high.max(inode.st.lock().unwrap().attr.size);
                if high >= 8 << 20 {
                    let off = high - (4 << 20);
                    let got = a2.read(&me2, &hr, off, 4 << 20).unwrap();
                    if got.len() < 4 << 20 || got.contains(&0) {
                        bad.push(format!("读 {off}（已提交到 {high}）读到了 0"));
                    }
                }
            }
            bad
        });
        let piece = vec![round + 1; 1 << 20];
        for i in 0..96u64 {
            a.write(&me, &h, i << 20, &piece).unwrap();
        }
        a.flush(o.fh).unwrap();
        stop.store(true, Ordering::Relaxed);
        let mut bad = watcher.join().unwrap();
        bad.extend(reader.join().unwrap());
        assert!(bad.is_empty(), "第 {round} 轮：{bad:?}");
        a.release(r.fh);
        a.release(o.fh);
    }
    run.stop();
}

/// inline 副本带着版本（tests 审查的复现）：A 读打开 "abc"；B 追加 "def"；A 只写打开（回复不带内容、属性已是新的）→ A 原来的
/// 读句柄读到 "abcdef"（不是 "abc" + 0）；A 在 6 处写 "X" 并 close → 新的读打开读到 "abcdefX"。
#[test]
fn the_inline_copy_is_refetched_when_it_is_older_than_the_attrs() {
    let (d, run) = node("inline");
    let (a, b) = (file_client(&d.0, &run), file_client(&d.0, &run));
    let me = who(1000, 1000);
    let o = b.create(&me, ROOT_INO, b"f", 0o644, 0, O_RDWR).unwrap();
    let ino = o.attr.ino;
    b.write(&me, &b.handle(o.fh).unwrap(), 0, b"abc").unwrap();
    b.flush(o.fh).unwrap();
    b.release(o.fh);
    let r = a.open(&me, ino, O_RDONLY).unwrap();
    let hr = a.handle(r.fh).unwrap();
    assert_eq!(a.read(&me, &hr, 0, 100).unwrap(), b"abc");
    write_close(&b, ino, 3, 3, b'd');
    let w = a.open(&me, ino, O_WRONLY).unwrap();
    assert_eq!(a.read(&me, &hr, 0, 100).unwrap(), b"abcddd", "读到的是当前版本，不是旧副本 + 0");
    a.write(&me, &a.handle(w.fh).unwrap(), 6, b"X").unwrap();
    a.flush(w.fh).unwrap();
    a.release(w.fh);
    a.release(r.fh);
    let r2 = a.open(&me, ino, O_RDONLY).unwrap();
    assert_eq!(a.read(&me, &a.handle(r2.fh).unwrap(), 0, 100).unwrap(), b"abcdddX");
    a.release(r2.fh);
    run.stop();
}
