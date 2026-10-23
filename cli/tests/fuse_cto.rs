//! 两个挂载点之间的 close-to-open（roadmap M6 的验收标准，design.md 3.1.3 第 3 条、3.1.4 C1/C2）：写者 close 之后，读者
//! **之后**发起的 open 一定看到新内容与新长度——包括在 1 秒的属性缓存窗口内（open 总是向 leader 取最新属性，change_id 变了就先
//! `inval_inode` 再回复）；读者自己读过、页缓存里有旧内容时也一样。目录项的变化在 attr_ttl（1 s）之后可见（放松 ①）。
//! 另有审查后补的几段（每段都对着一种真的退化：去掉 open 里的 `inval_inode`、"见过的 change_id"不按 + 1 规则、inline 副本不带版本、
//! 截断按本挂载记着的旧长度算）：读者只 stat 过、写者把文件改长；自己两次提交之间夹着别人的修改；只写打开之后读 inline 文件；
//! 别的挂载把文件写长之后本挂载截短再扩长（按路径与经句柄）。

mod common;

use std::fs::OpenOptions;
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::time::{Duration, Instant};

use common::{Node, bytes, read_all, sh, wait_for};

#[test]
fn close_to_open_between_two_mounts_even_inside_the_attr_cache_window() {
    let n = Node::start("cto");
    let (a, b) = (n.mount("a", &[]), n.mount("b", &[]));
    std::fs::write(a.p("cto.txt"), b"v0\n").unwrap();
    assert_eq!(std::fs::read(b.p("cto.txt")).unwrap(), b"v0\n");
    for i in 1..=20u64 {
        let content = format!("round {i}: {}\n", "x".repeat((i * 37 % 200) as usize)); // 长度来回变
        std::fs::write(a.p("cto.txt"), &content).unwrap(); // O_TRUNC + 写 + close
        let t0 = Instant::now();
        let got = std::fs::read(b.p("cto.txt")).unwrap(); // 紧接着在另一个挂载上 open + read
        assert_eq!(String::from_utf8(got).unwrap(), content, "第 {i} 轮：写者 close 之后读者 open 一定看到");
        assert!(t0.elapsed() < Duration::from_secs(1), "仍在 1 s 的属性缓存窗口内");
    }
    // 大文件：读者先读过（页缓存里有旧内容），写者改了中间一块再 close，读者重新 open 读到新内容
    std::fs::write(a.p("big"), bytes(1, 6 << 20)).unwrap();
    assert_eq!(read_all(&b.p("big")), bytes(1, 6 << 20));
    let f = std::fs::OpenOptions::new().write(true).open(a.p("big")).unwrap();
    f.write_all_at(&bytes(2, 100_000), 4_000_000).unwrap();
    drop(f);
    let mut want = bytes(1, 6 << 20);
    want[4_000_000..4_100_000].copy_from_slice(&bytes(2, 100_000));
    assert!(read_all(&b.p("big")) == want, "页缓存里的旧内容在 open 时被丢掉（C2）");
    // 目录项：另一个挂载上新建的文件，最多 attr_ttl（1 s）之后看得到
    std::fs::write(a.p("new"), b"n").unwrap();
    assert!(wait_for(3, || b.p("new").exists()), "目录项的变化在 attr_ttl 之后可见");
    a.unmount();
    b.unmount();
    n.stop();
}

/// 读者只 stat 过（属性新鲜、没读过内容），写者把文件改长并 close，读者立即 open + read（靠 open 里的 `inval_inode`）；
/// A 自己两次提交之间夹着 B 的修改（"见过的 change_id"只认恰好 + 1）。
#[test]
fn fresh_attrs_and_interleaved_commits_do_not_hide_new_data() {
    let n = Node::start("cto2");
    let (a, b) = (n.mount("a", &[]), n.mount("b", &[]));
    for i in 0..10usize {
        std::fs::write(a.p("grow"), b"abc").unwrap();
        std::fs::metadata(b.p("grow")).unwrap();
        let l = vec![b'L'; 100 + i];
        std::fs::write(a.p("grow"), &l).unwrap();
        assert_eq!(std::fs::read(b.p("grow")).unwrap(), l, "第 {i} 次：读者 stat 过，写者改长之后");
    }
    std::fs::write(a.p("mix"), vec![b'a'; 8192]).unwrap();
    let f = OpenOptions::new().read(true).write(true).open(a.p("mix")).unwrap();
    f.read_exact_at(&mut [0u8; 8192], 0).unwrap();
    OpenOptions::new().write(true).open(b.p("mix")).unwrap().write_all_at(&[b'B'; 4096], 0).unwrap();
    f.write_all_at(b"YYYYYYYYYY", 6000).unwrap();
    drop(f);
    let got = std::fs::read(a.p("mix")).unwrap();
    assert_eq!(&got[..4096], &[b'B'; 4096][..], "B 的修改夹在 A 的两次提交之间：A 重新打开时不能用旧页缓存");
    assert_eq!(&got[6000..6010], b"YYYYYYYYYY");
    a.unmount();
    b.unmount();
    n.stop();
}

/// inline 文件：A 读打开读过 "abc"；B 追加 "def"；A 只写打开（回复不带内容）→ A 原来的读句柄读到 "abcdef"；A 在 6 处写 "X" 并
/// close → A 新的读打开读到 "abcdefX"（不是旧副本 + 0）。
#[test]
fn a_write_only_open_does_not_leave_a_stale_inline_copy() {
    let n = Node::start("cto-inline");
    let (a, b) = (n.mount("a", &[]), n.mount("b", &[]));
    let py = format!(
        "import os\n\
         A, B = '{}', '{}'\n\
         open(B + '/f', 'wb').write(b'abc')\n\
         r = os.open(A + '/f', os.O_RDONLY); os.pread(r, 100, 0)\n\
         with open(B + '/f', 'ab') as f: f.write(b'def')\n\
         w = os.open(A + '/f', os.O_WRONLY)\n\
         print(os.pread(r, 100, 0))\n\
         os.pwrite(w, b'X', 6); os.close(w); os.close(r)\n\
         r2 = os.open(A + '/f', os.O_RDONLY)\n\
         print(os.pread(r2, 100, 0))\n",
        a.mp.display(),
        b.mp.display()
    );
    std::fs::write(n.dir.join("inl.py"), py).unwrap();
    let (rc, out) = sh(&n.dir, &format!("python3 {}", n.dir.join("inl.py").display()));
    assert_eq!((rc, out.trim()), (0, "b'abcdef'\nb'abcdefX'"), "{out}");
    a.unmount();
    b.unmount();
    n.stop();
}

/// A 写 4 MiB 并读过；B 追加 12 MiB；A 截到 1 MiB 再扩到 16 MiB（按路径 truncate(2) 一次、经 A 早就打开的写句柄 ftruncate 一次）→
/// [1 MiB, 16 MiB) 读到 0：截断的范围按线性一致的最新长度算，不按 A 记着的 4 MiB。
#[test]
fn truncating_after_another_mount_appended_leaves_no_stale_data() {
    let n = Node::start("cto-trunc");
    let (a, b) = (n.mount("a", &[]), n.mount("b", &[]));
    for by_handle in [false, true] {
        let name = format!("t{by_handle}");
        std::fs::write(a.p(&name), bytes(1, 4 << 20)).unwrap();
        assert!(read_all(&a.p(&name)) == bytes(1, 4 << 20));
        let held = OpenOptions::new().write(true).open(a.p(&name)).unwrap();
        OpenOptions::new().append(true).open(b.p(&name)).unwrap().write_all(&bytes(2, 12 << 20)).unwrap();
        if by_handle {
            held.set_len(1 << 20).unwrap();
            held.set_len(16 << 20).unwrap();
        } else {
            let py =
                format!("import os; p = '{}'; os.truncate(p, 1 << 20); os.truncate(p, 16 << 20)", a.p(&name).display());
            let (rc, out) = sh(&n.dir, &format!("python3 -c \"{py}\""));
            assert_eq!(rc, 0, "{out}");
        }
        drop(held);
        let got = read_all(&b.p(&name));
        assert_eq!(got.len(), 16 << 20);
        assert!(got[..1 << 20] == bytes(1, 4 << 20)[..1 << 20]);
        let bad = got[1 << 20..].iter().filter(|x| **x != 0).count();
        assert_eq!(bad, 0, "经句柄 = {by_handle}：[1 MiB, 16 MiB) 里冒出了旧数据");
    }
    a.unmount();
    b.unmount();
    n.stop();
}
