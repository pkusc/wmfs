//! FUSE 挂载的端到端测试（roadmap M6 的验收标准，design.md 8.1–8.4）：真的 `wmfs mount` 子进程挂在一个完整节点上。
//! - 普通文件、目录、符号链接、硬链接、xattr、chmod、截断、statfs；小文件走 inline、大文件走 chunk（向元数据核对）；
//! - spike（8.1）：协商了 FUSE_DONT_MASK，内核把原始 mode 与 umask 分开传来，`umask 027` 下新建文件 640、目录 750；
//! - 根目录的虚拟 xattr `user.wmfs.mgmt`：本地应答管理地址，改它 EPERM，不出现在 listxattr 里；
//! - `echo v > f`（O_TRUNC）之后的写直接进新 epoch（挂载日志里没有 STALE_LAYOUT 重写）；
//! - 被删除但仍打开的文件可以读完（打开集合经心跳上报，gc 不删）；`dd oflag=direct` 写入后内容一致；`O_DIRECT` 与 `O_SYNC` 的
//!   write 返回时已提交（不 close，另一个挂载就看得到长度）；
//! - inline 文件被截长（`truncate -s 100K`、fallocate、`truncate -s 10G`）之后，读内容之后的部分得到 0（不越界、挂载不崩）；
//! - C4：A 的数据还在写回缓冲里时 B 把文件截到 0，A close 时按新 layout 重写（挂载日志里有 STALE_LAYOUT 重写）；
//! - 随机的读写与截断（跨 chunk、跨 inline 阈值、关掉再打开）与内存里的参考模型逐字节一致。

mod common;

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{FileExt, MetadataExt, OpenOptionsExt, PermissionsExt};

use common::{Node, bytes, read_all, sh};
use wmfs_core::base::rand::XorShift64;
use wmfs_core::meta::schema::records::F_INLINE;

#[test]
fn posix_basics_with_inline_and_chunk_files() {
    let n = Node::start("basics");
    let m = n.mount("m", &[]);
    for (i, size) in [0usize, 6, 65536, 65537, (9 << 20) + 123].into_iter().enumerate() {
        let data = bytes(i as u64, size);
        std::fs::write(m.p(&format!("f{i}")), &data).unwrap();
        assert_eq!(read_all(&m.p(&format!("f{i}"))), data, "{size} 字节往返");
        let a = n.meta_attr(&format!("/f{i}"));
        assert_eq!(a.size, size as u64);
        assert_eq!(a.flags & F_INLINE != 0, size <= 65536, "≤ 64 KiB 走 inline，否则走 chunk（{size} 字节）");
    }
    std::fs::create_dir_all(m.p("d/e")).unwrap();
    std::fs::rename(m.p("f1"), m.p("d/e/g")).unwrap();
    assert_eq!(std::fs::read(m.p("d/e/g")).unwrap(), bytes(1, 6));
    std::os::unix::fs::symlink("e/g", m.p("d/link")).unwrap();
    assert_eq!(std::fs::read_link(m.p("d/link")).unwrap().to_str(), Some("e/g"));
    assert_eq!(std::fs::read(m.p("d/link")).unwrap(), bytes(1, 6), "符号链接");
    std::fs::hard_link(m.p("f2"), m.p("d/hard")).unwrap();
    assert_eq!(std::fs::metadata(m.p("f2")).unwrap().nlink(), 2, "硬链接");
    assert_eq!(std::fs::read(m.p("d/hard")).unwrap(), bytes(2, 65536));
    let (rc, out) = sh(
        &m.mp,
        "python3 -c \"import os; os.setxattr('f3','user.k',b'hi'); print(os.getxattr('f3','user.k'), os.listxattr('f3')); os.removexattr('f3','user.k'); print(os.listxattr('f3'))\"",
    );
    assert_eq!((rc, out.trim()), (0, "b'hi' ['user.k']\n[]"), "xattr");
    std::fs::set_permissions(m.p("f3"), std::fs::Permissions::from_mode(0o600)).unwrap();
    assert_eq!(std::fs::metadata(m.p("f3")).unwrap().mode() & 0o777, 0o600, "chmod");
    let f = OpenOptions::new().write(true).open(m.p("f4")).unwrap();
    f.set_len(100).unwrap();
    f.set_len(5 << 20).unwrap();
    drop(f);
    let got = read_all(&m.p("f4"));
    assert_eq!(&got[..100], &bytes(4, (9 << 20) + 123)[..100]);
    assert!(got[100..].iter().all(|b| *b == 0), "截短再扩长：读为 0");
    let names: Vec<String> =
        std::fs::read_dir(&m.mp).unwrap().map(|e| e.unwrap().file_name().into_string().unwrap()).collect();
    for want in ["d", "f0", "f2", "f3", "f4", "home", "scratch"] {
        assert!(names.contains(&want.to_string()), "readdir 里有 {want}：{names:?}");
    }
    let (rc, out) = sh(&m.mp, "df -k . | tail -1");
    assert_eq!(rc, 0, "statfs：{out}");
    std::fs::remove_dir(m.p("d/e")).expect_err("非空目录删不掉");
    std::fs::remove_file(m.p("d/e/g")).unwrap();
    std::fs::remove_dir(m.p("d/e")).unwrap();
    m.unmount();
    n.stop();
}

#[test]
fn spike_kernel_passes_mode_and_umask_separately() {
    let n = Node::start("umask");
    let m = n.mount("m", &[("WMFS_LOG", "debug")]);
    let (rc, out) = sh(&m.mp, "umask 027; touch f; mkdir d; mkfifo p; stat -c '%n %a' f d p");
    assert_eq!(rc, 0, "{out}");
    assert_eq!(out.trim(), "f 640\nd 750\np 640", "umask 027 下新建文件 640、目录 750");
    let log = m.log_text();
    assert!(
        log.contains("create \"f\"：内核传来 mode=100666 umask=27"),
        "FUSE_DONT_MASK：原始 mode 与 umask 分开传来：{log}"
    );
    assert!(log.contains("mkdir \"d\"：内核传来 mode=777 umask=27"), "{log}");
    assert!(log.contains("mknod \"p\"：内核传来 mode=10666 umask=27"), "{log}");
    m.unmount();
    n.stop();
}

#[test]
fn root_virtual_xattr_reports_the_mgmt_addresses() {
    let n = Node::start("vxattr");
    let m = n.mount("m", &[]);
    let mgmt = n.run.as_ref().unwrap().mgmt_addr.clone();
    let py = "import os, errno\n\
              print(os.getxattr('.', 'user.wmfs.mgmt').decode())\n\
              print('user.wmfs.mgmt' in os.listxattr('.'))\n\
              for f in (lambda: os.setxattr('.', 'user.wmfs.mgmt', b'x'), lambda: os.removexattr('.', 'user.wmfs.mgmt')):\n\
              \x20   try:\n\
              \x20       f(); print('ok')\n\
              \x20   except OSError as e:\n\
              \x20       print(errno.errorcode[e.errno])\n";
    std::fs::write(n.dir.join("vx.py"), py).unwrap();
    let (rc, out) = sh(&m.mp, &format!("python3 {}", n.dir.join("vx.py").display()));
    assert_eq!(
        (rc, out.trim().to_string()),
        (0, format!("{mgmt}\nFalse\nEPERM\nEPERM")),
        "只在根目录、只读、不出现在 listxattr"
    );
    m.unmount();
    n.stop();
}

#[test]
fn o_trunc_writes_go_straight_to_the_new_epoch() {
    let n = Node::start("otrunc");
    let m = n.mount("m", &[]);
    let (rc, out) = sh(&m.mp, "echo v1 > f && echo v22 > f && echo v333 > f && cat f");
    assert_eq!((rc, out.as_str()), (0, "v333\n"));
    assert_eq!(n.meta_attr("/f").epoch, 3, "每次 O_TRUNC 截断到 0 换一个 epoch");
    std::fs::write(m.p("big"), bytes(7, 5 << 20)).unwrap();
    std::fs::write(m.p("big"), bytes(8, 6 << 20)).unwrap(); // 覆盖写一个 chunk 文件：截断到 0 之后写进新 epoch
    assert_eq!(read_all(&m.p("big")), bytes(8, 6 << 20));
    assert!(!m.log_text().contains("STALE_LAYOUT"), "截断回复里带着新 layout：没有白写一轮旧 epoch：{}", m.log_text());
    m.unmount();
    n.stop();
}

#[test]
fn a_deleted_but_open_file_can_be_read_to_the_end() {
    let n = Node::start("orphan");
    let (a, b) = (n.mount("a", &[]), n.mount("b", &[]));
    let data = bytes(3, (6 << 20) + 5);
    std::fs::write(a.p("f"), &data).unwrap();
    let f = File::open(a.p("f")).unwrap();
    std::fs::remove_file(b.p("f")).unwrap(); // 另一个客户端删掉它
    std::thread::sleep(std::time::Duration::from_secs(9)); // 超过孤儿的 6 s 与 gc 的一轮：仍打开着，不能删
    let mut got = vec![0u8; data.len()];
    f.read_exact_at(&mut got, 0).unwrap();
    assert!(got == data, "删除后仍打开：读得完、内容一致");
    drop(f);
    a.unmount();
    b.unmount();
    n.stop();
}

#[test]
fn o_direct_and_o_sync_writes_are_committed_when_write_returns() {
    let n = Node::start("direct");
    let (a, b) = (n.mount("a", &[]), n.mount("b", &[]));
    let src = n.dir.join("src");
    std::fs::write(&src, bytes(5, 9 << 20)).unwrap();
    let cmd = format!(
        "dd if={} of=f bs=1M oflag=direct status=none && dd if={} of=small bs=4k count=3 oflag=direct status=none",
        src.display(),
        src.display()
    );
    let (rc, out) = sh(&a.mp, &cmd);
    assert_eq!(rc, 0, "{out}");
    assert_eq!(read_all(&b.p("f")), bytes(5, 9 << 20), "dd oflag=direct 写入后内容一致（另一个挂载）");
    assert_eq!(read_all(&b.p("small")), bytes(5, 12288));
    // O_SYNC：write 返回时已经落盘并提交了长度——不 close，另一个挂载也看得到
    let mut f = OpenOptions::new().write(true).create(true).custom_flags(libc::O_SYNC).open(a.p("sync")).unwrap();
    f.write_all(&bytes(6, 100_000)).unwrap();
    assert_eq!(std::fs::metadata(b.p("sync")).unwrap().len(), 100_000, "O_SYNC 的 write 返回 = 已提交");
    assert_eq!(read_all(&b.p("sync")), bytes(6, 100_000));
    drop(f);
    // O_DIRECT：每次 write 同步写进链并提交长度才返回——两次 1 MiB 的 write 之后不 close，另一个挂载 stat 到 2 MiB
    let py = format!(
        "import os, mmap\n\
         fd = os.open('od', os.O_CREAT | os.O_WRONLY | os.O_DIRECT, 0o644)\n\
         buf = mmap.mmap(-1, 1 << 20)\n\
         buf.write(b'z' * (1 << 20))\n\
         os.write(fd, buf); os.write(fd, buf)\n\
         print(os.stat('{}').st_size)\n\
         os.close(fd)\n",
        b.p("od").display()
    );
    std::fs::write(n.dir.join("od.py"), py).unwrap();
    let (rc, out) = sh(&a.mp, &format!("python3 {}", n.dir.join("od.py").display()));
    assert_eq!((rc, out.trim()), (0, "2097152"), "O_DIRECT 的 write 返回 = 已提交：{out}");
    a.unmount();
    b.unmount();
    n.stop();
}

/// inline 文件的内容比长度短（`len(INLINE) ≤ size`，3.2.7）时读内容之后的部分：读到 0，不越界（以前会 panic 掉 fuser 的
/// 工作线程，16 次之后整个挂载就没了）。
#[test]
fn reading_past_the_inline_content_returns_zeros() {
    let n = Node::start("inline-tail");
    let m = n.mount("m", &[]);
    std::fs::write(m.p("g"), b"abc").unwrap();
    let f = OpenOptions::new().read(true).write(true).open(m.p("g")).unwrap();
    f.set_len(100_000).unwrap();
    let mut got = vec![1u8; 4096];
    f.read_exact_at(&mut got, 50_000).unwrap();
    assert!(got.iter().all(|b| *b == 0), "内容之后读为 0");
    drop(f);
    assert_eq!(&read_all(&m.p("g"))[..4], b"abc\0");
    assert!(n.meta_attr("/g").flags & F_INLINE != 0, "截长的小文件仍是 inline");
    // 20 轮（以前每次越界都会 panic 掉一个 fuser 工作线程，16 次之后整个挂载就没了）；都在一个 python 进程里做，少 fork
    let py = "import os\n\
              ok = 0\n\
              for _ in range(20):\n\
              \x20   fd = os.open('fa', os.O_CREAT | os.O_RDWR, 0o644)\n\
              \x20   os.posix_fallocate(fd, 0, 1 << 20)\n\
              \x20   a = os.pread(fd, 4096, 500000) == bytes(4096) and os.fstat(fd).st_size == 1 << 20\n\
              \x20   os.close(fd)\n\
              \x20   fd = os.open('big', os.O_CREAT | os.O_RDWR, 0o644)\n\
              \x20   os.ftruncate(fd, 10 << 30)\n\
              \x20   b = os.pread(fd, 4096, 5 << 30) == bytes(4096)\n\
              \x20   os.close(fd)\n\
              \x20   os.unlink('fa'); os.unlink('big')\n\
              \x20   ok += a and b\n\
              print(ok)\n";
    std::fs::write(n.dir.join("fa.py"), py).unwrap();
    let (rc, out) = sh(&m.mp, &format!("python3 {}", n.dir.join("fa.py").display()));
    assert_eq!((rc, out.trim()), (0, "20"), "fallocate 与截长到 10 GiB 之后读，20 轮都是 0：{out}");
    assert!(!m.log_text().contains("panicked"), "挂载日志里不该有 panic：{}", m.log_text());
    m.unmount();
    n.stop();
}

/// C4：A 的数据还在写回缓冲里（没 close）时，B 用 O_TRUNC 把文件截到 0（新 epoch）；A close 时提交回 STALE_LAYOUT → 重新 Open、
/// 按新 layout 重写 → B 读到 100 个 0 + A 的数据，A 的挂载日志里有"STALE_LAYOUT … 重写"。
/// 整段放在一个 python 进程里做：测试进程里别的用例的线程在 fork 子进程，子进程 exec 时关掉继承来的 fd 也会让内核发 FLUSH
/// （每次 close 都发），A 的数据就可能在 B 截断之前提前提交——那也是合法的结果（放松 ⑧），只是这里要测的是"没提交的数据重写"。
#[test]
fn a_truncate_by_another_mount_makes_the_writer_rewrite_into_the_new_epoch() {
    let n = Node::start("c4");
    let (a, b) = (n.mount("a", &[]), n.mount("b", &[]));
    let py = format!(
        "import os\n\
         A, B = '{}', '{}'\n\
         data = bytes((i * 7 + 3) % 251 for i in range(200000))\n\
         fd = os.open(A, os.O_CREAT | os.O_TRUNC | os.O_RDWR, 0o644)\n\
         os.pwrite(fd, data, 100)\n\
         open(B, 'wb').close()\n\
         os.close(fd)\n\
         got = open(B, 'rb').read()\n\
         print(len(got), got == bytes(100) + data)\n",
        a.p("c4").display(),
        b.p("c4").display()
    );
    std::fs::write(n.dir.join("c4.py"), py).unwrap();
    let (rc, out) = sh(&n.dir, &format!("python3 {}", n.dir.join("c4.py").display()));
    assert_eq!((rc, out.trim()), (0, "200100 True"), "A 的数据进了新 epoch：{out}");
    assert!(a.log_text().contains("STALE_LAYOUT"), "A 重写过：{}", a.log_text());
    a.unmount();
    b.unmount();
    n.stop();
}

/// 随机的写、截断、读、关掉再打开，与参考模型逐字节比（跨 4 MiB 的 chunk 边界、跨 64 KiB 的 inline 阈值）。
#[test]
fn random_reads_writes_and_truncates_match_a_model() {
    let n = Node::start("model");
    let m = n.mount("m", &[]);
    let mut rng = XorShift64::new(42);
    let mut model: Vec<u8> = Vec::new();
    let path = m.p("f");
    let mut f = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(&path).unwrap();
    for step in 0..300 {
        let max = if step < 100 { 70_000 } else { 9 << 20 }; // 先在 inline 阈值附近，再跨 chunk
        match rng.below(10) {
            0..=4 => {
                let off = rng.below(max) as usize;
                let len = 1 + rng.below(200_000) as usize;
                let d = bytes(step, len);
                f.write_all_at(&d, off as u64).unwrap();
                if model.len() < off + len {
                    model.resize(off + len, 0);
                }
                model[off..off + len].copy_from_slice(&d);
            }
            5 => {
                let s = rng.below(max) as usize;
                f.set_len(s as u64).unwrap();
                model.resize(s, 0);
            }
            6 => {
                f.flush().unwrap();
                f.sync_all().unwrap();
                drop(f);
                f = OpenOptions::new().read(true).write(true).open(&path).unwrap();
            }
            _ => {
                let off = rng.below(model.len() as u64 + 1) as usize;
                let len = rng.below(300_000) as usize;
                let mut got = vec![0u8; len];
                let k = read_at_most(&f, &mut got, off as u64);
                let want = &model[off.min(model.len())..(off + len).min(model.len())];
                assert_eq!(&got[..k], want, "第 {step} 步：读 [{off}, +{len})");
            }
        }
        assert_eq!(f.metadata().unwrap().len(), model.len() as u64, "第 {step} 步：长度");
    }
    drop(f);
    assert!(read_all(&path) == model, "关掉之后整个文件一致");
    m.unmount();
    n.stop();
}

fn read_at_most(f: &File, buf: &mut [u8], off: u64) -> usize {
    let mut n = 0;
    while n < buf.len() {
        match f.read_at(&mut buf[n..], off + n as u64).unwrap() {
            0 => break,
            k => n += k,
        }
    }
    n
}
