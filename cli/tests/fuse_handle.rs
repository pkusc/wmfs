//! ADR-0036（已打开的句柄学 NFS）经 FUSE 的部分：属主用 `open(O_CREAT|O_WRONLY, 0444)` 建的文件照常写入并 close（小文件与写过
//! 64 KiB 的）；属主 `fchmod 0444` 之后继续写、`ftruncate`、`fsync` 成功；按路径 `truncate` 属主自己的 0444 文件、以写方式打开它
//! 仍是 EACCES；`git init/add/commit`（git 的对象文件就是先建 0444 再写）在挂载点上照常工作。其他用户在 chmod 之后提交报
//! EACCES 的用例换不了 uid，在 wmfs-testkit 的 `meta_handle_authz.rs` 里（不同 uid 的客户端身份）。
//! M7 第一步（ADR-0037 第 4、12 条）：属主经写句柄写一个已经 fchmod 0444 的文件时，另一个挂载（同一属主）把它截断到 0、换了 epoch——
//! 写者 close 时 `STALE_LAYOUT` → `ReopenHandle` 取新 layout、照常重写进新 epoch、close 成功（M6 时这里是 ESTALE）。

mod common;

use common::{Node, sh};

#[test]
fn owner_writes_through_handles_of_read_only_files() {
    let n = Node::start("nfs");
    let m = n.mount("m", &[]);
    let py = r#"
import os, errno
fd = os.open("ro_small", os.O_CREAT | os.O_WRONLY | os.O_EXCL, 0o444)
os.write(fd, b"x" * 100); os.close(fd)
fd = os.open("ro_big", os.O_CREAT | os.O_WRONLY | os.O_EXCL, 0o444)
os.write(fd, b"y" * 200000); os.close(fd)
print(oct(os.stat("ro_small").st_mode & 0o777), os.stat("ro_small").st_size, os.stat("ro_big").st_size)
fd = os.open("chm", os.O_CREAT | os.O_RDWR, 0o644)
os.write(fd, b"a" * 1000)
os.fchmod(fd, 0o444)
os.write(fd, b"b" * 70000)
os.ftruncate(fd, 5000)
os.fsync(fd)
os.close(fd)
print(os.stat("chm").st_size, open("chm", "rb").read() == b"a" * 1000 + b"b" * 4000)
for f in (lambda: os.truncate("chm", 10), lambda: os.open("chm", os.O_WRONLY)):
    try:
        f(); print("ok")
    except OSError as e:
        print(errno.errorcode[e.errno])
"#;
    std::fs::write(n.dir.join("h.py"), py).unwrap();
    let (rc, out) = sh(&m.mp, &format!("python3 {}", n.dir.join("h.py").display()));
    assert_eq!((rc, out.trim()), (0, "0o444 100 200000\n5000 True\nEACCES\nEACCES"), "{out}");
    let git = "git init -q repo && cd repo && git config user.email t@t && git config user.name t && \
               echo hello > a && git add a && git commit -qm one && echo world >> a && git commit -qam two && \
               git log --oneline | wc -l && git fsck --no-progress 2>&1 | wc -l && find .git/objects -type f | head -1 | xargs stat -c %a";
    let (rc, out) = sh(&m.mp, git);
    assert_eq!((rc, out.split_whitespace().collect::<Vec<_>>()), (0, vec!["2", "0", "444"]), "git 照常工作：{out}");
    m.unmount();
    n.stop();
}

#[test]
fn owner_rewrites_a_read_only_file_after_another_mount_truncated_it() {
    let n = Node::start("reopen");
    let (a, b) = (n.mount("a", &[]), n.mount("b", &[]));
    let py = r#"
import os, sys
a, b = sys.argv[1], sys.argv[2]
fd = os.open(a, os.O_CREAT | os.O_RDWR | os.O_EXCL, 0o644)
os.write(fd, b"x" * 200000); os.fsync(fd)   # 已提交：chunk 文件，epoch 1
os.pwrite(fd, b"y" * 1000, 200000)           # 还在写回缓冲里
os.truncate(b, 0)                            # 另一个挂载截断到 0：换了 epoch
os.fchmod(fd, 0o444)
os.close(fd)                                 # 提交旧 epoch → STALE_LAYOUT → ReopenHandle → 重写进新 epoch
d = open(a, "rb").read()
print(os.stat(a).st_size, d[200000:] == b"y" * 1000, d[:200000] == bytes(200000), oct(os.stat(a).st_mode & 0o777))
"#;
    std::fs::write(n.dir.join("r.py"), py).unwrap();
    let cmd = format!("python3 {} {} {}", n.dir.join("r.py").display(), a.p("f").display(), b.p("f").display());
    let (rc, out) = sh(&n.dir, &cmd);
    assert_eq!((rc, out.trim()), (0, "201000 True True 0o444"), "写者 close 成功、数据进了新 epoch：{out}");
    assert!(a.log_text().contains("STALE_LAYOUT"), "确实走了 C4 的重新取 layout：{}", a.log_text());
    a.unmount();
    b.unmount();
    n.stop();
}
