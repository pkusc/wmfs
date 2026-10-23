//! M7 经 FUSE 的部分（真的 `wmfs mount` 子进程挂在一个完整节点上）：
//! - readdir 快照里 `..` 的 inode 号是父目录（ADR-0037 第 13 条；根目录的 `..` 是它自己；自己 rename 到别的目录之后跟着变；另一个
//!   挂载只 lookup 过路径也对）。Rust 的 `read_dir` 与 python 的 `os.scandir` 都去掉了 `.` 与 `..`，所以用 python 的 ctypes 直接调
//!   libc 的 `readdir`（cli 的测试里不能写 unsafe，0.2 第 4 条）。
//! - `chunk=64K` 的目录（`wmfs set <挂载点下的路径>`：CLI 按挂载来源认出挂载点、读 `user.wmfs.mgmt` 找到集群）：新文件的 layout 是
//!   64 KiB 的 chunk，写、读、跨 chunk 覆盖写、截断到 chunk 中间、再扩长，与内存里的模型逐字节一致（另一个挂载上读也一样）。
//! - `consistency=cto,attr_ttl=…` 真的变成内核的属性缓存时长：attr_ttl=5s 的目录里，另一个挂载刚 stat 过的文件被改长之后，5 秒内
//!   stat 仍是旧长度；attr_ttl=0s 的目录里立即是新长度（open 照样按 C2 看到新内容）。

mod common;

use std::fs::OpenOptions;
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::Path;

use common::{Node, bytes, read_all, sh};
use wmfs_testkit::harness::node::node_dir;

/// 列出目录 `dir` 里 `.` 与 `..` 的 d_ino（libc readdir 原样给出的）。
const DOTS: &str = r#"
import ctypes, sys
class Dirent(ctypes.Structure):
    _fields_ = [("d_ino", ctypes.c_uint64), ("d_off", ctypes.c_int64), ("d_reclen", ctypes.c_ushort),
                ("d_type", ctypes.c_ubyte), ("d_name", ctypes.c_char * 256)]
libc = ctypes.CDLL(None)
libc.opendir.restype = ctypes.c_void_p
libc.opendir.argtypes = [ctypes.c_char_p]
libc.readdir.restype = ctypes.POINTER(Dirent)
libc.readdir.argtypes = [ctypes.c_void_p]
libc.closedir.argtypes = [ctypes.c_void_p]
for p in sys.argv[1:]:
    d = libc.opendir(p.encode())
    got = {}
    while True:
        e = libc.readdir(d)
        if not e:
            break
        got[e.contents.d_name.decode()] = e.contents.d_ino
    libc.closedir(d)
    print(got["."], got[".."])
"#;

/// 目录 `dir` 的 (`.`, `..`) 两个 d_ino。
fn dots(n: &Node, dir: &Path) -> (u64, u64) {
    let script = n.dir.join("dots.py");
    std::fs::write(&script, DOTS).unwrap();
    let (rc, out) = sh(&n.dir, &format!("python3 {} {}", script.display(), dir.display()));
    assert_eq!(rc, 0, "{out}");
    let v: Vec<u64> = out.split_whitespace().map(|x| x.parse().unwrap()).collect();
    (v[0], v[1])
}

fn ino(p: &Path) -> u64 {
    std::fs::metadata(p).unwrap().ino()
}

#[test]
fn dotdot_in_readdir_is_the_parent_directory() {
    let n = Node::start("dotdot");
    let (a, b) = (n.mount("a", &[]), n.mount("b", &[]));
    assert_eq!(dots(&n, &a.mp), (1, 1), "根目录的 .. 是它自己");
    std::fs::create_dir_all(a.p("x/y")).unwrap();
    std::fs::create_dir(a.p("z")).unwrap();
    assert_eq!(dots(&n, &a.p("x")), (ino(&a.p("x")), 1));
    assert_eq!(dots(&n, &a.p("x/y")), (ino(&a.p("x/y")), ino(&a.p("x"))), "mkdir 时记下的父目录");
    std::fs::rename(a.p("x/y"), a.p("z/y")).unwrap();
    assert_eq!(dots(&n, &a.p("z/y")).1, ino(&a.p("z")), "自己 rename 到别的目录之后跟着变");
    assert_eq!(dots(&n, &b.p("z/y")).1, ino(&b.p("z")), "另一个挂载：按路径 lookup 时记下的父目录");
    let ls = sh(&b.mp, "ls -a z >/dev/null").0; // readdirplus 也记下子目录的父目录
    assert_eq!(ls, 0);
    assert_eq!(dots(&n, &b.p("z/y")).1, ino(&b.p("z")));
    a.unmount();
    b.unmount();
    n.stop();
}

/// 在 `dir` 里跑 `wmfs <args>`（admin key；集群地址由 CLI 从挂载点换算），返回 (退出码, 输出)。
fn wmfs(n: &Node, args: &str) -> (i32, String) {
    let key = node_dir(&n.dir).join("keys/admin.key");
    sh(&n.dir, &format!("{} {args} --key-file {}", env!("CARGO_BIN_EXE_wmfs"), key.display()))
}

#[test]
fn chunk_64k_files_read_write_and_truncate_across_chunks() {
    let n = Node::start("chunk64k");
    let (a, b) = (n.mount("a", &[]), n.mount("b", &[]));
    std::fs::create_dir(a.p("small")).unwrap();
    let (rc, out) = wmfs(&n, &format!("set {} chunk=64K --json", a.p("small").display()));
    assert!(rc == 0 && out.contains(r#""chunk":"64K""#), "{out}");
    let (rc, out) = wmfs(&n, &format!("get {} chunk", a.p("small").display()));
    assert!(rc == 0 && out.contains("chunk=64K") && out.contains("来自 /small"), "{out}");
    let mut model = bytes(7, (1 << 20) + 12345); // 17 个 chunk 还多一点
    std::fs::write(a.p("small/f"), &model).unwrap();
    let mgmt = &n.run.as_ref().unwrap().mgmt_addr;
    let stat = format!("fs stat /small/f --json --cluster {mgmt}");
    let (rc, out) = wmfs(&n, &stat);
    assert!(rc == 0 && out.contains(r#""chunk_size":65536"#), "新文件按目录属性用 64 KiB 的 chunk：{out}");
    assert_eq!(read_all(&b.p("small/f")), model, "另一个挂载读到的一样");
    let f = OpenOptions::new().read(true).write(true).open(a.p("small/f")).unwrap();
    let patch = bytes(8, 300_000);
    f.write_all_at(&patch, 65536 - 100).unwrap(); // 跨 5 个 chunk 的覆盖写
    model[65536 - 100..65536 - 100 + patch.len()].copy_from_slice(&patch);
    let cut = 3 * 65536 + 1000;
    f.set_len(cut as u64).unwrap(); // 截断到第 3 个 chunk 中间
    model.truncate(cut);
    f.set_len(600_000).unwrap();
    model.resize(600_000, 0);
    drop(f);
    assert_eq!(read_all(&a.p("small/f")), model, "覆盖写、截断、扩长之后与模型一致");
    assert_eq!(read_all(&b.p("small/f")), model, "另一个挂载 open 之后也一致（cto）");
    a.unmount();
    b.unmount();
    n.stop();
}

#[test]
fn attr_ttl_from_the_directory_attribute_reaches_the_kernel() {
    let n = Node::start("attrttl");
    let (a, b) = (n.mount("a", &[]), n.mount("b", &[]));
    for (d, ttl) in [("t0", "0s"), ("t5", "5s")] {
        std::fs::create_dir(a.p(d)).unwrap();
        let (rc, out) = wmfs(&n, &format!("set {} consistency=cto,attr_ttl={ttl}", a.p(d).display()));
        assert_eq!(rc, 0, "{out}");
        std::fs::write(a.p(&format!("{d}/f")), b"1").unwrap();
    }
    let size = |d: &str| std::fs::metadata(b.p(&format!("{d}/f"))).unwrap().len();
    assert_eq!((size("t0"), size("t5")), (1, 1));
    for d in ["t0", "t5"] {
        std::fs::write(a.p(&format!("{d}/f")), b"22").unwrap();
    }
    assert_eq!(size("t0"), 2, "attr_ttl=0s：内核不缓存属性");
    assert_eq!(size("t5"), 1, "attr_ttl=5s：5 秒内 stat 仍是内核缓存的旧长度（放松 ①）");
    assert_eq!(std::fs::read(b.p("t5/f")).unwrap(), b"22", "open 照样看到新内容（C2）");
    a.unmount();
    b.unmount();
    n.stop();
}
