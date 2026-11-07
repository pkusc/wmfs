//! M12 经 FUSE 的锁与 strong（真的 `wmfs mount` 子进程，两个挂载点 = 两个客户端；design.md 8.5、8.6、3.5.3）。锁与 mmap 的系统调用用
//! python3（测试里不写 unsafe，0.2 第 4 条）：
//! - **F_GETLK 的小 spike**（3.5.3、附录 B ③）：别的挂载持有 [100, 文件尾) 的写锁，F_GETLK 回 `l_type = F_WRLCK`、`l_start = 100`、
//!   `l_len = 0`（到文件尾 = 内核的 OFFSET_MAX）；不相交的区间回 `F_UNLCK`。
//! - 只读 fd 的 close 释放这个进程的 POSIX 锁（只读打开不再回 `FOPEN_NOFLUSH`，8.7 的 M12 注记）；flock 跨挂载点互斥；OFD 锁
//!   （`F_OFD_SETLK`）在最后一个 close 时释放、dup 出来的 fd 还开着就不释放（M12 审查：release 时按句柄解锁，`client/locks.rs`）。
//! - **锁即同步点、在锁回调里调用内核通知不出问题**（8.6 要求的测试）：两个挂载上的进程各自把 fd 一直开着（页缓存里有旧内容），
//!   交替地加锁、读计数器、加一、写回、解锁（`lock_counter.py`）；拿到锁的那一刻（setlk 回调里或 lock-wait 线程上）`inval_inode`，读到的
//!   一定是对方刚提交的值——fcntl 与 flock 各跑一遍，结果恰好是总次数，并且不超时（没有死锁）。另有一项专看**没有竞争、立即拿到**的那条
//!   路（不经 lock-wait 线程）：B 的页缓存里是旧内容，A 加锁写完放锁之后，B 的 F_SETLK 立即成功、读到新内容。
//! - 17 个进程同时 F_SETLKW 等同一把锁时（按条件等它们都卡在 FUSE 请求里），挂载点上的 `ls` 照样立即返回；等锁的进程被 kill -9 时
//!   1.5 s 内退出（lock-wait 线程下一次重试时看到 SIGKILL），之后锁照常给别人。
//! - 写出一直失败（fail point）时：显式解锁照样成功、拿着锁 close 也照样释放，错误留给写者的 close（M12 审查）。
//! - strong：`mmap(MAP_SHARED)` → `ENODEV`；打开之后才用 `fcntl(F_SETFL)` 加上 `O_APPEND` 的两个挂载并发追加互不覆盖（按每次 WRITE 的标志）。

mod common;

use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use common::{Node, sh, wait_for};
use wmfs_testkit::harness::node::node_dir;

fn wmfs(n: &Node, args: &str) -> (i32, String) {
    let key = node_dir(&n.dir).join("keys/admin.key");
    sh(&n.dir, &format!("{} {args} --key-file {}", env!("CARGO_BIN_EXE_wmfs"), key.display()))
}

/// 写出 python3 程序 `<name>.py`（`argv[1..]` = `args`）并跑完它。`name` 不要与标准库模块同名（脚本所在目录在 sys.path 最前面）。
fn py(n: &Node, name: &str, code: &str, args: &[String]) -> (i32, String) {
    let script = n.dir.join(format!("{name}.py"));
    std::fs::write(&script, code).unwrap();
    sh(&n.dir, &format!("timeout 180 python3 {} {}", script.display(), args.join(" ")))
}

/// 后台跑一条 shell 命令（等锁、持锁的进程；自己加 `timeout`，测试失败时也不留下进程）。
fn spawn(cmd: &str) -> Child {
    Command::new("sh").args(["-c", cmd]).spawn().unwrap()
}

/// 进程 `pid` 正卡在一个 FUSE 请求里等挂载进程回复（例如等锁）。
fn in_fuse(pid: u32) -> bool {
    std::fs::read_to_string(format!("/proc/{pid}/wchan")).is_ok_and(|w| w == "request_wait_answer")
}

/// 另一个进程能不能立即拿到 `path` 上的 flock（flock 与 fcntl 共用一个锁空间，放松 ②：别人拿着哪种锁都拿不到）。
fn free(n: &Node, path: &std::path::Path) -> bool {
    sh(&n.dir, &format!("flock -n {} -c true", path.display())).0 == 0
}

const HOLD: &str = r#"
import fcntl, os, sys, time
fd = os.open(sys.argv[1], os.O_RDWR)
fcntl.lockf(fd, fcntl.LOCK_EX, 0, 100)          # [100, 文件尾)
open(sys.argv[2], "w").close()
while not os.path.exists(sys.argv[3]):
    time.sleep(0.05)
"#;

const GETLK: &str = r#"
import fcntl, os, struct, sys
fd = os.open(sys.argv[1], os.O_RDWR)
fmt = "hhqqi"                                     # struct flock（64 位 Linux）
for start, length in ((0, 0), (200, 0), (0, 50)):
    r = struct.unpack(fmt, fcntl.fcntl(fd, fcntl.F_GETLK, struct.pack(fmt, fcntl.F_WRLCK, 0, start, length, 0)))
    print(r[0], r[2], r[3])
"#;

const RO_CLOSE: &str = r#"
import fcntl, os, sys
fd = os.open(sys.argv[1], os.O_RDONLY)
fcntl.lockf(fd, fcntl.LOCK_SH)
os.close(fd)
fd2 = os.open(sys.argv[2], os.O_RDWR)
fcntl.lockf(fd2, fcntl.LOCK_EX | fcntl.LOCK_NB)  # 只读 fd 关掉之后读锁已经释放：拿得到
print("ok")
"#;

/// OFD 写锁（整个文件）；关掉一个 dup、问另一个挂载拿不拿得到（argv[2]），再关掉最后一个 fd。
const OFD: &str = r#"
import fcntl, os, struct, subprocess, sys
fd = os.open(sys.argv[1], os.O_RDWR)
fd2 = os.dup(fd)
fcntl.fcntl(fd, fcntl.F_OFD_SETLK, struct.pack("hhqqi", fcntl.F_WRLCK, 0, 0, 0, 0))
os.close(fd)
print(subprocess.run(["flock", "-n", sys.argv[2], "-c", "true"]).returncode)
os.close(fd2)
"#;

#[test]
fn getlk_spike_read_only_close_flock_and_ofd_locks_across_mounts() {
    let n = Node::start("locks1");
    let (a, b) = (n.mount("a", &[]), n.mount("b", &[]));
    std::fs::write(a.p("f"), b"x").unwrap();
    let (ready, go) = (n.dir.join("ready"), n.dir.join("go"));
    let args = [a.p("f").display().to_string(), ready.display().to_string(), go.display().to_string()];
    let script = n.dir.join("hold.py");
    std::fs::write(&script, HOLD).unwrap();
    let mut holder = Command::new("timeout").args(["120", "python3"]).arg(&script).args(&args).spawn().unwrap();
    assert!(wait_for(20, || ready.exists()), "持锁的进程拿到锁");
    let (rc, out) = py(&n, "getlk", GETLK, &[b.p("f").display().to_string()]);
    assert_eq!(rc, 0, "{out}");
    let want = "1 100 0\n1 100 0\n2 0 50\n"; // F_WRLCK = 1、F_UNLCK = 2；冲突锁 l_start = 100、l_len = 0
    assert_eq!(out, want, "F_GETLK 看得到到文件尾（l_len = 0）的冲突锁；不相交的区间回 F_UNLCK");
    std::fs::write(&go, b"").unwrap();
    holder.wait().unwrap();
    let (rc, out) = py(&n, "ro_close", RO_CLOSE, &[a.p("f").display().to_string(), b.p("f").display().to_string()]);
    assert_eq!((rc, out.trim()), (0, "ok"), "只读 fd 的 close 释放 POSIX 锁");
    // flock：A 持有时 B 的 flock -n 失败，A 放了之后成功
    let mut h = Command::new("flock").arg(a.p("f")).args(["-c", "sleep 3"]).spawn().unwrap();
    assert!(wait_for(10, || !free(&n, &b.p("f"))));
    h.wait().unwrap();
    assert!(wait_for(10, || free(&n, &b.p("f"))));
    // OFD 锁：dup 出来的 fd 还开着时不释放；最后一个 close（RELEASE）之后释放
    let (rc, out) = py(&n, "ofd", OFD, &[a.p("f").display().to_string(), b.p("f").display().to_string()]);
    assert_eq!((rc, out.trim()), (0, "1"), "还有一个 fd 开着：OFD 锁还在");
    assert!(wait_for(10, || free(&n, &b.p("f"))), "最后一个 fd 关掉之后 OFD 锁释放了");
    a.unmount();
    b.unmount();
    n.stop();
}

/// B 一直开着文件、页缓存里是旧内容；测试让 A 写完（argv[2] 出现）之后，F_SETLK 立即拿到锁再读。
const NB_READER: &str = r#"
import fcntl, os, sys, time
fd = os.open(sys.argv[1], os.O_RDWR)
os.pread(fd, 100, 0)
open(sys.argv[2], "w").close()
while not os.path.exists(sys.argv[3]):
    time.sleep(0.02)
fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)  # 没有竞争：立即拿到（setlk 回调里 inval_inode，不经 lock-wait 线程）
print(os.pread(fd, 100, 0).decode(), end="")
"#;

const NB_WRITER: &str = r#"
import fcntl, os, sys
fd = os.open(sys.argv[1], os.O_RDWR)
fcntl.lockf(fd, fcntl.LOCK_EX)
os.pwrite(fd, b"new-value-longer", 0)
fcntl.lockf(fd, fcntl.LOCK_UN)                   # 解锁之前先提交
os.close(fd)
"#;

#[test]
fn locks_are_sync_points_and_notifying_the_kernel_inside_lock_callbacks_does_not_hang() {
    let n = Node::start("locks2");
    let (a, b) = (n.mount("a", &[]), n.mount("b", &[]));
    std::fs::write(a.p("cnt"), b"0").unwrap();
    let script = n.dir.join("lock_counter.py");
    std::fs::write(&script, include_str!("lock_counter.py")).unwrap();
    for (kind, total) in [("fcntl", 200), ("flock", 400)] {
        let run = |p: std::path::PathBuf| {
            Command::new("timeout").args(["180", "python3"]).arg(&script).arg(p).args([kind, "100"]).spawn().unwrap()
        };
        let (mut x, mut y) = (run(a.p("cnt")), run(b.p("cnt")));
        assert!(x.wait().unwrap().success() && y.wait().unwrap().success(), "{kind}：两边都在时限内做完（没有死锁）");
        assert_eq!(std::fs::read_to_string(a.p("cnt")).unwrap(), total.to_string(), "{kind}：计数器恰好是总次数");
    }
    // 没有竞争、立即拿到锁的那条路也是同步点：属性缓存 10 s，不靠缓存过期
    std::fs::create_dir(a.p("c")).unwrap();
    let (rc, out) = wmfs(&n, &format!("set {} consistency=cto,attr_ttl=10s", a.p("c").display()));
    assert_eq!(rc, 0, "{out}");
    std::fs::write(a.p("c/nb"), b"old-value").unwrap();
    let (ready, go) = (n.dir.join("nb-ready"), n.dir.join("nb-go"));
    let reader = n.dir.join("nb_reader.py");
    std::fs::write(&reader, NB_READER).unwrap();
    let mut cmd = Command::new("timeout");
    cmd.args(["60", "python3"]).arg(&reader).arg(b.p("c/nb")).arg(&ready).arg(&go).stdout(Stdio::piped());
    let r = cmd.spawn().unwrap();
    assert!(wait_for(20, || ready.exists()), "B 读过一次（页缓存里是旧内容）");
    let (rc, out) = py(&n, "nb_writer", NB_WRITER, &[a.p("c/nb").display().to_string()]);
    assert_eq!(rc, 0, "{out}");
    std::fs::write(&go, b"").unwrap();
    let o = r.wait_with_output().unwrap();
    assert_eq!(
        String::from_utf8_lossy(&o.stdout),
        "new-value-longer",
        "F_SETLK 立即拿到锁之后读到 A 放锁之前提交的内容"
    );
    a.unmount();
    b.unmount();
    n.stop();
}

#[test]
fn many_waiters_do_not_block_the_mount_and_a_killed_waiter_gives_up() {
    let n = Node::start("locks3");
    let (a, b) = (n.mount("a", &[]), n.mount("b", &[]));
    let lk = a.p("lk").display().to_string();
    std::fs::write(&lk, b"").unwrap();
    let go = n.dir.join("go").display().to_string();
    let mut holder = spawn(&format!("timeout 120 flock {lk} -c 'while [ ! -e {go} ]; do sleep 0.05; done'"));
    assert!(wait_for(10, || !free(&n, &b.p("lk"))), "持锁的进程先拿到锁");
    let mut waiters: Vec<Child> =
        (0..17).map(|_| Command::new("flock").arg(&lk).args(["-c", "true"]).spawn().unwrap()).collect();
    assert!(wait_for(30, || waiters.iter().all(|w| in_fuse(w.id()))), "17 个进程都卡在 F_SETLKW 里等锁");
    let t0 = Instant::now();
    let (rc, _) = sh(&n.dir, &format!("ls {}", a.mp.display()));
    let took = t0.elapsed();
    assert!(rc == 0 && took < Duration::from_secs(1), "17 个进程在等锁时 ls 立即返回（用了 {took:?}）");
    // 等锁的进程被 kill -9：卡在内核里等我们回复；lock-wait 线程下一次重试（≤ 1 s）时看到 SIGKILL、回 EINTR，它就退出了
    let mut doomed = Command::new("flock").arg(b.p("lk")).args(["-c", "true"]).spawn().unwrap();
    assert!(wait_for(10, || in_fuse(doomed.id())));
    let t0 = Instant::now();
    doomed.kill().unwrap();
    assert!(wait_for(5, || doomed.try_wait().unwrap().is_some()));
    let took = t0.elapsed();
    assert!(took < Duration::from_millis(1500), "被 kill -9 的等锁进程 {took:?} 才退出");
    std::fs::write(&go, b"").unwrap();
    assert!(holder.wait().unwrap().success());
    for w in &mut waiters {
        assert!(w.wait().unwrap().success(), "放锁之后等锁的进程都拿到了锁");
    }
    assert!(wait_for(10, || free(&n, &b.p("lk"))), "锁没有留给被杀掉的进程");
    a.unmount();
    b.unmount();
    n.stop();
}

/// 打开之后才用 F_SETFL 加上 O_APPEND，写 argv[3] 行 "argv[2] i"。
const SETFL_APPEND: &str = r#"
import fcntl, os, sys
fd = os.open(sys.argv[1], os.O_WRONLY)
fcntl.fcntl(fd, fcntl.F_SETFL, fcntl.fcntl(fd, fcntl.F_GETFL) | os.O_APPEND)
for i in range(int(sys.argv[3])):
    os.write(fd, f"{sys.argv[2]} {i}\n".encode())
os.close(fd)
"#;

#[test]
fn strong_files_refuse_shared_mmap_and_appends_follow_each_writes_flags() {
    let n = Node::start("locks4");
    let (a, b) = (n.mount("a", &[]), n.mount("b", &[]));
    std::fs::create_dir(a.p("s")).unwrap();
    let (rc, out) = wmfs(&n, &format!("set {} consistency=strong", a.p("s").display()));
    assert_eq!(rc, 0, "{out}");
    std::fs::write(a.p("s/x"), b"v2\n").unwrap();
    let mmap = "import mmap,os,sys\nf=os.open(sys.argv[1],os.O_RDWR)\ntry:\n    mmap.mmap(f,0)\nexcept OSError as e:\n    print(e.errno)\nm=mmap.mmap(f,0,access=mmap.ACCESS_COPY)\nprint(m[:2])\n";
    let (rc, out) = py(&n, "shared_mmap", mmap, &[a.p("s/x").display().to_string()]);
    assert_eq!((rc, out.as_str()), (0, "19\nb'v2'\n"), "MAP_SHARED → ENODEV（19）；MAP_PRIVATE 可用");
    std::fs::write(a.p("s/ap"), b"").unwrap();
    let script = n.dir.join("setfl.py");
    std::fs::write(&script, SETFL_APPEND).unwrap();
    let run = |p: std::path::PathBuf, tag: &str| {
        Command::new("python3").arg(&script).arg(p).args([tag, "100"]).spawn().unwrap()
    };
    let (mut x, mut y) = (run(a.p("s/ap"), "A"), run(b.p("s/ap"), "B"));
    assert!(x.wait().unwrap().success() && y.wait().unwrap().success());
    let text = std::fs::read_to_string(a.p("s/ap")).unwrap();
    for tag in ["A", "B"] {
        let mine: Vec<&str> = text.lines().filter(|l| l.starts_with(tag)).collect();
        let want: Vec<String> = (0..100).map(|i| format!("{tag} {i}")).collect();
        assert_eq!(mine, want, "F_SETFL 加上的 O_APPEND 也经 AppendReserve：{tag} 的 100 行一行不丢、各自有序");
    }
    a.unmount();
    b.unmount();
    n.stop();
}

/// 拿着锁写 30 万字节（超过 inline 上限，close 时按 chunk 写出、在提交之前失败），argv[3] = unlock 时先显式解锁，再 close。
const WRITE_FAILS: &str = r#"
import fcntl, os, sys
fd = os.open(sys.argv[1], os.O_RDWR | os.O_CREAT, 0o644)
lock = fcntl.flock if sys.argv[2] == "flock" else fcntl.lockf
lock(fd, fcntl.LOCK_EX)
os.write(fd, b"x" * 300000)
if sys.argv[3] == "unlock":
    lock(fd, fcntl.LOCK_UN)
    print("unlocked", end=" ")
try:
    os.close(fd)
    print("closed")
except OSError as e:
    print("close", e.errno)
"#;

#[test]
fn a_failing_write_out_neither_keeps_the_lock_nor_hides_the_error() {
    let n = Node::start("locks5");
    let bad = n.mount("bad", &[("WMFS_FAILPOINTS", "client.flush.before_commit=return(EIO)")]);
    let ok = n.mount("ok", &[]);
    for (i, (kind, how)) in
        [("flock", "unlock"), ("fcntl", "unlock"), ("flock", "close"), ("fcntl", "close")].iter().enumerate()
    {
        let f = format!("w{i}");
        let (rc, out) =
            py(&n, "wfail", WRITE_FAILS, &[bad.p(&f).display().to_string(), kind.to_string(), how.to_string()]);
        let want = if *how == "unlock" { "unlocked close 5" } else { "close 5" };
        assert_eq!((rc, out.trim()), (0, want), "{kind}/{how}：解锁成功，写出的错误留给写者的 close（EIO）");
        assert!(wait_for(10, || free(&n, &ok.p(&f))), "{kind}/{how}：另一个挂载随后拿得到锁");
    }
    bad.unmount();
    ok.unmount();
    n.stop();
}
