//! libc 的安全封装（design.md 0.2 第 4 条）。
//!
//! 全仓库除 `buf/pool.rs`、`hooks/cabi.rs`、`plugins/rdma/` 外，只有这个文件允许写 `unsafe`；
//! 其他模块与 crate（cli、插件、testkit）只调用这里的安全函数。每处 `unsafe` 都写 `// SAFETY:` 说明。
//! 不引入任何信号处理 crate（ctrlc、signal-hook 等）。
//!
//! 信号的用法（实现注记 M0：拆成两个函数）：`main` 在**创建任何线程之前**调用
//! `block_signals(&[SIGTERM, SIGINT])`，之后创建的线程都继承这个屏蔽；`main` 最后调用
//! `wait_signals(&[SIGTERM, SIGINT])` 阻塞等待。若只在创建线程之后再屏蔽，信号可能投递到别的线程、按默认动作杀掉进程。

use std::ffi::CString;
use std::fs::File;
use std::net::TcpStream;
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

use super::ErrCode;

pub use libc::{SIGCONT, SIGINT, SIGKILL, SIGSTOP, SIGTERM};

fn last_err() -> ErrCode {
    ErrCode::from(std::io::Error::last_os_error())
}

/// 文件系统容量（字节）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FsStat {
    pub total_bytes: u64,
    pub free_bytes: u64,  // 含只给 root 用的保留块
    pub avail_bytes: u64, // 普通用户可用
}

/// `statvfs(path)`：local-fs 的 statfs 与 `wmfs doctor` 用。
pub fn statvfs(path: &Path) -> Result<FsStat, ErrCode> {
    let c = CString::new(path.as_os_str().as_bytes()).map_err(|_| ErrCode::EINVAL)?;
    // SAFETY: libc::statvfs 是只含整数字段的 C 结构体，全零是合法的初始值。
    let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: c 是以 NUL 结尾的有效路径，st 是本地可写结构体；statvfs 只读 c、只写 st。
    let r = unsafe { libc::statvfs(c.as_ptr(), &mut st) };
    if r != 0 {
        return Err(last_err());
    }
    let f = st.f_frsize as u64;
    Ok(FsStat {
        total_bytes: st.f_blocks as u64 * f,
        free_bytes: st.f_bfree as u64 * f,
        avail_bytes: st.f_bavail as u64 * f,
    })
}

/// 在内核里把 `src[src_off..]` 复制到 `dst[dst_off..]`，最多 `len` 字节，返回实际复制的字节数
/// （可能少于 len；`EXDEV/ENOSYS/EOPNOTSUPP` 时调用方退回 pread/pwrite，design.md 3.2.5 R1）。
pub fn copy_file_range(src: &File, src_off: u64, dst: &File, dst_off: u64, len: usize) -> Result<usize, ErrCode> {
    let mut si = src_off as libc::loff_t;
    let mut di = dst_off as libc::loff_t;
    // SAFETY: 两个 fd 在调用期间由 &File 保证有效；偏移指针指向本地变量；flags 必须为 0。
    let n = unsafe { libc::copy_file_range(src.as_raw_fd(), &mut si, dst.as_raw_fd(), &mut di, len, 0) };
    if n < 0 { Err(last_err()) } else { Ok(n as usize) }
}

/// 用内核随机数填满 `buf`（`rand::fill` 的底层）。
pub fn getrandom(buf: &mut [u8]) -> Result<(), ErrCode> {
    let mut done = 0;
    while done < buf.len() {
        let rest = &mut buf[done..];
        // SAFETY: 指针与长度来自同一个可写切片；flags 0 = 阻塞直到内核熵池就绪。
        let n = unsafe { libc::getrandom(rest.as_mut_ptr().cast(), rest.len(), 0) };
        if n < 0 {
            let e = last_err();
            if e == ErrCode::EINTR {
                continue;
            }
            return Err(e);
        }
        done += n as usize;
    }
    Ok(())
}

/// fsync 一个目录（rename 产生的目录项要靠它才持久，design.md 3.2.1、3.2.5）。不需要 unsafe。
pub fn fsync_dir(dir: &Path) -> Result<(), ErrCode> {
    File::open(dir)?.sync_all()?;
    Ok(())
}

/// 唯一的原子替换工具（design.md 3.2.1）：写 `<dir>/.<name>.tmp-<随机>` → fdatasync → rename 覆盖 → fsync(目录)。
/// 崩溃后要么是旧内容、要么是新内容。文件权限 0600（NODE、密钥、超级块、任务游标都只给本用户读）。不需要 unsafe。
pub fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), ErrCode> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let dir = path.parent().filter(|d| !d.as_os_str().is_empty()).unwrap_or(Path::new("."));
    let name = path.file_name().ok_or(ErrCode::EINVAL)?.to_string_lossy();
    let tmp = dir.join(format!(".{name}.tmp-{:016x}", super::rand::random_u64()));
    let write = || -> std::io::Result<()> {
        let mut f = std::fs::OpenOptions::new().write(true).create_new(true).mode(0o600).open(&tmp)?;
        f.write_all(bytes)?;
        f.sync_data()?;
        std::fs::rename(&tmp, path)
    };
    if let Err(e) = write() {
        let _ = std::fs::remove_file(&tmp);
        return Err(e.into());
    }
    fsync_dir(dir)
}

/// 给**一个**进程发信号（`wmfs dev kill/stop/pause/resume` 用；sig = 0 只检查进程是否存在）。
/// pid ≤ 0 一律 `EINVAL`：`kill(0, ..)` 会发给自己所在的整个进程组，`kill(-1, ..)` 会发给当前用户的所有进程——
/// pid 文件为空或损坏时绝不能变成这两种。真要给进程组发信号时，另写一个名字明确的函数。
pub fn send_signal(pid: i32, sig: i32) -> Result<(), ErrCode> {
    if pid <= 0 {
        return Err(ErrCode::EINVAL);
    }
    // SAFETY: kill 只接收两个整数参数，不涉及任何内存；pid > 0 保证只作用于这一个进程。
    let r = unsafe { libc::kill(pid, sig) };
    if r != 0 { Err(last_err()) } else { Ok(()) }
}

/// 进程是否存在（EPERM 表示存在但不是我们的）；pid ≤ 0 返回 false。
pub fn process_alive(pid: i32) -> bool {
    match send_signal(pid, 0) {
        Ok(()) => true,
        Err(e) => e == ErrCode::EPERM,
    }
}

fn sigset(sigs: &[i32]) -> libc::sigset_t {
    // SAFETY: sigset_t 是普通 C 结构体，先清零，再由 sigemptyset/sigaddset 按 POSIX 初始化；参数都是本地变量。
    unsafe {
        let mut set: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&mut set);
        for s in sigs {
            libc::sigaddset(&mut set, *s);
        }
        set
    }
}

/// 在当前线程屏蔽这些信号；之后由它创建的线程都继承屏蔽。必须在创建任何线程之前调用。
pub fn block_signals(sigs: &[i32]) -> Result<(), ErrCode> {
    let set = sigset(sigs);
    // SAFETY: set 已初始化；不需要旧掩码，所以第三个参数传空指针（POSIX 允许）。
    let r = unsafe { libc::pthread_sigmask(libc::SIG_BLOCK, &set, std::ptr::null_mut()) };
    if r != 0 { Err(ErrCode(r as u16)) } else { Ok(()) }
}

/// 阻塞等待其中一个（已被屏蔽的）信号，返回信号编号（main 线程收到 SIGTERM 后开始优雅退出）。
pub fn wait_signals(sigs: &[i32]) -> Result<i32, ErrCode> {
    let set = sigset(sigs);
    let mut sig: libc::c_int = 0;
    // SAFETY: set 已初始化，sig 指向本地变量，sigwait 只写它。
    let r = unsafe { libc::sigwait(&set, &mut sig) };
    if r != 0 { Err(ErrCode(r as u16)) } else { Ok(sig) }
}

/// 打开 TCP keepalive（tcp 插件建连后调用，插件里不写 unsafe）。
pub fn set_keepalive(s: &TcpStream) -> Result<(), ErrCode> {
    let on: libc::c_int = 1;
    let len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
    // SAFETY: fd 由 &TcpStream 保证有效；optval 指向本地 c_int，optlen 与之一致；setsockopt 只读它。
    let r = unsafe {
        libc::setsockopt(s.as_raw_fd(), libc::SOL_SOCKET, libc::SO_KEEPALIVE, (&on as *const libc::c_int).cast(), len)
    };
    if r != 0 { Err(last_err()) } else { Ok(()) }
}

/// RLIMIT_MEMLOCK 的软限制（字节）；`Ok(None)` = 不限。RDMA 要把整个内存池注册（锁定）进网卡（design.md 3.6.1）。
pub fn memlock_limit() -> Result<Option<u64>, ErrCode> {
    let mut rl = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
    // SAFETY: rl 是本地结构体，getrlimit 只写它。
    let r = unsafe { libc::getrlimit(libc::RLIMIT_MEMLOCK, &mut rl) };
    if r != 0 {
        return Err(last_err());
    }
    Ok(if rl.rlim_cur == libc::RLIM_INFINITY { None } else { Some(rl.rlim_cur) })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};

    #[test]
    fn statvfs_root() {
        let s = statvfs(Path::new("/")).unwrap();
        assert!(s.total_bytes > 0 && s.avail_bytes <= s.free_bytes && s.free_bytes <= s.total_bytes);
        assert_eq!(statvfs(Path::new("/no/such/dir")), Err(ErrCode::ENOENT));
    }

    #[test]
    fn copy_file_range_and_fsync_dir() {
        let dir = crate::base::test_dir("sys-cfr");
        let (a, b) = (dir.join("a"), dir.join("b"));
        std::fs::write(&a, b"0123456789").unwrap();
        let src = File::open(&a).unwrap();
        let dst = std::fs::OpenOptions::new().create(true).truncate(true).write(true).read(true).open(&b).unwrap();
        let n = copy_file_range(&src, 2, &dst, 4, 5).unwrap();
        assert_eq!(n, 5);
        let mut got = Vec::new();
        File::open(&b).unwrap().read_to_end(&mut got).unwrap();
        assert_eq!(got, b"\x00\x00\x00\x0023456", "目标前 4 字节是洞（读为 0）");
        fsync_dir(&dir).unwrap();
        assert_eq!(fsync_dir(&dir.join("missing")), Err(ErrCode::ENOENT));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn atomic_write_replaces_whole_file() {
        use std::os::unix::fs::PermissionsExt;
        let dir = crate::base::test_dir("sys-aw");
        let p = dir.join("NODE");
        atomic_write(&p, b"old").unwrap();
        atomic_write(&p, b"new content").unwrap();
        assert_eq!(std::fs::read(&p).unwrap(), b"new content");
        assert_eq!(std::fs::metadata(&p).unwrap().permissions().mode() & 0o777, 0o600);
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 1, "不留临时文件");
        assert!(atomic_write(&dir.join("no/such/dir/x"), b"x").is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn random_bytes() {
        let mut a = [0u8; 64];
        getrandom(&mut a).unwrap();
        assert_ne!(a, [0u8; 64]);
        getrandom(&mut []).unwrap();
    }

    #[test]
    fn signals_to_processes() {
        let me = std::process::id() as i32;
        assert!(process_alive(me));
        assert!(!process_alive(i32::MAX - 1));
        assert_eq!(send_signal(i32::MAX - 1, 0), Err(ErrCode(libc::ESRCH as u16)));
        // pid ≤ 0 会作用于整个进程组或全部进程，必须拒绝（这里只用信号 0，不会真的发出任何信号）
        assert_eq!(send_signal(0, 0), Err(ErrCode::EINVAL));
        assert_eq!(send_signal(-1, 0), Err(ErrCode::EINVAL));
        assert_eq!(send_signal(i32::MIN, 0), Err(ErrCode::EINVAL));
        assert!(!process_alive(0) && !process_alive(-1));
    }

    #[test]
    fn block_then_wait_in_one_thread() {
        // 在一个单独的线程里屏蔽 SIGUSR1、给自己这个线程发一个，再用 wait_signals 收下：不影响别的线程。
        std::thread::spawn(|| {
            block_signals(&[libc::SIGUSR1]).unwrap();
            // SAFETY: raise 只把信号发给调用它的线程；该信号已被本线程屏蔽，所以只会挂起等待 sigwait。
            unsafe { libc::raise(libc::SIGUSR1) };
            assert_eq!(wait_signals(&[libc::SIGUSR1]), Ok(libc::SIGUSR1));
        })
        .join()
        .unwrap();
    }

    #[test]
    fn keepalive_and_memlock() {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let mut c = TcpStream::connect(l.local_addr().unwrap()).unwrap();
        set_keepalive(&c).unwrap();
        c.write_all(b"x").unwrap();
        let (mut s, _) = l.accept().unwrap();
        let mut b = [0u8; 1];
        s.read_exact(&mut b).unwrap();
        assert_eq!(&b, b"x");
        let m = memlock_limit().unwrap();
        assert!(m.is_none_or(|v| v > 0));
    }
}
