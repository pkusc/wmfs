//! fuser 的 `Filesystem` 实现（design.md 8.1–8.4）：每个方法只是把内核的请求换成元数据请求或 `FileClient` 的调用，再按 8.7
//! 映射错误码回复内核。方法取 `&self`（fuser 16 个工作线程并发调用），状态都在 `FileClient` 与 `WmfsFs` 的 Mutex 里。
//!
//! - **init**：协商 `FUSE_POSIX_ACL | FUSE_DONT_MASK | FUSE_DO_READDIRPLUS | FUSE_READDIRPLUS_AUTO | FUSE_HANDLE_KILLPRIV_V2` 与
//!   M12 的 `FUSE_POSIX_LOCKS | FUSE_FLOCK_LOCKS`（集群锁，`locks.rs`）中内核支持的那部分，`max_write`、`max_readahead` 1 MiB，
//!   `max_background` 64（为什么这样协商、umask 的 spike `log_mode_umask`：design.md 8.1 与它的 M6 注记 ①②）。
//! - **flush**（每次 close）：写句柄 → 写出脏数据并提交（cto），只读句柄不写出（不替写者报告写错误）；M12 起再释放这个进程（lock owner）
//!   在这个文件上的 POSIX 锁（所以只读打开不再回 `FOPEN_NOFLUSH`，8.7 的 M12 注记）。**release**（最后一个 close）释放记在这个句柄上的
//!   锁（flock 与 OFD 锁，`client/locks.rs`）。**write**：strong 的 `O_APPEND` 按这次 WRITE 带的标志判断（此刻的 f_flags，F_SETFL 之后也对，8.5）。
//! - **open**（C2）：change_id 变了 → 先 `inval_inode(ino, 0, 0)` 再回复、不带 `FOPEN_KEEP_CACHE`；strong 或 `O_DIRECT` → `FOPEN_DIRECT_IO`。
//! - **setattr**：改长度交给 `FileClient::set_size`（带着写句柄时发 TruncateHandle，ADR-0036）；其余属性一个 `SetAttr`。
//! - 不支持的：`ioctl` → `ENOTTY`（chattr），`copy_file_range` → `EOPNOTSUPP`（内核自动退回普通拷贝），`fallocate` 的非 0 模式
//!   → `EOPNOTSUPP`（服务端判），`lseek`、`poll`、`bmap` 用 fuser 默认的 ENOSYS（内核改用自己的通用实现）。

use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::time::SystemTime;

use fuser::{
    CopyFileRangeFlags, Errno, FileHandle, Filesystem, FopenFlags, Generation, INodeNo, InitFlags, IoctlFlags,
    KernelConfig, LockOwner, OpenFlags, RenameFlags, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory,
    ReplyDirectoryPlus, ReplyEmpty, ReplyEntry, ReplyIoctl, ReplyLock, ReplyOpen, ReplyStatfs, ReplyWrite, ReplyXattr,
    Request, TimeOrNow, WriteFlags,
};
use wmfs_core::base::log;
use wmfs_core::client::locks::{LockReq, setlock_kind};
use wmfs_core::meta::ns::common::{MODE_MASK, kind_from_mode};
use wmfs_core::proto::meta::{MetaOp, MetaReply, SetAttrIn};

use super::WmfsFs;
use super::ops::{fopen, name, time_set};
use super::util::{errno, is_virtual};

impl Filesystem for WmfsFs {
    fn init(&mut self, _req: &Request, cfg: &mut KernelConfig) -> std::io::Result<()> {
        let want = InitFlags::FUSE_POSIX_ACL
            | InitFlags::FUSE_DONT_MASK
            | InitFlags::FUSE_DO_READDIRPLUS
            | InitFlags::FUSE_READDIRPLUS_AUTO
            | InitFlags::FUSE_HANDLE_KILLPRIV_V2
            | InitFlags::FUSE_POSIX_LOCKS
            | InitFlags::FUSE_FLOCK_LOCKS;
        let _ = cfg.add_capabilities(want & cfg.capabilities());
        let _ = cfg.set_max_write(1 << 20);
        let _ = cfg.set_max_readahead(1 << 20);
        let _ = cfg.set_max_background(64);
        Ok(())
    }

    fn destroy(&mut self) {
        let bad = self.fc.flush_all(); // 卸载（或进程退出）之前把全部脏数据写出去
        if bad > 0 {
            log::error("fuse", format_args!("卸载时有 {bad} 个文件的数据没能写出去"));
        }
    }

    fn lookup(&self, req: &Request, parent: INodeNo, n: &OsStr, reply: ReplyEntry) {
        let r = self.fc.meta.read(&self.who(req), MetaOp::Lookup { parent: parent.0, name: name(n) });
        self.entry(reply, self.note_dir(parent.0, r));
    }

    fn forget(&self, _req: &Request, ino: INodeNo, _nlookup: u64) {
        self.fc.forget(ino.0);
    }

    fn getattr(&self, req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        match self.attr(req, ino.0) {
            Ok(a) => {
                let (ttl, fa) = self.fattr(a);
                reply.attr(&ttl, &fa);
            }
            Err(e) => reply.error(errno(e)),
        }
    }

    fn setattr(
        &self,
        req: &Request,
        ino: INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<fuser::BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        let set = SetAttrIn {
            mode: mode.map(|m| m & MODE_MASK),
            uid,
            gid,
            size: None,
            atime: time_set(atime),
            mtime: time_set(mtime),
            epoch_seen: 0,
        };
        self.do_setattr(req, ino.0, size, fh.map(|f| f.0), set, reply);
    }

    fn readlink(&self, req: &Request, ino: INodeNo, reply: ReplyData) {
        match self.fc.meta.read(&self.who(req), MetaOp::ReadLink { ino: ino.0 }) {
            Ok(MetaReply::Link { target }) => reply.data(&target),
            Ok(_) => reply.error(Errno::EIO),
            Err(e) => reply.error(errno(e)),
        }
    }

    fn mknod(&self, req: &Request, parent: INodeNo, n: &OsStr, mode: u32, umask: u32, rdev: u32, reply: ReplyEntry) {
        self.log_mode_umask("mknod", n, mode, umask);
        let Some(kind) = kind_from_mode(mode) else { return reply.error(Errno::EINVAL) };
        let op = MetaOp::Mknod { parent: parent.0, name: name(n), mode: mode & MODE_MASK, umask, kind, rdev };
        self.entry(reply, self.fc.meta.write(&self.who(req), op));
    }

    fn mkdir(&self, req: &Request, parent: INodeNo, n: &OsStr, mode: u32, umask: u32, reply: ReplyEntry) {
        self.log_mode_umask("mkdir", n, mode, umask);
        let op = MetaOp::Mkdir { parent: parent.0, name: name(n), mode: mode & MODE_MASK, umask };
        let r = self.fc.meta.write(&self.who(req), op);
        self.entry(reply, self.note_dir(parent.0, r));
    }

    fn unlink(&self, req: &Request, parent: INodeNo, n: &OsStr, reply: ReplyEmpty) {
        let r = self.fc.meta.write(&self.who(req), MetaOp::Unlink { parent: parent.0, name: name(n) });
        self.empty(reply, r.map(|_| ()));
    }

    fn rmdir(&self, req: &Request, parent: INodeNo, n: &OsStr, reply: ReplyEmpty) {
        let r = self.fc.meta.write(&self.who(req), MetaOp::Rmdir { parent: parent.0, name: name(n) });
        self.empty(reply, r.map(|_| ()));
    }

    fn symlink(&self, req: &Request, parent: INodeNo, n: &OsStr, target: &Path, reply: ReplyEntry) {
        let op = MetaOp::Symlink { parent: parent.0, name: name(n), target: target.as_os_str().as_bytes().to_vec() };
        self.entry(reply, self.fc.meta.write(&self.who(req), op));
    }

    fn rename(
        &self,
        req: &Request,
        parent: INodeNo,
        n: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        flags: RenameFlags,
        reply: ReplyEmpty,
    ) {
        let op = MetaOp::Rename {
            parent: parent.0,
            name: name(n),
            new_parent: newparent.0,
            new_name: name(newname),
            flags: flags.bits(),
        };
        let who = self.who(req);
        let r = self.fc.meta.write(&who, op).map(|_| ());
        if r.is_ok() && newparent != parent {
            self.note_moved_dir(&who, newparent.0, newname); // 换了父目录的目录：记下新的父目录（快照里的 ..）
            if flags.contains(RenameFlags::RENAME_EXCHANGE) {
                self.note_moved_dir(&who, parent.0, n);
            }
        }
        self.empty(reply, r);
    }

    fn link(&self, req: &Request, ino: INodeNo, newparent: INodeNo, newname: &OsStr, reply: ReplyEntry) {
        let op = MetaOp::Link { ino: ino.0, new_parent: newparent.0, new_name: name(newname) };
        self.entry(reply, self.fc.meta.write(&self.who(req), op));
    }

    fn open(&self, req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        self.do_open(req, ino, flags, reply);
    }

    fn read(
        &self,
        req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyData,
    ) {
        let r = self.fc.handle(fh.0).and_then(|h| self.fc.read(&self.who(req), &h, offset, size));
        match r {
            Ok(d) => reply.data(&d),
            Err(e) => reply.error(errno(e)),
        }
    }

    fn write(
        &self,
        req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
        write_flags: WriteFlags,
        flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyWrite,
    ) {
        let (who, kill) = (self.who(req), write_flags.contains(WriteFlags::FUSE_WRITE_KILL_SUIDGID));
        let r = self.fc.handle(fh.0).and_then(|h| {
            let h = h.with_write_flags(flags.0 as u32); // 这次 WRITE 的标志（见文件头）
            if kill {
                self.fc.commit_to_drop_suid(&who, &h)?; // 去掉 setuid/setgid 要马上看得到（M10，`commit_to_drop_suid`）
            }
            self.fc.write(&who, &h, offset, data)
        });
        match r {
            Ok(n) => reply.written(n),
            Err(e) => reply.error(errno(e)),
        }
    }

    fn flush(&self, _req: &Request, _ino: INodeNo, fh: FileHandle, lock_owner: LockOwner, reply: ReplyEmpty) {
        let r = self.fc.flush(fh.0); // 每次 close：写句柄写出去并提交（cto）
        self.fc.drop_locks(fh.0, lock_owner.0); // 这个进程在这个文件上的 POSIX 锁随 close 释放（M12）
        self.empty(reply, r);
    }

    fn release(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        self.fc.release_locks(fh.0); // 记在这个句柄上的 flock、OFD 锁随最后一个 close 释放（M12）
        self.fc.release(fh.0);
        reply.ok();
    }

    fn fsync(&self, _req: &Request, _ino: INodeNo, fh: FileHandle, _datasync: bool, reply: ReplyEmpty) {
        self.empty(reply, self.fc.fsync(fh.0));
    }

    fn opendir(&self, req: &Request, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        match self.fc.open_dir(&self.who(req), ino.0) {
            Ok(fh) => reply.opened(FileHandle(fh), FopenFlags::empty()),
            Err(e) => reply.error(errno(e)),
        }
    }

    fn readdir(&self, _req: &Request, _ino: INodeNo, fh: FileHandle, offset: u64, reply: ReplyDirectory) {
        self.do_readdir(fh.0, offset, reply);
    }

    fn readdirplus(&self, _req: &Request, _ino: INodeNo, fh: FileHandle, offset: u64, reply: ReplyDirectoryPlus) {
        self.do_readdirplus(fh.0, offset, reply);
    }

    fn releasedir(&self, _req: &Request, _ino: INodeNo, fh: FileHandle, _flags: OpenFlags, reply: ReplyEmpty) {
        self.fc.close_dir(fh.0);
        reply.ok();
    }

    fn fsyncdir(&self, _req: &Request, _ino: INodeNo, fh: FileHandle, _datasync: bool, reply: ReplyEmpty) {
        // 目录的修改在回复之前已经在元数据多数派落盘；被驱逐的会话里打开的目录句柄 → EIO（与 readdir 相同，ADR-0041 第 9 条）
        self.empty(reply, self.fc.dir(fh.0).map(|_| ()));
    }

    fn statfs(&self, req: &Request, _ino: INodeNo, reply: ReplyStatfs) {
        self.do_statfs(req, reply);
    }

    fn getlk(
        &self,
        req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        lock_owner: LockOwner,
        start: u64,
        end: u64,
        typ: i32,
        _pid: u32,
        reply: ReplyLock,
    ) {
        let Some(kind) = setlock_kind(typ) else { return reply.error(Errno::EINVAL) };
        self.do_getlk(req, fh.0, LockReq { owner: lock_owner.0, start, end, kind, pid: 0 }, reply);
    }

    fn setlk(
        &self,
        req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        lock_owner: LockOwner,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
        sleep: bool,
        reply: ReplyEmpty,
    ) {
        let Some(kind) = setlock_kind(typ) else { return reply.error(Errno::EINVAL) };
        self.do_setlk(req, ino, fh.0, LockReq { owner: lock_owner.0, start, end, kind, pid }, sleep, reply);
    }

    fn setxattr(
        &self,
        req: &Request,
        ino: INodeNo,
        n: &OsStr,
        value: &[u8],
        flags: i32,
        _position: u32,
        reply: ReplyEmpty,
    ) {
        if is_virtual(ino.0, n.as_bytes()) {
            return reply.error(Errno::EPERM);
        }
        let op = MetaOp::SetXattr { ino: ino.0, name: name(n), value: value.to_vec(), flags: flags as u32 };
        self.empty(reply, self.fc.meta.write(&self.who(req), op).map(|_| ()));
    }

    fn getxattr(&self, req: &Request, ino: INodeNo, n: &OsStr, size: u32, reply: ReplyXattr) {
        self.do_getxattr(req, ino.0, n, size, reply);
    }

    fn listxattr(&self, req: &Request, ino: INodeNo, size: u32, reply: ReplyXattr) {
        self.do_listxattr(req, ino.0, size, reply);
    }

    fn removexattr(&self, req: &Request, ino: INodeNo, n: &OsStr, reply: ReplyEmpty) {
        if is_virtual(ino.0, n.as_bytes()) {
            return reply.error(Errno::EPERM);
        }
        let r = self.fc.meta.write(&self.who(req), MetaOp::RemoveXattr { ino: ino.0, name: name(n) });
        self.empty(reply, r.map(|_| ()));
    }

    fn create(&self, req: &Request, parent: INodeNo, n: &OsStr, mode: u32, umask: u32, flags: i32, reply: ReplyCreate) {
        self.log_mode_umask("create", n, mode, umask);
        match self.fc.create(&self.who(req), parent.0, n.as_bytes(), mode & MODE_MASK, umask, flags as u32) {
            Ok(o) => {
                let (ttl, fa) = self.fattr(o.attr);
                reply.created(&ttl, &fa, Generation(0), FileHandle(o.fh), fopen(false, o.direct));
            }
            Err(e) => reply.error(errno(e)),
        }
    }

    fn fallocate(
        &self,
        req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        length: u64,
        mode: i32,
        reply: ReplyEmpty,
    ) {
        let who = self.who(req);
        let r = self.fc.handle(fh.0).and_then(|h| self.fc.fallocate(&who, &h, offset, length, mode as u32));
        self.empty(reply, r.map(|_| ()));
    }

    fn ioctl(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _flags: IoctlFlags,
        _cmd: u32,
        _in_data: &[u8],
        _out_size: u32,
        reply: ReplyIoctl,
    ) {
        reply.error(Errno::ENOTTY); // chattr 标志不支持（3.1.1）
    }

    fn copy_file_range(
        &self,
        _req: &Request,
        _ino_in: INodeNo,
        _fh_in: FileHandle,
        _offset_in: u64,
        _ino_out: INodeNo,
        _fh_out: FileHandle,
        _offset_out: u64,
        _len: u64,
        _flags: CopyFileRangeFlags,
        reply: ReplyWrite,
    ) {
        reply.error(Errno::EOPNOTSUPP); // 内核自动退回普通拷贝（3.1.1）
    }
}
