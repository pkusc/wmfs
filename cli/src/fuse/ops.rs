//! FUSE 回调里稍长的那几段（`fs.rs` 的 `Filesystem` 实现只做分派，每个文件 ≤ 400 行）：回复的公共写法、setattr（改长度交给
//! `FileClient::set_size`，带着写句柄时发 TruncateHandle，ADR-0036；其余属性一个 SetAttr）、open（C2 的失效与打开标志）、
//! readdir/readdirplus（按 `FileClient` 里目录快照的下标续读）、statfs、xattr（含根目录的虚拟 xattr）。

use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;

use fuser::{
    Errno, FileHandle, FopenFlags, Generation, INodeNo, OpenFlags, ReplyAttr, ReplyDirectory, ReplyDirectoryPlus,
    ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs, ReplyXattr, Request, TimeOrNow,
};
use wmfs_core::base::{ErrCode, log};
use wmfs_core::client::meta_client::Caller;
use wmfs_core::meta::schema::records::KIND_DIR;
use wmfs_core::proto::meta::{AttrOut, MetaOp, MetaReply, SetAttrIn, TimeSet};

use super::WmfsFs;
use super::util::{errno, file_type, is_virtual, ts_of};

pub(super) fn name(n: &OsStr) -> Vec<u8> {
    n.as_bytes().to_vec()
}

pub(super) fn time_set(t: Option<TimeOrNow>) -> Option<TimeSet> {
    t.map(|t| match t {
        TimeOrNow::Now => TimeSet::Now,
        TimeOrNow::SpecificTime(s) => TimeSet::At(ts_of(s)),
    })
}

impl WmfsFs {
    pub(super) fn entry(&self, reply: ReplyEntry, r: Result<MetaReply, ErrCode>) {
        match r {
            Ok(MetaReply::Entry { attr }) => {
                let (ttl, fa) = self.fattr(attr);
                reply.entry(&ttl, &fa, Generation(0));
            }
            Ok(_) => reply.error(Errno::EIO),
            Err(e) => reply.error(errno(e)),
        }
    }

    /// lookup / mkdir 的回复是目录 → 记下它的父目录（readdir 快照里 `..` 的 inode 号，ADR-0037 第 13 条）。
    pub(super) fn note_dir(&self, parent: u64, r: Result<MetaReply, ErrCode>) -> Result<MetaReply, ErrCode> {
        if let Ok(MetaReply::Entry { attr }) = &r
            && attr.kind == KIND_DIR
        {
            self.fc.note_parent(attr.ino, parent);
        }
        r
    }

    /// 自己的 rename 把 `dir/name` 移到了另一个目录：查一下它是不是目录，是就记下新的父目录（查不到就算了：下次 lookup 会记）。
    pub(super) fn note_moved_dir(&self, who: &Caller, dir: u64, n: &OsStr) {
        let _ = self.note_dir(dir, self.fc.meta.read(who, MetaOp::Lookup { parent: dir, name: name(n) }));
    }

    pub(super) fn empty(&self, reply: ReplyEmpty, r: Result<(), ErrCode>) {
        match r {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(errno(e)),
        }
    }

    pub(super) fn attr(&self, req: &Request, ino: u64) -> Result<AttrOut, ErrCode> {
        match self.fc.meta.read(&self.who(req), MetaOp::GetAttr { ino })? {
            MetaReply::Entry { attr } => Ok(attr),
            _ => Err(ErrCode::INTERNAL),
        }
    }

    pub(super) fn xattr_reply(&self, reply: ReplyXattr, size: u32, r: Result<Vec<u8>, ErrCode>) {
        match r {
            Ok(v) if size == 0 => reply.size(v.len() as u32),
            Ok(v) if v.len() > size as usize => reply.error(Errno::ERANGE),
            Ok(v) => reply.data(&v),
            Err(e) => reply.error(errno(e)),
        }
    }

    /// 8.1 的 spike：create/mkdir/mknod 在 debug 级日志里打印内核传来的 mode 与 umask（`WMFS_LOG=debug`）。
    pub(super) fn log_mode_umask(&self, op: &str, name: &OsStr, mode: u32, umask: u32) {
        log::debug("fuse", format_args!("{op} {name:?}：内核传来 mode={mode:o} umask={umask:o}"));
    }

    pub(super) fn do_setattr(
        &self,
        req: &Request,
        ino: u64,
        size: Option<u64>,
        fh: Option<u64>,
        set: SetAttrIn,
        reply: ReplyAttr,
    ) {
        let who = self.who(req);
        let mut last = None;
        if let Some(s) = size {
            match self.fc.set_size(&who, ino, s, fh) {
                Ok(a) => last = Some(a),
                Err(e) => return reply.error(errno(e)),
            }
        }
        if set.mode.is_some() || set.uid.is_some() || set.gid.is_some() || set.atime.is_some() || set.mtime.is_some() {
            match self.fc.meta.write(&who, MetaOp::SetAttr { ino, set }) {
                Ok(MetaReply::Entry { attr }) => last = Some(attr),
                Ok(_) => return reply.error(Errno::EIO),
                Err(e) => return reply.error(errno(e)),
            }
        }
        match last.map_or_else(|| self.attr(req, ino), Ok) {
            Ok(a) => {
                let (ttl, fa) = self.fattr(a);
                reply.attr(&ttl, &fa);
            }
            Err(e) => reply.error(errno(e)),
        }
    }

    pub(super) fn do_open(&self, req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        match self.fc.open(&self.who(req), ino.0, flags.0 as u32) {
            Ok(o) => {
                if !o.keep_cache
                    && let Some(n) = self.notifier.get()
                {
                    let _ = n.inval_inode(ino, 0, 0); // C2：别人改过了，回复 open 之前丢掉属性与页缓存
                }
                reply.opened(FileHandle(o.fh), fopen(o.keep_cache, o.direct));
            }
            Err(e) => reply.error(errno(e)),
        }
    }

    pub(super) fn do_readdir(&self, fh: u64, offset: u64, mut reply: ReplyDirectory) {
        let Some(d) = self.fc.dir(fh) else { return reply.error(Errno::EBADF) };
        for (i, e) in d.iter().enumerate().skip(offset as usize) {
            if reply.add(INodeNo(e.ino), i as u64 + 1, file_type(e.kind), OsStr::from_bytes(&e.name)) {
                break;
            }
        }
        reply.ok();
    }

    pub(super) fn do_readdirplus(&self, fh: u64, offset: u64, mut reply: ReplyDirectoryPlus) {
        let Some(d) = self.fc.dir(fh) else { return reply.error(Errno::EBADF) };
        for (i, e) in d.iter().enumerate().skip(offset as usize) {
            let Some(a) = e.attr.clone() else { continue };
            let (ttl, fa) = self.fattr(a);
            if reply.add(INodeNo(e.ino), i as u64 + 1, OsStr::from_bytes(&e.name), &ttl, &fa, Generation(0)) {
                break;
            }
        }
        reply.ok();
    }

    pub(super) fn do_statfs(&self, req: &Request, reply: ReplyStatfs) {
        match self.fc.meta.read(&self.who(req), MetaOp::StatFs) {
            Ok(MetaReply::StatFs { bsize, blocks, bfree, files, ffree, namelen }) => {
                reply.statfs(blocks, bfree, bfree, files, ffree, bsize, namelen, bsize)
            }
            Ok(_) => reply.error(Errno::EIO),
            Err(e) => reply.error(errno(e)),
        }
    }

    pub(super) fn do_getxattr(&self, req: &Request, ino: u64, n: &OsStr, size: u32, reply: ReplyXattr) {
        if is_virtual(ino, n.as_bytes()) {
            return self.xattr_reply(reply, size, Ok(self.mgmt_addrs()));
        }
        let v = match self.fc.meta.read(&self.who(req), MetaOp::GetXattr { ino, name: name(n) }) {
            Ok(MetaReply::Xattr { value }) => Ok(value),
            Ok(_) => Err(ErrCode::INTERNAL),
            Err(e) => Err(e),
        };
        self.xattr_reply(reply, size, v);
    }

    pub(super) fn do_listxattr(&self, req: &Request, ino: u64, size: u32, reply: ReplyXattr) {
        let v = match self.fc.meta.read(&self.who(req), MetaOp::ListXattr { ino }) {
            Ok(MetaReply::XattrNames { names }) => {
                let mut out = Vec::new(); // 每个名字后面一个 NUL（listxattr(2) 的格式）
                for n in names {
                    out.extend(n);
                    out.push(0);
                }
                Ok(out)
            }
            Ok(_) => Err(ErrCode::INTERNAL),
            Err(e) => Err(e),
        };
        self.xattr_reply(reply, size, v);
    }
}

pub(super) fn fopen(keep_cache: bool, direct: bool) -> FopenFlags {
    if direct {
        FopenFlags::FOPEN_DIRECT_IO
    } else if keep_cache {
        FopenFlags::FOPEN_KEEP_CACHE
    } else {
        FopenFlags::empty()
    }
}
