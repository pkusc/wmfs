//! 目录句柄（design.md 8.2、3.1.4）：opendir 时按名字分页读出的完整快照（readdir 的 offset = 快照下标；POSIX 允许 readdir 反映某个
//! 快照），带属性（readdirplus 用）；前面补上 `.` 与 `..`。实现注记（M7，ADR-0037 第 13 条）：`..` 的 inode 号是父目录（不改协议：
//! 客户端在 lookup、mkdir、readdirplus 与自己的 rename 时记下目录的父目录，[`FileClient::note_parent`]）；根目录的 `..` 是它自己；
//! 不知道时（极少）沿用目录自己的并写 debug 日志。实现注记（M10）：为了 `files.rs` ≤ 400 行从那里拆出来；目录句柄与文件句柄一样
//! 记着打开时的会话号，被驱逐的会话里打开的 → 此后 `EIO`（M10 审查，3.4.4："所有已打开句柄此后返回 EIO"，即使快照就在本地）。

use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::files::FileClient;
use super::meta_client::Caller;
use crate::base::{ErrCode, lock, log};
use crate::meta::schema::keys::ROOT_INO;
use crate::meta::schema::records::KIND_DIR;
use crate::proto::meta::{AttrOut, DirEntryOut, MetaOp, MetaReply};

/// 一个目录句柄：打开时的会话号与快照。
#[derive(Clone)]
pub(super) struct DirHandle {
    session: u64,
    entries: Arc<Vec<DirEntryOut>>,
}

impl FileClient {
    /// 记下目录 `dir` 的父目录（lookup、mkdir、readdirplus 与自己的 rename 时；只对目录记，内核 forget 时一起丢）。
    pub fn note_parent(&self, dir: u64, parent: u64) {
        lock(&self.parents).insert(dir, parent);
    }

    /// opendir：读出完整的目录快照（按名字分页，每页 1024 条、带属性），前面补 `.` 与 `..`；返回目录句柄。`..` 是父目录
    /// （根目录的是它自己；不知道时——极少，例如别的客户端刚移动了它——沿用目录自己的，写一行 debug 日志）；快照里的子目录顺便
    /// 记下父目录。实现注记（M7，ADR-0037 第 13 条）：M6 的 `..` 一律是目录自己。
    pub fn open_dir(&self, who: &Caller, ino: u64) -> Result<u64, ErrCode> {
        self.meta.gate()?;
        let sid = self.meta.session(); // 快照属于这个会话：中途换了会话 → SESSION_EVICTED（EIO）
        let MetaReply::Entry { attr: me } = self.meta.read_in(sid, who, MetaOp::GetAttr { ino })? else {
            return Err(ErrCode::INTERNAL);
        };
        let up = if ino == ROOT_INO { Some(ROOT_INO) } else { lock(&self.parents).get(&ino).copied() };
        let up = up.unwrap_or_else(|| {
            log::debug("client", format_args!("目录 {ino} 的父目录不知道：快照里的 .. 先用它自己"));
            ino
        });
        // fuser 的 readdirplus 用属性里的 ino 填 d_ino（内核对 . 与 .. 不用别的属性），所以 .. 的属性换上父目录的号
        let dot = |name: &[u8], ino| {
            let attr = Some(AttrOut { ino, ..me.clone() });
            DirEntryOut { name: name.to_vec(), ino, kind: KIND_DIR, attr }
        };
        let mut all = vec![dot(b".", ino), dot(b"..", up)];
        let mut after = Vec::new();
        loop {
            let op = MetaOp::ReadDir { ino, after: after.clone(), limit: 1024, plus: true };
            let MetaReply::Dir { entries, eof } = self.meta.read_in(sid, who, op)? else {
                return Err(ErrCode::INTERNAL);
            };
            after = entries.last().map(|e| e.name.clone()).unwrap_or_default();
            all.extend(entries);
            if eof || after.is_empty() {
                break;
            }
        }
        let mut parents = lock(&self.parents);
        for e in all[2..].iter().filter(|e| e.kind == KIND_DIR) {
            parents.insert(e.ino, ino);
        }
        drop(parents);
        let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
        lock(&self.dirs).insert(fh, DirHandle { session: sid, entries: Arc::new(all) });
        Ok(fh)
    }

    /// 目录句柄的快照：先过自停闸门（与文件句柄相同）；不存在 → `EBADF`；被驱逐的会话里打开的 → `EIO`（见文件头）。
    pub fn dir(&self, fh: u64) -> Result<Arc<Vec<DirEntryOut>>, ErrCode> {
        self.meta.gate()?;
        let d = lock(&self.dirs).get(&fh).cloned().ok_or(ErrCode::EBADF)?;
        if d.session == self.meta.session() { Ok(d.entries) } else { Err(ErrCode::EIO) }
    }

    /// releasedir。
    pub fn close_dir(&self, fh: u64) {
        lock(&self.dirs).remove(&fh);
    }
}
