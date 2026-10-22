//! 模拟掉电的 redb 后端 `MemCrashBackend`（design.md 9.2）：实现 redb 2.6 的 `StorageBackend`，用
//! `Builder::create_with_backend` 打开。写入先放在"易失层"，只有 `sync_data` 之后才算落盘；[`MemCrashBackend::crash`]
//! = 丢掉易失层、只用已落盘的字节造一个"重启后"的新后端。这才真正模拟了掉电（`kill -9` 不丢页缓存，fsync 写错了也照样
//! "不丢"，测不出来），而且不碰真实磁盘、不做真实 fsync。
//!
//! [`MemCrashBackend::freeze_after`]：在之后第 k 次 `sync_data` 处"掉电"——那之后的 sync 不再让任何东西落盘，进程却照常
//! 往下跑；测试据此判断哪些确认"真的发生过"（确认时的 sync 计数 ≤ 掉电点）。

use std::io;
use std::sync::{Arc, Mutex, MutexGuard};

use redb::StorageBackend;

#[derive(Debug)]
enum Op {
    Write(u64, Vec<u8>),
    SetLen(u64),
}

#[derive(Debug, Default)]
struct State {
    volatile: Vec<u8>, // 读写都看它（= 进程看到的文件内容）
    durable: Vec<u8>,  // 掉电后剩下的
    pending: Vec<Op>,  // 上次 sync 之后的写入，sync 时按顺序落到 durable
    syncs: u64,
    freeze_at: Option<u64>, // 第几次 sync 之后不再落盘
}

#[derive(Debug, Clone, Default)]
pub struct MemCrashBackend {
    s: Arc<Mutex<State>>,
}

impl MemCrashBackend {
    pub fn new() -> MemCrashBackend {
        MemCrashBackend::default()
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.s.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// 掉电并重启：只留下已落盘的字节，返回一个独立的新后端（原后端照常可用，互不影响）。
    pub fn crash(&self) -> MemCrashBackend {
        let s = self.lock();
        let st = State { volatile: s.durable.clone(), durable: s.durable.clone(), ..State::default() };
        MemCrashBackend { s: Arc::new(Mutex::new(st)) }
    }

    /// 到目前为止 `sync_data` 的次数。
    pub fn syncs(&self) -> u64 {
        self.lock().syncs
    }

    /// 在之后第 k 次 sync 处掉电（k = 0：现在就掉电，之后的一切都不再落盘）。
    pub fn freeze_after(&self, k: u64) {
        let mut s = self.lock();
        s.freeze_at = Some(s.syncs + k);
    }

    /// 掉电点（`freeze_after` 设的）。
    pub fn frozen_at(&self) -> Option<u64> {
        self.lock().freeze_at
    }
}

fn apply(buf: &mut Vec<u8>, op: Op) {
    match op {
        Op::SetLen(n) => buf.resize(n as usize, 0),
        Op::Write(off, data) => {
            let end = off as usize + data.len();
            if end > buf.len() {
                buf.resize(end, 0);
            }
            buf[off as usize..end].copy_from_slice(&data);
        }
    }
}

impl StorageBackend for MemCrashBackend {
    fn len(&self) -> io::Result<u64> {
        Ok(self.lock().volatile.len() as u64)
    }

    fn read(&self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        let s = self.lock();
        let end = offset as usize + len;
        if end > s.volatile.len() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "读超出了文件长度"));
        }
        Ok(s.volatile[offset as usize..end].to_vec())
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        let mut s = self.lock();
        apply(&mut s.volatile, Op::SetLen(len));
        s.pending.push(Op::SetLen(len));
        Ok(())
    }

    fn sync_data(&self, _eventual: bool) -> io::Result<()> {
        let mut s = self.lock();
        s.syncs += 1;
        let pending = std::mem::take(&mut s.pending);
        if s.freeze_at.is_none_or(|f| s.syncs <= f) {
            for op in pending {
                apply(&mut s.durable, op);
            }
        }
        Ok(())
    }

    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        let mut s = self.lock();
        apply(&mut s.volatile, Op::Write(offset, data.to_vec()));
        s.pending.push(Op::Write(offset, data.to_vec()));
        Ok(())
    }
}
