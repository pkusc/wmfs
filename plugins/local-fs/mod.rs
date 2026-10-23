//! local-fs：chunk_store 插件（design.md 4.9、3.2.2、3.2.5，ADR-0008、ADR-0021）——把本体编码好的 chunk 字节存成目录里的文件
//! （ext4 或 XFS）。插件不解析 chunk 头（格式归本体，3.7.3）。
//!
//! 路径（3.2.2）：已提交 `c/<chain:08x>/<ino%256:02x>/<ino:016x>-<epoch:08x>/<idx:08x>`，pending `p/<chain>-<ino>-<epoch>-<idx>.p`，
//! 同步/修复用的临时文件 `t/<…>.t`；按链分目录，重同步时"列出我在这条链上的全部 chunk"只要一次目录遍历。
//! - `write_pending`（R1）：`O_CREAT|O_TRUNC` 打开 pending 文件，`pwrite` 各段，未改动的块用 `copy_file_range` 从已提交文件同偏移
//!   复制（`EXDEV/ENOSYS/EOPNOTSUPP/EINVAL` 时退回 `pread/pwrite`）。写路径不 fsync（R2，持久性靠多副本与客户端 fsync）。
//! - `commit_pending`：（sync 时先 `sync_data`）→ `rename` 覆盖旧版本 → sync 或新建了目录时 fsync 目录（一直到 `c/`）。
//! - `sync`（R2）：对已提交文件 `sync_data`，再 fsync 它所在的目录与各级父目录（rename 产生的目录项否则可能丢）。
//! - `install`（R3）：`t/` 临时文件 → `sync_data` → `rename` → fsync 目录。删除（R4）= unlink / 删目录，不 fsync。
//! - `clear_temp`（R5）：启动时清空 `p/`、`t/`。`statfs`：容量 = 文件系统的大小（`statvfs`），已用 = 本 target 已提交文件实际
//!   占的块（打开时遍历一次，之后随提交与删除增减）；与 node.toml 的 `capacity` 取小由本体做。
//!
//! fail point（3.11）：`chunk.pending.eio`（写 pending 失败）、`chunk.pending.torn`（只写一半 pending 然后 abort）、
//! `chunk.commit.before_rename`、`chunk.read.eio`、`chunk.read.corrupt`（读出的字节翻转一个比特）、`chunk.sync.eio`；
//! 以及本体包在每次调用外层的 `plugin.local-fs.chunk_store`。指标 `wmfs_chunkstore_op_seconds{op}`。
//! 实现注记（M5）：首发没有做 4.9 写的 fd LRU 缓存（每次调用打开文件；本机 4 MiB chunk 下打开文件的开销远小于读写本身）。

use std::fs::{File, OpenOptions};
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use wmfs_core::api::failpoint::{fail_point, fail_point_hit};
use wmfs_core::api::metrics::{self, Histogram};
use wmfs_core::base::types::ChunkId;
use wmfs_core::base::{ErrCode, sys};
use wmfs_core::hooks::manifest::{CORE_ABI_VERSION, Hook, Manifest};
use wmfs_core::hooks::traits::{ChunkReader, ChunkStore, StoreStat, TargetStore};

mod walk;

pub static MANIFEST: Manifest = Manifest {
    name: "local-fs",
    version: "0.1.0",
    hooks: &[Hook::ChunkStore],
    altitude: 0, // 唯一型钩子
    hot_path: true,
    config_schema: "",
    abi_version: CORE_ABI_VERSION,
    attrs: &[],
    budget_ns: 100_000_000, // 3.7.4：chunk_store local-fs 每次调用 100 ms（本职就是 IO）
    fail_points: &[
        "plugin.local-fs.chunk_store",
        "chunk.pending.eio",
        "chunk.pending.torn",
        "chunk.commit.before_rename",
        "chunk.read.eio",
        "chunk.read.corrupt",
        "chunk.sync.eio",
    ],
};

#[derive(Default)]
pub struct LocalFs {}

impl LocalFs {
    pub fn new() -> LocalFs {
        LocalFs {}
    }
}

impl ChunkStore for LocalFs {
    fn manifest(&self) -> &'static Manifest {
        &MANIFEST
    }

    fn open_target(&self, root: &Path, _target_id: u32) -> Result<Arc<dyn TargetStore>, ErrCode> {
        for d in ["c", "p", "t"] {
            std::fs::create_dir_all(root.join(d))?;
        }
        let (_, used) = walk::count(&root.join("c"))?; // 已提交文件实际占的字节
        let op = |o: &str| {
            metrics::histogram(
                "wmfs_chunkstore_op_seconds",
                "chunk_store 一次调用的耗时",
                &[("op", o)],
                metrics::SECONDS_BUCKETS,
            )
        };
        Ok(Arc::new(LocalTarget {
            root: root.to_path_buf(),
            used: AtomicU64::new(used),
            write_secs: op("write_pending"),
            commit_secs: op("commit_pending"),
            open_secs: op("open_committed"),
            sync_secs: op("sync"),
        }))
    }
}

/// 一个 target 目录。
pub struct LocalTarget {
    root: PathBuf,
    used: AtomicU64, // 已提交文件实际占的字节（块数 × 512）
    write_secs: Arc<Histogram>,
    commit_secs: Arc<Histogram>,
    open_secs: Arc<Histogram>,
    sync_secs: Arc<Histogram>,
}

/// 已提交版本的路径（3.2.2）：`<root>/c/<chain:08x>/<ino%256:02x>/<ino:016x>-<epoch:08x>/<idx:08x>`。
/// `wmfs debug chunk-path` 也用它（CLI 在本机直接找文件）。
pub fn committed_path(root: &Path, id: &ChunkId) -> PathBuf {
    epoch_dir(root, id.chain, id.ino, id.epoch).join(format!("{:08x}", id.idx))
}

fn epoch_dir(root: &Path, chain: u32, ino: u64, epoch: u32) -> PathBuf {
    root.join(format!("c/{chain:08x}/{:02x}/{ino:016x}-{epoch:08x}", ino % 256))
}

fn flat_name(id: &ChunkId, ext: &str) -> String {
    format!("{:08x}-{:016x}-{:08x}-{:08x}.{ext}", id.chain, id.ino, id.epoch, id.idx)
}

/// 调用 `f` 并把耗时记进直方图 `h`（`wmfs_chunkstore_op_seconds{op}`）。
fn timed<T>(h: &Histogram, f: impl FnOnce() -> Result<T, ErrCode>) -> Result<T, ErrCode> {
    let t0 = Instant::now();
    let r = f();
    h.observe(t0.elapsed().as_secs_f64());
    r
}

/// 文件实际占的字节（不存在 → 0）。
fn blocks(p: &Path) -> u64 {
    std::fs::metadata(p).map_or(0, |m| m.blocks() * 512)
}

/// fsync `file` 所在的目录与各级父目录，一直到 `<root>/c`（新目录的目录项也要落盘）。
fn sync_dirs(root: &Path, file: &Path) -> Result<(), ErrCode> {
    let stop = root.join("c");
    let mut d = file.parent();
    while let Some(dir) = d {
        sys::fsync_dir(dir)?;
        if dir == stop || !dir.starts_with(&stop) {
            break;
        }
        d = dir.parent();
    }
    Ok(())
}

impl LocalTarget {
    fn pending(&self, id: &ChunkId) -> PathBuf {
        self.root.join("p").join(flat_name(id, "p"))
    }

    /// 已用减少 `n` 字节，最少减到 0（已用随 st_blocks 估算，会漂移：延迟分配、预分配）。一次原子操作完成，并发删除也不会减成负数。
    fn sub_used(&self, n: u64) {
        let _ = self.used.try_update(Ordering::Relaxed, Ordering::Relaxed, |u| Some(u.saturating_sub(n)));
    }

    /// rename 到已提交的位置（目录不存在就建），调整已用；`sync` 时之前 fdatasync、之后 fsync 各级目录。
    fn publish(&self, from: &Path, id: &ChunkId, sync: bool) -> Result<(), ErrCode> {
        let to = committed_path(&self.root, id);
        let dir = to.parent().ok_or(ErrCode::INTERNAL)?;
        let new_dir = !dir.exists();
        std::fs::create_dir_all(dir)?;
        if sync {
            File::open(from)?.sync_data()?;
        }
        let (new, old) = (blocks(from), blocks(&to));
        std::fs::rename(from, &to)?;
        self.used.fetch_add(new, Ordering::Relaxed);
        self.sub_used(old);
        if sync || new_dir { sync_dirs(&self.root, &to) } else { Ok(()) }
    }
}

/// 把 `src` 的 `[off, off + len)` 复制到 `dst` 的同一位置：先 `copy_file_range`（XFS 上是 reflink），不支持就 pread/pwrite。
fn copy_range(src: &File, dst: &File, off: u64, len: u64) -> Result<(), ErrCode> {
    let mut done = 0u64;
    while done < len {
        let n = match sys::copy_file_range(src, off + done, dst, off + done, (len - done) as usize) {
            Ok(n) => n as u64,
            Err(ErrCode::EXDEV | ErrCode::ENOSYS | ErrCode::EOPNOTSUPP | ErrCode::EINVAL) => {
                let mut buf = vec![0u8; ((len - done) as usize).min(64 << 10)]; // 退回用的小缓冲（不是 bulk 数据的拷贝）
                let n = src.read_at(&mut buf, off + done)?;
                dst.write_all_at(&buf[..n], off + done)?;
                n as u64
            }
            Err(e) => return Err(e),
        };
        if n == 0 {
            return Err(ErrCode::CORRUPT); // 已提交文件比块表说的短
        }
        done += n;
    }
    Ok(())
}

impl TargetStore for LocalTarget {
    fn read_at(&self, id: &ChunkId, off: u64, buf: &mut [u8]) -> Result<Option<usize>, ErrCode> {
        match self.open_committed(id)? {
            Some(mut r) => r.read_at(off, buf).map(Some),
            None => Ok(None),
        }
    }

    fn open_committed(&self, id: &ChunkId) -> Result<Option<Box<dyn ChunkReader>>, ErrCode> {
        timed(&self.open_secs, || match File::open(committed_path(&self.root, id)) {
            Ok(f) => Ok(Some(Box::new(Reader { len: f.metadata()?.len(), f }) as Box<dyn ChunkReader>)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        })
    }

    fn write_pending(&self, id: &ChunkId, writes: &[(u64, &[u8])], copies: &[(u64, u64)]) -> Result<(), ErrCode> {
        timed(&self.write_secs, || {
            fail_point("chunk.pending.eio").map_err(ErrCode)?;
            let f = OpenOptions::new().write(true).create(true).truncate(true).open(self.pending(id))?;
            if fail_point_hit("chunk.pending.torn") {
                let half: usize = writes.iter().map(|(_, d)| d.len()).sum::<usize>() / 2;
                let mut left = half;
                for (off, d) in writes {
                    let n = d.len().min(left);
                    f.write_all_at(&d[..n], *off)?;
                    left -= n;
                }
                std::process::abort(); // 模拟"pending 文件写到一半崩溃"（3.11）
            }
            for (off, d) in writes {
                f.write_all_at(d, *off)?;
            }
            if !copies.is_empty() {
                let src = File::open(committed_path(&self.root, id))?;
                for (off, len) in copies {
                    copy_range(&src, &f, *off, *len)?;
                }
            }
            Ok(())
        })
    }

    fn read_pending_at(&self, id: &ChunkId, off: u64, buf: &mut [u8]) -> Result<usize, ErrCode> {
        Ok(File::open(self.pending(id))?.read_at(buf, off)?)
    }

    fn commit_pending(&self, id: &ChunkId, sync: bool) -> Result<(), ErrCode> {
        timed(&self.commit_secs, || {
            fail_point("chunk.commit.before_rename").map_err(ErrCode)?;
            self.publish(&self.pending(id), id, sync)
        })
    }

    fn abort_pending(&self, id: &ChunkId) -> Result<(), ErrCode> {
        match std::fs::remove_file(self.pending(id)) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e.into()),
            _ => Ok(()),
        }
    }

    fn install(&self, id: &ChunkId, writes: &[(u64, &[u8])]) -> Result<(), ErrCode> {
        let tmp = self.root.join("t").join(flat_name(id, "t"));
        let f = OpenOptions::new().write(true).create(true).truncate(true).open(&tmp)?;
        for (off, d) in writes {
            f.write_all_at(d, *off)?;
        }
        drop(f);
        self.publish(&tmp, id, true)
    }

    fn sync(&self, ids: &[ChunkId]) -> Result<(), ErrCode> {
        timed(&self.sync_secs, || {
            fail_point("chunk.sync.eio").map_err(ErrCode)?;
            for id in ids {
                let p = committed_path(&self.root, id);
                match File::open(&p) {
                    Ok(f) => f.sync_data()?,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue, // 从未写过：没有要落盘的
                    Err(e) => return Err(e.into()),
                }
                sync_dirs(&self.root, &p)?;
            }
            Ok(())
        })
    }

    fn delete_epoch(&self, chain: u32, ino: u64, epoch: u32) -> Result<u64, ErrCode> {
        let dir = epoch_dir(&self.root, chain, ino, epoch);
        let (files, bytes) = walk::count(&dir)?;
        if dir.exists() {
            std::fs::remove_dir_all(&dir)?;
        }
        self.sub_used(bytes);
        Ok(files)
    }

    fn delete_chunk(&self, id: &ChunkId) -> Result<(), ErrCode> {
        let p = committed_path(&self.root, id);
        let b = blocks(&p);
        match std::fs::remove_file(&p) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(e.into()),
            _ => {}
        }
        self.sub_used(b);
        Ok(())
    }

    fn list_chain(&self, chain: u32, after: Option<&ChunkId>, limit: usize) -> Result<Vec<ChunkId>, ErrCode> {
        let mut v: Vec<ChunkId> = walk::chunks(&self.root.join(format!("c/{chain:08x}")), chain)?;
        v.sort();
        Ok(v.into_iter().filter(|c| after.is_none_or(|a| c > a)).take(limit).collect())
    }

    fn list_epochs(&self, after: Option<(u32, u64, u32)>, limit: usize) -> Result<Vec<(u32, u64, u32, u64)>, ErrCode> {
        let mut v = walk::epochs(&self.root.join("c"))?;
        v.sort();
        Ok(v.into_iter().filter(|e| after.is_none_or(|a| (e.0, e.1, e.2) > a)).take(limit).collect())
    }

    fn delete_chain_dir(&self, chain: u32) -> Result<(), ErrCode> {
        let dir = self.root.join(format!("c/{chain:08x}"));
        let (_, bytes) = walk::count(&dir)?;
        if dir.exists() {
            std::fs::remove_dir_all(&dir)?;
        }
        self.sub_used(bytes);
        Ok(())
    }

    fn clear_temp(&self) -> Result<(), ErrCode> {
        for d in ["p", "t"] {
            let dir = self.root.join(d);
            for e in std::fs::read_dir(&dir)? {
                std::fs::remove_file(e?.path())?;
            }
        }
        Ok(())
    }

    fn statfs(&self) -> Result<StoreStat, ErrCode> {
        let fs = sys::statvfs(&self.root)?;
        Ok(StoreStat { capacity: fs.total_bytes, used: self.used.load(Ordering::Relaxed) })
    }
}

/// 一个已提交版本的读句柄：打开时的那个文件，之后的 rename 不影响它（3.7.3）。
struct Reader {
    f: File,
    len: u64,
}

impl ChunkReader for Reader {
    fn read_at(&mut self, off: u64, buf: &mut [u8]) -> Result<usize, ErrCode> {
        fail_point("chunk.read.eio").map_err(ErrCode)?;
        let n = self.f.read_at(buf, off)?;
        if n > 0 && fail_point_hit("chunk.read.corrupt") {
            buf[n / 2] ^= 1 << (n % 8); // 模拟坏盘：翻转一个比特（3.11）
        }
        Ok(n)
    }

    fn len(&self) -> u64 {
        self.len
    }
}

#[cfg(test)]
mod tests;
