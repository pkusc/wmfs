//! local-fs 的目录遍历：统计占用、列出一条链上的 chunk、列出全部 (chain, ino, epoch) 目录（同步、scrub、本地孤儿扫描用）。
//! 目录结构见 mod.rs：`c/<chain:08x>/<ino%256:02x>/<ino:016x>-<epoch:08x>/<idx:08x>`；名字不合规矩的条目一律跳过。

use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::time::UNIX_EPOCH;

use wmfs_core::base::ErrCode;
use wmfs_core::base::types::ChunkId;

/// 目录的子项（不存在 → 空）。
fn entries(dir: &Path) -> Result<Vec<std::fs::DirEntry>, ErrCode> {
    match std::fs::read_dir(dir) {
        Ok(rd) => rd.map(|e| e.map_err(ErrCode::from)).collect(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(vec![]),
        Err(e) => Err(e.into()),
    }
}

/// `dir` 下的 (文件数, 实际占的字节 = 块数 × 512)，递归。
pub fn count(dir: &Path) -> Result<(u64, u64), ErrCode> {
    let (mut files, mut bytes) = (0, 0);
    for e in entries(dir)? {
        let m = e.metadata()?;
        if m.is_dir() {
            let (f, b) = count(&e.path())?;
            (files, bytes) = (files + f, bytes + b);
        } else {
            (files, bytes) = (files + 1, bytes + m.blocks() * 512);
        }
    }
    Ok((files, bytes))
}

/// `<ino:016x>-<epoch:08x>` → (ino, epoch)。
fn parse_epoch_dir(name: &str) -> Option<(u64, u32)> {
    let (ino, epoch) = name.split_once('-')?;
    Some((u64::from_str_radix(ino, 16).ok()?, u32::from_str_radix(epoch, 16).ok()?))
}

/// 一条链的目录 `c/<chain>` 下的全部 chunk（不保证顺序）。
pub fn chunks(chain_dir: &Path, chain: u32) -> Result<Vec<ChunkId>, ErrCode> {
    let mut out = Vec::new();
    for bucket in entries(chain_dir)? {
        for ed in entries(&bucket.path())? {
            let Some((ino, epoch)) = ed.file_name().to_str().and_then(parse_epoch_dir) else { continue };
            for f in entries(&ed.path())? {
                if let Some(idx) = f.file_name().to_str().and_then(|n| u32::from_str_radix(n, 16).ok()) {
                    out.push(ChunkId { chain, ino, epoch, idx });
                }
            }
        }
    }
    Ok(out)
}

/// `c/` 下的全部 (chain, ino, epoch, 目录 mtime ms)（不保证顺序）。
pub fn epochs(c_dir: &Path) -> Result<Vec<(u32, u64, u32, u64)>, ErrCode> {
    let mut out = Vec::new();
    for cd in entries(c_dir)? {
        let Some(chain) = cd.file_name().to_str().and_then(|n| u32::from_str_radix(n, 16).ok()) else { continue };
        for bucket in entries(&cd.path())? {
            for ed in entries(&bucket.path())? {
                let Some((ino, epoch)) = ed.file_name().to_str().and_then(parse_epoch_dir) else { continue };
                let mtime = ed.metadata()?.modified().ok().and_then(|t| t.duration_since(UNIX_EPOCH).ok());
                out.push((chain, ino, epoch, mtime.map_or(0, |d| d.as_millis() as u64)));
            }
        }
    }
    Ok(out)
}
