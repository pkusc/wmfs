//! `wmfs debug chunk-path / chunk-header / corrupt-chunk`（design.md 7.2）：在本机直接看、改 chunk 文件（开发集群的 target 都在本机）。
//!
//! - `chunk-path <路径> <idx> [--node n]`：经元数据查出文件第 idx 个 chunk 在哪条链、链上成员的 target（默认 head = 第一个 Serving
//!   成员，`--node` 选那个节点上的副本），按 local-fs 的路径规则（3.2.2，`registry::chunk_path`）拼出**已提交版本**的本地路径，只打印
//!   这一行（演示里 `$(…)` 直接用它）。
//! - `chunk-header <文件>`：解析一个 chunk 文件的固定头与块表（3.2.4）：版本、校验与变换算法、长度、有数据的块数。
//! - `corrupt-chunk <路径> <idx> --node n [--block b]`：按块表找到第 b 块的存储字节，翻转其中一个字节（演示与验收里制造坏块只用它，
//!   不手算偏移）。之后读到这一块时校验和对不上：单副本上读返回 `EIO`（诚实报错）。

use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use serde_json::json;
use wmfs_core::base::ErrCode;
use wmfs_core::client::data_client::chunk_id;
use wmfs_core::client::meta_client::{Caller, MetaClient};
use wmfs_core::data::format::{BlockEntry, CHUNK_HEADER_LEN, ChunkHeader, decode_table, table_len_of};
use wmfs_core::data::format::{checksum_name, compress_name, encrypt_name};
use wmfs_core::meta::cluster::chains::SERVING;
use wmfs_core::meta::schema::records::LayoutV1;
use wmfs_core::proto::meta::{MetaOp, MetaReply, O_RDONLY};

use crate::fs::{caller, connect_with};
use crate::registry::chunk_path as local_path;
use crate::{fail, out};

#[derive(clap::Args)]
pub struct Where {
    /// 文件系统里的路径，例如 /big
    path: String,
    /// 第几个 chunk（从 0 起）
    idx: u32,
    /// 链上哪个节点的副本（默认链的 head：第一个 Serving 成员）
    #[arg(long)]
    node: Option<u32>,
    /// 集群的管理地址，逗号分隔（默认 $WMFS_CLUSTER）
    #[arg(long)]
    cluster: Option<String>,
    /// 密钥文件（默认 $WMFS_KEY_FILE，否则 ~/.config/wmfs/admin.key）
    #[arg(long)]
    key_file: Option<PathBuf>,
    /// 输出 JSON
    #[arg(long)]
    json: bool,
}

/// 路径 → (ino, layout)：ResolvePath，再只读 Open 取 layout。
fn resolve_layout(c: &MetaClient, who: &Caller, path: &str) -> Result<(u64, LayoutV1), ErrCode> {
    let ino = match c.read(who, MetaOp::ResolvePath { path: path.as_bytes().to_vec() })? {
        MetaReply::Resolved { ino, .. } => ino,
        _ => return Err(ErrCode::INTERNAL),
    };
    match c.read(who, MetaOp::Open { ino, flags: O_RDONLY })? {
        MetaReply::Opened(o) => Ok((ino, o.layout.ok_or(ErrCode::EINVAL)?)),
        _ => Err(ErrCode::INTERNAL),
    }
}

/// 文件第 idx 个 chunk 的已提交版本在本机上的路径（见文件头）。
fn locate(w: &Where) -> Result<PathBuf, String> {
    let (c, _rpc) = connect_with(&w.cluster, &w.key_file, false).map_err(|(_, e)| e)?;
    let (who, _) = caller(&None)?;
    let r = resolve_layout(&c, &who, &w.path);
    let map = c.refresh_map();
    c.close();
    let (ino, l) = r.map_err(|e| format!("{}：{e}", w.path))?;
    let map = map.map_err(|e| format!("取集群图失败：{e}"))?;
    let id = chunk_id(&l, ino, w.idx).map_err(|e| format!("{}：{e}", w.path))?;
    let rec = map.chains.iter().find(|(c, _)| *c == id.chain).map(|(_, r)| r).ok_or("集群图里没有这条链")?;
    let target = |t: u32| map.targets.iter().find(|(id, _)| *id == t).map(|(_, r)| r);
    let t = match w.node {
        Some(n) => rec.members.iter().filter_map(|(t, _)| target(*t)).find(|t| t.node == n),
        None => rec.members.iter().filter(|(_, s)| *s == SERVING).find_map(|(t, _)| target(*t)),
    };
    let t = t.ok_or(match w.node {
        Some(n) => format!("链 {} 在节点 {n} 上没有成员", id.chain),
        None => format!("链 {} 没有 Serving 成员", id.chain),
    })?;
    Ok(local_path(Path::new(&t.path), &id))
}

/// 读一个 chunk 文件的固定头与块表。
fn header(file: &Path) -> Result<(ChunkHeader, Vec<BlockEntry>), String> {
    let f = std::fs::File::open(file).map_err(|e| format!("打开 {} 失败：{e}", file.display()))?;
    let mut fixed = [0u8; CHUNK_HEADER_LEN];
    f.read_exact_at(&mut fixed, 0).map_err(|e| format!("读 {} 的头失败：{e}", file.display()))?;
    let mut table = vec![0u8; table_len_of(&fixed)];
    f.read_exact_at(&mut table, CHUNK_HEADER_LEN as u64).map_err(|e| format!("读块表失败：{e}"))?;
    let h = ChunkHeader::decode(&fixed, &table).map_err(|e| format!("{} 不是好的 chunk 文件：{e}", file.display()))?;
    Ok((h, decode_table(&table).map_err(|e| e.to_string())?))
}

pub fn chunk_path(w: &Where) -> i32 {
    match locate(w) {
        Ok(p) if w.json => out(json!({"path": p.display().to_string()})),
        Ok(p) => out(p.display()),
        Err(e) => fail(e),
    }
}

pub fn chunk_header(file: &Path, json: bool) -> i32 {
    let (h, table) = match header(file) {
        Ok(x) => x,
        Err(e) => return fail(e),
    };
    let (ck, cmp, enc) = (checksum_name(h.checksum_alg), compress_name(h.compress_alg), encrypt_name(h.encrypt_alg));
    let data = table.iter().filter(|e| e.stored_len > 0).count();
    if json {
        return out(json!({
            "version": h.chunk_version, "checksum": ck, "compress": cmp, "encrypt": enc,
            "logical_len": h.logical_len, "chunk_size": h.chunk_size, "block_size": h.block_size, "nblocks": h.nblocks,
            "ino": h.ino, "epoch": h.epoch, "chunk_idx": h.chunk_idx, "chain_id": h.chain_id,
            "data_blocks": data, "write_time_ms": h.write_time_ms,
        }));
    }
    out(format!(
        "version={}（{:#x}）\nchecksum={ck} compress={cmp} encrypt={enc}\nlogical_len={} chunk_size={} block_size={} nblocks={}\n\
         ino={} epoch={} chunk_idx={} chain={}\n有数据的块：{data} 个（洞 {} 个）",
        h.chunk_version,
        h.chunk_version,
        h.logical_len,
        h.chunk_size,
        h.block_size,
        h.nblocks,
        h.ino,
        h.epoch,
        h.chunk_idx,
        h.chain_id,
        h.nblocks as usize - data,
    ))
}

pub fn corrupt(w: &Where, block: u32) -> i32 {
    let r = locate(w).and_then(|file| {
        let (h, table) = header(&file)?;
        let e = table.get(block as usize).ok_or(format!("第 {block} 块不存在（共 {} 块）", h.nblocks))?;
        if e.stored_len == 0 {
            return Err(format!("第 {block} 块是洞（没有存数据），没有可翻转的字节"));
        }
        let off = h.block_offset(block) + e.stored_len as u64 / 2;
        let f = std::fs::OpenOptions::new().read(true).write(true).open(&file).map_err(|e| e.to_string())?;
        let mut b = [0u8; 1];
        f.read_exact_at(&mut b, off).map_err(|e| e.to_string())?;
        f.write_all_at(&[b[0] ^ 0xFF], off).map_err(|e| e.to_string())?;
        Ok((file, off))
    });
    match r {
        Ok((file, off)) if w.json => {
            out(json!({"ok": true, "file": file.display().to_string(), "offset": off, "block": block}))
        }
        Ok((file, off)) => out(format!("已翻转 {} 偏移 {off} 的一个字节（第 {block} 块）", file.display())),
        Err(e) => fail(e),
    }
}
