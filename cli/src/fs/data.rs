//! `wmfs fs put/get/cat/truncate`（design.md 7.2，roadmap M5）：经 MetaClient + DataClient 读写文件的数据（不经 FUSE）。
//!
//! - `put <本地文件|-> <路径>`：Create（已存在就打开它，有内容时先截断到 0——新 epoch，像 `cp` 覆盖已有文件）；内容不超过 layout 的
//!   `inline_max` 且文件还是 inline → 一个 `CommitWrite` 带 inline 补丁（小文件只在元数据里）；否则（文件还是 inline 时先
//!   `Uninline`，ADR-0034）按 chunk（默认 4 MiB）逐个 `WriteChunk` 到链的 head，最后 `CommitWrite` 提交长度。不 fsync（与 close 相同，
//!   semantics.md 2.5）。
//! - `get <路径> <本地文件|->` / `cat <路径>`：Open（只读）→ inline 文件直接用回复里的内容；否则逐个 chunk `ReadChunk`，chunk 的
//!   `logical_len` 之后与 `CHUNK_NOT_FOUND`（先 GetAttr 确认 epoch 没变，C5）都按 0 补齐到文件长度。坏块 → `EIO`（单副本修不了）。
//! - `truncate <路径> <长度>`：Open（写）→ 截到 s > 0 时先对 `[⌊s/chunk⌋, ⌈旧长度/chunk⌉)` 的每个 chunk 发 `TruncateChunk`（inline
//!   文件没有 chunk，跳过），再 `SetAttr{size}`（先数据、后元数据，放松 ⑦）；截到 0 只发 `SetAttr`（服务端换 epoch）。

use std::io::{Read, Write};
use std::time::Duration;

use serde_json::json;
use wmfs_core::base::ErrCode;
use wmfs_core::base::types::{ChunkId, Ts};
use wmfs_core::buf::{Buf, Lane};
use wmfs_core::client::data_client::{DataClient, chunk_id};
use wmfs_core::client::meta_client::{Caller, MetaClient};
use wmfs_core::meta::schema::records::{F_INLINE, LayoutV1};
use wmfs_core::proto::meta::{MetaOp, MetaReply, O_RDONLY, O_WRONLY, OpenOut, SetAttrIn};

use super::{Done, Failed, failed};

/// 取一个 chunk 大小的缓冲最多等多久：一次性命令用自己的小池子，上一个 chunk 的缓冲可能还没从连接的发送队列里放回来，
/// 所以比服务端的 `buf::POOL_WAIT`（100 ms）等得久。
const CLI_POOL_WAIT: Duration = Duration::from_secs(5);

/// 一条数据命令要用的客户端与身份。
pub struct Ctx<'a> {
    pub meta: &'a MetaClient,
    pub data: &'a DataClient,
    pub who: &'a Caller,
}

impl Ctx<'_> {
    fn opened(&self, path: &str, r: Result<MetaReply, ErrCode>) -> Result<OpenOut, Failed> {
        match r.map_err(|e| failed(path, e))? {
            MetaReply::Opened(o) => Ok(o),
            _ => Err(failed(path, ErrCode::INTERNAL)),
        }
    }

    fn commit(&self, path: &str, o: &OpenOut, new_size: u64, patches: Vec<(u64, Vec<u8>)>) -> Result<(), Failed> {
        let epoch = layout(path, o)?.epoch;
        let mtime = Ts::default(); // 服务端用 leader 的时钟（放松 ⑫）
        let op = MetaOp::CommitWrite {
            ino: o.attr.ino,
            epoch,
            new_size,
            mtime,
            inline_patches: patches,
            uninline_expect_change_id: None,
            want_quota: false,
            close: true,
        };
        self.meta.write(self.who, op).map(|_| ()).map_err(|e| failed(path, e))
    }

    fn set_size(&self, path: &str, ino: u64, size: u64, epoch_seen: u32) -> Result<OpenOut, Failed> {
        let set =
            SetAttrIn { mode: None, uid: None, gid: None, size: Some(size), atime: None, mtime: None, epoch_seen };
        self.opened(path, self.meta.write(self.who, MetaOp::SetAttr { ino, set }))
    }

    fn buf(&self, path: &str, n: usize) -> Result<Buf, Failed> {
        self.data.pool().get(n.max(1), Lane::Normal, CLI_POOL_WAIT).map_err(|e| failed(path, e))
    }
}

fn layout<'a>(path: &str, o: &'a OpenOut) -> Result<&'a LayoutV1, Failed> {
    o.layout.as_ref().ok_or_else(|| failed(path, ErrCode::EINVAL)) // 不是普通文件
}

/// 文件第 `idx` 个 chunk（`idx` 超过 u32 = 文件长过了最大长度 → `EFBIG`；layout 没有链 → `EINVAL`）。
fn chunk_of(path: &str, o: &OpenOut, l: &LayoutV1, idx: u64) -> Result<ChunkId, Failed> {
    let idx = u32::try_from(idx).map_err(|_| failed(path, ErrCode::EFBIG))?;
    chunk_id(l, o.attr.ino, idx).map_err(|e| failed(path, e))
}

/// 读满 `buf`（或读到文件尾），返回读到的字节数。
fn fill(input: &mut dyn Read, buf: &mut [u8]) -> std::io::Result<usize> {
    let mut n = 0;
    while n < buf.len() {
        match input.read(&mut buf[n..])? {
            0 => break,
            k => n += k,
        }
    }
    Ok(n)
}

fn io_err(path: &str, e: std::io::Error) -> Failed {
    failed(path, ErrCode::from(e))
}

/// `put`：见文件头。`parent`、`name` 已由调用方解析好。
pub fn put(x: &Ctx, src: &str, dst: &str, (parent, name): (u64, Vec<u8>), umask: u32) -> Result<Done, Failed> {
    let mut input: Box<dyn Read> = if src == "-" {
        Box::new(std::io::stdin().lock())
    } else {
        Box::new(std::fs::File::open(src).map_err(|e| io_err(src, e))?)
    };
    let op = MetaOp::Create { parent, name, mode: 0o666, umask, excl: false };
    let mut o = x.opened(dst, x.meta.write(x.who, op))?;
    if o.attr.size > 0 {
        o = x.set_size(dst, o.attr.ino, 0, 0)?; // 覆盖已有的内容：截断到 0（新 epoch，按当前属性重建 layout）
    }
    let cs = layout(dst, &o)?.chunk_size as usize;
    let mut buf = x.buf(dst, cs)?;
    let mut n = fill(&mut input, buf.as_mut_slice()).map_err(|e| io_err(src, e))?;
    let inline_max = layout(dst, &o)?.inline_max as usize;
    if o.attr.flags & F_INLINE != 0 && n <= inline_max && n < cs {
        x.commit(dst, &o, n as u64, vec![(0, buf.as_slice()[..n].to_vec())])?;
        let j = json!({"ok": true, "ino": o.attr.ino, "size": n, "inline": true});
        return Ok(Done {
            text: format!("已写入 {dst}（{n} 字节，inline：内容存在元数据里）"), json: j
        });
    }
    if o.attr.flags & F_INLINE != 0 {
        o = x.opened(dst, x.meta.read(x.who, MetaOp::Uninline { ino: o.attr.ino }))?; // 长过阈值：请服务端先转成 chunk
    }
    let l = layout(dst, &o)?.clone();
    let (mut idx, mut total) = (0u64, 0u64);
    loop {
        if n > 0 {
            let fb = buf.freeze();
            let ext = vec![(0, n as u32)];
            let chunk = chunk_of(dst, &o, &l, idx)?;
            x.data.write(&o.cap, chunk, ext, vec![fb.slice(0, n as u32)], false).map_err(|e| failed(dst, e))?;
            total += n as u64;
        }
        if n < cs {
            break;
        }
        (idx, buf) = (idx + 1, x.buf(dst, cs)?);
        n = fill(&mut input, buf.as_mut_slice()).map_err(|e| io_err(src, e))?;
    }
    x.commit(dst, &o, total, vec![])?;
    let chunks = total.div_ceil(cs as u64);
    let j = json!({"ok": true, "ino": o.attr.ino, "size": total, "inline": false, "chunks": chunks});
    Ok(Done { text: format!("已写入 {dst}（{total} 字节，{chunks} 个 chunk）"), json: j })
}

/// 往 `out` 写 `n` 个 0。
fn zeros(out: &mut dyn Write, mut n: u64) -> std::io::Result<()> {
    let z = [0u8; 65536];
    while n > 0 {
        let k = n.min(z.len() as u64) as usize;
        out.write_all(&z[..k])?;
        n -= k as u64;
    }
    Ok(())
}

/// `get` / `cat`：见文件头。`dst` 为 "-" 时写到标准输出。
pub fn get(x: &Ctx, ino: u64, src: &str, dst: &str) -> Result<Done, Failed> {
    let o = x.opened(src, x.meta.read(x.who, MetaOp::Open { ino, flags: O_RDONLY }))?;
    let size = o.attr.size;
    let mut out: Box<dyn Write> = if dst == "-" {
        Box::new(std::io::stdout().lock())
    } else {
        Box::new(std::io::BufWriter::new(std::fs::File::create(dst).map_err(|e| io_err(dst, e))?))
    };
    let w = |r: std::io::Result<()>| r.map_err(|e| io_err(dst, e));
    if o.attr.flags & F_INLINE != 0 {
        let data = o.inline.clone().unwrap_or_default();
        let n = (data.len() as u64).min(size);
        w(out.write_all(&data[..n as usize]))?;
        w(zeros(&mut out, size - n))?; // [len(INLINE), size) 读为 0（3.2.7 的不变式）
    } else {
        let l = layout(src, &o)?;
        let cs = l.chunk_size as u64;
        for idx in 0..size.div_ceil(cs) {
            let len = cs.min(size - idx * cs) as u32;
            let into = x.buf(src, len as usize)?;
            match x.data.read(&o.cap, l, chunk_of(src, &o, l, idx)?, 0, len, into, None) {
                Ok((b, n)) => {
                    w(out.write_all(&b.as_slice()[..n as usize]))?;
                    w(zeros(&mut out, (len - n) as u64))?;
                }
                Err(ErrCode::CHUNK_NOT_FOUND) => {
                    // C5：先确认 epoch 没变（没被截断到 0 / 迁移），才把这个 chunk 当空洞
                    match x.meta.read(x.who, MetaOp::GetAttr { ino }).map_err(|e| failed(src, e))? {
                        MetaReply::Entry { attr } if attr.epoch == l.epoch => w(zeros(&mut out, len as u64))?,
                        _ => return Err(failed(src, ErrCode::ESTALE)), // 读的过程中文件被截断或换了布局：重新读一遍
                    }
                }
                Err(e) => return Err(failed(src, e)),
            }
        }
    }
    w(out.flush())?;
    let text = format!("已读出 {src} → {dst}（{size} 字节）"); // dst 是 "-" 时调用方不打印它：标准输出上只有文件内容
    Ok(Done { text, json: json!({"ok": true, "ino": ino, "size": size}) })
}

/// `truncate`：见文件头。
pub fn truncate(x: &Ctx, ino: u64, path: &str, size: u64) -> Result<Done, Failed> {
    let o = x.opened(path, x.meta.read(x.who, MetaOp::Open { ino, flags: O_WRONLY }))?;
    let l = layout(path, &o)?;
    if size > 0 && o.attr.flags & F_INLINE == 0 {
        let cs = l.chunk_size as u64;
        for idx in size / cs..o.attr.size.div_ceil(cs) {
            let new_len = size.saturating_sub(idx * cs).min(cs) as u32;
            x.data.truncate(&o.cap, chunk_of(path, &o, l, idx)?, new_len).map_err(|e| failed(path, e))?;
        }
    }
    let o = x.set_size(path, ino, size, l.epoch)?;
    Ok(Done {
        text: format!("已把 {path} 截断到 {size} 字节"),
        json: json!({"ok": true, "ino": ino, "size": o.attr.size}),
    })
}
