//! 递归用量计数 RSTAT（design.md 3.9，ADR-0026，键空间 0x06）：每个目录一条 `RStatV1`，覆盖它的整个子树（不含自己）——
//! `bytes` = 子树里**普通文件**的逻辑大小之和，`files` = 非目录 inode 数（符号链接、设备节点各算 1 个、0 字节），`dirs` = 子目录数，
//! `per_class` = 普通文件按存储类分的 (字节, 文件数)。读取 = 一次点查（`wmfs usage`、配额检查都直接读它）。
//!
//! 更新：在执行器闭包里，对受影响 inode 的**主父目录**到根的每个祖先读-改-写（批次覆盖层让同一批对根的多次更新只写一次），
//! O(深度)。一个 inode 计在它的主父目录名下、只计一次（硬链接只在主链接所在目录计，与 CephFS 相同）；孤儿（nlink = 0）不计——
//! 最后一个链接被删时就扣掉，之后再写它也不改计数。各操作的增量（3.9 的表）由命名空间代码调这里的函数：create/mknod/symlink/
//! mkdir → [`apply`]（新目录先 [`init_dir`]）；unlink/rmdir/被覆盖的 rename 目标 → [`apply`] 负的增量，删的是主链接但还有别的链接 →
//! [`transfer`] 到新的主父目录；rename → [`transfer`]（到最近公共祖先为止）；改了长度或存储类 → [`changed`]。
//! 自检：离线工具 `wmfs debug rstat-check <data_dir>`（[`check_data_dir`]：从全部 inode 重新算一遍，与 RSTAT 逐个比较）。

use std::collections::HashMap;
use std::path::Path;

use crate::base::ErrCode;
use crate::meta::attrs::MAX_DEPTH;
use crate::meta::engine::txn::{KvRead, KvScan, KvTxn, OpCtx};
use crate::meta::engine::view::ReadView;
use crate::meta::ns::common::Node;
use crate::meta::schema::keys::{self, ROOT_INO, T_INODE, T_RSTAT};
use crate::meta::schema::kv;
use crate::meta::schema::records::{InodeV1, KIND_DIR, KIND_FILE, RStatV1, decode_v1};

/// 一份计数的增量（可正可负）。
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Delta {
    pub bytes: i64,
    pub files: i64,
    pub dirs: i64,
    pub per_class: Vec<(u16, i64, i64)>, // (class, bytes, files)
}

impl Delta {
    /// inode `n` 计在它的主父目录祖先链上的那一份：目录 = 它的 RSTAT + 1 个目录；普通文件 = 逻辑大小 + 1 个文件（计入它的存储类）；
    /// 其他 = 1 个文件；孤儿（nlink = 0）= 0。
    pub fn of(v: &dyn KvRead, n: &Node) -> Result<Delta, ErrCode> {
        Ok(match n.rec.kind {
            _ if n.rec.nlink == 0 => Delta::default(),
            KIND_DIR => {
                let r = get(v, n.ino)?;
                let per_class = r.per_class.iter().map(|(c, b, f)| (*c, *b as i64, *f as i64)).collect();
                Delta { bytes: r.bytes as i64, files: r.files as i64, dirs: r.dirs as i64 + 1, per_class }
            }
            KIND_FILE => file_delta(n.rec.class_id, n.rec.size),
            _ => Delta { files: 1, ..Delta::default() },
        })
    }

    pub fn neg(&self) -> Delta {
        let per_class = self.per_class.iter().map(|(c, b, f)| (*c, -b, -f)).collect();
        Delta { bytes: -self.bytes, files: -self.files, dirs: -self.dirs, per_class }
    }

    /// `self − o`（同一个类合成一项、去掉 0：长度没变的提交算出来正好是"没有变化"，`apply` 就什么也不写）。
    pub fn minus(&self, o: &Delta) -> Delta {
        let mut d = self.clone();
        d.bytes -= o.bytes;
        d.files -= o.files;
        d.dirs -= o.dirs;
        for (c, b, f) in &o.per_class {
            match d.per_class.iter_mut().find(|(k, ..)| k == c) {
                Some((_, bytes, files)) => {
                    *bytes -= *b;
                    *files -= *f;
                }
                None => d.per_class.push((*c, -*b, -*f)),
            }
        }
        d.per_class.retain(|(_, b, f)| *b != 0 || *f != 0);
        d
    }
}

fn file_delta(class: u16, size: u64) -> Delta {
    let per_class = if class == 0 { vec![] } else { vec![(class, size as i64, 1)] };
    Delta { bytes: size as i64, files: 1, dirs: 0, per_class }
}

/// 一个目录的 RSTAT（没有记录 = 全 0）。
pub fn get(v: &dyn KvRead, dir: u64) -> Result<RStatV1, ErrCode> {
    Ok(kv::get(v, &keys::rstat(dir))?.unwrap_or_default())
}

/// 把增量加进一条计数（饱和：不会因为减成负数而回绕；正常情况下不会发生，rstat-check 查得出来）。
pub fn add(r: &mut RStatV1, d: &Delta) {
    let f = |x: u64, dx: i64| x.saturating_add_signed(dx);
    r.bytes = f(r.bytes, d.bytes);
    r.files = f(r.files, d.files);
    r.dirs = f(r.dirs, d.dirs);
    for (c, b, n) in &d.per_class {
        match r.per_class.iter_mut().find(|(k, ..)| k == c) {
            Some((_, bytes, files)) => {
                *bytes = f(*bytes, *b);
                *files = f(*files, *n);
            }
            None => r.per_class.push((*c, f(0, *b), f(0, *n))),
        }
    }
    r.per_class.retain(|(_, b, n)| *b != 0 || *n != 0);
    r.per_class.sort_by_key(|(c, ..)| *c);
}

fn add_to(v: &mut dyn KvTxn, dir: u64, d: &Delta) -> Result<(), ErrCode> {
    let mut r = get(v, dir)?;
    add(&mut r, d);
    kv::put(v, &keys::rstat(dir), &r);
    Ok(())
}

/// 新目录的空 RSTAT（mkdir）。
pub fn init_dir(v: &mut dyn KvTxn, dir: u64) {
    kv::put(v, &keys::rstat(dir), &RStatV1::default());
}

/// `dir` 到根的每个祖先（含 `dir`）都加上 `d`。
pub fn apply(v: &mut dyn KvTxn, c: &OpCtx, dir: u64, d: &Delta) -> Result<(), ErrCode> {
    if *d == Delta::default() {
        return Ok(());
    }
    for a in c.attrs.ancestors(v, dir)?.iter() {
        add_to(v, *a, d)?;
    }
    Ok(())
}

/// 一份计数从 `from` 的祖先链搬到 `to` 的祖先链（rename、主链接换了目录），到最近公共祖先为止（它和它上面的不变）。
pub fn transfer(v: &mut dyn KvTxn, c: &OpCtx, from: u64, to: u64, d: &Delta) -> Result<(), ErrCode> {
    if from == to || *d == Delta::default() {
        return Ok(());
    }
    let (af, at) = (c.attrs.ancestors(v, from)?, c.attrs.ancestors(v, to)?);
    let neg = d.neg();
    for a in af.iter().take_while(|a| !at.contains(a)) {
        add_to(v, *a, &neg)?;
    }
    for a in at.iter().take_while(|a| !af.contains(a)) {
        add_to(v, *a, d)?;
    }
    Ok(())
}

/// 同一个普通文件的长度或存储类变了（截断、提交写入、fallocate）：`before` 是改之前的那一份（[`Delta::of`]）。
pub fn changed(v: &mut dyn KvTxn, c: &OpCtx, before: &Delta, after: &Node) -> Result<(), ErrCode> {
    let d = Delta::of(v, after)?.minus(before);
    apply(v, c, after.rec.parent, &d)
}

/// 离线自检的结果。
#[derive(Debug, Default)]
pub struct Report {
    pub inodes: u64,
    pub dirs: u64,
    pub problems: Vec<String>,
}

/// 从全部 inode 重新算一遍每个目录应有的 RSTAT，与记录逐个比较（3.9 的自检；只读，整个元数据库读进内存）。
pub fn check(v: &dyn KvScan) -> Result<Report, ErrCode> {
    let mut all: HashMap<u64, InodeV1> = HashMap::new();
    for (k, b) in kv::scan_tag(v, T_INODE)? {
        all.insert(kv::key_id(&k), decode_v1(&b)?);
    }
    let mut want: HashMap<u64, RStatV1> =
        all.iter().filter(|(_, n)| n.kind == KIND_DIR).map(|(i, _)| (*i, RStatV1::default())).collect();
    let mut rep = Report { inodes: all.len() as u64, dirs: want.len() as u64, problems: vec![] };
    for (ino, n) in &all {
        if *ino == ROOT_INO || n.nlink == 0 {
            continue;
        }
        let d = match n.kind {
            KIND_DIR => Delta { dirs: 1, ..Delta::default() },
            KIND_FILE => file_delta(n.class_id, n.size),
            _ => Delta { files: 1, ..Delta::default() },
        };
        let (mut p, mut depth) = (n.parent, 0);
        loop {
            let Some(w) = want.get_mut(&p) else {
                rep.problems.push(format!("inode {ino} 的祖先 {p} 不是存在的目录"));
                break;
            };
            add(w, &d);
            depth += 1;
            if p == ROOT_INO || depth > MAX_DEPTH {
                break;
            }
            p = all.get(&p).map_or(0, |x| x.parent);
        }
    }
    let mut have: HashMap<u64, RStatV1> = HashMap::new();
    for (k, b) in kv::scan_tag(v, T_RSTAT)? {
        have.insert(kv::key_id(&k), decode_v1(&b)?);
    }
    for (d, w) in &want {
        match have.remove(d) {
            Some(h) if h == *w => {}
            Some(h) => rep.problems.push(format!("目录 {d}：RSTAT 是 {h:?}，按 inode 算应是 {w:?}")),
            None => rep.problems.push(format!("目录 {d} 没有 RSTAT 记录（按 inode 算应是 {w:?}）")),
        }
    }
    for d in have.keys() {
        rep.problems.push(format!("{d} 不是存在的目录，却有 RSTAT 记录"));
    }
    rep.problems.sort();
    Ok(rep)
}

/// `wmfs debug rstat-check <data_dir>`：打开 `<data_dir>/meta/meta.redb`（节点要先停掉：redb 不允许两个进程同时打开）做 [`check`]。
pub fn check_data_dir(data_dir: &Path) -> Result<Report, String> {
    let p = data_dir.join("meta/meta.redb");
    let db = redb::Database::open(&p)
        .map_err(|e| format!("打不开元数据库 {}：{e}（节点还在运行？先停掉它，例如 wmfs dev down）", p.display()))?;
    let rtx = db.begin_read().map_err(|e| format!("读 {} 失败：{e}", p.display()))?;
    let view = ReadView::new(&rtx).map_err(|e| format!("读 {} 失败：{e}", p.display()))?;
    check(&view).map_err(|e| format!("扫描 {} 失败：{e}", p.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deltas_add_up_and_per_class_stays_sorted() {
        let mut r = RStatV1::default();
        add(&mut r, &file_delta(2, 100));
        add(&mut r, &file_delta(1, 5));
        add(&mut r, &Delta { dirs: 1, ..Delta::default() });
        assert_eq!(r, RStatV1 { bytes: 105, files: 2, dirs: 1, per_class: vec![(1, 5, 1), (2, 100, 1)] });
        add(&mut r, &file_delta(2, 100).neg());
        assert_eq!(r.per_class, vec![(1, 5, 1)], "减到 0 的类去掉");
        let d = file_delta(1, 30).minus(&file_delta(1, 5));
        assert_eq!(d.per_class, vec![(1, 25, 0)], "同一个类合成一项");
        assert_eq!(file_delta(1, 30).minus(&file_delta(1, 30)), Delta::default(), "长度没变：没有变化");
        add(&mut r, &d);
        assert_eq!((r.bytes, r.files, r.per_class.clone()), (30, 1, vec![(1, 30, 1)]), "长度从 5 变成 30");
        add(&mut r, &Delta { files: -5, ..Delta::default() });
        assert_eq!(r.files, 0, "饱和，不回绕");
    }
}
