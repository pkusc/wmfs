//! 带种子的随机测试（design.md 9.1；自带的 xorshift，失败时消息里有种子）：在一个小命名空间上随机做 mkdir、create、symlink、
//! link、unlink、rmdir、rename（含 NOREPLACE / EXCHANGE），每一步之后检查 KV 的不变式——
//! ① 每个目录项都指向一个存在的 inode，种类一致；② 每个 inode 的链接（主链接 + extra_links）恰好是指向它的全部目录项；
//! ③ 非目录的 nlink = 链接数，nlink 为 0 ⇔ 孤儿；④ 目录的 size = 直接子项数、nlink = 2 + 子目录数、parent 指针指向
//! 唯一指向它的目录项；⑤ 从根沿目录项走得到每个目录（没有环、没有悬空的子树）。操作失败（EEXIST、ENOTEMPTY……）是正常的，
//! 不变式必须一直成立（失败的操作整体丢弃）。

use std::collections::{BTreeMap, BTreeSet};

use wmfs_core::base::rand::XorShift64;
use wmfs_core::meta::schema::keys::{self, ROOT_INO};
use wmfs_core::meta::schema::records::{DentryV1, F_ORPHAN, InodeV1, KIND_DIR, decode_v1};
use wmfs_core::proto::meta::{MetaOp, RENAME_EXCHANGE, RENAME_NOREPLACE};
use wmfs_testkit::harness::ns::{Ns, OWNER};

const NAMES: [&str; 5] = ["a", "b", "c", "d", "e"];

/// 检查不变式；违反了返回说明。
fn check(ns: &Ns) -> Result<(), String> {
    let inodes: BTreeMap<u64, InodeV1> = ns
        .tag(keys::T_INODE)
        .iter()
        .map(|(k, v)| (u64::from_be_bytes(k[1..9].try_into().unwrap()), decode_v1(v).unwrap()))
        .collect();
    let mut links: BTreeMap<u64, BTreeSet<(u64, Vec<u8>)>> = BTreeMap::new();
    let mut children: BTreeMap<u64, (u64, u64)> = BTreeMap::new(); // 目录 → (子项数, 子目录数)
    for (k, v) in ns.tag(keys::T_DENTRY) {
        let (parent, name) = (u64::from_be_bytes(k[1..9].try_into().unwrap()), k[9..].to_vec());
        let d: DentryV1 = decode_v1(&v).unwrap();
        let i = inodes.get(&d.ino).ok_or(format!("目录项 {parent}/{name:?} 指向不存在的 inode {}", d.ino))?;
        if i.kind != d.kind {
            return Err(format!("目录项 {parent}/{name:?} 的种类与 inode {} 不一致", d.ino));
        }
        links.entry(d.ino).or_default().insert((parent, name));
        let e = children.entry(parent).or_default();
        e.0 += 1;
        e.1 += (d.kind == KIND_DIR) as u64;
    }
    for (ino, i) in &inodes {
        let got = links.remove(ino).unwrap_or_default();
        let mut want: BTreeSet<(u64, Vec<u8>)> = i.extra_links.iter().cloned().collect();
        if *ino == ROOT_INO {
            want.clear();
        } else if i.nlink > 0 {
            want.insert((i.parent, i.name.clone()));
        }
        if got != want {
            return Err(format!("inode {ino} 的链接 {want:?} 与目录项 {got:?} 不一致"));
        }
        let (n, subdirs) = children.get(ino).copied().unwrap_or_default();
        if i.kind == KIND_DIR {
            if (i.size, i.nlink as u64) != (n, 2 + subdirs) {
                return Err(format!("目录 {ino}：size {} nlink {}，实际 {n} 项、{subdirs} 个子目录", i.size, i.nlink));
            }
        } else if i.nlink as usize != got.len() || (i.nlink == 0) != (i.flags & F_ORPHAN != 0) {
            return Err(format!(
                "inode {ino}：nlink {}、孤儿标记 {}，实际 {} 个链接",
                i.nlink,
                i.flags & F_ORPHAN,
                got.len()
            ));
        }
    }
    // 从根走得到每个目录
    let (mut seen, mut todo) = (BTreeSet::from([ROOT_INO]), vec![ROOT_INO]);
    while let Some(d) = todo.pop() {
        for (k, v) in ns.tag(keys::T_DENTRY).iter().filter(|(k, _)| k[1..9] == d.to_be_bytes()) {
            let c: DentryV1 = decode_v1(v).unwrap();
            if c.kind == KIND_DIR && !seen.insert(c.ino) {
                return Err(format!("目录 {} 出现了两次（{k:?}）：目录树里有环或重复", c.ino));
            }
            if c.kind == KIND_DIR {
                todo.push(c.ino);
            }
        }
    }
    let dirs = inodes.iter().filter(|(_, i)| i.kind == KIND_DIR).count();
    if seen.len() != dirs {
        return Err(format!("从根走到了 {} 个目录，一共有 {dirs} 个", seen.len()));
    }
    Ok(())
}

fn random_op(r: &mut XorShift64, dirs: &[u64], files: &[u64]) -> MetaOp {
    let dir = |r: &mut XorShift64| dirs[r.below(dirs.len() as u64) as usize];
    let name = |r: &mut XorShift64| NAMES[r.below(NAMES.len() as u64) as usize].as_bytes().to_vec();
    match r.below(10) {
        0 | 1 => MetaOp::Mkdir { parent: dir(r), name: name(r), mode: 0o755, umask: 0 },
        2 | 3 => MetaOp::Create { parent: dir(r), name: name(r), mode: 0o644, umask: 0, excl: r.below(2) == 0 },
        4 => MetaOp::Symlink { parent: dir(r), name: name(r), target: b"x".to_vec() },
        5 if !files.is_empty() => {
            MetaOp::Link { ino: files[r.below(files.len() as u64) as usize], new_parent: dir(r), new_name: name(r) }
        }
        6 => MetaOp::Unlink { parent: dir(r), name: name(r) },
        7 => MetaOp::Rmdir { parent: dir(r), name: name(r) },
        _ => {
            let flags = [0, 0, RENAME_NOREPLACE, RENAME_EXCHANGE][r.below(4) as usize];
            MetaOp::Rename { parent: dir(r), name: name(r), new_parent: dir(r), new_name: name(r), flags }
        }
    }
}

#[test]
fn random_namespace_ops_keep_the_invariants() {
    let seeds: u64 = std::env::var("WMFS_NS_SEEDS").ok().and_then(|s| s.parse().ok()).unwrap_or(200);
    let mut ok_ops = 0;
    for seed in 1..=seeds {
        let mut r = XorShift64::new(seed);
        let mut ns = Ns::new();
        for step in 0..120 {
            let inodes: Vec<(u64, InodeV1)> = ns
                .tag(keys::T_INODE)
                .iter()
                .map(|(k, v)| (u64::from_be_bytes(k[1..9].try_into().unwrap()), decode_v1(v).unwrap()))
                .collect();
            let dirs: Vec<u64> = inodes.iter().filter(|(_, i)| i.kind == KIND_DIR).map(|(n, _)| *n).collect();
            let files: Vec<u64> =
                inodes.iter().filter(|(_, i)| i.kind != KIND_DIR && i.nlink > 0).map(|(n, _)| *n).collect();
            let op = random_op(&mut r, &dirs, &files);
            let what = format!("{op:?}");
            if ns.op(&OWNER, op).is_ok() {
                ok_ops += 1;
            }
            if let Err(e) = check(&ns) {
                panic!("种子 {seed} 第 {step} 步（{what}）之后不变式不成立：{e}");
            }
        }
    }
    assert!(ok_ops > seeds * 30, "大部分随机操作应该成功：{ok_ops}");
}
