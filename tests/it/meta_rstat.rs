//! M7 的递归用量计数 RSTAT（design.md 3.9 的增量表，ADR-0026），在内存 KV 视图上：
//! - 逐条：创建（文件、符号链接、设备节点、目录）、改长度、硬链接（只在主链接所在目录计；删主链接时搬到 extra_links[0] 的目录）、
//!   rename（跨目录搬到最近公共祖先为止、覆盖目标、RENAME_EXCHANGE）、孤儿（立即扣掉、之后再写也不计）、rmdir；
//! - 带种子的随机操作（create / 改长度 / CommitWrite / Fallocate / unlink / rename（跨目录、覆盖目标、对调）/ link / rmdir / mkdir /
//!   symlink），每一步之后用离线自检 `rstat::check` 从全部 inode 重新算一遍、与 RSTAT 逐个比较（失败时消息里有种子；
//!   `WMFS_RSTAT_SEEDS` 改种子数）；
//! - 自检本身查得出不一致（bytes 错了、目录没有 RSTAT、文件有 RSTAT）：它是上面所有检查共用的判据。

use wmfs_core::base::ErrCode;
use wmfs_core::base::rand::XorShift64;
use wmfs_core::base::types::Ts;
use wmfs_core::meta::rstat;
use wmfs_core::meta::schema::keys::{self, ROOT_INO};
use wmfs_core::meta::schema::kv::key_id;
use wmfs_core::meta::schema::records::{InodeV1, KIND_DIR, KIND_FIFO, KIND_FILE, RStatV1, decode_v1, encode_v1};
use wmfs_core::proto::meta::{MetaOp, MetaReply, RENAME_EXCHANGE, RENAME_NOREPLACE, SetAttrIn};
use wmfs_testkit::harness::ns::{Ns, OWNER};

fn rs(ns: &Ns, dir: u64) -> (u64, u64, u64) {
    let r: RStatV1 = decode_v1(&ns.kv.0[&keys::rstat(dir)]).unwrap();
    (r.bytes, r.files, r.dirs)
}

fn size(ns: &mut Ns, ino: u64, s: u64) {
    ns.set_size(&OWNER, ino, s).unwrap();
}

fn rename(ns: &mut Ns, p: u64, n: &str, np: u64, nn: &str, flags: u32) -> Result<MetaReply, ErrCode> {
    ns.op(&OWNER, MetaOp::Rename { parent: p, name: n.into(), new_parent: np, new_name: nn.into(), flags })
}

fn check(ns: &Ns) -> Vec<String> {
    rstat::check(&ns.kv).unwrap().problems
}

#[test]
fn increments_follow_the_table_of_3_9() {
    let mut ns = Ns::new();
    assert_eq!(rs(&ns, ROOT_INO), (0, 0, 2), "init：/home、/scratch");
    let a = ns.mkdir(&OWNER, ROOT_INO, "a").unwrap();
    let b = ns.mkdir(&OWNER, a, "b").unwrap();
    let c = ns.mkdir(&OWNER, ROOT_INO, "c").unwrap();
    assert_eq!(
        (rs(&ns, ROOT_INO), rs(&ns, a), rs(&ns, b)),
        ((0, 0, 5), (0, 0, 1), (0, 0, 0)),
        "mkdir：dirs + 1，新目录从 0 开始"
    );
    let f = ns.create(&OWNER, b, "f").unwrap();
    size(&mut ns, f, 1000);
    ns.op(&OWNER, MetaOp::Symlink { parent: b, name: b"l".to_vec(), target: b"f".to_vec() }).unwrap();
    let fifo = MetaOp::Mknod { parent: b, name: b"p".to_vec(), mode: 0o644, umask: 0, kind: KIND_FIFO, rdev: 0 };
    ns.op(&OWNER, fifo).unwrap();
    assert_eq!(
        (rs(&ns, a), rs(&ns, ROOT_INO)),
        ((1000, 3, 1), (1000, 3, 5)),
        "文件、符号链接、设备节点各算 1 个，只有文件有字节"
    );
    // 硬链接：只在主链接所在目录计；删掉主链接 → 搬到 extra_links[0] 的目录
    ns.op(&OWNER, MetaOp::Link { ino: f, new_parent: c, new_name: b"h".to_vec() }).unwrap();
    assert_eq!((rs(&ns, a), rs(&ns, c)), ((1000, 3, 1), (0, 0, 0)), "link 不变");
    ns.op(&OWNER, MetaOp::Unlink { parent: b, name: b"f".to_vec() }).unwrap();
    assert_eq!((rs(&ns, a), rs(&ns, c), rs(&ns, ROOT_INO)), ((0, 2, 1), (1000, 1, 0), (1000, 3, 5)), "计数搬到 /c");
    // rename：跨目录，到最近公共祖先为止
    rename(&mut ns, c, "h", b, "h2", 0).unwrap();
    assert_eq!((rs(&ns, b), rs(&ns, c), rs(&ns, ROOT_INO)), ((1000, 3, 0), (0, 0, 0), (1000, 3, 5)));
    rename(&mut ns, a, "b", c, "b", 0).unwrap();
    assert_eq!((rs(&ns, a), rs(&ns, c)), ((0, 0, 0), (1000, 3, 1)), "目录连同它的整个子树一起搬");
    // 覆盖目标：被覆盖的文件按 unlink 扣
    let g = ns.create(&OWNER, a, "g").unwrap();
    size(&mut ns, g, 7);
    rename(&mut ns, a, "g", b, "h2", 0).unwrap();
    assert_eq!((rs(&ns, b), rs(&ns, a)), ((7, 3, 0), (0, 0, 0)), "h2 被覆盖（最后一个链接）：扣掉它的 1000 字节");
    // RENAME_EXCHANGE：两边各自搬
    let x = ns.create(&OWNER, a, "x").unwrap();
    size(&mut ns, x, 50);
    rename(&mut ns, a, "x", b, "l", RENAME_EXCHANGE).unwrap();
    assert_eq!((rs(&ns, a), rs(&ns, b)), ((0, 1, 0), (57, 3, 0)), "x（50 字节）进了 b，符号链接 l 进了 a");
    // 孤儿：最后一个链接删掉就扣掉；之后再改长度也不计
    ns.op(&OWNER, MetaOp::Unlink { parent: b, name: b"l".to_vec() }).unwrap();
    assert_eq!(rs(&ns, b), (7, 2, 0));
    size(&mut ns, x, 999);
    assert_eq!(rs(&ns, b), (7, 2, 0), "孤儿不计");
    assert_eq!(rename(&mut ns, ROOT_INO, "a", c, "b", RENAME_NOREPLACE).err(), Some(ErrCode::EEXIST));
    ns.op(&OWNER, MetaOp::Unlink { parent: a, name: b"x".to_vec() }).unwrap(); // 对调之后 a/x 是那个符号链接
    ns.op(&OWNER, MetaOp::Rmdir { parent: ROOT_INO, name: b"a".to_vec() }).unwrap();
    assert_eq!(rs(&ns, ROOT_INO), (7, 2, 4), "rmdir：dirs − 1");
    assert_eq!(check(&ns), Vec::<String>::new(), "离线自检与增量一致");
}

const NAMES: [&str; 4] = ["a", "b", "c", "d"];

/// 一个随机操作：`files` 是 (inode, 当前 layout 的 epoch)，CommitWrite 要带对 epoch。
fn random_op(r: &mut XorShift64, dirs: &[u64], files: &[(u64, u32)]) -> MetaOp {
    let pick = |r: &mut XorShift64, v: &[u64]| v[r.below(v.len() as u64) as usize];
    let file = |r: &mut XorShift64| files[r.below(files.len() as u64) as usize];
    let name = |r: &mut XorShift64| NAMES[r.below(NAMES.len() as u64) as usize].as_bytes().to_vec();
    let len = |r: &mut XorShift64| r.below(3) * r.below(200_000);
    match r.below(14) {
        0 | 1 => MetaOp::Mkdir { parent: pick(r, dirs), name: name(r), mode: 0o755, umask: 0 },
        2 | 3 => MetaOp::Create { parent: pick(r, dirs), name: name(r), mode: 0o644, umask: 0, excl: false },
        4 => MetaOp::Symlink { parent: pick(r, dirs), name: name(r), target: b"t".to_vec() },
        5 | 6 if !files.is_empty() => {
            let set = SetAttrIn {
                mode: None,
                uid: None,
                gid: None,
                size: Some(len(r)),
                atime: None,
                mtime: None,
                epoch_seen: 0,
            };
            MetaOp::SetAttr { ino: file(r).0, set }
        }
        7 if !files.is_empty() => MetaOp::Link { ino: file(r).0, new_parent: pick(r, dirs), new_name: name(r) },
        8 => MetaOp::Unlink { parent: pick(r, dirs), name: name(r) },
        9 => MetaOp::Rmdir { parent: pick(r, dirs), name: name(r) },
        10 if !files.is_empty() => {
            MetaOp::Fallocate { ino: file(r).0, offset: r.below(100_000), len: 1 + len(r), mode: 0 }
        }
        11 if !files.is_empty() => {
            let (ino, epoch) = file(r);
            MetaOp::CommitWrite {
                ino,
                epoch,
                new_size: len(r),
                mtime: Ts::default(),
                inline_patches: vec![],
                uninline_expect_change_id: None,
                want_quota: false,
                close: false,
            }
        }
        _ => {
            let flags = [0, 0, RENAME_NOREPLACE, RENAME_EXCHANGE][r.below(4) as usize];
            MetaOp::Rename { parent: pick(r, dirs), name: name(r), new_parent: pick(r, dirs), new_name: name(r), flags }
        }
    }
}

#[test]
fn random_ops_keep_rstat_consistent() {
    let seeds: u64 = std::env::var("WMFS_RSTAT_SEEDS").ok().and_then(|s| s.parse().ok()).unwrap_or(150);
    let mut ok = 0;
    for seed in 1..=seeds {
        let mut r = XorShift64::new(seed);
        let mut ns = Ns::new();
        for step in 0..100 {
            let all: Vec<(u64, InodeV1)> =
                ns.tag(keys::T_INODE).iter().map(|(k, v)| (key_id(k), decode_v1(v).unwrap())).collect();
            let dirs: Vec<u64> = all.iter().filter(|(_, i)| i.kind == KIND_DIR).map(|(n, _)| *n).collect();
            let epoch = |i: &InodeV1| i.layout.as_ref().map_or(0, |l| l.epoch);
            let files: Vec<(u64, u32)> =
                all.iter().filter(|(_, i)| i.kind == KIND_FILE && i.nlink > 0).map(|(n, i)| (*n, epoch(i))).collect();
            let op = random_op(&mut r, &dirs, &files);
            let what = format!("{op:?}");
            ok += ns.op(&OWNER, op).is_ok() as u64;
            let bad = check(&ns);
            assert!(bad.is_empty(), "种子 {seed} 第 {step} 步（{what}）之后 RSTAT 不一致：{bad:?}");
        }
    }
    assert!(ok > seeds * 30, "大部分随机操作应该成功：{ok}");
}

#[test]
fn the_offline_check_finds_every_kind_of_inconsistency() {
    let mut ns = Ns::new();
    let a = ns.mkdir(&OWNER, ROOT_INO, "a").unwrap();
    let b = ns.mkdir(&OWNER, a, "b").unwrap();
    let f = ns.create(&OWNER, b, "f").unwrap();
    size(&mut ns, f, 100);
    assert_eq!(check(&ns), Vec::<String>::new());
    let mut r: RStatV1 = decode_v1(&ns.kv.0[&keys::rstat(a)]).unwrap();
    r.bytes += 1;
    ns.kv.0.insert(keys::rstat(a), encode_v1(&r)); // ① 一个目录的 bytes 错了
    ns.kv.0.remove(&keys::rstat(b)); // ② 一个目录没有 RSTAT
    ns.kv.0.insert(keys::rstat(f), encode_v1(&RStatV1::default())); // ③ 一个文件有 RSTAT
    let p = check(&ns);
    assert_eq!(p.len(), 3, "每种不一致各报一条：{p:?}");
    assert!(p.iter().any(|x| x.starts_with(&format!("目录 {a}：RSTAT 是"))), "{p:?}");
    assert!(p.iter().any(|x| x.starts_with(&format!("目录 {b} 没有 RSTAT 记录"))), "{p:?}");
    assert!(p.iter().any(|x| x.starts_with(&format!("{f} 不是存在的目录，却有 RSTAT 记录"))), "{p:?}");
}
