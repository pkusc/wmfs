//! POSIX 元数据语义（design.md 9.1、3.1.1、3.5.1）：create/mkdir/mknod/symlink/link/unlink/rmdir、readdir 分页、lookup 与
//! 按路径解析、setgid 目录、名字规则、权限位与粘滞位（含"没有执行权限就看不出目录里有哪些名字"）、孤儿标记、changelog
//! （每个修改型操作都写）。跑在内存 KV 视图上（`harness::ns`），每个操作一个"批次"、失败整体丢弃；新文件的 layout 指向一条
//! 刚生成的链（roadmap M4 的验收标准）。

use wmfs_core::base::ErrCode;
use wmfs_core::meta::schema::keys::{self, GC_ORPHAN, ROOT_INO};
use wmfs_core::meta::schema::records::{
    ChainRecV1, ChangeRecV1, F_INLINE, F_ORPHAN, GcRecV1, KIND_CHR, KIND_DIR, KIND_FIFO, KIND_FILE, KIND_SYMLINK,
    decode_v1,
};
use wmfs_core::proto::meta::{MetaOp, MetaReply, O_RDONLY, O_RDWR, O_WRONLY, SetAttrIn};
use wmfs_testkit::harness::ns::{Ns, OWNER, ROOT, user};

fn name(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

#[test]
fn mkdir_and_create_update_the_parent_and_new_files_get_a_fresh_chain() {
    let mut ns = Ns::new();
    let root0 = ns.attr(ROOT_INO);
    let a = ns.mkdir(&OWNER, ROOT_INO, "a").unwrap();
    assert_eq!(a, 18, "普通 inode 从 18 开始分配（init 用了 16、17）");
    let at = ns.attr(a);
    assert_eq!((at.kind, at.mode, at.uid, at.gid, at.nlink, at.size), (KIND_DIR, 0o755, 1000, 1001, 2, 0));
    let root = ns.attr(ROOT_INO);
    assert_eq!((root.nlink, root.size, root.change_id), (root0.nlink + 1, root0.size + 1, root0.change_id + 1));
    assert!(root.mtime > root0.mtime && root.ctime > root0.ctime, "父目录的 mtime/ctime 更新了");

    let MetaReply::Opened(o) =
        ns.op(&OWNER, MetaOp::Create { parent: a, name: name("f"), mode: 0o100644, umask: 0o022, excl: true }).unwrap()
    else {
        panic!("Create 应回 Opened");
    };
    assert_eq!((o.attr.kind, o.attr.mode, o.attr.nlink, o.attr.size), (KIND_FILE, 0o644, 1, 0), "类型位被去掉");
    assert_ne!(o.attr.flags & F_INLINE, 0, "inline 阈值 64K：新文件内联");
    assert_eq!((o.inline, o.quota_end), (Some(vec![]), u64::MAX));
    let l = o.layout.expect("普通文件有 layout");
    assert_eq!((l.epoch, l.chains.clone(), l.replicas, l.chunk_size, l.inline_max), (1, vec![1], 3, 4 << 20, 64 << 10));
    let chain: ChainRecV1 = decode_v1(&ns.kv.0[&keys::chain(1)]).unwrap();
    assert_eq!(
        (chain.replicas, chain.members.clone(), chain.ver),
        (3, vec![(1, 1)], 1),
        "刚生成的链：期望 3、实际 1 个成员"
    );
    let global = |ns: &Ns, g: &str| ns.kv.0[&keys::global(g)].clone();
    assert_eq!(decode_v1::<u64>(&global(&ns, keys::G_MAP_EPOCH)).unwrap(), 2, "生成新链：map_epoch + 1");
    let g = ns.create(&OWNER, a, "g").unwrap();
    assert_eq!(ns.inode(g).unwrap().layout.unwrap().chains, vec![1], "已有同 (class, replicas) 的链：不再生成");
    assert_eq!(decode_v1::<u64>(&global(&ns, keys::G_MAP_EPOCH)).unwrap(), 2);
    assert_eq!(ns.ls(a), vec!["f", "g"]);
}

#[test]
fn existing_names_and_bad_names_are_rejected() {
    let mut ns = Ns::new();
    let a = ns.mkdir(&OWNER, ROOT_INO, "a").unwrap();
    let f = ns.create(&OWNER, a, "f").unwrap();
    assert_eq!(ns.mkdir(&OWNER, ROOT_INO, "a"), Err(ErrCode::EEXIST));
    assert_eq!(ns.create(&OWNER, a, "f"), Err(ErrCode::EEXIST), "O_EXCL");
    let again = MetaOp::Create { parent: a, name: name("f"), mode: 0o644, umask: 0, excl: false };
    assert!(matches!(ns.op(&OWNER, again), Ok(MetaReply::Opened(o)) if o.attr.ino == f), "没有 O_EXCL：打开已有的文件");
    assert_eq!(ns.create(&OWNER, 999, "x"), Err(ErrCode::ENOENT), "父目录不存在");
    assert_eq!(ns.create(&OWNER, f, "x"), Err(ErrCode::ENOTDIR), "父目录是文件");
    assert_eq!(ns.mkdir(&OWNER, a, ""), Err(ErrCode::ENOENT));
    assert_eq!(ns.mkdir(&OWNER, a, &"n".repeat(256)), Err(ErrCode::ENAMETOOLONG));
    assert!(ns.mkdir(&OWNER, a, &"n".repeat(255)).is_ok(), "255 字节可以");
    for bad in ["x/y", ".", "..", "nul\0"] {
        assert_eq!(ns.mkdir(&OWNER, a, bad), Err(ErrCode::EINVAL), "{bad:?}");
    }
    let um = MetaOp::Mkdir { parent: a, name: name("um"), mode: 0o777, umask: 0o027 };
    assert!(matches!(ns.op(&OWNER, um), Ok(MetaReply::Entry { attr }) if attr.mode == 0o750), "mode & !umask");
}

#[test]
fn setgid_directories_pass_their_group_down() {
    let mut ns = Ns::new();
    let g = ns.mkdir(&ROOT, ROOT_INO, "g").unwrap();
    let set = SetAttrIn { mode: Some(0o2777), gid: Some(500), ..unset() };
    ns.op(&ROOT, MetaOp::SetAttr { ino: g, set }).unwrap();
    let sub = ns.mkdir(&OWNER, g, "sub").unwrap();
    let s = ns.attr(sub);
    assert_eq!((s.gid, s.mode & 0o2000), (500, 0o2000), "子目录继承组与 setgid");
    let op = MetaOp::Create { parent: g, name: name("f"), mode: 0o2755, umask: 0, excl: true };
    let MetaReply::Opened(o) = ns.op(&OWNER, op).unwrap() else { panic!() };
    assert_eq!((o.attr.gid, o.attr.mode), (500, 0o755), "不在组 500 里：新文件的 setgid 被去掉");
}

#[test]
fn symlinks_and_device_nodes() {
    let mut ns = Ns::new();
    let op = MetaOp::Symlink { parent: ROOT_INO, name: name("l"), target: name("some/where") };
    let MetaReply::Entry { attr } = ns.op(&OWNER, op).unwrap() else { panic!() };
    assert_eq!((attr.kind, attr.mode, attr.size), (KIND_SYMLINK, 0o777, 10));
    assert_eq!(ns.read(&OWNER, MetaOp::ReadLink { ino: attr.ino }), Ok(MetaReply::Link { target: name("some/where") }));
    assert_eq!(ns.read(&OWNER, MetaOp::ReadLink { ino: ROOT_INO }), Err(ErrCode::EINVAL), "不是符号链接");
    let sym = |t: Vec<u8>| MetaOp::Symlink { parent: ROOT_INO, name: name("m"), target: t };
    assert_eq!(ns.op(&OWNER, sym(vec![])), Err(ErrCode::ENOENT));
    assert_eq!(ns.op(&OWNER, sym(vec![b'x'; 4097])), Err(ErrCode::ENAMETOOLONG));
    assert!(ns.op(&OWNER, sym(vec![b'x'; 4096])).is_ok());
    let nod =
        |n: &str, kind, rdev| MetaOp::Mknod { parent: ROOT_INO, name: name(n), mode: 0o644, umask: 0, kind, rdev };
    let MetaReply::Entry { attr } = ns.op(&OWNER, nod("p", KIND_FIFO, 9)).unwrap() else { panic!() };
    assert_eq!((attr.kind, attr.rdev), (KIND_FIFO, 0), "fifo 没有设备号");
    assert_eq!(ns.op(&OWNER, nod("c", KIND_CHR, 0x0501)), Err(ErrCode::EPERM), "只有 root 能建设备节点");
    let MetaReply::Entry { attr } = ns.op(&ROOT, nod("c", KIND_CHR, 0x0501)).unwrap() else { panic!() };
    assert_eq!((attr.kind, attr.rdev), (KIND_CHR, 0x0501));
    assert_eq!(ns.op(&OWNER, nod("d", KIND_DIR, 0)), Err(ErrCode::EINVAL), "目录用 mkdir");
}

#[test]
fn hard_links_unlink_and_orphans() {
    let mut ns = Ns::new();
    let d = ns.mkdir(&OWNER, ROOT_INO, "d").unwrap();
    let f = ns.create(&OWNER, ROOT_INO, "f").unwrap();
    let link = |n: &str, ino| MetaOp::Link { ino, new_parent: d, new_name: name(n) };
    let MetaReply::Entry { attr } = ns.op(&OWNER, link("f2", f)).unwrap() else { panic!() };
    assert_eq!(attr.nlink, 2);
    assert_eq!(ns.inode(f).unwrap().extra_links, vec![(d, name("f2"))]);
    assert_eq!(ns.attr(d).size, 1, "目标目录多一项");
    assert_eq!(ns.op(&OWNER, link("f2", f)), Err(ErrCode::EEXIST));
    assert_eq!(ns.op(&OWNER, link("dd", d)), Err(ErrCode::EPERM), "目录不能硬链接");
    let mut i = ns.inode(f).unwrap();
    i.nlink = 1000;
    ns.kv.0.insert(keys::inode(f), wmfs_core::meta::schema::records::encode_v1(&i));
    assert_eq!(ns.op(&OWNER, link("f3", f)), Err(ErrCode::EMLINK), "每个文件 ≤ 1000 个链接");
    i.nlink = 2;
    ns.kv.0.insert(keys::inode(f), wmfs_core::meta::schema::records::encode_v1(&i));

    ns.op(&OWNER, MetaOp::Unlink { parent: ROOT_INO, name: name("f") }).unwrap();
    let i = ns.inode(f).unwrap();
    assert_eq!(
        (i.nlink, i.parent, i.name.clone(), i.extra_links.len()),
        (1, d, name("f2"), 0),
        "主链接改为 extra_links[0]"
    );
    assert_eq!(i.flags & F_ORPHAN, 0);
    ns.op(&OWNER, MetaOp::Unlink { parent: d, name: name("f2") }).unwrap();
    let i = ns.inode(f).expect("最后一个链接删掉之后 inode 还在（可能有人打开着）");
    assert_eq!((i.nlink, i.flags & F_ORPHAN), (0, F_ORPHAN), "孤儿标记");
    let gc: GcRecV1 = decode_v1(&ns.kv.0[&keys::gc(GC_ORPHAN, f, 0)]).unwrap();
    assert_eq!(gc.due_ms, gc.created_ms + 6000, "6 s 之后才可回收（3.5.6）");
    assert_eq!(gc.chains, vec![1]);
    assert_eq!(ns.op(&OWNER, link("back", f)), Err(ErrCode::ENOENT), "孤儿不能再被链接回来");
    assert_eq!(ns.ls(d), Vec::<String>::new());
}

#[test]
fn unlink_and_rmdir_rules() {
    let mut ns = Ns::new();
    let d = ns.mkdir(&OWNER, ROOT_INO, "d").unwrap();
    ns.create(&OWNER, d, "f").unwrap();
    let rm = |p, n: &str| MetaOp::Unlink { parent: p, name: name(n) };
    let rd = |p, n: &str| MetaOp::Rmdir { parent: p, name: name(n) };
    assert_eq!(ns.op(&OWNER, rm(ROOT_INO, "d")), Err(ErrCode::EISDIR));
    assert_eq!(ns.op(&OWNER, rd(d, "f")), Err(ErrCode::ENOTDIR));
    assert_eq!(ns.op(&OWNER, rd(ROOT_INO, "d")), Err(ErrCode::ENOTEMPTY));
    assert_eq!(ns.op(&OWNER, rm(d, "nope")), Err(ErrCode::ENOENT));
    ns.op(&OWNER, rm(d, "f")).unwrap();
    let before = ns.attr(ROOT_INO);
    ns.op(&OWNER, rd(ROOT_INO, "d")).unwrap();
    assert!(ns.inode(d).is_none(), "空目录直接删掉");
    let after = ns.attr(ROOT_INO);
    assert_eq!((after.nlink, after.size), (before.nlink - 1, before.size - 1));
    ns.op(&OWNER, rd(ROOT_INO, "home")).unwrap();
    assert!(
        !ns.kv.0.contains_key(&keys::dirattr(16)) && !ns.kv.0.contains_key(&keys::rstat(16)),
        "目录属性与递归计数一起删"
    );
}

#[test]
fn readdir_is_paged_by_name() {
    let mut ns = Ns::new();
    let d = ns.mkdir(&OWNER, ROOT_INO, "d").unwrap();
    let mut want: Vec<String> = (0..30).map(|i| format!("f{i:02}")).collect();
    for n in &want {
        ns.create(&OWNER, d, n).unwrap();
    }
    want.sort();
    let (mut got, mut after, mut pages) = (Vec::new(), Vec::new(), 0);
    loop {
        let op = MetaOp::ReadDir { ino: d, after: after.clone(), limit: 7, plus: pages % 2 == 0 };
        let MetaReply::Dir { entries, eof } = ns.read(&OWNER, op).unwrap() else { panic!() };
        assert!(entries.len() <= 7);
        assert!(entries.iter().all(|e| e.attr.is_some() == (pages % 2 == 0)), "plus 时带属性");
        after = entries.last().map(|e| e.name.clone()).unwrap_or_default();
        got.extend(entries.into_iter().map(|e| String::from_utf8(e.name).unwrap()));
        pages += 1;
        if eof {
            break;
        }
    }
    assert_eq!((got, pages), (want, 5), "30 项、每页 7 项：5 页，按名字字节序，不重不漏");
    assert_eq!(
        ns.read(&OWNER, MetaOp::ReadDir { ino: 19, after: vec![], limit: 10, plus: false }),
        Err(ErrCode::ENOTDIR)
    );
}

#[test]
fn lookup_and_resolve_path() {
    let mut ns = Ns::new();
    let a = ns.mkdir(&OWNER, ROOT_INO, "a").unwrap();
    let b = ns.mkdir(&OWNER, a, "b").unwrap();
    let f = ns.create(&OWNER, b, "f").unwrap();
    ns.op(&OWNER, MetaOp::Symlink { parent: a, name: name("s"), target: name("b") }).unwrap();
    assert_eq!(ns.ino("/"), Ok(ROOT_INO));
    assert_eq!(ns.ino("/a/./b/../b/f"), Ok(f));
    assert_eq!(ns.ino("//a//b/"), Ok(b));
    assert_eq!(ns.ino("/.."), Ok(ROOT_INO), "根的 .. 是根");
    assert_eq!(ns.ino("/a/b/f/x"), Err(ErrCode::ENOTDIR));
    assert_eq!(ns.ino("/a/s/f"), Err(ErrCode::ENOTDIR), "中间的符号链接不跟随");
    assert_eq!(ns.ino("/a/nope"), Err(ErrCode::ENOENT));
    assert_eq!(ns.ino("a/b"), Err(ErrCode::EINVAL), "要绝对路径");
    let MetaReply::Entry { attr } = ns.read(&OWNER, MetaOp::Lookup { parent: b, name: name("f") }).unwrap() else {
        panic!()
    };
    assert_eq!(attr.ino, f);
    assert_eq!(ns.read(&OWNER, MetaOp::Lookup { parent: b, name: name("g") }), Err(ErrCode::ENOENT));
}

#[test]
fn permission_bits_and_sticky_directories() {
    let mut ns = Ns::new();
    let p = ns.mkdir(&OWNER, ROOT_INO, "p").unwrap(); // 0755，属于 1000
    let (u2, u3) = (user(2000, 2000), user(3000, 3000));
    assert_eq!(ns.mkdir(&u2, p, "x"), Err(ErrCode::EACCES), "非属主不能在 0755 的目录里建");
    assert_eq!(ns.create(&u2, p, "x"), Err(ErrCode::EACCES));
    assert!(ns.mkdir(&ROOT, p, "x").is_ok(), "root 可以");
    let priv_dir = MetaOp::Mkdir { parent: ROOT_INO, name: name("priv"), mode: 0o700, umask: 0 };
    let MetaReply::Entry { attr } = ns.op(&OWNER, priv_dir).unwrap() else { panic!() };
    ns.create(&OWNER, attr.ino, "s").unwrap();
    assert_eq!(
        ns.read(&u2, MetaOp::ReadDir { ino: attr.ino, after: vec![], limit: 9, plus: false }),
        Err(ErrCode::EACCES)
    );
    assert_eq!(ns.read(&u2, MetaOp::Lookup { parent: attr.ino, name: name("s") }), Err(ErrCode::EACCES));
    assert_eq!(ns.read(&u2, MetaOp::ResolvePath { path: name("/priv/s") }), Err(ErrCode::EACCES), "路径上每一段都要 x");

    let scratch = 17; // 1777
    let f2 = ns.create(&u2, scratch, "f2").unwrap();
    assert_eq!(ns.attr(f2).uid, 2000);
    assert_eq!(ns.op(&u3, MetaOp::Unlink { parent: scratch, name: name("f2") }), Err(ErrCode::EPERM), "粘滞位");
    let mv = MetaOp::Rename { parent: scratch, name: name("f2"), new_parent: scratch, new_name: name("z"), flags: 0 };
    assert_eq!(ns.op(&u3, mv), Err(ErrCode::EPERM));
    ns.op(&OWNER, MetaOp::Unlink { parent: scratch, name: name("f2") }).unwrap(); // OWNER 是 /scratch 的属主
    let open = |ino, flags| MetaOp::Open { ino, flags };
    let f = ns.create(&OWNER, p, "f").unwrap(); // 0644
    assert!(matches!(ns.read(&u2, open(f, O_RDONLY)), Ok(MetaReply::Opened(o)) if o.layout.is_some()), "其他人可读");
    assert_eq!(ns.read(&u2, open(f, O_WRONLY)), Err(ErrCode::EACCES), "其他人不可写");
    assert_eq!(ns.read(&OWNER, open(p, O_RDWR)), Err(ErrCode::EISDIR));
    let again = MetaOp::Create { parent: p, name: name("f"), mode: 0o644, umask: 0, excl: false };
    assert_eq!(ns.op(&u2, again), Err(ErrCode::EACCES), "按名字打开已有的 0644 文件也要写权限");
}

#[test]
fn names_stay_hidden_without_search_permission() {
    let mut ns = Ns::new();
    let priv_dir = MetaOp::Mkdir { parent: ROOT_INO, name: name("priv"), mode: 0o733, umask: 0 }; // 其他人有 wx、没有 r
    let MetaReply::Entry { attr } = ns.op(&OWNER, priv_dir).unwrap() else { panic!() };
    let d = attr.ino;
    let f = ns.create(&OWNER, d, "f").unwrap();
    ns.op(&OWNER, MetaOp::SetAttr { ino: f, set: SetAttrIn { mode: Some(0o666), ..unset() } }).unwrap();
    ns.mkdir(&OWNER, d, "sub").unwrap();
    let u2 = user(2000, 2000);
    let create = |n: &str, excl| MetaOp::Create { parent: d, name: name(n), mode: 0o644, umask: 0, excl };
    assert!(
        matches!(ns.op(&u2, create("f", false)), Ok(MetaReply::Opened(o)) if o.attr.ino == f),
        "有 x：打开 0666 的文件"
    );
    assert_eq!(ns.op(&u2, create("f", true)), Err(ErrCode::EEXIST), "有 x：看得到名字");
    // 去掉其他人的 x（0730）：名字在不在都是 EACCES，看不出来
    ns.op(&OWNER, MetaOp::SetAttr { ino: d, set: SetAttrIn { mode: Some(0o730), ..unset() } }).unwrap();
    for (what, op) in [
        ("create 已有的名字（不带 O_EXCL）", create("f", false)),
        ("create 已有的名字（O_EXCL）", create("f", true)),
        ("create 新名字", create("new", true)),
        ("unlink 已有的", MetaOp::Unlink { parent: d, name: name("f") }),
        ("unlink 不存在的", MetaOp::Unlink { parent: d, name: name("nope") }),
        ("unlink 一个目录", MetaOp::Unlink { parent: d, name: name("sub") }),
        ("rmdir 已有的", MetaOp::Rmdir { parent: d, name: name("sub") }),
        ("rmdir 不存在的", MetaOp::Rmdir { parent: d, name: name("nope") }),
        ("rmdir 一个文件", MetaOp::Rmdir { parent: d, name: name("f") }),
    ] {
        assert_eq!(ns.op(&u2, op), Err(ErrCode::EACCES), "{what}");
    }
    assert!(ns.inode(f).is_some_and(|i| i.nlink == 1), "什么也没删");
}

fn unset() -> SetAttrIn {
    SetAttrIn { mode: None, uid: None, gid: None, size: None, atime: None, mtime: None, epoch_seen: 0 }
}

#[test]
fn changelog_records_successful_modifications_only() {
    let mut ns = Ns::new();
    let a = ns.mkdir(&OWNER, ROOT_INO, "a").unwrap();
    assert_eq!(ns.mkdir(&OWNER, ROOT_INO, "a"), Err(ErrCode::EEXIST));
    let f = ns.create(&OWNER, a, "f").unwrap();
    ns.op(&OWNER, MetaOp::Unlink { parent: a, name: name("f") }).unwrap();
    let recs: Vec<(u64, ChangeRecV1)> = ns
        .tag(keys::T_CHANGELOG)
        .iter()
        .map(|(k, v)| (u64::from_be_bytes(k[1..9].try_into().unwrap()), decode_v1(v).unwrap()))
        .collect();
    let ops: Vec<(u64, &str, u64)> =
        recs.iter().map(|(seq, r)| (*seq, wmfs_core::api::ops::op_name(r.op).unwrap(), r.ino)).collect();
    assert_eq!(ops, vec![(1, "Mkdir", a), (2, "Create", f), (3, "Unlink", f)], "失败的 mkdir 不写；seq 从 1 起");
    let r = &recs[1].1;
    assert_eq!(
        (r.uid, r.gid, r.session, r.trace_id, r.job_id, r.parent, r.name.clone()),
        (1000, 1001, 7, 3, 9, a, name("f"))
    );
    assert_eq!(decode_v1::<u64>(&ns.kv.0[&keys::global(keys::G_NEXT_CHANGELOG)]).unwrap(), 4);
}

/// changelog 的一条记录里测试关心的部分：(操作名, ino, parent, 名字, new_parent, 新名字)。
type Event = (&'static str, u64, u64, Vec<u8>, u64, Vec<u8>);

/// changelog 里的全部记录。
fn changes(ns: &Ns) -> Vec<Event> {
    ns.tag(keys::T_CHANGELOG)
        .iter()
        .map(|(_, v)| decode_v1::<ChangeRecV1>(v).unwrap())
        .map(|r| (wmfs_core::api::ops::op_name(r.op).unwrap(), r.ino, r.parent, r.name, r.new_parent, r.new_name))
        .collect()
}

#[test]
fn every_modification_writes_its_changelog_record() {
    let mut ns = Ns::new();
    let d = ns.mkdir(&OWNER, ROOT_INO, "d").unwrap();
    let f = ns.create(&OWNER, d, "f").unwrap();
    let nod = MetaOp::Mknod { parent: d, name: name("p"), mode: 0o644, umask: 0, kind: KIND_FIFO, rdev: 0 };
    let MetaReply::Entry { attr: p } = ns.op(&OWNER, nod).unwrap() else { panic!() };
    let MetaReply::Entry { attr: s } =
        ns.op(&OWNER, MetaOp::Symlink { parent: d, name: name("s"), target: name("f") }).unwrap()
    else {
        panic!()
    };
    ns.op(&OWNER, MetaOp::Link { ino: f, new_parent: ROOT_INO, new_name: name("h") }).unwrap();
    let mv = MetaOp::Rename { parent: d, name: name("f"), new_parent: d, new_name: name("g"), flags: 0 };
    ns.op(&OWNER, mv).unwrap();
    ns.op(&OWNER, MetaOp::SetAttr { ino: f, set: SetAttrIn { mode: Some(0o600), ..unset() } }).unwrap();
    let x = MetaOp::SetXattr { ino: f, name: name("user.k"), value: name("v"), flags: 0 };
    ns.op(&OWNER, x).unwrap();
    ns.op(&OWNER, MetaOp::RemoveXattr { ino: f, name: name("user.k") }).unwrap();
    ns.op(&OWNER, MetaOp::Unlink { parent: d, name: name("p") }).unwrap();
    let e = ns.mkdir(&OWNER, d, "e").unwrap();
    ns.op(&OWNER, MetaOp::Rmdir { parent: d, name: name("e") }).unwrap();
    let none = Vec::new();
    assert_eq!(
        changes(&ns),
        vec![
            ("Mkdir", d, ROOT_INO, name("d"), 0, none.clone()),
            ("Create", f, d, name("f"), 0, none.clone()),
            ("Mknod", p.ino, d, name("p"), 0, none.clone()),
            ("Symlink", s.ino, d, name("s"), 0, none.clone()),
            ("Link", f, ROOT_INO, name("h"), 0, none.clone()),
            ("Rename", f, d, name("f"), d, name("g")),
            ("SetAttr", f, 0, none.clone(), 0, none.clone()),
            ("SetXattr", f, 0, name("user.k"), 0, none.clone()),
            ("RemoveXattr", f, 0, name("user.k"), 0, none.clone()),
            ("Unlink", p.ino, d, name("p"), 0, none.clone()),
            ("Mkdir", e, d, name("e"), 0, none.clone()),
            ("Rmdir", e, d, name("e"), 0, none.clone()),
        ],
        "每个成功的修改都按顺序写一条（操作名、ino、parent、名字）"
    );
}
