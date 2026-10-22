//! setattr 与 xattr（design.md 3.5.1、3.4.2.9、3.1.1）：chmod/chown/时间的属主规则、setuid/setgid 的清除、截断到 s > 0
//! （inline 文件截 INLINE、epoch 不符 → STALE_LAYOUT）与**截断到 0 按当前属性重建 layout**（新 epoch、按当前的存储类重新选链、
//! 旧 epoch 写 GC 记录）、xattr 的名字空间与大小限制、POSIX ACL（权限位同步、F_HAS_ACL、默认 ACL 的继承与 umask 规则、
//! M10 之前带扩展 ACL 的 inode 只有 root 能访问）、inline 文件的内容只给有读权限的人。

use wmfs_core::base::ErrCode;
use wmfs_core::base::types::Ts;
use wmfs_core::meta::ns::acl::{self, Ace, GROUP_OBJ, MASK, OTHER, USER, USER_OBJ};
use wmfs_core::meta::schema::keys::{self, GC_EPOCH, ROOT_INO};
use wmfs_core::meta::schema::records::{
    ChainRecV1, F_HAS_ACL, F_INLINE, GcRecV1, StorageClassV1, decode_v1, encode_v1,
};
use wmfs_core::proto::meta::{MetaOp, MetaReply, SetAttrIn, TimeSet, XATTR_CREATE, XATTR_REPLACE};
use wmfs_testkit::harness::ns::{Ns, OWNER, ROOT, Who, user};

fn set() -> SetAttrIn {
    SetAttrIn { mode: None, uid: None, gid: None, size: None, atime: None, mtime: None, epoch_seen: 0 }
}

fn sa(ns: &mut Ns, who: &Who, ino: u64, s: SetAttrIn) -> Result<MetaReply, ErrCode> {
    ns.op(who, MetaOp::SetAttr { ino, set: s })
}

#[test]
fn chmod_chown_and_times_follow_the_owner_rules() {
    let mut ns = Ns::new();
    let f = ns.create(&OWNER, ROOT_INO, "f").unwrap();
    let other = user(2000, 2000);
    assert_eq!(sa(&mut ns, &other, f, SetAttrIn { mode: Some(0o600), ..set() }), Err(ErrCode::EPERM));
    assert!(matches!(sa(&mut ns, &OWNER, f, SetAttrIn { mode: Some(0o4755), ..set() }), Ok(MetaReply::Entry { .. })));
    assert_eq!(ns.attr(f).mode, 0o4755);
    let me_in_500 = Who { groups: vec![500], ..OWNER.clone() };
    assert_eq!(sa(&mut ns, &OWNER, f, SetAttrIn { gid: Some(777), ..set() }), Err(ErrCode::EPERM), "不在组 777 里");
    sa(&mut ns, &me_in_500, f, SetAttrIn { gid: Some(500), ..set() }).unwrap();
    let a = ns.attr(f);
    assert_eq!((a.gid, a.mode), (500, 0o755), "属主改到自己所在的组；chown 去掉 setuid");
    assert_eq!(
        sa(&mut ns, &OWNER, f, SetAttrIn { uid: Some(3000), ..set() }),
        Err(ErrCode::EPERM),
        "只有 root 能改 uid"
    );
    sa(&mut ns, &ROOT, f, SetAttrIn { uid: Some(3000), ..set() }).unwrap();
    assert_eq!(ns.attr(f).uid, 3000);
    sa(&mut ns, &ROOT, f, SetAttrIn { mode: Some(0o2775), ..set() }).unwrap();
    sa(&mut ns, &user(3000, 9), f, SetAttrIn { mode: Some(0o2775), ..set() }).unwrap();
    assert_eq!(ns.attr(f).mode, 0o775, "不在文件所属组里的非 root 设不了 setgid（静默去掉）");

    let g = ns.create(&OWNER, 17, "g").unwrap(); // /scratch 里 0644
    let at = |secs| Some(TimeSet::At(Ts { secs, nanos: 0 }));
    assert_eq!(sa(&mut ns, &other, g, SetAttrIn { mtime: at(5), ..set() }), Err(ErrCode::EPERM), "指定时间限属主");
    assert_eq!(sa(&mut ns, &other, g, SetAttrIn { mtime: Some(TimeSet::Now), ..set() }), Err(ErrCode::EACCES));
    sa(&mut ns, &OWNER, g, SetAttrIn { mode: Some(0o666), ..set() }).unwrap();
    let before = ns.attr(g);
    sa(&mut ns, &other, g, SetAttrIn { atime: Some(TimeSet::Now), mtime: Some(TimeSet::Now), ..set() }).unwrap();
    let after = ns.attr(g);
    assert!(after.mtime > before.mtime && after.ctime > before.ctime, "有写权限就能 touch");
    sa(&mut ns, &OWNER, g, SetAttrIn { mtime: at(5), ..set() }).unwrap();
    assert_eq!(ns.attr(g).mtime, Ts { secs: 5, nanos: 0 });
    assert!(ns.attr(g).change_id > after.change_id);
}

#[test]
fn truncate_to_a_size_keeps_the_layout_and_trims_inline_data() {
    let mut ns = Ns::new();
    let f = ns.create(&OWNER, ROOT_INO, "f").unwrap();
    ns.kv.0.insert(keys::inline(f), b"0123456789".to_vec());
    let r = sa(&mut ns, &OWNER, f, SetAttrIn { size: Some(4), ..set() }).unwrap();
    let MetaReply::Opened(o) = r else { panic!("改了 size 回 Opened：{r:?}") };
    assert_eq!((o.attr.size, o.attr.epoch, o.inline), (4, 1, Some(b"0123".to_vec())), "len(INLINE) ≤ size");
    sa(&mut ns, &OWNER, f, SetAttrIn { size: Some(100), ..set() }).unwrap();
    assert_eq!(ns.kv.0[&keys::inline(f)], b"0123".to_vec(), "扩回来也只会读到 0（INLINE 不变）");
    let mut i = ns.inode(f).unwrap();
    i.flags &= !F_INLINE; // 装作已经转成 chunk 存储（M5）
    ns.kv.0.insert(keys::inode(f), encode_v1(&i));
    let stale = SetAttrIn { size: Some(10), epoch_seen: 9, ..set() };
    assert_eq!(sa(&mut ns, &OWNER, f, stale), Err(ErrCode::STALE_LAYOUT), "客户端截断的是别的 epoch");
    sa(&mut ns, &OWNER, f, SetAttrIn { size: Some(10), epoch_seen: 1, ..set() }).unwrap();
    assert_eq!(ns.attr(f).size, 10);
    assert_eq!(sa(&mut ns, &OWNER, ROOT_INO, SetAttrIn { size: Some(0), ..set() }), Err(ErrCode::EISDIR));
    assert_eq!(sa(&mut ns, &user(2000, 2000), f, SetAttrIn { size: Some(1), ..set() }), Err(ErrCode::EACCES));
}

#[test]
fn truncate_to_zero_rebuilds_the_layout_from_the_current_attributes() {
    let mut ns = Ns::new();
    let f = ns.create(&OWNER, ROOT_INO, "f").unwrap();
    let mut i = ns.inode(f).unwrap();
    i.flags &= !F_INLINE; // 已经有 chunk 数据的文件
    i.size = 123;
    ns.kv.0.insert(keys::inode(f), encode_v1(&i));
    ns.kv.0.insert(keys::inline(f), b"stale".to_vec());
    // "当前属性"变了：default 类的副本数从 3 改成 1（M13 的 class update；这里直接改记录）
    let mut class: StorageClassV1 = decode_v1(&ns.kv.0[&keys::class(1)]).unwrap();
    class.replica = 1;
    ns.kv.0.insert(keys::class(1), encode_v1(&class));
    let MetaReply::Opened(o) = sa(&mut ns, &OWNER, f, SetAttrIn { size: Some(0), ..set() }).unwrap() else { panic!() };
    let l = o.layout.unwrap();
    assert_eq!((o.attr.size, l.epoch, l.replicas, l.chains.clone()), (0, 2, 1, vec![2]), "新 epoch、按新的副本数选链");
    let chain: ChainRecV1 = decode_v1(&ns.kv.0[&keys::chain(2)]).unwrap();
    assert_eq!(chain.replicas, 1, "(default, 1) 还没有链：生成了一条");
    let i = ns.inode(f).unwrap();
    assert_eq!((i.max_epoch, i.class_id, i.flags & F_INLINE), (2, 1, F_INLINE), "F_INLINE 按新 layout 重新判定");
    assert!(!ns.kv.0.contains_key(&keys::inline(f)), "INLINE 清空");
    let gc: GcRecV1 = decode_v1(&ns.kv.0[&keys::gc(GC_EPOCH, f, 1)]).unwrap();
    assert_eq!((gc.chains, gc.due_ms - gc.created_ms), (vec![1], 60_000), "旧 epoch 写 GC 记录，宽限 epoch_gc_grace");
    sa(&mut ns, &OWNER, f, SetAttrIn { size: Some(0), ..set() }).unwrap();
    assert!(!ns.kv.0.contains_key(&keys::gc(GC_EPOCH, f, 2)), "旧 inode 是 inline 的：没有 chunk，不写 GC 记录");
    assert_eq!(ns.inode(f).unwrap().layout.unwrap().epoch, 3);
}

fn sx(ns: &mut Ns, who: &Who, ino: u64, name: &str, value: &[u8], flags: u32) -> Result<MetaReply, ErrCode> {
    ns.op(who, MetaOp::SetXattr { ino, name: name.into(), value: value.to_vec(), flags })
}

fn gx(ns: &mut Ns, who: &Who, ino: u64, name: &str) -> Result<Vec<u8>, ErrCode> {
    match ns.read(who, MetaOp::GetXattr { ino, name: name.into() })? {
        MetaReply::Xattr { value } => Ok(value),
        other => panic!("GetXattr 的回复不对：{other:?}"),
    }
}

#[test]
fn xattr_namespaces_flags_and_limits() {
    let mut ns = Ns::new();
    let f = ns.create(&OWNER, ROOT_INO, "f").unwrap(); // 0644
    let other = user(2000, 2000);
    sx(&mut ns, &OWNER, f, "user.k", b"v1", 0).unwrap();
    assert_eq!(gx(&mut ns, &other, f, "user.k"), Ok(b"v1".to_vec()), "有读权限就能读 user.*");
    assert_eq!(sx(&mut ns, &other, f, "user.k", b"x", 0), Err(ErrCode::EACCES), "没有写权限");
    assert_eq!(sx(&mut ns, &OWNER, f, "user.k", b"v2", XATTR_CREATE), Err(ErrCode::EEXIST));
    assert_eq!(sx(&mut ns, &OWNER, f, "user.new", b"v", XATTR_REPLACE), Err(ErrCode::ENODATA));
    sx(&mut ns, &OWNER, f, "user.k", b"v2", XATTR_REPLACE).unwrap();
    assert_eq!(gx(&mut ns, &OWNER, f, "user.k"), Ok(b"v2".to_vec()));
    assert_eq!(gx(&mut ns, &OWNER, f, "user.none"), Err(ErrCode::ENODATA));
    assert_eq!(sx(&mut ns, &OWNER, f, "trusted.t", b"v", 0), Err(ErrCode::EPERM), "trusted.* 只给 root");
    sx(&mut ns, &ROOT, f, "trusted.t", b"v", 0).unwrap();
    assert_eq!(gx(&mut ns, &OWNER, f, "trusted.t"), Err(ErrCode::ENODATA), "非 root 看不见 trusted.*");
    assert_eq!(sx(&mut ns, &OWNER, f, "security.s", b"v", 0), Err(ErrCode::EPERM), "security.* 的写只给 root");
    sx(&mut ns, &ROOT, f, "security.s", b"v", 0).unwrap();
    assert_eq!(gx(&mut ns, &other, f, "security.s"), Ok(b"v".to_vec()));
    let names = |ns: &mut Ns, who: &Who| match ns.read(who, MetaOp::ListXattr { ino: f }).unwrap() {
        MetaReply::XattrNames { names } => names.into_iter().map(|n| String::from_utf8(n).unwrap()).collect::<Vec<_>>(),
        other => panic!("{other:?}"),
    };
    assert_eq!(names(&mut ns, &OWNER), vec!["security.s", "user.k"]);
    assert_eq!(names(&mut ns, &ROOT), vec!["security.s", "trusted.t", "user.k"]);
    for (n, code) in
        [("foo.bar", ErrCode::EOPNOTSUPP), ("user.", ErrCode::EOPNOTSUPP), ("system.other", ErrCode::EOPNOTSUPP)]
    {
        assert_eq!(sx(&mut ns, &OWNER, f, n, b"v", 0), Err(code), "{n}");
    }
    assert_eq!(sx(&mut ns, &OWNER, f, &format!("user.{}", "n".repeat(251)), b"v", 0), Err(ErrCode::ERANGE));
    assert_eq!(sx(&mut ns, &OWNER, f, "user.big", &vec![0; 65537], 0), Err(ErrCode::E2BIG));
    sx(&mut ns, &OWNER, f, "user.a", &vec![1; 40000], 0).unwrap();
    assert_eq!(sx(&mut ns, &OWNER, f, "user.b", &vec![1; 30000], 0), Err(ErrCode::ENOSPC), "一个 inode 合计 ≤ 64 KiB");
    let rm = |n: &str| MetaOp::RemoveXattr { ino: f, name: n.into() };
    ns.op(&OWNER, rm("user.k")).unwrap();
    assert_eq!(ns.op(&OWNER, rm("user.k")), Err(ErrCode::ENODATA));
    let l = ns.op(&OWNER, MetaOp::Symlink { parent: ROOT_INO, name: b"l".to_vec(), target: b"f".to_vec() }).unwrap();
    let MetaReply::Entry { attr } = l else { panic!() };
    assert_eq!(sx(&mut ns, &OWNER, attr.ino, "user.k", b"v", 0), Err(ErrCode::EPERM), "user.* 只能挂在文件与目录上");
}

fn ace(tag: u16, perm: u16, id: u32) -> Ace {
    Ace { tag, perm, id }
}

const ANY: u32 = u32::MAX;

#[test]
fn access_acls_sync_the_mode_bits() {
    let mut ns = Ns::new();
    let f = ns.create(&OWNER, ROOT_INO, "f").unwrap();
    let minimal = acl::encode(&[ace(USER_OBJ, 6, ANY), ace(GROUP_OBJ, 4, ANY), ace(OTHER, 0, ANY)]);
    sx(&mut ns, &OWNER, f, "system.posix_acl_access", &minimal, 0).unwrap();
    assert_eq!(ns.attr(f).mode, 0o640, "与权限位等价的 ACL：只改权限位");
    assert_eq!(gx(&mut ns, &OWNER, f, "system.posix_acl_access"), Err(ErrCode::ENODATA), "不存它");
    let ext = acl::encode(&[
        ace(USER_OBJ, 6, ANY),
        ace(USER, 7, 2000),
        ace(GROUP_OBJ, 4, ANY),
        ace(MASK, 6, ANY),
        ace(OTHER, 0, ANY),
    ]);
    assert_eq!(
        sx(&mut ns, &user(2000, 2000), f, "system.posix_acl_access", &ext, 0),
        Err(ErrCode::EPERM),
        "只有属主或 root"
    );
    sx(&mut ns, &OWNER, f, "system.posix_acl_access", &ext, 0).unwrap();
    let a = ns.attr(f);
    assert_eq!((a.mode, a.flags & F_HAS_ACL), (0o660, F_HAS_ACL), "组位来自 MASK");
    ns.op(&ROOT, MetaOp::SetAttr { ino: f, set: SetAttrIn { mode: Some(0o604), ..set() } }).unwrap();
    let got = acl::parse(&gx(&mut ns, &ROOT, f, "system.posix_acl_access").unwrap()).unwrap();
    assert_eq!((got[3], got[0].perm, got[4].perm), (ace(MASK, 0, ANY), 6, 4), "chmod 同步 USER_OBJ / MASK / OTHER");
    let bad = acl::encode(&[ace(USER_OBJ, 6, ANY), ace(USER, 7, 2000), ace(GROUP_OBJ, 4, ANY), ace(OTHER, 0, ANY)]);
    assert_eq!(
        sx(&mut ns, &ROOT, f, "system.posix_acl_access", &bad, 0),
        Err(ErrCode::EINVAL),
        "有命名用户就得有 MASK"
    );
    ns.op(&ROOT, MetaOp::RemoveXattr { ino: f, name: b"system.posix_acl_access".to_vec() }).unwrap();
    assert_eq!(ns.attr(f).flags & F_HAS_ACL, 0);
}

#[test]
fn only_root_gets_past_an_extended_acl_before_m10() {
    let mut ns = Ns::new();
    let d = ns.mkdir(&OWNER, ROOT_INO, "d").unwrap();
    ns.create(&OWNER, d, "f").unwrap();
    let ext = acl::encode(&[
        ace(USER_OBJ, 7, ANY),
        ace(USER, 7, 2000),
        ace(GROUP_OBJ, 5, ANY),
        ace(MASK, 7, ANY),
        ace(OTHER, 5, ANY),
    ]);
    sx(&mut ns, &OWNER, d, "system.posix_acl_access", &ext, 0).unwrap();
    assert_ne!(ns.attr(d).flags & F_HAS_ACL, 0);
    let lookup = MetaOp::Lookup { parent: d, name: b"f".to_vec() };
    let readdir = MetaOp::ReadDir { ino: d, after: vec![], limit: 10, plus: false };
    assert!(ns.read(&ROOT, lookup.clone()).is_ok(), "root 不受 ACL 限制");
    assert!(ns.read(&ROOT, readdir.clone()).is_ok());
    assert!(ns.create(&ROOT, d, "byroot").is_ok());
    assert!(ns.read(&ROOT, MetaOp::ResolvePath { path: b"/d/f".to_vec() }).is_ok());
    for who in [OWNER, user(2000, 2000)] {
        assert_eq!(ns.read(&who, lookup.clone()), Err(ErrCode::EACCES), "posix-acl 在 M10：{who:?} 还过不去");
        assert_eq!(ns.read(&who, readdir.clone()), Err(ErrCode::EACCES));
        assert_eq!(ns.create(&who, d, "x"), Err(ErrCode::EACCES));
    }
}

#[test]
fn inline_content_goes_only_to_readers() {
    let mut ns = Ns::new();
    let op = MetaOp::Create { parent: ROOT_INO, name: b"f".to_vec(), mode: 0o622, umask: 0, excl: true };
    let MetaReply::Opened(o) = ns.op(&OWNER, op).unwrap() else { panic!() };
    let f = o.attr.ino;
    ns.kv.0.insert(keys::inline(f), b"secret".to_vec());
    let mut i = ns.inode(f).unwrap();
    i.size = 6;
    ns.kv.0.insert(keys::inode(f), encode_v1(&i));
    let other = user(2000, 2000); // 0622：其他人只有写权限
    let inline = |r: Result<MetaReply, ErrCode>| match r {
        Ok(MetaReply::Opened(o)) => o.inline,
        other => panic!("应回 Opened：{other:?}"),
    };
    assert_eq!(ns.read(&other, MetaOp::Open { ino: f, flags: 0 }), Err(ErrCode::EACCES), "没有读权限");
    assert_eq!(inline(ns.read(&other, MetaOp::Open { ino: f, flags: 1 })), None, "只写打开：不带内容");
    let again = MetaOp::Create { parent: ROOT_INO, name: b"f".to_vec(), mode: 0o644, umask: 0, excl: false };
    assert_eq!(inline(ns.op(&other, again.clone())), None, "Create 打开已有的文件：没有读权限就不带");
    assert_eq!(inline(sa(&mut ns, &other, f, SetAttrIn { size: Some(6), ..set() })), None, "截断的回复也不带");
    assert_eq!(inline(ns.read(&OWNER, MetaOp::Open { ino: f, flags: 2 })), Some(b"secret".to_vec()), "读写打开");
    assert_eq!(inline(ns.op(&OWNER, again)), Some(b"secret".to_vec()), "属主有读权限");
}

#[test]
fn default_acls_are_inherited_and_replace_the_umask() {
    let mut ns = Ns::new();
    let d = ns.mkdir(&OWNER, ROOT_INO, "d").unwrap();
    let f0 = ns.create(&OWNER, ROOT_INO, "f0").unwrap();
    let def = acl::encode(&[ace(USER_OBJ, 7, ANY), ace(GROUP_OBJ, 5, ANY), ace(OTHER, 0, ANY)]);
    assert_eq!(
        sx(&mut ns, &OWNER, f0, "system.posix_acl_default", &def, 0),
        Err(ErrCode::EACCES),
        "默认 ACL 只能挂在目录上"
    );
    sx(&mut ns, &OWNER, d, "system.posix_acl_default", &def, 0).unwrap();
    let create = MetaOp::Create { parent: d, name: b"f".to_vec(), mode: 0o666, umask: 0o077, excl: true };
    let MetaReply::Opened(o) = ns.op(&OWNER, create).unwrap() else { panic!() };
    assert_eq!((o.attr.mode, o.attr.flags & F_HAS_ACL), (0o640, 0), "按 ACL 规则、不用 umask（用 umask 会是 0600）");
    let mkdir = MetaOp::Mkdir { parent: d, name: b"sub".to_vec(), mode: 0o777, umask: 0o077 };
    let MetaReply::Entry { attr } = ns.op(&OWNER, mkdir).unwrap() else { panic!() };
    assert_eq!(attr.mode, 0o750);
    assert_eq!(gx(&mut ns, &OWNER, attr.ino, "system.posix_acl_default"), Ok(def.clone()), "新目录继承默认 ACL");
    let named = acl::encode(&[
        ace(USER_OBJ, 7, ANY),
        ace(USER, 7, 2000),
        ace(GROUP_OBJ, 5, ANY),
        ace(MASK, 7, ANY),
        ace(OTHER, 0, ANY),
    ]);
    sx(&mut ns, &OWNER, d, "system.posix_acl_default", &named, 0).unwrap();
    let create = MetaOp::Create { parent: d, name: b"g".to_vec(), mode: 0o664, umask: 0o077, excl: true };
    let MetaReply::Opened(o) = ns.op(&OWNER, create).unwrap() else { panic!() };
    assert_eq!((o.attr.mode, o.attr.flags & F_HAS_ACL), (0o660, F_HAS_ACL), "带命名用户：新文件有访问 ACL");
    let got = acl::parse(&gx(&mut ns, &ROOT, o.attr.ino, "system.posix_acl_access").unwrap()).unwrap();
    assert_eq!(got[3], ace(MASK, 6, ANY), "MASK 与请求的组位求交");
    let empty = acl::encode(&[]);
    sx(&mut ns, &OWNER, d, "system.posix_acl_default", &empty, 0).unwrap();
    assert_eq!(gx(&mut ns, &OWNER, d, "system.posix_acl_default"), Err(ErrCode::ENODATA), "只有头 = 删除");
    let create = MetaOp::Create { parent: d, name: b"h".to_vec(), mode: 0o666, umask: 0o077, excl: true };
    let MetaReply::Opened(o) = ns.op(&OWNER, create).unwrap() else { panic!() };
    assert_eq!(o.attr.mode, 0o600, "没有默认 ACL 时用 umask");
}
