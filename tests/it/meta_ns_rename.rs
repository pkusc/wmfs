//! rename 的全部规则（design.md 3.1.2）：改名、跨目录移动（含目录：两个父目录的 nlink 与它的 parent 指针）、覆盖文件
//! （旧目标变孤儿）与空目录、类型不匹配、非空目标、RENAME_NOREPLACE / RENAME_EXCHANGE / RENAME_WHITEOUT、不得把目录移进
//! 自己的子树、同一个文件的两个链接、硬链接的改名、粘滞位、换父目录要对被移动的目录有写权限、没有执行权限的目录里
//! 看不出有哪些名字、changelog。
//! rename 对并发读者的原子可见性在 meta_ns_engine.rs（真的经过 raft 与读事务）。

use wmfs_core::base::ErrCode;
use wmfs_core::meta::schema::keys::{self, ROOT_INO};
use wmfs_core::meta::schema::records::{ChangeRecV1, F_ORPHAN, decode_v1};
use wmfs_core::proto::meta::{MetaOp, MetaReply, RENAME_EXCHANGE, RENAME_NOREPLACE, RENAME_WHITEOUT};
use wmfs_testkit::harness::ns::{Ns, OWNER, user};

fn mv(p: u64, n: &str, np: u64, nn: &str, flags: u32) -> MetaOp {
    MetaOp::Rename { parent: p, name: n.as_bytes().to_vec(), new_parent: np, new_name: nn.as_bytes().to_vec(), flags }
}

#[test]
fn rename_within_and_across_directories() {
    let mut ns = Ns::new();
    let (a, b) = (ns.mkdir(&OWNER, ROOT_INO, "a").unwrap(), ns.mkdir(&OWNER, ROOT_INO, "b").unwrap());
    let f = ns.create(&OWNER, a, "f").unwrap();
    ns.op(&OWNER, mv(a, "f", a, "g", 0)).unwrap();
    assert_eq!((ns.ls(a), ns.inode(f).unwrap().name), (vec!["g".to_string()], b"g".to_vec()));
    let ctime = ns.attr(f).ctime;
    ns.op(&OWNER, mv(a, "g", b, "h", 0)).unwrap();
    assert_eq!((ns.ls(a).len(), ns.ls(b)), (0, vec!["h".to_string()]));
    let i = ns.inode(f).unwrap();
    assert_eq!((i.parent, i.name.clone()), (b, b"h".to_vec()));
    assert!(ns.attr(f).ctime > ctime, "rename 更新被移动对象的 ctime");
    let d = ns.mkdir(&OWNER, a, "d").unwrap();
    let (na, nb) = (ns.attr(a).nlink, ns.attr(b).nlink);
    ns.op(&OWNER, mv(a, "d", b, "d", 0)).unwrap();
    assert_eq!((ns.attr(a).nlink, ns.attr(b).nlink), (na - 1, nb + 1), "目录的 .. 跟着走");
    assert_eq!(ns.inode(d).unwrap().parent, b);
    assert_eq!((ns.attr(a).size, ns.attr(b).size), (0, 2));
    assert_eq!(ns.op(&OWNER, mv(a, "nope", b, "x", 0)), Err(ErrCode::ENOENT));
    assert_eq!(ns.op(&OWNER, mv(a, "x", f, "x", 0)), Err(ErrCode::ENOTDIR), "目标父不是目录");
}

#[test]
fn overwriting_targets() {
    let mut ns = Ns::new();
    let a = ns.mkdir(&OWNER, ROOT_INO, "a").unwrap();
    let (f, g) = (ns.create(&OWNER, a, "f").unwrap(), ns.create(&OWNER, a, "g").unwrap());
    ns.op(&OWNER, mv(a, "f", a, "g", 0)).unwrap();
    assert_eq!(ns.ls(a), vec!["g"]);
    assert_eq!(ns.attr(a).size, 1);
    let old = ns.inode(g).unwrap();
    assert_eq!((old.nlink, old.flags & F_ORPHAN), (0, F_ORPHAN), "被覆盖的文件：最后一个链接没了 → 孤儿");
    assert_eq!(ns.ino("/a/g"), Ok(f));
    let (d1, d2) = (ns.mkdir(&OWNER, a, "d1").unwrap(), ns.mkdir(&OWNER, a, "d2").unwrap());
    let nlink = ns.attr(a).nlink;
    ns.op(&OWNER, mv(a, "d1", a, "d2", 0)).unwrap();
    assert!(ns.inode(d2).is_none(), "被覆盖的空目录直接删掉");
    assert_eq!((ns.attr(a).nlink, ns.ino("/a/d2")), (nlink - 1, Ok(d1)));
    ns.create(&OWNER, d1, "x").unwrap();
    let e = ns.mkdir(&OWNER, a, "e").unwrap();
    assert_eq!(ns.op(&OWNER, mv(a, "e", a, "d2", 0)), Err(ErrCode::ENOTEMPTY), "目标目录必须为空");
    assert_eq!(ns.op(&OWNER, mv(a, "e", a, "g", 0)), Err(ErrCode::ENOTDIR), "目录不能覆盖文件");
    assert_eq!(ns.op(&OWNER, mv(a, "g", a, "e", 0)), Err(ErrCode::EISDIR), "文件不能覆盖目录");
    assert_eq!(ns.inode(e).unwrap().parent, a);
}

#[test]
fn flags_noreplace_exchange_and_whiteout() {
    let mut ns = Ns::new();
    let (a, b) = (ns.mkdir(&OWNER, ROOT_INO, "a").unwrap(), ns.mkdir(&OWNER, ROOT_INO, "b").unwrap());
    let f = ns.create(&OWNER, a, "f").unwrap();
    let d = ns.mkdir(&OWNER, b, "d").unwrap();
    assert_eq!(ns.op(&OWNER, mv(a, "f", b, "d", RENAME_NOREPLACE)), Err(ErrCode::EEXIST));
    assert!(ns.op(&OWNER, mv(a, "f", a, "f2", RENAME_NOREPLACE)).is_ok(), "目标不存在时照常改名");
    assert_eq!(ns.op(&OWNER, mv(a, "f2", b, "none", RENAME_EXCHANGE)), Err(ErrCode::ENOENT), "交换要求两边都在");
    assert_eq!(ns.op(&OWNER, mv(a, "f2", b, "d", RENAME_WHITEOUT)), Err(ErrCode::EINVAL), "不支持 WHITEOUT");
    assert_eq!(ns.op(&OWNER, mv(a, "f2", b, "d", RENAME_NOREPLACE | RENAME_EXCHANGE)), Err(ErrCode::EINVAL));
    let (na, nb) = (ns.attr(a).nlink, ns.attr(b).nlink);
    ns.op(&OWNER, mv(a, "f2", b, "d", RENAME_EXCHANGE)).unwrap();
    assert_eq!((ns.ino("/a/f2"), ns.ino("/b/d")), (Ok(d), Ok(f)), "两个目录项对调");
    assert_eq!((ns.attr(a).nlink, ns.attr(b).nlink), (na + 1, nb - 1), "目录换了父目录：两边的 nlink 跟着变");
    assert_eq!((ns.inode(d).unwrap().parent, ns.inode(f).unwrap().parent), (a, b));
    assert_eq!((ns.attr(a).size, ns.attr(b).size), (1, 1), "交换不改条目数");
}

#[test]
fn directories_cannot_move_into_their_own_subtree() {
    let mut ns = Ns::new();
    let a = ns.mkdir(&OWNER, ROOT_INO, "a").unwrap();
    let b = ns.mkdir(&OWNER, a, "b").unwrap();
    let c = ns.mkdir(&OWNER, b, "c").unwrap();
    assert_eq!(ns.op(&OWNER, mv(ROOT_INO, "a", c, "a", 0)), Err(ErrCode::EINVAL));
    assert_eq!(ns.op(&OWNER, mv(ROOT_INO, "a", a, "x", 0)), Err(ErrCode::EINVAL), "移进自己");
    assert_eq!(ns.op(&OWNER, mv(a, "b", ROOT_INO, "a", 0)), Err(ErrCode::ENOTEMPTY), "目标是自己的祖先（非空）");
    let z = ns.mkdir(&OWNER, ROOT_INO, "z").unwrap();
    assert_eq!(ns.op(&OWNER, mv(ROOT_INO, "z", c, "z2", RENAME_EXCHANGE)), Err(ErrCode::ENOENT));
    ns.mkdir(&OWNER, c, "w").unwrap();
    assert_eq!(ns.op(&OWNER, mv(ROOT_INO, "a", c, "w", RENAME_EXCHANGE)), Err(ErrCode::EINVAL), "交换也不行");
    assert_eq!(ns.inode(z).unwrap().parent, ROOT_INO);
}

#[test]
fn hard_links_and_rename() {
    let mut ns = Ns::new();
    let (a, b) = (ns.mkdir(&OWNER, ROOT_INO, "a").unwrap(), ns.mkdir(&OWNER, ROOT_INO, "b").unwrap());
    let f = ns.create(&OWNER, a, "f").unwrap();
    ns.op(&OWNER, MetaOp::Link { ino: f, new_parent: b, new_name: b"l".to_vec() }).unwrap();
    ns.op(&OWNER, mv(a, "f", b, "l", 0)).unwrap();
    assert_eq!(
        (ns.ls(a), ns.ls(b)),
        (vec!["f".to_string()], vec!["l".to_string()]),
        "同一个文件的两个链接：什么也不做"
    );
    assert_eq!(ns.attr(f).nlink, 2);
    ns.op(&OWNER, mv(b, "l", b, "l2", 0)).unwrap();
    let i = ns.inode(f).unwrap();
    assert_eq!(
        (i.parent, i.name.clone(), i.extra_links.clone()),
        (a, b"f".to_vec(), vec![(b, b"l2".to_vec())]),
        "改的是额外链接"
    );
}

#[test]
fn sticky_bit_and_write_permission_on_a_moved_directory() {
    let mut ns = Ns::new();
    let (u2, u3) = (user(2000, 2000), user(3000, 3000));
    let scratch = 17;
    ns.create(&u3, scratch, "theirs").unwrap();
    assert_eq!(ns.op(&u2, mv(scratch, "theirs", scratch, "mine", 0)), Err(ErrCode::EPERM), "粘滞目录里不能动别人的");
    ns.create(&u2, scratch, "mine").unwrap();
    assert_eq!(ns.op(&u2, mv(scratch, "mine", scratch, "theirs", 0)), Err(ErrCode::EPERM), "也不能覆盖别人的");
    let op = MetaOp::Mkdir { parent: scratch, name: b"ro".to_vec(), mode: 0o555, umask: 0 };
    ns.op(&u2, op).unwrap();
    let home = 16;
    assert_eq!(ns.op(&u2, mv(scratch, "ro", home, "ro", 0)), Err(ErrCode::EACCES), "要改它的 ..：对它自己要有写权限");
    assert!(ns.op(&u2, mv(scratch, "ro", scratch, "ro2", 0)).is_ok(), "同一个父目录里改名不需要");
}

#[test]
fn names_stay_hidden_in_directories_without_search_permission() {
    let mut ns = Ns::new();
    let u2 = user(2000, 2000);
    let op = MetaOp::Mkdir { parent: ROOT_INO, name: b"p".to_vec(), mode: 0o730, umask: 0 }; // 其他人什么都没有
    let MetaReply::Entry { attr } = ns.op(&OWNER, op).unwrap() else { panic!() };
    let p = attr.ino;
    ns.create(&OWNER, p, "f").unwrap();
    let scratch = 17;
    ns.create(&u2, scratch, "x").unwrap();
    for (what, op) in [
        ("源目录没有 x：已有的名字", mv(p, "f", scratch, "y", 0)),
        ("源目录没有 x：不存在的名字（不是 ENOENT）", mv(p, "nope", scratch, "y", 0)),
        ("目标目录没有 x", mv(scratch, "x", p, "f", 0)),
        ("目标目录没有 x：NOREPLACE（不是 EEXIST）", mv(scratch, "x", p, "f", RENAME_NOREPLACE)),
        ("目标目录没有 x：EXCHANGE", mv(scratch, "x", p, "f", RENAME_EXCHANGE)),
    ] {
        assert_eq!(ns.op(&u2, op), Err(ErrCode::EACCES), "{what}");
    }
    assert_eq!(ns.ls(scratch), vec!["x".to_string()], "什么也没动");
}

#[test]
fn rename_writes_one_changelog_record() {
    let mut ns = Ns::new();
    let (a, b) = (ns.mkdir(&OWNER, ROOT_INO, "a").unwrap(), ns.mkdir(&OWNER, ROOT_INO, "b").unwrap());
    let f = ns.create(&OWNER, a, "f").unwrap();
    ns.op(&OWNER, mv(a, "f", b, "g", 0)).unwrap();
    let (_, last) = ns.tag(keys::T_CHANGELOG).pop().unwrap();
    let r: ChangeRecV1 = decode_v1(&last).unwrap();
    assert_eq!(wmfs_core::api::ops::op_name(r.op), Some("Rename"));
    assert_eq!((r.ino, r.parent, r.name, r.new_parent, r.new_name), (f, a, b"f".to_vec(), b, b"g".to_vec()));
}
