//! M12 的回收站、ttl 与审计（design.md 第 5 章"回收站细节"、3.9 的"移入/移出回收站"、3.8 变更通知 ⑥、3.7.7 的 AuditFilter），在内存 KV
//! 视图上逐条检查（`harness::ns`）：
//! - unlink → 移进 `trash` 来源目录下的 `.wmfs-trash/<文件属主的 uid>/`（两层的 mode 与属主、名字、`user.wmfs.origin`、到期时间与
//!   TRASH_INDEX、计数仍在来源子树里、changelog `Unlink` 带 `trash=`）；mv 出回收站 = 恢复；回收站里再删 = 真删；rename 覆盖掉的文件也进
//!   回收站；还有别的硬链接的不进；用回收站里的旧版本覆盖恢复之后回收站目录的 size 仍是条目数（M12 审查）；很长的 UTF-8 名字截断时不切在
//!   字符中间；别的用户看不到、读不到别人回收站里的文件（属主自己能 mv 回来），root 删 alice 的文件进 alice 的子目录，别人建的同名目录不用，
//!   建回收站的两层不检查配额（ADR-0043 第 1 条）；
//! - `trash::expire`（trash-expire 的一项）：到期的真删、过时的索引只删索引；`trash::ttl`：过期的删（`trash` 生效就进回收站）、没过期的不动；
//!   两个都回"真删了没有"；
//! - `audit`：changelog 的 audited 标记、`ops=`/`uids=` 过滤、被拒绝的修改也记一条（result = 错误码）、第一次启用时建 `audit-log` 游标；
//!   rename 也看目标目录；
//! - `consistency=strong` 目录里新建的文件不 inline。

use wmfs_core::base::{ErrCode, Failed};
use wmfs_core::meta::attr_ops;
use wmfs_core::meta::attr_registry::split;
use wmfs_core::meta::changelog::{self, AUDIT_CURSOR, Subject};
use wmfs_core::meta::ns::audit_of;
use wmfs_core::meta::ns::trash::{self, ORIGIN, TRASH_DIR};
use wmfs_core::meta::rstat;
use wmfs_core::meta::schema::keys::{self, T_CHANGELOG, T_TRASH_INDEX};
use wmfs_core::meta::schema::records::{ChangeRecV1, F_INLINE, F_ORPHAN, F_TRASH_DIR, decode_v1};
use wmfs_core::proto::meta::{MetaOp, MetaReply};
use wmfs_testkit::harness::ns::{Ns, OWNER, ROOT, user};

const HOME: u64 = 16;
const SCRATCH: u64 = 17;

fn set(ns: &mut Ns, path: &str, spec: &str) -> Result<Vec<String>, Failed> {
    let items = split(spec)?;
    ns.txn(&ROOT, |v, c| attr_ops::set(v, c, path, &items)).map(|r| r.warnings)
}

fn unlink(ns: &mut Ns, parent: u64, name: &str) -> Result<MetaReply, ErrCode> {
    ns.op(&OWNER, MetaOp::Unlink { parent, name: name.as_bytes().to_vec() })
}

fn rename(ns: &mut Ns, parent: u64, name: &str, to: u64, new: &str) -> Result<MetaReply, ErrCode> {
    let op = MetaOp::Rename {
        parent,
        name: name.as_bytes().to_vec(),
        new_parent: to,
        new_name: new.as_bytes().to_vec(),
        flags: 0,
    };
    ns.op(&OWNER, op)
}

fn events(ns: &Ns) -> Vec<ChangeRecV1> {
    ns.tag(T_CHANGELOG).iter().map(|(_, b)| decode_v1(b).unwrap()).collect()
}

fn extra<'a>(e: &'a ChangeRecV1, k: &str) -> Option<&'a str> {
    e.extra.iter().find(|(x, _)| x == k).map(|(_, v)| v.as_str())
}

/// 建一个长度为 `size` 的文件（只改元数据）。
fn file(ns: &mut Ns, parent: u64, name: &str, size: u64) -> u64 {
    let f = ns.create(&OWNER, parent, name).unwrap();
    ns.set_size(&OWNER, f, size).unwrap();
    f
}

#[test]
fn rm_moves_into_the_trash_and_mv_restores() {
    let mut ns = Ns::new();
    set(&mut ns, "/home", "trash=7d").unwrap();
    let a = ns.mkdir(&OWNER, HOME, "a").unwrap();
    let f = file(&mut ns, a, "doc", 1000);
    let usage = |ns: &mut Ns, d| ns.view(&ROOT, |v, _| rstat::get(v, d).unwrap());
    let (home0, a0) = (usage(&mut ns, HOME), usage(&mut ns, a));
    unlink(&mut ns, a, "doc").unwrap();
    let top = ns.ino("/home/.wmfs-trash").unwrap();
    let tr = ns.inode(top).unwrap();
    assert!(tr.flags & F_TRASH_DIR != 0 && tr.mode == 0o711, "回收站：0711（列不出有哪些用户）、标 F_TRASH_DIR");
    assert_eq!((tr.uid, tr.gid), (OWNER.uid, OWNER.gid), "属主同 trash 的来源目录（/home 是 init 时的属主）");
    let t = ns.ino("/home/.wmfs-trash/1000").unwrap();
    let ur = ns.inode(t).unwrap();
    assert!(ur.flags & F_TRASH_DIR != 0 && ur.mode == 0o700, "文件属主的子目录：0700、也标 F_TRASH_DIR");
    assert_eq!((ur.uid, ur.gid), (OWNER.uid, OWNER.gid), "属主 = 文件的属主，属组 = 文件的属组");
    let names = ns.ls(t);
    let i = ns.inode(f).unwrap();
    assert_eq!(names, vec![format!("{}-{f}-doc", i.ctime.secs)], "名字 <unix秒>-<ino>-<原名>");
    assert_eq!((i.parent, i.nlink), (t, 1), "移进去，不是删掉");
    assert_eq!(i.trash_expire_ms, i.ctime.secs as u64 * 1000 + 7 * 86_400_000, "到期 = now + 保留期");
    assert!(ns.kv.0.contains_key(&keys::trash_index(i.trash_expire_ms, f)), "TRASH_INDEX");
    let origin = ns.read(&OWNER, MetaOp::GetXattr { ino: f, name: ORIGIN.to_vec() }).unwrap();
    assert_eq!(origin, MetaReply::Xattr { value: b"/home/a/doc".to_vec() }, "user.wmfs.origin = 原路径");
    let (home1, a1) = (usage(&mut ns, HOME), usage(&mut ns, a));
    assert_eq!((home1.bytes, home1.files), (home0.bytes, home0.files), "仍计入 trash 来源目录的子树（配额照算）");
    assert_eq!((a1.bytes, a1.files), (a0.bytes - 1000, a0.files - 1), "原来的目录不再计它");
    assert_eq!((usage(&mut ns, t).bytes, usage(&mut ns, top).bytes), (1000, 1000), "回收站的两层都计它");
    assert_eq!((usage(&mut ns, top).dirs, home1.dirs), (1, home0.dirs + 2), "子目录也计进去");
    let ev = events(&ns).pop().unwrap();
    assert_eq!(extra(&ev, "trash"), Some(names[0].as_str()), "changelog 的 Unlink 带 trash=<新名字>");
    // mv 出来 = 恢复：到期时间与索引清掉
    rename(&mut ns, t, &names[0], a, "doc").unwrap();
    let i = ns.inode(f).unwrap();
    assert_eq!((i.parent, i.trash_expire_ms), (a, 0));
    assert!(ns.tag(T_TRASH_INDEX).is_empty(), "恢复之后没有到期索引");
    assert_eq!(usage(&mut ns, a).bytes, a0.bytes, "计数搬回来");
    // 回收站里再删 = 真删（孤儿，之后由 gc 回收）
    unlink(&mut ns, a, "doc").unwrap();
    let name = ns.ls(t).pop().unwrap();
    unlink(&mut ns, t, &name).unwrap();
    let i = ns.inode(f).unwrap();
    assert!(i.nlink == 0 && i.flags & F_ORPHAN != 0, "回收站里再删：真删");
    assert!(ns.tag(T_TRASH_INDEX).is_empty(), "真删时索引一并删掉");
    assert_eq!(usage(&mut ns, HOME).files, home0.files - 1);
}

#[test]
fn overwritten_rename_targets_go_to_the_trash_but_extra_links_do_not() {
    let mut ns = Ns::new();
    set(&mut ns, "/home", "trash=1d").unwrap();
    let (old, new) = (file(&mut ns, HOME, "b", 5), file(&mut ns, HOME, "a", 7));
    rename(&mut ns, HOME, "a", HOME, "b").unwrap();
    let t = ns.ino("/home/.wmfs-trash/1000").unwrap();
    assert_eq!(ns.ino("/home/b").unwrap(), new);
    assert_eq!(ns.ls(t), vec![format!("{}-{old}-b", ns.inode(old).unwrap().ctime.secs)], "被覆盖的 b 进了回收站");
    let ev = events(&ns);
    assert!(ev.iter().any(|e| extra(e, "trash").is_some() && e.ino == old), "被覆盖的文件另有一条带 trash= 的 Unlink");
    // 还有别的硬链接：只删这个名字、不进回收站（数据还在）
    let g = file(&mut ns, HOME, "g", 3);
    ns.op(&OWNER, MetaOp::Link { ino: g, new_parent: HOME, new_name: b"g2".to_vec() }).unwrap();
    unlink(&mut ns, HOME, "g").unwrap();
    assert_eq!(ns.inode(g).unwrap().nlink, 1);
    assert_eq!(ns.ls(t).len(), 1, "多链接的文件没进回收站");
    // 没设 trash 的目录照常删
    let s = file(&mut ns, SCRATCH, "s", 1);
    unlink(&mut ns, SCRATCH, "s").unwrap();
    assert_eq!(ns.inode(s).unwrap().nlink, 0);
}

/// 用回收站里的旧版本覆盖恢复（源目录就是回收站、被覆盖的新版本又进回收站）：回收站目录的 size 还是条目数（rmdir 按它判空）；很长的
/// UTF-8 名字截断时不切在一个字符中间。
#[test]
fn restoring_over_a_newer_file_keeps_the_trash_dir_consistent() {
    let mut ns = Ns::new();
    set(&mut ns, "/home", "trash=7d").unwrap();
    let old = file(&mut ns, HOME, "doc", 5);
    unlink(&mut ns, HOME, "doc").unwrap();
    let top = ns.ino("/home/.wmfs-trash").unwrap();
    let t = ns.ino("/home/.wmfs-trash/1000").unwrap();
    let new = file(&mut ns, HOME, "doc", 7);
    let name = ns.ls(t).pop().unwrap();
    rename(&mut ns, t, &name, HOME, "doc").unwrap();
    assert_eq!(ns.ino("/home/doc").unwrap(), old, "旧版本恢复了");
    let names = ns.ls(t);
    assert_eq!(names, vec![format!("{}-{new}-doc", ns.inode(new).unwrap().ctime.secs)], "被覆盖的新版本进了回收站");
    assert_eq!((ns.inode(t).unwrap().size, ns.inode(top).unwrap().size), (1, 1), "回收站目录的 size = 条目数");
    let rmdir = MetaOp::Rmdir { parent: top, name: b"1000".to_vec() };
    assert_eq!(ns.op(&OWNER, rmdir), Err(ErrCode::ENOTEMPTY), "回收站里还有东西：rmdir 不成");
    let rmdir = MetaOp::Rmdir { parent: HOME, name: TRASH_DIR.to_vec() };
    assert_eq!(ns.op(&OWNER, rmdir), Err(ErrCode::ENOTEMPTY));
    let long = "回".repeat(85); // 255 字节：加上 <秒>-<ino>- 的前缀之后要截断
    let f = file(&mut ns, HOME, &long, 1);
    unlink(&mut ns, HOME, &long).unwrap();
    let got = ns.ls(t).into_iter().find(|n| n.contains(&format!("-{f}-"))).expect("进了回收站、名字是合法的 UTF-8");
    assert!(got.len() <= 255 && got.ends_with('回'), "截断退到完整的字符：{got}");
}

/// 回收站按文件的属主分子目录（ADR-0043 第 1 条）：别的用户列不出 `.wmfs-trash`、进不了别人的子目录（看不到名字，也就打不开文件），
/// 属主自己能 mv 回来；root 删 alice 的文件进 alice 的子目录；别人建的同名目录不用（照常删）；建回收站的两层不检查配额。
#[test]
fn each_owner_has_a_private_trash() {
    let mut ns = Ns::new();
    set(&mut ns, "/home", "trash=7d").unwrap();
    let (alice, bob) = (user(2000, 2000), user(3000, 3000));
    let d = ns.mkdir(&alice, HOME, "alice").unwrap();
    let f = ns.create(&alice, d, "secret").unwrap();
    ns.op(&alice, MetaOp::Unlink { parent: d, name: b"secret".to_vec() }).unwrap();
    let (top, mine) = (ns.ino("/home/.wmfs-trash").unwrap(), ns.ino("/home/.wmfs-trash/2000").unwrap());
    let i = ns.inode(mine).unwrap();
    assert_eq!((i.mode, i.uid, i.gid), (0o700, 2000, 2000), "alice 的子目录");
    let name = ns.ls(mine).pop().unwrap().into_bytes();
    let ls = |ino| MetaOp::ReadDir { ino, after: vec![], limit: 100, plus: false };
    assert_eq!(ns.read(&bob, ls(top)), Err(ErrCode::EACCES), "bob 列不出有哪些用户");
    assert_eq!(ns.read(&bob, ls(mine)), Err(ErrCode::EACCES), "bob 列不出 alice 的子目录");
    let lookup = MetaOp::Lookup { parent: mine, name: name.clone() };
    assert_eq!(ns.read(&bob, lookup), Err(ErrCode::EACCES), "bob 查不到里面的名字，也就打不开");
    let back = MetaOp::Rename { parent: mine, name, new_parent: d, new_name: b"secret".to_vec(), flags: 0 };
    ns.op(&alice, back).unwrap();
    assert_eq!((ns.inode(f).unwrap().parent, ns.inode(f).unwrap().trash_expire_ms), (d, 0), "alice 自己 mv 回来了");
    // root 删 alice 的文件：进 alice 的子目录（按文件的属主，不是删它的人）；bob 删自己的进 bob 的
    ns.op(&ROOT, MetaOp::Unlink { parent: d, name: b"secret".to_vec() }).unwrap();
    assert_eq!(ns.inode(f).unwrap().parent, mine);
    assert!(ns.ino("/home/.wmfs-trash/0").is_err(), "没有给删它的 root 建子目录");
    let g = ns.create(&bob, HOME, "g").unwrap();
    ns.op(&bob, MetaOp::Unlink { parent: HOME, name: b"g".to_vec() }).unwrap();
    assert_eq!(ns.inode(g).unwrap().parent, ns.ino("/home/.wmfs-trash/3000").unwrap());
    // `.wmfs-trash` 的属主（/home 的属主）替 uid 4000 建了一个自己的目录：不往里放，照常删
    ns.mkdir(&OWNER, top, "4000").unwrap();
    let carol = user(4000, 4000);
    let h = ns.create(&carol, HOME, "h").unwrap();
    ns.op(&carol, MetaOp::Unlink { parent: HOME, name: b"h".to_vec() }).unwrap();
    assert_eq!(ns.inode(h).unwrap().nlink, 0, "别人建的同名目录不用：真删");
    // 建回收站的两层不检查配额：文件数已经到了上限，rm 照样成功、进回收站
    let q = ns.mkdir(&OWNER, HOME, "q").unwrap();
    set(&mut ns, "/home/q", "quota=1M,files=1").unwrap();
    set(&mut ns, "/home/q", "trash=1d").unwrap();
    let k = file(&mut ns, q, "k", 1);
    unlink(&mut ns, q, "k").unwrap();
    assert_eq!(ns.inode(k).unwrap().parent, ns.ino("/home/q/.wmfs-trash/1000").unwrap());
    let rep = ns.view(&ROOT, |v, _| rstat::check(v).unwrap());
    assert!(rep.problems.is_empty(), "递归计数与按 inode 重算的一致（回收站的两层是手动加的）：{:?}", rep.problems);
}

#[test]
fn expire_and_ttl_delete_what_is_due() {
    let mut ns = Ns::new();
    set(&mut ns, "/home", "trash=10s").unwrap();
    let f = file(&mut ns, HOME, "x", 1);
    unlink(&mut ns, HOME, "x").unwrap();
    let due = ns.inode(f).unwrap().trash_expire_ms;
    // 过时的索引（到期时间对不上）：只删索引，文件还在回收站里
    let stale = ns.txn(&ROOT, |v, c| {
        v.put(&keys::trash_index(due - 1, f), vec![]);
        trash::expire(v, c, due - 1, f)
    });
    assert_eq!((stale, ns.inode(f).unwrap().nlink), (Ok(false), 1), "过时的索引：没删（插件不计数）");
    assert_eq!(ns.txn(&ROOT, |v, c| trash::expire(v, c, due, f)), Ok(true));
    assert_eq!(ns.inode(f).unwrap().nlink, 0, "到期：真删");
    assert_eq!(events(&ns).pop().unwrap().op, wmfs_core::api::ops::op_code("TrashExpire").unwrap());
    // ttl：mtime 早于 now − ttl 的才删；/home 有 trash，所以进回收站，changelog 的 Unlink 带 reason=ttl
    let d = ns.mkdir(&OWNER, HOME, "t").unwrap();
    set(&mut ns, "/home/t", "ttl=30s").unwrap();
    let g = file(&mut ns, d, "old", 1);
    assert_eq!(ns.txn(&ROOT, |v, c| trash::ttl(v, c, d, b"old", g)), Ok(false));
    assert_eq!(ns.inode(g).unwrap().parent, d, "还没过期：不动");
    ns.now += 60;
    assert_eq!(ns.txn(&ROOT, |v, c| trash::ttl(v, c, d, b"old", g)), Ok(true));
    let i = ns.inode(g).unwrap();
    assert!(i.trash_expire_ms > 0 && i.nlink == 1, "过期：按 unlink 删（trash 生效就进回收站）");
    let ev = events(&ns).pop().unwrap();
    assert_eq!((extra(&ev, "reason"), ev.ino), (Some("ttl"), g));
    assert!(set(&mut ns, "/home", "ttl=0s").is_err() && set(&mut ns, "/home", "trash=366d").is_err());
}

#[test]
fn audit_marks_events_filters_them_and_records_rejections() {
    let mut ns = Ns::new();
    let before = changelog_last(&mut ns);
    set(&mut ns, "/home", "audit=on").unwrap();
    let cur = ns.view(&ROOT, |v, _| changelog::get_cursor(v, AUDIT_CURSOR).unwrap()).expect("第一次启用时建游标");
    assert_eq!(cur.seq, before + 1, "游标 = 启用时最大的 seq（含这次 AttrSet 自己；之后的记录按它保留）");
    let f = ns.create(&OWNER, HOME, "f").unwrap();
    ns.create(&OWNER, SCRATCH, "s").unwrap();
    let ev = events(&ns);
    let created = |ino| ev.iter().find(|e| e.ino == ino).unwrap().audited;
    assert!(created(f), "/home 里的 Create 被审计");
    assert!(!ev.last().unwrap().audited, "/scratch 没开 audit");
    // 过滤：/home/d 只审计 uid 1000 的 unlink
    let d = ns.mkdir(&OWNER, HOME, "d").unwrap();
    set(&mut ns, "/home/d", "audit=on,ops=unlink,uids=1000").unwrap();
    let g = ns.create(&OWNER, d, "g").unwrap();
    unlink(&mut ns, d, "g").unwrap();
    let ev = events(&ns);
    let of = |op: &str| {
        ev.iter().rev().find(|e| e.op == wmfs_core::api::ops::op_code(op).unwrap() && (e.ino == g || e.parent == d))
    };
    assert!(!of("Create").unwrap().audited && of("Unlink").unwrap().audited, "ops=unlink");
    set(&mut ns, "/home", "audit=on,uids=2000").unwrap();
    let mine = ns.create(&OWNER, HOME, "mine").unwrap();
    let theirs = ns.create(&user(2000, 2000), HOME, "theirs").unwrap();
    let ev = events(&ns);
    let audited = |ino| ev.iter().rev().find(|e| e.ino == ino).unwrap().audited;
    assert!(!audited(mine) && audited(theirs), "uids=2000：只审计 uid 2000 的");
    // 被拒绝的修改（executor 的 run_one 调用）：audited 时也写一条 result = 错误码
    let s = Subject { op: "Unlink", parent: HOME, name: b"nope".to_vec(), ..Default::default() };
    ns.txn(&user(2000, 2000), |v, c| changelog::rejected(v, c, &s, ErrCode::ENOENT)).unwrap();
    let e = events(&ns).pop().unwrap();
    assert_eq!((e.result, e.audited, e.name.as_slice()), (ErrCode::ENOENT.0, true, &b"nope"[..]));
    let s = Subject { op: "Unlink", parent: SCRATCH, name: b"nope".to_vec(), ..Default::default() };
    let n = events(&ns).len();
    ns.txn(&ROOT, |v, c| changelog::rejected(v, c, &s, ErrCode::ENOENT)).unwrap();
    assert_eq!(events(&ns).len(), n, "不审计的目录：被拒绝的修改不记");
    // rename 也看目标目录：从不审计的 /scratch mv 进 audit=on 的 /home/e 也带 audited；被拒绝的也一样
    let e = ns.mkdir(&OWNER, HOME, "e").unwrap();
    set(&mut ns, "/home/e", "audit=on").unwrap();
    ns.create(&OWNER, SCRATCH, "mv").unwrap();
    rename(&mut ns, SCRATCH, "mv", e, "mv").unwrap();
    assert!(events(&ns).pop().unwrap().audited, "mv 进 audit=on 的子树：带 audited");
    let op =
        MetaOp::Rename { parent: SCRATCH, name: b"nope".to_vec(), new_parent: e, new_name: b"x".to_vec(), flags: 0 };
    let s = audit_of(&op).unwrap();
    ns.txn(&ROOT, |v, c| changelog::rejected(v, c, &s, ErrCode::ENOENT)).unwrap();
    let r = events(&ns).pop().unwrap();
    assert_eq!((r.result, r.audited, r.new_parent), (ErrCode::ENOENT.0, true, e), "被拒绝的 rename：目标目录审计也记");
}

fn changelog_last(ns: &mut Ns) -> u64 {
    ns.view(&ROOT, |v, _| changelog::last_seq(v).unwrap())
}

#[test]
fn strong_directories_do_not_inline_new_files() {
    let mut ns = Ns::new();
    let d = ns.mkdir(&OWNER, HOME, "s").unwrap();
    set(&mut ns, "/home/s", "consistency=strong").unwrap();
    let f = ns.create(&OWNER, d, "f").unwrap();
    let i = ns.inode(f).unwrap();
    assert!(i.flags & F_INLINE == 0 && i.layout.unwrap().inline_max == 0, "strong 目录里的新文件不 inline");
    let a = ns.attr(f);
    assert_eq!((a.consistency, a.ttl_ms), (2, 0), "strong：AttrOut.consistency = 2、TTL 0");
}
