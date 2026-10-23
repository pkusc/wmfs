//! 写入相关的元数据操作（design.md 3.5.1 的 CommitWrite / Fallocate、3.3.4、3.3.5、ADR-0034），在内存 KV 上跑：
//! - inline 文件的补丁、`len(INLINE) ≤ size` 不变式：`echo hello>f; echo x>f; truncate -s 6 f` 读到 "x\n" 加 4 个 0（roadmap M5）；
//!   补丁越过当前长度时中间补 0、越过 inline_max → EINVAL；
//! - CommitWrite 的 epoch CAS（STALE_LAYOUT）、清除 INLINE 的 change_id CAS、写权限、非属主写入去掉 setuid、changelog SetSize；
//! - Fallocate（mode 0）、GetInline 只给能读的人、RenewCap；
//! - Open / Create / 改长度的 SetAttr 签的 cap：权限位按打开方式、链与变换参数来自 layout、有效期 cap_ttl、K_cap 验得过；
//!   只读路径签 cap 之前核对会话（session 0 → EINVAL、会话不在 → SESSION_EVICTED、别的 key 的会话 → EACCES）；
//! - 单个文件最大 chunk_size × 2³²（改长度、CommitWrite、Fallocate 超过 → EFBIG）；inline 文件截断到 0 也给旧 epoch 写 GC 记录。

use wmfs_core::base::ErrCode;
use wmfs_core::base::types::Ts;
use wmfs_core::meta::schema::keys::{self, GC_EPOCH, ROOT_INO, T_CHANGELOG};
use wmfs_core::meta::schema::records::{ChangeRecV1, F_INLINE, SessionRecV1, decode_v1, encode_v1};
use wmfs_core::proto::cap::{self, RIGHT_READ, RIGHT_WRITE};
use wmfs_core::proto::meta::{MetaOp, MetaReply, O_RDONLY, O_RDWR, O_WRONLY, OpenOut, SetAttrIn};
use wmfs_testkit::harness::meta::CAP_KEY;
use wmfs_testkit::harness::ns::{Ns, OWNER, SESSION, Who, user};

fn open(ns: &mut Ns, who: &Who, ino: u64, flags: u32) -> Result<OpenOut, ErrCode> {
    match ns.read(who, MetaOp::Open { ino, flags })? {
        MetaReply::Opened(o) => Ok(o),
        other => panic!("Open 的回复不对：{other:?}"),
    }
}

fn commit(ino: u64, epoch: u32, new_size: u64, patches: Vec<(u64, Vec<u8>)>, uninline: Option<u64>) -> MetaOp {
    MetaOp::CommitWrite {
        ino,
        epoch,
        new_size,
        mtime: Ts::default(),
        inline_patches: patches,
        uninline_expect_change_id: uninline,
        want_quota: false,
        close: true,
    }
}

fn size(s: u64, epoch_seen: u32) -> MetaOp {
    MetaOp::SetAttr {
        ino: 0, // 调用方改
        set: SetAttrIn { mode: None, uid: None, gid: None, size: Some(s), atime: None, mtime: None, epoch_seen },
    }
}

fn set_size(ns: &mut Ns, who: &Who, ino: u64, s: u64) -> Result<OpenOut, ErrCode> {
    let MetaOp::SetAttr { set, .. } = size(s, 0) else { unreachable!() };
    match ns.op(who, MetaOp::SetAttr { ino, set })? {
        MetaReply::Opened(o) => Ok(o),
        other => panic!("改长度的 SetAttr 应回 Opened：{other:?}"),
    }
}

fn epoch(o: &OpenOut) -> u32 {
    o.layout.as_ref().unwrap().epoch
}

/// 客户端读整个文件的样子：inline 内容 + 补 0 到 size。
fn content(o: &OpenOut) -> Vec<u8> {
    let mut d = o.inline.clone().unwrap();
    d.resize(o.attr.size as usize, 0);
    d
}

#[test]
fn echo_hello_then_echo_x_then_truncate_6_reads_x_newline_and_four_zeros() {
    let mut ns = Ns::new();
    let f = ns.create(&OWNER, ROOT_INO, "f").unwrap();
    let o = open(&mut ns, &OWNER, f, O_RDWR).unwrap();
    assert!(o.attr.flags & F_INLINE != 0, "新文件默认 inline（inline_max 64K）");
    ns.op(&OWNER, commit(f, epoch(&o), 6, vec![(0, b"hello\n".to_vec())], None)).unwrap(); // echo hello > f
    let o = set_size(&mut ns, &OWNER, f, 0).unwrap(); // echo x > f：O_TRUNC = 先 SetAttr{size: 0}
    assert_eq!((o.attr.size, o.inline.as_deref()), (0, Some(&b""[..])), "截断到 0：INLINE 清空");
    ns.op(&OWNER, commit(f, epoch(&o), 2, vec![(0, b"x\n".to_vec())], None)).unwrap();
    let o = set_size(&mut ns, &OWNER, f, 6).unwrap(); // truncate -s 6 f
    assert_eq!(o.attr.size, 6);
    assert_eq!(content(&open(&mut ns, &OWNER, f, O_RDONLY).unwrap()), b"x\n\0\0\0\0", "旧的 \"llo\" 不能重新出现");
    match ns.read(&OWNER, MetaOp::GetInline { ino: f }).unwrap() {
        MetaReply::Inline { data, .. } => assert_eq!(data, b"x\n", "len(INLINE) = 2 ≤ size = 6"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn inline_patches_fill_gaps_with_zeros_and_respect_inline_max() {
    let mut ns = Ns::new();
    let f = ns.create(&OWNER, ROOT_INO, "f").unwrap();
    let e = epoch(&open(&mut ns, &OWNER, f, O_RDWR).unwrap());
    ns.op(&OWNER, commit(f, e, 0, vec![(4, b"ab".to_vec()), (10, b"z".to_vec())], None)).unwrap();
    let o = open(&mut ns, &OWNER, f, O_RDONLY).unwrap();
    assert_eq!((o.attr.size, content(&o)), (11, b"\0\0\0\0ab\0\0\0\0z".to_vec()), "size 至少到补丁末尾");
    let big = vec![1u8; 10];
    assert_eq!(ns.op(&OWNER, commit(f, e, 0, vec![(65_530, big)], None)), Err(ErrCode::EINVAL), "越过 inline_max");
    assert_eq!(ns.op(&OWNER, commit(f, e, 70_000, vec![], None)), Err(ErrCode::EINVAL), "inline 文件没有 chunk");
    assert_eq!(ns.op(&OWNER, commit(f, e + 1, 1, vec![], None)), Err(ErrCode::STALE_LAYOUT), "epoch 不对");
    ns.op(&OWNER, commit(f, e, 11, vec![(0, b"Q".to_vec())], None)).unwrap();
    assert_eq!(content(&open(&mut ns, &OWNER, f, O_RDONLY).unwrap())[0], b'Q');
}

#[test]
fn uninline_is_a_change_id_cas_and_writes_no_changelog() {
    let mut ns = Ns::new();
    let f = ns.create(&OWNER, ROOT_INO, "f").unwrap();
    let o = open(&mut ns, &OWNER, f, O_RDWR).unwrap();
    ns.op(&OWNER, commit(f, epoch(&o), 3, vec![(0, b"abc".to_vec())], None)).unwrap();
    let o = open(&mut ns, &OWNER, f, O_RDONLY).unwrap();
    let logs = ns.tag(T_CHANGELOG).len();
    let stale = commit(f, epoch(&o), 0, vec![], Some(o.attr.change_id - 1));
    assert_eq!(ns.op(&OWNER, stale), Err(ErrCode::STALE_LAYOUT), "change_id 变了：CAS 失败");
    let r = ns.op(&OWNER, commit(f, epoch(&o), 0, vec![], Some(o.attr.change_id))).unwrap();
    let MetaReply::Committed { attr, .. } = r else { panic!("{r:?}") };
    assert_eq!((attr.flags & F_INLINE, attr.size), (0, 3), "清除 F_INLINE，大小不变");
    assert!(!ns.kv.0.contains_key(&keys::inline(f)), "INLINE 删掉了");
    assert_eq!(ns.tag(T_CHANGELOG).len(), logs, "只换了存放位置：不写 changelog");
    let again = commit(f, epoch(&o), 0, vec![], Some(attr.change_id));
    assert_eq!(ns.op(&OWNER, again), Err(ErrCode::STALE_LAYOUT), "已经不是 inline");
    let patch = commit(f, epoch(&o), 3, vec![(0, b"z".to_vec())], None);
    assert_eq!(ns.op(&OWNER, patch), Err(ErrCode::STALE_LAYOUT), "已经转成 chunk 的文件不接受 inline 补丁");
    assert_eq!(ns.read(&OWNER, MetaOp::GetInline { ino: f }), Err(ErrCode::STALE_LAYOUT));
}

#[test]
fn commit_write_needs_write_permission_kills_suid_and_logs_set_size() {
    let mut ns = Ns::new();
    let f = ns.create(&OWNER, ROOT_INO, "f").unwrap();
    let chmod = |m| MetaOp::SetAttr {
        ino: f,
        set: SetAttrIn { mode: Some(m), uid: None, gid: None, size: None, atime: None, mtime: None, epoch_seen: 0 },
    };
    ns.op(&OWNER, chmod(0o4646)).unwrap(); // setuid + 其他人可写
    let o = open(&mut ns, &OWNER, f, O_RDONLY).unwrap();
    let e = epoch(&o);
    assert_eq!(ns.op(&user(2000, 2000), commit(f, e, 1, vec![(0, b"a".to_vec())], None)).map(|_| ()), Ok(()));
    assert_eq!(ns.inode(f).unwrap().mode & 0o4000, 0, "非属主写入：去掉 setuid");
    ns.op(&OWNER, chmod(0o644)).unwrap();
    assert_eq!(ns.op(&user(2000, 2000), commit(f, e, 2, vec![], None)), Err(ErrCode::EACCES), "没有写权限");
    let last = ns.tag(T_CHANGELOG).into_iter().rev().find_map(|(_, v)| {
        let r: ChangeRecV1 = decode_v1(&v).unwrap();
        (r.op == 14).then_some(r)
    });
    let r = last.expect("CommitWrite 写 changelog SetSize");
    // CommitWrite.close 服务端还不用（3.3.4 的 M6 注记 ③：等主管决定），changelog 里只有 via
    assert_eq!((r.ino, r.size, r.extra), (f, 1, vec![("via".to_string(), "CommitWrite".to_string())]));
}

#[test]
fn fallocate_extends_only_with_mode_0() {
    let mut ns = Ns::new();
    let f = ns.create(&OWNER, ROOT_INO, "f").unwrap();
    let fa = |offset, len, mode| MetaOp::Fallocate { ino: f, offset, len, mode };
    assert_eq!(ns.op(&OWNER, fa(0, 10, 1)), Err(ErrCode::EOPNOTSUPP), "只支持 mode 0");
    assert_eq!(ns.op(&OWNER, fa(0, 0, 0)), Err(ErrCode::EINVAL));
    assert!(ns.op(&OWNER, fa(100, 900, 0)).is_ok());
    assert_eq!(ns.attr(f).size, 1000);
    assert!(ns.op(&OWNER, fa(0, 10, 0)).is_ok());
    assert_eq!(ns.attr(f).size, 1000, "不会把文件变短");
    assert_eq!(ns.op(&user(2000, 2000), fa(0, 5000, 0)), Err(ErrCode::EACCES));
    assert_eq!(ns.op(&OWNER, MetaOp::Fallocate { ino: ROOT_INO, offset: 0, len: 1, mode: 0 }), Err(ErrCode::EISDIR));
}

#[test]
fn get_inline_only_for_readers_and_caps_follow_the_open_mode() {
    let mut ns = Ns::new();
    let f = ns.create(&OWNER, ROOT_INO, "f").unwrap();
    let o = open(&mut ns, &OWNER, f, O_RDWR).unwrap();
    ns.op(&OWNER, commit(f, epoch(&o), 2, vec![(0, b"hi".to_vec())], None)).unwrap();
    let chmod = MetaOp::SetAttr {
        ino: f,
        set: SetAttrIn { mode: Some(0o622), uid: None, gid: None, size: None, atime: None, mtime: None, epoch_seen: 0 },
    };
    ns.op(&OWNER, chmod).unwrap();
    let other = user(2000, 2000);
    assert_eq!(ns.read(&other, MetaOp::GetInline { ino: f }), Err(ErrCode::EACCES), "只有写权限：拿不到内容");
    let w = open(&mut ns, &other, f, O_WRONLY).unwrap();
    assert_eq!(w.inline, None, "写打开不带内容");
    let c = cap::verify(&CAP_KEY, &w.cap).unwrap();
    assert_eq!((c.rights, c.ino, c.epoch, c.session), (RIGHT_WRITE, f, epoch(&w), 7));
    assert_eq!(c.chains, w.layout.as_ref().unwrap().chains);
    assert_eq!(c.expires_ms, ns.now as u64 * 1000 + 600_000, "有效期 = now + cap_ttl（10 min）");
    assert_eq!(cap::verify(&[0; 32], &w.cap), Err(ErrCode::BAD_CAP), "别的 K_cap 验不过");
    let r = open(&mut ns, &OWNER, f, O_RDONLY).unwrap();
    assert_eq!(cap::verify(&CAP_KEY, &r.cap).unwrap().rights, RIGHT_READ);
    let rw = open(&mut ns, &OWNER, f, O_RDWR).unwrap();
    assert_eq!(cap::verify(&CAP_KEY, &rw.cap).unwrap().rights, RIGHT_READ | RIGHT_WRITE);
    let o = set_size(&mut ns, &other, f, 0).unwrap();
    assert_eq!(
        cap::verify(&CAP_KEY, &o.cap).unwrap().rights,
        RIGHT_WRITE,
        "改长度的 SetAttr 回写 cap（不能读的人只有写）"
    );
    match ns.read(&other, MetaOp::RenewCap { ino: f, write: true }).unwrap() {
        MetaReply::Cap { cap: b } => assert_eq!(cap::verify(&CAP_KEY, &b).unwrap().rights, RIGHT_WRITE),
        x => panic!("{x:?}"),
    }
    assert_eq!(ns.read(&other, MetaOp::RenewCap { ino: f, write: false }), Err(ErrCode::EACCES));
    let d = ns.mkdir(&OWNER, ROOT_INO, "d").unwrap();
    assert!(open(&mut ns, &OWNER, d, O_RDONLY).unwrap().cap.is_empty(), "目录没有数据：不签 cap");
    let create = MetaOp::Create { parent: ROOT_INO, name: b"f".to_vec(), mode: 0o644, umask: 0, excl: false };
    let MetaReply::Opened(o) = ns.op(&other, create).unwrap() else { panic!() };
    assert_eq!(
        cap::verify(&CAP_KEY, &o.cap).unwrap().rights,
        RIGHT_WRITE,
        "Create 打开已有的文件：写（不能读就没有读）"
    );
    let fresh = MetaOp::Create { parent: ROOT_INO, name: b"g".to_vec(), mode: 0o200, umask: 0, excl: true };
    let MetaReply::Opened(o) = ns.op(&OWNER, fresh).unwrap() else { panic!() };
    assert_eq!(cap::verify(&CAP_KEY, &o.cap).unwrap().rights, RIGHT_READ | RIGHT_WRITE, "新建的文件：读写都可以");
}

#[test]
fn caps_on_the_read_path_need_a_usable_session() {
    let mut ns = Ns::new();
    let f = ns.create(&OWNER, ROOT_INO, "f").unwrap();
    assert!(open(&mut ns, &OWNER, f, O_RDONLY).is_ok());
    ns.session = 0;
    let e = open(&mut ns, &OWNER, f, O_RDONLY).err();
    assert_eq!(e, Some(ErrCode::EINVAL), "session 0 的 cap 不属于任何会话：只签给内部请求与 Server 角色");
    assert_eq!(ns.read(&OWNER, MetaOp::RenewCap { ino: f, write: false }), Err(ErrCode::EINVAL));
    ns.session = 8;
    assert_eq!(open(&mut ns, &OWNER, f, O_RDONLY).err(), Some(ErrCode::SESSION_EVICTED), "没有这个会话");
    ns.session = SESSION;
    let mut rec: SessionRecV1 = decode_v1(&ns.kv.0[&keys::session(SESSION)]).unwrap();
    rec.key_id = 2;
    ns.kv.0.insert(keys::session(SESSION), encode_v1(&rec));
    assert_eq!(open(&mut ns, &OWNER, f, O_RDONLY).err(), Some(ErrCode::EACCES), "别的 key 打开的会话");
}

#[test]
fn file_size_is_capped_at_chunk_size_times_2_pow_32() {
    let mut ns = Ns::new();
    let f = ns.create(&OWNER, ROOT_INO, "f").unwrap();
    let o = open(&mut ns, &OWNER, f, O_RDWR).unwrap();
    let max = (o.layout.as_ref().unwrap().chunk_size as u64) << 32; // 4 MiB 的 chunk：16 PiB（chunk 号是 u32）
    assert_eq!(set_size(&mut ns, &OWNER, f, max + 1).err(), Some(ErrCode::EFBIG), "改长度");
    assert_eq!(set_size(&mut ns, &OWNER, f, u64::MAX).err(), Some(ErrCode::EFBIG));
    assert_eq!(ns.op(&OWNER, commit(f, epoch(&o), max + 1, vec![], None)), Err(ErrCode::EFBIG), "CommitWrite");
    let fa = MetaOp::Fallocate { ino: f, offset: max, len: 1, mode: 0 };
    assert_eq!(ns.op(&OWNER, fa), Err(ErrCode::EFBIG), "Fallocate");
    assert_eq!(set_size(&mut ns, &OWNER, f, max).unwrap().attr.size, max, "正好到上限可以");
}

#[test]
fn truncating_an_inline_file_to_0_records_its_old_epoch_for_gc() {
    let mut ns = Ns::new();
    let f = ns.create(&OWNER, ROOT_INO, "f").unwrap();
    let o = open(&mut ns, &OWNER, f, O_RDWR).unwrap();
    assert!(o.attr.flags & F_INLINE != 0);
    let z = set_size(&mut ns, &OWNER, f, 0).unwrap();
    assert_eq!(epoch(&z), epoch(&o) + 1);
    let gc = keys::gc(GC_EPOCH, f, epoch(&o));
    assert!(ns.kv.0.contains_key(&gc), "inline 文件的旧 epoch 下面也可能有 chunk（没做完的 Uninline 留下的）：交给 gc");
}

/// M6 的 SetAtime（relatime 批量、尽力而为）：读打开时按 relatime 给出 atime_due；SetAtime 只把 atime 往后改（不超过 leader 的
/// 现在）、不改 ctime 与 change_id（不让别的客户端的缓存失效）；读不了的、已经删掉的项跳过；一批超过 1024 项 → EINVAL。
#[test]
fn set_atime_is_best_effort_and_keeps_change_id() {
    let mut ns = Ns::new();
    let f = ns.create(&OWNER, ROOT_INO, "f").unwrap();
    let o = open(&mut ns, &OWNER, f, O_RDONLY).unwrap();
    assert!(o.atime_due, "atime 不晚于 mtime：relatime 该更新");
    assert!(!open(&mut ns, &OWNER, f, O_WRONLY).unwrap().atime_due, "写打开不更新 atime");
    let before = ns.attr(f);
    let later = Ts { secs: ns.now + 100, nanos: 0 };
    let set = |items| MetaOp::SetAtime { items };
    assert_eq!(ns.op(&OWNER, set(vec![(f, later), (999_999, later)])), Ok(MetaReply::Ok), "已经删掉的项跳过");
    let after = ns.attr(f);
    assert!(after.atime > before.atime && after.atime < later, "往后改，但不超过 leader 的现在：{:?}", after.atime);
    assert_eq!((after.change_id, after.ctime), (before.change_id, before.ctime), "atime 不是内容变化");
    assert!(!open(&mut ns, &OWNER, f, O_RDONLY).unwrap().atime_due, "atime 晚于 mtime 与 ctime：不必再更新");
    let old = Ts { secs: 1, nanos: 0 };
    ns.op(&OWNER, set(vec![(f, old)])).unwrap();
    assert_eq!(ns.attr(f).atime, after.atime, "只往后走");
    let chmod = MetaOp::SetAttr {
        ino: f,
        set: SetAttrIn { mode: Some(0o600), uid: None, gid: None, size: None, atime: None, mtime: None, epoch_seen: 0 },
    };
    ns.op(&OWNER, chmod).unwrap();
    let a = ns.attr(f).atime;
    ns.op(&user(2000, 2000), set(vec![(f, Ts { secs: ns.now, nanos: 0 })])).unwrap();
    assert_eq!(ns.attr(f).atime, a, "读不了这个文件的人：跳过");
    assert_eq!(ns.op(&OWNER, set(vec![(f, old); 1025])), Err(ErrCode::EINVAL), "一批最多 1024 项");
}
