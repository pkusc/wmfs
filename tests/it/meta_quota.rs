//! M10 的配额、acl.default 与路径限制（design.md 3.9、4.3、4.4、第 5 章），在内存 KV 视图上逐条检查语义（`harness::ns`）：
//! - 文件数配额精确（文件 + 子目录）；字节配额：Create / Open（写）回的 `quota_end` = 长度 + min(quantum, 剩余)；扩展长度精确检查；
//!   CommitWrite 的护栏（Δ ≤ quantum 时最多到 hard + 16 × quantum；Δ > quantum 且超了硬限 → 拒绝；长度不更新）；
//! - 软限：越过时开始计时、写 `PolicyViolation` 事件；宽限期内按硬限，过后按软限拒绝；回到软限以内之后再越过重新计时；
//! - Stacked：`inherit=false` 挡不住上级配额；`quota.<类>` 只计那个类、类要存在；跨 scope 的 rename 精确检查；孤儿不计；
//! - `acl.default`：父目录没有默认 ACL 时新文件按它得到访问 ACL、不用 umask；
//! - path-caps：子树里放行、上级目录只能查名字与取属性（core 判定的路径遍历）、子树外 `EACCES`、只读的根上写 `EROFS`；
//! - M10 审查：同一目录上的总量与按类的软限各自计时；设配额时重判软限、unset/删目录时删掉起点；rename 覆盖与对调按扣掉之后的用量查；
//!   link 的源文件也受路径限制；ADR-0041：硬链接不跨配额范围（`EXDEV`）、重放同一份配额配置不推后正在计时的宽限期。

use wmfs_core::base::{ErrCode, Failed};
use wmfs_core::meta::attr_ops;
use wmfs_core::meta::attr_registry::split;
use wmfs_core::meta::ns::acl;
use wmfs_core::meta::schema::keys::{self, ROOT_INO, T_CHANGELOG};
use wmfs_core::meta::schema::records::{ChangeRecV1, decode_v1};
use wmfs_core::proto::meta::{MetaOp, MetaReply, O_RDONLY, O_RDWR, RENAME_EXCHANGE, SetAttrIn};
use wmfs_testkit::harness::ns::{Ns, OWNER, ROOT, user};

fn set(ns: &mut Ns, path: &str, spec: &str) -> Result<(), Failed> {
    let items = split(spec)?;
    ns.txn(&ROOT, |v, c| attr_ops::set(v, c, path, &items)).map(|_| ())
}

/// `/q`（属主 OWNER）上设 `spec`，新文件不 inline（CommitWrite 才能把长度提到 64 KiB 以上）。
fn quota_dir(ns: &mut Ns, spec: &str) -> u64 {
    let q = ns.mkdir(&OWNER, ROOT_INO, "q").unwrap();
    set(ns, "/q", &format!("inline=0,{spec}")).unwrap();
    q
}

/// 建文件，回 (ino, quota_end)。
fn create(ns: &mut Ns, parent: u64, name: &str) -> Result<(u64, u64), ErrCode> {
    let op = MetaOp::Create { parent, name: name.as_bytes().to_vec(), mode: 0o644, umask: 0, excl: true };
    match ns.op(&OWNER, op)? {
        MetaReply::Opened(o) => Ok((o.attr.ino, o.quota_end)),
        r => panic!("{r:?}"),
    }
}

/// CommitWrite 把长度提到 `size`（按已授予的额度写好了的数据，同客户端的写回），回新的 quota_end。
fn commit(ns: &mut Ns, ino: u64, size: u64) -> Result<u64, ErrCode> {
    commit_as(ns, ino, size, false)
}

/// 同 [`commit`]；`want_quota` = 客户端专门来要额度（`ensure_quota`）：增长按插件的判定精确检查（ADR-0041 第 8 条 ⑤）。
fn commit_as(ns: &mut Ns, ino: u64, size: u64, want_quota: bool) -> Result<u64, ErrCode> {
    let epoch = ns.inode(ino).unwrap().layout.unwrap().epoch;
    let op = MetaOp::CommitWrite {
        ino,
        epoch,
        new_size: size,
        mtime: Default::default(),
        inline_patches: vec![],
        uninline_expect_change_id: None,
        want_quota,
        close: false,
    };
    match ns.op(&OWNER, op)? {
        MetaReply::Committed { quota_end, .. } => Ok(quota_end),
        r => panic!("{r:?}"),
    }
}

/// 按路径改长度（SetAttr{size}；不是 inline 的文件要带看到的 epoch，ADR-0035 第 9 条）。
fn resize(ns: &mut Ns, ino: u64, size: u64) -> Result<MetaReply, ErrCode> {
    let epoch_seen = ns.inode(ino).unwrap().layout.map_or(0, |l| l.epoch);
    let set = SetAttrIn { mode: None, uid: None, gid: None, size: Some(size), atime: None, mtime: None, epoch_seen };
    ns.op(&OWNER, MetaOp::SetAttr { ino, set })
}

const M: u64 = 1 << 20;

#[test]
fn file_counts_are_exact() {
    let mut ns = Ns::new();
    let q = quota_dir(&mut ns, "quota=1G,files=3");
    create(&mut ns, q, "a").unwrap();
    ns.mkdir(&OWNER, q, "d").unwrap();
    create(&mut ns, q, "b").unwrap();
    assert_eq!(create(&mut ns, q, "c"), Err(ErrCode::EDQUOT), "文件 + 子目录 = 3：第 4 个拒绝");
    assert_eq!(ns.mkdir(&OWNER, q, "e"), Err(ErrCode::EDQUOT));
    assert!(ns.create(&OWNER, ROOT_INO, "outside").is_ok(), "子树外不受影响");
}

#[test]
fn grants_exact_checks_and_the_commit_guard() {
    let mut ns = Ns::new();
    let q = quota_dir(&mut ns, "quota=8M,quantum=1M");
    let (f, end) = create(&mut ns, q, "f").unwrap();
    assert_eq!(end, M, "新文件：0 + min(quantum, 剩余)");
    assert_eq!(commit(&mut ns, f, M), Ok(2 * M), "提交 1 MiB：额度续到 2 MiB");
    let MetaReply::Opened(o) = ns.read(&OWNER, MetaOp::Open { ino: f, flags: O_RDWR }).unwrap() else { panic!() };
    assert_eq!(o.quota_end, 2 * M, "写打开回额度");
    let MetaReply::Opened(o) = ns.read(&OWNER, MetaOp::Open { ino: f, flags: O_RDONLY }).unwrap() else { panic!() };
    assert_eq!(o.quota_end, u64::MAX, "只读打开不给额度");
    assert_eq!(resize(&mut ns, f, 8 * M + 1), Err(ErrCode::EDQUOT), "扩展长度：精确检查");
    assert!(resize(&mut ns, f, 7 * M).is_ok());
    assert_eq!(commit(&mut ns, f, 8 * M), Ok(8 * M), "正好用满：额度为 0");
    assert_eq!(commit(&mut ns, f, 10 * M), Err(ErrCode::EDQUOT), "单次增长 2 MiB > quantum 且超了硬限：拒绝");
    assert_eq!(ns.inode(f).unwrap().size, 8 * M, "长度没有更新");
    let (g, _) = create(&mut ns, q, "g").unwrap();
    assert_eq!(commit_as(&mut ns, g, M, true), Err(ErrCode::EDQUOT), "专门来要额度：增长 ≤ quantum 也按硬限精确拒绝");
    assert_eq!(ns.inode(g).unwrap().size, 0);
    for i in 1..=16 {
        assert!(commit(&mut ns, g, i * M).is_ok(), "已授予的额度（守协议的并发写者）：最多超出 16 × quantum");
    }
    assert_eq!(commit(&mut ns, g, 17 * M), Err(ErrCode::EDQUOT), "超过 hard + 16 × quantum：不守协议的客户端也被挡住");
}

fn soft_events(ns: &Ns) -> usize {
    let ev = |(_, b): &(Vec<u8>, Vec<u8>)| decode_v1::<ChangeRecV1>(b).unwrap();
    ns.tag(T_CHANGELOG).iter().map(ev).filter(|e| e.extra.iter().any(|(k, v)| k == "kind" && v == "quota_soft")).count()
}

#[test]
fn soft_limits_start_a_grace_period_and_bind_after_it() {
    let mut ns = Ns::new();
    let q = quota_dir(&mut ns, "quota=10M,soft=4M,grace=1m");
    let (f, _) = create(&mut ns, q, "f").unwrap();
    resize(&mut ns, f, 3 * M).unwrap();
    assert_eq!(soft_events(&ns), 0);
    resize(&mut ns, f, 5 * M).unwrap();
    assert_eq!(soft_events(&ns), 1, "越过软限：PolicyViolation（kind=quota_soft）");
    let ev = |(_, b): &(Vec<u8>, Vec<u8>)| decode_v1::<ChangeRecV1>(b).unwrap();
    let pv = ns.tag(T_CHANGELOG).iter().map(ev).find(|e| e.extra.iter().any(|(_, v)| v == "quota_soft")).unwrap();
    let names: Vec<&str> = pv.extra.iter().map(|(k, _)| k.as_str()).collect();
    assert_eq!(names, ["kind", "dir", "class", "used", "soft", "grace"], "金样：extra 的字段名与次序（M10 审查）");
    assert!(ns.kv.0.contains_key(&keys::quota_state(q, keys::QUOTA_CLASS_TOTAL)), "QUOTA_STATE 记下起点");
    assert!(resize(&mut ns, f, 9 * M).is_ok(), "宽限期内按硬限");
    assert_eq!(soft_events(&ns), 1, "已经在计时：不再报");
    ns.now += 61;
    assert_eq!(resize(&mut ns, f, 9 * M + 1), Err(ErrCode::EDQUOT), "宽限期过后按软限拒绝增长");
    assert_eq!(create(&mut ns, q, "g").map(|(_, e)| e), Ok(0), "额度为 0（新文件照样能建：文件数没限制）");
    resize(&mut ns, f, 3 * M).unwrap();
    resize(&mut ns, f, 5 * M).unwrap();
    assert_eq!(soft_events(&ns), 2, "回到软限以内之后再越过：重新计时");
}

#[test]
fn stacked_quotas_classes_renames_and_orphans() {
    let mut ns = Ns::new();
    let q = quota_dir(&mut ns, "quota=4M");
    let sub = ns.mkdir(&OWNER, q, "sub").unwrap();
    set(&mut ns, "/q/sub", "inherit=false,quota=100M").unwrap();
    let (f, end) = create(&mut ns, sub, "f").unwrap();
    assert_eq!(end, 4 * M, "上级的 4M 照样管着（inherit=false 挡不住 Stacked 的配额）");
    assert_eq!(resize(&mut ns, f, 5 * M), Err(ErrCode::EDQUOT));
    let e = set(&mut ns, "/q", "quota.gold=1G").unwrap_err();
    assert!(e.code == ErrCode::EINVAL && e.msg.contains("gold"), "{e:?}");
    set(&mut ns, "/q", "quota.default=2M").unwrap();
    assert_eq!(resize(&mut ns, f, 3 * M), Err(ErrCode::EDQUOT), "按类的配额只计这个类：default 类 2M");
    let big = ns.create(&OWNER, ROOT_INO, "big").unwrap();
    ns.set_size(&OWNER, big, 3 * M).unwrap();
    let mv = |to: &str| MetaOp::Rename {
        parent: ROOT_INO,
        name: b"big".to_vec(),
        new_parent: q,
        new_name: to.into(),
        flags: 0,
    };
    assert_eq!(ns.op(&OWNER, mv("big")), Err(ErrCode::EDQUOT), "跨 scope 的 rename：新祖先链一侧精确检查");
    ns.set_size(&OWNER, big, M).unwrap();
    assert!(ns.op(&OWNER, mv("big")).is_ok());
    let u = ns.view(&ROOT, |v, c| wmfs_core::meta::quota::usage(v, c, "/q")).unwrap();
    assert_eq!((u.quotas.len(), u.quotas[0].used_bytes), (2, M), "/q 的总量与 default 类两个配额");
    ns.op(&OWNER, MetaOp::Unlink { parent: q, name: b"big".to_vec() }).unwrap();
    let MetaReply::Opened(o) = ns.read(&ROOT, MetaOp::Open { ino: big, flags: O_RDWR }).unwrap() else { panic!() };
    assert_eq!(o.quota_end, u64::MAX, "删除后仍打开的文件（孤儿）不计");
}

#[test]
fn acl_default_is_inherited_without_the_umask() {
    let mut ns = Ns::new();
    let d = ns.mkdir(&OWNER, ROOT_INO, "a").unwrap();
    set(&mut ns, "/a", "acl.default=u::rwx,g::r-x,o::---,u:1001:rwx").unwrap();
    let op = MetaOp::Create { parent: d, name: b"f".to_vec(), mode: 0o666, umask: 0o077, excl: true };
    let MetaReply::Opened(o) = ns.op(&OWNER, op).unwrap() else { panic!() };
    assert_eq!(o.attr.mode, 0o660, "按 ACL 与请求的 mode 求交，不用 umask（组位来自 mask）");
    let MetaReply::Xattr { value } =
        ns.read(&ROOT, MetaOp::GetXattr { ino: o.attr.ino, name: b"system.posix_acl_access".to_vec() }).unwrap()
    else {
        panic!()
    };
    assert_eq!(acl::to_text(&acl::parse(&value).unwrap()), "u::rw-,u:1001:rwx,g::r-x,m::rw-,o::---");
    let sub = ns.mkdir(&OWNER, d, "sub").unwrap();
    assert!(
        ns.read(&ROOT, MetaOp::GetXattr { ino: sub, name: b"system.posix_acl_default".to_vec() }).is_ok(),
        "新目录同时带上默认 ACL"
    );
    let g = ns.read(&ROOT, MetaOp::Lookup { parent: d, name: b"f".to_vec() });
    assert!(g.is_ok());
    let r = ns.read(&user(1001, 1001), MetaOp::Open { ino: o.attr.ino, flags: O_RDWR });
    assert!(r.is_ok(), "命名用户 1001 经 ACL 能读写");
}

#[test]
fn path_caps_confine_a_key_to_its_subtree() {
    let mut ns = Ns::new();
    let home = 16;
    let q = ns.mkdir(&OWNER, home, "q").unwrap();
    let private = ns.mkdir(&OWNER, home, "private").unwrap();
    ns.create(&OWNER, private, "s").unwrap();
    ns.caps = vec![(q, false)];
    let resolve = |ns: &mut Ns, p: &str| ns.read(&OWNER, MetaOp::ResolvePath { path: p.as_bytes().to_vec() });
    assert!(resolve(&mut ns, "/home/q").is_ok(), "上级目录可以查名字：走到子树里去");
    assert!(ns.read(&OWNER, MetaOp::GetAttr { ino: ROOT_INO }).is_ok(), "上级目录可以取属性（挂载时的 GETATTR(/)）");
    let ls = |ino| MetaOp::ReadDir { ino, after: vec![], limit: 10, plus: false };
    assert_eq!(ns.read(&OWNER, ls(ROOT_INO)), Err(ErrCode::EACCES), "上级目录不能列");
    assert!(ns.read(&OWNER, ls(q)).is_ok());
    assert_eq!(ns.read(&OWNER, ls(private)), Err(ErrCode::EACCES), "子树外");
    assert_eq!(ns.read(&OWNER, MetaOp::GetAttr { ino: private }), Err(ErrCode::EACCES), "子树外的目录不是上级目录");
    assert_eq!(resolve(&mut ns, "/home/private/s"), Err(ErrCode::EACCES));
    assert_eq!(ns.create(&OWNER, home, "x"), Err(ErrCode::EACCES), "上级目录不能改");
    assert!(ns.create(&OWNER, q, "x").is_ok());
    ns.caps = vec![(q, true)];
    assert_eq!(ns.create(&OWNER, q, "y"), Err(ErrCode::EROFS), "只读的根上写：EROFS");
    assert!(ns.read(&OWNER, ls(q)).is_ok());
    ns.caps = vec![];
    let mv =
        MetaOp::Rename { parent: home, name: b"q".to_vec(), new_parent: ROOT_INO, new_name: b"q2".to_vec(), flags: 0 };
    ns.op(&OWNER, mv).unwrap();
    ns.caps = vec![(q, false)];
    assert!(resolve(&mut ns, "/q2").is_ok(), "目录被 rename 之后，限制仍跟着它");
}

fn has_state(ns: &Ns, dir: u64, class: u16) -> bool {
    ns.kv.0.contains_key(&keys::quota_state(dir, class))
}

/// M10 审查：同一个目录上的 `quota` 与 `quota.<类>` 各自计时（插件只报第一个越过软限的 scope，本体对每个越过的都记起点）。
#[test]
fn a_total_and_a_class_quota_on_one_dir_time_their_soft_limits_separately() {
    let mut ns = Ns::new();
    let q = quota_dir(&mut ns, "quota=100M,soft=90M,quota.default=10M,soft=4M,grace=1m");
    let (f, _) = create(&mut ns, q, "f").unwrap();
    resize(&mut ns, f, 5 * M).unwrap();
    assert_eq!(soft_events(&ns), 1, "越过按类的软限：一条事件");
    assert!(
        has_state(&ns, q, 1) && !has_state(&ns, q, keys::QUOTA_CLASS_TOTAL),
        "起点记在按类的那一项（default 类 = 1）"
    );
    resize(&mut ns, f, 6 * M).unwrap();
    assert_eq!(soft_events(&ns), 1, "已经在计时：不再报");
    ns.now += 61;
    assert_eq!(resize(&mut ns, f, 7 * M), Err(ErrCode::EDQUOT), "宽限期过了：按类的软限拒绝增长");
}

/// M10 审查（3.8 变更通知 ⑤）：设配额时已经超了软限 → 立即开始计时；unset 删掉起点，之后重设的配额从头计时（不按很久以前的起点）。
#[test]
fn setting_a_quota_restarts_its_grace_period_and_unset_forgets_it() {
    let mut ns = Ns::new();
    let q = ns.mkdir(&OWNER, ROOT_INO, "q").unwrap();
    set(&mut ns, "/q", "inline=0").unwrap();
    let (f, _) = create(&mut ns, q, "f").unwrap();
    resize(&mut ns, f, 5 * M).unwrap();
    set(&mut ns, "/q", "quota=10M,soft=4M,grace=1m").unwrap();
    assert_eq!(soft_events(&ns), 1, "设上的时候已经超了软限：立即一条事件");
    assert!(has_state(&ns, q, keys::QUOTA_CLASS_TOTAL), "并记下起点");
    ns.txn(&ROOT, |v, c| attr_ops::unset(v, c, "/q", "quota")).unwrap();
    assert!(!has_state(&ns, q, keys::QUOTA_CLASS_TOTAL), "unset：起点一并删掉");
    ns.now += 3600;
    set(&mut ns, "/q", "quota=10M,soft=4M,grace=10m").unwrap();
    assert_eq!(soft_events(&ns), 2, "重设：从现在开始计时");
    assert!(resize(&mut ns, f, 6 * M).is_ok(), "新的宽限期内按硬限（不是一小时前的起点）");
    set(&mut ns, "/q", "quota=10M,soft=8M").unwrap();
    assert!(!has_state(&ns, q, keys::QUOTA_CLASS_TOTAL), "新的软限没超：旧的起点作废");
    ns.txn(&ROOT, |v, c| attr_ops::unset(v, c, "/q", "inline")).unwrap();
    set(&mut ns, "/q", "quota=10M,soft=4M").unwrap();
    ns.op(&OWNER, MetaOp::Unlink { parent: q, name: b"f".to_vec() }).unwrap();
    ns.op(&OWNER, MetaOp::Rmdir { parent: ROOT_INO, name: b"q".to_vec() }).unwrap();
    assert!(!has_state(&ns, q, keys::QUOTA_CLASS_TOTAL), "删掉目录：它的起点一并删掉");
}

/// M10 审查：跨 scope 的 rename 覆盖目标时先扣掉被覆盖的；RENAME_EXCHANGE 按对调之后的净增量检查。
#[test]
fn renames_into_a_full_quota_count_what_leaves_it() {
    let mut ns = Ns::new();
    let q = quota_dir(&mut ns, "quota=4M");
    let (big, _) = create(&mut ns, q, "big").unwrap();
    resize(&mut ns, big, 3 * M).unwrap();
    let small = ns.create(&OWNER, ROOT_INO, "small").unwrap();
    ns.set_size(&OWNER, small, 2 * M).unwrap();
    let mv = |name: &str, flags| MetaOp::Rename {
        parent: ROOT_INO,
        name: name.into(),
        new_parent: q,
        new_name: b"big".to_vec(),
        flags,
    };
    assert!(ns.op(&OWNER, mv("small", 0)).is_ok(), "2 MiB 覆盖 3 MiB：覆盖之后是 2 MiB，装得下");
    let used =
        |ns: &mut Ns| ns.view(&ROOT, |v, c| wmfs_core::meta::quota::usage(v, c, "/q")).unwrap().quotas[0].used_bytes;
    assert_eq!(used(&mut ns), 2 * M);
    let other = ns.create(&OWNER, ROOT_INO, "other").unwrap();
    ns.set_size(&OWNER, other, M).unwrap();
    assert!(ns.op(&OWNER, mv("other", RENAME_EXCHANGE)).is_ok(), "对调：进来 1 MiB、出去 2 MiB");
    assert_eq!(used(&mut ns), M);
    ns.set_size(&OWNER, small, 5 * M).unwrap(); // small 对调出来了：现在是根目录下的 other
    assert_eq!(ns.op(&OWNER, mv("other", RENAME_EXCHANGE)), Err(ErrCode::EDQUOT), "再对调回去：净增 4 MiB，超了");
}

/// M10 审查：有路径限制的 key 不能按 inode 号把子树外的文件硬链接进子树（link 另核对源文件，`ns::create::link`）。
#[test]
fn path_caps_also_check_the_source_of_a_link() {
    let mut ns = Ns::new();
    let home = 16;
    let q = ns.mkdir(&OWNER, home, "q").unwrap();
    let secret = ns.create(&OWNER, home, "secret").unwrap();
    let mine = ns.create(&OWNER, q, "mine").unwrap();
    ns.caps = vec![(q, false)];
    let link = |ino, name: &str| MetaOp::Link { ino, new_parent: q, new_name: name.into() };
    assert_eq!(ns.op(&OWNER, link(secret, "s")), Err(ErrCode::EACCES), "子树外的文件");
    assert!(ns.op(&OWNER, link(mine, "m2")).is_ok(), "子树里的照常");
    ns.caps = vec![];
    assert!(ns.op(&OWNER, link(secret, "s")).is_ok(), "没有路径限制的 key 照常");
}

/// ADR-0041 第 1 条（审查 #45）：硬链接不跨配额范围（同 XFS 的项目配额）——link 进配额范围不同的目录、有多个链接的文件跨范围
/// rename（含对调的两边）→ `EXDEV`，用量不变；同一个范围里照常；只有一个链接的文件照旧可以跨范围 rename（精确检查）。
#[test]
fn hard_links_never_span_two_quota_ranges() {
    let mut ns = Ns::new();
    let q = quota_dir(&mut ns, "quota=4M,quantum=1M");
    let big = ns.create(&OWNER, ROOT_INO, "big").unwrap();
    ns.set_size(&OWNER, big, 30 * M).unwrap();
    let link = |ino, new_parent, name: &str| MetaOp::Link { ino, new_parent, new_name: name.into() };
    let mv = |parent, name: &str, new_parent, new_name: &str, flags| MetaOp::Rename {
        parent,
        name: name.into(),
        new_parent,
        new_name: new_name.into(),
        flags,
    };
    let used =
        |ns: &mut Ns| ns.view(&ROOT, |v, c| wmfs_core::meta::quota::usage(v, c, "/q")).unwrap().quotas[0].used_bytes;
    assert_eq!(ns.op(&OWNER, link(big, q, "big")), Err(ErrCode::EXDEV), "审查的复现：30 MiB 的文件链接进 4 MiB 的配额");
    assert_eq!(used(&mut ns), 0, "用量不变");
    assert!(ns.op(&OWNER, link(big, ROOT_INO, "big2")).is_ok(), "同一个范围（都没有配额）里照常");
    let (f, _) = create(&mut ns, q, "f").unwrap();
    let sub = ns.mkdir(&OWNER, q, "sub").unwrap();
    assert!(ns.op(&OWNER, link(f, sub, "f2")).is_ok(), "同一个配额范围里的子目录照常");
    assert_eq!(ns.op(&OWNER, mv(ROOT_INO, "big2", q, "big", 0)), Err(ErrCode::EXDEV), "两个链接的文件进 /q");
    assert_eq!(ns.op(&OWNER, mv(sub, "f2", ROOT_INO, "f2", 0)), Err(ErrCode::EXDEV), "两个链接的文件出 /q");
    assert!(ns.op(&OWNER, mv(sub, "f2", q, "f3", 0)).is_ok(), "同一个范围里照常");
    let small = ns.create(&OWNER, ROOT_INO, "small").unwrap();
    ns.set_size(&OWNER, small, M).unwrap();
    assert_eq!(ns.op(&OWNER, mv(ROOT_INO, "big", q, "f", RENAME_EXCHANGE)), Err(ErrCode::EXDEV), "对调：根目录这边");
    assert_eq!(ns.op(&OWNER, mv(ROOT_INO, "small", q, "f", RENAME_EXCHANGE)), Err(ErrCode::EXDEV), "对调：/q 这边");
    assert_eq!(used(&mut ns), 0);
    assert!(ns.op(&OWNER, mv(ROOT_INO, "small", q, "small", 0)).is_ok(), "一个链接：跨范围照旧（精确检查）");
    assert_eq!(used(&mut ns), M);
}

/// ADR-0041 第 8 条 ②：设配额键时已经在计时（设置之前就超了旧的软限）→ 保留原起点、不写新事件——重放同一份配置不把宽限期一再
/// 推后；改 `grace=` 对正在计时的起点立即生效。
#[test]
fn replaying_a_quota_keeps_its_running_grace_period() {
    let mut ns = Ns::new();
    let q = quota_dir(&mut ns, "quota=10M,soft=4M,grace=1m");
    let (f, _) = create(&mut ns, q, "f").unwrap();
    resize(&mut ns, f, 5 * M).unwrap();
    assert_eq!(soft_events(&ns), 1, "越过软限：开始计时");
    ns.now += 50;
    set(&mut ns, "/q", "inline=0,quota=10M,soft=4M,grace=1m").unwrap();
    assert_eq!(soft_events(&ns), 1, "重放同一份配置：不写新事件");
    ns.now += 15;
    assert_eq!(resize(&mut ns, f, 6 * M), Err(ErrCode::EDQUOT), "宽限期从第一次越过软限时算起，已经过了");
    set(&mut ns, "/q", "quota=10M,soft=4M,grace=10m").unwrap();
    assert_eq!(soft_events(&ns), 1);
    assert!(resize(&mut ns, f, 6 * M).is_ok(), "改 grace= 立即生效：起点不变，按新的宽限期算");
}
