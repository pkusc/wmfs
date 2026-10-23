//! M7 的目录属性（design.md 3.8、3.1.5、3.10、第 5 章，ADR-0026、ADR-0016），在内存 KV 视图上直接跑 `attr_ops` 与命名空间闭包：
//! 继承、`inherit=false`、来源、校验（只能挂在目录上、存储类存在、"必须有插件消费"）、成功但警告（单 target 上 replicas=3、
//! stripe=2）、changelog 的 AttrSet/AttrUnset、attr_epoch 与 RECONCILE_Q、新文件按属性建 layout、`AttrOut.ttl_ms` = attr_ttl、
//! 孤儿的父目录删掉之后用根目录的属性、声明 vs 实际与递归用量。

use std::sync::Arc;

use wmfs_core::api::ops::op_code;
use wmfs_core::api::types::{AttrGetReply, AttrSetReply};
use wmfs_core::base::{ErrCode, Failed};
use wmfs_core::hooks::pipeline::Pipeline;
use wmfs_core::hooks::registry::PluginSet;
use wmfs_core::meta::attr_ops;
use wmfs_core::meta::attr_registry::split;
use wmfs_core::meta::schema::keys::{self, G_ATTR_EPOCH, ROOT_INO, T_CHANGELOG, T_RECONCILE_Q};
use wmfs_core::meta::schema::records::{ChangeRecV1, F_INLINE, ReconcileItemV1, decode_v1};
use wmfs_core::proto::meta::{MetaOp, MetaReply};
use wmfs_plugins::posix_mode::PosixMode;
use wmfs_testkit::harness::ns::{Ns, OWNER, ROOT};

fn set(ns: &mut Ns, path: &str, spec: &str) -> Result<AttrSetReply, Failed> {
    let items = split(spec)?;
    ns.txn(&ROOT, |v, c| attr_ops::set(v, c, path, &items))
}

fn get(ns: &mut Ns, path: &str) -> AttrGetReply {
    ns.view(&ROOT, |v, c| attr_ops::get(v, c, path, true)).unwrap_or_else(|e| panic!("get {path}：{e:?}"))
}

/// (值, 来源)
fn at(r: &AttrGetReply, key: &str) -> (String, String) {
    let e = r.attrs.iter().find(|e| e.key == key).unwrap_or_else(|| panic!("没有 {key}"));
    (e.value.clone(), e.source.clone())
}

fn pair(v: &str, s: &str) -> (String, String) {
    (v.into(), s.into())
}

fn epoch(ns: &Ns) -> u64 {
    decode_v1(&ns.kv.0[&keys::global(G_ATTR_EPOCH)]).unwrap()
}

#[test]
fn inheritance_sources_and_inherit_false() {
    let mut ns = Ns::new();
    let p = ns.mkdir(&OWNER, ROOT_INO, "proj").unwrap();
    let a = ns.mkdir(&OWNER, p, "a").unwrap();
    ns.mkdir(&OWNER, a, "b").unwrap();
    let w = set(&mut ns, "/proj", "chunk=1M,stripe=2").unwrap();
    assert_eq!(w.set.get("chunk").map(String::as_str), Some("1M"), "回复的是规范化之后的显示形式");
    assert!(w.warnings.iter().any(|x| x.contains("只有 1 条可用的链，新文件实际只有 1 个条带")), "{:?}", w.warnings);
    set(&mut ns, "/proj/a", "chunk=8M").unwrap();
    let g = get(&mut ns, "/proj/a/b");
    assert_eq!((at(&g, "chunk"), at(&g, "stripe")), (pair("8M", "/proj/a"), pair("2", "/proj")), "最近的祖先值");
    assert_eq!(at(&g, "replicas"), pair("3", "(默认值)"), "副本数默认 = 存储类的");
    assert_eq!(at(&g, "consistency"), pair("cto,attr_ttl=1s", "(默认值)"));
    assert!(g.semantics.contains("durability=replicas:声明3/实际1"), "{}", g.semantics);
    assert!(g.warnings.iter().any(|x| x.contains("只有 1 个故障域")), "{:?}", g.warnings);
    let u = ns.txn(&ROOT, |v, c| attr_ops::unset(v, c, "/proj/a", "chunk")).unwrap();
    assert_eq!(u.old, "8M");
    assert_eq!(at(&get(&mut ns, "/proj/a/b"), "chunk"), pair("1M", "/proj"));
    set(&mut ns, "/proj/a", "inherit=false,inline=0").unwrap();
    let g = get(&mut ns, "/proj/a/b");
    assert_eq!(
        (at(&g, "chunk"), at(&g, "inline")),
        (pair("4M", "(默认值)"), pair("0", "/proj/a")),
        "inherit=false 挡住上面"
    );
    assert_eq!(at(&g, "inherit"), pair("true", "(默认值)"), "inherit 只作用于设置它的目录");
    assert_eq!(at(&get(&mut ns, "/proj/a"), "inherit"), pair("false", "/proj/a"));
    ns.create(&OWNER, p, "f").unwrap();
    let g = get(&mut ns, "/proj/f");
    assert_eq!((g.dir.as_str(), at(&g, "chunk")), ("/proj", pair("1M", "/proj")), "文件看它所在目录的生效属性");
    let paths: Vec<String> = get(&mut ns, "/").subtree.into_iter().map(|d| d.path).collect();
    assert_eq!(paths, vec!["/home", "/scratch", "/proj", "/proj/a"], "-r：子树里显式设置了属性的目录");
}

#[test]
fn validation_consumers_and_warnings() {
    let mut ns = Ns::new();
    let p = ns.mkdir(&OWNER, ROOT_INO, "proj").unwrap();
    ns.create(&OWNER, p, "f").unwrap();
    let e = set(&mut ns, "/proj/f", "chunk=1M").unwrap_err();
    assert!(e.code == ErrCode::EINVAL && e.msg.contains("只能挂在目录上"), "{e:?}");
    let e = set(&mut ns, "/proj", "class=gold").unwrap_err();
    assert!(e.code == ErrCode::EINVAL && e.msg.contains("存储类 gold 不存在"), "{e:?}");
    let e = set(&mut ns, "/nope", "chunk=1M").unwrap_err();
    assert!(e.code == ErrCode::ENOENT && e.msg.contains("/nope"), "{e:?}");
    // 这条管线里没有 crc32c（transform）：checksum 没有插件消费
    let e = set(&mut ns, "/proj", "checksum=crc32c").unwrap_err();
    assert!(e.code == ErrCode::EINVAL && e.msg.contains("没有已启用的插件消费"), "{e:?}");
    let w = set(&mut ns, "/proj", "replicas=3").unwrap().warnings;
    assert!(w.iter().any(|x| x.contains("当前只有 1 个故障域，新文件实际为 1 副本")), "{w:?}");
    assert!(w.iter().any(|x| x.contains("已有文件在协调器转换完成前")), "{w:?}");
    let e = ns.txn(&ROOT, |v, c| attr_ops::unset(v, c, "/proj", "stripe")).unwrap_err();
    assert_eq!(e.code, ErrCode::ENOENT, "没设置过的键");
    let e = ns.txn(&ROOT, |v, c| attr_ops::unset(v, c, "/proj", "chunck")).unwrap_err();
    assert_eq!((e.code, e.msg.as_str()), (ErrCode::EINVAL, "未知属性 chunck"));
    let e = ns.txn(&ROOT, |v, c| attr_ops::unset(v, c, "/proj", "quota")).unwrap_err();
    assert!(e.code == ErrCode::EOPNOTSUPP && e.msg.contains("M10"), "与 set 的说法相同：{e:?}");
    // 没有 label-match（placement）的管线：stripe 没有消费者，chunk（core）照常
    let mut set_ = PluginSet::new(&[]);
    set_.authz(Arc::new(PosixMode::new()));
    let mut bare = Ns::with_pipeline(Pipeline::new(set_));
    bare.mkdir(&OWNER, ROOT_INO, "x").unwrap();
    let e = set(&mut bare, "/x", "chunk=1M,stripe=2").unwrap_err();
    assert!(e.msg.contains("stripe 没有已启用的插件消费") && e.msg.contains("label-match"), "{e:?}");
    assert!(set(&mut bare, "/x", "chunk=1M").is_ok(), "core 消费的键");
}

#[test]
fn changes_are_logged_and_bump_attr_epoch() {
    let mut ns = Ns::new();
    let p = ns.mkdir(&OWNER, ROOT_INO, "proj").unwrap();
    let e0 = epoch(&ns);
    set(&mut ns, "/proj", "chunk=1M,replicas=2").unwrap();
    assert_eq!(epoch(&ns), e0 + 1, "属性修改令 attr_epoch + 1");
    ns.txn(&ROOT, |v, c| attr_ops::unset(v, c, "/proj", "chunk")).unwrap();
    let log: Vec<ChangeRecV1> = ns.tag(T_CHANGELOG).iter().map(|(_, b)| decode_v1(b).unwrap()).collect();
    let (set_op, unset_op) = (op_code("AttrSet").unwrap(), op_code("AttrUnset").unwrap());
    assert_eq!((set_op, unset_op), (100, 101), "ADR-0029 的码表");
    let attr: Vec<(u16, Vec<(String, String)>)> =
        log.iter().filter(|r| r.op == set_op || r.op == unset_op).map(|r| (r.op, r.extra.clone())).collect();
    let kv =
        |k: &str, o: &str, n: &str| vec![("key".into(), k.into()), ("old".into(), o.into()), ("new".into(), n.into())];
    assert_eq!(
        attr,
        vec![
            (set_op, kv("chunk", "", "1M")),
            (set_op, kv("replicas", "", "2")),
            (unset_op, vec![("key".into(), "chunk".into()), ("old".into(), "1M".into())])
        ],
        "AttrSet/AttrUnset 各一条"
    );
    assert!(log.iter().filter(|r| r.op == set_op).all(|r| r.ino == p), "记的是目录");
    let q: Vec<ReconcileItemV1> = ns.tag(T_RECONCILE_Q).iter().map(|(_, b)| decode_v1(b).unwrap()).collect();
    assert_eq!(q, vec![ReconcileItemV1 { dir: p, reason: 1 }], "replicas（新数据 + 协调器）写一条；unset chunk 不写");
    let q2 = ns.mkdir(&OWNER, ROOT_INO, "q").unwrap();
    let e1 = epoch(&ns);
    let mv =
        MetaOp::Rename { parent: ROOT_INO, name: b"proj".to_vec(), new_parent: q2, new_name: b"p".to_vec(), flags: 0 };
    ns.op(&OWNER, mv).unwrap();
    assert_eq!(epoch(&ns), e1 + 1, "目录 rename 令 attr_epoch + 1（继承关系变了）");
    let q: Vec<ReconcileItemV1> = ns.tag(T_RECONCILE_Q).iter().map(|(_, b)| decode_v1(b).unwrap()).collect();
    assert_eq!(q.last(), Some(&ReconcileItemV1 { dir: p, reason: 2 }));
    assert_eq!(at(&get(&mut ns, "/q/p"), "replicas"), pair("2", "/q/p"), "移动之后来源跟着目录走");
}

#[test]
fn new_files_follow_the_attributes() {
    let mut ns = Ns::new();
    let p = ns.mkdir(&OWNER, ROOT_INO, "p").unwrap();
    set(&mut ns, "/p", "chunk=64K,inline=0,stripe=4,consistency=cto,attr_ttl=3s").unwrap();
    let op = MetaOp::Create { parent: p, name: b"f".to_vec(), mode: 0o644, umask: 0, excl: true };
    let MetaReply::Opened(o) = ns.op(&OWNER, op).unwrap() else { panic!() };
    let l = o.layout.unwrap();
    assert_eq!(
        (l.chunk_size, l.block_size, l.inline_max, l.chains.len()),
        (65536, 65536, 0, 1),
        "stripe 4 但只有 1 条链"
    );
    assert_eq!((o.attr.flags & F_INLINE, o.attr.ttl_ms), (0, 3000), "inline=0：不 inline；TTL = attr_ttl");
    assert_eq!(ns.attr(p).ttl_ms, 3000, "目录按它自己的生效属性");
    assert_eq!(ns.attr(ROOT_INO).ttl_ms, 1000);
    let MetaReply::Entry { attr } = ns.read(&OWNER, MetaOp::Lookup { parent: p, name: b"f".to_vec() }).unwrap() else {
        panic!()
    };
    assert_eq!(attr.ttl_ms, 3000);
    set(&mut ns, "/p", "replicas=1").unwrap();
    let f2 = ns.create(&OWNER, p, "g").unwrap();
    assert_eq!(ns.inode(f2).unwrap().layout.unwrap().replicas, 1, "新文件按新的副本数选链");
    let MetaReply::Opened(o) = ns.set_size(&OWNER, o.attr.ino, 0).unwrap() else { panic!() };
    assert_eq!(o.layout.unwrap().replicas, 1, "截断到 0 = 新文件：按当前属性重建 layout");
}

#[test]
fn orphans_use_the_root_attributes_once_their_parent_is_gone() {
    let mut ns = Ns::new();
    let p = ns.mkdir(&OWNER, ROOT_INO, "p").unwrap();
    set(&mut ns, "/p", "consistency=cto,attr_ttl=3s").unwrap();
    let q = ns.mkdir(&OWNER, p, "q").unwrap(); // 没有自己的 DIRATTR：删掉它不加 attr_epoch
    let f = ns.create(&OWNER, q, "f").unwrap();
    assert_eq!(ns.attr(f).ttl_ms, 3000, "继承 /p 的 attr_ttl（解析缓存里现在有 /p/q 的项）");
    ns.op(&OWNER, MetaOp::Unlink { parent: q, name: b"f".to_vec() }).unwrap();
    assert_eq!(ns.attr(f).ttl_ms, 3000, "孤儿：主父目录还在，照旧用它的");
    ns.op(&OWNER, MetaOp::Rmdir { parent: p, name: b"q".to_vec() }).unwrap();
    assert_eq!(ns.attr(f).ttl_ms, 1000, "主父目录删掉了：用根目录的——不管缓存里还有没有已删目录的项");
}

#[test]
fn status_reports_declared_and_actual_and_usage_reads_rstat() {
    let mut ns = Ns::new();
    let p = ns.mkdir(&OWNER, ROOT_INO, "p").unwrap();
    set(&mut ns, "/p", "stripe=2").unwrap();
    let s = ns.view(&ROOT, |v, c| attr_ops::status(v, c, "/")).unwrap();
    let w = s.warnings.join("\n");
    assert!(w.contains("目录 /home 声明 3 副本") && w.contains("目录 /scratch 声明 2 副本"), "{w}");
    assert!(w.contains("目录 /p 声明 stripe=2"), "{w}");
    let home = ns.view(&ROOT, |v, c| attr_ops::status(v, c, "/home")).unwrap().warnings;
    let mine = |x: &&String| x.starts_with("目录 /home 声明") || x.starts_with("这个目录：声明 3 副本");
    assert_eq!(home.iter().filter(mine).count(), 1, "目录自己的声明只报一次（另一条是存储类的集群警告）：{home:?}");
    let r = s.attrs.iter().find(|a| a.key == "replicas").unwrap();
    assert_eq!((r.declared.as_str(), r.actual.new_files.as_str(), r.mismatched), ("3", "1", true));
    assert!(s.attrs.iter().filter(|a| a.key != "replicas").all(|a| !a.mismatched), "{:?}", s.attrs);
    assert_eq!(s.attrs.len(), 8, "每个已实现的属性都有声明与实际");
    let f = ns.create(&OWNER, p, "f").unwrap();
    ns.op(&OWNER, MetaOp::Symlink { parent: p, name: b"l".to_vec(), target: b"f".to_vec() }).unwrap();
    ns.set_size(&OWNER, f, 5000).unwrap();
    ns.mkdir(&OWNER, p, "d").unwrap();
    let u = ns.view(&ROOT, |v, c| attr_ops::usage(v, c, "/p")).unwrap();
    assert_eq!((u.bytes, u.files, u.dirs), (5000, 2, 1), "符号链接算文件、0 字节");
    assert_eq!(u.per_class.len(), 1);
    assert_eq!((u.per_class[0].class.as_str(), u.per_class[0].files), ("default", 1));
    let e = ns.view(&ROOT, |v, c| attr_ops::usage(v, c, "/p/f")).unwrap_err();
    assert_eq!(e.code, ErrCode::ENOTDIR);
}
