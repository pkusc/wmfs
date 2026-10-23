//! 继承解析与缓存的单元测试（一个 BTreeMap 当 KV）：最近的祖先值、`inherit=false`、来源、默认副本数取存储类的、按 attr_epoch
//! 失效、`clear` 丢掉按"没能提交的批次"算出来的项。

use std::collections::BTreeMap;

use super::*;
use crate::init::default_class;
use crate::meta::schema::records::{KIND_DIR, encode_v1};

struct Kv(BTreeMap<Vec<u8>, Vec<u8>>);

impl KvRead for Kv {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ErrCode> {
        Ok(self.0.get(key).cloned())
    }
}

impl Kv {
    fn put<T: serde::Serialize>(&mut self, k: Vec<u8>, v: &T) {
        self.0.insert(k, encode_v1(v));
    }
    fn attrs(&mut self, dir: u64, kv: &[(&str, &str)]) {
        let entries = kv.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        self.put(keys::dirattr(dir), &AttrMapV1 { entries });
    }
    fn epoch(&mut self, e: u64) {
        self.put(keys::global(G_ATTR_EPOCH), &e);
    }
}

fn dir(parent: u64) -> InodeV1 {
    InodeV1 {
        kind: KIND_DIR,
        mode: 0o755,
        uid: 0,
        gid: 0,
        nlink: 2,
        size: 0,
        atime: Default::default(),
        mtime: Default::default(),
        ctime: Default::default(),
        btime: Default::default(),
        rdev: 0,
        parent,
        name: b"d".to_vec(),
        extra_links: vec![],
        flags: 0,
        change_id: 1,
        class_id: 0,
        layout: None,
        pending_epoch: 0,
        max_epoch: 0,
        trash_expire_ms: 0,
        symlink: vec![],
    }
}

/// 根 1 → 16 → 17；default 类 3 副本；attr_epoch 1。
fn tree() -> Kv {
    let mut kv = Kv(BTreeMap::new());
    kv.epoch(1);
    kv.put(keys::class_name("default"), &ClassIdV1 { id: 1 });
    kv.put(keys::class(1), &default_class(3));
    kv.put(keys::inode(ROOT_INO), &dir(0));
    kv.put(keys::inode(16), &dir(ROOT_INO));
    kv.put(keys::inode(17), &dir(16));
    kv
}

#[test]
fn nearest_ancestor_wins_and_sources_are_recorded() {
    let mut kv = tree();
    kv.attrs(ROOT_INO, &[("chunk", "65536"), ("stripe", "4")]);
    kv.attrs(16, &[("chunk", "1048576"), ("consistency", "cto,attr_ttl=3000")]);
    let r = AttrResolver::default();
    let e = r.resolve(&kv, 17).unwrap();
    assert_eq!((e.chunk, e.stripe, e.attr_ttl_ms, e.replicas), (1 << 20, 4, 3000, 3), "副本数默认取存储类的");
    assert_eq!((e.source("chunk"), e.source("stripe"), e.source("consistency"), e.source("replicas")), (16, 1, 16, 0));
    assert_eq!(*r.ancestors(&kv, 17).unwrap(), vec![17, 16, ROOT_INO]);
    assert_eq!(r.resolve(&kv, 99), Err(ErrCode::ENOENT));
}

#[test]
fn inherit_false_stops_inherit_keys_above_it() {
    let mut kv = tree();
    kv.attrs(ROOT_INO, &[("replicas", "2")]);
    kv.attrs(16, &[("chunk", "1048576"), ("inherit", "false")]);
    let e = AttrResolver::default().resolve(&kv, 17).unwrap();
    assert_eq!((e.chunk, e.source("chunk")), (1 << 20, 16), "inherit=false 的目录自己的值仍生效");
    assert_eq!((e.replicas, e.source("replicas")), (3, 0), "不再向上取：/ 上的 replicas=2 被挡住，回到默认值");
}

#[test]
fn cache_is_tagged_with_attr_epoch_and_clear_drops_uncommitted_results() {
    let mut kv = tree();
    kv.attrs(16, &[("chunk", "1048576")]);
    let r = AttrResolver::default();
    assert_eq!(r.resolve(&kv, 17).unwrap().chunk, 1 << 20);
    kv.attrs(16, &[("chunk", "65536")]);
    assert_eq!(r.resolve(&kv, 17).unwrap().chunk, 1 << 20, "attr_epoch 没变：用缓存（改属性的批次一定会让它加一）");
    kv.epoch(2);
    assert_eq!(r.resolve(&kv, 17).unwrap().chunk, 65536, "attr_epoch 变了：整个缓存作废");
    // 一个没能提交的批次：它的覆盖层里 attr_epoch 已经是 3、属性是 8M——按它算出的项必须在批次失败时丢掉
    let mut overlay = Kv(kv.0.clone());
    overlay.attrs(16, &[("chunk", "8388608")]);
    overlay.epoch(3);
    assert_eq!(r.resolve(&overlay, 17).unwrap().chunk, 8 << 20);
    r.clear();
    // 之后真正提交的是另一个修改，attr_epoch 也是 3
    kv.attrs(16, &[("chunk", "2097152")]);
    kv.epoch(3);
    assert_eq!(r.resolve(&kv, 17).unwrap().chunk, 2 << 20, "clear 之后不会拿到没提交的批次算出来的值");
}

#[test]
fn deep_trees_resolve_and_broken_parent_pointers_stop() {
    let mut kv = tree();
    kv.attrs(16, &[("chunk", "1048576")]);
    let mut parent = 17;
    for ino in 100..5100u64 {
        kv.put(keys::inode(ino), &dir(parent)); // 比 M6 的上限 4096 层还深：规格没有深度限制
        parent = ino;
    }
    let r = AttrResolver::default();
    assert_eq!(r.resolve(&kv, parent).unwrap().chunk, 1 << 20);
    assert_eq!(r.ancestors(&kv, parent).unwrap().len(), 5003);
    assert_eq!(r.resolve(&kv, parent).unwrap().source("chunk"), 16, "第二次：命中缓存（长链按长度计入缓存大小）");
    kv.put(keys::inode(16), &dir(17)); // 损坏：16 ↔ 17 成环
    kv.epoch(2);
    assert_eq!(r.resolve(&kv, 17), Err(ErrCode::INTERNAL), "走满 MAX_DEPTH 步还没到根就停下，不会一直走");
}
