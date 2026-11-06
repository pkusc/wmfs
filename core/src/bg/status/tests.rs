//! status.rs 的单元测试：哪些差别算"要迁移"、加密目录里的 inline 怎么比、逐文件统计怎么填进 status 的回复。

use super::*;
use crate::api::types::{AttrActual, StatusAttr};
use crate::data::format::{CMP_LZ4, CMP_ZSTD};

fn file(class: u16, l: LayoutV1) -> InodeV1 {
    InodeV1 {
        kind: KIND_FILE,
        nlink: 1,
        parent: 1,
        name: b"f".to_vec(),
        class_id: class,
        layout: Some(l),
        ..Default::default()
    }
}

fn layout() -> LayoutV1 {
    LayoutV1 {
        epoch: 1,
        chunk_size: 4 << 20,
        block_size: 64 << 10,
        chains: vec![1],
        replicas: 3,
        codec: 1,
        checksum: 1,
        inline_max: 65536,
        ..Default::default()
    }
}

#[test]
fn only_the_four_reconciled_keys_make_a_file_differ() {
    let mut e = EffectiveAttrs::defaults(1, 3);
    let i = file(1, layout());
    assert!(!differs(&i, &e));
    e.chunk = 1 << 20;
    e.checksum = 2;
    assert!(!differs(&i, &e), "chunk、checksum 不迁移");
    e.compress = CMP_ZSTD;
    e.compress_level = 3;
    assert!(differs(&i, &e), "compress 要迁移");
    let mut l = layout();
    (l.compress, l.compress_level) = (CMP_ZSTD, 3);
    assert!(!differs(&file(1, l.clone()), &e));
    e.compress_level = 5;
    assert!(differs(&file(1, l.clone()), &e), "zstd 的级别也算");
    e.compress = CMP_LZ4;
    e.compress_level = 0;
    l.compress = CMP_LZ4;
    l.compress_level = 7;
    assert!(!differs(&file(1, l), &e), "不是 zstd 时级别不算");
    let e = EffectiveAttrs { encrypt_key: 4, ..EffectiveAttrs::defaults(1, 3) };
    assert!(differs(&i, &e), "encrypt 要迁移");
    assert!(differs(&i, &EffectiveAttrs::defaults(2, 3)), "class 要迁移");
    assert!(differs(&i, &EffectiveAttrs::defaults(1, 2)), "replicas 要迁移");
    assert!(!differs(&InodeV1 { layout: None, ..i }, &e), "没有 layout 的不比");
}

#[test]
fn inline_is_compared_with_what_new_files_actually_get() {
    let enc = EffectiveAttrs { encrypt_key: 4, ..EffectiveAttrs::defaults(1, 3) };
    let converted = file(1, LayoutV1 { encrypt: 1, encrypt_key: 4, inline_max: 0, ..layout() });
    let inline = |i: &InodeV1, e| keys_of(i, e).iter().find(|(k, _)| *k == "inline").unwrap().1;
    assert!(!inline(&converted, &enc), "加密目录里的文件不 inline（阈值 0）：与声明的 64K 不算不一致");
    assert!(inline(&file(1, layout()), &enc), "还没转换的文件（阈值 64K）才算");
    assert!(!inline(&file(1, layout()), &EffectiveAttrs::defaults(1, 3)));
}

#[test]
fn counts_are_filled_into_the_status_reply() {
    let attr = |k: &str| StatusAttr {
        key: k.into(),
        declared: "x".into(),
        actual: AttrActual { new_files: "x".into(), ..Default::default() },
        ..Default::default()
    };
    let mut r = StatusReply {
        attrs: vec![attr("compress"), attr("chunk"), attr("consistency"), attr("replicas")],
        ..Default::default()
    };
    let mut c = Counts { scanned: 30, files: 20, truncated: true, ..Default::default() };
    c.mismatched.insert("compress", 19);
    c.open.insert("compress", 1);
    fill(&mut r, &c);
    let a = &r.attrs[0];
    assert_eq!(
        (a.actual.files, a.actual.mismatched_files, a.actual.open_files, a.actual.migrates),
        (Some(20), Some(19), Some(1), Some(true))
    );
    assert!(a.mismatched);
    let ch = &r.attrs[1];
    assert_eq!(
        (ch.actual.mismatched_files, ch.actual.open_files, ch.actual.migrates, ch.mismatched),
        (Some(0), None, Some(false), false)
    );
    assert_eq!(r.attrs[2].actual.files, None, "没有逐文件统计的键不动");
    assert_eq!(r.attrs[3].actual.short_files, Some(0), "replicas 总有 short_files（M9 起）");
    assert!(r.truncated && r.warnings.iter().any(|w| w.contains("只看了前")), "扫不全要明说");
}

/// 硬链接文件只在主链接那一项计（与递归计数同一个口径）：子树里同一个 inode 的几个名字只数一次。
#[test]
fn a_hard_linked_file_is_counted_once_at_its_primary_link() {
    let e = std::sync::Arc::new(EffectiveAttrs { compress: CMP_LZ4, ..EffectiveAttrs::defaults(1, 3) });
    let i = file(1, layout()); // 主链接：目录 1 里的 f
    let seen = |parent, name: &[u8]| Walked { ino: 20, inode: i.clone(), parent, name: name.to_vec(), eff: e.clone() };
    let (mut c, map, live) = (Counts::default(), ClusterMap::default(), Live::default());
    for w in [seen(7, b"link"), seen(1, b"f"), seen(1, b"g")] {
        count(&mut c, &w, &map, &live);
    }
    assert_eq!((c.files, c.mismatched.get("compress").copied()), (1, Some(1)), "三个名字，只数主链接那一个");
}
