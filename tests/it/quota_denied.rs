//! 告警 quota_denied 看的 `wmfs_quota_denied_total{dir}`（design.md 4.14、7.3；M14 审查）：客户端写到没有余量时，来要额度的
//! CommitWrite（`want_quota`、长度不变）也计一次拒绝——回复的 `quota_end` = 长度，客户端随后报 `EDQUOT`；以前只有 create、truncate、
//! fallocate 被拒才计数，用户一直撞配额、一直拿到 EDQUOT，告警却不触发。进程级的计数器：单独一个测试程序、一个测试函数。

use wmfs_core::api::metrics;
use wmfs_core::meta::attr_ops;
use wmfs_core::meta::attr_registry::split;
use wmfs_core::meta::schema::keys::ROOT_INO;
use wmfs_core::proto::meta::{MetaOp, MetaReply};
use wmfs_testkit::harness::ns::{Ns, OWNER, ROOT};

const M: u64 = 1 << 20;

/// CommitWrite 到 `size`（`want_quota` = 客户端专门来要额度），回 quota_end。
fn commit(ns: &mut Ns, ino: u64, size: u64, want_quota: bool) -> u64 {
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
    match ns.op(&OWNER, op).unwrap() {
        MetaReply::Committed { quota_end, .. } => quota_end,
        r => panic!("{r:?}"),
    }
}

#[test]
fn asking_for_quota_with_none_left_counts_a_denial() {
    let mut ns = Ns::new();
    let q = ns.mkdir(&OWNER, ROOT_INO, "q").unwrap();
    let items = split("inline=0,quota=2M,quantum=1M").unwrap();
    ns.txn(&ROOT, |v, c| attr_ops::set(v, c, "/q", &items)).unwrap();
    let f = ns.create(&OWNER, q, "f").unwrap();
    let denied = || metrics::counter("wmfs_quota_denied_total", "", &[("dir", &q.to_string())]).get();
    let deny = || metrics::counter("wmfs_quota_decisions_total", "", &[("result", "deny")]).get();
    assert_eq!(commit(&mut ns, f, M, false), 2 * M, "还有余量：额度续上");
    assert_eq!(commit(&mut ns, f, 2 * M, false), 2 * M, "正好用满：额度为 0");
    let (before, deny_before) = (denied(), deny());
    assert_eq!(commit(&mut ns, f, 2 * M, true), 2 * M, "用完了额度来要：长度不变、额度仍为 0（客户端报 EDQUOT）");
    assert_eq!((denied() - before, deny() - deny_before), (1, 1), "这次拒绝计入 wmfs_quota_denied_total{{dir}}");
    let g = ns.create(&OWNER, ROOT_INO, "g").unwrap();
    let before = denied();
    commit(&mut ns, g, 0, true);
    assert_eq!(denied(), before, "不受配额管着的文件：不问插件");
}
