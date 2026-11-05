//! path-caps 的单元测试：manifest、没有限制时弃权、对象本身或祖先是允许的根 → 放行、只读的根上写 → EROFS（另一个可写的根覆盖它时
//! 放行）、子树外 → EACCES、目录项操作看父目录、经句柄的写与 open 相同、判定计数。

use std::sync::Arc;

use wmfs_core::base::types::{Principal, Role};
use wmfs_core::hooks::manifest::check_manifest_toml;
use wmfs_core::hooks::traits::{ConnIdentity, InodeView};

use super::*;

fn who(caps: &[(u64, bool)]) -> Principal {
    let conn = Arc::new(ConnIdentity {
        key_id: 3,
        role: Role::Client,
        tenant: 0,
        uid_ranges: vec![(0, u32::MAX)],
        root_squash: false,
        path_caps: caps.to_vec(),
    });
    Principal { uid: 1000, gid: 1000, groups: vec![], tenant: 0, job_id: 0, conn }
}

fn node(ino: u64) -> InodeView<'static> {
    InodeView { ino, kind: 2, mode: 0o777, uid: 1000, gid: 1000, acl_access: None }
}

fn check(w: &Principal, op: AuthzOp, t: Option<u64>, p: Option<u64>, anc: &[u64]) -> Decision {
    let (t, p) = (t.map(node), p.map(node));
    let i = AuthzInput { who: w, op, target: t.as_ref(), parent: p.as_ref(), victim: None, ancestors: anc };
    PathCaps::new().check(&i)
}

const ALLOW: Decision = Decision::Allow;

#[test]
fn manifest_toml_matches_code() {
    check_manifest_toml(&MANIFEST, include_str!("manifest.toml")).unwrap();
}

#[test]
fn keys_are_confined_to_their_subtrees() {
    let q = who(&[(20, false)]); // 只能进 /home/q（inode 20）
    assert_eq!(check(&who(&[]), AuthzOp::ReadDir, Some(30), None, &[1]), Decision::Abstain, "没有限制：弃权");
    assert_eq!(check(&q, AuthzOp::ReadDir, Some(20), None, &[1, 16]), ALLOW, "允许的根本身");
    assert_eq!(
        check(&q, AuthzOp::Open { read: true, write: true }, Some(25), None, &[1, 16, 20]),
        ALLOW,
        "根下面的文件"
    );
    assert_eq!(check(&q, AuthzOp::Create, None, Some(20), &[1, 16]), ALLOW, "目录项操作看父目录");
    let out = Decision::Deny(ErrCode::EACCES);
    assert_eq!(check(&q, AuthzOp::ReadDir, Some(30), None, &[1, 16]), out, "/home/private：子树外");
    assert_eq!(check(&q, AuthzOp::Create, None, Some(16), &[1]), out, "在 /home 里建：子树外");
    assert_eq!(check(&q, AuthzOp::Handle { read: false, write: true }, Some(31), None, &[1, 16]), out, "经句柄也一样");
    assert_eq!(check(&q, AuthzOp::Admin, None, None, &[]), Decision::Abstain, "管理操作不归它管");
}

#[test]
fn read_only_roots_reject_writes_with_erofs() {
    let ro = who(&[(40, true)]);
    let erofs = Decision::Deny(ErrCode::EROFS);
    assert_eq!(check(&ro, AuthzOp::Open { read: true, write: false }, Some(41), None, &[1, 40]), ALLOW);
    assert_eq!(check(&ro, AuthzOp::Open { read: true, write: true }, Some(41), None, &[1, 40]), erofs);
    assert_eq!(check(&ro, AuthzOp::Unlink, None, Some(40), &[1]), erofs, "删也是写");
    assert_eq!(check(&ro, AuthzOp::Handle { read: false, write: true }, Some(41), None, &[1, 40]), erofs);
    assert_eq!(check(&ro, AuthzOp::Lookup, None, Some(40), &[1]), ALLOW, "查名字是读");
    let both = who(&[(40, true), (42, false)]);
    let w = AuthzOp::Open { read: false, write: true };
    assert_eq!(check(&both, w, Some(43), None, &[1, 40, 42]), ALLOW, "落在一个可写的根下：可写");
}

#[test]
fn decisions_are_counted_by_result() {
    let c = |r| metrics::counter("wmfs_path_caps_decisions_total", "", &[("result", r)]).get();
    let (a, d, ab) = (c("allow"), c("deny"), c("abstain"));
    let q = who(&[(20, false)]);
    check(&q, AuthzOp::ReadDir, Some(20), None, &[1]);
    check(&q, AuthzOp::ReadDir, Some(30), None, &[1]);
    check(&who(&[]), AuthzOp::ReadDir, Some(30), None, &[1]);
    assert!(c("allow") > a && c("deny") > d && c("abstain") > ab);
}
