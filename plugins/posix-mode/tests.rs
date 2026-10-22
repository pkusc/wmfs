//! posix-mode 的单元测试：manifest、属主/属组/其他人三组位、附加组、root 放行（带扩展 ACL 的 inode 也放行）、扩展 ACL 弃权、
//! 粘滞位、chmod/chown/时间的属主规则、rename 换父目录要能写被移动的目录、缺了要检查的 inode 时拒绝（失败即关闭）、
//! 判定计数。

use std::sync::Arc;

use wmfs_core::base::types::Role;
use wmfs_core::hooks::manifest::check_manifest_toml;
use wmfs_core::hooks::traits::ConnIdentity;

use super::*;

fn who(uid: u32, gid: u32, groups: &[u32]) -> Principal {
    let conn = Arc::new(ConnIdentity {
        key_id: 2,
        role: Role::Client,
        tenant: 0,
        uid_ranges: vec![(0, u32::MAX)],
        root_squash: false,
        path_caps: vec![],
    });
    Principal { uid, gid, groups: groups.to_vec(), tenant: 0, job_id: 0, conn }
}

fn node(mode: u32, uid: u32, gid: u32) -> InodeView<'static> {
    InodeView { ino: 16, kind: 2, mode, uid, gid, acl_access: None }
}

fn check(w: &Principal, op: AuthzOp, t: Option<&InodeView>, p: Option<&InodeView>, v: Option<&InodeView>) -> Decision {
    let input = AuthzInput { who: w, op, target: t, parent: p, victim: v, ancestors: &[] };
    PosixMode::new().check(&input)
}

const DENY: Decision = Decision::Deny(ErrCode::EACCES);
const EPERM: Decision = Decision::Deny(ErrCode::EPERM);
const ALLOW: Decision = Decision::Allow;

#[test]
fn manifest_toml_matches_code() {
    check_manifest_toml(&MANIFEST, include_str!("manifest.toml")).unwrap();
}

#[test]
fn owner_group_and_other_bits() {
    let d = node(0o750, 1000, 100); // rwx r-x ---
    let mk = AuthzOp::Mkdir;
    assert_eq!(check(&who(1000, 1, &[]), mk, None, Some(&d), None), ALLOW, "属主有 wx");
    assert_eq!(check(&who(2000, 100, &[]), mk, None, Some(&d), None), DENY, "属组只有 r-x：不能建");
    assert_eq!(check(&who(2000, 100, &[]), AuthzOp::Lookup, None, Some(&d), None), ALLOW, "属组有 x");
    assert_eq!(check(&who(2000, 5, &[100]), AuthzOp::ReadDir, Some(&d), None, None), ALLOW, "附加组也算属组");
    assert_eq!(check(&who(2000, 5, &[]), AuthzOp::Lookup, None, Some(&d), None), DENY, "其他人什么都没有");
    assert_eq!(check(&who(0, 0, &[]), mk, None, Some(&node(0, 1, 1)), None), ALLOW, "root 全部放行");
    let f = node(0o640, 1000, 100);
    let open = |r, w| AuthzOp::Open { read: r, write: w };
    assert_eq!(check(&who(2000, 100, &[]), open(true, false), Some(&f), None, None), ALLOW);
    assert_eq!(check(&who(2000, 100, &[]), open(true, true), Some(&f), None, None), DENY, "属组没有 w");
    assert_eq!(check(&who(3000, 3, &[]), AuthzOp::GetAttr, Some(&f), None, None), ALLOW, "getattr 不要权限");
    assert_eq!(check(&who(1000, 1, &[]), AuthzOp::Unlink, None, Some(&d), Some(&f)), ALLOW);
}

#[test]
fn extended_acl_abstains_and_missing_inode_is_denied() {
    let acl = [2u8, 0, 0, 0];
    let d = InodeView { acl_access: Some(&acl), ..node(0o777, 1, 1) };
    assert_eq!(check(&who(2000, 2, &[]), AuthzOp::Create, None, Some(&d), None), Decision::Abstain, "交给 posix-acl");
    assert_eq!(
        check(&who(1, 1, &[]), AuthzOp::ReadDir, Some(&d), None, None),
        Decision::Abstain,
        "属主也交给 posix-acl"
    );
    let root = who(0, 0, &[]);
    for op in [AuthzOp::Lookup, AuthzOp::Create] {
        assert_eq!(check(&root, op, None, Some(&d), None), ALLOW, "root 不受 ACL 限制（CAP_DAC_OVERRIDE）");
    }
    assert_eq!(check(&root, AuthzOp::ReadDir, Some(&d), None, None), ALLOW);
    assert_eq!(check(&who(2000, 2, &[]), AuthzOp::Create, None, None, None), DENY, "core 没给父目录：失败即关闭");
}

#[test]
fn decisions_are_counted_by_result() {
    let c = |r| metrics::counter("wmfs_posix_mode_decisions_total", "", &[("result", r)]).get();
    let (a, d, ab) = (c("allow"), c("deny"), c("abstain"));
    let f = node(0o600, 1000, 1000);
    let rw = AuthzOp::Open { read: true, write: true };
    assert_eq!(check(&who(1000, 1, &[]), rw, Some(&f), None, None), ALLOW, "属主 rw");
    assert_eq!(check(&who(2000, 2, &[]), rw, Some(&f), None, None), DENY, "其他人没有 r、w");
    assert_eq!(check(&who(2000, 2, &[]), AuthzOp::Admin, None, None, None), Decision::Abstain);
    assert!(c("allow") > a && c("deny") > d && c("abstain") > ab, "三种结果各自计数");
}

#[test]
fn sticky_directories() {
    let tmp = node(0o1777, 0, 0); // /scratch 那样
    let mine = node(0o644, 1000, 1000);
    let theirs = node(0o644, 2000, 2000);
    let me = who(1000, 1000, &[]);
    assert_eq!(check(&me, AuthzOp::Unlink, None, Some(&tmp), Some(&mine)), ALLOW, "删自己的");
    assert_eq!(check(&me, AuthzOp::Unlink, None, Some(&tmp), Some(&theirs)), EPERM, "粘滞目录里不能删别人的");
    assert_eq!(check(&me, AuthzOp::RenameFrom, None, Some(&tmp), Some(&theirs)), EPERM);
    assert_eq!(check(&me, AuthzOp::RenameTo, None, Some(&tmp), Some(&theirs)), EPERM, "也不能覆盖别人的");
    let own_tmp = node(0o1777, 1000, 1000);
    assert_eq!(check(&me, AuthzOp::Unlink, None, Some(&own_tmp), Some(&theirs)), ALLOW, "目录属主可以");
    assert_eq!(check(&who(0, 0, &[]), AuthzOp::Unlink, None, Some(&tmp), Some(&theirs)), ALLOW, "root 可以");
}

#[test]
fn rename_of_a_directory_to_another_parent_needs_write_on_it() {
    let parent = node(0o777, 0, 0);
    let ro_dir = node(0o555, 1000, 1000);
    let me = who(1000, 1000, &[]);
    assert_eq!(check(&me, AuthzOp::RenameTo, Some(&ro_dir), Some(&parent), None), DENY, "要改它的 ..");
    assert_eq!(check(&me, AuthzOp::RenameTo, None, Some(&parent), None), ALLOW, "同一个父目录里改名：不需要");
}

#[test]
fn setattr_owner_rules() {
    let f = node(0o666, 1000, 1000);
    let other = who(2000, 2000, &[]);
    let sa = |mode, owner, size, times, times_now| AuthzOp::SetAttr { mode, owner, size, times, times_now };
    assert_eq!(check(&other, sa(true, false, false, false, false), Some(&f), None, None), EPERM, "chmod 限属主");
    assert_eq!(check(&other, sa(false, true, false, false, false), Some(&f), None, None), EPERM, "chown 限属主");
    assert_eq!(check(&other, sa(false, false, false, true, false), Some(&f), None, None), EPERM, "指定时间限属主");
    assert_eq!(check(&other, sa(false, false, false, false, true), Some(&f), None, None), ALLOW, "touch 只要写权限");
    assert_eq!(check(&other, sa(false, false, true, false, false), Some(&f), None, None), ALLOW, "truncate 要写权限");
    let ro = node(0o644, 1000, 1000);
    assert_eq!(check(&other, sa(false, false, true, false, false), Some(&ro), None, None), DENY);
    assert_eq!(check(&other, sa(false, false, false, false, true), Some(&ro), None, None), DENY);
    assert_eq!(
        check(&who(1000, 1, &[]), sa(false, false, false, false, true), Some(&node(0o444, 1000, 1)), None, None),
        ALLOW
    );
    assert_eq!(check(&who(1000, 1, &[]), sa(true, true, false, true, false), Some(&ro), None, None), ALLOW, "属主");
}

#[test]
fn xattr_ops_use_read_and_write_bits() {
    let f = node(0o640, 1000, 100);
    let g = who(2000, 100, &[]);
    assert_eq!(check(&g, AuthzOp::GetXattr, Some(&f), None, None), ALLOW);
    assert_eq!(check(&g, AuthzOp::SetXattr, Some(&f), None, None), DENY);
    assert_eq!(check(&g, AuthzOp::ListXattr, Some(&f), None, None), ALLOW);
    assert_eq!(check(&g, AuthzOp::Admin, None, None, None), Decision::Abstain);
}
