//! posix-acl 的单元测试：manifest、没有 ACL 与 root 弃权、属主 / 命名用户 / 属组 / 命名组 / 其他人与 MASK（照 Linux）、在某个组里
//! 却不够时不落到 other、经句柄时属主放行、几个 inode 的组合、判定计数。

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

fn e(tag: u16, perm: u16, id: u32) -> Ace {
    Ace { tag, perm, id }
}

/// 属主 1000（rwx）、命名用户 2000（rx）、属组 100（r-x）、命名组 7（rwx）、MASK r-x、其他人 ---。
fn acl_bytes() -> Vec<u8> {
    acl::encode(&[
        e(USER_OBJ, 7, 0),
        e(USER, 5, 2000),
        e(GROUP_OBJ, 5, 0),
        e(GROUP, 7, 7),
        e(MASK, 5, 0),
        e(OTHER, 0, 0),
    ])
}

fn node(acl: Option<&[u8]>) -> InodeView<'_> {
    InodeView { ino: 16, kind: 2, mode: 0o750, uid: 1000, gid: 100, acl_access: acl }
}

fn check(w: &Principal, op: AuthzOp, t: Option<&InodeView>, p: Option<&InodeView>) -> Decision {
    PosixAcl::new().check(&AuthzInput { who: w, op, target: t, parent: p, victim: None, ancestors: &[] })
}

const DENY: Decision = Decision::Deny(ErrCode::EACCES);
const ALLOW: Decision = Decision::Allow;
const ABSTAIN: Decision = Decision::Abstain;

#[test]
fn manifest_toml_matches_code() {
    check_manifest_toml(&MANIFEST, include_str!("manifest.toml")).unwrap();
}

#[test]
fn no_acl_and_root_abstain() {
    let plain = node(None);
    assert_eq!(
        check(&who(2000, 2, &[]), AuthzOp::ReadDir, Some(&plain), None),
        ABSTAIN,
        "没有扩展 ACL：交给 posix-mode"
    );
    let b = acl_bytes();
    let a = node(Some(&b));
    assert_eq!(
        check(&who(0, 0, &[]), AuthzOp::Create, None, Some(&a)),
        ABSTAIN,
        "root：posix-mode 放行（CAP_DAC_OVERRIDE）"
    );
    assert_eq!(check(&who(2000, 2, &[]), AuthzOp::GetAttr, Some(&a), None), ABSTAIN, "getattr 不要权限");
    let sa = AuthzOp::SetAttr { mode: true, owner: false, size: false, times: false, times_now: false };
    assert_eq!(check(&who(2000, 2, &[]), sa, Some(&a), None), ABSTAIN, "chmod 的属主规则归 posix-mode");
}

#[test]
fn entries_are_matched_like_linux() {
    let b = acl_bytes();
    let a = node(Some(&b));
    let open = |r, w| AuthzOp::Open { read: r, write: w };
    assert_eq!(
        check(&who(1000, 1, &[]), open(true, true), Some(&a), None),
        ALLOW,
        "属主：USER_OBJ rwx（不受 MASK 限制）"
    );
    assert_eq!(check(&who(2000, 2, &[]), AuthzOp::ReadDir, Some(&a), None), ALLOW, "命名用户 r-x");
    assert_eq!(check(&who(2000, 2, &[]), AuthzOp::Create, None, Some(&a)), DENY, "命名用户没有 w");
    assert_eq!(check(&who(3000, 7, &[]), AuthzOp::Create, None, Some(&a)), DENY, "命名组 rwx，可 MASK 只有 r-x");
    assert_eq!(check(&who(3000, 7, &[]), AuthzOp::Lookup, None, Some(&a)), ALLOW, "命名组的 x 在 MASK 里");
    assert_eq!(check(&who(3000, 5, &[100]), AuthzOp::ReadDir, Some(&a), None), ALLOW, "附加组里有属组");
    assert_eq!(check(&who(3000, 5, &[]), AuthzOp::Lookup, None, Some(&a)), DENY, "其他人什么都没有");
    let other_r = acl::encode(&[e(USER_OBJ, 7, 0), e(GROUP_OBJ, 0, 0), e(GROUP, 0, 9), e(MASK, 7, 0), e(OTHER, 4, 0)]);
    let o = node(Some(&other_r));
    assert_eq!(check(&who(3000, 9, &[]), AuthzOp::ReadDir, Some(&o), None), DENY, "在某个组里却不够：不落到 other");
    assert_eq!(check(&who(3000, 5, &[]), AuthzOp::ReadDir, Some(&o), None), ALLOW, "不在任何组里：other 的 r");
    let size = AuthzOp::SetAttr { mode: false, owner: false, size: true, times: false, times_now: false };
    assert_eq!(check(&who(2000, 2, &[]), size, Some(&a), None), DENY, "truncate 要 w");
    assert_eq!(check(&who(2000, 2, &[]), AuthzOp::GetXattr, Some(&a), None), ALLOW);
}

/// ADR-0036：经已打开的句柄读写，属主不看 ACL；其他人与 open 相同。RenameTo 的两个 inode 一起判。
#[test]
fn handles_trust_the_owner_and_several_inodes_combine() {
    let none = acl::encode(&[e(USER_OBJ, 0, 0), e(USER, 0, 2000), e(GROUP_OBJ, 0, 0), e(MASK, 0, 0), e(OTHER, 0, 0)]);
    let f = node(Some(&none));
    let h = AuthzOp::Handle { read: false, write: true };
    assert_eq!(check(&who(1000, 1, &[]), h, Some(&f), None), ALLOW, "属主经写句柄提交：不看 ACL");
    assert_eq!(check(&who(2000, 2, &[]), h, Some(&f), None), DENY, "其他人与 open 相同");
    let plain = node(None);
    assert_eq!(check(&who(1000, 1, &[]), h, Some(&plain), None), ABSTAIN, "没有 ACL：交给 posix-mode");
    let b = acl_bytes();
    let dir = node(Some(&b));
    let r = |w: &Principal, t| check(w, AuthzOp::RenameTo, t, Some(&dir));
    assert_eq!(r(&who(3000, 7, &[]), None), DENY, "目标父目录没有 w（MASK）");
    let rw_dir = acl::encode(&[e(USER_OBJ, 7, 0), e(USER, 7, 2000), e(GROUP_OBJ, 5, 0), e(MASK, 7, 0), e(OTHER, 0, 0)]);
    let d2 = node(Some(&rw_dir));
    let moved = node(None);
    let x = check(&who(2000, 2, &[]), AuthzOp::RenameTo, Some(&moved), Some(&d2));
    assert_eq!(x, ALLOW, "父目录的 ACL 放行，被移动的目录没有 ACL（posix-mode 判）：合起来放行");
}

#[test]
fn decisions_are_counted_by_result() {
    let c = |r| metrics::counter("wmfs_posix_acl_decisions_total", "", &[("result", r)]).get();
    let (a, d, ab) = (c("allow"), c("deny"), c("abstain"));
    let b = acl_bytes();
    let n = node(Some(&b));
    check(&who(2000, 2, &[]), AuthzOp::ReadDir, Some(&n), None);
    check(&who(3000, 5, &[]), AuthzOp::ReadDir, Some(&n), None);
    check(&who(3000, 5, &[]), AuthzOp::ReadDir, Some(&node(None)), None);
    assert!(c("allow") > a && c("deny") > d && c("abstain") > ab);
}
