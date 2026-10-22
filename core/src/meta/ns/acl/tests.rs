//! ACL 的单元测试：编码往返、Linux 的 posix_acl_valid 规则（顺序、必需的条目、有命名条目就要 MASK、perm ≤ 7）、
//! 与权限位等价的判断、继承（create_masq：求交、MASK 优先于 GROUP_OBJ、不用 umask）、chmod 同步。

use super::*;

const ANY: u32 = u32::MAX;

fn a(tag: u16, perm: u16, id: u32) -> Ace {
    Ace { tag, perm, id }
}

fn minimal(u: u16, g: u16, o: u16) -> Vec<Ace> {
    vec![a(USER_OBJ, u, ANY), a(GROUP_OBJ, g, ANY), a(OTHER, o, ANY)]
}

#[test]
fn encode_parse_round_trip_and_bad_input() {
    let acl = vec![
        a(USER_OBJ, 7, ANY),
        a(USER, 5, 1001),
        a(GROUP_OBJ, 5, ANY),
        a(GROUP, 1, 7),
        a(MASK, 7, ANY),
        a(OTHER, 0, ANY),
    ];
    let b = encode(&acl);
    assert_eq!(b.len(), 4 + 8 * 6);
    assert_eq!(&b[..4], &[2, 0, 0, 0], "le32 version = 2");
    assert_eq!(parse(&b), Ok(acl));
    assert_eq!(parse(&encode(&[])), Ok(vec![]), "只有头 = 没有 ACL");
    for bad in [vec![], vec![2, 0, 0], vec![3, 0, 0, 0], [encode(&minimal(7, 5, 0)), vec![0]].concat()] {
        assert_eq!(parse(&bad), Err(ErrCode::EINVAL), "{bad:?}");
    }
}

#[test]
fn validity_follows_linux() {
    let ok = |v: &[Ace]| parse(&encode(v)).is_ok();
    assert!(ok(&minimal(6, 4, 4)));
    assert!(!ok(&[a(GROUP_OBJ, 4, ANY), a(USER_OBJ, 6, ANY), a(OTHER, 4, ANY)]), "顺序不对");
    assert!(!ok(&[a(USER_OBJ, 6, ANY), a(OTHER, 4, ANY)]), "缺 GROUP_OBJ");
    assert!(!ok(&[a(USER_OBJ, 6, ANY), a(USER, 6, 5), a(GROUP_OBJ, 4, ANY), a(OTHER, 4, ANY)]), "有命名用户就要 MASK");
    assert!(
        ok(&[a(USER_OBJ, 6, ANY), a(GROUP_OBJ, 4, ANY), a(MASK, 4, ANY), a(OTHER, 4, ANY)]),
        "没有命名条目也可以有 MASK"
    );
    assert!(!ok(&[a(USER_OBJ, 8, ANY), a(GROUP_OBJ, 4, ANY), a(OTHER, 4, ANY)]), "perm 只有 rwx 三位");
    assert!(!ok(&[a(USER_OBJ, 6, ANY), a(GROUP_OBJ, 4, ANY), a(OTHER, 4, ANY), a(OTHER, 4, ANY)]), "OTHER 只能一个");
}

#[test]
fn equivalence_with_the_mode_bits() {
    assert_eq!(equiv_mode(&minimal(6, 4, 0), 0o4777), (0o4640, false), "非权限位不变");
    let ext = [a(USER_OBJ, 6, ANY), a(USER, 7, 9), a(GROUP_OBJ, 4, ANY), a(MASK, 2, ANY), a(OTHER, 0, ANY)];
    assert_eq!(equiv_mode(&ext, 0), (0o620, true), "组位来自 MASK");
}

#[test]
fn inheritance_ignores_the_umask_and_masks_the_requested_mode() {
    let d = encode(&minimal(7, 5, 0));
    let p = |mode, access_acl, default_acl| Ok(NewPerms { mode, access_acl, default_acl });
    assert_eq!(on_create(None, 0o666, 0o022, false), p(0o644, None, None), "没有默认 ACL：用 umask");
    assert_eq!(on_create(Some(&d), 0o666, 0o077, false), p(0o640, None, None), "不用 umask；等价时不存访问 ACL");
    assert_eq!(on_create(Some(&d), 0o777, 0o077, true), p(0o750, None, Some(d.clone())), "新目录带上默认 ACL");
    let named = encode(&[a(USER_OBJ, 7, ANY), a(USER, 7, 5), a(GROUP_OBJ, 5, ANY), a(MASK, 7, ANY), a(OTHER, 5, ANY)]);
    let np = on_create(Some(&named), 0o640, 0, false).unwrap();
    assert_eq!((np.mode, np.default_acl), (0o640, None));
    let got = parse(&np.access_acl.unwrap()).unwrap();
    assert_eq!((got[0].perm, got[3].perm, got[4].perm, got[2].perm), (6, 4, 0, 5), "MASK 求交，GROUP_OBJ 不动");
}

#[test]
fn chmod_updates_user_mask_and_other() {
    let mut v = vec![a(USER_OBJ, 7, ANY), a(USER, 7, 5), a(GROUP_OBJ, 7, ANY), a(MASK, 7, ANY), a(OTHER, 7, ANY)];
    chmod(&mut v, 0o640);
    assert_eq!(v.iter().map(|e| e.perm).collect::<Vec<_>>(), vec![6, 7, 7, 4, 0], "有 MASK 时改 MASK、不改 GROUP_OBJ");
    let mut m = minimal(7, 7, 7);
    chmod(&mut m, 0o751);
    assert_eq!(m.iter().map(|e| e.perm).collect::<Vec<_>>(), vec![7, 5, 1]);
}
