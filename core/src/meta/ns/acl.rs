//! POSIX ACL 的二进制格式与"新文件继承默认 ACL"（design.md 3.7.3 末尾、4.3、8.1）。继承是 create 语义的一部分，所以在 core；
//! 访问判定由 posix-acl 插件做（M10）。
//!
//! 格式与 Linux 的 xattr 相同（`system.posix_acl_access` / `system.posix_acl_default` 的值）：le32 version = 2，之后每条
//! 8 字节 `{le16 tag, le16 perm, le32 id}`；tag：USER_OBJ 0x01、USER 0x02、GROUP_OBJ 0x04、GROUP 0x08、MASK 0x10、
//! OTHER 0x20。只有头没有条目 = "没有 ACL"。校验、继承（create_masq）、chmod 同步、"与权限位等价"的判断都照 Linux
//! 的 `fs/posix_acl.c` 写（`posix_acl_valid`、`posix_acl_create_masq`、`__posix_acl_chmod`、`posix_acl_equiv_mode`）。

use crate::base::ErrCode;

pub const ACL_VERSION: u32 = 2;
pub const USER_OBJ: u16 = 0x01;
pub const USER: u16 = 0x02;
pub const GROUP_OBJ: u16 = 0x04;
pub const GROUP: u16 = 0x08;
pub const MASK: u16 = 0x10;
pub const OTHER: u16 = 0x20;

/// 一条 ACL 条目。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Ace {
    pub tag: u16,
    pub perm: u16,
    pub id: u32,
}

/// 解码并校验（顺序：USER_OBJ、USER…、GROUP_OBJ、GROUP…、MASK、OTHER；有 USER/GROUP 条目就必须有 MASK；perm ≤ 7）；
/// 不合法 → `EINVAL`。只有头 → 空表（"没有 ACL"）。
pub fn parse(b: &[u8]) -> Result<Vec<Ace>, ErrCode> {
    if b.len() < 4 || !(b.len() - 4).is_multiple_of(8) || b[0..4] != ACL_VERSION.to_le_bytes() {
        return Err(ErrCode::EINVAL);
    }
    let le16 = |x: &[u8]| u16::from_le_bytes([x[0], x[1]]);
    let a: Vec<Ace> = b[4..]
        .chunks(8)
        .map(|e| Ace { tag: le16(&e[0..2]), perm: le16(&e[2..4]), id: u32::from_le_bytes([e[4], e[5], e[6], e[7]]) })
        .collect();
    if a.is_empty() || valid(&a) { Ok(a) } else { Err(ErrCode::EINVAL) }
}

pub fn encode(a: &[Ace]) -> Vec<u8> {
    let mut b = ACL_VERSION.to_le_bytes().to_vec();
    for e in a {
        b.extend_from_slice(&e.tag.to_le_bytes());
        b.extend_from_slice(&e.perm.to_le_bytes());
        b.extend_from_slice(&e.id.to_le_bytes());
    }
    b
}

/// Linux 的 posix_acl_valid：按状态机检查条目顺序与必需的条目。
fn valid(a: &[Ace]) -> bool {
    let (mut state, mut needs_mask) = (USER_OBJ, false);
    for e in a {
        if e.perm & !7 != 0 {
            return false;
        }
        state = match (e.tag, state) {
            (USER_OBJ, USER_OBJ) => USER,
            (USER, USER) | (GROUP, GROUP) => {
                needs_mask = true;
                state
            }
            (GROUP_OBJ, USER) => GROUP,
            (MASK, GROUP) => OTHER,
            (OTHER, OTHER) => 0,
            (OTHER, GROUP) if !needs_mask => 0,
            _ => return false,
        };
    }
    state == 0
}

/// ACL 对应的权限位（posix_acl_equiv_mode）：返回（新的 mode——非权限位不变，ACL 是否比权限位多出东西）。
/// 多出 USER/GROUP/MASK 条目 = "扩展 ACL"（要存下来、置 F_HAS_ACL）；否则它与权限位等价，只改权限位即可。
pub fn equiv_mode(a: &[Ace], mode: u32) -> (u32, bool) {
    let (mut m, mut extended) = (0u32, false);
    for e in a {
        let p = (e.perm & 7) as u32;
        match e.tag {
            USER_OBJ => m |= p << 6,
            GROUP_OBJ => m |= p << 3,
            OTHER => m |= p,
            MASK => {
                m = (m & !0o070) | (p << 3);
                extended = true;
            }
            _ => extended = true,
        }
    }
    ((mode & !0o777) | m, extended)
}

/// 新对象继承父目录的默认 ACL（posix_acl_create_masq）：把条目与请求的 mode 求交，返回（最终 mode，是否扩展 ACL）。
fn create_masq(a: &mut [Ace], mode: u32) -> (u32, bool) {
    let (mut m, mut extended, mut group) = (mode, false, None);
    for (i, e) in a.iter_mut().enumerate() {
        match e.tag {
            USER_OBJ => {
                e.perm &= ((m >> 6) & 7) as u16;
                m &= ((e.perm as u32) << 6) | !0o700;
            }
            OTHER => {
                e.perm &= (m & 7) as u16;
                m &= e.perm as u32 | !0o007;
            }
            GROUP_OBJ if group.is_none() => group = Some(i), // 有 MASK 时被下面的 MASK 覆盖
            MASK => {
                group = Some(i);
                extended = true;
            }
            USER | GROUP => extended = true,
            _ => {}
        }
    }
    if let Some(g) = group {
        a[g].perm &= ((m >> 3) & 7) as u16;
        m &= ((a[g].perm as u32) << 3) | !0o070;
    }
    ((mode & !0o777) | (m & 0o777), extended)
}

/// chmod 之后同步访问 ACL（__posix_acl_chmod）：USER_OBJ、MASK（没有 MASK 时是 GROUP_OBJ）、OTHER 跟着新的权限位走。
pub fn chmod(a: &mut [Ace], mode: u32) {
    let has_mask = a.iter().any(|e| e.tag == MASK);
    for e in a.iter_mut() {
        match e.tag {
            USER_OBJ => e.perm = ((mode >> 6) & 7) as u16,
            MASK => e.perm = ((mode >> 3) & 7) as u16,
            GROUP_OBJ if !has_mask => e.perm = ((mode >> 3) & 7) as u16,
            OTHER => e.perm = (mode & 7) as u16,
            _ => {}
        }
    }
}

/// 新建对象的权限位与要写下的 ACL（[`on_create`] 的结果）。
#[derive(Debug, PartialEq, Eq)]
pub struct NewPerms {
    pub mode: u32,
    pub access_acl: Option<Vec<u8>>,  // 扩展的访问 ACL（与权限位等价时为 None）
    pub default_acl: Option<Vec<u8>>, // 新目录要带的默认 ACL（继承父目录的）
}

/// 新建对象的权限位与 ACL（3.5.1、8.1）：父目录有默认 ACL → 按 ACL 规则、**不应用 umask**，得到扩展的访问 ACL 与新目录
/// 要带的默认 ACL；没有 → `mode & !umask`。
pub fn on_create(parent_default: Option<&[u8]>, mode: u32, umask: u32, is_dir: bool) -> Result<NewPerms, ErrCode> {
    let mut a = match parent_default {
        Some(d) => parse(d)?,
        None => vec![],
    };
    if a.is_empty() {
        return Ok(NewPerms { mode: mode & !umask, access_acl: None, default_acl: None });
    }
    let (mode, extended) = create_masq(&mut a, mode);
    let default_acl = parent_default.filter(|_| is_dir).map(<[u8]>::to_vec);
    Ok(NewPerms { mode, access_acl: extended.then(|| encode(&a)), default_acl })
}

#[cfg(test)]
mod tests;
