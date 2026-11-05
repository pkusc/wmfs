//! posix-acl 授权插件（design.md 4.3、3.7.2，altitude 220）：POSIX.1e ACL 的访问判定（照 Linux 的 `posix_acl_permission`）。
//!
//! - 要看的 inode 没有扩展 ACL（`InodeView.acl_access` 为空）→ 对它弃权（权限位由 posix-mode 判）；一个请求里判过的 inode 有一个拒绝
//!   → 拒绝，否则有一个放行 → 放行，一个都没有 ACL → 弃权（与 posix-mode 的结果按 3.7.2 的组合规则合起来）。
//! - **root**（squash 之后仍是 0 的）→ 弃权：root 的放行由 posix-mode 负责（与 Linux 的 CAP_DAC_OVERRIDE 相同，它在"有扩展 ACL 就
//!   弃权"之前），所以带扩展 ACL 的 inode 对 root 仍是放行（M10 定下的，ADR-0033 第 12 条留的问题）。
//! - 只判断 ACL 管的那几位（读 / 写 / 执行），操作表与 posix-mode 相同；属主才能做的事（chmod、chown、指定时间）、粘滞目录的规则
//!   与 ACL 无关，posix-mode 照常判（它只在要看权限位时才对带 ACL 的 inode 弃权）。
//! - 经已打开的句柄（`Handle`，ADR-0036）：**属主放行、不看 ACL**（与 posix-mode 相同，学 NFS）；其他人与 open 相同。
//! - 判定（照 Linux）：属主 → USER_OBJ；命名用户 → 它的条目再与 MASK 求交；属组或命名组里有一条够用的（再与 MASK 求交）→ 放行，
//!   调用者在某个组里、可哪条都不够 → 拒绝；都不是 → OTHER。ACL 解不开（不应发生：写入时已校验）→ 拒绝（失败即关闭）。
//!
//! 指标 `wmfs_posix_acl_decisions_total{result}`；fail point `plugin.posix-acl.authz`（由 core 包在调用外层）。继承默认 ACL 与
//! `acl.default` 属性在 core 的 create 里做（`meta/ns/acl.rs`，3.7.3 末尾）。

use std::sync::Arc;

use wmfs_core::api::metrics::{self, Counter};
use wmfs_core::base::ErrCode;
use wmfs_core::base::types::Principal;
use wmfs_core::hooks::manifest::{CORE_ABI_VERSION, Hook, Manifest};
use wmfs_core::hooks::traits::{AuthzInput, AuthzOp, AuthzPlugin, Decision, InodeView};
use wmfs_core::meta::ns::acl::{self, Ace, GROUP, GROUP_OBJ, MASK, OTHER, USER, USER_OBJ};
use wmfs_core::meta::ns::common::in_group;

pub static MANIFEST: Manifest = Manifest {
    name: "posix-acl",
    version: "0.1.0",
    hooks: &[Hook::Authz],
    altitude: 220,
    hot_path: true,
    config_schema: "",
    abi_version: CORE_ABI_VERSION,
    attrs: &["acl.default"],
    budget_ns: 5_000, // 3.7.4：authz 5 µs
    fail_points: &["plugin.posix-acl.authz"],
};

const R: u16 = 4;
const W: u16 = 2;
const X: u16 = 1;

pub struct PosixAcl {
    allow: Arc<Counter>,
    deny: Arc<Counter>,
    abstain: Arc<Counter>,
}

impl PosixAcl {
    pub fn new() -> PosixAcl {
        let mk = |r| metrics::counter("wmfs_posix_acl_decisions_total", "posix-acl 的判定", &[("result", r)]);
        PosixAcl { allow: mk("allow"), deny: mk("deny"), abstain: mk("abstain") }
    }
}

impl Default for PosixAcl {
    fn default() -> PosixAcl {
        PosixAcl::new()
    }
}

impl AuthzPlugin for PosixAcl {
    fn manifest(&self) -> &'static Manifest {
        &MANIFEST
    }

    fn check(&self, input: &AuthzInput) -> Decision {
        let d = decide(input);
        match d {
            Decision::Allow => &self.allow,
            Decision::Deny(_) => &self.deny,
            Decision::Abstain => &self.abstain,
        }
        .inc();
        d
    }
}

/// 对 inode `n` 要 `want`：没有 inode、没有扩展 ACL、调用者是 root → 弃权；否则按 ACL 判（见文件头）。
fn need(who: &Principal, n: Option<&InodeView>, want: u16) -> Decision {
    let Some((n, bytes)) = n.and_then(|n| n.acl_access.map(|b| (n, b))) else { return Decision::Abstain };
    if who.uid == 0 {
        return Decision::Abstain;
    }
    match acl::parse(bytes) {
        Ok(a) if allows(&a, who, n, want) => Decision::Allow,
        _ => Decision::Deny(ErrCode::EACCES),
    }
}

/// Linux 的 posix_acl_permission（条目已按 USER_OBJ、USER…、GROUP_OBJ、GROUP…、MASK、OTHER 排好，写入时校验过）。
fn allows(a: &[Ace], who: &Principal, n: &InodeView, want: u16) -> bool {
    let mask = a.iter().find(|e| e.tag == MASK).map_or(7, |e| e.perm);
    let ok = |perm: u16| perm & want == want;
    let mut in_some_group = false;
    for e in a {
        match e.tag {
            USER_OBJ if who.uid == n.uid => return ok(e.perm),
            USER if who.uid == e.id => return ok(e.perm & mask),
            GROUP_OBJ | GROUP => {
                let gid = if e.tag == GROUP_OBJ { n.gid } else { e.id };
                if in_group(who, gid) {
                    if ok(e.perm & mask) {
                        return true;
                    }
                    in_some_group = true;
                }
            }
            OTHER => return !in_some_group && ok(e.perm),
            _ => {}
        }
    }
    false
}

/// 合并几项：任一拒绝 → 拒绝；否则任一放行 → 放行；否则弃权（只判过有 ACL 的 inode，见文件头）。
fn any(ds: &[Decision]) -> Decision {
    if let Some(d) = ds.iter().find(|d| matches!(d, Decision::Deny(_))) {
        return *d;
    }
    if ds.contains(&Decision::Allow) { Decision::Allow } else { Decision::Abstain }
}

fn decide(i: &AuthzInput) -> Decision {
    let w = i.who;
    let rw = |read: bool, write: bool| (if read { R } else { 0 }) | (if write { W } else { 0 });
    match i.op {
        AuthzOp::Lookup => need(w, i.parent, X),
        AuthzOp::ReadDir => need(w, i.target, R),
        AuthzOp::Open { read, write } => need(w, i.target, rw(read, write)),
        AuthzOp::Create
        | AuthzOp::Mkdir
        | AuthzOp::Mknod
        | AuthzOp::Symlink
        | AuthzOp::Link
        | AuthzOp::Unlink
        | AuthzOp::Rmdir
        | AuthzOp::RenameFrom => need(w, i.parent, W | X),
        AuthzOp::RenameTo => any(&[need(w, i.parent, W | X), need(w, i.target, W)]), // target = 换了父目录的目录（它的 ..）
        AuthzOp::SetAttr { size, times_now, .. } => {
            let now = times_now && !i.target.is_some_and(|t| t.uid == w.uid);
            if size || now { need(w, i.target, W) } else { Decision::Abstain }
        }
        AuthzOp::GetXattr => need(w, i.target, R),
        AuthzOp::SetXattr | AuthzOp::RemoveXattr => need(w, i.target, W),
        AuthzOp::Handle { read, write } => match i.target {
            // ADR-0036：属主经已打开的句柄读写不看 ACL（与 posix-mode 相同）
            Some(t) if t.acl_access.is_some() && t.uid == w.uid && w.uid != 0 => Decision::Allow,
            t => need(w, t, rw(read, write)),
        },
        AuthzOp::GetAttr | AuthzOp::ReadLink | AuthzOp::ListXattr | AuthzOp::Lock | AuthzOp::Admin => Decision::Abstain,
    }
}

#[cfg(test)]
mod tests;
