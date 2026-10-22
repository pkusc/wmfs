//! posix-mode 授权插件（design.md 4.3、3.7.2，altitude 210）：经典 POSIX 权限位。
//!
//! - 按属主 / 属组（含附加组）/ 其他人取对应的 rwx 三位；root（squash 之后仍是 0 的）全部放行（元数据操作里没有"执行文件"），
//!   带扩展 ACL 的 inode 也一样（与 Linux 的 CAP_DAC_OVERRIDE 相同）；
//! - 不是 root 时，inode 带扩展 ACL（`InodeView.acl_access` 不为空）→ 对它**弃权**，由 posix-acl（220，M10）判定；
//! - 目录项操作：对父目录要写 + 执行；lookup 要执行；readdir 要读；open 按打开方式要读/写；
//! - 粘滞位：粘滞目录里只有目录属主、被删对象的属主与 root 能删除或改名（否则 `EPERM`，与 Linux 相同）；
//! - 换了父目录的目录（rename 的 target）：要能写它自己（更新它的 `..`）；
//! - chmod、chown、把时间设成指定值：只有属主或 root（`EPERM`）；设成"现在"：属主，或有写权限；改长度：要有写权限。
//!   chown 的取值规则（只有 root 能改 uid、属主只能把 gid 改成自己所在的组）由 core 检查——插件看不到新值。
//!
//! 指标 `wmfs_posix_mode_decisions_total{result}`；fail point `plugin.posix-mode.authz`（由 core 包在调用外层，触发 = 以注入的码拒绝）。

use std::sync::Arc;

use wmfs_core::api::metrics::{self, Counter};
use wmfs_core::base::ErrCode;
use wmfs_core::base::types::Principal;
use wmfs_core::hooks::manifest::{CORE_ABI_VERSION, Hook, Manifest};
use wmfs_core::hooks::traits::{AuthzInput, AuthzOp, AuthzPlugin, Decision, InodeView};
use wmfs_core::meta::ns::common::S_ISVTX;

pub static MANIFEST: Manifest = Manifest {
    name: "posix-mode",
    version: "0.1.0",
    hooks: &[Hook::Authz],
    altitude: 210,
    hot_path: true,
    config_schema: "",
    abi_version: CORE_ABI_VERSION,
    attrs: &[],
    budget_ns: 5_000, // 3.7.4：authz 5 µs
    fail_points: &["plugin.posix-mode.authz"],
};

const R: u32 = 4;
const W: u32 = 2;
const X: u32 = 1;

pub struct PosixMode {
    allow: Arc<Counter>,
    deny: Arc<Counter>,
    abstain: Arc<Counter>,
}

impl PosixMode {
    pub fn new() -> PosixMode {
        let mk = |r| metrics::counter("wmfs_posix_mode_decisions_total", "posix-mode 的判定", &[("result", r)]);
        PosixMode { allow: mk("allow"), deny: mk("deny"), abstain: mk("abstain") }
    }
}

impl Default for PosixMode {
    fn default() -> PosixMode {
        PosixMode::new()
    }
}

impl AuthzPlugin for PosixMode {
    fn manifest(&self) -> &'static Manifest {
        &MANIFEST
    }

    fn check(&self, input: &AuthzInput) -> Decision {
        let d = decide(input);
        let counter = match d {
            Decision::Allow => &self.allow,
            Decision::Deny(_) => &self.deny,
            Decision::Abstain => &self.abstain,
        };
        counter.inc();
        d
    }
}

/// 属主或 root（chmod、chown、指定时间、粘滞位都是这个规则）。
fn owner_or_root(who: &Principal, n: &InodeView) -> bool {
    who.uid == 0 || who.uid == n.uid
}

/// 对 inode `n` 要 `want`（R/W/X 的组合）。core 没给要检查的 inode = 程序写错了：拒绝（失败即关闭）。
fn need(who: &Principal, n: Option<&InodeView>, want: u32) -> Decision {
    let Some(n) = n else { return Decision::Deny(ErrCode::EACCES) };
    if who.uid == 0 {
        return Decision::Allow; // root 不受权限位与 ACL 限制（先于 ACL 判断，见文件头）
    }
    if n.acl_access.is_some() {
        return Decision::Abstain; // 扩展 ACL：交给 posix-acl
    }
    let bits = if who.uid == n.uid {
        n.mode >> 6
    } else if who.gid == n.gid || who.groups.contains(&n.gid) {
        n.mode >> 3
    } else {
        n.mode
    };
    if bits & want == want { Decision::Allow } else { Decision::Deny(ErrCode::EACCES) }
}

/// 粘滞目录 `dir` 里删除或改名 `victim`：只有目录属主、victim 的属主与 root 可以。
fn sticky(who: &Principal, dir: Option<&InodeView>, victim: Option<&InodeView>) -> Decision {
    match (dir, victim) {
        (Some(d), Some(v)) if d.mode & S_ISVTX != 0 && !owner_or_root(who, d) && who.uid != v.uid => {
            Decision::Deny(ErrCode::EPERM)
        }
        _ => Decision::Allow,
    }
}

fn owner(who: &Principal, n: Option<&InodeView>) -> Decision {
    match n {
        Some(n) if owner_or_root(who, n) => Decision::Allow,
        _ => Decision::Deny(ErrCode::EPERM),
    }
}

/// 合并几项检查：任一拒绝 → 拒绝（第一个的码）；否则任一弃权 → 弃权；否则放行。
fn all(ds: &[Decision]) -> Decision {
    if let Some(d) = ds.iter().find(|d| matches!(d, Decision::Deny(_))) {
        return *d;
    }
    if ds.contains(&Decision::Abstain) { Decision::Abstain } else { Decision::Allow }
}

fn decide(i: &AuthzInput) -> Decision {
    let w = i.who;
    match i.op {
        AuthzOp::Lookup => need(w, i.parent, X),
        AuthzOp::GetAttr | AuthzOp::ReadLink | AuthzOp::ListXattr | AuthzOp::Lock => Decision::Allow,
        AuthzOp::ReadDir => need(w, i.target, R),
        AuthzOp::Open { read, write } => {
            let want = (if read { R } else { 0 }) | (if write { W } else { 0 });
            need(w, i.target, want)
        }
        AuthzOp::Create | AuthzOp::Mkdir | AuthzOp::Mknod | AuthzOp::Symlink | AuthzOp::Link => {
            need(w, i.parent, W | X)
        }
        AuthzOp::Unlink | AuthzOp::Rmdir | AuthzOp::RenameFrom => {
            all(&[need(w, i.parent, W | X), sticky(w, i.parent, i.victim)])
        }
        AuthzOp::RenameTo => {
            let moved = i.target.map_or(Decision::Allow, |t| need(w, Some(t), W)); // 换了父目录的目录
            all(&[need(w, i.parent, W | X), sticky(w, i.parent, i.victim), moved])
        }
        AuthzOp::SetAttr { mode, owner: chown, size, times, times_now } => {
            let mut ds = Vec::with_capacity(3);
            if mode || chown || times {
                ds.push(owner(w, i.target));
            }
            if size {
                ds.push(need(w, i.target, W));
            }
            if times_now && !i.target.is_some_and(|t| owner_or_root(w, t)) {
                ds.push(need(w, i.target, W));
            }
            all(&ds)
        }
        AuthzOp::GetXattr => need(w, i.target, R),
        AuthzOp::SetXattr | AuthzOp::RemoveXattr => need(w, i.target, W),
        AuthzOp::Admin => Decision::Abstain, // 管理操作的权限由管理 API 按 key 的角色检查，不是权限位的事
    }
}

#[cfg(test)]
mod tests;
