//! path-caps 授权插件（design.md 4.3、ADR-0009，altitude 250）：把一把 key 限制在若干子树里（CephFS 的 path 限制的等价物）。
//!
//! 规则来自复制状态 `KEYMETA.path_caps`（`wmfs key create --path /proj [--path-ro /shared]` 写入时解析成目录的 inode 号：目录被 rename
//! 之后限制仍跟着它），握手时放进连接身份（`ConnIdentity.path_caps`，每项 (子树根, 只读?)）。
//! - 这把 key 没有限制（空）→ 弃权；
//! - 要看的对象 = 目录项操作的父目录（lookup、create、unlink、rename……），否则是目标本身；它本身或它的祖先（`AuthzInput.ancestors`，
//!   根 → 父，core 按 attr_epoch 缓存）里有一个允许的根 → 放行；落在的根全都是只读的、而这是写类操作 → `Deny(EROFS)`；
//! - 其余 → `Deny(EACCES)`。经已打开的句柄（`Handle`）当成同样读写的 `Open` 来查：路径限制不因为是属主而放松（ADR-0036 第 2 条）。
//! - link 的源文件：core 另按"取属性"对它过一次授权（实现注记（M10 审查），`ns::create::link`），本插件按它自己的祖先判——子树外的
//!   文件不能按 inode 号被链接进子树。
//! - 4.3 的"目标是某个允许的根的祖先目录、操作是 Lookup/GetAttr → 放行（只为路径遍历）"由 core 判定（实现注记（M10）：`AuthzInput`
//!   里没有允许的根的祖先链，C ABI v1 也没有）：这类请求 core 交给管线的连接身份不带 path_caps，本插件对它弃权（`ns::common::authz`）。
//!
//! 指标 `wmfs_path_caps_decisions_total{result}`；fail point `plugin.path-caps.authz`（由 core 包在调用外层）。

use std::sync::Arc;

use wmfs_core::api::metrics::{self, Counter};
use wmfs_core::base::ErrCode;
use wmfs_core::hooks::manifest::{CORE_ABI_VERSION, Hook, Manifest};
use wmfs_core::hooks::traits::{AuthzInput, AuthzOp, AuthzPlugin, Decision};

pub static MANIFEST: Manifest = Manifest {
    name: "path-caps",
    version: "0.1.0",
    hooks: &[Hook::Authz],
    altitude: 250,
    hot_path: true,
    config_schema: "",
    abi_version: CORE_ABI_VERSION,
    attrs: &[],
    budget_ns: 2_000, // 4.3：2 µs
    fail_points: &["plugin.path-caps.authz"],
};

pub struct PathCaps {
    allow: Arc<Counter>,
    deny: Arc<Counter>,
    abstain: Arc<Counter>,
}

impl PathCaps {
    pub fn new() -> PathCaps {
        let mk = |r| metrics::counter("wmfs_path_caps_decisions_total", "path-caps 的判定", &[("result", r)]);
        PathCaps { allow: mk("allow"), deny: mk("deny"), abstain: mk("abstain") }
    }
}

impl Default for PathCaps {
    fn default() -> PathCaps {
        PathCaps::new()
    }
}

impl AuthzPlugin for PathCaps {
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

/// 写类操作（只读的根上 → `EROFS`）。
fn writes(op: AuthzOp) -> bool {
    match op {
        AuthzOp::Open { write, .. } | AuthzOp::Handle { write, .. } => write,
        AuthzOp::Lookup
        | AuthzOp::GetAttr
        | AuthzOp::ReadDir
        | AuthzOp::ReadLink
        | AuthzOp::GetXattr
        | AuthzOp::ListXattr
        | AuthzOp::Lock
        | AuthzOp::Admin => false,
        _ => true,
    }
}

fn decide(i: &AuthzInput) -> Decision {
    let caps = &i.who.conn.path_caps;
    if caps.is_empty() || i.op == AuthzOp::Admin {
        return Decision::Abstain; // 没有限制；管理操作的权限由管理 API 按 key 的角色检查
    }
    // 目录项操作看父目录，其余看目标（core 给的 ancestors 也是这个对象的）
    let Some(obj) = i.parent.or(i.target) else { return Decision::Deny(ErrCode::EACCES) };
    // 对象落在的那些允许的根，各自是不是只读的
    let hit_ro: Vec<bool> =
        caps.iter().filter(|(r, _)| obj.ino == *r || i.ancestors.contains(r)).map(|(_, ro)| *ro).collect();
    if hit_ro.is_empty() {
        return Decision::Deny(ErrCode::EACCES);
    }
    if writes(i.op) && hit_ro.iter().all(|ro| *ro) {
        return Decision::Deny(ErrCode::EROFS);
    }
    Decision::Allow
}

#[cfg(test)]
mod tests;
