//! 有路径限制的 key 交给 authz 管线的两样东西（实现注记（M10），design.md 4.3 的 path-caps；`common::authz` 调用）：被访问对象的祖先链
//! （`AuthzInput.ancestors`），以及"路径遍历的放宽"。core 只放宽、从不因为路径限制拒绝——限制本身全在 path-caps 插件里。实现注记
//! （M10 审查）：为了 common.rs ≤ 400 行从那里挪出来。

use std::sync::Arc;

use super::common::Node;
use crate::base::ErrCode;
use crate::base::types::Principal;
use crate::hooks::traits::AuthzOp;
use crate::meta::engine::txn::{KvRead, OpCtx};
use crate::meta::schema::keys::ROOT_INO;

/// 有路径限制的 key（实现注记（M10），4.3 path-caps）：对象 `obj` 的祖先（根 → 父；对象的父目录已经删掉了——孤儿——就当没有）；
/// 对象是某个允许的根的**严格祖先**、操作是 Lookup/GetAttr（只为走到子树里去）→ 另给一个不带 path_caps 的身份，path-caps 对它弃权
/// （`AuthzInput` 里没有允许的根的祖先链，这一条只能由 core 判；core 只放宽、从不因为路径限制拒绝）。
pub(super) fn path_view(
    v: &dyn KvRead,
    c: &OpCtx,
    op: AuthzOp,
    obj: Option<&Node>,
) -> Result<(Vec<u64>, Option<Principal>), ErrCode> {
    let caps = &c.req.who.conn.path_caps;
    let Some(o) = obj.filter(|_| !caps.is_empty()) else { return Ok((vec![], None)) };
    let chain = |ino: u64| match c.attrs.ancestors(v, ino) {
        Err(ErrCode::ENOENT) => Ok(Arc::new(vec![])),
        r => r,
    };
    let mut anc = if o.ino == ROOT_INO { vec![] } else { chain(o.rec.parent)?.to_vec() };
    anc.reverse();
    let mut walk = false;
    if matches!(op, AuthzOp::Lookup | AuthzOp::GetAttr) && o.is_dir() {
        for (root, _) in caps {
            walk |= chain(*root)?.iter().skip(1).any(|a| *a == o.ino);
        }
    }
    let relaxed = walk.then(|| {
        let mut id = (*c.req.who.conn).clone();
        id.path_caps.clear();
        Principal { conn: Arc::new(id), ..c.req.who.clone() }
    });
    Ok((anc, relaxed))
}
