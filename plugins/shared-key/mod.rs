//! shared-key 认证插件（design.md 4.2、3.3.2，altitude 110）：连接建立时验证客户端的 HMAC 证明，返回连接身份。
//!
//! - `key_id == 0` → 角色 Server（服务端之间；不查 KEYMETA）；否则角色、租户、uid 范围、root_squash、path_caps 取自
//!   KEYMETA（`Role::from_u8`），被撤销或不存在的 key 直接拒绝；
//! - 证明 = `HMAC-SHA256(secret(key_id), "wmfs-auth-v1" ‖ client_nonce ‖ server_nonce ‖ role ‖ cluster_id ‖ key_id)`，
//!   用 core 的 `verify_auth_proof` 常数时间比较；`server_proof` 让客户端反过来确认服务端也有这把 key（双向认证）。
//! - "Hello.role == KEYMETA.role"、"Accept 之后 ConnIdentity.role == Hello.role" 由 core 再强制一遍（与本插件无关）。
//!
//! 指标 `wmfs_shared_key_auth_total{result}`；fail point `plugin.shared-key.authn`（由 core 包在调用外层，触发 = 拒绝）。

use std::sync::Arc;

use wmfs_core::api::metrics::{self, Counter};
use wmfs_core::base::types::Role;
use wmfs_core::hooks::manifest::{CORE_ABI_VERSION, Hook, Manifest};
use wmfs_core::hooks::traits::{AuthnInput, AuthnPlugin, AuthnResult, ConnIdentity, KeyLookup};
use wmfs_core::init::SERVER_KEY;
use wmfs_core::proto::handshake::{ProofInput, server_proof, verify_auth_proof};

pub static MANIFEST: Manifest = Manifest {
    name: "shared-key",
    version: "0.1.0",
    hooks: &[Hook::Authn],
    altitude: 110,
    hot_path: true,
    config_schema: "",
    abi_version: CORE_ABI_VERSION,
    attrs: &[],
    budget_ns: 20_000, // 每连接 20 µs（4.2）
    fail_points: &["plugin.shared-key.authn"],
};

pub struct SharedKey {
    accepted: Arc<Counter>,
    rejected: Arc<Counter>,
}

impl SharedKey {
    pub fn new() -> SharedKey {
        let mk = |r| metrics::counter("wmfs_shared_key_auth_total", "shared-key 认证的结果", &[("result", r)]);
        SharedKey { accepted: mk("accept"), rejected: mk("reject") }
    }
}

impl Default for SharedKey {
    fn default() -> SharedKey {
        SharedKey::new()
    }
}

impl AuthnPlugin for SharedKey {
    fn manifest(&self) -> &'static Manifest {
        &MANIFEST
    }

    fn authenticate(&self, input: &AuthnInput, keys: &dyn KeyLookup) -> AuthnResult {
        match identity(input, keys).filter(|_| proof_ok(input, keys)) {
            Some(id) => {
                self.accepted.inc();
                AuthnResult::Accept(id)
            }
            None => {
                self.rejected.inc();
                AuthnResult::Reject
            }
        }
    }

    fn server_proof(&self, input: &AuthnInput, keys: &dyn KeyLookup) -> Option<[u8; 32]> {
        keys.secret(input.key_id).map(|s| server_proof(&s, &input.server_nonce, &input.client_nonce))
    }
}

/// 这把 key 对应的连接身份；key 不存在、被撤销或角色无效 → None。
fn identity(input: &AuthnInput, keys: &dyn KeyLookup) -> Option<ConnIdentity> {
    if input.key_id == SERVER_KEY {
        let all = vec![(0, u32::MAX)];
        return Some(ConnIdentity {
            key_id: 0,
            role: Role::Server,
            tenant: 0,
            uid_ranges: all,
            root_squash: false,
            path_caps: vec![],
        });
    }
    let m = keys.meta(input.key_id).filter(|m| !m.revoked)?;
    let role = Role::from_u8(m.role)?;
    let (tenant, uid_ranges, root_squash, path_caps) = (m.tenant, m.uid_ranges, m.root_squash, m.path_caps);
    Some(ConnIdentity { key_id: input.key_id, role, tenant, uid_ranges, root_squash, path_caps })
}

fn proof_ok(input: &AuthnInput, keys: &dyn KeyLookup) -> bool {
    let Some(secret) = keys.secret(input.key_id) else { return false };
    let p = ProofInput {
        client_nonce: &input.client_nonce,
        server_nonce: &input.server_nonce,
        role: input.role.as_u8(),
        cluster_id: &input.cluster_id,
        key_id: input.key_id,
    };
    verify_auth_proof(&secret, &p, input.proof)
}

#[cfg(test)]
mod tests;
