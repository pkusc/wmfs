//! 握手、认证与能力协商的消息（design.md 3.3.2）与 Ping（3.3.3）。
//!
//! 握手由 core 在传输插件交出的 `Conn` 上执行，与传输无关（TCP、RDMA、mem 完全相同）：
//! ```text
//! C → S  Hello     { proto_min, proto_max, features, required, role, cluster_id, key_id, client_nonce, software }
//! S → C  Challenge { proto, features /*双方交集*/, cluster_id, node_id, server_nonce }
//! C → S  Auth      { proof }   // HMAC-SHA256(secret(key_id), "wmfs-auth-v1" ‖ client_nonce ‖ server_nonce ‖ role ‖ cluster_id ‖ key_id)
//! S → C  Welcome   { key_id, role /*同 Hello*/, tenant, server_proof }  // HMAC(secret(key_id), "wmfs-srv-v1" ‖ server_nonce ‖ client_nonce)
//!        或 ERROR(AUTH_FAILED / PROTO_UNSUPPORTED)，然后断开
//! ```
//! - 未完成认证的连接只允许握手帧；认证完成后 core 调用 `Conn::mark_authenticated()`。
//! - 角色规则（core 强制）：`key_id == 0` ⇒ 角色必须是 Server；`key_id ≥ 1` ⇒ KEYMETA 里有它、未撤销、
//!   且 `KEYMETA.role == Hello.role`；authn 插件 Accept 之后 core 再核对 `ConnIdentity.role == Hello.role`。
//! - 密钥派生（只有 cluster.key 是秘密根）：`secret(k) = HMAC-SHA256(K_master, "wmfs-key-v1" ‖ k_be32)`；
//!   `K_cap = HMAC(K_master, "wmfs-cap-v1")`；`K_wrap = HMAC(K_master, "wmfs-wrap-v1")`。
//! - 版本：帧头 proto_major 必须相等；握手在 [min, max] 交集里选最高（首发只有 1）。
//! - 首发不加密线上流量（无 TLS）：只在可信网络使用（ADR-0009）。
//!
//! 证明与派生的计算在 M1 实现（shared-key 插件与 core/proto）。

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Hello {
    pub proto_min: u8,
    pub proto_max: u8,
    pub features: u64,
    pub required: u64,
    pub role: u8,             // Role::as_u8()：1 admin 2 client 3 server
    pub cluster_id: [u8; 16], // 未知全 0
    pub key_id: u32,
    pub client_nonce: [u8; 16],
    pub software: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Challenge {
    pub proto: u8,
    pub features: u64, // 双方交集
    pub cluster_id: [u8; 16],
    pub node_id: u32,
    pub server_nonce: [u8; 16],
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Auth {
    pub proof: [u8; 32], // HMAC-SHA256(secret(key_id), "wmfs-auth-v1" ‖ client_nonce ‖ server_nonce ‖ role ‖ cluster_id ‖ key_id)
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Welcome {
    pub key_id: u32,
    pub role: u8, // 同 Hello
    pub tenant: u32,
    pub server_proof: [u8; 32], // HMAC(secret(key_id), "wmfs-srv-v1" ‖ server_nonce ‖ client_nonce)
}

/// Ping（0x0005）：`PingBody { payload }` + 可选 bulk，原样回显（传输自检、测延迟带宽）。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct PingBody {
    pub payload: Vec<u8>,
}
