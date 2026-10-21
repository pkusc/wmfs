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
//! 证明与派生的计算在本文件末尾（M1）：client 端（core 的 RpcClient）与 authn 插件（shared-key）用同一组函数，
//! 两边不会各拼各的字节。实现注记（M1）：规格没写 `proof` 输入里整数的字节序，取 `role` 1 字节、`key_id` **大端** 4 字节
//! （与 `secret(k)` 的 `k_be32` 一致），`cluster_id` 是 Hello 里的 16 字节（未知时全 0）；金样在 `tests/golden/auth.golden`。

use hmac::{Hmac, KeyInit, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

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

// ---- 密钥派生与证明（3.3.2）----

/// HMAC-SHA256(key, parts 依次拼接)。
pub fn hmac_sha256(key: &[u8], parts: &[&[u8]]) -> [u8; 32] {
    mac(key, parts).finalize().into_bytes().into()
}

/// 常数时间校验 `tag == HMAC-SHA256(key, parts…)`（不会因为前几个字节对了就早退，防计时攻击）。
pub fn hmac_verify(key: &[u8], parts: &[&[u8]], tag: &[u8]) -> bool {
    mac(key, parts).verify_slice(tag).is_ok()
}

fn mac(key: &[u8], parts: &[&[u8]]) -> Hmac<Sha256> {
    let mut m = <Hmac<Sha256> as KeyInit>::new_from_slice(key)
        .unwrap_or_else(|_| panic!("不变量破坏：HMAC 接受任意长度的密钥"));
    for p in parts {
        m.update(p);
    }
    m
}

/// `secret(k) = HMAC-SHA256(K_master, "wmfs-key-v1" ‖ k_be32)`：key k 的秘密（密钥文件里存的就是它）。
pub fn derive_secret(master: &[u8; 32], key_id: u32) -> [u8; 32] {
    hmac_sha256(master, &[b"wmfs-key-v1", &key_id.to_be_bytes()])
}

/// `K_cap = HMAC(K_master, "wmfs-cap-v1")`：能力令牌的签名密钥（M5 起用）。
pub fn cap_key(master: &[u8; 32]) -> [u8; 32] {
    hmac_sha256(master, &[b"wmfs-cap-v1"])
}

/// `K_wrap = HMAC(K_master, "wmfs-wrap-v1")`：包裹数据加密密钥用（M11 起用）。
pub fn wrap_key(master: &[u8; 32]) -> [u8; 32] {
    hmac_sha256(master, &[b"wmfs-wrap-v1"])
}

/// Auth 消息里的证明要覆盖的字段（Hello 与 Challenge 里的值）。
pub struct ProofInput<'a> {
    pub client_nonce: &'a [u8; 16],
    pub server_nonce: &'a [u8; 16],
    pub role: u8,
    pub cluster_id: &'a [u8; 16],
    pub key_id: u32,
}

/// `proof = HMAC-SHA256(secret, "wmfs-auth-v1" ‖ client_nonce ‖ server_nonce ‖ role ‖ cluster_id ‖ key_id_be32)`。
pub fn auth_proof(secret: &[u8; 32], p: &ProofInput) -> [u8; 32] {
    hmac_sha256(secret, &[&auth_msg(p)])
}

/// 常数时间校验客户端的证明（authn 插件用）。
pub fn verify_auth_proof(secret: &[u8; 32], p: &ProofInput, proof: &[u8; 32]) -> bool {
    hmac_verify(secret, &[&auth_msg(p)], proof)
}

/// 证明覆盖的字节（安全契约，金样 `tests/golden/auth.golden`），按顺序：`"wmfs-auth-v1"`、client_nonce（16）、
/// server_nonce（16）、role（1 字节）、cluster_id（16）、key_id（4 字节大端）。
fn auth_msg(p: &ProofInput) -> Vec<u8> {
    let mut m = b"wmfs-auth-v1".to_vec();
    m.extend_from_slice(p.client_nonce);
    m.extend_from_slice(p.server_nonce);
    m.push(p.role);
    m.extend_from_slice(p.cluster_id);
    m.extend_from_slice(&p.key_id.to_be_bytes());
    m
}

/// `server_proof = HMAC(secret, "wmfs-srv-v1" ‖ server_nonce ‖ client_nonce)`：客户端用它确认对面真的有这把 key（双向认证）。
pub fn server_proof(secret: &[u8; 32], server_nonce: &[u8; 16], client_nonce: &[u8; 16]) -> [u8; 32] {
    hmac_sha256(secret, &[b"wmfs-srv-v1", server_nonce, client_nonce])
}

/// 常数时间校验服务端的证明（客户端用）。
pub fn verify_server_proof(secret: &[u8; 32], server_nonce: &[u8; 16], client_nonce: &[u8; 16], tag: &[u8]) -> bool {
    hmac_verify(secret, &[b"wmfs-srv-v1", server_nonce, client_nonce], tag)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hmac_matches_rfc4231_case_2() {
        // RFC 4231 测试向量 2：key = "Jefe"，data = "what do ya want for nothing?"
        let got = hmac_sha256(b"Jefe", &[b"what do ya ", b"want for nothing?"]);
        let want = "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843";
        assert_eq!(wmfs_api::hex::hex(&got), want);
        assert!(hmac_verify(b"Jefe", &[b"what do ya want for nothing?"], &got));
        assert!(!hmac_verify(b"Jefe", &[b"what do ya want for nothing!"], &got));
        assert!(!hmac_verify(b"Jefe", &[b"what do ya want for nothing?"], &got[..31]), "截短的 tag 必须失败");
    }

    #[test]
    fn proofs_bind_every_field() {
        let secret = derive_secret(&[7; 32], 2);
        assert_ne!(secret, derive_secret(&[7; 32], 1), "不同 key id 的秘密不同");
        let (cn, sn, cid) = ([1u8; 16], [2u8; 16], [3u8; 16]);
        let p = ProofInput { client_nonce: &cn, server_nonce: &sn, role: 2, cluster_id: &cid, key_id: 2 };
        let proof = auth_proof(&secret, &p);
        assert!(verify_auth_proof(&secret, &p, &proof));
        // 换掉任何一个字段（例如 client key 想冒充 server 角色），证明都不再成立
        let q = ProofInput { role: 3, ..p };
        assert!(!verify_auth_proof(&secret, &q, &proof));
        let q = ProofInput { key_id: 3, ..p };
        assert!(!verify_auth_proof(&secret, &q, &proof));
        assert!(!verify_auth_proof(&derive_secret(&[8; 32], 2), &p, &proof), "别的集群的 key 不行");
        let sp = server_proof(&secret, &sn, &cn);
        assert!(verify_server_proof(&secret, &sn, &cn, &sp));
        assert!(!verify_server_proof(&secret, &cn, &sn, &sp), "两个 nonce 的顺序也被绑定");
        assert_ne!(cap_key(&[7; 32]), wrap_key(&[7; 32]));
    }
}
