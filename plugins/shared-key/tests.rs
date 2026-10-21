//! shared-key 的单元测试：manifest、正确证明通过、错误证明/撤销/不存在的 key 被拒、key 0 = Server、server_proof 可验。

use wmfs_core::hooks::manifest::check_manifest_toml;
use wmfs_core::meta::schema::records::KeyMetaV1;
use wmfs_core::proto::handshake::{auth_proof, derive_secret, verify_server_proof};

use super::*;

const MASTER: [u8; 32] = [9; 32];

struct Keys;
impl KeyLookup for Keys {
    fn secret(&self, key_id: u32) -> Option<[u8; 32]> {
        (key_id <= 3).then(|| derive_secret(&MASTER, key_id))
    }
    fn meta(&self, key_id: u32) -> Option<KeyMetaV1> {
        let role = match key_id {
            1 => 1,
            2 | 3 => 2,
            _ => return None,
        };
        let uid_ranges = vec![(1000, 1999)];
        let revoked = key_id == 3;
        Some(KeyMetaV1 {
            name: "k".into(),
            role,
            tenant: 5,
            uid_ranges,
            root_squash: true,
            path_caps: vec![(16, true)],
            revoked,
            created_ms: 0,
        })
    }
}

/// 用 key_id 的真实秘密（或 `secret` 指定的错误秘密）造一份 AuthnInput 的证明。
fn proof(key_id: u32, role: Role, secret: [u8; 32]) -> [u8; 32] {
    let p =
        ProofInput { client_nonce: &[1; 16], server_nonce: &[2; 16], role: role.as_u8(), cluster_id: &[3; 16], key_id };
    auth_proof(&secret, &p)
}

fn input(key_id: u32, role: Role, proof: &[u8; 32]) -> AuthnInput<'_> {
    AuthnInput { key_id, role, cluster_id: [3; 16], client_nonce: [1; 16], server_nonce: [2; 16], proof, peer: "test" }
}

#[test]
fn manifest_toml_matches_code() {
    check_manifest_toml(&MANIFEST, include_str!("manifest.toml")).unwrap();
}

#[test]
fn correct_proof_is_accepted_with_keymeta_identity() {
    let sk = SharedKey::new();
    let pr = proof(2, Role::Client, derive_secret(&MASTER, 2));
    let AuthnResult::Accept(id) = sk.authenticate(&input(2, Role::Client, &pr), &Keys) else { panic!("应接受") };
    assert_eq!((id.key_id, id.role, id.tenant, id.root_squash), (2, Role::Client, 5, true));
    assert_eq!((id.uid_ranges, id.path_caps), (vec![(1000, 1999)], vec![(16, true)]));
    let pr0 = proof(0, Role::Server, derive_secret(&MASTER, 0));
    let AuthnResult::Accept(id0) = sk.authenticate(&input(0, Role::Server, &pr0), &Keys) else {
        panic!("key 0 应接受")
    };
    assert_eq!(id0.role, Role::Server, "key 0 固定是 Server，不查 KEYMETA");
    let sp = sk.server_proof(&input(2, Role::Client, &pr), &Keys).unwrap();
    assert!(verify_server_proof(&derive_secret(&MASTER, 2), &[2; 16], &[1; 16], &sp), "客户端能验证服务端的证明");
}

#[test]
fn wrong_secret_revoked_or_unknown_keys_are_rejected() {
    let sk = SharedKey::new();
    let rejected = |key_id, role, secret| {
        let pr = proof(key_id, role, secret);
        matches!(sk.authenticate(&input(key_id, role, &pr), &Keys), AuthnResult::Reject)
    };
    assert!(rejected(2, Role::Client, [0x77; 32]), "错误的秘密（例如 wmfs debug gen-key 生成的 key）");
    assert!(rejected(2, Role::Client, derive_secret(&[8; 32], 2)), "别的集群的 key");
    assert!(rejected(3, Role::Client, derive_secret(&MASTER, 3)), "已撤销");
    assert!(rejected(9, Role::Client, derive_secret(&MASTER, 9)), "不存在");
    // 证明里绑定的角色与输入的角色不同（有人在路上改了 Hello.role）
    let pr = proof(2, Role::Client, derive_secret(&MASTER, 2));
    assert!(matches!(sk.authenticate(&input(2, Role::Server, &pr), &Keys), AuthnResult::Reject));
}
