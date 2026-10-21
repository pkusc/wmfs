//! 金样（M1）：密钥派生、握手证明、管理 API 签名与 NODE 文件——这些字节一旦有集群在用就不能改
//! （改了 = 老节点 / 老密钥文件 / 老 CLI 全部认证失败），所以和消息格式一样拍下快照（design.md 3.2.11）。
//! 管理 API 签名的期望值与 wmfs-mgr 那边（dashboard/src/api/tests.rs 的 SIG_GOLDEN）是同一个数：两边各算各的，必须一致。

mod common;

use common::Golden;
use sha2::{Digest, Sha256};
use wmfs_api::auth::sign_input;
use wmfs_api::hex::hex;
use wmfs_core::init::NodeFile;
use wmfs_core::proto::handshake::*;

const MASTER: [u8; 32] = [0x11; 32];

#[test]
fn golden_auth() {
    let mut g = Golden::new("auth");
    for k in [0u32, 1, 2, 0x0102_0304] {
        g.add(&format!("secret.key{k}"), derive_secret(&MASTER, k).to_vec());
    }
    g.add("K_cap", cap_key(&MASTER).to_vec());
    g.add("K_wrap", wrap_key(&MASTER).to_vec());
    // 相邻字段取不同的值：nonce、角色、集群、key id 任何一个换了位置或宽度都会让证明变
    let secret = derive_secret(&MASTER, 2);
    let p = ProofInput {
        client_nonce: &[0xA1; 16],
        server_nonce: &[0xB2; 16],
        role: 2,
        cluster_id: &[0xC3; 16],
        key_id: 0x0102_0304,
    };
    g.add("auth_proof", auth_proof(&secret, &p).to_vec());
    g.add("server_proof", server_proof(&secret, &[0xB2; 16], &[0xA1; 16]).to_vec());
    // 管理 API 签名（7.1）：与 wmfs-mgr 的金样同一组输入
    let text = sign_input("POST", "/api/v1/failpoints", 1_792_454_400_123, &hex(&Sha256::digest(b"{}")));
    let sig = hmac_sha256(&[0x11; 32], &[text.as_bytes()]);
    assert_eq!(
        hex(&sig),
        "d4713d8b4cce54afac1da24bed20a6871f5a7ea00570cb2926ca58c1c09fc163",
        "与 wmfs-mgr 的 SIG_GOLDEN 不一致"
    );
    g.add("mgmt_sig", sig.to_vec());
    // NODE 文件（3.2.2）：TOML 文本
    let node = NodeFile { format: 1, cluster_id: "c1".repeat(16), node_id: 7, created_ms: 1_792_454_400_000 };
    g.add("NODE", toml::to_string(&node).unwrap().into_bytes());
    g.check();
}
