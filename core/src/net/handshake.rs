//! 3.3.2 握手：由 core 在传输插件交出的 `Conn` 上执行，与传输无关（TCP、RDMA、mem 完全相同）。
//!
//! 两边都在该连接的**读线程**上执行：读帧用 `recv_checked`，发帧经 `ConnHandle`（写线程）。
//! ```text
//! C → S  Hello(req 1) → S → C  Challenge | ERROR(PROTO_UNSUPPORTED / AUTH_FAILED)
//! C → S  Auth(req 2)  → S → C  Welcome   | ERROR(AUTH_FAILED)，然后断开
//! ```
//! 角色规则由 core 强制（与 authn 插件无关）：`key_id == 0` ⇒ 角色必须是 Server；`key_id ≥ 1` ⇒ KEYMETA 里有它、未撤销、
//! 且 `KEYMETA.role == Hello.role`；authn 插件 Accept 之后再核对 `ConnIdentity.role == Hello.role`。于是 client key
//! 不可能冒充 server。未完成认证的连接只允许握手帧，其他帧直接断开（计数 `wmfs_conn_handshake_failed_total`）。
//! 握手成功后两端都调用 `ConnHandle::authenticated`（传输插件从此才收带 bulk 的帧，客户端写线程从此才发保活 Ping）。
//! 实现注记（M1）：握手失败时 ERROR 帧的 `msg_type` 沿用请求的（Hello 或 Auth），`kind = Response`、flags 带 ERROR。

use wmfs_api::metrics;
use wmfs_api::types::KeyFile;

use crate::base::types::Role;
use crate::base::{ErrCode, rand};
use crate::hooks::pipeline::Pipeline;
use crate::hooks::traits::{AuthnInput, Conn, ConnIdentity, KeyLookup};
use crate::net::conn::{ConnHandle, recv_checked};
use crate::proto::frame::{
    ErrorBody, FLAG_ERROR, FrameHeader, FrameKind, InFrame, MSG_AUTH, MSG_CHALLENGE, MSG_HELLO, MSG_WELCOME, OutFrame,
    PROTO_MAJOR,
};
use crate::proto::handshake::{
    Auth, Challenge, Hello, ProofInput, SERVER_KEY, Welcome, auth_proof, verify_server_proof,
};
use crate::proto::{decode_body, encode_body};

/// 本端支持的协议 feature 位（3.3.2；随功能实现逐个加入：M1 还一个都没有）。
pub(crate) const OUR_FEATURES: u64 = 0;
const SOFTWARE: &str = concat!("wmfs ", env!("CARGO_PKG_VERSION"));

/// 服务端握手需要的东西。
pub(crate) struct ServerSide<'a> {
    pub cluster_id: [u8; 16],
    pub node_id: u32,
    pub pipeline: &'a Pipeline,
    pub keys: &'a dyn KeyLookup,
}

/// 服务端握手。成功返回连接身份（已记进句柄、已 `mark_authenticated`）；失败时 ERROR 帧（如果该回）已进发送队列，
/// 调用方直接返回、丢掉句柄即可——写线程把它写完后关闭连接。
pub(crate) fn server(h: &ConnHandle, conn: &dyn Conn, s: &ServerSide) -> Result<ConnIdentity, ErrCode> {
    let f = next(conn, MSG_HELLO)?;
    let hello: Hello = decode_body(&f.body).map_err(|_| reject(h, &f.hdr, ErrCode::PROTO_UNSUPPORTED))?;
    let role = check_hello(&hello, s).map_err(|e| reject(h, &f.hdr, e))?;
    let mut server_nonce = [0u8; 16];
    rand::fill(&mut server_nonce);
    let features = hello.features & OUR_FEATURES;
    let ch = Challenge { proto: PROTO_MAJOR, features, cluster_id: s.cluster_id, node_id: s.node_id, server_nonce };
    h.send(frame(FrameKind::Response, MSG_CHALLENGE, f.hdr.req_id, encode_body(&ch)))?;

    let f = next(conn, MSG_AUTH)?;
    let auth: Auth = decode_body(&f.body).map_err(|_| reject(h, &f.hdr, ErrCode::PROTO_UNSUPPORTED))?;
    let input = AuthnInput {
        key_id: hello.key_id,
        role,
        cluster_id: hello.cluster_id,
        client_nonce: hello.client_nonce,
        server_nonce,
        proof: &auth.proof,
        peer: h.peer(),
    };
    let (id, server_proof) = s.pipeline.authn(&input, s.keys).map_err(|e| reject(h, &f.hdr, e))?;
    if id.role != role {
        return Err(reject(h, &f.hdr, ErrCode::AUTH_FAILED));
    }
    let w = Welcome { key_id: hello.key_id, role: role.as_u8(), tenant: id.tenant, server_proof };
    h.send(frame(FrameKind::Response, MSG_WELCOME, f.hdr.req_id, encode_body(&w)))?;
    h.authenticated(Some(id.clone()));
    Ok(id)
}

/// 版本、feature、集群、角色规则（core 强制的部分）。
fn check_hello(hello: &Hello, s: &ServerSide) -> Result<Role, ErrCode> {
    if hello.proto_min > PROTO_MAJOR || hello.proto_max < PROTO_MAJOR || hello.required & !OUR_FEATURES != 0 {
        return Err(ErrCode::PROTO_UNSUPPORTED);
    }
    let role = Role::from_u8(hello.role).ok_or(ErrCode::AUTH_FAILED)?;
    if hello.cluster_id != [0; 16] && hello.cluster_id != s.cluster_id {
        return Err(ErrCode::AUTH_FAILED); // 连错了集群
    }
    let ok = if hello.key_id == SERVER_KEY {
        role == Role::Server
    } else {
        s.keys.meta(hello.key_id).is_some_and(|m| !m.revoked && m.role == hello.role)
    };
    if ok { Ok(role) } else { Err(ErrCode::AUTH_FAILED) }
}

/// 读下一帧，必须是指定类型的握手请求；连接出错或来了别的帧 → 返回错误（调用方直接断开，不回 ERROR）。
fn next(conn: &dyn Conn, want: u16) -> Result<InFrame, ErrCode> {
    let f = recv_checked(conn)?;
    if f.hdr.kind != FrameKind::Request || f.hdr.msg_type != want {
        count_failed(ErrCode::PROTO_UNSUPPORTED); // 未认证的连接只允许握手帧：直接断开
        return Err(ErrCode::PROTO_UNSUPPORTED);
    }
    Ok(f)
}

/// 把 ERROR 帧放进发送队列并计数，返回错误码本身（给 `map_err` 用）。
fn reject(h: &ConnHandle, req: &FrameHeader, code: ErrCode) -> ErrCode {
    count_failed(code);
    let mut f = frame(FrameKind::Response, req.msg_type, req.req_id, encode_body(&ErrorBody::new(code, 0)));
    f.hdr.flags = FLAG_ERROR;
    let _ = h.send(f);
    code
}

fn count_failed(code: ErrCode) {
    let name = code.to_string();
    metrics::counter("wmfs_conn_handshake_failed_total", "握手被拒的连接数", &[("code", &name)]).inc();
}

fn frame(kind: FrameKind, msg: u16, req_id: u64, body: Vec<u8>) -> OutFrame {
    OutFrame { hdr: FrameHeader::new(kind, msg, req_id, 0), body, bulk: vec![], recv_into: None }
}

/// 客户端握手需要的东西。
pub(crate) struct ClientSide<'a> {
    pub key: &'a KeyFile,
    pub role: Role,
    pub cluster_id: [u8; 16], // 未知全 0
}

/// 客户端握手：发 Hello、算证明、校验 `server_proof`（双向认证，防止连到冒充的服务端）。
/// 服务端回的 ERROR 原样变成错误码（`AUTH_FAILED`、`PROTO_UNSUPPORTED`）。
pub(crate) fn client(h: &ConnHandle, conn: &dyn Conn, c: &ClientSide) -> Result<(), ErrCode> {
    let mut client_nonce = [0u8; 16];
    rand::fill(&mut client_nonce);
    let (role, key_id) = (c.role.as_u8(), c.key.key_id);
    let hello = Hello {
        proto_min: PROTO_MAJOR,
        proto_max: PROTO_MAJOR,
        features: OUR_FEATURES,
        required: 0,
        role,
        cluster_id: c.cluster_id,
        key_id,
        client_nonce,
        software: SOFTWARE.into(),
    };
    h.send(frame(FrameKind::Request, MSG_HELLO, 1, encode_body(&hello)))?;
    let ch: Challenge = answer(conn, MSG_CHALLENGE)?;
    if ch.proto != PROTO_MAJOR {
        return Err(ErrCode::PROTO_UNSUPPORTED);
    }
    let p = ProofInput {
        client_nonce: &client_nonce,
        server_nonce: &ch.server_nonce,
        role,
        cluster_id: &c.cluster_id,
        key_id,
    };
    h.send(frame(FrameKind::Request, MSG_AUTH, 2, encode_body(&Auth { proof: auth_proof(&c.key.secret, &p) })))?;
    let w: Welcome = answer(conn, MSG_WELCOME)?;
    let proof_ok = verify_server_proof(&c.key.secret, &ch.server_nonce, &client_nonce, &w.server_proof);
    if w.key_id != key_id || w.role != role || !proof_ok {
        return Err(ErrCode::AUTH_FAILED);
    }
    h.authenticated(None);
    Ok(())
}

/// 读服务端的握手回复：ERROR → 它的错误码；类型不对 → PROTO_UNSUPPORTED。
fn answer<T: serde::de::DeserializeOwned>(conn: &dyn Conn, want: u16) -> Result<T, ErrCode> {
    let f = recv_checked(conn)?;
    if let Some(e) = f.error() {
        return Err(ErrCode(e.code));
    }
    if f.hdr.kind != FrameKind::Response || f.hdr.msg_type != want {
        return Err(ErrCode::PROTO_UNSUPPORTED);
    }
    decode_body(&f.body).map_err(|_| ErrCode::PROTO_UNSUPPORTED)
}
