//! M10 的管理端点（design.md 7.1、3.4.3、3.4.4）：认证 key 与会话。只有就绪的元数据 leader 回答（`api.rs` 的 leader_gate）。
//!
//! - `GET /api/v1/keys`：全部 key（不含秘密；path_caps 显示成根目录现在的路径）。
//! - `POST /api/v1/keys {name, role, tenant, uids, paths, paths_ro, root_squash}`（admin）：一个批次——`next_key` 取号、写 KEYMETA
//!   （`paths`/`paths_ro` 按 uid 0 解析成**目录**的 inode 号：目录被 rename 后限制跟着它）、`map_epoch += 1`、changelog `KeyCreate`；
//!   回复带一次性的 key 文件内容（秘密由 cluster.key 派生，服务端不存，3.4.3）。`uids` 不给 = 不限。
//! - `POST /api/v1/keys/<id>/revoke`（admin）：一个批次——`revoked = true`、`map_epoch += 1`、changelog `KeyRevoke`；之后驱逐这把 key
//!   的全部会话（第一阶段，`meta/evict.rs`；撤销之后它开不了新会话，所以撤销之后扫一遍不会漏）。已撤销的 key 签名的管理请求一律 401；
//!   用它建立的连接在各节点拿到新集群图之后、收到下一帧时关闭（ADR-0009，`net/server.rs`），在那之前上面的请求随会话被驱逐而失败
//!   （`SESSION_EVICTED`，重开会话 → `EACCES`）。最后一把有效的 admin key 不能撤销（`EPERM`：否则集群再也管不了）。
//! - `GET /api/v1/sessions`：全部会话与 24 h 内的 EVICTED 记录；`POST /api/v1/sessions/<id>/evict`（admin，id 是十六进制）：手动驱逐
//!   （回复时第一阶段已完成，第二阶段由 cluster-mgr 在数据节点确认之后做）。

use std::collections::HashSet;

use super::api::{failed_at, read, write};
use super::http::{MgmtCtx, Reply, json};
use crate::api::types::{
    KeyCreateReply, KeyCreateReq, KeyFile, KeyInfo, KeyRevokeReply, KeysReply, PathCapInfo, SessionRow, SessionsReply,
};
use crate::base::types::{ReqCtx, Role};
use crate::base::{ErrCode, Failed};
use crate::meta::changelog::{self, Change};
use crate::meta::engine::EmbeddedEngine;
use crate::meta::engine::txn::KvScan;
use crate::meta::evict::{self, EVICTED_KEEP_MS};
use crate::meta::ns::read_ops::{node_at, path_of};
use crate::meta::schema::keys::{self, G_NEXT_KEY, T_EVICTED, T_KEYMETA, T_SESSION};
use crate::meta::schema::kv;
use crate::meta::schema::records::{EvictedV1, KeyMetaV1, SessionRecV1, decode_v1};
use crate::meta::sessions::SESSION_ACTIVE;

/// 这个路径归这里管吗。
pub fn handles(path: &str) -> bool {
    matches!(path, "/api/v1/keys" | "/api/v1/sessions")
        || path.starts_with("/api/v1/keys/")
        || path.starts_with("/api/v1/sessions/")
}

/// leader_gate 之后的分派：按 (方法, 路径) 找处理函数（`/keys/<id>/revoke`、`/sessions/<id>/evict` 的 id 一起取出来）。
pub(super) fn serve(ctx: &MgmtCtx, eng: &EmbeddedEngine, req: &ReqCtx, method: &str, path: &str, body: &[u8]) -> Reply {
    let id_in = |prefix: &str, suffix: &str| path.strip_prefix(prefix).and_then(|p| p.strip_suffix(suffix));
    let r = match (method, path, id_in("/api/v1/keys/", "/revoke"), id_in("/api/v1/sessions/", "/evict")) {
        ("GET", "/api/v1/keys", ..) => read(eng, req, |v, _c| list_keys(v)).map(|x| json(200, &x)),
        ("POST", "/api/v1/keys", ..) => match serde_json::from_slice::<KeyCreateReq>(body) {
            Ok(k) => create(ctx, eng, req, k),
            Err(e) => Err(bad(format!("body 应为 {{\"name\": …, \"role\": …}}：{e}"))),
        },
        ("GET", "/api/v1/sessions", ..) => read(eng, req, |v, c| sessions(v, c.req.now_ms)).map(|x| json(200, &x)),
        ("POST", _, Some(id), _) => {
            id.parse().map_err(|_| bad(format!("key 号 {id:?} 不是数字"))).and_then(|id| revoke(eng, req, id))
        }
        ("POST", _, _, Some(id)) => evict_one(eng, req, id),
        _ => Err(bad(format!("{path} 不支持 {method}"))),
    };
    r.unwrap_or_else(|f| failed_at(eng, f))
}

/// `POST /api/v1/sessions/<id>/evict`：手动驱逐（回复时第一阶段已完成）。
fn evict_one(eng: &EmbeddedEngine, req: &ReqCtx, id: &str) -> Result<Reply, Failed> {
    let sid = u64::from_str_radix(id, 16).map_err(|_| bad(format!("会话号 {id:?} 不是十六进制")))?;
    let why = format!("管理员手动驱逐（key {}）", req.who.conn.key_id);
    evict::evict(eng, sid, why).map_err(|e| no_session(e, sid))?;
    Ok(json(200, &SessionRow { id: format!("{sid:x}"), state: "evicting".into(), ..Default::default() }))
}

fn bad(msg: String) -> Failed {
    Failed::new(ErrCode::EINVAL, msg)
}

fn no_session(e: ErrCode, sid: u64) -> Failed {
    if e == ErrCode::ENOENT {
        Failed::new(e, format!("没有会话 {sid:x}（已关闭或已驱逐完）"))
    } else {
        e.into()
    }
}

/// `1000-1999,3000` → uid 范围；空 = 不限（0 到 2^32 − 1）。
fn parse_uids(t: &str) -> Result<Vec<(u32, u32)>, Failed> {
    if t.trim().is_empty() {
        return Ok(vec![(0, u32::MAX)]);
    }
    let one = |p: &str| -> Result<(u32, u32), Failed> {
        let (a, b) = p.split_once('-').unwrap_or((p, p));
        let n = |x: &str| {
            x.trim().parse::<u32>().map_err(|_| bad(format!("uid 范围 {p:?} 不对（应形如 1000-1999 或 3000）")))
        };
        let (lo, hi) = (n(a)?, n(b)?);
        if lo > hi { Err(bad(format!("uid 范围 {p:?} 的下界大于上界"))) } else { Ok((lo, hi)) }
    };
    t.split(',').map(|p| one(p.trim())).collect()
}

fn show_uids(r: &[(u32, u32)]) -> String {
    let one = |(a, b): &(u32, u32)| if a == b { a.to_string() } else { format!("{a}-{b}") };
    r.iter().map(one).collect::<Vec<_>>().join(",")
}

fn role_name(r: u8) -> String {
    Role::from_u8(r).map_or(format!("role#{r}"), |x| x.name().into())
}

fn list_keys(v: &dyn KvScan) -> Result<KeysReply, Failed> {
    let mut out = Vec::new();
    for (k, b) in kv::scan_tag(v, T_KEYMETA)? {
        let m: KeyMetaV1 = decode_v1(&b)?;
        let cap = |(ino, ro): &(u64, bool)| PathCapInfo {
            ino: *ino,
            path: path_of(v, *ino).unwrap_or_default(),
            read_only: *ro,
        };
        out.push(KeyInfo {
            id: kv::key_id(&k) as u32,
            name: m.name,
            role: role_name(m.role),
            tenant: m.tenant,
            uids: show_uids(&m.uid_ranges),
            root_squash: m.root_squash,
            paths: m.path_caps.iter().map(cap).collect(),
            revoked: m.revoked,
            created_ms: m.created_ms,
        });
    }
    Ok(KeysReply { keys: out })
}

fn create(ctx: &MgmtCtx, eng: &EmbeddedEngine, req: &ReqCtx, k: KeyCreateReq) -> Result<Reply, Failed> {
    let role = Role::from_name(&k.role).filter(|r| *r != Role::Server);
    let role = role.ok_or_else(|| bad(format!("角色 {:?} 不对（client 或 admin）", k.role)))?;
    if k.name.trim().is_empty() {
        return Err(bad("key 要有名字".into()));
    }
    let uid_ranges = parse_uids(&k.uids)?;
    let id = write(eng, req, move |v, c| {
        let all = k.paths.iter().map(|p| (p, false)).chain(k.paths_ro.iter().map(|p| (p, true)));
        let mut path_caps = Vec::new();
        for (p, ro) in all {
            let n = node_at(v, c, p)?;
            if !n.is_dir() {
                return Err(Failed::new(ErrCode::ENOTDIR, format!("{p} 不是目录（路径限制给的是子树的根）")));
            }
            path_caps.push((n.ino, ro));
        }
        let id = u32::try_from(kv::next_id(v, G_NEXT_KEY)?).map_err(|_| ErrCode::ENOSPC)?;
        let m = KeyMetaV1 {
            name: k.name.clone(),
            role: role.as_u8(),
            tenant: k.tenant,
            uid_ranges: uid_ranges.clone(),
            root_squash: k.root_squash,
            path_caps,
            revoked: false,
            created_ms: c.req.now_ms,
        };
        kv::put(v, &keys::keymeta(id), &m);
        kv::bump_map_epoch(v)?; // key 在集群图里（3.3.6）
        let extra =
            vec![("key".into(), id.to_string()), ("name".into(), k.name.clone()), ("role".into(), k.role.clone())];
        changelog::append(v, c, Change { op: "KeyCreate", extra, ..Default::default() })?;
        Ok(id)
    })?;
    let secret = ctx
        .keys
        .secret(id)
        .ok_or_else(|| Failed::new(ErrCode::INTERNAL, format!("key {id} 已建好，但取不到它的秘密")))?;
    Ok(json(200, &KeyCreateReply { id, key_file: KeyFile { key_id: id, secret }.to_line() }))
}

fn revoke(eng: &EmbeddedEngine, req: &ReqCtx, id: u32) -> Result<Reply, Failed> {
    // 别的有效 admin key（批次里再逐个点查：两把 admin key 同时被撤销时不会一把都不剩）
    let others = read(eng, req, |v, _c| {
        let ok = |b: &Vec<u8>| decode_v1::<KeyMetaV1>(b).is_ok_and(|m| m.role == Role::Admin.as_u8() && !m.revoked);
        let rows = kv::scan_tag(v, T_KEYMETA)?;
        Ok(rows
            .into_iter()
            .filter(|(k, b)| kv::key_id(k) as u32 != id && ok(b))
            .map(|(k, _)| kv::key_id(&k) as u32)
            .collect::<Vec<u32>>())
    })?;
    write(eng, req, move |v, c| {
        let key = keys::keymeta(id);
        let mut m: KeyMetaV1 =
            kv::get(v, &key)?.ok_or_else(|| Failed::new(ErrCode::ENOENT, format!("没有 key {id}")))?;
        if m.revoked {
            return Ok(()); // 已经撤销过（重试）
        }
        let admin_left =
            |k: &u32| kv::get::<KeyMetaV1>(v, &keys::keymeta(*k)).ok().flatten().is_some_and(|o| !o.revoked);
        if m.role == Role::Admin.as_u8() && !others.iter().any(admin_left) {
            return Err(Failed::new(ErrCode::EPERM, "不能撤销最后一把有效的 admin key（否则集群再也管不了）"));
        }
        m.revoked = true;
        kv::put(v, &key, &m);
        kv::bump_map_epoch(v)?;
        let extra = vec![("key".into(), id.to_string()), ("name".into(), m.name.clone())];
        changelog::append(v, c, Change { op: "KeyRevoke", extra, ..Default::default() })?;
        Ok(())
    })?;
    let mine = read(eng, req, |v, _c| {
        let rows = kv::scan_tag(v, T_SESSION)?;
        let hit = |b: &Vec<u8>| decode_v1::<SessionRecV1>(b).is_ok_and(|s| s.key_id == id && s.state == SESSION_ACTIVE);
        Ok(rows.into_iter().filter(|(_, b)| hit(b)).map(|(k, _)| kv::key_id(&k)).collect::<Vec<u64>>())
    })?;
    for sid in &mine {
        match evict::evict(eng, *sid, format!("key {id} 已被撤销")) {
            Ok(()) | Err(ErrCode::ENOENT) => {} // 期间自己关掉了
            Err(e) => return Err(e.into()),
        }
    }
    Ok(json(200, &KeyRevokeReply { id, evicted_sessions: mine.len() as u32 }))
}

/// 全部会话与 24 h 内的 EVICTED 记录（见文件头）。
fn sessions(v: &dyn KvScan, now_ms: u64) -> Result<SessionsReply, Failed> {
    let (mut out, mut seen) = (Vec::new(), HashSet::new());
    for (k, b) in kv::scan_tag(v, T_SESSION)? {
        let (sid, s) = (kv::key_id(&k), decode_v1::<SessionRecV1>(&b)?);
        let active = s.state == SESSION_ACTIVE;
        seen.insert(sid);
        out.push(SessionRow {
            id: format!("{sid:x}"),
            state: if active { "active" } else { "evicting" }.into(),
            client_name: s.client_name,
            host: s.host,
            mount: s.mount,
            key_id: s.key_id,
            created_ms: s.created_ms,
            evicted_ms: (!active).then_some(s.evict_since_ms),
        });
    }
    for (k, b) in kv::scan_tag(v, T_EVICTED)? {
        let (sid, e) = (kv::key_id(&k), decode_v1::<EvictedV1>(&b)?);
        if !seen.contains(&sid) && e.evicted_ms + EVICTED_KEEP_MS >= now_ms {
            out.push(SessionRow {
                id: format!("{sid:x}"),
                state: "evicted".into(),
                evicted_ms: Some(e.evicted_ms),
                ..Default::default()
            });
        }
    }
    Ok(SessionsReply { sessions: out })
}
