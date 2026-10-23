//! 依赖元数据的管理端点（design.md 7.1，M7）：目录属性 `GET/POST/DELETE /api/v1/attr`、递归用量 `GET /api/v1/usage`、声明 vs 实际
//! `GET /api/v1/status`、语义 `GET /api/v1/semantics`、集群运行参数 `POST /api/v1/cluster/settings`；changelog 与游标在 `changelog.rs`。
//!
//! - **只有就绪的元数据 leader 回答**：别的节点回 `409 {"error":"not_leader","leader":"http://<管理地址>"}`（ApiClient 自动跟随）；
//!   还不知道 leader、或 leader 刚当选还没就绪 → `503`。
//! - 读一律是线性一致读（`MetaEngine::read`，在管理 API 线程上）；写是 seq 0 的执行器闭包（`txn_blocking`），批次作废（`RETRY`）或
//!   执行器队列满（`BUSY`）时退避重试几次——没有应用过，重来是安全的；结果不确定（`NOT_LEADER`）或重试用尽就如实报错，不自动重做，
//!   也不拿闭包按批次覆盖层算出的业务错误当结论（[`settle_write`]）。
//! - 身份（实现注记（M7））：管理 API 的请求以 uid 0 解析路径（3.8"权限"：读对所有已认证的 key 开放，写要 admin——签名校验时已查）；
//!   changelog 里记的是 uid 0 与这把 key 的租户。
//! - 错误：`{"error": 错误码名的小写, "message": 中文原因}`；状态码：`ENOENT` 404、`EACCES/EPERM` 403、不是 leader 409、
//!   `NOT_READY/RETRY/BUSY` 503、其余参数类错误 400、内部错误 500。

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use raft::StateRole;

use super::auth::{self, Caller};
use super::http::{MgmtCtx, Reply, err, json};
use crate::api::types::{AttrSetReq, NotLeaderReply, SemanticsReply};
use crate::api::url::param;
use crate::base::types::{Principal, ReqCtx};
use crate::base::{ErrCode, Failed, clock};
use crate::hooks::traits::ConnIdentity;
use crate::meta::attr_ops;
use crate::meta::attr_registry::split;
use crate::meta::cluster::settings;
use crate::meta::engine::EmbeddedEngine;
use crate::meta::engine::txn::{KvScan, KvTxn, MetaEngine, OpCtx, TxnOp, txn_blocking};
use crate::proto::meta::MetaReply;

/// 这个路径归这里（或 `changelog.rs`）管吗。
pub fn handles(path: &str) -> bool {
    matches!(
        path,
        "/api/v1/attr" | "/api/v1/usage" | "/api/v1/status" | "/api/v1/semantics" | "/api/v1/cluster/settings"
    ) || path.starts_with("/api/v1/changelog")
}

/// 签名校验 → 有元数据引擎 → 本节点是就绪的 leader → 按 (方法, 路径) 分派（见文件头）。
pub(super) fn serve(ctx: &MgmtCtx, method: &str, url: &str, auth_hdr: Option<&str>, body: &[u8]) -> Reply {
    let (path, query) = url.split_once('?').unwrap_or((url, ""));
    let caller = match auth::check(&*ctx.keys, method, url, auth_hdr, body, method != "GET") {
        Ok(c) => c,
        Err(d) => return err(d.status, d.code, d.message),
    };
    let Some(eng) = &ctx.meta else {
        return err(503, "not_ready", "本节点没有元数据引擎".into());
    };
    if let Some(r) = leader_gate(eng) {
        return r;
    }
    let req = req_ctx(&caller);
    let q = |name: &str| param(query, name);
    if path.starts_with("/api/v1/changelog") {
        return super::changelog::serve(ctx, eng, &req, method, path, query, body);
    }
    let r = match (method, path) {
        ("GET", "/api/v1/attr") => {
            let p = q("path").unwrap_or_default();
            let recursive = q("recursive").is_some_and(|x| x == "1");
            read(eng, &req, |v, c| attr_ops::get(v, c, &p, recursive)).map(|x| json(200, &x))
        }
        ("POST", "/api/v1/attr") => match serde_json::from_slice::<AttrSetReq>(body) {
            Ok(a) => attr_set(eng, &req, a),
            Err(e) => Err(Failed::new(ErrCode::EINVAL, format!("body 应为 {{\"path\": …, \"spec\": …}}：{e}"))),
        },
        ("DELETE", "/api/v1/attr") => {
            let p = q("path").unwrap_or_default();
            let k = q("key").unwrap_or_default();
            write(eng, &req, move |v, c| attr_ops::unset(v, c, &p, &k)).map(|x| json(200, &x))
        }
        ("GET", "/api/v1/usage") => {
            let p = q("path").unwrap_or_default();
            read(eng, &req, |v, c| attr_ops::usage(v, c, &p)).map(|x| json(200, &x))
        }
        ("GET", "/api/v1/status") => {
            let p = q("path").unwrap_or_else(|| "/".into());
            read(eng, &req, |v, c| attr_ops::status(v, c, &p)).map(|x| json(200, &x))
        }
        ("GET", "/api/v1/semantics") => read(eng, &req, |_v, c| Ok(semantics(c))).map(|x| json(200, &x)),
        ("POST", "/api/v1/cluster/settings") => cluster_set(eng, &req, body),
        _ => Err(Failed::new(ErrCode::EINVAL, format!("{path} 不支持 {method}"))),
    };
    r.unwrap_or_else(failed)
}

fn attr_set(eng: &EmbeddedEngine, req: &ReqCtx, a: AttrSetReq) -> Result<Reply, Failed> {
    let items = split(&a.spec)?; // 服务端切分、解析、规范化（3.8）
    let path = a.path;
    write(eng, req, move |v, c| attr_ops::set(v, c, &path, &items)).map(|x| json(200, &x))
}

fn cluster_set(eng: &EmbeddedEngine, req: &ReqCtx, body: &[u8]) -> Result<Reply, Failed> {
    let bad = |msg: String| Failed::new(ErrCode::EINVAL, msg);
    let m: BTreeMap<String, String> =
        serde_json::from_slice(body).map_err(|e| bad(format!("body 应为 {{\"名字\": \"值\"}}：{e}")))?;
    let items = settings::parse(&m).map_err(bad)?;
    write(eng, req, move |v, c| Ok(settings::apply(v, c, &items)?)).map(|x| json(200, &x))
}

/// 不是就绪的 leader → 409（知道 leader 的管理地址）或 503。
fn leader_gate(eng: &EmbeddedEngine) -> Option<Reply> {
    let r = eng.role();
    if r.ready {
        return None;
    }
    if r.role == StateRole::Leader {
        return Some(err(503, "not_ready", "本节点刚当选元数据 leader、还没就绪，稍后重试".into()));
    }
    let leader = Some(r.leader_id).filter(|id| *id != 0).and_then(|id| eng.node_addrs(id));
    match leader.map(|a| a.mgmt_addr).filter(|a| !a.is_empty()) {
        Some(a) => {
            let message = format!("本节点不是元数据 leader，leader 的管理地址是 {a}");
            Some(json(
                409,
                &NotLeaderReply { error: "not_leader".into(), message, leader: Some(format!("http://{a}")) },
            ))
        }
        None => Some(err(503, "not_leader", "还没有元数据 leader（正在选举？可以先 wmfs dev wait leader）".into())),
    }
}

/// 管理 API 请求的上下文（见文件头的"身份"）。
fn req_ctx(c: &Caller) -> ReqCtx {
    let conn = Arc::new(ConnIdentity {
        key_id: c.key_id,
        role: c.role,
        tenant: c.tenant,
        uid_ranges: vec![(0, u32::MAX)],
        root_squash: false,
        path_caps: vec![],
    });
    let who = Principal { uid: 0, gid: 0, groups: vec![], tenant: c.tenant, job_id: 0, conn };
    ReqCtx {
        who,
        session: 0,
        seq: 0,
        pid: 0,
        trace_id: 0,
        now: clock::wall_ts(),
        now_ms: clock::wall_ms(),
        internal: false,
    }
}

/// 可以退避重试的错误（没有应用过）。
fn retryable(e: ErrCode) -> bool {
    matches!(e, ErrCode::RETRY | ErrCode::BUSY | ErrCode::NOT_READY)
}

/// 读与写最多各试几次。
const RETRIES: u32 = 5;

/// 第 `i` 次失败之后、重试之前等一会儿：20、40、80、160 ms。
fn backoff(i: u32) {
    std::thread::sleep(Duration::from_millis(20 << i));
}

/// 线性一致读：`f` 在 ReadIndex 之后的视图上算出结果（ReadIndex 超时之类的可重试错误重试几次）。
pub(super) fn read<T>(
    eng: &EmbeddedEngine,
    req: &ReqCtx,
    mut f: impl FnMut(&dyn KvScan, &OpCtx) -> Result<T, Failed>,
) -> Result<T, Failed> {
    for i in 0..RETRIES {
        let mut out = None;
        let r = eng.read(req, &mut |v, c| {
            out = Some(f(v, c));
            Ok(MetaReply::Ok)
        });
        match r {
            Ok(_) => return out.unwrap_or(Err(ErrCode::INTERNAL.into())),
            Err(e) if retryable(e) && i + 1 < RETRIES => backoff(i),
            Err(e) => return Err(e.into()),
        }
    }
    Err(ErrCode::RETRY.into())
}

/// 修改：seq 0 的执行器闭包（见文件头）。执行器的闭包只能回 `MetaReply`，所以 `f` 自己的结果（`T`，或带原因的错误）经一个容量
/// 为 1 的通道带出来；`f` 放在 `Arc` 里，是因为重试时要再交给执行器一次。
pub(super) fn write<T: Send + 'static>(
    eng: &EmbeddedEngine,
    req: &ReqCtx,
    f: impl Fn(&mut dyn KvTxn, &OpCtx) -> Result<T, Failed> + Send + Sync + 'static,
) -> Result<T, Failed> {
    let f = Arc::new(f);
    for i in 0..RETRIES {
        let (tx, rx) = crossbeam_channel::bounded(1);
        let g = f.clone();
        let op: TxnOp = Box::new(move |v, c| {
            let r = g(v, c);
            let code = r.as_ref().err().map(|e| e.code);
            let _ = tx.try_send(r);
            code.map_or(Ok(MetaReply::Ok), Err)
        });
        let r = txn_blocking(eng as &dyn MetaEngine, req.clone(), op);
        match settle_write(r, rx.try_recv().ok(), i + 1 == RETRIES) {
            Some(out) => return out,
            None => backoff(i),
        }
    }
    Err(ErrCode::RETRY.into())
}

/// 一次提交的结局 → 回给调用者的结果；None = 退避之后重试（批次作废、队列满：什么也没应用）。`mine` 是闭包自己的结果（闭包没跑
/// 就是 None），`last` = 这是最后一次。闭包的业务错误（带原因）只在批次确实有了结果、txn 回的正是闭包自己的错误码时才带出：闭包是
/// 对着批次覆盖层算的，覆盖层里还有同批前面的、可能没能提交的修改，结果不确定（`NOT_LEADER`）或重试用尽时它的 ENOENT 之类不能
/// 当成确定的结论（3.4.1.4 规则 2），如实报 txn 的错误码。
fn settle_write<T>(
    r: Result<MetaReply, ErrCode>,
    mine: Option<Result<T, Failed>>,
    last: bool,
) -> Option<Result<T, Failed>> {
    match (r, mine) {
        (Ok(_), Some(out)) => Some(out),
        (Err(e), _) if retryable(e) && !last => None,
        (Err(e), Some(Err(m))) if e == m.code => Some(Err(m)),
        (r, _) => Some(Err(r.err().unwrap_or(ErrCode::INTERNAL).into())),
    }
}

/// 失败 → HTTP 回复（见文件头的状态码）。
pub(super) fn failed(f: Failed) -> Reply {
    let status = match f.code {
        ErrCode::ENOENT => 404,
        ErrCode::EACCES | ErrCode::EPERM => 403,
        ErrCode::NOT_LEADER => 409,
        ErrCode::NOT_READY | ErrCode::RETRY | ErrCode::BUSY => 503,
        ErrCode::INTERNAL | ErrCode::EIO => 500,
        _ => 400,
    };
    let name = f.code.name().map_or(format!("e{}", f.code.0), |n| n.to_ascii_lowercase());
    let msg = if f.msg.is_empty() { format!("失败：{}", f.code) } else { f.msg };
    err(status, &name, msg)
}

/// `GET /api/v1/semantics`：要点与当前集群参数。这里的文字与 docs/semantics.md 第 2、3 节同步：改了那边（例如 M12 实现 strong 与
/// 集群锁时）要一起改这里。
fn semantics(c: &OpCtx) -> SemanticsReply {
    let s = |x: &[&str]| x.iter().map(|t| t.to_string()).collect();
    SemanticsReply {
        consistency: s(&[
            "元数据操作在元数据 leader 上线性一致；rename 对所有观察者原子",
            "cto（默认）：写者 close() 之后，之后任何节点发起的 open() 一定看到它写的数据、长度与 mtime；\
             目录项与属性缓存最长 attr_ttl（目录属性 consistency=cto,attr_ttl=…，默认 1 s，挂载参数 --max-attr-ttl 只能调小）",
            "strong：M12 起可设置（每次 read 先线性一致地取长度，每次 write 同步写全部副本并提交）",
        ]),
        relaxations: s(&[
            "atime 按 relatime，批量、尽力而为",
            "跨节点共享可写 mmap 不支持一致（strong 下 MAP_SHARED 返回 ENODEV）",
            "cto 下跨节点 O_APPEND 由各节点内核按自己缓存的长度计算偏移，不保证顺序",
            "M12 之前 fcntl 锁与 flock 只在本节点有效；flock 与 fcntl 共用一个锁空间",
            "缩短截断不是崩溃原子的（先截数据、后改长度）",
            "所有副本同时掉电时，未 fsync 的数据可能丢失",
            "已打开的文件被去掉权限后：属主的句柄照常可用，其他用户之后可能 EACCES（学 NFS）",
            "mtime/ctime 用元数据 leader 的时钟；节点间墙钟误差需 < 5 分钟",
            "inotify 只看得到本节点的变化；inode 号永不复用、generation 恒为 0",
            "完整清单见 docs/semantics.md 第 3 节",
        ]),
        settings: settings::show(c.settings),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn closure_errors_count_only_when_the_batch_really_had_that_result() {
        let enoent = || Some(Err::<(), Failed>(Failed::new(ErrCode::ENOENT, "路径 /p 不存在")));
        let r = settle_write(Err(ErrCode::ENOENT), enoent(), false).unwrap().unwrap_err();
        assert_eq!((r.code, r.msg.as_str()), (ErrCode::ENOENT, "路径 /p 不存在"), "批次应用了、闭包自己失败：带着原因");
        let r = settle_write(Err(ErrCode::NOT_LEADER), enoent(), false).unwrap().unwrap_err();
        assert_eq!(r.code, ErrCode::NOT_LEADER, "结果不确定（批次丢失）：覆盖层上算出的 ENOENT 不算数");
        assert!(settle_write(Err(ErrCode::RETRY), enoent(), false).is_none(), "批次作废：重试");
        let r = settle_write(Err(ErrCode::RETRY), enoent(), true).unwrap().unwrap_err();
        assert_eq!(r.code, ErrCode::RETRY, "重试用尽：如实报 RETRY，也不是闭包的 ENOENT");
        let r = settle_write(Err(ErrCode::EIO), Some(Ok::<u8, Failed>(7)), false).unwrap().unwrap_err();
        assert_eq!(r.code, ErrCode::EIO, "闭包成功、批次没应用（fail point）");
        assert_eq!(settle_write(Ok(MetaReply::Ok), Some(Ok::<u8, Failed>(7)), false).unwrap().unwrap(), 7);
        let r = settle_write::<u8>(Err(ErrCode::BUSY), None, false);
        assert!(r.is_none(), "执行器队列满（闭包没跑）：重试");
    }
}
