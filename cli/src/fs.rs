//! `wmfs fs …`：不经 FUSE 的调试客户端（design.md 7.2）。M4 有元数据命令：`ls [-l]`、`stat`、`mkdir`、`rmdir`、`rm`、`mv`、
//! `ln [-s]`、`touch`；`cat/put/get/truncate`（数据）在 M5。
//!
//! 每条命令 = 问 health 找到元数据 leader → `MetaClient::connect`（GetClusterMap + SessionOpen）→ 按路径解析（`ResolvePath`，
//! 服务端逐段检查执行权限）→ 一个或几个元数据请求 → SessionClose。修改型请求带 seq，重试用同一个 seq（`--verbose` 打印每次
//! 重试，例如"超时重试 seq=… → 去重命中"）；总期限 30 s。身份：默认是本进程的 uid/gid/附加组与 umask（读
//! `/proc/self/status`）；`--as-uid UID[:GID]` 以别的用户身份发请求——只允许 admin key（3.3.3：只有 admin 能声明任意 uid；
//! 本机没开 `user_allow_other`，多用户演示靠它）。`--json` 输出机器可读的结果（`scripts/accept.sh` 只解析它）；失败时也是
//! JSON：`{"error": 码名, "errno", "message"}`，请求失败再加 `path`——发请求之前的失败（参数写错 `EINVAL`、client key 加
//! `--as-uid` 是 `EACCES`、找不到元数据 leader 是 `ETIMEDOUT`、认证失败 `AUTH_FAILED`）也一样。
//! 路径是文件系统里的绝对路径；`mv` 等同 rename(2)（目标是已有目录时按 rename 的规则，不会"移进去"）。

mod show;

use std::path::PathBuf;
use std::time::Duration;

use serde_json::json;
use wmfs_core::base::log::{self, Level};
use wmfs_core::base::types::Role;
use wmfs_core::base::{ErrCode, sys};
use wmfs_core::client::meta_client::{Caller, MetaClient, MetaClientOpts, SessionInfo};
use wmfs_core::meta::schema::records::{KIND_DIR, KIND_FILE};
use wmfs_core::net::client::default_role;
use wmfs_core::proto::meta::{AttrOut, MetaOp, MetaReply, SetAttrIn, TimeSet};

use crate::{cluster_addrs, fail, leader, load_key, out};

#[derive(clap::Args)]
pub struct FsArgs {
    /// 以这个用户的身份发请求（只允许 admin key），例如 --as-uid 2000 或 --as-uid 2000:2000
    #[arg(long, global = true, value_name = "UID[:GID]")]
    as_uid: Option<String>,
    /// 集群的管理地址，逗号分隔（默认 $WMFS_CLUSTER）
    #[arg(long, global = true)]
    cluster: Option<String>,
    /// 密钥文件（默认 $WMFS_KEY_FILE，否则 ~/.config/wmfs/admin.key）
    #[arg(long, global = true)]
    key_file: Option<PathBuf>,
    /// 输出 JSON
    #[arg(long, global = true)]
    json: bool,
    /// 打印每次重试（例如"超时重试 seq=… → 去重命中"）
    #[arg(long, global = true)]
    verbose: bool,
    #[command(subcommand)]
    cmd: FsCmd,
}

#[derive(clap::Subcommand)]
pub enum FsCmd {
    /// 列目录（-l：带属性）
    Ls {
        #[arg(short = 'l')]
        long: bool,
        path: String,
    },
    /// 属性与 layout（数据放在哪几条链上）
    Stat { path: String },
    /// 建目录（权限 0777 & ~umask）
    Mkdir { path: String },
    /// 删空目录
    Rmdir { path: String },
    /// 删文件（不递归）
    Rm { path: String },
    /// 改名 / 移动（等同 rename(2)）
    Mv { from: String, to: String },
    /// 硬链接（-s：符号链接，目标原样保存）
    Ln {
        #[arg(short = 's')]
        symbolic: bool,
        target: String,
        link: String,
    },
    /// 文件不存在就建（权限 0666 & ~umask），存在就把时间改成现在
    Touch { path: String },
}

/// 一条命令的结果：给人看的文字与 JSON。
struct Done {
    text: String,
    json: serde_json::Value,
}

/// 一个失败的元数据请求：哪条路径、什么错误码。
struct Failed {
    path: String,
    code: ErrCode,
}

/// 单次 RPC 超时 10 s（3.3.8）；一条命令连同重试的总期限 30 s（CLI 的一次性命令）。
const OPTS: MetaClientOpts =
    MetaClientOpts { rpc_timeout: Duration::from_secs(10), op_timeout: Duration::from_secs(30) };

pub fn run(a: FsArgs) -> i32 {
    log::set_level(if a.verbose { Level::Info } else { Level::Warn });
    let (who, umask) = match caller(&a.as_uid) {
        Ok(x) => x,
        Err(e) => return setup_failed(a.json, ErrCode::EINVAL, e),
    };
    let client = match connect(&a) {
        Ok(c) => c,
        Err((code, e)) => return setup_failed(a.json, code, e),
    };
    let r = exec(&client, &who, umask, &a.cmd);
    client.close();
    match r {
        Ok(d) => out(if a.json { d.json.to_string() } else { d.text }),
        Err(f) => {
            let msg = show::strerror(f.code);
            let name = f.code.name().unwrap_or("?");
            if a.json {
                let j = json!({"error": name, "errno": f.code.0, "message": msg, "path": f.path});
                let _ = out(j);
                1
            } else {
                fail(format!("wmfs fs：{}：{msg}（{name}）", f.path))
            }
        }
    }
}

/// 发请求之前就失败了（见文件头）：`--json` 时输出 `{"error", "errno", "message"}`，否则把原因写到标准错误；退出码 1。
fn setup_failed(json: bool, code: ErrCode, msg: String) -> i32 {
    if !json {
        return fail(msg);
    }
    let _ = out(json!({"error": code.name().unwrap_or("?"), "errno": code.0, "message": msg}));
    1
}

/// 找到 leader 并打开会话；失败时给出错误码（见文件头）与给人看的原因。
fn connect(a: &FsArgs) -> Result<MetaClient, (ErrCode, String)> {
    let key = load_key(&a.key_file).map_err(|e| (ErrCode::EINVAL, e))?;
    if a.as_uid.is_some() && default_role(key.key_id) != Role::Admin {
        let why = "--as-uid 只能和 admin key 一起用（只有 admin 能声明任意 uid，design.md 3.3.3）";
        return Err((ErrCode::EACCES, why.into()));
    }
    let cluster = cluster_addrs(&a.cluster).map_err(|e| (ErrCode::EINVAL, e))?;
    let l = leader::connect(&cluster, key).map_err(|e| (ErrCode::ETIMEDOUT, e))?; // 多半是 30 s 内没找到 leader
    let host = std::fs::read_to_string("/proc/sys/kernel/hostname").unwrap_or_default().trim().to_string();
    let info = SessionInfo { client_name: "wmfs-fs".into(), host, host_id: String::new(), mount: String::new() };
    MetaClient::connect_with(l.rpc, &[l.ep], info, OPTS).map_err(|e| (e, format!("连接元数据 leader 失败：{e}")))
}

/// 身份与 umask：`--as-uid UID[:GID]`（附加组为空）或本进程的 uid/gid/附加组；umask 一律取本进程的（读 /proc/self/status，读不到按 022）。
fn caller(as_uid: &Option<String>) -> Result<(Caller, u32), String> {
    let pid = std::process::id();
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let field = |name: &str| status.lines().find_map(|l| l.strip_prefix(name)).map(str::trim).unwrap_or("");
    let umask = u32::from_str_radix(field("Umask:"), 8).unwrap_or(0o022);
    if let Some(s) = as_uid {
        let bad = || format!("--as-uid 应写成 UID 或 UID:GID：{s:?}");
        let (u, g) = s.split_once(':').unwrap_or((s, s));
        let (uid, gid) = (u.parse().map_err(|_| bad())?, g.parse().map_err(|_| bad())?);
        return Ok((Caller { uid, gid, groups: vec![], pid, job_id: 0 }, umask));
    }
    let (uid, gid) = sys::euid_egid();
    let groups = field("Groups:").split_whitespace().filter_map(|g| g.parse().ok()).collect();
    Ok((Caller { uid, gid, groups, pid, job_id: 0 }, umask))
}

/// "/a/b/c" → ("/a/b", "c")；根目录没有父目录与名字 → `EINVAL`。
fn split(path: &str) -> Result<(String, Vec<u8>), ErrCode> {
    let p = path.trim_end_matches('/');
    match p.rsplit_once('/') {
        Some((parent, name)) if path.starts_with('/') && !name.is_empty() => {
            Ok((if parent.is_empty() { "/".into() } else { parent.into() }, name.as_bytes().to_vec()))
        }
        _ => Err(ErrCode::EINVAL),
    }
}

/// 把错误码记到某条路径上。
fn failed(path: &str, code: ErrCode) -> Failed {
    Failed { path: path.to_string(), code }
}

fn exec(c: &MetaClient, who: &Caller, umask: u32, cmd: &FsCmd) -> Result<Done, Failed> {
    let resolve = |path: &str| -> Result<(u64, u8), Failed> {
        match c.read(who, MetaOp::ResolvePath { path: path.as_bytes().to_vec() }).map_err(|e| failed(path, e))? {
            MetaReply::Resolved { ino, kind } => Ok((ino, kind)),
            _ => Err(failed(path, ErrCode::INTERNAL)),
        }
    };
    let getattr = |path: &str, ino: u64| -> Result<AttrOut, Failed> {
        match c.read(who, MetaOp::GetAttr { ino }).map_err(|e| failed(path, e))? {
            MetaReply::Entry { attr } => Ok(attr),
            _ => Err(failed(path, ErrCode::INTERNAL)),
        }
    };
    let parent_of = |path: &str| -> Result<(u64, Vec<u8>), Failed> {
        let (p, name) = split(path).map_err(|e| failed(path, e))?;
        Ok((resolve(&p)?.0, name))
    };
    let ok = |text: String, ino: u64| Done { text, json: json!({"ok": true, "ino": ino}) };
    match cmd {
        FsCmd::Mkdir { path } => {
            let (parent, name) = parent_of(path)?;
            let r = c.write(who, MetaOp::Mkdir { parent, name, mode: 0o777, umask }).map_err(|e| failed(path, e))?;
            let ino = show::ino_of(&r);
            Ok(ok(format!("已创建目录 {path}（ino {ino}）"), ino))
        }
        FsCmd::Touch { path } => {
            let (parent, name) = parent_of(path)?;
            match c.write(who, MetaOp::Create { parent, name, mode: 0o666, umask, excl: true }) {
                Ok(r) => Ok(ok(format!("已创建 {path}（ino {}）", show::ino_of(&r)), show::ino_of(&r))),
                Err(ErrCode::EEXIST) => {
                    let (ino, _) = resolve(path)?;
                    let now = Some(TimeSet::Now);
                    let set = SetAttrIn {
                        mode: None,
                        uid: None,
                        gid: None,
                        size: None,
                        atime: now,
                        mtime: now,
                        epoch_seen: 0,
                    };
                    c.write(who, MetaOp::SetAttr { ino, set }).map_err(|e| failed(path, e))?;
                    Ok(ok(format!("已把 {path} 的时间改成现在"), ino))
                }
                Err(e) => Err(failed(path, e)),
            }
        }
        FsCmd::Ln { symbolic: true, target, link } => {
            let (parent, name) = parent_of(link)?;
            let op = MetaOp::Symlink { parent, name, target: target.as_bytes().to_vec() };
            let ino = show::ino_of(&c.write(who, op).map_err(|e| failed(link, e))?);
            Ok(ok(format!("已创建符号链接 {link} → {target}"), ino))
        }
        FsCmd::Ln { symbolic: false, target, link } => {
            let (ino, _) = resolve(target)?;
            let (new_parent, new_name) = parent_of(link)?;
            c.write(who, MetaOp::Link { ino, new_parent, new_name }).map_err(|e| failed(link, e))?;
            Ok(ok(format!("已创建硬链接 {link} → {target}（ino {ino}）"), ino))
        }
        FsCmd::Mv { from, to } => {
            let (parent, name) = parent_of(from)?;
            let (new_parent, new_name) = parent_of(to)?;
            let op = MetaOp::Rename { parent, name, new_parent, new_name, flags: 0 };
            c.write(who, op).map_err(|e| failed(from, e))?;
            Ok(Done { text: format!("已把 {from} 改名为 {to}"), json: json!({"ok": true}) })
        }
        FsCmd::Rm { path } | FsCmd::Rmdir { path } => {
            let (parent, name) = parent_of(path)?;
            let op = if matches!(cmd, FsCmd::Rm { .. }) {
                MetaOp::Unlink { parent, name }
            } else {
                MetaOp::Rmdir { parent, name }
            };
            c.write(who, op).map_err(|e| failed(path, e))?;
            Ok(Done { text: format!("已删除 {path}"), json: json!({"ok": true}) })
        }
        FsCmd::Stat { path } => {
            let (ino, kind) = resolve(path)?;
            let attr = getattr(path, ino)?;
            // 普通文件的 layout 在 Open 的回复里（只读打开；没有读权限时只显示属性）
            let layout = if kind == KIND_FILE {
                match c.read(who, MetaOp::Open { ino, flags: 0 }) {
                    Ok(MetaReply::Opened(o)) => o.layout,
                    _ => None,
                }
            } else {
                None
            };
            Ok(show::stat(path, &attr, layout.as_ref(), &c.map()))
        }
        FsCmd::Ls { long, path } => {
            let (ino, kind) = resolve(path)?;
            if kind != KIND_DIR {
                let attr = getattr(path, ino)?;
                let name = split(path).map(|(_, n)| n).unwrap_or_default();
                return Ok(show::ls(vec![(name, ino, kind, Some(attr))], *long));
            }
            let mut entries = Vec::new();
            let mut after = Vec::new();
            loop {
                let op = MetaOp::ReadDir { ino, after: after.clone(), limit: 1024, plus: *long };
                let MetaReply::Dir { entries: page, eof } = c.read(who, op).map_err(|e| failed(path, e))? else {
                    return Err(failed(path, ErrCode::INTERNAL));
                };
                after = page.last().map(|e| e.name.clone()).unwrap_or_default();
                entries.extend(page.into_iter().map(|e| (e.name, e.ino, e.kind, e.attr)));
                if eof || after.is_empty() {
                    break;
                }
            }
            Ok(show::ls(entries, *long))
        }
    }
}
