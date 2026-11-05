//! `wmfs key create|list|revoke` 与 `wmfs session list|evict`（design.md 7.2、7.1，roadmap M10）：经带签名的管理 API（admin key，
//! 409 自动跟随 leader）。`--json` 原样输出管理 API 的 JSON。
//!
//! - `wmfs key create <名字> [--role client|admin] [--tenant N] [--uids 1000-1999,3000] [--root-squash] [--path /p]…
//!   [--path-ro /q]… [--out F]`：路径是文件系统里的绝对路径（必须是目录；服务端解析成 inode 号，目录被 rename 后限制跟着它）。新 key 的文件内容只在
//!   这一次回复里：`--out` 写进文件（权限 0600），不给就打印到标准输出。
//! - `wmfs key list`、`wmfs key revoke <id>`（它的会话一并被驱逐；最后一把有效的 admin key 不能撤销）。
//! - `wmfs session list`（含 24 h 内的 EVICTED 记录，状态大写显示：ACTIVE / EVICTING / EVICTED）、`wmfs session evict <id>`（十六进制
//!   会话号，同挂载日志里的"会话 …"）。

use std::path::PathBuf;

use wmfs_core::api::types::{
    KeyCreateReply, KeyCreateReq, KeyInfo, KeyRevokeReply, KeysReply, SessionRow, SessionsReply,
};
use wmfs_core::base::{clock, sys};
use wmfs_mgr::api::{ApiClient, ApiError};

use crate::attr::{ApiArgs, run};

#[derive(clap::Subcommand)]
pub enum KeyCmd {
    /// 建一把新 key（打印或写出一次性的 key 文件）
    Create {
        name: String,
        /// 角色：client（挂载与 wmfs fs）或 admin（管理命令）
        #[arg(long, default_value = "client")]
        role: String,
        #[arg(long, default_value_t = 0)]
        tenant: u32,
        /// 这把 key 能声明的 uid，例如 1000-1999,3000（不给 = 不限）
        #[arg(long, default_value = "")]
        uids: String,
        /// 把 uid/gid 0 映射成 65534（nobody）
        #[arg(long)]
        root_squash: bool,
        /// 只能访问这些子树（文件系统里的绝对路径，可重复）
        #[arg(long)]
        path: Vec<String>,
        /// 只读地访问这些子树（可重复）
        #[arg(long)]
        path_ro: Vec<String>,
        /// 把 key 文件写到这里（权限 0600；不给就打印到标准输出）
        #[arg(long)]
        out: Option<PathBuf>,
        #[command(flatten)]
        api: ApiArgs,
    },
    /// 列出全部 key（不含秘密）
    List {
        #[command(flatten)]
        api: ApiArgs,
    },
    /// 撤销一把 key（用它开的会话一并驱逐）
    Revoke {
        id: u32,
        #[command(flatten)]
        api: ApiArgs,
    },
}

#[derive(clap::Subcommand)]
pub enum SessionCmd {
    /// 列出客户端会话（含 24 h 内被驱逐的）
    List {
        #[command(flatten)]
        api: ApiArgs,
    },
    /// 手动驱逐一个会话（两阶段：先拉黑、等数据节点确认，再释放锁）
    Evict {
        /// 会话号（十六进制，同挂载日志里的"会话 …"）
        id: String,
        #[command(flatten)]
        api: ApiArgs,
    },
}

fn key_line(k: &KeyInfo) -> String {
    let paths: Vec<String> = k
        .paths
        .iter()
        .map(|p| {
            format!(
                "{}{}",
                if p.path.is_empty() { "(已删除)" } else { &p.path },
                if p.read_only { "（只读）" } else { "" }
            )
        })
        .collect();
    let mut s = format!("key {:<3} {:<12} {:<6} 租户 {}  uid {}", k.id, k.name, k.role, k.tenant, k.uids);
    if k.root_squash {
        s += "  root_squash";
    }
    if !paths.is_empty() {
        s += &format!("  只能访问 {}", paths.join(", "));
    }
    if k.revoked {
        s += "  已撤销";
    }
    s
}

fn session_line(s: &SessionRow) -> String {
    let mut t = format!("{:<16} {:<8}", s.id, s.state.to_uppercase());
    if s.state != "evicted" {
        t += &format!(" key {:<3} {} {}", s.key_id, s.client_name, s.host); // 驱逐完的只剩 EVICTED 记录，没有客户端信息
    }
    if !s.mount.is_empty() {
        t += &format!("  {}", s.mount);
    }
    if let Some(at) = s.evicted_ms {
        t += &format!("  {} s 前被驱逐", clock::wall_ms().saturating_sub(at) / 1000);
    }
    t
}

pub fn run_key(cmd: KeyCmd) -> i32 {
    match cmd {
        KeyCmd::Create { name, role, tenant, uids, root_squash, path, path_ro, out, api } => {
            let req = KeyCreateReq { name, role, tenant, uids, paths: path, paths_ro: path_ro, root_squash };
            let dest = out.clone();
            let call = move |c: &ApiClient, _: &str| {
                let r = c.post::<_, KeyCreateReply>("/api/v1/keys", &req)?;
                if let Some(p) = &dest {
                    let e = |e: wmfs_core::base::ErrCode| ApiError {
                        status: 0,
                        code: "eio".into(),
                        message: format!("写 {} 失败：{e}", p.display()),
                    };
                    sys::atomic_write(p, r.key_file.as_bytes()).map_err(e)?;
                }
                Ok(r)
            };
            let show = move |r: &KeyCreateReply| match &out {
                Some(p) => format!("已建 key {}，key 文件写到了 {}（权限 0600；只有这一次）", r.id, p.display()),
                None => format!("已建 key {}（下面是 key 文件的内容，只有这一次）：\n{}", r.id, r.key_file.trim_end()),
            };
            run(&api, None, call, show)
        }
        KeyCmd::List { api } => {
            let call = |c: &ApiClient, _: &str| c.get::<KeysReply>("/api/v1/keys");
            run(&api, None, call, |r: &KeysReply| r.keys.iter().map(key_line).collect::<Vec<_>>().join("\n"))
        }
        KeyCmd::Revoke { id, api } => {
            let call =
                move |c: &ApiClient, _: &str| c.post::<_, KeyRevokeReply>(&format!("/api/v1/keys/{id}/revoke"), &());
            let show = |r: &KeyRevokeReply| format!("已撤销 key {}（驱逐了它的 {} 个会话）", r.id, r.evicted_sessions);
            run(&api, None, call, show)
        }
    }
}

pub fn run_session(cmd: SessionCmd) -> i32 {
    match cmd {
        SessionCmd::List { api } => {
            let call = |c: &ApiClient, _: &str| c.get::<SessionsReply>("/api/v1/sessions");
            let show = |r: &SessionsReply| {
                if r.sessions.is_empty() {
                    return "没有会话".to_string();
                }
                r.sessions.iter().map(session_line).collect::<Vec<_>>().join("\n")
            };
            run(&api, None, call, show)
        }
        SessionCmd::Evict { id, api } => {
            let call =
                move |c: &ApiClient, _: &str| c.post::<_, SessionRow>(&format!("/api/v1/sessions/{id}/evict"), &());
            let show = |r: &SessionRow| format!("会话 {} 驱逐第一阶段已完成（数据节点确认之后释放它的锁）", r.id);
            run(&api, None, call, show)
        }
    }
}
