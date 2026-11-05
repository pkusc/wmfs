//! M10 的管理端点（design.md 7.1）：认证 key（`GET/POST /api/v1/keys`、`POST /api/v1/keys/<id>/revoke`）与会话
//! （`GET /api/v1/sessions`、`POST /api/v1/sessions/<id>/evict`）。只增字段（金样在 `access/tests.rs`）。

use serde::{Deserialize, Serialize};

/// `POST /api/v1/keys` 的请求体（7.1 `{name, role, tenant, uids, paths, paths_ro, root_squash}`）。`role` 是 `client` 或
/// `admin`；`uids` 形如 `1000-1999,3000`（空 = 不限）；`paths`/`paths_ro` 是允许的子树（读写 / 只读，空 = 不限），建 key 时解析成
/// inode 号。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct KeyCreateReq {
    pub name: String,
    pub role: String,
    #[serde(default)]
    pub tenant: u32,
    #[serde(default)]
    pub uids: String,
    #[serde(default)]
    pub paths: Vec<String>,
    #[serde(default)]
    pub paths_ro: Vec<String>,
    #[serde(default)]
    pub root_squash: bool,
}

/// `POST /api/v1/keys` 的回复：新 key 的号与**一次性**的 key 文件内容（服务端不存秘密，之后取不回来）。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct KeyCreateReply {
    pub id: u32,
    pub key_file: String,
}

/// 一把 key 允许的一个子树：根的 inode 号与它现在的路径（目录被 rename 后限制跟着它；目录已删 → 空串）。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct PathCapInfo {
    pub ino: u64,
    pub path: String,
    pub read_only: bool,
}

/// `GET /api/v1/keys` 的一项（不含秘密）。`uids` 的写法同 [`KeyCreateReq`]（`0-4294967295` = 不限）。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct KeyInfo {
    pub id: u32,
    pub name: String,
    pub role: String,
    pub tenant: u32,
    pub uids: String,
    pub root_squash: bool,
    pub paths: Vec<PathCapInfo>,
    pub revoked: bool,
    pub created_ms: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct KeysReply {
    pub keys: Vec<KeyInfo>,
}

/// `POST /api/v1/keys/<id>/revoke` 的回复：一并驱逐（第一阶段）了几个会话。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct KeyRevokeReply {
    pub id: u32,
    pub evicted_sessions: u32,
}

/// `GET /api/v1/sessions` 的一项。`id` 是十六进制（与挂载日志里的"会话 …"相同）；`state`：`active`、`evicting`（第一阶段已完成，
/// 等数据节点确认）、`evicted`（两个阶段都完成，只剩 24 h 内的 EVICTED 记录：客户端信息已随会话记录删除，为空）。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct SessionRow {
    pub id: String,
    pub state: String,
    pub client_name: String,
    pub host: String,
    pub mount: String,
    pub key_id: u32,
    pub created_ms: u64,
    pub evicted_ms: Option<u64>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct SessionsReply {
    pub sessions: Vec<SessionRow>,
}

#[cfg(test)]
mod tests;
