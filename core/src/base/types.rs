//! 全仓库共用的小类型（design.md 3.2.8、3.2.4、3.12）：角色、时间戳、chunk 身份、请求上下文。
//!
//! 接口骨架（M0）：本文件是这些类型的唯一真源；改动任何一个都要同时改 ADR 与 design.md。

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::hooks::traits::ConnIdentity;

/// 全仓库唯一的角色编码：握手 Hello/Welcome、KeyMetaV1、ConnIdentity 都用它。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Role {
    Admin = 1,
    Client = 2,
    Server = 3,
}

/// 线上与 KV 里存 u8（0.2 第 11 条）。
impl Role {
    pub fn from_u8(v: u8) -> Option<Role> {
        match v {
            1 => Some(Role::Admin),
            2 => Some(Role::Client),
            3 => Some(Role::Server),
            _ => None,
        }
    }
    pub fn as_u8(self) -> u8 {
        self as u8
    }
}

/// 时间戳：秒 + 纳秒（atime/mtime/ctime/btime、changelog 都用它）。
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct Ts {
    pub secs: i64,
    pub nanos: u32,
}

/// 数据面的 chunk 身份（3.2.4）。
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ChunkId {
    pub chain: u32,
    pub ino: u64,
    pub epoch: u32,
    pub idx: u32,
}
// chain 由 layout 决定：chain = layout.chains[idx % layout.chains.len()]；在 (ino, epoch) 的生命周期内固定

// ---- 请求上下文（3.12）----
#[derive(Clone, Debug)]
pub struct Principal {
    pub uid: u32, // 已按 key 的 uid 范围校验、按 root_squash 映射
    pub gid: u32,
    pub groups: Vec<u32>,
    pub tenant: u32,
    pub job_id: u64,
    pub conn: Arc<ConnIdentity>,
}

#[derive(Clone, Debug)]
pub struct ReqCtx {
    pub who: Principal,
    pub session: u64,
    pub seq: u64,
    pub pid: u32,
    pub trace_id: u64,
    pub now: Ts, // now：leader 墙钟，写进写集
    pub now_ms: u64,
    pub internal: bool, // 内部操作（cluster-mgr、后台任务）：seq = 0、who = 内部 admin
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn role_codes_are_fixed() {
        // design.md 3.2.8：Admin = 1，Client = 2，Server = 3（线上与 KV 里存 u8）
        for (r, v) in [(Role::Admin, 1u8), (Role::Client, 2), (Role::Server, 3)] {
            assert_eq!(r.as_u8(), v);
            assert_eq!(Role::from_u8(v), Some(r));
        }
        assert_eq!(Role::from_u8(0), None);
        assert_eq!(Role::from_u8(4), None);
    }

    #[test]
    fn ts_orders_by_secs_then_nanos() {
        let a = Ts { secs: 1, nanos: 999 };
        let b = Ts { secs: 2, nanos: 0 };
        assert!(a < b);
        assert_eq!(Ts::default(), Ts { secs: 0, nanos: 0 });
    }
}
