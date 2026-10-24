//! 元数据库的初始状态与启动时的几个读（design.md 3.4.1.9、3.2.6）：init 写的 index = 1 的状态、join 的新节点写的空状态、读
//! `raft_state["applied"]` 与已应用的 GLOBAL 记录。`storage.rs` 原样转出它们（`meta::raft::storage::bootstrap` 等原路径照常可用）。
//!
//! 实现注记（M8）：[`bootstrap_empty`] 是 join 的新节点（3.4.3 第 3 步"以空 meta 存储启动"）：空日志、空 ConfState、`snap_meta =
//! applied = 0`——不可提升、不会自己 campaign（坑 P14），之后一律先由快照追上（leader 的日志从 index 2 起，index 1 是 init 的快照点）。
//! 规格没写新节点在快照到来之前怎样知道 leader 与快照源的地址（3.4.1.7 只说"用 Joined 回复里带的集群图"）：取最小方案，把 Joined 的
//! 集群图里的节点地址、key 表与几项 GLOBAL（cluster、settings、plugins、map_epoch）预先写进它的 kv 表（`seed`，由 `init/join.rs` 生成）：
//! 地址解析、握手认证、启动时的核对都照常读 kv，重启之后也还在；快照安装（规则 M5）先删掉整个 kv 表再重建，所以这些预写的记录
//! 不会留下来，也不会与任何副本的状态混在一起（applied 是 0，安装快照之前一个条目也应用不了）。

use raft::eraftpb::{ConfState, HardState};
use redb::{Database, ReadTransaction};

use super::storage::{KV, RAFT_LOG, RAFT_STATE, encode};
use crate::meta::schema::keys;
use crate::meta::schema::raft_state::{
    Applied, KEY_APPLIED, KEY_CONF_STATE, KEY_FORMAT, KEY_HARD_STATE, KEY_SNAP_META, MetaFormatV1, SnapMeta,
};
use crate::meta::schema::records::decode_v1;

/// init 写的 index = 1、term = 1 的初始状态（3.4.1.9，与 raft-rs 的 five_mem_node 示例一致）：
/// `snap_meta = (1, 1)`、`hard_state = {term: 1, vote: 0, commit: 1}`、`conf_state = {voters}`、
/// `applied = (1, 1, state_version = 1)`、`format`，以及初始 KV（3.4.3）。`Durability::Immediate`。
pub fn bootstrap(db: &Database, voters: &[u64], kv: &[(Vec<u8>, Vec<u8>)]) -> Result<(), String> {
    let mut hs = HardState::default();
    (hs.term, hs.commit) = (1, 1);
    let cs = ConfState { voters: voters.to_vec(), ..Default::default() };
    write(db, &hs, &cs, 1, kv)
}

/// join 的新节点的空状态（见文件头）：`hard_state`、`conf_state` 都是空的，`snap_meta = (0, 0)`、`applied = (0, 0, 0)`；`seed` 预写进
/// kv 表（快照安装时整个删掉）。
pub fn bootstrap_empty(db: &Database, seed: &[(Vec<u8>, Vec<u8>)]) -> Result<(), String> {
    write(db, &HardState::default(), &ConfState::default(), 0, seed)
}

fn write(db: &Database, hs: &HardState, cs: &ConfState, at: u64, kv: &[(Vec<u8>, Vec<u8>)]) -> Result<(), String> {
    let err = |e: &dyn std::fmt::Display| format!("写元数据库的初始状态失败：{e}");
    let tx = db.begin_write().map_err(|e| err(&e))?;
    {
        tx.open_table(RAFT_LOG).map_err(|e| err(&e))?;
        let mut st = tx.open_table(RAFT_STATE).map_err(|e| err(&e))?;
        let format = MetaFormatV1 { version: 1, incompat: vec![] }.encode();
        let applied = Applied { index: at, term: at, state_version: at }.encode();
        let snap = SnapMeta { index: at, term: at }.encode();
        for (k, v) in [
            (KEY_HARD_STATE, encode(hs)),
            (KEY_CONF_STATE, encode(cs)),
            (KEY_SNAP_META, snap.to_vec()),
            (KEY_APPLIED, applied.to_vec()),
            (KEY_FORMAT, format),
        ] {
            st.insert(k, v.as_slice()).map_err(|e| err(&e))?;
        }
        let mut t = tx.open_table(KV).map_err(|e| err(&e))?;
        for (k, v) in kv {
            t.insert(k.as_slice(), v.as_slice()).map_err(|e| err(&e))?;
        }
    }
    tx.commit().map_err(|e| err(&e))
}

/// 读 `raft_state["applied"]`（与 KV 同事务更新，所以与读事务里的 KV 一致）。
pub fn read_applied(rtx: &ReadTransaction) -> Result<Applied, String> {
    let st = rtx.open_table(RAFT_STATE).map_err(|e| format!("打不开 raft_state 表：{e}"))?;
    let v = st.get(KEY_APPLIED).map_err(|e| format!("读 raft_state[applied] 失败：{e}"))?;
    let v = v.ok_or("raft_state 里没有 applied")?;
    Applied::decode(v.value()).map_err(|e| format!("raft_state[applied] 损坏：{e}"))
}

/// 启动时读一条已应用的 GLOBAL 记录（3.2.7）：不存在 → `Ok(None)`；读盘出错或记录解不开 → 带键名的中文错误
/// （坏了的记录不能当成"没有"或默认值，启动就此失败）。启动装配与 raft 线程（装完快照之后重新比对插件清单）都用它；raft 线程上出错
/// 由调用方 `or_abort`。
pub fn read_global<T: serde::de::DeserializeOwned>(rtx: &ReadTransaction, name: &str) -> Result<Option<T>, String> {
    let t = rtx.open_table(KV).map_err(|e| format!("打不开 kv 表：{e}"))?;
    let v = t.get(keys::global(name).as_slice()).map_err(|e| format!("读 GLOBAL {name} 失败：{e}"))?;
    match v {
        None => Ok(None),
        Some(v) => decode_v1(v.value()).map(Some).map_err(|e| format!("GLOBAL {name} 损坏，解不开（{e}）")),
    }
}
