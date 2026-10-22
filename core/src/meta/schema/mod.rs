//! 元数据的磁盘格式（design.md 3.2.6–3.2.9，ADR-0020）：key 编码 [`keys`]、记录结构 [`records`]，
//! 以及 `meta.redb` 的 `raft_state` 表里由 wmfs 定义格式的值 [`raft_state`]；按记录类型读写 KV 的小函数在 [`kv`]（M4）。

pub mod keys;
pub mod kv;
pub mod raft_state;
pub mod records;
