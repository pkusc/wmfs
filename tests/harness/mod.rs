//! 测试框架（design.md 9.2–9.5）。
//!
//! - [`net`]：RPC 组件测试用的服务端、客户端与裸 TCP 工具（M1）
//! - [`mem_crash`]：模拟掉电的 redb 后端 `MemCrashBackend`（M3，9.2）
//! - [`meta`]：直接驱动 raft `Driver` 的确定性测试工具（M3，9.2）
//! - [`ns`]：在内存 KV 视图上跑命名空间操作（M4，9.1 的 POSIX 元数据语义）
//! - [`meta_rpc`]：元数据服务经真实的 RPC 与连到它的 MetaClient（M4）
//! - [`mem_target`]：内存版 TargetStore `MemTargetStore`（模拟 fsync 与掉电，M5，9.3）
//! - [`data`]：不经 RPC、直接在一个 DataNode 上读写 chunk（M5，9.1 的"chunk 格式"）
//! - [`node`]：完整的单节点（init + server::start）与连到它的客户端、参考客户端的 put/get（M5）

pub mod data;
pub mod mem_crash;
pub mod mem_target;
pub mod meta;
pub mod meta_rpc;
pub mod net;
pub mod node;
pub mod ns;
