//! 测试框架（design.md 9.2–9.5）。
//!
//! - [`net`]：RPC 组件测试用的服务端、客户端与裸 TCP 工具（M1）
//! - [`mem_crash`]：模拟掉电的 redb 后端 `MemCrashBackend`（M3，9.2）
//! - [`meta`]：直接驱动 raft `Driver` 的确定性测试工具（M3，9.2）
//! - [`ns`]：在内存 KV 视图上跑命名空间操作（M4，9.1 的 POSIX 元数据语义）
//! - [`meta_rpc`]：元数据服务经真实的 RPC 与连到它的 MetaClient（M4）

pub mod mem_crash;
pub mod meta;
pub mod meta_rpc;
pub mod net;
pub mod ns;
