//! 测试框架（design.md 9.2–9.5）。
//!
//! - [`net`]：RPC 组件测试用的服务端、客户端与裸 TCP 工具（M1）
//! - [`mem_crash`]：模拟掉电的 redb 后端 `MemCrashBackend`（M3，9.2）
//! - [`meta`]：直接驱动 raft `Driver` 的确定性测试工具（M3，9.2）

pub mod mem_crash;
pub mod meta;
pub mod net;
