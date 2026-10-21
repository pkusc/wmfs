//! 连接与 RPC（design.md 1.2、3.3.2、3.3.8、3.12）。
//!
//! - [`conn`]：core 对传输插件 `Conn` 的包装 `ConnHandle`（每连接一个读线程 + 一个写线程 + 有界发送队列）
//! - [`handshake`]：3.3.2 握手的服务端与客户端两侧（core 执行，与传输无关）
//! - [`server`]：`RpcServer`（accept、握手、角色权限表、分发）与回复句柄 `Responder`
//! - [`client`]：`RpcClient`（到多个对端的连接管理；call / start / call_cb / stream；按族选 scheme；rpc-timer）
//! - [`retry`]：3.3.8 的退避重试

pub mod client;
pub mod conn;
mod handshake;
pub mod retry;
pub mod server;
