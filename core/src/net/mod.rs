//! 连接与 RPC（design.md 1.2、3.3.8、3.12）。
//!
//! - [`conn`]：core 对传输插件 `Conn` 的包装 `ConnHandle`（每连接一个读线程 + 一个写线程 + 有界发送队列）
//! - [`client`]：`RpcClient`（到多个对端的连接管理；call / start / call_cb / stream；按族选 scheme）
//! - [`server`]：服务端对一个请求的回复句柄 `Responder`
//!
//! 接口骨架（M0）：签名以这里为准，实现在 M1。

pub mod client;
pub mod conn;
pub mod server;
