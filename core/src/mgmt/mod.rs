//! 管理面（design.md 7.1，ADR-0005）：每个 `wmfs server` 用 tiny_http 提供 HTTP/JSON 管理 API 与 `/metrics`。
//!
//! - [`http`]：监听、4 个处理线程 `mgmt-http-<i>`、路由与各端点（M1：`/metrics`、`/api/v1/health`、`/api/v1/failpoints`）
//! - [`auth`]：`Authorization: WMFS-HMAC key=,ts=,sig=` 的校验（写需要 admin，读需要任一已认证 key）
//!
//! 请求/响应结构都在 wmfs-api（`types.rs`、签名格式在 `auth.rs`），服务端与 wmfs-mgr 的 ApiClient 共用。
//! 错误统一为 `{"error": "<错误码名的小写>", "message": "<中文说明>"}`。`/metrics` 与 `/api/v1/health` 免认证。

pub mod auth;
pub mod http;

pub use http::{MgmtCtx, MgmtServer};
