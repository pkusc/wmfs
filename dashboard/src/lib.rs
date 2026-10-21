//! wmfs-mgr：进程外控制面插件宿主（design.md 2.1、4.13、4.14、7.4，ADR-0017）。
//!
//! - [`api`]：带 HMAC 签名的管理 API 客户端 `ApiClient`，CLI 与 mgr 共用（M1 起实现）；
//! - [`host`]：mgr 自己的 HTTP 路由与两个进程外钩子 trait（`EventSink`、`MgmtExt`，M14 起实现）。
//!
//! 这个 crate 不依赖 wmfs-core；它的插件照样有 fail point 与指标（都来自 wmfs-api）。

pub mod api;
pub mod host;
