//! wmfs-mgr：进程外控制面插件宿主（design.md 2.1、4.13、4.14、7.4，ADR-0017）。
//!
//! - [`api`]：带 HMAC 签名的管理 API 客户端 `ApiClient`，CLI 与 mgr 共用（M1）；
//! - [`host`]：mgr 自己的 HTTP 路由、两个进程外钩子 trait（`EventSink`、`MgmtExt`）、manifest 校验、调用包装、唯一的 changelog 长轮询
//!   循环（M14）；[`run`]：`wmfs mgr` 的装配（M14）；[`prom`]：抓取、合并各节点的 `/metrics`；
//! - 五个插件（M14）：[`dashboard`]、[`exporter`]（prometheus-exporter）、[`alert`]（mgmt_ext）与 [`audit_log`]、[`webhook`]（event_sink）。
//!
//! 这个 crate 不依赖 wmfs-core（只经管理 API、`/metrics`、changelog 接触系统，编译期保证）；它的插件照样有 fail point 与指标（都来自
//! wmfs-api）。墙钟用标准库的 `SystemTime`（0.2 第 6 条的例外）。

pub mod alert;
pub mod api;
pub mod audit_log;
pub mod dashboard;
pub mod exporter;
pub mod host;
pub mod prom;
pub mod run;
pub mod webhook;

pub use run::{MgrOpts, run};

/// 墙钟毫秒（告警的时刻、审计文件的日期、管理 API 的签名时间戳）。
pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64)
}

/// mgr 的日志：标准错误上一行一条、带 UTC 时刻（与 core 的日志同一个写法，`wmfs_api::units::utc_string`）；写不出去也不 panic。
pub(crate) fn log(msg: impl std::fmt::Display) {
    use std::io::Write;
    let _ = writeln!(std::io::stderr(), "{} mgr：{msg}", wmfs_api::units::utc_string(now_ms()));
}
