//! wmfs-api：本体对外的稳定接口（design.md 2.1、决定 D16）。
//!
//! 只依赖 serde 与 serde_json，core 与进程外的 wmfs-mgr（不依赖 core）共用：
//! - [`codes`]：错误码名字 ↔ 数值（design.md 3.3.7）
//! - [`ops`]：changelog 操作码名字 ↔ 数值（3.2.8，ADR-0029）
//! - [`metrics`]：指标注册表 + Prometheus 文本（3.11）
//! - [`failpoint`]：故障注入点 fail point（3.11）
//! - [`auth`]：管理 API 的 HMAC 签名格式（7.1）
//! - [`hex`]：字节 ↔ 小写 hex（cluster_id、密钥文件、签名共用）
//! - [`types`]：密钥文件、changelog 事件、管理 API 的请求/响应结构（7.1，随里程碑增加）
//! - [`url`]：管理 API 查询参数的百分号编解码（M7：`?path=` 里的路径）

pub mod auth;
pub mod codes;
pub mod failpoint;
pub mod hex;
pub mod metrics;
pub mod ops;
pub mod types;
pub mod url;
