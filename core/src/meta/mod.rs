//! 元数据（design.md 3.2.6–3.2.9、3.4.1、3.5、3.8、3.9）。
//!
//! - [`schema`]：KV 布局与记录（M0）
//! - [`raft`]：元数据 Raft——`RedbStorage` 与 raft 线程的驱动循环（M3）
//! - [`engine`]：元数据事务模型（3.5.1）与它的实现：单飞批次执行器、读闸门、去重（M3）
//! - [`service`]：`MetaCall` 消息族的服务端（M3：调试键空间；M4 起命名空间操作）
//! - [`sessions`]：客户端会话（M3 只有 `is_active`）
//! - [`attrs`]：目录属性解析（M4、M7）
//!
//! 命名空间操作 ns（M4）、属性与递归用量（M7）、集群管理 cluster（M4、M8、M9）随里程碑加入。

pub mod attrs;
pub mod engine;
pub mod raft;
pub mod schema;
pub mod service;
pub mod sessions;
