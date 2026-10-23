//! 元数据（design.md 3.2.6–3.2.9、3.4.1、3.5、3.8、3.9）。
//!
//! - [`schema`]：KV 布局与记录（M0）、按记录类型读写 KV 的小函数（M4）
//! - [`raft`]：元数据 Raft——`RedbStorage` 与 raft 线程的驱动循环（M3）
//! - [`engine`]：元数据事务模型（3.5.1）与它的实现：单飞批次执行器、读闸门、去重（M3）
//! - [`service`]：`MetaCall` 与 `NodeCall` 的服务端（M3：调试键空间；M4：命名空间、会话、`GetClusterMap`）
//! - [`ns`]：命名空间操作（M4）
//! - [`sessions`]：客户端会话（M4：open / heartbeat / close；驱逐在 M10）
//! - [`changelog`]：changelog 写入（M4）
//! - [`cluster`]：集群图与集群警告、链的生成、cluster-mgr 线程（M4 起）
//! - [`attrs`]：目录属性解析（M4 只有内置默认值；M7：继承、来源、按 attr_epoch 的缓存）
//! - [`attr_registry`]：属性注册表与服务端切分（M7）；[`attr_ops`]：设置、删除、查询、用量、声明 vs 实际（M7，管理 API 用）
//! - [`rstat`]：递归用量计数（M7）
//! - [`uninline`]：服务端把 inline 小文件转成 chunk（M5，ADR-0034）
//! - [`gc_meta`]：GC 记录的元数据一侧（gc 任务用，M5）
//!
//! 配额（quota，M10）、锁（M12）随里程碑加入。

pub mod attr_ops;
pub mod attr_registry;
pub mod attrs;
pub mod changelog;
pub mod cluster;
pub mod engine;
pub mod gc_meta;
pub mod ns;
pub mod raft;
pub mod rstat;
pub mod schema;
pub mod service;
pub mod sessions;
pub mod uninline;
