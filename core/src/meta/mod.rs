//! 元数据（design.md 3.2.6–3.2.9、3.4.1、3.5、3.8、3.9）。
//!
//! M0 只有接口骨架：KV 布局与记录（[`schema`]）、元数据事务模型（[`engine`]，3.5.1）、目录属性解析（[`attrs`]）。
//! raft（M3）、命名空间操作 ns（M4）、属性与递归用量（M7）、集群管理 cluster（M4、M8、M9）随里程碑加入。

pub mod attrs;
pub mod engine;
pub mod schema;
