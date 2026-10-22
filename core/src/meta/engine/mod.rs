//! 元数据引擎（design.md 3.4.1.4–3.4.1.6、3.5.1，ADR-0019）：单线程串行执行器 + 单飞批次 + 写集复制 + 批量 ReadIndex。
//!
//! - [`txn`]：事务模型的 trait 与类型（`KvRead/KvScan/KvTxn`、`MetaEngine`、`OpCtx`、`TxnRequest`……，M0 骨架）
//! - [`engine`]：`MetaEngine` 的首发实现 embedded-kv（raft 线程 + 执行器线程）与读 KEYMETA 的 key 表 `KvKeys`
//! - [`executor`]：单飞批次执行器（边取边执行、≤ 256 个操作、写集 768 KiB 上限、`expect_version`）
//! - [`view`]：批次覆盖层 `BatchView`（操作级回滚、写集字节计数）与只读视图 `ReadView`
//! - [`read_gate`]：线性一致读的批量 ReadIndex（raft 线程上用）
//! - [`dedup`]：请求去重表（会话 + seq，3.5.2）

pub mod dedup;
#[allow(clippy::module_inception)] // 文件名按 design.md 2.2 的模块清单（engine.rs = MetaEngine 的实现）
pub mod engine;
pub mod executor;
pub mod read_gate;
pub mod txn;
pub mod view;

// 调用方写 `meta::engine::EmbeddedEngine` 即可（`meta::engine::engine::…` 的原路径照常可用）
pub use engine::{EmbeddedEngine, EngineOpts, KvKeys, MANIFEST};
