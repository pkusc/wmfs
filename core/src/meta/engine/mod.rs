//! 元数据引擎（design.md 3.4.1.4、3.5.1，ADR-0019）：单线程串行执行器 + 单飞批次 + 写集复制。
//!
//! M0 只有 [`txn`]：事务模型的 trait 与类型（`KvRead/KvScan/KvTxn`、`MetaEngine`、`OpCtx`、`TxnRequest`……）。
//! 执行器 executor.rs、覆盖层 view.rs、读闸门 read_gate.rs、去重 dedup.rs、MetaEngine 的实现 engine.rs 在 M3 加入。

pub mod txn;
