//! 钩子管线与插件 ABI（design.md 3.7，ADR-0025）。
//!
//! 管线顺序由本体写死：`transport → decode(core) → authn → authz → quota_admission → qos → core exec →
//! placement → transform(checksum→compress→encrypt) → codec → chunk_store → response → event_sink(异步)`。
//! - [`manifest`]：`Hook`、`Manifest`、ABI 版本号（3.7.1）
//! - [`traits`]：各钩子的 Rust trait（3.7.3；全部对象安全，跨模块用 `Arc<dyn …>`）
//! - [`pipeline`]：按 altitude 排好的插件与统一的调用包装（3.7.4、3.12）
//! - [`policy`]：策略求值点（3.7.7，为二期 WASM 预留）
//!
//! 插件注册表只在 `cli/src/registry.rs`（3.7.5）；动态库 C ABI（cabi.rs，3.7.6）在 M14。

pub mod manifest;
pub mod pipeline;
pub mod policy;
pub mod traits;
