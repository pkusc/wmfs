//! plugin-mem-transport：进程内存里的传输插件（design.md 2.1、9.3）。
//!
//! 它是一个独立 crate，只依赖 `wmfs-core` 的公开 API（`hooks::traits::{Transport, Conn}`），
//! 用来证明"新增一个传输插件 = 新 crate + `cli/src/registry.rs` 一行 + 一个 feature，core 一行不改"。
//! M1 实现；M0 只有这个空壳。
