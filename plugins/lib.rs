//! wmfs-plugins：全部进程内首发插件（design.md 2.1、第 4 章）。
//!
//! 这个文件只有声明：每个插件一个目录 `<name>/{mod.rs, manifest.toml, docs.md, tests.rs}`、一个 cargo feature，
//! 例如 `#[cfg(feature = "tcp")] #[path = "tcp/mod.rs"] pub mod tcp;`。
//! 插件只能 `use wmfs_core::...` 的公开 API，不得 `use` 其他插件模块。
//! 第一个插件（tcp、shared-key）在 M1 加入；M0 只有这个空壳。
