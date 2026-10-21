//! wmfs-plugins：全部进程内首发插件（design.md 2.1、第 4 章）。
//!
//! 这个文件只有声明：每个插件一个目录 `<name>/{mod.rs, manifest.toml, docs.md, tests.rs}`、一个 cargo feature。
//! 插件只能 `use wmfs_core::...` 的公开 API，不得 `use` 其他插件模块。注册表在 `cli/src/registry.rs`。

#[cfg(feature = "tcp")]
#[path = "tcp/mod.rs"]
pub mod tcp;

#[cfg(feature = "shared-key")]
#[path = "shared-key/mod.rs"]
pub mod shared_key;
