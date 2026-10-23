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

#[cfg(feature = "rdma")]
#[path = "rdma/mod.rs"]
pub mod rdma;

#[cfg(feature = "posix-mode")]
#[path = "posix-mode/mod.rs"]
pub mod posix_mode;

#[cfg(feature = "label-match")]
#[path = "label-match/mod.rs"]
pub mod label_match;

#[cfg(feature = "local-fs")]
#[path = "local-fs/mod.rs"]
pub mod local_fs;

#[cfg(feature = "crc32c")]
#[path = "crc32c/mod.rs"]
pub mod crc32c;

#[cfg(feature = "replica")]
#[path = "replica/mod.rs"]
pub mod replica;

#[cfg(feature = "fifo")]
#[path = "fifo/mod.rs"]
pub mod fifo;

#[cfg(feature = "gc")]
#[path = "gc/mod.rs"]
pub mod gc;
