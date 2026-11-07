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

#[cfg(feature = "posix-acl")]
#[path = "posix-acl/mod.rs"]
pub mod posix_acl;

#[cfg(feature = "path-caps")]
#[path = "path-caps/mod.rs"]
pub mod path_caps;

#[cfg(feature = "quota")]
#[path = "quota/mod.rs"]
pub mod quota;

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

#[cfg(feature = "cache-memory")]
#[path = "cache-memory/mod.rs"]
pub mod cache_memory;

#[cfg(feature = "xxh3")]
#[path = "xxh3/mod.rs"]
pub mod xxh3;

#[cfg(feature = "blake3")]
#[path = "blake3/mod.rs"]
pub mod blake3;

#[cfg(feature = "lz4")]
#[path = "lz4/mod.rs"]
pub mod lz4;

#[cfg(feature = "zstd")]
#[path = "zstd/mod.rs"]
pub mod zstd;

#[cfg(feature = "aes-gcm")]
#[path = "aes-gcm/mod.rs"]
pub mod aes_gcm;

#[cfg(feature = "reconciler")]
#[path = "reconciler/mod.rs"]
pub mod reconciler;

#[cfg(feature = "scrub")]
#[path = "scrub/mod.rs"]
pub mod scrub;

#[cfg(feature = "trash-expire")]
#[path = "trash-expire/mod.rs"]
pub mod trash_expire;

#[cfg(feature = "ttl")]
#[path = "ttl/mod.rs"]
pub mod ttl;
