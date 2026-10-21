//! 基础设施（design.md 2.2 core/base）：错误码、单位与时长解析、时钟、随机数、日志、node.toml、
//! libc 的安全封装、物理盘解析。别的模块都依赖它。
//!
//! 依赖方向：base 是最底层，**不得引用 crate 里的其他模块**。只有两个例外，因为规格把这两个类型放在了别处：
//! `types.rs` 的 `Principal` 引用 `hooks::traits::ConnIdentity`（3.7.3），`config.rs` 的 `NodeConfig` 引用
//! `buf::pool::PoolConfig`（3.6.1）。不要再增加新的例外。
//!
//! 硬规则（design.md 0.2）：所有时间来自 [`clock`]，所有随机数来自 [`rand::fill`]；
//! `unsafe` 只允许出现在 [`sys`]（以及将来的 `buf/pool.rs`、`hooks/cabi.rs`、`plugins/rdma/`）。

pub mod clock;
pub mod config;
pub mod devinfo;
pub mod errcode;
pub mod log;
pub mod rand;
pub mod sys;
pub mod types;
pub mod units;

pub use errcode::ErrCode;

/// 测试用：建一个本次测试独有的临时目录（用完由测试自己删除）。
#[cfg(test)]
pub(crate) fn test_dir(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("wmfs-test-{name}-{}-{:x}", std::process::id(), rand::random_u64()));
    std::fs::create_dir_all(&d).unwrap();
    d
}
