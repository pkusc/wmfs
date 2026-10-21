//! wmfs-testkit：只给测试用的 crate（design.md 2.1、9.2–9.5）。
//!
//! 以后这里会有：`harness/`（单机多进程集群框架）、`profiles/`（minimal、hpc_rdma、nas_safe）、
//! 模拟掉电的 redb 后端 `MemCrashBackend` 与内存版 `MemTargetStore`、Jepsen-lite 检查器。
//! 组件测试放在这里，避免 core 的 dev-dependency 循环。
//!
//! `golden/` 目录放金样（每种消息、记录、帧头、chunk 头一份固定字节，`constants.golden` 是全部数值常量），
//! 比对它们的测试在 `core/tests/golden_*.rs`（`cargo test -p wmfs-core golden`）。
