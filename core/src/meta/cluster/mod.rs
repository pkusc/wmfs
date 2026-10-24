//! 集群管理（design.md 3.3.6、3.4.2.1、3.4.2.10、1.2 的 cluster-mgr）。
//!
//! - [`map`]：集群图——由已提交状态构建、按 `map_epoch` 缓存、顺带计算集群警告（M4：短链部分）
//! - [`chains`]：链的生成 `gen_chains` 与给 placement 看的 `ChainView`（M4；链管理器在 M9）
//! - [`cluster_mgr`]：cluster-mgr 线程（M4 只在 leader 就绪与集群图变化时把集群警告写进日志）
//!
//! - [`targets`]：target 心跳的 leader 一侧（更新 TargetRecV1、记下用量）与 StatFs（M5）
//! - [`settings`]：集群运行参数 `wmfs cluster set`（M7）
//!
//! - [`join`]：`NodeOp::Join` 的 leader 一侧（插件清单比对、分配节点与 target 号、写记录，M8）
//! - [`plugins`]：集群插件清单的比对与退出、`wmfs cluster plugins add|remove`、`--adopt-local-plugins`（M8）
//!
//! - [`chain_plan`]：链管理器的决定（纯函数：离线、重启、唯一幸存者、回来、修复、drain、新 target，M9）
//! - [`chain_mgr`]：链管理器在 cluster-mgr 线程上的一轮、SyncDone、force-serving、调小租约的过渡（M9）

pub mod chain_mgr;
pub mod chain_plan;
pub mod chains;
pub mod cluster_mgr;
pub mod join;
pub mod map;
pub mod plugins;
pub mod settings;
pub mod targets;
