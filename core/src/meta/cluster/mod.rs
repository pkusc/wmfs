//! 集群管理（design.md 3.3.6、3.4.2.1、3.4.2.10、1.2 的 cluster-mgr）。
//!
//! - [`map`]：集群图——由已提交状态构建、按 `map_epoch` 缓存、顺带计算集群警告（M4：短链部分）
//! - [`chains`]：链的生成 `gen_chains` 与给 placement 看的 `ChainView`（M4；链管理器在 M9）
//! - [`cluster_mgr`]：cluster-mgr 线程（M4 只在 leader 就绪与集群图变化时把集群警告写进日志）
//!
//! - [`targets`]：target 心跳的 leader 一侧（更新 TargetRecV1、记下用量）与 StatFs（M5）
//!
//! join（M8）、集群插件清单的比对与退出（plugins.rs，M8）随里程碑加入。

pub mod chains;
pub mod cluster_mgr;
pub mod map;
pub mod targets;
