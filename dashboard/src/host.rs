//! mgr 宿主：自己的 HTTP 路由（tiny_http）与两个进程外钩子 trait（design.md 3.7.3、3.12、4.13、4.14）。
//!
//! 接口骨架（M0）：签名以这里为准，宿主在 M14 实现。宿主规则：启动时读每个插件的 manifest.toml，
//! 校验理念文档 4.3.1 的 7 个必填字段与 `abi_version == MGR_ABI_VERSION`，不符合就拒绝启动；
//! changelog 只有宿主的一个长轮询循环在拉，按插件分发。

use wmfs_api::types::ChangeEvent;

use crate::api::ApiClient;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HttpIn {
    pub method: String,
    pub path: String,
    pub query: String,
    pub body: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HttpOut {
    pub status: u16,
    pub content_type: &'static str,
    pub body: Vec<u8>,
}

/// mgr 自己的 HTTP 路由（tiny_http）；MgmtExt 插件在 start() 里注册页面。
pub struct Router {/* 路径前缀 → 处理函数 */}

#[allow(unused_variables)] // 骨架：函数体在 M14 实现，实现后删除这一行
impl Router {
    pub fn add(&mut self, prefix: &str, h: Box<dyn Fn(&HttpIn) -> HttpOut + Send + Sync>) {
        unimplemented!("M14：Router::add")
    }
}

// ---- event_sink / mgmt_ext：进程外的两个钩子，只能用管理 API 客户端 ----
pub const MGR_ABI_VERSION: u32 = 1; // 这两个 trait 或其参数变化就 +1

pub trait EventSink: Send {
    /// include_str!("manifest.toml")：mgr 启动时校验 7 个必填字段与 abi_version
    fn manifest_toml(&self) -> &'static str;
    /// ChangeEvent 来自 wmfs-api（changelog 记录的 JSON 形态）
    fn deliver(&mut self, batch: &[ChangeEvent]) -> Result<(), String>;
}

pub trait MgmtExt: Send {
    fn manifest_toml(&self) -> &'static str;
    fn start(&mut self, api: &ApiClient, router: &mut Router) -> Result<(), String>;
    /// 每 5 s 一次
    fn tick(&mut self, api: &ApiClient);
}
// mgr 宿主（host.rs）对每次 deliver/tick 先过 fail_point("plugin.<name>.<event_sink|mgmt_ext>")，再计时、计数（指标来自 wmfs-api 注册表）

#[cfg(test)]
mod tests {
    use super::*;

    /// 编译期检查（design.md 0.2 第 3 条）：两个进程外钩子 trait 必须对象安全（宿主持有 `Box<dyn …>`）。
    #[allow(dead_code)]
    fn mgr_hook_traits_are_object_safe(_: &dyn EventSink, _: &dyn MgmtExt) {}
}
