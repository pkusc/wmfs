//! mgr 宿主：自己的 HTTP 路由（tiny_http）与两个进程外钩子 trait（design.md 3.7.3、3.12、4.13、4.14）。
//!
//! 宿主规则（4.13，对五个插件都一样）：启动时读每个插件的 manifest.toml，校验理念文档 4.3.1 的 7 个必填字段与
//! `abi_version == MGR_ABI_VERSION`，不符合就拒绝启动（[`check_manifest`]）；每次 deliver/tick 先过 fail point
//! `plugin.<名字>.<event_sink|mgmt_ext>`，再计时、计数（[`Hooked`]：`wmfs_hook_*{hook,plugin}`，mgr 自己的 `/metrics` 输出）；changelog
//! 只有宿主的一个长轮询循环在拉（`host/events.rs`），按插件分发。HTTP 服务与每 5 s 一次的 tick 在 `host/serve.rs`。

use std::sync::Arc;
use std::time::Instant;

use wmfs_api::failpoint::fail_point;
use wmfs_api::metrics::{self, Counter, Histogram};
use wmfs_api::types::ChangeEvent;

use crate::api::ApiClient;

pub(crate) mod events;
pub(crate) mod serve;

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

impl HttpOut {
    /// JSON 回复（实现注记（M14）：只增的便捷函数）。
    pub fn json<T: serde::Serialize>(status: u16, v: &T) -> HttpOut {
        let body = serde_json::to_vec(v).unwrap_or_else(|e| format!("{{\"error\":\"{e}\"}}").into_bytes());
        HttpOut { status, content_type: "application/json", body }
    }
    /// 纯文本回复。
    pub fn text(status: u16, s: String) -> HttpOut {
        HttpOut { status, content_type: "text/plain; charset=utf-8", body: s.into_bytes() }
    }
}

/// 一个路由的处理函数。
pub type Handler = Box<dyn Fn(&HttpIn) -> HttpOut + Send + Sync>;

/// mgr 自己的 HTTP 路由（tiny_http）；MgmtExt 插件在 start() 里注册页面。
#[derive(Default)]
pub struct Router {
    routes: Vec<(String, Handler)>,
}

impl Router {
    /// 登记 `prefix`：路径等于它、或以 `<prefix>/` 开头的请求都交给 `h`（"/" 只匹配首页）。
    pub fn add(&mut self, prefix: &str, h: Handler) {
        self.routes.push((prefix.trim_end_matches('/').to_string(), h));
    }

    /// 最长的前缀匹配；没有 → 404。
    pub fn route(&self, req: &HttpIn) -> HttpOut {
        let hit = |p: &str| match p {
            "" => req.path == "/",
            p => req.path == p || req.path.strip_prefix(p).is_some_and(|r| r.starts_with('/')),
        };
        match self.routes.iter().filter(|(p, _)| hit(p)).max_by_key(|(p, _)| p.len()) {
            Some((_, h)) => h(req),
            None => HttpOut::text(404, format!("没有这个页面：{}", req.path)),
        }
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

/// 启动时校验插件的 manifest.toml（见文件头）：7 个必填字段齐全、类型对；`abi_version == MGR_ABI_VERSION`；声明了钩子 `hook`；进程外插件
/// `hot_path = false`、`altitude = 0`；`fail_points` 里有 `plugin.<名字>.<钩子>`。返回插件名；错误是给人看的中文（mgr 据此拒绝启动）。
pub fn check_manifest(text: &str, hook: &str) -> Result<String, String> {
    let t: toml::Table = text.parse().map_err(|e| format!("manifest.toml 不是合法 TOML：{e}"))?;
    let name = t.get("name").and_then(|v| v.as_str()).ok_or("manifest 缺少 name")?.to_string();
    let bad = |why: &str| Err(format!("插件 {name} 的 manifest：{why}，拒绝启动"));
    let hooks =
        t.get("hooks").and_then(|v| v.as_array()).map(|a| a.iter().filter_map(|h| h.as_str()).collect::<Vec<_>>());
    let fps = t
        .get("fail_points")
        .and_then(|v| v.as_array())
        .map(|a| a.iter().filter_map(|h| h.as_str()).collect::<Vec<_>>());
    if !t.get("version").is_some_and(|v| v.is_str()) || !t.get("config_schema").is_some_and(|v| v.is_table()) {
        return bad("缺少 version 或 config_schema");
    }
    if !hooks.is_some_and(|h| h.contains(&hook)) {
        return bad(&format!("hooks 里没有 {hook}"));
    }
    if t.get("altitude").and_then(|v| v.as_integer()) != Some(0)
        || t.get("hot_path").and_then(|v| v.as_bool()) != Some(false)
    {
        return bad("进程外插件的 altitude 填 0、hot_path 填 false");
    }
    if t.get("abi_version").and_then(|v| v.as_integer()) != Some(MGR_ABI_VERSION as i64) {
        return bad(&format!("abi_version 与 mgr 的 {MGR_ABI_VERSION} 不一致"));
    }
    if !fps.is_some_and(|f| f.contains(&format!("plugin.{name}.{hook}").as_str())) {
        return bad(&format!("fail_points 里没有声明 plugin.{name}.{hook}"));
    }
    Ok(name)
}

/// 一个插件在一个钩子上的调用包装（3.7.4 的进程外版本）：先过 fail point，再计时、计数。
pub(crate) struct Hooked {
    pub name: String,
    fp: String,
    calls: Arc<Counter>,
    secs: Arc<Histogram>,
    errs: Arc<Counter>,
}

impl Hooked {
    pub(crate) fn new(name: &str, hook: &'static str) -> Hooked {
        let l = [("hook", hook), ("plugin", name)];
        Hooked {
            name: name.to_string(),
            fp: format!("plugin.{name}.{hook}"),
            calls: metrics::counter("wmfs_hook_calls_total", "钩子调用次数", &l),
            secs: metrics::histogram("wmfs_hook_seconds", "钩子单次调用耗时", &l, metrics::SECONDS_BUCKETS),
            errs: metrics::counter("wmfs_hook_errors_total", "钩子调用出错（含 fail point 触发）的次数", &l),
        }
    }

    /// fail point 触发（配置了 `return(码)`）时不调用 `f`，直接以注入的码失败（计为一次错误）。
    pub(crate) fn call<T>(&self, f: impl FnOnce() -> Result<T, String>) -> Result<T, String> {
        if let Err(c) = fail_point(&self.fp) {
            self.errs.inc();
            return Err(format!("fail point {}：{}", self.fp, wmfs_api::codes::name(c).unwrap_or("?")));
        }
        let t0 = Instant::now();
        let r = f();
        self.calls.inc();
        self.secs.observe(t0.elapsed().as_secs_f64());
        if r.is_err() {
            self.errs.inc();
        }
        r
    }
}

#[cfg(test)]
mod tests;
