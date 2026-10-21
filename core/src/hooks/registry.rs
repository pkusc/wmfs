//! 插件集合 `PluginSet` 与启动校验（design.md 3.7.2、3.7.5，ADR-0025）。
//!
//! 注册表在二进制 crate（`cli/src/registry.rs`，全仓库唯一"知道有哪些插件"的文件）：它 `new` 一个集合、每个插件一行
//! 加进来、再 `validate`；core 只认 trait。`node.toml` 只能启停插件（`[plugins] disable`）与给插件部署参数
//! （`[plugin.<name>]`），**不能改顺序**——顺序由 altitude 决定。
//!
//! 实现注记（M1）：M1 的集合里只有 transport 与 authn 两种钩子（只有它们的 `add` 方法），`validate` 也只校验这两种用得到的
//! 规则。其余钩子特有的规则随各自的 `add` 方法一起加入：唯一型钩子（chunk_store、meta_engine）"恰好一个"（M3、M5）、
//! transform 的类别子区间（checksum 700–729、compress 730–759、encrypt 760–789）与 qos "只允许一个活跃"（M5、M13）。

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::base::config::PluginTable;
use crate::hooks::manifest::{CORE_ABI_VERSION, Hook, Manifest};
use crate::hooks::traits::{AuthnPlugin, Transport};
use crate::meta::schema::records::PluginListV1;

/// 本节点装上的插件（按钩子分开、同一钩子内按 altitude 升序）。
#[derive(Default)]
pub struct PluginSet {
    disable: Vec<String>,
    transports: Vec<Arc<dyn Transport>>,
    authn: Vec<Arc<dyn AuthnPlugin>>,
}

impl PluginSet {
    /// `disable`：node.toml 的 `[plugins] disable`；被禁用的插件加进来时直接跳过（不认识的名字不报错：
    /// 例如 minimal profile 的 `["quota", "mclock"]`，在这两个插件实现之前也能用）。
    pub fn new(disable: &[String]) -> PluginSet {
        PluginSet { disable: disable.to_vec(), ..Default::default() }
    }

    fn enabled(&self, m: &Manifest) -> bool {
        !self.disable.iter().any(|d| d == m.name)
    }

    pub fn transport(&mut self, t: Arc<dyn Transport>) {
        if self.enabled(t.manifest()) {
            self.transports.push(t);
        }
    }

    pub fn authn(&mut self, a: Arc<dyn AuthnPlugin>) {
        if self.enabled(a.manifest()) {
            self.authn.push(a);
            self.authn.sort_by_key(|p| p.manifest().altitude);
        }
    }

    pub fn transports(&self) -> &[Arc<dyn Transport>] {
        &self.transports
    }

    pub fn authn_plugins(&self) -> &[Arc<dyn AuthnPlugin>] {
        &self.authn
    }

    /// 全部 (钩子, manifest)。
    fn all(&self) -> Vec<(Hook, &'static Manifest)> {
        let t = self.transports.iter().map(|p| (Hook::Transport, p.manifest()));
        t.chain(self.authn.iter().map(|p| (Hook::Authn, p.manifest()))).collect()
    }

    /// 启动时校验（3.7.2）：每个插件的 manifest（[`check_one`]）；同一钩子内不重名、altitude 不相同；不同的插件不同名；
    /// 传输的 scheme 唯一；`[plugin.<name>]` 只能给已注册的插件、只能写它 `config_schema` 里有的键。错误是给人看的中文。
    pub fn validate(&self, plugin_cfg: &BTreeMap<String, PluginTable>) -> Result<(), String> {
        let all = self.all();
        for (i, (hook, m)) in all.iter().enumerate() {
            check_one(*hook, m)?;
            // 和排在前面的插件逐个比
            for (h2, o) in &all[..i] {
                if h2 == hook && o.name == m.name {
                    return Err(format!("{} 钩子上有两个插件都叫 {}", hook.name(), m.name));
                }
                if h2 == hook && m.altitude != 0 && o.altitude == m.altitude {
                    let (a, h) = (m.altitude, hook.name());
                    return Err(format!(
                        "插件 {} 与 {} 在 {h} 钩子上的 altitude 都是 {a}（同一钩子内必须唯一）",
                        m.name, o.name
                    ));
                }
                // 同一个插件可以挂在多个钩子上（它的 manifest 声明了这几个钩子）；名字相同、manifest 却不同 = 两个插件撞名
                if h2 != hook && o.name == m.name && **o != **m {
                    return Err(format!(
                        "有两个不同的插件都叫 {}（{} 与 {} 钩子上各一个）",
                        m.name,
                        h2.name(),
                        hook.name()
                    ));
                }
            }
        }
        for (i, t) in self.transports.iter().enumerate() {
            if self.transports[..i].iter().any(|o| o.scheme() == t.scheme()) {
                return Err(format!("有两个传输插件都用 scheme {}", t.scheme()));
            }
        }
        for (name, table) in plugin_cfg {
            let Some((_, m)) = all.iter().find(|(_, m)| m.name == name) else {
                if self.disable.contains(name) {
                    continue; // 被禁用的插件的配置不使用
                }
                return Err(format!("node.toml 里有 [plugin.{name}]，但本二进制没有启用这个插件"));
            };
            let keys = schema_keys(m)?;
            if let Some(k) = table.keys().find(|k| !keys.contains(k)) {
                return Err(format!("node.toml 的 [plugin.{name}] 里有不认识的键 {k}（可用：{keys:?}）"));
            }
        }
        Ok(())
    }

    /// 本节点会影响策略的插件清单（3.7.2 第 1 条）：8 个钩子上启用的插件各一条 `"钩子:名字"`，排序；只比名字、不比版本。
    pub fn plugin_list(&self) -> PluginListV1 {
        let mut plugins: Vec<String> = self
            .all()
            .into_iter()
            .filter(|(h, _)| h.in_cluster_list())
            .map(|(h, m)| format!("{}:{}", h.name(), m.name))
            .collect();
        plugins.sort();
        plugins.dedup();
        PluginListV1 { plugins }
    }
}

/// 一个插件挂在一个钩子上时的 manifest 检查（3.7.2）：`abi_version` 匹配（不匹配拒绝启动，不尝试适配）；manifest 声明了
/// 这个钩子；altitude 在区间内（不排序与唯一型的钩子填 0）；传输插件一定在热路径上；热路径插件给出 `budget_ns`；
/// `fail_points` 里声明了 `plugin.<名字>.<钩子>`；`config_schema` 合法。
fn check_one(hook: Hook, m: &Manifest) -> Result<(), String> {
    let (n, h) = (m.name, hook.name());
    if m.abi_version != CORE_ABI_VERSION {
        return Err(format!("插件 {n} 的 abi_version {} 与本体的 {CORE_ABI_VERSION} 不一致，拒绝启动", m.abi_version));
    }
    if !m.hooks.contains(&hook) {
        return Err(format!("插件 {n} 的 manifest 没有声明钩子 {h}"));
    }
    match hook.altitude_range() {
        Some((lo, hi)) if !(lo..=hi).contains(&m.altitude) => {
            return Err(format!("插件 {n} 的 altitude {} 不在 {h} 的区间 {lo}–{hi} 里", m.altitude));
        }
        None if m.altitude != 0 => return Err(format!("插件 {n}：{h} 钩子的 altitude 必须填 0")),
        _ => {}
    }
    if hook == Hook::Transport && !m.hot_path {
        return Err(format!("插件 {n}：传输插件一定在热路径上，必须写 hot_path = true"));
    }
    if m.hot_path && m.budget_ns == 0 {
        return Err(format!("插件 {n} 在热路径上，必须给出 budget_ns（单次调用的时间预算）"));
    }
    let fp = format!("plugin.{n}.{h}");
    if !m.fail_points.contains(&fp.as_str()) {
        return Err(format!("插件 {n} 的 fail_points 里没有声明 {fp}"));
    }
    schema_keys(m)?;
    Ok(())
}

/// `config_schema` 的全部键；每一项必须是 `{ type, default, doc }` 表。
fn schema_keys(m: &Manifest) -> Result<Vec<String>, String> {
    let t: PluginTable =
        m.config_schema.parse().map_err(|e| format!("插件 {} 的 config_schema 不是合法 TOML：{e}", m.name))?;
    for (k, v) in &t {
        let ok = v.as_table().is_some_and(|e| ["type", "default", "doc"].iter().all(|f| e.contains_key(*f)));
        if !ok {
            return Err(format!("插件 {} 的配置项 {k} 应写成 {{ type, default, doc }}", m.name));
        }
    }
    Ok(t.keys().cloned().collect())
}

#[cfg(test)]
mod tests;
