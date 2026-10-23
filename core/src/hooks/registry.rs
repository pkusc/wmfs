//! 插件集合 `PluginSet` 与启动校验（design.md 3.7.2、3.7.5，ADR-0025）。
//!
//! 注册表在二进制 crate（`cli/src/registry.rs`，全仓库唯一"知道有哪些插件"的文件）：它 `new` 一个集合、每个插件一行
//! 加进来、再 `validate`；core 只认 trait。`node.toml` 只能启停插件（`[plugins] disable`）与给插件部署参数
//! （`[plugin.<name>]`），**不能改顺序**——顺序由 altitude 决定。
//!
//! 实现注记（M1）：M1 的集合里只有 transport 与 authn 两种钩子（只有它们的 `add` 方法），`validate` 也只校验这两种用得到的
//! 规则。其余钩子特有的规则随各自的 `add` 方法一起加入：唯一型钩子（chunk_store、meta_engine）"恰好一个"（M3、M5）、
//! transform 的类别子区间（checksum 700–729、compress 730–759、encrypt 760–789）与 qos "只允许一个活跃"（M5、M13）。
//! 实现注记（M3）：meta_engine 的实现（embedded-kv）在 core 里（元数据 Raft 是契约），注册表登记的是它的 manifest
//! （[`PluginSet::meta_engine`]），服务端装配时据此建引擎。`validate` 查"最多一个"（只做传输的进程——`wmfs ping`、
//! 组件测试——不需要元数据引擎），服务端装配（`server::start`）再查"恰好一个"。
//! 实现注记（M4）：加了 authz 与 placement 两种钩子（各自的 add 方法与访问器）；它们的规则与 authn 相同（altitude 区间、
//! 同一钩子内不重名、altitude 不相同），已由 `check_one` 与 `validate` 的通用部分覆盖。
//! 实现注记（M5）：加了 qos、codec、transform、chunk_store、bg_task 五种钩子。各自的规则：transform 的 altitude 要落在它
//! 那一类的子区间（checksum 700–729、compress 730–759、encrypt 760–789），同一类里算法编号不重复；codec 编号不重复；
//! qos 只允许一个活跃、chunk_store 最多一个（服务端装配时再查"恰好一个"）。后台任务（`BgTask` 只是 `Send`）放在锁里，
//! 服务端装配时整批取走交给调度器（[`PluginSet::take_bg_tasks`]），它们的 manifest 另存一份给清单与校验用。

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::base::config::PluginTable;
use crate::base::lock;
use crate::bg::task::BgTask;
use crate::hooks::manifest::{CORE_ABI_VERSION, Hook, Manifest};
use crate::hooks::traits::{
    AuthnPlugin, AuthzPlugin, ChunkStore, CodecPlugin, PlacementPlugin, QosPlugin, TransformKind, TransformPlugin,
    Transport,
};
use crate::meta::schema::records::PluginListV1;

/// 本节点装上的插件（按钩子分开、同一钩子内按 altitude 升序）。
#[derive(Default)]
pub struct PluginSet {
    disable: Vec<String>,
    transports: Vec<Arc<dyn Transport>>,
    authn: Vec<Arc<dyn AuthnPlugin>>,
    authz: Vec<Arc<dyn AuthzPlugin>>,
    placement: Vec<Arc<dyn PlacementPlugin>>,
    meta_engines: Vec<&'static Manifest>,
    qos: Vec<Arc<dyn QosPlugin>>,
    codecs: Vec<Arc<dyn CodecPlugin>>,
    transforms: Vec<Arc<dyn TransformPlugin>>,
    chunk_stores: Vec<Arc<dyn ChunkStore>>,
    bg_tasks: Mutex<Vec<Box<dyn BgTask>>>, // 服务端装配时整批取走（调度器独占它们）
    bg_manifests: Vec<&'static Manifest>,  // 取走之后清单与校验仍要用
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

    pub fn authz(&mut self, a: Arc<dyn AuthzPlugin>) {
        if self.enabled(a.manifest()) {
            self.authz.push(a);
            self.authz.sort_by_key(|p| p.manifest().altitude);
        }
    }

    pub fn placement(&mut self, p: Arc<dyn PlacementPlugin>) {
        if self.enabled(p.manifest()) {
            self.placement.push(p);
            self.placement.sort_by_key(|p| p.manifest().altitude);
        }
    }

    /// 唯一型钩子 meta_engine：登记实现的 manifest（实现在 core，见文件头的实现注记）。
    pub fn meta_engine(&mut self, m: &'static Manifest) {
        if self.enabled(m) {
            self.meta_engines.push(m);
        }
    }

    pub fn qos(&mut self, p: Arc<dyn QosPlugin>) {
        if self.enabled(p.manifest()) {
            self.qos.push(p);
        }
    }

    pub fn codec(&mut self, p: Arc<dyn CodecPlugin>) {
        if self.enabled(p.manifest()) {
            self.codecs.push(p);
        }
    }

    pub fn transform(&mut self, p: Arc<dyn TransformPlugin>) {
        if self.enabled(p.manifest()) {
            self.transforms.push(p);
            self.transforms.sort_by_key(|p| p.manifest().altitude);
        }
    }

    pub fn chunk_store(&mut self, p: Arc<dyn ChunkStore>) {
        if self.enabled(p.manifest()) {
            self.chunk_stores.push(p);
        }
    }

    pub fn bg_task(&mut self, t: Box<dyn BgTask>) {
        if self.enabled(t.manifest()) {
            self.bg_manifests.push(t.manifest());
            lock(&self.bg_tasks).push(t);
        }
    }

    /// 取走全部后台任务（服务端装配时交给调度器；只能取一次，之后是空的）。
    pub fn take_bg_tasks(&self) -> Vec<Box<dyn BgTask>> {
        std::mem::take(&mut *lock(&self.bg_tasks))
    }

    pub fn qos_plugins(&self) -> &[Arc<dyn QosPlugin>] {
        &self.qos
    }

    pub fn codec_plugins(&self) -> &[Arc<dyn CodecPlugin>] {
        &self.codecs
    }

    pub fn transform_plugins(&self) -> &[Arc<dyn TransformPlugin>] {
        &self.transforms
    }

    /// 启用的 chunk_store（`validate` 之后最多一个）。
    pub fn chunk_store_plugin(&self) -> Option<&Arc<dyn ChunkStore>> {
        self.chunk_stores.first()
    }

    /// 启用的 meta_engine（`validate` 之后最多一个）。
    pub fn meta_engine_manifest(&self) -> Option<&'static Manifest> {
        self.meta_engines.first().copied()
    }

    pub fn transports(&self) -> &[Arc<dyn Transport>] {
        &self.transports
    }

    pub fn authn_plugins(&self) -> &[Arc<dyn AuthnPlugin>] {
        &self.authn
    }

    pub fn authz_plugins(&self) -> &[Arc<dyn AuthzPlugin>] {
        &self.authz
    }

    pub fn placement_plugins(&self) -> &[Arc<dyn PlacementPlugin>] {
        &self.placement
    }

    /// 全部 (钩子, manifest)。
    fn all(&self) -> Vec<(Hook, &'static Manifest)> {
        let t = self.transports.iter().map(|p| (Hook::Transport, p.manifest()));
        let a = self.authn.iter().map(|p| (Hook::Authn, p.manifest()));
        let z = self.authz.iter().map(|p| (Hook::Authz, p.manifest()));
        let pl = self.placement.iter().map(|p| (Hook::Placement, p.manifest()));
        let me = self.meta_engines.iter().map(|m| (Hook::MetaEngine, *m));
        let q = self.qos.iter().map(|p| (Hook::Qos, p.manifest()));
        let c = self.codecs.iter().map(|p| (Hook::Codec, p.manifest()));
        let tf = self.transforms.iter().map(|p| (Hook::Transform, p.manifest()));
        let cs = self.chunk_stores.iter().map(|p| (Hook::ChunkStore, p.manifest()));
        let bg = self.bg_manifests.iter().map(|m| (Hook::BgTask, *m));
        t.chain(a).chain(z).chain(pl).chain(me).chain(q).chain(c).chain(tf).chain(cs).chain(bg).collect()
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
        if self.meta_engines.len() > 1 {
            let names: Vec<&str> = self.meta_engines.iter().map(|m| m.name).collect();
            return Err(format!("meta_engine 是唯一型钩子，却启用了 {} 个：{names:?}", names.len()));
        }
        self.check_data_hooks()?;
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

    /// 数据面几种钩子（qos、chunk_store、transform、codec，M5）特有的规则（见文件头的实现注记）。
    fn check_data_hooks(&self) -> Result<(), String> {
        let one = |n: usize, h: Hook| match n {
            0 | 1 => Ok(()),
            n => Err(format!(
                "{} 只允许一个活跃的插件，却启用了 {n} 个（用 node.toml 的 [plugins] disable 停掉多余的）",
                h.name()
            )),
        };
        one(self.qos.len(), Hook::Qos)?;
        one(self.chunk_stores.len(), Hook::ChunkStore)?;
        for (i, t) in self.transforms.iter().enumerate() {
            let (m, kind) = (t.manifest(), t.kind());
            let (lo, hi) = transform_range(kind);
            if !(lo..=hi).contains(&m.altitude) {
                return Err(format!(
                    "transform 插件 {} 是 {kind:?} 类，altitude {} 应在 {lo}–{hi}",
                    m.name, m.altitude
                ));
            }
            if let Some(o) = self.transforms[..i].iter().find(|o| o.kind() == kind && o.alg_id() == t.alg_id()) {
                return Err(format!(
                    "transform 插件 {} 与 {} 的算法编号都是 {}",
                    m.name,
                    o.manifest().name,
                    t.alg_id()
                ));
            }
        }
        for (i, c) in self.codecs.iter().enumerate() {
            if let Some(o) = self.codecs[..i].iter().find(|o| o.codec_id() == c.codec_id()) {
                let (a, b) = (c.manifest().name, o.manifest().name);
                return Err(format!("codec 插件 {a} 与 {b} 的编号都是 {}", c.codec_id()));
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

/// transform 每一类的 altitude 子区间（3.7.2）。
fn transform_range(kind: TransformKind) -> (u16, u16) {
    match kind {
        TransformKind::Checksum => (700, 729),
        TransformKind::Compress => (730, 759),
        TransformKind::Encrypt => (760, 789),
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
