//! 插件 manifest（design.md 3.7.1，ADR-0025）。
//!
//! 每个插件目录里另有给人看的 `manifest.toml`（理念文档 4.3.1 的 7 个必填字段 + `attrs`、`budget_ns`、`fail_points`），
//! 内置插件用 `include_str!` 编进二进制，单元测试断言它与代码里的常量逐字段相等，防止漂移。
//! 启动时 `PluginSet::validate()` 检查 altitude 区间与唯一性、`abi_version`、热路径预算、fail point 声明（3.7.2，M1 实现）。

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Hook {
    Transport,
    Authn,
    Authz,
    QuotaAdmission,
    Qos,
    Placement,
    Codec,
    Transform,
    ChunkStore,
    MetaEngine,
    CacheStore,
    BgTask,
    EventSink,
    Gateway,
    MgmtExt,
}

#[derive(Debug)]
pub struct Manifest {
    pub name: &'static str,
    pub version: &'static str,
    pub hooks: &'static [Hook],
    pub altitude: u16, // 唯一型钩子填 0
    pub hot_path: bool,
    pub config_schema: &'static str,    // TOML 文本：每个配置项 { type, default, doc }
    pub abi_version: u32,               // 静态插件 = CORE_ABI_VERSION；动态库 = 其 C ABI 版本
    pub attrs: &'static [&'static str], // 本插件消费的目录属性键（属性注册表据此校验，3.8）
    pub budget_ns: u64,                 // 热路径单次调用预算；超出计入 wmfs_hook_over_budget_total
    pub fail_points: &'static [&'static str],
}

pub const CORE_ABI_VERSION: u32 = 1; // 任何钩子 trait 或其参数结构变化（哪怕只加一个字段）都 +1

/// core 支持的动态库 C ABI 版本（3.7.6，首发 `[1]`）：不在列表里的一律拒绝加载并说明原因。
pub const C_ABI_VERSIONS: &[u32] = &[1];

impl Hook {
    /// 全部钩子（按 design.md 3.7.1 的顺序）；新增变体时 `name()` 的 match 会编译失败，提醒同时加到这里。
    pub const ALL: [Hook; 15] = [
        Hook::Transport,
        Hook::Authn,
        Hook::Authz,
        Hook::QuotaAdmission,
        Hook::Qos,
        Hook::Placement,
        Hook::Codec,
        Hook::Transform,
        Hook::ChunkStore,
        Hook::MetaEngine,
        Hook::CacheStore,
        Hook::BgTask,
        Hook::EventSink,
        Hook::Gateway,
        Hook::MgmtExt,
    ];

    /// manifest.toml、fail point 名 `plugin.<名字>.<钩子>` 与集群插件清单 `钩子:名字` 里的写法（例如 "quota_admission"）。
    pub fn name(self) -> &'static str {
        match self {
            Hook::Transport => "transport",
            Hook::Authn => "authn",
            Hook::Authz => "authz",
            Hook::QuotaAdmission => "quota_admission",
            Hook::Qos => "qos",
            Hook::Placement => "placement",
            Hook::Codec => "codec",
            Hook::Transform => "transform",
            Hook::ChunkStore => "chunk_store",
            Hook::MetaEngine => "meta_engine",
            Hook::CacheStore => "cache_store",
            Hook::BgTask => "bg_task",
            Hook::EventSink => "event_sink",
            Hook::Gateway => "gateway",
            Hook::MgmtExt => "mgmt_ext",
        }
    }

    pub fn from_name(s: &str) -> Option<Hook> {
        Hook::ALL.into_iter().find(|h| h.name() == s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hook_names_round_trip() {
        for (i, h) in Hook::ALL.into_iter().enumerate() {
            assert_eq!(Hook::from_name(h.name()), Some(h));
            assert!(!Hook::ALL[i + 1..].contains(&h), "{h:?} 在 ALL 里重复");
        }
        assert_eq!(Hook::QuotaAdmission.name(), "quota_admission");
        assert_eq!(Hook::from_name("Quota"), None);
        assert_eq!(CORE_ABI_VERSION, 1);
        assert_eq!(C_ABI_VERSIONS, &[1]);
    }
}
