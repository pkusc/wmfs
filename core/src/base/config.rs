//! node.toml：本节点的部署参数（design.md 3.2.2、3.7.2；`NetConfig` 见 3.12）。
//!
//! 只放部署参数：各传输插件的监听地址（`[plugin.<name>]`）、control/data scheme（`[net]`）、管理 API 地址
//! （`[mgmt]`）、target 列表（`[[target]]`）、内存池大小（`[mem]`）、插件启停（`[plugins]`）。
//! 行为策略一律是目录属性，不得出现在这里（理念文档原则 3）。不认识的键 = 错误（拼写错误不会被静默忽略）；
//! 插件自己的配置表 `[plugin.<name>]` 由插件按它的 `config_schema` 校验（3.7.2）。
//!
//! 例子（`wmfs init` 生成的样子）。没写的段取默认值；段里没写的键一般也取默认值，**例外是 `[mem]`**：
//! 要么整段省略（等于 `PoolConfig::server()`），要么四个键都写（只写一部分会报"missing field"）。
//! ```toml
//! debug_ops = false
//! failpoints_api = false
//!
//! [net]
//! control_scheme = "tcp"
//! data_scheme = "tcp"
//!
//! [mgmt]
//! listen = "127.0.0.1:9380"
//!
//! [mem]
//! s_mb = 256
//! m_slots = 124
//! l_slots = 15
//! repl_lane = true
//!
//! [plugins]
//! disable = []
//!
//! [plugin.tcp]
//! listen = "127.0.0.1:9300"
//!
//! [[target]]
//! path = "/home/me/wmfs-data/targets/1"
//! labels = ["nvme"]
//! capacity = "20G"
//! ```

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::units;
use crate::buf::pool::PoolConfig;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct NodeConfig {
    pub debug_ops: bool,      // 允许 DebugPut/DebugGet（仍只给 admin key；M3 演示用）
    pub failpoints_api: bool, // 允许经管理 API 配置 fail point（测试用；wmfs dev 默认打开）
    pub net: NetConfig,
    pub mgmt: MgmtConfig,
    pub mem: PoolConfig,
    pub plugins: PluginsConfig,
    pub plugin: BTreeMap<String, toml::Table>, // [plugin.<name>]：插件的部署参数
    #[serde(rename = "target")]
    pub targets: Vec<TargetConfig>, // [[target]]
    pub debug: DebugConfig,
}

impl Default for NodeConfig {
    fn default() -> NodeConfig {
        NodeConfig {
            debug_ops: false,
            failpoints_api: false,
            net: NetConfig::default(),
            mgmt: MgmtConfig::default(),
            mem: PoolConfig::server(),
            plugins: PluginsConfig::default(),
            plugin: BTreeMap::new(),
            targets: Vec::new(),
            debug: DebugConfig::default(),
        }
    }
}

// node.toml [net] 或挂载参数（3.12）
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct NetConfig {
    pub control_scheme: String,
    pub data_scheme: String,
    pub write_timeout_ms: u32, // 5000
    pub send_queue: u32,       // 1024
}

impl Default for NetConfig {
    fn default() -> NetConfig {
        NetConfig { control_scheme: "tcp".into(), data_scheme: "tcp".into(), write_timeout_ms: 5000, send_queue: 1024 }
    }
}

/// `[mgmt]`：管理 API（HTTP）的监听地址（7.1；`wmfs dev` 下第 n 个节点为 127.0.0.1:19n80）。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct MgmtConfig {
    pub listen: String,
}

impl Default for MgmtConfig {
    fn default() -> MgmtConfig {
        MgmtConfig { listen: "127.0.0.1:9380".into() }
    }
}

/// `[plugins]`：只能启停插件，不能改顺序（3.7.2）。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
#[serde(default, deny_unknown_fields)]
pub struct PluginsConfig {
    pub disable: Vec<String>, // 例如 ["quota", "mclock"]（minimal profile）
    pub dylib: Vec<String>,   // 动态库插件的路径（3.7.6；加载失败 = 拒绝启动）
}

/// `[[target]]`：一个 target = 一个目录/一块盘。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TargetConfig {
    pub path: String,
    #[serde(default)]
    pub labels: Vec<String>, // 存储类按标签匹配（6.1）
    #[serde(default = "zero")]
    pub capacity: String, // 容量上限，例如 "20G"；"0" = 不限（与 statvfs 取小，防止测试写满系统盘）
}

fn zero() -> String {
    "0".into()
}

impl TargetConfig {
    /// 容量上限（字节）；0 = 不限。写错时返回中文原因（不会静默当成"不限"）；`NodeConfig::parse` 已对每个 target 调用过它。
    pub fn capacity_bytes(&self) -> Result<u64, String> {
        units::parse_size(&self.capacity).map_err(|e| format!("target {} 的 capacity：{e}", self.path))
    }
}

/// `[debug]`：只给开发环境用。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
#[serde(default, deny_unknown_fields)]
pub struct DebugConfig {
    pub host_id: Option<String>, // wmfs dev 用它给每个节点模拟不同的"主机"（locality 演示，1.1）
}

impl NodeConfig {
    /// 解析并校验 node.toml 的文本；错误信息是给人看的中文。
    pub fn parse(text: &str) -> Result<NodeConfig, String> {
        let c: NodeConfig = toml::from_str(text).map_err(|e| format!("node.toml 解析失败：{e}"))?;
        if c.net.control_scheme.is_empty() || c.net.data_scheme.is_empty() {
            return Err("node.toml：[net] control_scheme / data_scheme 不能为空".into());
        }
        if c.mem.s_mb == 0 || c.mem.m_slots == 0 || c.mem.l_slots == 0 {
            return Err("node.toml：[mem] 的 s_mb、m_slots、l_slots 都必须大于 0".into());
        }
        for t in &c.targets {
            t.capacity_bytes().map_err(|e| format!("node.toml：{e}"))?;
        }
        Ok(c)
    }

    /// 写回文件用的 TOML 文本。
    pub fn to_toml(&self) -> String {
        toml::to_string(self).unwrap_or_else(|e| panic!("不变量破坏：NodeConfig 无法编码为 TOML：{e}"))
    }

    /// `[plugin.<name>]` 表（传给插件的构造函数）；没有就是空表。
    pub fn plugin_cfg(&self, name: &str) -> toml::Table {
        self.plugin.get(name).cloned().unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXAMPLE: &str = r#"
debug_ops = true
[net]
data_scheme = "rdma"
[mgmt]
listen = "127.0.0.1:19180"
[mem]
s_mb = 128
m_slots = 31
l_slots = 8
repl_lane = true
[plugins]
disable = ["quota", "mclock"]
[plugin.tcp]
listen = "127.0.0.1:19100"
[[target]]
path = "/tmp/t1"
labels = ["nvme"]
capacity = "20G"
[[target]]
path = "/tmp/t2"
[debug]
host_id = "host-2"
"#;

    #[test]
    fn parse_example() {
        let c = NodeConfig::parse(EXAMPLE).unwrap();
        assert!(c.debug_ops && !c.failpoints_api);
        assert_eq!(c.net.control_scheme, "tcp", "没写的键取默认值");
        assert_eq!(c.net.data_scheme, "rdma");
        assert_eq!(c.net.write_timeout_ms, 5000);
        assert_eq!(c.mem, PoolConfig::dev_server());
        assert_eq!(c.plugins.disable, vec!["quota", "mclock"]);
        assert_eq!(c.plugin_cfg("tcp").get("listen").and_then(|v| v.as_str()), Some("127.0.0.1:19100"));
        assert!(c.plugin_cfg("rdma").is_empty());
        assert_eq!(c.targets.len(), 2);
        assert_eq!(c.targets[0].capacity_bytes(), Ok(20 << 30));
        assert_eq!(c.targets[1].capacity_bytes(), Ok(0), "没写 capacity = 不限");
        let bad = TargetConfig { path: "/t".into(), labels: vec![], capacity: "20Q".into() };
        assert!(bad.capacity_bytes().unwrap_err().contains("/t"), "写错的容量返回错误，而不是当成不限");
        assert_eq!(c.debug.host_id.as_deref(), Some("host-2"));
    }

    #[test]
    fn empty_file_gives_defaults_and_round_trips() {
        let c = NodeConfig::parse("").unwrap();
        assert_eq!(c, NodeConfig::default());
        assert_eq!(c.mem, PoolConfig::server());
        let full = NodeConfig::parse(EXAMPLE).unwrap();
        assert_eq!(NodeConfig::parse(&full.to_toml()).unwrap(), full);
    }

    #[test]
    fn unknown_keys_and_bad_values_are_rejected() {
        assert!(NodeConfig::parse("[net]\ncontol_scheme = \"tcp\"\n").unwrap_err().contains("contol_scheme"));
        assert!(NodeConfig::parse("qos = 1\n").is_err(), "行为策略不能写进 node.toml");
        assert!(NodeConfig::parse("[[target]]\npath = \"/x\"\ncapacity = \"20Q\"\n").is_err());
        assert!(NodeConfig::parse("[net]\ndata_scheme = \"\"\n").is_err());
        assert!(NodeConfig::parse("[mem]\ns_mb = 0\nm_slots = 1\nl_slots = 1\nrepl_lane = false\n").is_err());
        // 每一段都拒绝拼错的键
        assert!(NodeConfig::parse("[mgmt]\nlisen = \"x\"\n").unwrap_err().contains("lisen"));
        assert!(NodeConfig::parse("[mem]\ns_mbb = 1\n").is_err());
        assert!(NodeConfig::parse("[plugins]\ndisabel = []\n").is_err());
        assert!(NodeConfig::parse("[[target]]\npath = \"/x\"\nlabel = []\n").is_err());
        assert!(NodeConfig::parse("[debug]\nhostid = \"h\"\n").is_err());
        // [mem] 只写一部分键 = 错误（见文件头的说明）
        assert!(NodeConfig::parse("[mem]\ns_mb = 128\n").is_err());
    }
}
