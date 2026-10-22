//! `wmfs dev` 的单元测试：端口规则、`up --rdma` 的声明式改写。

use super::*;

#[test]
fn dev_ports_follow_the_19n00_scheme() {
    assert_eq!(super::ports(1), (19100, 19180));
    assert_eq!(super::ports(3), (19300, 19380));
}

#[test]
fn up_rdma_is_declarative() {
    let nd = std::env::temp_dir().join(format!("wmfs-dev-rdma-{}-{}", std::process::id(), clock::mono_ms()));
    std::fs::create_dir_all(&nd).unwrap();
    let cfg = NodeConfig { mem: PoolConfig::dev_server(), ..NodeConfig::default() };
    std::fs::write(nd.join("node.toml"), cfg.to_toml()).unwrap();
    let read = || NodeConfig::parse(&std::fs::read_to_string(nd.join("node.toml")).unwrap()).unwrap();
    let ip = Some("10.77.0.1".parse().unwrap());
    assert!(set_rdma(&nd, 2, ip).unwrap(), "--rdma：改写了 node.toml");
    let got = read();
    let r = got.plugin_cfg("rdma");
    assert_eq!(r.get("listen").and_then(|v| v.as_str()), Some("10.77.0.1:19201"), "引导端口 19n01");
    assert_eq!(r.get("max_qps").and_then(|v| v.as_integer()), Some(32), "dev 的 S 档只够 32 个 QP");
    assert_eq!((got.net.control_scheme.as_str(), got.net.data_scheme.as_str()), ("tcp", "rdma"));
    crate::registry::build_plugins(&got).unwrap(); // 插件按它的 config_schema 认这些键
    assert!(!set_rdma(&nd, 2, ip).unwrap(), "同样的参数再 up 一次：不变");
    assert!(set_rdma(&nd, 2, None).unwrap(), "不带 --rdma：改回只用 tcp");
    let got = read();
    assert!(!got.plugin.contains_key("rdma") && got.net.data_scheme == "tcp", "{got:?}");
    assert_eq!(got, cfg, "与初始化时的 node.toml 一样");
    assert!(!set_rdma(&nd, 2, None).unwrap());
    std::fs::remove_dir_all(&nd).unwrap();
}
