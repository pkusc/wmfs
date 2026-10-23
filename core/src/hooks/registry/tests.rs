//! PluginSet 校验的单元测试：好的集合、各种坏 manifest、重名与 altitude 冲突、配置键、禁用、manifest.toml 比对
//! （从 registry.rs 拆出，免得那个文件超过 400 行），以及 M5 的几种钩子特有的规则（qos 只能一个、transform 的子区间、编号唯一）。

use std::time::Duration;

use super::*;
use crate::base::ErrCode;
use crate::buf::{Buf, MemPool, RegionInfo};
use crate::hooks::manifest::check_manifest_toml;
use crate::hooks::traits::{AuthnInput, AuthnResult, BlockMeta, Conn, KeyLookup, TransformCtx};

struct FakeT(&'static Manifest, &'static str);
impl Transport for FakeT {
    fn manifest(&self) -> &'static Manifest {
        self.0
    }
    fn scheme(&self) -> &'static str {
        self.1
    }
    fn register_region(&self, _: &RegionInfo) -> Result<(), ErrCode> {
        Ok(())
    }
    fn listen(&self, _: MemPool, _: Arc<dyn Fn(Box<dyn Conn>) + Send + Sync>) -> Result<Option<String>, ErrCode> {
        Ok(None)
    }
    fn connect(&self, _: &str, _: MemPool, _: Duration) -> Result<Box<dyn Conn>, ErrCode> {
        Err(ErrCode::ENOSYS)
    }
}

struct FakeA(&'static Manifest);
impl AuthnPlugin for FakeA {
    fn manifest(&self) -> &'static Manifest {
        self.0
    }
    fn authenticate(&self, _: &AuthnInput, _: &dyn KeyLookup) -> AuthnResult {
        AuthnResult::Abstain
    }
    fn server_proof(&self, _: &AuthnInput, _: &dyn KeyLookup) -> Option<[u8; 32]> {
        None
    }
}

/// 造一个 manifest；`edit` 改掉其中一项（测试用，leak 成 'static）。
fn man(name: &'static str, hook: Hook, altitude: u16, edit: impl FnOnce(&mut Manifest)) -> &'static Manifest {
    let fp: &'static str = Box::leak(format!("plugin.{name}.{}", hook.name()).into_boxed_str());
    let mut m = Manifest {
        name,
        version: "0.1.0",
        hooks: Box::leak(vec![hook].into_boxed_slice()),
        altitude,
        hot_path: true,
        config_schema: "listen = { type = \"addr\", default = \"0.0.0.0:9300\", doc = \"监听地址\" }",
        abi_version: CORE_ABI_VERSION,
        attrs: &[],
        budget_ns: 1000,
        fail_points: Box::leak(vec![fp].into_boxed_slice()),
    };
    edit(&mut m);
    Box::leak(Box::new(m))
}

fn set(t: &[(&'static Manifest, &'static str)], a: &[&'static Manifest]) -> PluginSet {
    let mut s = PluginSet::new(&["off".to_string()]);
    for (m, scheme) in t {
        s.transport(Arc::new(FakeT(m, scheme)));
    }
    for m in a {
        s.authn(Arc::new(FakeA(m)));
    }
    s
}

fn no_cfg() -> BTreeMap<String, toml::Table> {
    BTreeMap::new()
}

#[test]
fn good_set_validates_and_lists_policy_plugins() {
    let s = set(
        &[(man("tcp", Hook::Transport, 0, |_| {}), "tcp")],
        &[man("b", Hook::Authn, 150, |_| {}), man("a", Hook::Authn, 110, |_| {})],
    );
    s.validate(&no_cfg()).unwrap();
    assert_eq!(s.authn_plugins()[0].manifest().name, "a", "同一钩子按 altitude 升序");
    // transport 不进集群插件清单（各节点可以不同）
    assert_eq!(s.plugin_list().plugins, vec!["authn:a", "authn:b"]);
}

#[test]
fn bad_manifests_are_rejected() {
    let t = |m| set(&[(m, "tcp")], &[]).validate(&no_cfg());
    let a = |m| set(&[], &[m]).validate(&no_cfg());
    assert!(t(man("t", Hook::Transport, 0, |m| m.abi_version = 2)).unwrap_err().contains("abi_version"));
    assert!(t(man("t", Hook::Transport, 5, |_| {})).unwrap_err().contains("altitude"));
    assert!(a(man("x", Hook::Authn, 99, |_| {})).unwrap_err().contains("100–199"));
    assert!(t(man("t", Hook::Transport, 0, |m| m.budget_ns = 0)).unwrap_err().contains("budget_ns"));
    assert!(t(man("t", Hook::Transport, 0, |m| m.hot_path = false)).is_err(), "传输一定在热路径上");
    assert!(t(man("t", Hook::Transport, 0, |m| m.fail_points = &[])).unwrap_err().contains("plugin.t.transport"));
    assert!(t(man("t", Hook::Transport, 0, |m| m.hooks = &[Hook::Authn])).is_err());
    assert!(t(man("t", Hook::Transport, 0, |m| m.config_schema = "listen = 1")).unwrap_err().contains("type"));
}

#[test]
fn duplicates_are_rejected() {
    let tcp = man("tcp", Hook::Transport, 0, |_| {});
    let tcp2 = man("tcp2", Hook::Transport, 0, |_| {});
    assert!(set(&[(tcp, "tcp"), (tcp2, "tcp")], &[]).validate(&no_cfg()).unwrap_err().contains("scheme"));
    let a1 = man("a1", Hook::Authn, 110, |_| {});
    let a2 = man("a2", Hook::Authn, 110, |_| {});
    assert!(set(&[], &[a1, a2]).validate(&no_cfg()).is_err(), "同一钩子 altitude 冲突");
    assert!(set(&[], &[a1, a1]).validate(&no_cfg()).unwrap_err().contains("都叫 a1"), "同一钩子重名");
    // 不同钩子上同名、manifest 却不同：两个插件撞名
    let other = man("tcp", Hook::Authn, 120, |_| {});
    assert!(set(&[(tcp, "tcp")], &[other]).validate(&no_cfg()).unwrap_err().contains("两个不同的插件都叫 tcp"));
}

#[test]
fn config_tables_are_checked_against_schema() {
    let s = set(&[(man("tcp", Hook::Transport, 0, |_| {}), "tcp")], &[]);
    let cfg = |name: &str, text: &str| BTreeMap::from([(name.to_string(), text.parse::<toml::Table>().unwrap())]);
    s.validate(&cfg("tcp", "listen = \"127.0.0.1:1\"")).unwrap();
    assert!(s.validate(&cfg("tcp", "lisen = \"x\"")).unwrap_err().contains("lisen"));
    assert!(s.validate(&cfg("fastx", "listen = \"x\"")).unwrap_err().contains("fastx"));
    s.validate(&cfg("off", "anything = 1")).unwrap(); // 被禁用的插件的配置不检查
}

#[test]
fn disabled_plugins_are_skipped() {
    let s = set(&[(man("off", Hook::Transport, 0, |_| {}), "tcp")], &[]);
    assert!(s.transports().is_empty());
}

#[test]
fn manifest_toml_must_match_code() {
    let m = man("tcp", Hook::Transport, 0, |_| {});
    let text = r#"
name = "tcp"
version = "0.1.0"
hooks = ["transport"]
altitude = 0
hot_path = true
abi_version = 1
attrs = []
budget_ns = 1000
fail_points = ["plugin.tcp.transport"]
[config_schema]
listen = { type = "addr", default = "0.0.0.0:9300", doc = "监听地址" }
"#;
    check_manifest_toml(m, text).unwrap();
    assert!(check_manifest_toml(m, &text.replace("1000", "999")).is_err(), "预算不一致");
    assert!(check_manifest_toml(m, &text.replace("0.0.0.0", "1.1.1.1")).is_err(), "配置默认值不一致");
    assert!(check_manifest_toml(m, &format!("{text}\nextra = 1")).is_err(), "多出的字段");
}

#[test]
fn meta_engine_is_a_unique_hook_outside_the_cluster_list() {
    use crate::meta::engine::engine::MANIFEST as KV;
    let mut s = PluginSet::new(&[]);
    s.meta_engine(&KV);
    s.validate(&no_cfg()).unwrap();
    assert_eq!(s.meta_engine_manifest().map(|m| m.name), Some("embedded-kv"));
    assert!(s.plugin_list().plugins.is_empty(), "meta_engine 不进集群插件清单（3.7.2 第 1 条）");
    s.meta_engine(&KV);
    assert!(s.validate(&no_cfg()).is_err(), "唯一型钩子：最多一个");
    let mut d = PluginSet::new(&["embedded-kv".into()]);
    d.meta_engine(&KV);
    assert!(d.meta_engine_manifest().is_none(), "node.toml 禁用了它：服务端装配时会拒绝启动");
}

struct FakeQ(&'static Manifest);
impl QosPlugin for FakeQ {
    fn manifest(&self) -> &'static Manifest {
        self.0
    }
    fn enqueue(&self, _: crate::hooks::traits::QosItem, _: crate::hooks::traits::QosSpec) {}
    fn dequeue(&self, _: std::time::Instant) -> Result<u64, Option<std::time::Instant>> {
        Err(None)
    }
    fn stats(&self) -> Vec<crate::hooks::traits::QueueStat> {
        vec![]
    }
}

struct FakeX(&'static Manifest, TransformKind, u8);
impl TransformPlugin for FakeX {
    fn manifest(&self) -> &'static Manifest {
        self.0
    }
    fn kind(&self) -> TransformKind {
        self.1
    }
    fn alg_id(&self) -> u8 {
        self.2
    }
    fn encode(&self, _: &TransformCtx, i: Buf, _: &mut BlockMeta, _: &MemPool) -> Result<Buf, ErrCode> {
        Ok(i)
    }
    fn decode(&self, _: &TransformCtx, i: Buf, _: &BlockMeta, _: &MemPool) -> Result<Buf, ErrCode> {
        Ok(i)
    }
}

struct FakeC(&'static Manifest, u8);
impl CodecPlugin for FakeC {
    fn manifest(&self) -> &'static Manifest {
        self.0
    }
    fn codec_id(&self) -> u8 {
        self.1
    }
    fn shards(&self, _: usize) -> Vec<crate::hooks::traits::ShardSpec> {
        vec![]
    }
    fn read_order(&self, _: &[(u32, u32)], _: Option<u32>, _: &crate::base::types::ChunkId) -> Vec<u32> {
        vec![]
    }
}

#[test]
fn data_hooks_one_qos_transform_subranges_and_unique_ids() {
    let qos = |n, alt| Arc::new(FakeQ(man(n, Hook::Qos, alt, |_| {}))) as Arc<dyn QosPlugin>;
    let mut s = PluginSet::new(&[]);
    s.qos(qos("q1", 401));
    s.validate(&no_cfg()).unwrap();
    s.qos(qos("q2", 402));
    assert!(s.validate(&no_cfg()).unwrap_err().contains("只允许一个"), "qos 只能有一个活跃的插件");
    let x = |n, alt, k, id| Arc::new(FakeX(man(n, Hook::Transform, alt, |_| {}), k, id)) as Arc<dyn TransformPlugin>;
    let one = |p: Arc<dyn TransformPlugin>| {
        let mut s = PluginSet::new(&[]);
        s.transform(p);
        s.validate(&no_cfg())
    };
    one(x("ck", 701, TransformKind::Checksum, 1)).unwrap();
    assert!(one(x("ck", 735, TransformKind::Checksum, 1)).unwrap_err().contains("700–729"), "校验类在 700–729");
    assert!(one(x("z", 701, TransformKind::Compress, 2)).unwrap_err().contains("730–759"), "压缩类在 730–759");
    let mut s = PluginSet::new(&[]);
    s.transform(x("a", 701, TransformKind::Checksum, 1));
    s.transform(x("b", 702, TransformKind::Checksum, 1));
    assert!(s.validate(&no_cfg()).unwrap_err().contains("算法编号"), "同一类的算法编号不能重复");
    let c = |n, alt, id| Arc::new(FakeC(man(n, Hook::Codec, alt, |_| {}), id)) as Arc<dyn CodecPlugin>;
    let mut s = PluginSet::new(&[]);
    s.codec(c("r1", 610, 1));
    s.codec(c("r2", 620, 1));
    assert!(s.validate(&no_cfg()).unwrap_err().contains("编号"), "codec 编号不能重复");
}
