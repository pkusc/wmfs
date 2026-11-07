//! 存储类的单元测试（design.md 6.1、6.3；roadmap M13 的验收"物理盘解析单测"）：TOML 的解析、缺省值与校验；update 的规则；QoS 包络；
//! 隔离承诺——同一块 NVMe 的两个分区、同一个 PV 上的两个 LV 都判为共享，loop（解析不出物理盘）判为 unknown、按共享对待（用构造的假
//! `/sys` 目录走一遍 `base::devinfo::resolve`，再按解析出的 `phys_devs` 算）。

use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};

use super::*;
use crate::base::devinfo::resolve;
use crate::meta::schema::records::{ChainRecV1, TargetRecV1};

const GOLD: &str = "name = \"gold\"\n[devices]\nlabel = \"*\"\n[redundancy]\nreplica = 2\n[qos]\niops_max = 300\n";

fn class(id: u16, toml: &str) -> StorageClassV1 {
    StorageClassV1 { id, ..parse(toml, None).unwrap() }
}

fn dedicated(id: u16, name: &str, label: &str) -> StorageClassV1 {
    let t = format!("[devices]\nlabel = \"{label}\"\ndedicated = true\n[redundancy]\nreplica = 1\n");
    StorageClassV1 { id, ..parse(&t, Some(name)).unwrap() }
}

fn target(node: u32, label: &str, phys_devs: Vec<String>) -> TargetRecV1 {
    TargetRecV1 {
        node,
        path: String::new(),
        labels: vec![label.into()],
        phys_devs,
        capacity: 0,
        boot_id: 0,
        state: 1,
        last_up_ms: 0,
        read_only: false,
    }
}

fn map(targets: Vec<TargetRecV1>, classes: Vec<StorageClassV1>) -> ClusterMap {
    let targets = targets.into_iter().enumerate().map(|(i, t)| (i as u32 + 1, t)).collect();
    ClusterMap { targets, classes, ..Default::default() }
}

#[test]
fn the_demo_class_parses_with_minimal_defaults() {
    let k = parse(GOLD, Some("gold")).unwrap();
    assert_eq!((k.name.as_str(), k.label.as_str(), k.replica, k.iops_max, k.weight), ("gold", "*", 2, 300, 1));
    assert_eq!((k.priority, k.failure_domain, k.locality, k.dedicated), (PRIORITY_NORMAL, DOMAIN_NODE, 0, false));
    assert_eq!(parse(GOLD, None).unwrap().name, "gold", "没给名字就用文件里的");
    let full = "[devices]\nlabel = \"hdd\"\n[redundancy]\nreplica = 3\n[qos]\nbw_min = \"1M\"\nbw_max = \"4G\"\nburst = \"30s\"\n\
                latency_target = \"500us\"\n[placement]\nlocality = \"prefer-client\"\n[lifecycle]\ntier_to = \"cold\"\nwhen = \"idle>30d\"\n";
    let k = parse(full, Some("warm")).unwrap();
    assert_eq!(k.priority, PRIORITY_LOW, "label 为 hdd 的类缺省 low（4.5、6.3）");
    assert_eq!((k.bw_min, k.bw_max, k.burst_ms, k.latency_target_us), (1 << 20, 4 << 30, 30_000, 500));
    assert_eq!((k.locality, k.tier_to.as_deref(), k.tier_when.as_deref()), (1, Some("cold"), Some("idle>30d")));
    assert_eq!(envelope(&k).r, 8, "bw_min 1M 按每 128 KiB 一个成本单位折算进 r");
    for (bad, code) in [
        ("[redundancy]\nreplica = 2\nec = \"8+3\"\n", ErrCode::FEATURE_UNSUPPORTED),
        ("[devices]\nlabel = \"*\"\n", ErrCode::EINVAL), // 没写 replica：不猜
        ("[redundancy]\nreplica = 9\n", ErrCode::EINVAL),
        ("[redundancy]\nreplica = 1\n[qos]\npriority = \"high\"\n", ErrCode::EINVAL),
        ("[redundancy]\nreplica = 1\nwhat = 1\n", ErrCode::EINVAL), // 不认识的键
        ("[redundancy]\nreplica = 1\n[qos]\niops_min = 10\niops_max = 5\n", ErrCode::EINVAL),
        ("[redundancy]\nreplica = 1\n[qos]\niops_max = 100\nbw_min = \"1G\"\n", ErrCode::EINVAL), // 折算后 r = 8192 > l
        ("[redundancy]\nreplica = 1\n[lifecycle]\ntier_to = \"x\"\nwhen = \"hot\"\n", ErrCode::EINVAL),
        ("name = \"other\"\n[redundancy]\nreplica = 1\n", ErrCode::EINVAL), // 与命令行的名字不同
    ] {
        assert_eq!(parse(bad, Some("c1")).map(|_| ()).unwrap_err().code, code, "{bad}");
    }
}

#[test]
fn update_rules() {
    let old = parse(GOLD, None).unwrap();
    let qos = StorageClassV1 { iops_max: 3000, weight: 5, priority: PRIORITY_LOW, ..old.clone() };
    assert!(!changes(&old, &qos).unwrap(), "只改 QoS（含 priority）：不迁移");
    assert!(changes(&old, &StorageClassV1 { replica: 3, ..old.clone() }).unwrap(), "改 replica：整树协调");
    let life = StorageClassV1 { tier_to: Some("x".into()), tier_when: Some("age>1d".into()), ..old.clone() };
    assert!(changes(&old, &life).unwrap(), "改 [lifecycle]：整树协调");
    for bad in [
        StorageClassV1 { label: "nvme".into(), ..old.clone() },
        StorageClassV1 { dedicated: true, ..old.clone() },
        StorageClassV1 { failure_domain: 2, ..old.clone() },
    ] {
        assert_eq!(changes(&old, &bad).unwrap_err().code, ErrCode::EINVAL);
    }
}

/// 一个假的 sysfs：nvme0n1 的两个分区 p1、p2；nvme1n1 的分区 p1 上做了一个 PV，上面两个 LV（dm-0、dm-1）；nvme2n1 整盘；loop0。
fn fake_sys() -> PathBuf {
    let r = crate::base::test_dir("classes-sys");
    let mk = |p: &str| std::fs::create_dir_all(r.join(p)).unwrap();
    let part = |p: &str| {
        mk(p);
        std::fs::write(r.join(p).join("partition"), "1").unwrap();
    };
    part("devices/pci/nvme0n1/nvme0n1p1");
    part("devices/pci/nvme0n1/nvme0n1p2");
    part("devices/pci/nvme1n1/nvme1n1p1");
    mk("devices/pci/nvme2n1");
    for lv in ["dm-0", "dm-1"] {
        mk(&format!("devices/virtual/{lv}/slaves"));
        symlink("../../../pci/nvme1n1/nvme1n1p1", r.join(format!("devices/virtual/{lv}/slaves/nvme1n1p1"))).unwrap();
    }
    mk("devices/virtual/loop0");
    mk("dev/block");
    for (num, to) in [
        ("259:1", "devices/pci/nvme0n1/nvme0n1p1"),
        ("259:2", "devices/pci/nvme0n1/nvme0n1p2"),
        ("253:0", "devices/virtual/dm-0"),
        ("253:1", "devices/virtual/dm-1"),
        ("259:9", "devices/pci/nvme2n1"),
        ("7:0", "devices/virtual/loop0"),
    ] {
        symlink(format!("../../{to}"), r.join("dev/block").join(num)).unwrap();
    }
    r
}

/// 两个 dedicated 类 a、b 各一个 target，它们的目录分别在设备号 `da`、`db` 上：两个类的隔离承诺。
fn two_classes(sys: &Path, da: (u32, u32), db: (u32, u32)) -> (Isolation, Isolation) {
    let m = map(
        vec![target(1, "a", resolve(sys, da.0, da.1)), target(2, "b", resolve(sys, db.0, db.1))],
        vec![dedicated(2, "a", "a"), dedicated(3, "b", "b")],
    );
    (isolation(&m, &m.classes[0]), isolation(&m, &m.classes[1]))
}

#[test]
fn isolation_follows_physical_disks_not_partitions_or_lvs() {
    let sys = fake_sys();
    let (shared, ded) = (Isolation::Shared, Isolation::Dedicated);
    assert_eq!(two_classes(&sys, (259, 1), (259, 2)), (shared, shared), "同一块 NVMe 的两个分区：共享");
    assert_eq!(two_classes(&sys, (253, 0), (253, 1)), (shared, shared), "同一个 PV 上的两个 LV：共享");
    assert_eq!(two_classes(&sys, (259, 1), (259, 9)), (ded, ded), "两块不同的盘：各自独占");
    assert!(resolve(&sys, 7, 0).is_empty(), "loop：unknown");
    assert_eq!(two_classes(&sys, (7, 0), (259, 9)), (shared, ded), "unknown 一律按共享对待");
    let low = StorageClassV1 { priority: PRIORITY_LOW, ..dedicated(4, "c", "c") };
    let m = map(vec![target(1, "c", resolve(&sys, 259, 9))], vec![low]);
    assert_eq!(isolation(&m, &m.classes[0]), Isolation::Low, "priority = low：只承诺低优先级");
    assert!(!downgraded(&m, &m.classes[0]), "priority = low 的 dedicated 类真的独占了盘：不算降级");
    let none = map(vec![], vec![dedicated(2, "a", "a")]);
    assert_eq!(isolation(&none, &none.classes[0]), shared, "一个 target 也没有：承诺不了独占");
    std::fs::remove_dir_all(&sys).unwrap();
}

#[test]
fn a_dedicated_class_takes_its_targets_away_from_the_others() {
    let disk = |d: &str| vec![d.to_string()];
    let m = map(
        vec![target(1, "fast", disk("nvme0n1")), target(2, "x", disk("nvme1n1")), target(3, "x", disk("nvme1n1"))],
        vec![class(1, "name = \"default\"\n[redundancy]\nreplica = 1\n"), dedicated(2, "fast", "fast")],
    );
    assert_eq!(isolation(&m, &m.classes[1]), Isolation::Dedicated, "fast 独占 nvme0n1：default 用不了它");
    assert_eq!(
        isolation(&m, &m.classes[0]),
        Isolation::Shared,
        "default 没声明 dedicated：物理上碰巧不共用也不承诺尾延迟"
    );
    assert_eq!(nodes(&m, &m.classes[0]), 2, "default 能用的 target 在节点 2、3 上");
    let shared = map(vec![target(1, "fast", disk("nvme0n1")), target(2, "x", disk("nvme0n1"))], m.classes.clone());
    assert_eq!(isolation(&shared, &shared.classes[1]), Isolation::Shared, "另一个类的 target 与它在同一块盘上");
}

/// 审查后：default（label `*`）的链先占着 target 1、放了数据，之后建的 dedicated 类拿走 target 1——旧链与旧数据还在那块盘上：shared、降级。
#[test]
fn old_chains_of_other_classes_still_share_the_disk() {
    let disk = |d: &str| vec![d.to_string()];
    let mut m = map(
        vec![target(1, "fast", disk("nvme0n1")), target(2, "x", disk("nvme1n1"))],
        vec![class(1, "name = \"default\"\n[redundancy]\nreplica = 1\n"), dedicated(2, "fast", "fast")],
    );
    assert_eq!(isolation(&m, &m.classes[1]), Isolation::Dedicated, "default 还没有链：fast 独占 nvme0n1");
    let chain = ChainRecV1 {
        ver: 1,
        incarnation: 1,
        class_id: 1,
        replicas: 1,
        members: vec![(1, 1)],
        last_serving: None,
        removed: vec![],
    };
    m.chains.push((1, chain));
    assert_eq!(isolation(&m, &m.classes[1]), Isolation::Shared, "default 的旧链还在 target 1 上");
    assert!(downgraded(&m, &m.classes[1]));
    let slow = class(3, "name = \"slow\"\n[devices]\nlabel = \"slow\"\n[redundancy]\nreplica = 1\n");
    let ro = TargetRecV1 { read_only: true, ..target(2, "slow", disk("nvme0n1")) };
    let m2 = map(vec![target(1, "fast", disk("nvme0n1")), ro], vec![dedicated(2, "fast", "fast"), slow]);
    assert!(downgraded(&m2, &m2.classes[0]), "只读的 target 照样服务读：slow 的它与 fast 在同一块盘上，也算共用");
}

/// 审查后：dedicated 类占走 target 之后别的类一个可用的 target 也没有（例如 label `*`）→ 新建时拒绝；只占走一部分的可以建。
#[test]
fn a_dedicated_class_may_not_starve_the_others() {
    let disk = |d: &str| vec![d.to_string()];
    let m = map(
        vec![target(1, "fast", disk("a")), target(2, "x", disk("b"))],
        vec![class(1, "name = \"default\"\n[redundancy]\nreplica = 1\n")],
    );
    assert_eq!(
        starved(&m, &dedicated(2, "all", "*")),
        Some("default".into()),
        "label * 的 dedicated 类占走全部 target"
    );
    assert_eq!(starved(&m, &dedicated(2, "fast", "fast")), None, "default 还有 target 2");
    assert_eq!(starved(&m, &dedicated(2, "none", "nothing")), None, "一个 target 也没占走");
    let plain = class(2, "name = \"all\"\n[devices]\nlabel = \"*\"\n[redundancy]\nreplica = 1\n");
    assert_eq!(starved(&m, &plain), None, "不独占的类不占走别人的 target");
}

#[test]
fn per_node_shares_are_split_without_rounding() {
    let disk = |d: &str| vec![d.to_string()];
    let mut m = map(
        (1..=3).map(|n| target(n, "x", disk("d"))).collect(),
        vec![class(1, "name = \"default\"\n[redundancy]\nreplica = 1\n[qos]\niops_max = 300\nbw_max = \"3M\"\n")],
    );
    let p = per_node(&m, &m.classes[0], 0);
    assert_eq!((p.r, p.l, p.bw_max, p.w, p.nodes), (0.0, 100.0, 1048576.0, 1, 3), "类的包络按 3 个节点均分");
    m.qos.push((77, QosSpecV1 { r: 1, w: 2, l: 2, bw_max: 0, burst_ms: 0 }));
    let p = per_node(&m, &m.classes[0], 77);
    assert!((p.l - 2.0 / 3.0).abs() < 1e-9 && p.w == 2, "目录的 qos：l = 2 / 3，不取整成 0（0 是不限）");
    assert_eq!(per_node(&m, &m.classes[0], 78).l, 100.0, "QOS 表里没有的 qos_id：用类的包络");
    assert_eq!((cost(4096), cost(1 << 20), cost(4 << 20)), (1, 9, 33));
}

/// ADR-0044 第 7 条：label 是 hdd 的类用的 target 在 nvme 盘上（按设备名）→ 标注"label hdd 位于 nvme 设备上（测试用途）"。
#[test]
fn an_hdd_label_on_an_nvme_disk_is_marked() {
    let hdd = class(2, "name = \"bulk\"\n[devices]\nlabel = \"hdd\"\n[redundancy]\nreplica = 1\n");
    let on = |devs: &[&str], label: &str| {
        let m = map(vec![target(1, label, devs.iter().map(|d| d.to_string()).collect())], vec![hdd.clone()]);
        hdd_on_nvme(&m, &m.classes[0])
    };
    assert!(on(&["nvme0n1"], "hdd"), "打了 hdd 标签的测试 target 在 NVMe 上");
    assert!(on(&["sda", "nvme1n1"], "hdd"), "有一块是 nvme 就标");
    assert!(!on(&["sda"], "hdd") && !on(&[], "hdd"), "真的 HDD、unknown：不标");
    assert!(!on(&["nvme0n1"], "ssd"), "这个类没有用到那个 target");
    let ssd = class(3, "name = \"fast\"\n[devices]\nlabel = \"nvme\"\n[redundancy]\nreplica = 1\n");
    let m = map(vec![target(1, "nvme", vec!["nvme0n1".into()])], vec![ssd]);
    assert!(!hdd_on_nvme(&m, &m.classes[0]), "label 不是 hdd：不标");
    assert_eq!(HDD_ON_NVME, "label hdd 位于 nvme 设备上（测试用途）");
}
