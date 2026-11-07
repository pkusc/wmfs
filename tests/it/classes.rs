//! 存储类、QoS 属性、分层、放置（design.md 6.1、6.3、4.5、4.6、4.7、4.12、3.10；roadmap M13 的验收）：完整的单节点（mclock、全部后台任务）与
//! 同一个进程里的 3 节点集群，经管理 API（与 `wmfs class`、`wmfs set` 同一个 `ApiClient`）：
//! - 存储类的增删改查：重名 `EEXIST`；改 `class` 触发迁移；`class update` 只改 qos 时不迁移、改 replica 时触发整树协调（`reconcile`）；
//!   改 label 拒绝；用过的类（有链）、目录属性还提到的类、别的类 `tier_to` 指向的类不能删，没用过的能删；`[lifecycle] tier_to` 的类要存在；
//! - 声明了 dedicated、实际与别的类共用物理盘（同一块盘上的两个 target）→ shared + `isolation_downgraded` 事件；dedicated 类独占它的 target；
//!   会让别的类一个 target 也没有的 dedicated 类（label `*`）建不了，default 照样能建文件（M13 审查）；
//! - `tier` 用短条件（`size>100K&age>1s`）按预期迁移，条件不成立的留着；分层过去之后条件不再成立（截短到 100K 以下），协调器与 tiering
//!   也不把它迁回去；status 的 tier 一行；
//! - `qos` 属性：QOS 表随集群图下发，新文件的 cap 带它的来源目录、数据节点按它排队，`GET /api/v1/qos` 列出这个队列与被上限推迟的次数；
//!   `qos=<类>` 是活的借用（存类名；改了类的 [qos] 表里跟着变、不迁移；被借用的类删不掉，ADR-0044 第 2 条）；unset 之后表里没了；3 节点时
//!   status 的每节点份额按 3 均分（小数不取整）；
//! - `locality=prefer-client`：新文件的链头在客户端所在节点上（会话的 host_id = 节点的 host_id）；读副本的顺序（codec 的 `read_order`）：
//!   同主机的副本优先，没有同主机的就按 chunk 分散到各副本；
//! - rebalance：一条链明显更满（prefer-client 把 1 副本类的文件都放在节点 2）→ `wmfs task run rebalance` 把文件迁到别的链，内容不变、条带数
//!   不变、旧布局记了 GC kind 3；
//! - 金样：`ClassCreate`/`ClassUpdate`/`PolicyViolation`（kind=isolation_downgraded）的 extra 字段名与次序（只增的取值）。

use std::sync::Arc;
use std::time::Duration;

use wmfs_core::api::metrics;
use wmfs_core::api::types::{AttrGetReply, AttrUnsetReply, ClassInfo, ClassReply, ClassesReply, QosReply};
use wmfs_core::buf::{Lane, PoolConfig};
use wmfs_core::client::data_client::DataClient;
use wmfs_core::meta::schema::keys::{self, GC_MOVED, ROOT_INO};
use wmfs_core::meta::schema::records::{InodeV1, QosSpecV1, decode_v1};
use wmfs_core::proto::meta::{MetaOp, O_RDONLY, SetAttrIn};
use wmfs_core::server::Running;
use wmfs_mgr::api::{ApiClient, ApiError};
use wmfs_plugins::replica::Replica;
use wmfs_testkit::harness::bg::{api, api_at, attr, extra_keys, has_event, mkdir, set, status, task, tasks, text};
use wmfs_testkit::harness::cluster::Cluster;
use wmfs_testkit::harness::meta::get_raw;
use wmfs_testkit::harness::node::{
    Client, TempDir, chunk_of, client, connect_as, get, init, init_targets, opened, put, set_full_bg, set_mclock,
    set_settings, start, wait_for, who,
};

fn class_toml(replica: u8, extra: &str) -> String {
    format!("[devices]\nlabel = \"*\"\n[redundancy]\nreplica = {replica}\n{extra}")
}

fn create(a: &ApiClient, name: &str, toml: &str) -> Result<ClassReply, ApiError> {
    a.send_raw("POST", &format!("/api/v1/classes?name={name}"), toml.as_bytes())
}

fn update(a: &ApiClient, name: &str, toml: &str) -> Result<ClassReply, ApiError> {
    a.send_raw("PUT", &format!("/api/v1/classes/{name}"), toml.as_bytes())
}

fn classes(a: &ApiClient) -> Vec<ClassInfo> {
    a.get::<ClassesReply>("/api/v1/classes").unwrap().classes
}

fn inode(run: &Running, ino: u64) -> InodeV1 {
    decode_v1(&get_raw(&run.meta, &keys::inode(ino)).unwrap().unwrap()).unwrap()
}

/// 等协调器（leader 上）在现在之后又做完两轮。
fn two_reconciler_passes(a: &ApiClient) -> bool {
    let pass = || tasks(a).into_iter().find(|t| t.kind == "reconciler").map_or(0, |t| t.pass);
    let p0 = pass();
    wait_for(30, || pass() >= p0 + 2)
}

#[test]
fn class_crud_update_rules_and_migrations() {
    set_full_bg();
    set_mclock();
    let d = TempDir::new("classes-crud");
    init(&d.0);
    let run = start(&d.0);
    set_settings(&run.meta, |s| s.epoch_gc_grace_ms = 0);
    let (a, c, me) = (api(&d.0, &run.mgmt_addr), client(&d.0, &run, "admin"), who(1000, 1000));
    let r = create(&a, "gold", &class_toml(1, "[qos]\niops_max = 300\n")).unwrap();
    let k = &r.class;
    assert_eq!((k.name.as_str(), k.replica, k.nodes, k.per_node.iops_max, r.reconcile), ("gold", 1, 1, 300, false));
    assert_eq!((k.isolation.as_str(), k.downgraded), ("shared", false), "与 default 共用唯一的 target");
    let gold = k.id;
    assert_eq!(extra_keys(&a, "ClassCreate", Some(("name", "gold"))), "name,id");
    assert_eq!(create(&a, "gold", &class_toml(1, "")).unwrap_err().code, "eexist");
    assert!(classes(&a).iter().any(|k| k.name == "gold"));

    // 改 class 触发迁移（内容不变）
    let p = mkdir(&c, ROOT_INO, "p");
    let data = text(1, 300_000);
    let f = put(&c, &me, p, "f", &data).unwrap().attr.ino;
    assert_eq!(inode(&run, f).class_id, 1);
    set(&a, "/p", "class=gold").unwrap();
    assert!(wait_for(60, || inode(&run, f).class_id == gold), "改 class 之后协调器把已有文件迁过去");
    assert_eq!(get(&c, &me, f).unwrap(), data);

    // update 只改 qos：不迁移
    let epoch = inode(&run, f).layout.unwrap().epoch;
    let r = update(&a, "gold", &class_toml(1, "[qos]\niops_max = 600\nweight = 3\n")).unwrap();
    assert!(!r.reconcile && r.class.qos.iops_max == 600 && r.class.qos.weight == 3);
    assert_eq!(extra_keys(&a, "ClassUpdate", Some(("reconcile", "false"))), "name,id,reconcile");
    assert!(two_reconciler_passes(&a));
    assert_eq!(inode(&run, f).layout.unwrap().epoch, epoch, "只改 QoS：没有迁移");

    // update 改 replica：整树协调，没单独设 replicas 的文件按新副本数迁移
    let r = update(&a, "gold", &class_toml(2, "[qos]\niops_max = 600\n")).unwrap();
    assert!(r.reconcile);
    assert!(wait_for(60, || inode(&run, f).layout.unwrap().replicas == 2), "改 replica：协调器从根整树协调");
    assert_eq!(get(&c, &me, f).unwrap(), data);

    // 不能改 label；删除的规则
    assert_eq!(
        update(&a, "gold", "[devices]\nlabel = \"ssd\"\n[redundancy]\nreplica = 2\n").unwrap_err().code,
        "einval"
    );
    assert_eq!(update(&a, "nope", &class_toml(1, "")).unwrap_err().code, "enoent");
    let delete = |name: &str| a.delete::<ClassReply>(&format!("/api/v1/classes/{name}"));
    assert_eq!(delete("gold").unwrap_err().code, "ebusy", "/p 的属性还提到它");
    a.delete::<AttrUnsetReply>("/api/v1/attr?path=/p&key=class").unwrap();
    let e = delete("gold").unwrap_err();
    assert!(e.code == "ebusy" && e.message.contains("数据链"), "没有目录提到它了，可它已经有链（建过文件）：{e:?}");
    assert_eq!(delete("default").unwrap_err().code, "ebusy");
    create(&a, "spare", &class_toml(1, "")).unwrap();
    create(&a, "warm", &class_toml(1, "[lifecycle]\ntier_to = \"spare\"\nwhen = \"idle>30d\"\n")).unwrap();
    let e = delete("spare").unwrap_err();
    assert!(e.code == "ebusy" && e.message.contains("tier_to"), "别的类的 [lifecycle] 指向它：{e:?}");
    assert_eq!(delete("warm").unwrap().class.name, "warm");
    assert_eq!(delete("spare").unwrap().class.name, "spare");
    assert!(!classes(&a).iter().any(|k| k.name == "spare"));
    let life = class_toml(1, "[lifecycle]\ntier_to = \"nope\"\nwhen = \"idle>30d\"\n");
    assert_eq!(create(&a, "warm", &life).unwrap_err().code, "einval", "[lifecycle] tier_to 的类要存在");
}

#[test]
fn a_short_tier_condition_moves_files_and_they_stay_there() {
    set_full_bg();
    set_mclock();
    let d = TempDir::new("classes-tier");
    init(&d.0);
    let run = start(&d.0);
    set_settings(&run.meta, |s| s.epoch_gc_grace_ms = 0);
    let (a, c, me) = (api(&d.0, &run.mgmt_addr), client(&d.0, &run, "admin"), who(1000, 1000));
    let hot = create(&a, "hot", &class_toml(1, "")).unwrap().class.id;
    let cold = create(&a, "cold", &class_toml(1, "")).unwrap().class.id;
    let t = mkdir(&c, ROOT_INO, "t");
    set(&a, "/t", "class=hot").unwrap();
    let big = put(&c, &me, t, "big", &text(2, 300_000)).unwrap().attr.ino;
    let small = put(&c, &me, t, "small", &text(3, 70_000)).unwrap().attr.ino;
    assert_eq!((inode(&run, big).class_id, inode(&run, small).class_id), (hot, hot));
    let r = set(&a, "/t", "tier=hot->cold:size>100K&age>1s").unwrap();
    assert_eq!(r.set["tier"], "hot->cold:size>100K&age>1s");
    let pending = || attr(&status(&a, "/t"), "tier").actual.mismatched_files;
    assert!(wait_for(30, || pending() == Some(1)), "条件成立（大于 100K、建了 1 s 以上）、还没迁的：1 个");
    let moved = wait_for(60, || {
        task(&a, "tiering", "run", None);
        wait_for(3, || inode(&run, big).class_id == cold)
    });
    assert!(moved, "tiering 把大文件迁到 cold");
    assert_eq!(inode(&run, small).class_id, hot, "条件不成立的留着");
    assert_eq!(get(&c, &me, big).unwrap(), text(2, 300_000));
    assert_eq!(pending(), Some(0));
    assert_eq!(attr(&status(&a, "/t"), "class").actual.mismatched_files, Some(0), "声明就是分层之后的类：不算不一致");
    // 分层过去之后条件不再成立（截短到 100K 以下）：已经在 cold 里的文件留在 cold（3.10 的 declared），协调器、tiering 都不把它迁回 hot
    let epoch_seen = inode(&run, big).layout.unwrap().epoch;
    let set_size =
        SetAttrIn { mode: None, uid: None, gid: None, size: Some(50_000), atime: None, mtime: None, epoch_seen };
    c.meta.write(&me, MetaOp::SetAttr { ino: big, set: set_size }).unwrap();
    set(&a, "/t", "compress=none").unwrap();
    assert!(two_reconciler_passes(&a));
    task(&a, "tiering", "run", None);
    assert!(two_reconciler_passes(&a));
    assert_eq!(inode(&run, big).class_id, cold, "分层之后（条件已不成立）不会被协调器迁回去");
    let st = status(&a, "/t");
    assert_eq!(
        (attr(&st, "class").actual.mismatched_files, attr(&st, "tier").actual.mismatched_files),
        (Some(0), Some(0))
    );
}

#[test]
fn a_dedicated_class_on_a_shared_disk_is_downgraded_honestly() {
    set_full_bg();
    set_mclock();
    let d = TempDir::new("classes-dedicated");
    init_targets(&d.0, &[&["fast"], &[]]); // 两个 target 在同一块盘（同一个目录树）上
    let run = start(&d.0);
    let (a, c, me) = (api(&d.0, &run.mgmt_addr), client(&d.0, &run, "admin"), who(1000, 1000));
    let r = create(&a, "fast", "[devices]\nlabel = \"fast\"\ndedicated = true\n[redundancy]\nreplica = 1\n").unwrap();
    assert_eq!(
        (r.class.isolation.as_str(), r.class.downgraded),
        ("shared", true),
        "与 default 的 target 共用物理盘：shared"
    );
    let kv = Some(("kind", "isolation_downgraded"));
    assert!(has_event(&a, "PolicyViolation", kv));
    assert_eq!(extra_keys(&a, "PolicyViolation", kv), "kind,class,isolation");
    // fast 独占 target 1：default 类的新文件只落在 target 2 上
    let f = put(&c, &me, ROOT_INO, "x", &text(4, 300_000)).unwrap();
    let map = c.meta.map();
    for ch in f.layout.unwrap().chains {
        let (_, rec) = map.chains.iter().find(|(id, _)| *id == ch).unwrap();
        assert!(rec.members.iter().all(|(t, _)| *t == 2), "default 的链不用 fast 独占的 target 1：{rec:?}");
    }
    let g: AttrGetReply = a.get("/api/v1/attr?path=/").unwrap();
    assert!(
        g.semantics.contains("isolation=shared(只承诺份额,不承诺尾延迟)"),
        "mclock：按实际部署算的承诺：{}",
        g.semantics
    );
    // label * 的 dedicated 类会占走全部 target（default 与 fast 就一个可用的 target 也没有了）：建不了；default 照样能建文件
    let all = "[devices]\nlabel = \"*\"\ndedicated = true\n[redundancy]\nreplica = 1\n";
    assert_eq!(create(&a, "all", all).unwrap_err().code, "einval");
    assert!(put(&c, &me, ROOT_INO, "y", b"still writable").is_ok());
}

#[test]
fn qos_attr_reaches_the_data_node() {
    set_full_bg();
    set_mclock();
    let d = TempDir::new("classes-qos");
    init(&d.0);
    let run = start(&d.0);
    let (a, c, me) = (api(&d.0, &run.mgmt_addr), client(&d.0, &run, "admin"), who(1000, 1000));
    let q = mkdir(&c, ROOT_INO, "q");
    let r = set(&a, "/q", "qos=r=0,w=2,l=3000").unwrap();
    assert_eq!(r.set["qos"], "r=0,w=2,l=3000");
    let want = QosSpecV1 { r: 0, w: 2, l: 3000, bw_max: 0, burst_ms: 0 };
    let in_map = |ok: &dyn Fn(Option<&QosSpecV1>) -> bool| {
        wait_for(10, || {
            let _ = c.meta.refresh_map();
            ok(c.meta.map().qos.iter().find(|(d, _)| *d == q).map(|(_, s)| s))
        })
    };
    assert!(in_map(&|s| s.is_some_and(|s| format!("{s:?}") == format!("{want:?}"))), "QOS 表随集群图下发");
    put(&c, &me, q, "f", &text(5, 300_000)).unwrap();
    let qid = q.to_string();
    let served =
        metrics::counter("wmfs_qos_served_total", "", &[("tenant", "0"), ("qos_id", &qid), ("phase", "weight")]);
    assert!(served.get() > 0, "新文件的 cap 带着 qos 的来源目录：数据节点按 (租户, /q, 类) 的队列派发");
    let row = attr(&status(&a, "/q"), "qos");
    assert_eq!(row.declared, "r=0,w=2,l=3000");
    assert!(row.actual.new_files.starts_with("每节点 r=0,w=2,l=3000（1 个节点均分）"), "{}", row.actual.new_files);
    assert!(row.actual.new_files.contains("isolation=shared"), "没声明 dedicated 的类只承诺份额");
    assert_eq!((row.actual.deficit, row.mismatched), (Some(0), false), "没有保底：没有欠额");
    // 上限很小的目录：连着写几个文件，后面的被上限推迟；本节点的 GET /api/v1/qos 列出这个队列
    let lim = mkdir(&c, ROOT_INO, "lim");
    set(&a, "/lim", "qos=r=0,w=1,l=6").unwrap();
    let qos = || a.get::<QosReply>("/api/v1/qos").unwrap();
    let limited =
        |r: &QosReply| r.queues.iter().any(|x| x.qos_id == Some(lim) && x.tenant == Some(0) && x.limited_total > 0);
    let mut i = 0;
    let ok = wait_for(30, || {
        i += 1; // 数据节点按心跳拉到新的集群图（QOS 表）之后，连着写的第二个起就要等上限（每个写的成本 3：每 0.5 s 一个）
        put(&c, &me, lim, &format!("f{i}"), &text(5, 300_000)).unwrap();
        limited(&qos())
    });
    assert!(ok, "GET /api/v1/qos 列出这个目录的队列、被上限推迟过：{:?}", qos());
    assert_eq!(qos().plugin, "mclock");
    // qos=<类> 是活的借用（ADR-0044 第 2 条）：存类名、QOS 表里放类此刻的包络；改了类的 [qos] → 跟着变、不迁移；被借用的类删不掉
    create(&a, "gold", &class_toml(1, "[qos]\niops_max = 300\n")).unwrap();
    assert_eq!(set(&a, "/q", "qos=gold").unwrap().set["qos"], "gold", "存类名");
    let l_is = |l: u32| move |s: Option<&QosSpecV1>| s.is_some_and(|s| (s.r, s.w, s.l) == (0, 1, l));
    assert!(in_map(&l_is(300)), "QOS 表里是 gold 此刻的包络");
    assert!(!update(&a, "gold", &class_toml(1, "[qos]\niops_max = 900\n")).unwrap().reconcile, "只改 qos：不迁移");
    assert!(in_map(&l_is(900)), "改了 gold 的 [qos]：借用它的目录的 QOS 记录跟着变");
    let row = attr(&status(&a, "/q"), "qos");
    assert!(
        row.declared == "gold" && row.actual.new_files.starts_with("每节点 r=0,w=1,l=900（1 个节点均分）"),
        "{row:?}"
    );
    let e = a.delete::<ClassReply>("/api/v1/classes/gold").unwrap_err();
    assert!(e.code == "ebusy" && e.message.contains("/q"), "被 qos= 借用的类不能删：{e:?}");
    a.delete::<AttrUnsetReply>("/api/v1/attr?path=/q&key=qos").unwrap();
    assert!(in_map(&|s| s.is_none()), "unset 之后 QOS 表里没了");
}

/// 读 `ino` 的每个 chunk（整块），用 `dc`（它的 codec 与"主机"决定读哪个副本）。
fn read_chunks(c: &Client, dc: &DataClient, ino: u64) {
    let o = opened(c.meta.read(&who(1000, 1000), MetaOp::Open { ino, flags: O_RDONLY })).unwrap();
    let l = o.layout.as_ref().unwrap();
    for idx in 0..o.attr.size.div_ceil(l.chunk_size as u64) as u32 {
        let into = c.rpc.pool().get(l.chunk_size as usize, Lane::Normal, Duration::from_secs(5)).unwrap();
        dc.read(&o.cap, l, chunk_of(&o, idx), 0, l.chunk_size, into, None).unwrap();
    }
}

/// 节点 1–3 上的 target 各读出了多少字节（指标是进程级的：同一个测试程序里别的用例的读也算在里面，只看增量的下限）。
fn read_bytes(c: &Client) -> Vec<u64> {
    let map = c.meta.map();
    let target_of = |n: u32| map.targets.iter().find(|(_, t)| t.node == n).map(|(id, _)| *id).unwrap();
    let get = |t: u32| metrics::counter("wmfs_chunk_read_bytes_total", "", &[("target", &t.to_string())]).get();
    (1..=3).map(|n| get(target_of(n))).collect()
}

#[test]
fn placement_reads_and_rebalance_on_three_nodes() {
    set_full_bg();
    set_mclock(); // 同一个测试程序里的用例都用同一组插件（这两个开关是进程级的：init 与 start 之间不能变）
    let cl = Cluster::start_sized("classes-locality", 3, "256M"); // rebalance 的使用率按 target 的容量算
    cl.wait_leader();
    let nd = cl.node_dir(1);
    let mgmt: Vec<String> = (1..=3).map(|i| cl.run(i).mgmt_addr.clone()).collect();
    let a = api_at(&nd, &mgmt.join(","), "admin");
    let pool = PoolConfig { s_mb: 8, m_slots: 6, l_slots: 1, repl_lane: false };
    let c = connect_as(&nd, &cl.seeds(), "admin", pool, "host-2");
    c.meta.start_heartbeat();
    let me = who(1000, 1000);
    let (l, any) = (mkdir(&c, ROOT_INO, "l"), mkdir(&c, ROOT_INO, "any"));
    set(&a, "/l", "locality=prefer-client").unwrap();
    let head_node = |parent: u64, name: &str| {
        let f = put(&c, &me, parent, name, &text(6, 100_000)).unwrap();
        let _ = c.meta.refresh_map();
        let map = c.meta.map();
        let ch = f.layout.unwrap().chains[0];
        let (_, rec) = map.chains.iter().find(|(id, _)| *id == ch).unwrap();
        let t = rec.members[0].0;
        map.targets.iter().find(|(id, _)| *id == t).unwrap().1.node
    };
    for i in 0..6 {
        assert_eq!(head_node(l, &format!("f{i}")), 2, "prefer-client：链头在客户端所在的节点（host-2）上");
    }
    let spread: std::collections::BTreeSet<u32> = (0..12).map(|i| head_node(any, &format!("g{i}"))).collect();
    assert!(spread.len() > 1, "locality=any：链头分散在各节点：{spread:?}");

    // 每节点份额按这个类现在的 3 个节点均分，与数据节点执行的是同一份（小数不取整：l=2 → 0.7，不是 0 = 不限）
    mkdir(&c, ROOT_INO, "q3");
    for (spec, want) in [
        ("qos=r=0,w=1,l=300", "每节点 r=0,w=1,l=100（3 个节点均分）"),
        ("qos=r=0,w=1,l=2", "每节点 r=0,w=1,l=0.7（3 个节点均分）"),
    ] {
        set(&a, "/q3", spec).unwrap();
        let row = attr(&status(&a, "/q3"), "qos");
        assert!(row.actual.new_files.starts_with(want), "{spec}：{}", row.actual.new_files);
    }

    // 读副本的顺序（codec 的 read_order）：同主机的副本优先；没有同主机的 → 按 chunk 分散到各副本（不再总是读链头）
    let big = put(&c, &me, any, "big", &text(7, 12 << 20)).unwrap().attr.ino; // 3 个 chunk、3 副本
    let dc = |host: &str| {
        let d = DataClient::for_meta(c.rpc.clone(), c.meta.clone(), Duration::from_secs(10), Duration::from_secs(30));
        d.with_codec(Arc::new(Replica::new())).with_host(host.into())
    };
    let before = read_bytes(&c);
    read_chunks(&c, &dc("host-2"), big);
    let after = read_bytes(&c);
    assert!(after[1] - before[1] >= 12 << 20, "同主机（节点 2）的副本优先：{before:?} → {after:?}");
    let before = read_bytes(&c);
    read_chunks(&c, &dc("host-9"), big);
    let after = read_bytes(&c);
    let spread = (0..3).filter(|i| after[*i] - before[*i] >= 4 << 20).count();
    assert!(spread >= 2, "没有同主机的副本：3 个 chunk 分散到至少两个副本：{before:?} → {after:?}");

    // rebalance：1 副本的类有 3 条链（各一个 target）；prefer-client 把 /rb 的文件都放在节点 2 那条链上，它的使用率比别的高出 20% 以上
    create(&a, "r1", &class_toml(1, "")).unwrap();
    let rb = mkdir(&c, ROOT_INO, "rb");
    set(&a, "/rb", "class=r1,locality=prefer-client").unwrap();
    let files: Vec<(u64, Vec<u8>)> = (0..8)
        .map(|i| {
            let data = text(20 + i, 8 << 20);
            (put(&c, &me, rb, &format!("f{i}"), &data).unwrap().attr.ino, data)
        })
        .collect();
    let leader = cl.run(cl.wait_leader());
    let layout =
        |ino: u64| -> InodeV1 { decode_v1(&get_raw(&leader.meta, &keys::inode(ino)).unwrap().unwrap()).unwrap() };
    let full_chain = layout(files[0].0).layout.unwrap().chains[0];
    let first: Vec<(u64, u32, u32)> = files
        .iter()
        .map(|(ino, _)| (*ino, layout(*ino).layout.unwrap().chains[0], layout(*ino).layout.unwrap().epoch))
        .collect();
    assert!(first.iter().all(|(_, ch, _)| *ch == full_chain), "都在节点 2 那条链上：{first:?}");
    let moved = || first.iter().filter(|(ino, ..)| layout(*ino).layout.unwrap().chains != vec![full_chain]).count();
    let ok = wait_for(90, || {
        task(&a, "rebalance", "run", None); // 用量要等心跳报上来：按条件反复跑
        wait_for(5, || moved() > 0)
    });
    assert!(ok, "rebalance 把文件从最满的链上迁走");
    for (ino, ch, epoch) in &first {
        let i = layout(*ino);
        let l = i.layout.unwrap();
        assert_eq!(l.chains.len(), 1, "条带数不变");
        if l.chains[0] != *ch {
            assert!(
                get_raw(&leader.meta, &keys::gc(GC_MOVED, *ino, *epoch)).unwrap().is_some(),
                "旧布局记了 GC kind 3"
            );
        }
    }
    for (ino, data) in &files {
        assert!(get(&c, &me, *ino).unwrap() == *data, "内容不变");
    }
}
