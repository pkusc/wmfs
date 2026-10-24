//! 集群插件清单（design.md 3.7.2，roadmap M8）：真的 `wmfs server` 子进程（`CARGO_BIN_EXE_wmfs`）。
//! `wmfs cluster plugins add authz:deny-uid` 之后，节点因本地清单与集群清单不一致而退出，退出码 3，日志列出差异与修法；照原样再启动
//! 也是退出码 3（等同拒绝启动）；逃生口 `wmfs server --adopt-local-plugins` 成为就绪 leader，把集群清单改回本地的（changelog
//! `PluginListReset`）；之后不带参数照常启动。端口由 harness 挑空闲的，丢弃时（包括失败 panic 时）结束子进程、删掉目录。

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use wmfs_core::api::types::{ChangelogPage, Health, KeyFile, PluginsReply};
use wmfs_core::base::config::NodeConfig;
use wmfs_mgr::api::ApiClient;
use wmfs_testkit::harness::node::{TempDir, free_port, init, node_dir};

/// 一个 `wmfs server` 子进程（stdout/stderr 追加到 `<节点>/stdout.log`；它自己的日志在 `<节点>/logs/server.log`）：丢弃时结束它。
struct Server(Child);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn server(nd: &Path, extra: &[&str]) -> Server {
    let log = std::fs::OpenOptions::new().create(true).append(true).open(nd.join("stdout.log")).unwrap();
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_wmfs"));
    cmd.arg("server").arg("--data-dir").arg(nd).args(extra).env_remove("WMFS_FAILPOINTS");
    cmd.stdin(Stdio::null()).stdout(log.try_clone().unwrap()).stderr(log);
    Server(cmd.spawn().unwrap())
}

/// 等子进程退出（最多 `secs` 秒），返回退出码（被信号结束时是 None）。
fn exit_code(s: &mut Server, secs: u64) -> Option<i32> {
    let t0 = Instant::now();
    while t0.elapsed() < Duration::from_secs(secs) {
        if let Some(st) = s.0.try_wait().unwrap() {
            return st.code();
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("{secs} s 内没有退出");
}

/// 等它成为就绪的 leader（`/api/v1/health` 的 meta_role 是 leader）。
fn wait_leader(api: &ApiClient, s: &mut Server) {
    let t0 = Instant::now();
    while !api.get::<Health>("/api/v1/health").is_ok_and(|h| h.meta_role == "leader") {
        assert!(s.0.try_wait().unwrap().is_none(), "server 退出了");
        assert!(t0.elapsed() < Duration::from_secs(30), "30 s 内没有成为就绪 leader");
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn wmfs(nd: &Path, mgmt: &str, args: &[&str]) -> Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_wmfs"));
    cmd.args(args).env("WMFS_CLUSTER", mgmt).env("WMFS_KEY_FILE", nd.join("keys/admin.key"));
    cmd.env("no_proxy", "127.0.0.1,localhost").env("NO_PROXY", "127.0.0.1,localhost").output().unwrap()
}

#[test]
fn a_changed_plugin_list_stops_the_node_with_exit_code_3_until_it_adopts_its_local_plugins() {
    let dir = TempDir::new("plugin-list");
    init(&dir.0);
    let nd = node_dir(&dir.0);
    let cfg = NodeConfig::parse(&std::fs::read_to_string(nd.join("node.toml")).unwrap()).unwrap();
    let key = KeyFile::parse(&std::fs::read_to_string(nd.join("keys/admin.key")).unwrap()).unwrap();
    let (mgmt, api) = (cfg.mgmt.listen.clone(), ApiClient::new(vec![cfg.mgmt.listen.clone()], key));
    let log = || std::fs::read_to_string(nd.join("logs/server.log")).unwrap_or_default();

    let mut s = server(&nd, &[]);
    wait_leader(&api, &mut s);
    let local: PluginsReply = api.get("/api/v1/cluster/plugins").unwrap();
    let o = wmfs(&nd, &mgmt, &["cluster", "plugins", "add", "authz:deny-uid", "--json"]);
    let out = String::from_utf8_lossy(&o.stdout);
    assert!(o.status.success() && out.contains("authz:deny-uid"), "{out}{}", String::from_utf8_lossy(&o.stderr));
    assert_eq!(exit_code(&mut s, 10), Some(3), "清单不一致：退出码 3；日志：{}", log());
    assert!(
        log().contains("本节点缺少 authz:deny-uid") && log().contains("--adopt-local-plugins"),
        "日志列出差异与修法：{}",
        log()
    );

    let mut s = server(&nd, &[]);
    assert_eq!(exit_code(&mut s, 30), Some(3), "照原样再启动：拒绝启动");

    let mut s = server(&nd, &["--adopt-local-plugins"]);
    wait_leader(&api, &mut s);
    let t0 = Instant::now();
    while api.get::<PluginsReply>("/api/v1/cluster/plugins").ok() != Some(local.clone()) {
        assert!(t0.elapsed() < Duration::from_secs(10), "逃生口应把集群清单改回本地的 {local:?}");
        std::thread::sleep(Duration::from_millis(50));
    }
    let page: ChangelogPage = api.get("/api/v1/changelog?limit=1000").unwrap();
    let ops: Vec<&str> = page.events.iter().map(|e| e.op.as_str()).collect();
    assert!(ops.contains(&"PluginListChange") && ops.contains(&"PluginListReset"), "{ops:?}");
    let st = Command::new("kill").arg("-TERM").arg(s.0.id().to_string()).status().unwrap();
    assert!(st.success() && exit_code(&mut s, 30) == Some(0), "SIGTERM：正常退出");

    let mut s = server(&nd, &[]);
    wait_leader(&api, &mut s); // 清单又一致了：照常启动
}

// ---- M9 第一步（ADR-0039 第 6 条）：`--adopt-local-plugins` 只管到本地清单第一次与已应用的集群清单一致、或集群清单被改过为止 ----

/// 3 个真进程节点：node1 由 harness 初始化，node2、node3 用 `wmfs join --no-start` 加入，都用 `wmfs server` 跑；等到 3 个 voter。
struct Three {
    _dir: TempDir,
    nodes: Vec<PathBuf>,
    mgmt: String, // 全部节点的管理地址（逗号分隔，ApiClient 与 WMFS_CLUSTER 用）
    api: ApiClient,
}

fn three(name: &str) -> (Three, Vec<Server>) {
    let dir = TempDir::new(name);
    init(&dir.0);
    let n1 = node_dir(&dir.0);
    let cfg = |nd: &Path| NodeConfig::parse(&std::fs::read_to_string(nd.join("node.toml")).unwrap()).unwrap();
    let rpc1 = cfg(&n1).plugin_cfg("tcp").get("listen").and_then(|v| v.as_str()).unwrap().to_string();
    let key = KeyFile::parse(&std::fs::read_to_string(n1.join("keys/admin.key")).unwrap()).unwrap();
    let mut servers = vec![server(&n1, &[])];
    wait_leader(&ApiClient::new(vec![cfg(&n1).mgmt.listen.clone()], key.clone()), &mut servers[0]);
    let mut nodes = vec![n1.clone()];
    for i in 2..=3 {
        let nd = dir.0.join(format!("n{i}"));
        let (rp, mp) = (free_port().to_string(), free_port().to_string());
        let key_file = n1.join("cluster.key");
        let args = ["join", &rpc1, "--cluster-key", key_file.to_str().unwrap(), "--data-dir", nd.to_str().unwrap()];
        let o = Command::new(env!("CARGO_BIN_EXE_wmfs"))
            .args(args)
            .args(["--rpc-port", &rp, "--mgmt-port", &mp, "--no-start"])
            .env_remove("WMFS_FAILPOINTS")
            .output()
            .unwrap();
        assert!(
            o.status.success(),
            "join：{}{}",
            String::from_utf8_lossy(&o.stdout),
            String::from_utf8_lossy(&o.stderr)
        );
        servers.push(server(&nd, &[]));
        nodes.push(nd);
    }
    let mgmt: Vec<String> = nodes.iter().map(|n| cfg(n).mgmt.listen.clone()).collect();
    let api = ApiClient::new(mgmt.clone(), key);
    let t0 = Instant::now();
    while !api
        .get::<wmfs_core::api::types::ClusterReply>("/api/v1/cluster")
        .is_ok_and(|c| c.voters.len() == 3 && c.voters_outgoing.is_empty())
    {
        assert!(t0.elapsed() < Duration::from_secs(60), "60 s 内没有 3 个 voter");
        std::thread::sleep(Duration::from_millis(100));
    }
    (Three { _dir: dir, nodes, mgmt: mgmt.join(","), api }, servers)
}

/// 改集群清单之后 3 个节点都以退出码 3 退出；返回改之前的（本地）清单。
fn add_and_all_exit(t: &Three, servers: &mut [Server]) -> PluginsReply {
    let local: PluginsReply = t.api.get("/api/v1/cluster/plugins").unwrap();
    let o = wmfs(&t.nodes[0], &t.mgmt, &["cluster", "plugins", "add", "authz:deny-uid", "--json"]);
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    for s in servers.iter_mut() {
        assert_eq!(exit_code(s, 30), Some(3), "清单不一致：每个节点都以退出码 3 退出");
    }
    local
}

/// 等集群清单变成 `want`（最多 30 s）。
fn wait_list(t: &Three, want: &PluginsReply) {
    let t0 = Instant::now();
    while t.api.get::<PluginsReply>("/api/v1/cluster/plugins").ok().as_ref() != Some(want) {
        assert!(t0.elapsed() < Duration::from_secs(30), "30 s 内集群清单没有变成 {want:?}");
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn resets(t: &Three) -> usize {
    let page: ChangelogPage = t.api.get("/api/v1/changelog?limit=1000").unwrap();
    page.events.iter().filter(|e| e.op == "PluginListReset").count()
}

#[test]
fn three_nodes_adopting_the_same_plugins_converge_and_keep_running() {
    let (t, mut s) = three("adopt-same");
    let local = add_and_all_exit(&t, &mut s);
    let mut s: Vec<Server> = t.nodes.iter().map(|n| server(n, &["--adopt-local-plugins"])).collect();
    wait_list(&t, &local);
    let t0 = Instant::now();
    while t0.elapsed() < Duration::from_secs(3) {
        assert!(s.iter_mut().all(|x| x.0.try_wait().unwrap().is_none()), "配置相同：收敛之后都照常运行");
        std::thread::sleep(Duration::from_millis(100));
    }
    assert_eq!(resets(&t), 1, "清单只被改回一次");
    // 逃生口用过了：去掉参数重启一个节点，照常运行
    let st = Command::new("kill").arg("-TERM").arg(s[2].0.id().to_string()).status().unwrap();
    assert!(st.success() && exit_code(&mut s[2], 30) == Some(0));
    s[2] = server(&t.nodes[2], &[]);
    let t0 = Instant::now();
    while t0.elapsed() < Duration::from_secs(3) {
        assert!(s[2].0.try_wait().unwrap().is_none(), "不带参数也照常运行");
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[test]
fn a_node_adopting_different_plugins_exits_after_the_list_is_fixed() {
    let (t, mut s) = three("adopt-diff");
    let local = add_and_all_exit(&t, &mut s);
    let mut s1 = server(&t.nodes[0], &["--adopt-local-plugins"]);
    let mut s2 = server(&t.nodes[1], &["--adopt-local-plugins"]);
    wait_list(&t, &local);
    let toml = t.nodes[2].join("node.toml");
    let text = std::fs::read_to_string(&toml).unwrap().replace("disable = []", "disable = [\"gc\"]");
    std::fs::write(&toml, text).unwrap();
    let mut s3 = server(&t.nodes[2], &["--adopt-local-plugins"]);
    assert_eq!(exit_code(&mut s3, 30), Some(3), "配置与改好的清单不同：逃生口已被用过，照常以退出码 3 退出");
    let log = std::fs::read_to_string(t.nodes[2].join("logs/server.log")).unwrap_or_default();
    assert!(log.contains("本节点缺少 bg_task:gc"), "{log}");
    assert!(s1.0.try_wait().unwrap().is_none() && s2.0.try_wait().unwrap().is_none());
    assert_eq!(t.api.get::<PluginsReply>("/api/v1/cluster/plugins").unwrap(), local, "清单没有被改来改去");
    assert_eq!(resets(&t), 1);
}
