//! 集群插件清单（design.md 3.7.2，roadmap M8）：真的 `wmfs server` 子进程（`CARGO_BIN_EXE_wmfs`）。
//! `wmfs cluster plugins add authz:deny-uid` 之后，节点因本地清单与集群清单不一致而退出，退出码 3，日志列出差异与修法；照原样再启动
//! 也是退出码 3（等同拒绝启动）；逃生口 `wmfs server --adopt-local-plugins` 成为就绪 leader，把集群清单改回本地的（changelog
//! `PluginListReset`）；之后不带参数照常启动。端口由 harness 挑空闲的，丢弃时（包括失败 panic 时）结束子进程、删掉目录。

use std::path::Path;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use wmfs_core::api::types::{ChangelogPage, Health, KeyFile, PluginsReply};
use wmfs_core::base::config::NodeConfig;
use wmfs_mgr::api::ApiClient;
use wmfs_testkit::harness::node::{TempDir, init, node_dir};

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
