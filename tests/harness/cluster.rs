//! 同一个进程里的多节点集群（design.md 9.5 的多节点版，M8）：node1 照常 init，其余节点用 `wmfs join` 的本地侧加入（种子 = node1
//! 的 rpc 地址），每个节点一个 `server::start`（插件与 `wmfs server` 默认装的一样），端口由测试挑空闲的。可以停掉（`Running::stop`，
//! SIGTERM 的路径：leader 先让出）；同一个进程里停掉的节点不能再起来（它的元数据库还被别的线程开着），重启与追平在多进程的
//! `scripts/accept.d/m8.sh` 里测。客户端（MetaClient）拿到全部节点的地址，自己跟随 leader。
//! 注意：日志、指标、fail point 都是进程级的——日志写进最后起的那个节点的 server.log，指标是全部节点加在一起的。

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use wmfs_core::base::config::{NodeConfig, TargetConfig};
use wmfs_core::base::log;
use wmfs_core::buf::PoolConfig;
use wmfs_core::init::{InitOpts, JoinError, JoinOpts, Profile, init_node, join_node, read_master};
use wmfs_core::meta::engine::txn::MetaEngine;
use wmfs_core::net::client::Endpoint;
use wmfs_core::server::{self, Running};
use wmfs_plugins::tcp;

use super::node::{Client, TempDir, connect_seeds, free_port, plugins};

/// 测试节点的内存池（小一些：一个进程里起好几个节点）。
const MEM: PoolConfig = PoolConfig { s_mb: 16, m_slots: 12, l_slots: 2, repl_lane: true };

pub struct Cluster {
    pub dir: TempDir,
    pub nodes: Vec<Option<Running>>, // 下标 0 = node1
}

impl Cluster {
    /// 起 `n` 个节点，等到 min(n, 3) 个元数据 voter。
    pub fn start(name: &str, n: usize) -> Cluster {
        let dir = TempDir::new(&format!("cluster-{name}"));
        let _ = std::fs::remove_dir_all(&dir.0);
        let mut c = Cluster { dir, nodes: Vec::new() };
        c.init_first();
        for i in 2..=n {
            c.join(i, &[]).unwrap();
        }
        c.wait_voters(n.min(3));
        c
    }

    pub fn node_dir(&self, i: usize) -> PathBuf {
        self.dir.0.join(format!("n{i}"))
    }

    fn target(&self, i: usize) -> Vec<TargetConfig> {
        let path = self.node_dir(i).join("targets/1").display().to_string();
        vec![TargetConfig { path, labels: vec![], capacity: "0".into() }]
    }

    fn init_first(&mut self) {
        let o = InitOpts {
            data_dir: self.node_dir(1),
            addr: "127.0.0.1".into(),
            rpc_port: free_port(),
            mgmt_port: free_port(),
            mem: MEM,
            failpoints_api: true,
            debug_ops: true,
            host_id: Some("host-1".into()),
            plugins: plugins(&NodeConfig::default()).plugin_list(),
            targets: self.target(1),
            profile: Profile::Default,
        };
        init_node(&o).unwrap();
        self.nodes.push(Some(boot(&self.node_dir(1))));
    }

    /// 第 i 个节点加入（`disable`：它停用的插件——不同的插件清单会被拒绝）。成功就起它。
    pub fn join(&mut self, i: usize, disable: &[&str]) -> Result<(), JoinError> {
        let master = read_master(&self.node_dir(1)).unwrap();
        let mut cfg = NodeConfig::default();
        cfg.plugins.disable = disable.iter().map(|s| s.to_string()).collect();
        // 种子优先找一个不是就绪 leader 的节点：join 要跟着 NOT_LEADER 的 leader_hint 找到 leader（3.4.3），这样每次多节点的 join 都走一遍
        let not_leader = self.nodes.iter().flatten().find(|r| !r.meta.role().ready);
        let seed = not_leader.unwrap_or_else(|| self.run(1)).addrs[0].1.clone();
        let o = JoinOpts {
            data_dir: self.node_dir(i),
            seed,
            master,
            addr: "127.0.0.1".into(),
            rpc_port: free_port(),
            mgmt_port: free_port(),
            mem: MEM,
            failpoints_api: true,
            debug_ops: true,
            host_id: Some(format!("host-{i}")),
            plugins: plugins(&cfg).plugin_list(),
            disable: cfg.plugins.disable.clone(),
            targets: self.target(i),
            meta_auto: true,
            transports: vec![Arc::new(tcp::TcpTransport::new(&Default::default(), 5000).unwrap())],
        };
        join_node(&o)?;
        let run = boot(&self.node_dir(i));
        if self.nodes.len() < i {
            self.nodes.resize_with(i, || None);
        }
        self.nodes[i - 1] = Some(run);
        Ok(())
    }

    pub fn run(&self, i: usize) -> &Running {
        self.nodes[i - 1].as_ref().unwrap_or_else(|| panic!("node{i} 没有在运行"))
    }

    /// 就绪的 leader 是第几个节点（没有 → None）。
    pub fn leader(&self) -> Option<usize> {
        self.nodes.iter().position(|r| r.as_ref().is_some_and(|r| r.meta.role().ready)).map(|i| i + 1)
    }

    /// 等到有就绪的 leader（最多 20 s）。
    pub fn wait_leader(&self) -> usize {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if let Some(l) = self.leader() {
                return l;
            }
            assert!(Instant::now() < deadline, "20 s 内没有就绪的 leader");
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// 等到 leader 看到 `n` 个 voter、不在 joint 状态（最多 60 s）。
    pub fn wait_voters(&self, n: usize) {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let st = self.run(self.wait_leader()).meta.status();
            if st.voters.len() >= n && st.voters_outgoing.is_empty() {
                return;
            }
            assert!(Instant::now() < deadline, "60 s 内没有 {n} 个 voter：{st:?}");
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// 停掉第 i 个节点（SIGTERM 的路径：leader 先让出 leader）。
    pub fn stop(&mut self, i: usize) {
        if let Some(r) = self.nodes[i - 1].take() {
            r.stop();
        }
    }

    /// 全部在运行的节点的地址（客户端的种子）。
    pub fn seeds(&self) -> Vec<Endpoint> {
        let live = self.nodes.iter().enumerate().filter_map(|(i, r)| r.as_ref().map(|r| (i, r)));
        live.map(|(i, r)| Endpoint { node: i as u32 + 1, addrs: r.addrs.clone() }).collect()
    }

    /// 用 node1 的 `<key>.key` 连到集群（MetaClient 跟随 leader）。
    pub fn client(&self, key: &str) -> Client {
        connect_seeds(
            &self.node_dir(1),
            &self.seeds(),
            key,
            PoolConfig { s_mb: 8, m_slots: 6, l_slots: 1, repl_lane: false },
        )
    }

    /// 等全部在运行的节点应用到同一个位置（最多 20 s），返回它。
    pub fn wait_caught_up(&self) -> u64 {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let applied: Vec<u64> = self.nodes.iter().flatten().map(|r| r.meta.meta_hash().unwrap().applied).collect();
            if applied.windows(2).all(|w| w[0] == w[1]) {
                return applied[0];
            }
            assert!(Instant::now() < deadline, "20 s 内各节点没有追平：{applied:?}");
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for r in self.nodes.iter_mut().filter_map(Option::take) {
            r.stop();
        }
    }
}

/// 起一个已有数据目录的节点（不等它当 leader：多节点时它多半是 follower）。
fn boot(nd: &Path) -> Running {
    let cfg = NodeConfig::parse(&std::fs::read_to_string(nd.join("node.toml")).unwrap()).unwrap();
    let run = server::start(nd, &cfg, plugins(&cfg), "test", Default::default()).unwrap();
    if std::env::var_os("WMFS_LOG").is_none() {
        log::set_level(log::Level::Warn);
    }
    run
}
