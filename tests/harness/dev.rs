//! 多进程的开发集群（design.md 9.5 "单机多进程集群测试框架"的多节点版，M15 定稿）：对着 `wmfs dev up` 起的集群（环境变量
//! `WMFS_DEV_DIR`，固定端口 19n00/19n80，`PATH` 里要有 `wmfs`），经命令行捣乱：
//! - `kill`/`start`、`pause`/`resume`：`wmfs dev kill|start|pause|resume`（SIGKILL、SIGSTOP、SIGCONT），**只对本开发集群** pid 文件里、
//!   命令行核对过的进程发信号（`cli/src/dev/proc.rs` 的 `live_pid` 先看 `/proc/<pid>/cmdline`）；
//! - `partition(a, b)` / `heal(a, b)`：两边的每个节点各设 fail point `net.drop.to.<对面的节点>`（design.md 3.11：发往那个节点的请求与
//!   raft 消息丢在路上），不用 root、不用 iptables；客户端（挂载、测试进程）不受影响，只有服务端之间不通；
//! - `skew_clock(n, …)` / `clear_clock(n)`：fail point `clock.mono.rate_ppm`（单调时钟快或慢）与 `clock.wall.skew_ms`（墙钟偏移）；
//!   `delay(n, …)`：fail point `net.delay`（节点 n 发出的每一帧晚这么久，3.11）。
//! - 核对捣乱真的生效了：[`Dev::hits`]（节点 `/metrics` 里 fail point 的触发次数）、[`Dev::serving_on`]（某个节点上有几个 Serving 的链成员）。
//!
//! 进程内的 [`super::cluster::Cluster`] 做不了这些：fail point 是进程级的，一个进程里的几个节点分不开。
//! 实现注记（M15）：与 9.5 的"每个测试独立根目录、端口按测试名哈希"不同，这里对着开发集群的固定端口 19n00/19n80：同一时间只能有一个
//! 开发集群（用到它的用例都是 `#[ignore]`，由验收脚本一个一个跑）。
//!
//! 实现注记（M15 验收）：`wmfs …` 命令不在测试进程里起，而交给 [`Dev::from_env`] 起的一个 shell（[`RUNNER`]）去跑——那时测试还没打开
//! 挂载点上的任何文件。经 FUSE 读写的用例（jepsen 的 FUSE 版本、chaos 的写者）开着挂载点上的文件，在这样的进程里起子进程时，exec 关掉
//! 继承来的这些文件（O_CLOEXEC）会给每个都发一次 FLUSH，它要排在同一个 inode 上正在进行的写后面；那个写要是正卡在被冻住的节点上，
//! 让节点继续的 `wmfs dev resume` 就被它要解救的写拖住（冻住 10 s 变成了 120 s）。捣乱的时机不能取决于被测的客户端。

use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::Mutex;

use wmfs_core::api::types::{ChainsReply, Health, KeyFile};
use wmfs_core::buf::PoolConfig;
use wmfs_core::net::client::Endpoint;
use wmfs_mgr::api::ApiClient;

use super::node::{Client, connect_seeds};

/// 3 节点开发集群的 rpc 地址（固定端口 19n00）：参考客户端的种子。
pub fn dev_seeds() -> Vec<Endpoint> {
    (1..=3).map(|n| Endpoint { node: n, addrs: vec![("tcp".into(), format!("127.0.0.1:19{n}00"))] }).collect()
}

/// 替测试进程跑 `wmfs` 命令的 shell（见文件头）：一行一个命令、参数之间用制表符分开（参数里没有制表符与换行），回一行退出码。
/// `wmfs` 的 stdin 是 /dev/null（不读走后面的命令行），输出都丢掉（同 `Command::output` 只看退出码）。
const RUNNER: &str = "set -f; IFS='\t'; while read -r line; do wmfs $line </dev/null >/dev/null 2>&1; echo $?; done";

pub struct Dev {
    pub dir: PathBuf,
    runner: Mutex<Runner>,
}

/// 跑着 [`RUNNER`] 的 shell：命令写进它的 stdin，退出码从它的 stdout 读。
struct Runner {
    child: Child,
    to: Option<ChildStdin>,
    from: BufReader<ChildStdout>,
}

impl Drop for Runner {
    fn drop(&mut self) {
        drop(self.to.take()); // shell 读到 EOF 就退出
        let _ = self.child.wait();
    }
}

impl Dev {
    /// `WMFS_DEV_DIR` 指的开发集群（3 个节点）。在打开挂载点上的任何文件之前调用（它起跑命令的 shell，见文件头）。
    pub fn from_env() -> Dev {
        let dir = PathBuf::from(std::env::var("WMFS_DEV_DIR").expect("WMFS_DEV_DIR"));
        let mut child = Command::new("sh")
            .args(["-c", RUNNER])
            .env("WMFS_DEV_DIR", &dir)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("起跑 wmfs 命令的 sh");
        let (to, from) = (child.stdin.take(), BufReader::new(child.stdout.take().expect("sh 的 stdout")));
        Dev { dir, runner: Mutex::new(Runner { child, to, from }) }
    }

    /// 连到这个集群的参考客户端（admin key；DataClient 直接读写，不经 FUSE 与页缓存）。
    pub fn client(&self) -> Client {
        let pool = PoolConfig { s_mb: 8, m_slots: 6, l_slots: 1, repl_lane: false };
        connect_seeds(&self.dir.join("n1"), &dev_seeds(), "admin", pool)
    }

    /// 跑一条 `wmfs …`（`WMFS_DEV_DIR` 指向这个集群），返回是否成功。由 [`RUNNER`] 跑，不在测试进程里起子进程（见文件头）。
    pub fn wmfs(&self, args: &[&str]) -> bool {
        let mut r = self.runner.lock().unwrap_or_else(|e| e.into_inner());
        let Runner { to, from, .. } = &mut *r;
        let sent = to.as_mut().is_some_and(|w| writeln!(w, "{}", args.join("\t")).is_ok());
        let mut rc = String::new();
        sent && from.read_line(&mut rc).is_ok() && rc.trim() == "0"
    }

    fn dev(&self, what: &str, n: u32) {
        assert!(self.wmfs(&["dev", what, &n.to_string()]), "wmfs dev {what} {n} 失败");
    }

    /// kill -9 节点 n。
    pub fn kill(&self, n: u32) {
        self.dev("kill", n);
    }

    /// 启动节点 n（等到它的管理 API 回答）。
    pub fn start(&self, n: u32) {
        self.dev("start", n);
    }

    /// SIGSTOP 节点 n。
    pub fn pause(&self, n: u32) {
        self.dev("pause", n);
    }

    /// SIGCONT 节点 n。
    pub fn resume(&self, n: u32) {
        self.dev("resume", n);
    }

    /// 在节点 n 上设（`action` 给了）或清掉 fail point `name`（经节点 n 的管理 API，用 node1 的 admin key）。
    pub fn failpoint(&self, n: u32, name: &str, action: Option<&str>) -> bool {
        let (key, addr) = (self.dir.join("n1/keys/admin.key").display().to_string(), format!("127.0.0.1:19{n}80"));
        match action {
            Some(a) => self.wmfs(&["failpoint", "set", &addr, name, a, "--key-file", &key]),
            None => self.wmfs(&["failpoint", "clear", &addr, name, "--key-file", &key]),
        }
    }

    /// 把 `a`（被隔离的一边）与 `b` 分开：每个节点丢掉发往对面的请求与 raft 消息（两边都设，于是双向都不通）。fail point 是一个一个
    /// 设的，中间有一小段只断了一部分。链管理器把持续 10 s 转发不到的**下游**成员当作活着的成员移出，要过 `repair_delay` 才补回来
    /// （3.4.2.10；转发失败由上游随心跳报给元数据 leader），所以顺序要保证这一小段里被报告转发不到的只会是 `a` 上的成员（`a` 之后会被
    /// 判离线、回来时照常重新加入）：先断"b → a"；再断"a → b"，其中先断 a 到元数据 leader 的——a 报不上去了，它之后转发不到谁都不算数。
    /// [`Dev::heal`] 的顺序同理。
    pub fn partition(&self, a: &[u32], b: &[u32]) {
        self.drops(b, a, Some("return(ECONNRESET)"));
        let mut b = b.to_vec();
        if let Some(l) = self.leader() {
            b.sort_by_key(|n| *n != l); // 元数据 leader 排在最前
        }
        self.drops(a, &b, Some("return(ECONNRESET)"));
    }

    /// 撤掉 [`Dev::partition`] 设的 fail point：先放开"b → a"——`a` 能向元数据 leader 报告之前，`b` 对 `a` 的转发已经恢复（反过来的话，
    /// "b → a 还断着、a → leader 已通"这一段里 `b` 报告转发不到 `a`，而 `a` 这时不会再被判离线：活着的成员被移出，要等 repair_delay）；
    /// 再放开"a → b"，发往元数据 leader 的最后放开。
    pub fn heal(&self, a: &[u32], b: &[u32]) {
        self.drops(b, a, None);
        let mut b = b.to_vec();
        if let Some(l) = self.leader() {
            b.sort_by_key(|n| *n == l); // 元数据 leader 排在最后
        }
        self.drops(a, &b, None);
    }

    /// 就绪的元数据 leader 是第几个节点（同 `wmfs dev leader`：各节点 health 的 `meta_role`；没有 → None）。
    pub fn leader(&self) -> Option<u32> {
        (1..=3).find(|n| {
            let api = self.api(&[*n]);
            api.get_at::<Health>(&api.addrs()[0], "/api/v1/health").is_ok_and(|h| h.meta_role == "leader")
        })
    }

    /// `from` 里的每个节点丢掉（`action` 给了）或不再丢掉发往 `to` 里每个节点的请求。
    fn drops(&self, from: &[u32], to: &[u32], action: Option<&str>) {
        for (x, y) in from.iter().flat_map(|x| to.iter().map(move |y| (*x, *y))) {
            assert!(self.failpoint(x, &format!("net.drop.to.{y}"), action), "node{x} 的 net.drop.to.{y}");
        }
    }

    /// 节点 n 的单调时钟按 `1 + rate_ppm/1e6` 的速率走、墙钟偏 `wall_ms` 毫秒。租约只依赖时钟速率的偏差有界（持有方 0.8 倍先自停）：
    /// 两个节点之间的速率比不超过 1.25 都安全，所以捣乱时一次只让一个节点快或慢 10%。
    pub fn skew_clock(&self, n: u32, rate_ppm: i64, wall_ms: i64) {
        assert!(self.failpoint(n, "clock.mono.rate_ppm", Some(&format!("return({rate_ppm})"))));
        assert!(self.failpoint(n, "clock.wall.skew_ms", Some(&format!("return({wall_ms})"))));
    }

    /// 撤掉 [`Dev::skew_clock`]（已经多走或少走的部分保留；墙钟回到真实时间）。
    pub fn clear_clock(&self, n: u32) {
        assert!(self.failpoint(n, "clock.mono.rate_ppm", None));
        assert!(self.failpoint(n, "clock.wall.skew_ms", None));
    }

    /// 节点 n 发出的每一帧在入队之后至少晚 `ms` 毫秒才写出去（fail point `net.delay`，帧与帧之间不累加）；`None` = 撤掉。
    pub fn delay(&self, n: u32, ms: Option<u64>) {
        let action = ms.map(|ms| format!("return({ms})"));
        assert!(self.failpoint(n, "net.delay", action.as_deref()), "node{n} 的 net.delay");
    }

    /// 用 node1 的 admin key 问这些节点的管理 API（连不上换下一个、跟随 leader）。
    fn api(&self, nodes: &[u32]) -> ApiClient {
        let text = std::fs::read_to_string(self.dir.join("n1/keys/admin.key")).expect("读 n1/keys/admin.key");
        ApiClient::new(nodes.iter().map(|n| format!("127.0.0.1:19{n}80")).collect(), KeyFile::parse(&text).unwrap())
    }

    /// 节点 n 的 fail point `name` 到现在触发了几次（它 `/metrics` 里的 `wmfs_failpoint_hits_total`；从没设过 → 0）。
    pub fn hits(&self, n: u32, name: &str) -> u64 {
        let api = self.api(&[n]);
        let text: String =
            api.get_at(&api.addrs()[0], "/metrics").unwrap_or_else(|e| panic!("读 node{n} 的 /metrics：{e}"));
        let want = format!("wmfs_failpoint_hits_total{{name=\"{name}\"}} ");
        text.lines().find_map(|l| l.strip_prefix(&want)).map_or(0, |v| v.trim().parse::<f64>().unwrap_or(0.0) as u64)
    }

    /// 节点 n 上的 target 在多少条链里是 Serving 的成员（问 `ask` 里的节点：分区时问多数派那一边；问不到 → None）。
    pub fn serving_on(&self, n: u32, ask: &[u32]) -> Option<usize> {
        let r: ChainsReply = self.api(ask).get("/api/v1/chains").ok()?;
        Some(r.chains.iter().flat_map(|c| &c.members).filter(|m| m.node == n && m.state == "serving").count())
    }
}
