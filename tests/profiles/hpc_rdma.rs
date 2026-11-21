//! hpc-rdma profile（design.md 9.7；M15 定稿）：`wmfs dev up --nodes 3 --profile hpc-rdma --rdma <IP>` 起 3 个节点——default 的初始 KV
//! （`/scratch replicas=2`、`/home replicas=3`，quota 与 mclock 都启用），文件数据走 rdma（init / join 时就把 `[plugin.rdma]` 写进
//! node.toml、把 rdma 地址公布进集群图）；客户端用 `wmfs mount … --transport rdma` 挂两个，各用一个租户的 key（租户 1、2）。必过测试：
//! - IOR hard 形状的负载运行中 kill -9 元数据 leader 与一个数据节点：8 个线程按 47008 字节交错写同一个文件（IO500 ior-hard 的形状），
//!   写完换一个线程读回校验——应用 0 个错误（IOR 本身跑在容器里，要把 RDMA 设备与 rdma-core 带进容器，首发不做，design.md 9.7 的
//!   M15 注记；容器里的 IOR/mdtest 冒烟在 minimal 与 nas-safe 上跑）；
//! - 两个租户的 qos：`/scratch/t1`（租户 1）的 `l=` 上限小，同样的写比 `/scratch/t2`（租户 2）慢得多；指标核对上限真的生效
//!   （`two_tenants_qos`）；
//! - 配额并发（`common::quota_bound`）、strong 追加、锁互斥（`common.rs`，经两个挂载点）；
//! - 数据路径确实走 RDMA：客户端写时服务端 RDMA READ（`wmfs_rdma_ops_total{op="read"}` 增长），客户端读时服务端 RDMA WRITE
//!   （`op="write"` 增长）——rdma 插件的单边操作都由服务端发起。
//!
//! **本机没有可用的 RDMA 设备**（人类 2026-10-20 决定本机不做 RDMA 实机测试，不建 Soft-RoCE）：用例开头打印 SKIP 就返回，
//! `scripts/profiles.sh hpc-rdma` 根本不跑它、直接显示 SKIP——这个 profile **未在本机验证**，发布说明不得声称支持它（semantics.md 第 10 节）。
//! 用开发集群的固定端口，所以是 `#[ignore]`；开发目录用 `WMFS_PROFILE_DIR`（scripts/profiles.sh 给的，超时被杀时由它兜底清理）。

#[allow(dead_code)] // 与 minimal.rs、nas_safe.rs 共用：每个 profile 只用其中一部分
mod common;

use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use common::Cluster;
use wmfs_core::api::types::{KeyFile, QosReply};
use wmfs_mgr::api::ApiClient;
use wmfs_testkit::harness::net::rdma_ip;

fn wmfs(dir: &Path, args: &[&str]) -> String {
    common::wmfs(dir, 3, args)
}

/// 节点 n 的管理地址。
fn addr(n: u32) -> String {
    format!("127.0.0.1:19{n}80")
}

/// 用 node1 的 admin key 问三个节点的管理 API（`/metrics` 与 `/api/v1/qos` 用 `get_at` 指定节点：参数是 `addrs()` 里的网址）。
fn api(dir: &Path) -> ApiClient {
    let key = KeyFile::parse(&std::fs::read_to_string(dir.join("n1/keys/admin.key")).unwrap()).unwrap();
    ApiClient::new((1..=3).map(addr).collect(), key)
}

/// 三个节点 `/metrics` 里以 `prefix` 开头的样本之和（例如 `wmfs_rdma_ops_total{op="read"}`）。
fn sum(dir: &Path, prefix: &str) -> f64 {
    let api = api(dir);
    let value = |l: &str| l.strip_prefix(prefix).and_then(|rest| rest.rsplit(' ').next()?.parse::<f64>().ok());
    let node = |a: &String| -> f64 { api.get_at::<String>(a, "/metrics").unwrap().lines().filter_map(value).sum() };
    api.addrs().iter().map(node).sum()
}

/// 一个 `wmfs mount … --transport rdma` 挂载（`wmfs dev mount` 没有 `--transport`；同 cli/tests/common 的 Mount，挂载命令不同）：被丢弃
/// 时卸载，最多等挂载进程 10 s，还没退出就 kill。
struct Mount(PathBuf, Child);

impl Mount {
    fn new(mp: &Path, key: &Path) -> Mount {
        std::fs::create_dir_all(mp).unwrap();
        let seeds = "127.0.0.1:19100,127.0.0.1:19200,127.0.0.1:19300";
        let log = mp.with_extension("log");
        let child = Command::new(env!("CARGO_BIN_EXE_wmfs"))
            .args(["mount", seeds, mp.to_str().unwrap(), "--transport", "rdma", "--key-file", key.to_str().unwrap()])
            .args(["--log", log.to_str().unwrap()])
            .spawn()
            .unwrap();
        let t0 = Instant::now();
        while !std::fs::read_to_string("/proc/self/mountinfo").unwrap().contains(&format!(" {} ", mp.display())) {
            assert!(t0.elapsed() < Duration::from_secs(30), "30 s 内没挂上（日志 {}）", log.display());
            std::thread::sleep(Duration::from_millis(100));
        }
        Mount(mp.to_path_buf(), child)
    }
}

impl Drop for Mount {
    fn drop(&mut self) {
        let ok = Command::new("fusermount3").arg("-u").arg(&self.0).status().is_ok_and(|s| s.success());
        if !ok {
            let _ = Command::new("fusermount3").arg("-uz").arg(&self.0).status();
        }
        let t0 = Instant::now();
        while self.1.try_wait().ok().flatten().is_none() && t0.elapsed() < Duration::from_secs(10) {
            std::thread::sleep(Duration::from_millis(20));
        }
        let _ = self.1.kill();
        let _ = self.1.wait();
    }
}

#[test]
#[ignore = "要 RDMA 设备与开发集群的固定端口：由 scripts/profiles.sh hpc-rdma 跑（本机没有设备：SKIP）"]
fn hpc_rdma_profile() {
    let Some(ip) = rdma_ip() else {
        println!(
            "SKIP hpc-rdma（原因：本机没有 ACTIVE 的 RoCE v2 端口——人类决定本机不做 RDMA 实机测试；未在本机验证）"
        );
        return;
    };
    let dir = std::env::var_os("WMFS_PROFILE_DIR") // scripts/profiles.sh 给的目录（超时被杀时它来兜底清理）
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join(format!("wmfs-profile-hpc-rdma-{}", std::process::id())));
    let c = Cluster(dir.clone());
    wmfs(&dir, &["dev", "up", "--nodes", "3", "--profile", "hpc-rdma", "--rdma", &ip.to_string()]);
    let key = |name: &str, tenant: &str| {
        let out = dir.join(format!("{name}.key"));
        wmfs(&dir, &["key", "create", name, "--tenant", tenant, "--out", out.to_str().unwrap()]);
        out
    };
    let (a, b) = (Mount::new(&dir.join("mnt"), &key("t1", "1")), Mount::new(&dir.join("mnt2"), &key("t2", "2")));
    let (sa, sb) = (a.0.join("scratch"), b.0.join("scratch"));
    ior_hard_shape_under_kills(&dir, &sa.join("ior"));
    two_tenants_qos(&dir, &sa, &sb);
    for d in ["strong", "locks", "q"] {
        std::fs::create_dir(sa.join(d)).unwrap();
    }
    wmfs(&dir, &["set", "/scratch/strong", "consistency=strong", "--json"]);
    common::strong_append(&sa.join("strong"), &sb.join("strong"), 200);
    common::lock_counter(&sa.join("locks"), &sb.join("locks"), 1000);
    let (hard_mib, writers) = (32, 4);
    common::quota_bound(&dir, 3, &sa.join("q"), &sb.join("q"), "/scratch/q", hard_mib, writers);
    rdma_carries_the_data(&dir, &sa, &sb);
    drop((a, b));
    drop(c);
}

/// 数据路径确实走 RDMA：rdma 插件的单边操作都由服务端发起——客户端写（请求带 bulk）时服务端 RDMA READ 取数据，客户端读（回复带 bulk）
/// 时服务端 RDMA WRITE 送数据。各断言一次（三个节点的 `wmfs_rdma_ops_total` 合起来）。
fn rdma_carries_the_data(dir: &Path, sa: &Path, sb: &Path) {
    let reads = sum(dir, "wmfs_rdma_ops_total{op=\"read\"}");
    let f = std::fs::File::create(sa.join("rdma-w")).unwrap();
    f.write_all_at(&vec![9u8; 4 << 20], 0).unwrap(); // 比 inline 的 64 KiB 大：走数据节点
    f.sync_all().unwrap();
    assert!(sum(dir, "wmfs_rdma_ops_total{op=\"read\"}") > reads, "客户端写：服务端 RDMA READ 取请求的 bulk");
    let writes = sum(dir, "wmfs_rdma_ops_total{op=\"write\"}");
    let back = std::fs::read(sb.join("ior")).unwrap(); // 另一个挂载：页缓存里没有，读经数据节点
    assert!(!back.is_empty());
    assert!(sum(dir, "wmfs_rdma_ops_total{op=\"write\"}") > writes, "客户端读：服务端 RDMA WRITE 送回复的 bulk");
}

const SEG: u64 = 47008; // IO500 ior-hard 的传输大小
const RANKS: u64 = 8;
const SEGS: u64 = 200;

fn seg(rank: u64, i: u64) -> Vec<u8> {
    (0..SEG).map(|j| (j ^ (rank * 131 + i * 7)) as u8).collect()
}

/// 8 个"rank"按 47008 字节交错写同一个文件（rank r 的第 i 段在 (i × 8 + r) × 47008），写的同时 kill -9 元数据 leader（起来之后）再
/// kill 一个别的节点；全部写完各自 fsync，再换一个 rank 读回校验：0 个错误。
fn ior_hard_shape_under_kills(dir: &Path, file: &Path) {
    std::fs::File::create(file).unwrap();
    let ts: Vec<_> = (0..RANKS)
        .map(|r| {
            let file = file.to_path_buf();
            std::thread::spawn(move || {
                let f = std::fs::OpenOptions::new().write(true).open(&file).unwrap();
                for i in 0..SEGS {
                    f.write_all_at(&seg(r, i), (i * RANKS + r) * SEG).unwrap();
                    std::thread::sleep(Duration::from_millis(20));
                }
                f.sync_all().unwrap();
            })
        })
        .collect();
    let leader = wmfs(dir, &["dev", "leader"]).trim().to_string();
    let other = if leader == "1" { "2" } else { "1" };
    for n in [leader.as_str(), other] {
        std::thread::sleep(Duration::from_secs(1));
        wmfs(dir, &["dev", "kill", n]);
        std::thread::sleep(Duration::from_secs(1));
        wmfs(dir, &["dev", "start", n]);
        wmfs(dir, &["dev", "wait", "leader", "--timeout", "60"]);
    }
    ts.into_iter().for_each(|t| t.join().expect("写的 rank 看到了错误"));
    wmfs(dir, &["dev", "wait", "chains-serving", "--timeout", "120"]);
    let f = std::fs::File::open(file).unwrap();
    for r in 0..RANKS {
        let reader = (r + 1) % RANKS; // 换一个 rank 读（-C）：在这个进程里只是换个顺序，内容必须一致
        for i in 0..SEGS {
            let mut got = vec![0u8; SEG as usize];
            f.read_exact_at(&mut got, (i * RANKS + reader) * SEG).unwrap();
            assert!(got == seg(reader, i), "rank {reader} 的第 {i} 段读回来不对");
        }
    }
}

/// 两个租户的 qos（9.7）：挂载 `sa` 用租户 1 的 key、`sb` 用租户 2 的。`/scratch/t1` 的上限小（l=270：3 个节点每节点 90 成本单位/秒，
/// 1 MiB 的写成本 9 → 每节点每秒 10 个），`/scratch/t2` 的大；各写 40 MiB 并 fsync——t1 明显慢。指标核对上限真的生效：(租户 1, t1) 的
/// 队列在某个节点上被上限推迟过（`GET /api/v1/qos` 的 `limited_total`）、它的 `wmfs_qos_served_total` 有派发，(租户 2, t2) 的队列没被推迟过。
fn two_tenants_qos(dir: &Path, sa: &Path, sb: &Path) {
    let data = vec![5u8; 40 << 20];
    let (mut took, mut ids) = (Vec::new(), Vec::new());
    for (t, q, scratch) in [("t1", "qos=r=0,w=1,l=270", sa), ("t2", "qos=r=30,w=3,l=30000", sb)] {
        std::fs::create_dir(scratch.join(t)).unwrap();
        let fs_path = format!("/scratch/{t}");
        wmfs(dir, &["set", &fs_path, q, "--json"]);
        let st: serde_json::Value = serde_json::from_str(&wmfs(dir, &["fs", "stat", &fs_path, "--json"])).unwrap();
        ids.push(st["ino"].as_u64().unwrap()); // qos_id = qos 属性的来源目录
        let t0 = Instant::now();
        let f = std::fs::File::create(scratch.join(t).join("f")).unwrap();
        f.write_all_at(&data, 0).unwrap();
        f.sync_all().unwrap();
        took.push(t0.elapsed());
    }
    println!("QoS：t1（租户 1）用了 {:?}，t2（租户 2）用了 {:?}", took[0], took[1]);
    assert!(took[0] > took[1] * 2, "t1 的 l= 上限生效：比 t2 慢得多");
    let api = api(dir);
    let queues: Vec<_> =
        api.addrs().iter().flat_map(|a| api.get_at::<QosReply>(a, "/api/v1/qos").unwrap().queues).collect();
    let limited = |tenant: u32, id: u64| -> u64 {
        let mine = queues.iter().filter(|q| q.tenant == Some(tenant) && q.qos_id == Some(id));
        mine.map(|q| q.limited_total).sum()
    };
    assert!(limited(1, ids[0]) > 0, "(租户 1, /scratch/t1) 的队列被 l= 上限推迟过：{queues:?}");
    assert_eq!(limited(2, ids[1]), 0, "(租户 2, /scratch/t2) 的上限很大，没被推迟过：{queues:?}");
    let served = sum(dir, &format!("wmfs_qos_served_total{{tenant=\"1\",qos_id=\"{}\",", ids[0]));
    assert!(served > 0.0, "(租户 1, /scratch/t1) 的队列有派发（wmfs_qos_served_total）");
}
