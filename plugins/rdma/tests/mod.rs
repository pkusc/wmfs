//! rdma 插件的单元测试，全部**不需要 RDMA 设备**，也不调用任何 ibverbs 函数（连 `DeviceList::new` 都不调）：
//! 这里是纯函数（配置、接收槽校验、完成队列尺寸、描述符表、RdmaSetup 金样、地址解析、sysfs 选 GID、信用控制、推迟注册）；
//! qp.rs 是 QP 状态迁移的属性（C-10）与收帧的状态机；conn.rs、peer.rs、teardown.rs 用假网卡（fake.rs）测连接本身
//! （正常收发、不按规矩来的对端、拆除顺序）；stack.rs 让 core 的 RpcServer / RpcClient 跑在假网卡上。
//! 测试名都在 `rdma::` 模块下，`cargo test -p wmfs-plugins --features rdma rdma` 能选中它们。

mod conn;
mod fake;
mod peer;
mod qp;
mod stack;
mod teardown;

use std::collections::BTreeMap;
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use wmfs_core::base::config::PluginTable;
use wmfs_core::base::{ErrCode, clock};
use wmfs_core::buf::{MemPool, PoolConfig};
use wmfs_core::hooks::manifest::check_manifest_toml;
use wmfs_core::hooks::registry::PluginSet;
use wmfs_core::hooks::traits::Transport;
use wmfs_core::proto::frame::{FLAG_BULK_REMOTE, FLAG_REPLY_REMOTE, FrameKind};

use super::credit::Credits;
use super::desc::{Desc, check_bulk, decode_descs, encode_descs};
use super::gid::{parse_gid, pick};
use super::params::*;
use super::setup::{RdmaSetup, setup_body, setup_frame, setup_header};
use super::verbs::Verbs;
use super::wire::{Op, parse_wr_id, wr_id};
use super::*;

fn table(s: &str) -> PluginTable {
    s.parse().unwrap()
}

#[test]
fn rdma_manifest_toml_matches_code_and_validates() {
    check_manifest_toml(&MANIFEST, include_str!("../manifest.toml")).unwrap();
    assert!(MANIFEST.fail_points.contains(&"plugin.rdma.transport"));
    let t = RdmaTransport::new(&table("listen = \"127.0.0.1:19101\"\nmax_qps = 32"), 5000).unwrap();
    let mut s = PluginSet::new(&[]);
    s.transport(Arc::new(t));
    let cfg =
        BTreeMap::from([("rdma".to_string(), table("listen = \"127.0.0.1:19101\"\ndevice = \"rxe0\"\nmax_qps = 32"))]);
    s.validate(&cfg).unwrap();
    let bad = BTreeMap::from([("rdma".to_string(), table("lisen = \"x\""))]);
    assert!(s.validate(&bad).unwrap_err().contains("lisen"), "不认识的键被拒绝");
}

#[test]
fn rdma_config_defaults_and_bad_values() {
    let c = RdmaCfg::parse(&PluginTable::new()).unwrap();
    assert_eq!(
        c,
        RdmaCfg { listen: None, device: None, gid_index: None, max_qps: 64 },
        "默认：不监听、自动选设备与 GID"
    );
    let c =
        RdmaCfg::parse(&table("listen = \"10.77.0.1:19101\"\ndevice = \"rxe0\"\ngid_index = 1\nmax_qps = 32")).unwrap();
    assert_eq!(c.listen, Some("10.77.0.1:19101".parse().unwrap()));
    assert_eq!((c.device.as_deref(), c.gid_index, c.max_qps), (Some("rxe0"), Some(1), 32));
    assert_eq!(RdmaCfg::parse(&table("listen = \"\"")).unwrap().listen, None, "空字符串 = 不监听");
    for bad in
        ["listen = 5", "listen = \"[::1]:1\"", "listen = \"10.0.0.1\"", "gid_index = 256", "max_qps = 0", "device = 1"]
    {
        assert!(RdmaCfg::parse(&table(bad)).is_err(), "{bad} 应被拒绝");
    }
    assert!(RdmaTransport::new(&PluginTable::new(), 0).is_err(), "写超时为 0");
}

#[test]
fn rdma_recv_slot_startup_check() {
    let mib = 1u64 << 20;
    assert!(check_recv_slots(64, 256 * mib).is_ok(), "server()：64 个 QP 配 256 MiB 恰好通过");
    assert!(check_recv_slots(32, 128 * mib).is_ok(), "dev_server()：32 个 QP 配 128 MiB 通过");
    let e = check_recv_slots(65, 256 * mib).unwrap_err();
    assert!(e.contains("至少 260 MiB") && e.contains("s_mb"), "{e}");
    let e = check_recv_slots(33, 128 * mib).unwrap_err();
    assert!(e.contains("至少 132 MiB"), "{e}");
}

#[test]
fn rdma_cq_and_device_capability_arithmetic() {
    assert_eq!(cq_entries(64), 64 * (16 + 48) + 64, "每个 QP 16 个接收完成 + 48 个发送完成，再留余量");
    const {
        assert!(MAX_SEND_WR >= 16 + 2 * 16, "一个请求的 16 个 READ + 一个回复的 16 个 WRITE 与 1 个 SEND")
    };
    let good = DevCaps { max_qp: 1000, max_qp_wr: 1024, max_cqe: 1 << 20, max_qp_rd_atom: 16, max_qp_init_rd_atom: 16 };
    assert!(check_caps("mlx5_0", 64, &good).is_ok());
    let small_cq = DevCaps { max_cqe: 1000, ..good };
    assert!(check_caps("rxe0", 64, &small_cq).unwrap_err().contains("完成队列"));
    let no_read = DevCaps { max_qp_rd_atom: 4, ..good };
    assert!(check_caps("rxe0", 64, &no_read).unwrap_err().contains("READ"));
    assert!(check_caps("rxe0", 2000, &good).unwrap_err().contains("max_qps"));
}

#[test]
fn rdma_descriptor_table_layout_and_limits() {
    let d = |i: u64| Desc { addr: 0x1000_0000_0000 + i, rkey: 0x77 + i as u32, len: 100 + i as u32 };
    for n in [1usize, 16] {
        let bulk: Vec<Desc> = (0..n as u64).map(d).collect();
        let mut t = Vec::new();
        encode_descs(&mut t, &bulk, Some(&d(99)));
        assert_eq!(t.len(), 1 + 16 * n + 16);
        assert_eq!(t[0] as usize, n);
        assert_eq!(&t[1..9], &d(0).addr.to_le_bytes(), "小端");
        assert_eq!(decode_descs(FLAG_BULK_REMOTE | FLAG_REPLY_REMOTE, &t), Ok((bulk, Some(d(99)))));
    }
    let mut g = Vec::new();
    encode_descs(&mut g, &[d(0), d(1)], Some(&d(9)));
    let hex: String = g.iter().map(|b| format!("{b:02x}")).collect();
    let golden = include_str!("../setup.golden").lines().find_map(|l| l.strip_prefix("desc.bulk2_reply ")).unwrap();
    assert_eq!(hex, golden.trim(), "描述符表的字节布局变了（两端一起升级）");
    let mut t = Vec::new();
    encode_descs(&mut t, &[], Some(&d(5)));
    assert_eq!(t.len(), 16, "只有 REPLY_REMOTE：恰好一个描述符，不带 n");
    assert_eq!(decode_descs(FLAG_REPLY_REMOTE, &t), Ok((vec![], Some(d(5)))));
    let bad = ErrCode::PROTO_UNSUPPORTED;
    assert_eq!(decode_descs(FLAG_BULK_REMOTE, &[0]), Err(bad), "n = 0");
    let mut t17 = vec![17u8];
    t17.extend(std::iter::repeat_n(0u8, 17 * 16));
    assert_eq!(decode_descs(FLAG_BULK_REMOTE, &t17), Err(bad), "n = 17");
    assert_eq!(decode_descs(FLAG_REPLY_REMOTE, &t[..15]), Err(bad), "短了");
    assert_eq!(decode_descs(0, &[1]), Err(bad), "多出来的字节");
    let mut edge = Vec::new();
    encode_descs(&mut edge, &[], Some(&Desc { addr: u64::MAX - 100, rkey: 1, len: 100 }));
    assert!(decode_descs(FLAG_REPLY_REMOTE, &edge).is_ok(), "addr + len 正好是 u64::MAX：不溢出");
    let mut over = Vec::new();
    encode_descs(&mut over, &[Desc { addr: u64::MAX - 10, rkey: 1, len: 100 }], None);
    assert_eq!(decode_descs(FLAG_BULK_REMOTE, &over), Err(bad), "对端给的 addr + len 溢出：拒绝（之后算偏移不会溢出）");
    assert!(check_bulk(&[d(0), d(1)], 201).is_ok());
    assert_eq!(check_bulk(&[d(0), d(1)], 200), Err(bad), "Σlen ≠ bulk_len：会写穿相邻的池槽");
    assert_eq!(check_bulk(&[Desc { len: 0, ..d(0) }, d(1)], 101), Err(bad), "len = 0");
    assert_eq!(check_bulk(&[], 0), Err(bad), "没有描述符");
}

/// RdmaSetup 的线上字节固定下来（金样在 setup.golden）；结构里没有 rkey。
#[test]
fn rdma_setup_golden_and_codec() {
    let m = RdmaSetup {
        qpn: 0x00ab_cdef,
        psn: 0x0012_3456,
        gid: [0xfe, 0x80, 0, 0, 0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8],
        mtu: 3,
    };
    let f = setup_frame(FrameKind::Request, &m);
    let hex: String = f.iter().map(|b| format!("{b:02x}")).collect();
    let golden = include_str!("../setup.golden").lines().find_map(|l| l.strip_prefix("RdmaSetup.request ")).unwrap();
    assert_eq!(hex, golden.trim(), "RdmaSetup 帧的字节变了（插件两端私有格式，改了要两端一起升级）");
    let h = setup_header(f[..48].try_into().unwrap(), FrameKind::Request).unwrap();
    assert_eq!(setup_body(&h, &f[48..]), Ok(m));
    assert_eq!(f.len() - 48, 4 + 3 + 16 + 1, "qpn、psn（varint）+ gid + mtu：没有 rkey");
    assert!(setup_header(f[..48].try_into().unwrap(), FrameKind::Response).is_err(), "kind 不对");
    let mut bad = f.clone();
    bad[60] ^= 1;
    assert!(setup_body(&h, &bad[48..]).is_err(), "body CRC 不对");
    let m0 = RdmaSetup { mtu: 9, ..m };
    let f0 = setup_frame(FrameKind::Response, &m0);
    let h0 = setup_header(f0[..48].try_into().unwrap(), FrameKind::Response).unwrap();
    assert!(setup_body(&h0, &f0[48..]).is_err(), "mtu 只能是 1..=5");
}

#[test]
fn rdma_address_psn_and_wr_id() {
    assert_eq!(parse_addr("127.0.0.1:19101"), Ok("127.0.0.1:19101".parse().unwrap()));
    for bad in ["127.0.0.1", "[::1]:19101", ""] {
        assert_eq!(parse_addr(bad), Err(ErrCode::EINVAL), "{bad}");
    }
    assert_eq!(psn([0xff; 4]), 0x00ff_ffff, "只取低 24 位");
    let id = wr_id(0xdead_beef, Op::Write, 0x0123_4567);
    assert_eq!(parse_wr_id(id), (0xdead_beef, Some(Op::Write), 0x23_4567));
}

/// 在临时目录里造一棵 sysfs 树：rxe0（ACTIVE，RoCE v2 的 10.77.0.1 在第 1 项）与 mlx5_0（DOWN）。
fn fake_sysfs() -> PathBuf {
    let root = std::env::temp_dir().join(format!("wmfs-rdma-sysfs-{}-{:x}", std::process::id(), clock::mono_ms()));
    let put = |p: &str, s: &str| {
        let f = root.join(p);
        std::fs::create_dir_all(f.parent().unwrap()).unwrap();
        std::fs::write(f, s).unwrap();
    };
    put("rxe0/ports/1/state", "4: ACTIVE\n");
    put("rxe0/ports/1/gids/0", "fe80:0000:0000:0000:0000:0000:0000:0001\n");
    put("rxe0/ports/1/gid_attrs/types/0", "IB/RoCE v1\n");
    put("rxe0/ports/1/gids/1", "0000:0000:0000:0000:0000:ffff:0a4d:0001\n");
    put("rxe0/ports/1/gid_attrs/types/1", "RoCE v2\n");
    put("rxe0/ports/1/gids/2", "0000:0000:0000:0000:0000:ffff:0a4d:0002\n"); // 没有 types 文件（空表项读它会 EINVAL）
    put("mlx5_0/ports/1/state", "1: DOWN\n");
    put("mlx5_0/ports/1/gids/3", "0000:0000:0000:0000:0000:ffff:0a00:0005\n");
    put("mlx5_0/ports/1/gid_attrs/types/3", "RoCE v2\n");
    root
}

#[test]
fn rdma_gid_is_picked_from_sysfs_by_local_ipv4() {
    let root = fake_sysfs();
    let r = pick(&root, Ipv4Addr::new(10, 77, 0, 1), None, None).unwrap();
    assert_eq!((r.device.as_str(), r.port, r.gid_index, r.active), ("rxe0", 1, 1, true));
    assert_eq!(r.gid, parse_gid("0000:0000:0000:0000:0000:ffff:0a4d:0001").unwrap());
    assert!(pick(&root, Ipv4Addr::new(10, 77, 0, 1), Some("rxe0"), Some(1)).is_ok(), "写了 device 与 gid_index 也认");
    let e = pick(&root, Ipv4Addr::new(10, 77, 0, 1), Some("mlx5_0"), None).unwrap_err();
    assert!(e.why.contains("设备 mlx5_0") && e.why.contains("没有对应") && !e.link_down, "{e:?}");
    let e = pick(&root, Ipv4Addr::new(10, 0, 0, 5), None, None).unwrap_err();
    assert!(e.why.contains("不是 ACTIVE"), "DOWN 的端口不用：{e:?}");
    assert!(e.link_down, "GID 在、只是链路不是 ACTIVE：connect 按连接断开处理（可能只是暂时断链）");
    let e = pick(&root, Ipv4Addr::LOCALHOST, None, None).unwrap_err();
    assert!(
        e.why.contains("rxe0/1 gid_index 1（10.77.0.1，ACTIVE）") && e.why.contains("mlx5_0/1 gid_index 3"),
        "列出候选：{e:?}"
    );
    assert!(!e.why.contains("gid_index 2"), "读不到类型的表项按未使用处理：{e:?}");
    assert!(!e.link_down, "本机根本没有这个地址的 GID：connect 报 EINVAL（不重试）");
    let empty = root.join("nothing");
    assert!(pick(&empty, Ipv4Addr::LOCALHOST, None, None).unwrap_err().why.contains("没有任何"));
    std::fs::remove_dir_all(&root).unwrap();
}

/// 信用按帧计、等信用不设期限（对端回得慢不算出错）、多出来的回复不让信用超过上限、close 能打断等待。
#[test]
fn rdma_credits_block_release_and_close() {
    let c = Arc::new(Credits::new(16));
    for _ in 0..16 {
        c.take().unwrap(); // 两个 req_id = 0 的保活也各占各的：信用按帧计，不按 req_id
    }
    assert_eq!(c.available(), 0);
    let wait_one = |c: &Arc<Credits>| {
        let (c2, (tx, rx)) = (c.clone(), mpsc::channel());
        std::thread::spawn(move || tx.send(c2.take()).unwrap());
        rx
    };
    let rx = wait_one(&c);
    assert!(rx.recv_timeout(Duration::from_millis(300)).is_err(), "第 17 个一直等（不按写超时放弃）");
    c.give(); // 一个回复到达
    assert_eq!(rx.recv_timeout(Duration::from_secs(5)).unwrap(), Ok(()), "回复到达后放行");
    for _ in 0..20 {
        c.give();
    }
    assert_eq!(c.available(), 16, "多出来的回复不会让信用超过上限");
    for _ in 0..16 {
        c.take().unwrap();
    }
    let rx = wait_one(&c);
    std::thread::sleep(Duration::from_millis(100));
    c.close();
    assert_eq!(rx.recv_timeout(Duration::from_secs(5)).unwrap(), Err(ErrCode::ECONNRESET), "close 唤醒等待者");
    assert_eq!(c.take(), Err(ErrCode::ECONNRESET), "关闭之后不再给信用");
}

fn small_pool() -> MemPool {
    MemPool::new(&PoolConfig { s_mb: 1, m_slots: 1, l_slots: 1, repl_lane: false }).unwrap()
}

/// 空配置的 rdma 传输（默认编进二进制）：注册内存只记下区域、不监听，不打开任何设备；连本机没有 RoCE GID 的地址立刻
/// `EINVAL`（RpcClient 不重试）。用一棵空的假 sysfs，与本机有没有设备无关。
#[test]
fn rdma_empty_config_never_touches_a_device() {
    let root = std::env::temp_dir().join(format!("wmfs-rdma-empty-{}-{:x}", std::process::id(), clock::mono_ms()));
    let c = RdmaCfg::parse(&PluginTable::new()).unwrap();
    let v = Arc::new(Verbs::new(&c, Path::new(&root)));
    let t = RdmaTransport::with_provider(c, 5000, v.clone()).unwrap();
    let p = small_pool();
    for _ in 0..2 {
        t.register_region(&p.region()).unwrap(); // 幂等
    }
    let on: Arc<dyn Fn(Box<dyn Conn>) + Send + Sync> = Arc::new(|_c| {});
    assert_eq!(t.listen(p.clone(), on).unwrap(), None, "listen 默认为空：不监听");
    let t0 = Instant::now();
    assert_eq!(t.connect("127.0.0.1:19101", p, Duration::from_secs(5)).err(), Some(ErrCode::EINVAL));
    assert!(t0.elapsed() < Duration::from_secs(2), "本机没有能用的设备：很快失败");
    assert_eq!(t.connect("no-such-address", small_pool(), Duration::from_secs(1)).err(), Some(ErrCode::EINVAL));
    assert_eq!(v.opened(), 0, "没有打开任何设备");
}

/// 有对应的 GID、只是端口链路不是 ACTIVE（例如交换机重启）：connect 按连接断开处理（`ECONNRESET`，RpcClient 重连重试），
/// 不报 `EINVAL`；也不打开设备。假 sysfs 里给 127.0.0.1 造一个 DOWN 端口上的 RoCE v2 GID。
#[test]
fn rdma_link_down_is_a_retryable_connection_error() {
    let root = std::env::temp_dir().join(format!("wmfs-rdma-down-{}-{:x}", std::process::id(), clock::mono_ms()));
    let put = |p: &str, s: &str| {
        let f = root.join(p);
        std::fs::create_dir_all(f.parent().unwrap()).unwrap();
        std::fs::write(f, s).unwrap();
    };
    put("mlx5_9/ports/1/state", "1: DOWN\n");
    put("mlx5_9/ports/1/gids/1", "0000:0000:0000:0000:0000:ffff:7f00:0001\n");
    put("mlx5_9/ports/1/gid_attrs/types/1", "RoCE v2\n");
    let c = RdmaCfg::parse(&PluginTable::new()).unwrap();
    let v = Arc::new(Verbs::new(&c, Path::new(&root)));
    let t = RdmaTransport::with_provider(c, 5000, v.clone()).unwrap();
    assert_eq!(t.connect("127.0.0.1:19101", small_pool(), Duration::from_secs(5)).err(), Some(ErrCode::ECONNRESET));
    assert_eq!(v.opened(), 0, "没有打开任何设备");
    std::fs::remove_dir_all(&root).unwrap();
}

/// 监听一侧的接收槽启动校验：不满足就拒绝监听（EINVAL，原因写进日志）。
#[test]
fn rdma_listen_rejects_too_many_qps_for_the_s_class() {
    let c = RdmaCfg::parse(&table("listen = \"127.0.0.1:0\"\nmax_qps = 64")).unwrap();
    let t = RdmaTransport::with_provider(c, 5000, Arc::new(fake::FakeNic::new())).unwrap();
    let on: Arc<dyn Fn(Box<dyn Conn>) + Send + Sync> = Arc::new(|_c| {});
    assert_eq!(t.listen(small_pool(), on).err(), Some(ErrCode::EINVAL), "S 档 1 MiB 放不下 64 × 16 个接收槽");
}
