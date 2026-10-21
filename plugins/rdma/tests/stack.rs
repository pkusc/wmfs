//! core 的 RpcServer / RpcClient 跑在假网卡上的 rdma 传输上：3.3.2 握手、Ping、4 MiB bulk 经 READ/WRITE 往返 CRC 全对、
//! 池满回 BUSY 且不断开、错误密钥被拒、一串用完就丢的客户端、服务端回得慢时 17 个在途请求照样全部成功（与 tcp 一致）、
//! fail point `plugin.rdma.transport` 让连接作废后重连重试成功——core 里没有任何 rdma 专用代码，照样能用（"新增传输插件
//! 不改 core 一行"）。authn 用测试里自己写的一个最小插件（与 shared-key 同样的证明；插件之间不能互相引用模块，design.md 2.1）。
//!
//! fail point 是全进程共享的（core 的写线程在每次发送前都查它），所以这个文件里走 core 的用例一律先拿 [`serial`] 这把锁，
//! 一个一个跑。

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use wmfs_core::api::types::KeyFile;
use wmfs_core::api::{failpoint, metrics};
use wmfs_core::base::ErrCode;
use wmfs_core::base::config::NetConfig;
use wmfs_core::base::types::Role;
use wmfs_core::buf::{Buf, Lane, MemPool, PoolConfig};
use wmfs_core::hooks::manifest::{CORE_ABI_VERSION, Hook, Manifest};
use wmfs_core::hooks::pipeline::Pipeline;
use wmfs_core::hooks::registry::PluginSet;
use wmfs_core::hooks::traits::{AuthnInput, AuthnPlugin, AuthnResult, ConnIdentity, KeyLookup, Transport};
use wmfs_core::init::InitKeys;
use wmfs_core::net::client::{Endpoint, Family, RpcClient, check_reply};
use wmfs_core::net::retry::retry;
use wmfs_core::net::server::{Responder, RpcServer, ServerId, Service};
use wmfs_core::proto::encode_body;
use wmfs_core::proto::frame::{FrameHeader, FrameKind, InFrame, MSG_DATA_CALL, MSG_PING, OutFrame};
use wmfs_core::proto::handshake::{PingBody, ProofInput, auth_proof, derive_secret, server_proof};

use super::super::RdmaTransport;
use super::super::params::RdmaCfg;
use super::conn::pool;
use super::fake::FakeNic;

const MASTER: [u8; 32] = [0x5e; 32];
const T: Duration = Duration::from_secs(10);

/// 这个文件里走 core 的用例一个一个跑（fail point 是全进程的）。
fn serial() -> MutexGuard<'static, ()> {
    static SERIAL: Mutex<()> = Mutex::new(());
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

static TEST_AUTHN: Manifest = Manifest {
    name: "test-authn",
    version: "0.1.0",
    hooks: &[Hook::Authn],
    altitude: 150,
    hot_path: false,
    config_schema: "",
    abi_version: CORE_ABI_VERSION,
    attrs: &[],
    budget_ns: 0,
    fail_points: &["plugin.test-authn.authn"],
};

/// 最小的 authn：核对 HMAC 证明（3.3.2），通过就按 Hello 里的角色接受。
struct TestAuthn;

impl AuthnPlugin for TestAuthn {
    fn manifest(&self) -> &'static Manifest {
        &TEST_AUTHN
    }
    fn authenticate(&self, i: &AuthnInput, keys: &dyn KeyLookup) -> AuthnResult {
        let Some(secret) = keys.secret(i.key_id) else { return AuthnResult::Reject };
        let p = ProofInput {
            client_nonce: &i.client_nonce,
            server_nonce: &i.server_nonce,
            role: i.role.as_u8(),
            cluster_id: &i.cluster_id,
            key_id: i.key_id,
        };
        if auth_proof(&secret, &p) != *i.proof {
            return AuthnResult::Reject;
        }
        let id = ConnIdentity {
            key_id: i.key_id,
            role: i.role,
            tenant: 0,
            uid_ranges: vec![],
            root_squash: false,
            path_caps: vec![],
        };
        AuthnResult::Accept(id)
    }
    fn server_proof(&self, i: &AuthnInput, keys: &dyn KeyLookup) -> Option<[u8; 32]> {
        keys.secret(i.key_id).map(|s| server_proof(&s, &i.server_nonce, &i.client_nonce))
    }
}

/// 假网卡上的 rdma 传输。服务端的 max_qps 与 tests/harness 的 rdma() 一样留了余量（8）：客户端断开之后，服务端最多要
/// 1 s 多才发现（读线程每秒看一眼引导 TCP），这期间那条连接还占着一个名额。
fn rdma(nic: &FakeNic, listen: bool, write_timeout_ms: u32) -> Arc<RdmaTransport> {
    let listen = listen.then(|| "127.0.0.1:0".parse().unwrap());
    let max_qps = if listen.is_some() { 8 } else { 2 };
    let cfg = RdmaCfg { listen, device: None, gid_index: None, max_qps };
    Arc::new(RdmaTransport::with_provider(cfg, write_timeout_ms, Arc::new(nic.clone())).unwrap())
}

/// 跑在假网卡 rdma 上的 RPC 服务端；返回 (服务端, 地址, 服务端的池)。池的 S 档 32 MiB：监听一侧的接收槽启动校验要求
/// ≥ 2 × max_qps × 16 × 128 KiB（max_qps = 8）。
fn server(nic: &FakeNic) -> (Arc<RpcServer>, Endpoint, MemPool) {
    let t = rdma(nic, true, NetConfig::default().write_timeout_ms);
    let mut set = PluginSet::new(&[]);
    set.transport(t.clone());
    set.authn(Arc::new(TestAuthn));
    set.validate(&BTreeMap::new()).unwrap();
    let pipeline = Arc::new(Pipeline::new(set));
    let keys = Arc::new(InitKeys::new(MASTER, 0));
    let rpc = RpcServer::new(ServerId { cluster_id: [0xc1; 16], node_id: 1 }, pipeline, keys, &NetConfig::default());
    let sp = MemPool::new(&PoolConfig { s_mb: 32, m_slots: 2, l_slots: 1, repl_lane: false }).unwrap();
    t.register_region(&sp.region()).unwrap(); // server.rs 在 listen 之前做的事
    let t: Arc<dyn Transport> = t;
    let addr = rpc.listen(&t, sp.clone()).unwrap().unwrap();
    (rpc, Endpoint { node: 1, addrs: vec![("rdma".into(), addr)] }, sp)
}

/// 控制与数据都走 rdma 的客户端（像 `wmfs ping rdma://…`）；返回 (客户端, 它的池——bulk 与 recv_into 都要从它取)。
fn client_with(nic: &FakeNic, key: KeyFile, role: Role, write_timeout_ms: u32) -> (Arc<RpcClient>, MemPool) {
    let t: Arc<dyn Transport> = rdma(nic, false, write_timeout_ms);
    let cp = pool(false);
    let net = NetConfig { control_scheme: "rdma".into(), data_scheme: "rdma".into(), ..NetConfig::default() };
    (RpcClient::new_with_role(cp.clone(), vec![t], key, role, net).unwrap(), cp)
}

fn client(nic: &FakeNic, key: KeyFile, role: Role) -> (Arc<RpcClient>, MemPool) {
    client_with(nic, key, role, NetConfig::default().write_timeout_ms)
}

fn key(id: u32) -> KeyFile {
    KeyFile { key_id: id, secret: derive_secret(&MASTER, id) }
}

fn ping(payload: &[u8]) -> OutFrame {
    let body = encode_body(&PingBody { payload: payload.to_vec() });
    OutFrame { hdr: FrameHeader::new(FrameKind::Request, MSG_PING, 0, 0), body, bulk: vec![], recv_into: None }
}

fn ops(op: &str) -> u64 {
    metrics::counter("wmfs_rdma_ops_total", "", &[("op", op)]).get()
}

fn bulk_call(c: &RpcClient, ep: &Endpoint, cp: &MemPool, data: &wmfs_core::buf::FrozenBuf) -> Result<Buf, ErrCode> {
    let into = cp.get(data.as_slice().len(), Lane::Normal, T)?;
    let f = OutFrame { bulk: vec![data.whole()], recv_into: Some(into), ..ping(b"bulk") };
    check_reply(c.call(ep, Family::Data, f, T)?)?.bulk.ok_or(ErrCode::EIO)
}

#[test]
fn rdma_stack_handshake_ping_and_4m_bulk_over_the_fake_nic() {
    let _g = serial();
    let nic = FakeNic::new();
    let (_rpc, ep, _sp) = server(&nic);
    let (c, cp) = client(&nic, key(1), Role::Admin);
    for i in 0..100u32 {
        let r = check_reply(c.call(&ep, Family::Data, ping(&i.to_le_bytes()), T).unwrap()).unwrap();
        assert_eq!(r.body, encode_body(&PingBody { payload: i.to_le_bytes().to_vec() }));
    }
    let (reads, writes) = (ops("read"), ops("write"));
    let mut b = cp.get(4 << 20, Lane::Normal, T).unwrap();
    b.as_mut_slice().iter_mut().enumerate().for_each(|(i, x)| *x = (i * 7 % 251) as u8);
    let data = b.freeze();
    let want = crc32c::crc32c(data.as_slice());
    for _ in 0..20 {
        let r = bulk_call(&c, &ep, &cp, &data).unwrap();
        assert_eq!((r.len(), crc32c::crc32c(r.as_slice())), (4 << 20, want), "回显逐字节一致");
    }
    assert!(ops("read") >= reads + 20 && ops("write") >= writes + 20, "bulk 走了 RDMA READ（请求）与 WRITE（回复）");
    assert_eq!((nic.n(&nic.0.rnr), nic.n(&nic.0.violations)), (0, 0), "没有 RNR、没有越权访问");
}

#[test]
fn rdma_stack_full_pool_answers_busy_and_wrong_keys_are_rejected() {
    let _g = serial();
    let nic = FakeNic::new();
    let (_rpc, ep, sp) = server(&nic);
    let (c, cp) = client(&nic, key(1), Role::Admin);
    let mut b = cp.get(1 << 20, Lane::Normal, T).unwrap();
    b.as_mut_slice().fill(9);
    let data = b.freeze();
    let hog: Vec<Buf> = (0..2).map(|_| sp.get(1 << 20, Lane::Normal, T).unwrap()).collect();
    assert_eq!(bulk_call(&c, &ep, &cp, &data).err(), Some(ErrCode::BUSY), "服务端池满：不发 READ、回 BUSY");
    drop(hog);
    assert!(bulk_call(&c, &ep, &cp, &data).is_ok(), "连接还在，重试成功");
    let (bad, _) = client(&nic, KeyFile { key_id: 2, secret: [0x77; 32] }, Role::Client);
    assert_eq!(bad.call(&ep, Family::Data, ping(b"x"), T).err(), Some(ErrCode::AUTH_FAILED), "错误密钥被拒");
}

/// 与 tests/it/net_rpc.rs 的 auth_case 同样的顺序：依次建 7 个客户端，各调一次就丢掉。被拒的那几个必须拿到 AUTH_FAILED，
/// 而不是因为前面的客户端在服务端还占着名额（服务端最多 1 s 多才发现它们走了）而拿到 ECONNRESET。
#[test]
fn rdma_stack_short_lived_clients_one_after_another() {
    let _g = serial();
    let nic = FakeNic::new();
    let (_rpc, ep, _sp) = server(&nic);
    let wrong = KeyFile { key_id: 2, secret: [0x77; 32] };
    let cases = [
        (key(2), Role::Client),
        (key(0), Role::Server),
        (wrong, Role::Client),
        (key(2), Role::Server),
        (key(1), Role::Client),
        (key(0), Role::Admin),
        (key(3), Role::Client),
    ];
    let got: Vec<Result<(), ErrCode>> = cases
        .into_iter()
        .map(|(k, role)| {
            client(&nic, k, role).0.call(&ep, Family::Data, ping(b"x"), T).and_then(check_reply).map(|_| ())
        })
        .collect();
    let mut want = vec![Ok(()), Ok(())];
    want.extend([Err(ErrCode::AUTH_FAILED); 5]);
    assert_eq!(got, want);
}

/// 服务端把每个请求压 800 ms 再回（链重驱动、QoS 限速、慢盘……），同时发 17 个：第 17 个在写线程里等信用，等的时间超过
/// 写超时（客户端这里只给 300 ms）也不能断开——17 个全部成功，与 tcp 一样。
#[test]
fn rdma_stack_slow_server_with_17_outstanding_requests() {
    struct Slow;
    impl Service for Slow {
        fn call(&self, f: InFrame, r: Responder) {
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(800));
                r.reply(f.body, vec![]);
            });
        }
    }
    let _g = serial();
    let nic = FakeNic::new();
    let (rpc, ep, _sp) = server(&nic);
    rpc.add_service(MSG_DATA_CALL, Arc::new(Slow));
    let (c, _cp) = client_with(&nic, key(1), Role::Admin, 300);
    check_reply(c.call(&ep, Family::Data, ping(b"warm"), T).unwrap()).unwrap(); // 先把连接建好
    let call =
        |i: u32| OutFrame { hdr: FrameHeader::new(FrameKind::Request, MSG_DATA_CALL, 0, 0), ..ping(&i.to_le_bytes()) };
    let pending: Vec<_> = (0..17).map(|i| c.start(&ep, Family::Data, call(i), T).unwrap()).collect();
    for (i, p) in pending.into_iter().enumerate() {
        let r = p.wait().and_then(check_reply).map(|f| f.body);
        assert_eq!(r, Ok(encode_body(&PingBody { payload: (i as u32).to_le_bytes().to_vec() })), "第 {} 个", i + 1);
    }
    assert_eq!(nic.n(&nic.0.rnr), 0, "从没触发 RNR");
}

/// fail point `plugin.rdma.transport`（design.md 第 4 章开头：每个插件至少一个 fail point 用例）：一次发送失败 = 连接作废，
/// 调用方按 3.3.8 的"连接断开 → 重连重试"建出新的 QP 后成功。
#[test]
fn rdma_stack_failpoint_breaks_the_connection_and_retry_reconnects() {
    let _g = serial();
    let nic = FakeNic::new();
    let (_rpc, ep, _sp) = server(&nic);
    let (c, _cp) = client(&nic, key(1), Role::Admin);
    check_reply(c.call(&ep, Family::Data, ping(b"a"), T).unwrap()).unwrap(); // 先把连接建好
    let qps = nic.n(&nic.0.qps_made);
    let name = "plugin.rdma.transport";
    let hits = || metrics::counter("wmfs_failpoint_hits_total", "", &[("name", name)]).get();
    let before = hits();
    failpoint::set(name, "times(1,return(EIO))").unwrap();
    let mut attempts = 0;
    let r = retry("ping", Duration::from_secs(30), |n| {
        attempts = n;
        c.call(&ep, Family::Data, ping(b"b"), T).and_then(check_reply)
    });
    failpoint::clear(name);
    assert!(r.is_ok(), "重连重试之后成功：{:?}", r.err());
    assert!(attempts >= 1, "第一次发送失败（连接作废），重连后的第 {} 次才成功", attempts + 1);
    assert_eq!(hits(), before + 1, "fail point 正好触发一次");
    assert!(nic.n(&nic.0.qps_made) >= qps + 2, "重连建了一对新的 QP");
}
