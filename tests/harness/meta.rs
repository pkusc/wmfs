//! 元数据测试的公共部分（design.md 9.2）：在 [`MemCrashBackend`] 上写 init 的 index = 1 初始状态、直接驱动 raft 的
//! `Driver`（"输入一步、处理一次 Ready"，不起线程）、提议批次、读 KV 与 raft_state、掉电重启；"测试扮演的对端"
//! （2 个 voter：节点 1 是我们的 Driver，节点 2 的 raft 消息由测试手写）；以及在同一种后端上起完整的元数据引擎
//! （raft + 执行器线程）、经执行器写与线性一致读。
//! raft-rs 与引擎的 INFO 日志直接写 stderr（libtest 捕获不到），这里只留 WARN 及以上；要看全部日志就设 `WMFS_LOG=info`。

use std::collections::BTreeMap;
use std::sync::Arc;

use crossbeam_channel::{Receiver, bounded};
use protobuf::Message as _;
use raft::StateRole;
use raft::eraftpb::{HardState, Message, MessageType};
use redb::Database;
use wmfs_core::base::ErrCode;
use wmfs_core::base::log::{self, Level};
use wmfs_core::base::types::{Principal, ReqCtx, Role, Ts};
use wmfs_core::hooks::pipeline::Pipeline;
use wmfs_core::hooks::registry::PluginSet;
use wmfs_core::hooks::traits::ConnIdentity;
use wmfs_core::init::{InitialKv, initial_kv};
use wmfs_core::meta::engine::txn::{MetaEngine, TxnOp, txn_blocking};
use wmfs_core::meta::engine::{EmbeddedEngine, EngineOpts};
use wmfs_core::meta::raft::driver::{Driver, DriverOpts, ProposeOutcome, RaftInput};
use wmfs_core::meta::raft::storage::{self, KV, RAFT_STATE, read_applied};
use wmfs_core::meta::schema::keys;
use wmfs_core::meta::schema::raft_state::{Applied, KEY_HARD_STATE, KEY_SNAP_META, SnapMeta};
use wmfs_core::meta::schema::records::{KvOp, PluginListV1, WriteBatchV1};
use wmfs_core::proto::meta::MetaReply;

use super::mem_crash::MemCrashBackend;

/// 测试节点的插件清单（与 init 写进 GLOBAL plugins 的相同，否则不就绪）。
pub fn plugins() -> PluginListV1 {
    PluginListV1 { plugins: vec!["authn:shared-key".into()] }
}

/// init 的固定输入。与 `core/tests/golden_init.rs` 的 `input()` 保持一致（core 的金样用不了 testkit），改一处要改两处。
pub fn sample_init() -> InitialKv {
    InitialKv {
        cluster_id: [0xc1; 16],
        created_ms: 1_792_454_400_123,
        owner: (1000, 1001),
        addrs: vec![("tcp".into(), "127.0.0.1:19100".into())],
        mgmt_addr: "127.0.0.1:19180".into(),
        host_id: "host-1".into(),
        machine_id: "0123456789abcdef0123456789abcdef".into(),
        hostname: "spark".into(),
        plugins: plugins(),
    }
}

/// 测试里只留 WARN 及以上的日志（没设 `WMFS_LOG` 时）。
pub fn quiet_logs() {
    if std::env::var_os("WMFS_LOG").is_none() {
        log::set_level(Level::Warn);
    }
}

pub fn open(b: &MemCrashBackend) -> Arc<Database> {
    Arc::new(Database::builder().create_with_backend(b.clone()).expect("在内存后端上打开 redb"))
}

/// 一个新后端，里面是 init 写的 index = 1 初始状态（voters 由测试指定）。
pub fn bootstrap(voters: &[u64]) -> MemCrashBackend {
    quiet_logs();
    let b = MemCrashBackend::new();
    storage::bootstrap(&open(&b), voters, &initial_kv(&sample_init())).expect("写初始状态");
    b
}

/// 一个直接驱动的节点。
pub struct Sim {
    pub id: u32,
    pub backend: MemCrashBackend,
    pub db: Arc<Database>,
    pub driver: Driver,
    plugins: PluginListV1,
}

/// 调试键空间里的一个 key。
pub fn dkey(k: &str) -> Vec<u8> {
    keys::debug(k.as_bytes())
}

impl Sim {
    pub fn start(backend: MemCrashBackend, id: u32) -> Sim {
        Sim::start_with(backend, id, plugins())
    }

    /// 同 [`Sim::start`]，本节点的插件清单由测试给（与 GLOBAL plugins 不同的节点当选了也不就绪）。
    pub fn start_with(backend: MemCrashBackend, id: u32, plugins: PluginListV1) -> Sim {
        quiet_logs();
        let db = open(&backend);
        let opts = DriverOpts { node_id: id, plugins: plugins.clone() };
        let driver = Driver::open(db.clone(), opts).expect("Driver::open");
        Sim { id, backend, db, driver, plugins }
    }

    /// 掉电并重启：只留下已落盘的字节（先取快照、再丢掉旧节点）。
    pub fn crash(self) -> Sim {
        let (b, id, plugins) = (self.backend.crash(), self.id, self.plugins.clone());
        drop(self);
        Sim::start_with(b, id, plugins)
    }

    /// 跑到就绪 leader（单节点：campaign 之后一两步就够）。
    pub fn until_ready(&mut self) {
        for _ in 0..50 {
            self.driver.process_ready();
            if self.driver.is_ready() {
                return;
            }
            self.driver.tick();
        }
        panic!("单节点 50 步还没有成为就绪 leader");
    }

    /// 投递一个批次的提议（不推进）；`expect = None` 时用当前的 state_version。
    pub fn propose(&mut self, ops: Vec<KvOp>, expect: Option<u64>) -> Receiver<ProposeOutcome> {
        let term = self.driver.published().role().term;
        let expect_version = expect.unwrap_or(self.driver.applied().state_version);
        let wb = WriteBatchV1 { expect_version, leader_term: term, ops, trace_ids: vec![] };
        let (done, rx) = bounded(1);
        self.driver.handle_input(RaftInput::Propose { data: wb.encode(), leader_term: term, done });
        rx
    }

    /// 提议并推进一步，返回结局（单节点一步就够：落盘 → 提交 → 应用）。
    pub fn write(&mut self, ops: Vec<KvOp>) -> ProposeOutcome {
        let rx = self.propose(ops, None);
        self.driver.process_ready();
        rx.try_recv().expect("单节点的提议一步之内应有结局")
    }

    /// 一次线性一致读的闸门（ReadIndex）。
    pub fn read_index(&mut self) -> Result<(), ErrCode> {
        let (done, rx) = bounded(1);
        self.driver.handle_input(RaftInput::ReadIndex { done });
        self.driver.process_ready();
        rx.try_recv().unwrap_or(Err(ErrCode::ETIMEDOUT))
    }

    /// 某个键空间里的全部键值（读已提交的状态，None 提交对新读事务可见）。
    pub fn kv(&self, tag: u8) -> BTreeMap<Vec<u8>, Vec<u8>> {
        let rtx = self.db.begin_read().unwrap();
        let t = rtx.open_table(KV).unwrap();
        let (lo, hi) = (vec![tag], vec![tag + 1]);
        t.range::<&[u8]>(lo.as_slice()..hi.as_slice())
            .unwrap()
            .map(|r| r.map(|(k, v)| (k.value().to_vec(), v.value().to_vec())).unwrap())
            .collect()
    }

    /// raft_state 里的 (hard_state, applied, snap_meta)。
    pub fn raft_state(&self) -> (HardState, Applied, SnapMeta) {
        let rtx = self.db.begin_read().unwrap();
        let st = rtx.open_table(RAFT_STATE).unwrap();
        let get = |k: &str| st.get(k).unwrap().unwrap().value().to_vec();
        let hs = HardState::parse_from_bytes(&get(KEY_HARD_STATE)).unwrap();
        (hs, read_applied(&rtx).unwrap(), SnapMeta::decode(&get(KEY_SNAP_META)).unwrap())
    }
}

// ---- 测试扮演的对端：2 个 voter，1 = 我们的 Driver，2 = 测试手写它的 raft 消息（不需要第二个 RawNode）----

/// 节点 `from` 发给节点 1 的一条 raft 消息。
pub fn msg(t: MessageType, from: u64, term: u64) -> Message {
    let mut m = Message::default();
    m.set_msg_type(t);
    (m.from, m.to, m.term) = (from, 1, term);
    m
}

/// voters = [1, 2] 的节点 1（不会自己 campaign，坑 P14）。
pub fn with_fake_peer() -> Sim {
    Sim::start(bootstrap(&[1, 2]), 1)
}

/// 节点 2 回应节点 1 发出的消息里的投票请求（预投票与投票都赞成），然后推进一步。
pub fn grant_votes(s: &mut Sim) {
    for m in s.driver.take_outbox() {
        let t = match m.get_msg_type() {
            MessageType::MsgRequestPreVote => MessageType::MsgRequestPreVoteResponse,
            MessageType::MsgRequestVote => MessageType::MsgRequestVoteResponse,
            _ => continue,
        };
        s.driver.handle_input(RaftInput::Msg(msg(t, 2, m.term)));
    }
    s.driver.process_ready();
}

/// tick 到节点 1 当选（节点 2 投票）。此时本任期的空条目还没提交，leader 未就绪。
pub fn elect(s: &mut Sim) {
    while s.driver.raw().raft.state != StateRole::Leader {
        s.driver.tick();
        s.driver.process_ready();
        grant_votes(s);
    }
}

/// 节点 2 确认到 leader 的最后一条（本任期的空条目随之提交、应用 → 就绪），然后推进一步。
pub fn ack_all(s: &mut Sim) {
    let mut ack = msg(MessageType::MsgAppendResponse, 2, s.driver.raw().raft.term);
    ack.index = s.driver.raw().raft.raft_log.last_index();
    s.driver.handle_input(RaftInput::Msg(ack));
    s.driver.process_ready();
}

/// 当选并已就绪的节点 1（2 个 voter，节点 2 由测试扮演）。
pub fn ready_leader_with_fake_peer() -> Sim {
    let mut s = with_fake_peer();
    elect(&mut s);
    ack_all(&mut s);
    assert!(s.driver.is_ready(), "节点 2 确认了本任期的空条目，节点 1 应已就绪");
    s
}

// ---- 完整的元数据引擎（raft 线程 + 执行器线程）----

/// 在后端 `b` 上起一个完整的元数据引擎（raft 线程 + 执行器线程），等它成为就绪 leader（单节点）。
pub fn engine(b: &MemCrashBackend, plugins: PluginListV1) -> Arc<EmbeddedEngine> {
    let eng = start_engine(b, plugins);
    wait_ready(&eng);
    eng
}

/// 同 [`engine`]，但不等它就绪（例如 voters = [1, 2] 的节点 1 永远当不上 leader）。
pub fn start_engine(b: &MemCrashBackend, plugins: PluginListV1) -> Arc<EmbeddedEngine> {
    quiet_logs();
    let db = Database::builder().create_with_backend(b.clone()).expect("在内存后端上打开 redb");
    let pipeline = Arc::new(Pipeline::new(PluginSet::new(&[])));
    EmbeddedEngine::start(db, EngineOpts { node_id: 1, plugins, pipeline }).expect("启动元数据引擎")
}

/// 等引擎成为就绪 leader（最多 10 s）；插件清单不一致的引擎永远等不到（调用方别用它）。
pub fn wait_ready(eng: &EmbeddedEngine) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !eng.role().ready {
        assert!(std::time::Instant::now() < deadline, "10 s 内没有成为就绪 leader：{:?}", eng.role());
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
}

/// 内部调用者的请求上下文（admin、seq 0）。
pub fn admin_ctx() -> ReqCtx {
    let conn = Arc::new(ConnIdentity {
        key_id: 1,
        role: Role::Admin,
        tenant: 0,
        uid_ranges: vec![(0, u32::MAX)],
        root_squash: false,
        path_caps: vec![],
    });
    let who = Principal { uid: 0, gid: 0, groups: vec![], tenant: 0, job_id: 0, conn };
    ReqCtx { who, session: 0, seq: 0, pid: 0, trace_id: 0, now: Ts::default(), now_ms: 0, internal: true }
}

/// 经执行器写一个任意的键（阻塞到批次有了结果）。
pub fn put_raw(eng: &EmbeddedEngine, key: Vec<u8>, value: Vec<u8>) -> Result<MetaReply, ErrCode> {
    let op: TxnOp = Box::new(move |v, _c| {
        v.put(&key, value);
        Ok(MetaReply::Ok)
    });
    txn_blocking(eng, admin_ctx(), op)
}

/// 线性一致地读一个键（先过读闸门）。
pub fn get_raw(eng: &EmbeddedEngine, key: &[u8]) -> Result<Option<Vec<u8>>, ErrCode> {
    match eng.read(&admin_ctx(), &mut |v, _c| Ok(MetaReply::DebugValue { value: v.get(key)? }))? {
        MetaReply::DebugValue { value } => Ok(value),
        other => panic!("意外的回复：{other:?}"),
    }
}
