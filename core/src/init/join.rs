//! `wmfs join` 的本地侧（design.md 3.4.3 的 join 第 1、3 步）：带着本节点的插件清单、地址与 target 向集群发 `NodeOp::Join`
//! （服务端身份 key 0：秘密由管理员复制过来的 cluster.key 派生，节点之间从不传输秘密），拿到 `Joined` 之后建数据目录。
//!
//! - 给的地址可以是任一节点：回 `NOT_LEADER` 带 `leader_hint` 就换到 leader（连续最多 3 跳，之后照常退避：两个节点互相指来指去时
//!   不忙转）；hint 指的地址连不上（刚被杀的 leader）就回到种子；还在选举（`NOT_READY`）、连不上就退避重试（30 s）。
//! - 被拒绝（插件清单不一致 → `EINVAL`，地址已被占用 → `EEXIST`）原样交给调用方，原因在 `ErrorBody.msg` 里（[`JoinError`]）；
//!   `wmfs join` 对插件清单不一致以退出码 3 结束。
//! - 数据目录与 init 的一样（cluster.key、node.toml、keys、TARGET 超级块，NODE 最后写），只是元数据库是**空的**
//!   （`bootstrap_empty`：空日志、空 ConfState，坑 P14；Joined 里的集群图预写进 kv，见 `seed_kv` 与 `meta/raft/boot.rs`）。
//!   实现注记（M8）：与 init 一样，NodeRecV1 的地址取 node.toml 的监听地址（`--addr:--rpc-port`），不先起传输去问 `listen()`。

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use wmfs_api::hex::hex;
use wmfs_api::types::KeyFile;

use super::{
    Listen, NodeFile, check_new_node, create_meta_db, node_config, read_fact, write_common, write_node,
    write_superblocks,
};
use crate::base::config::{NetConfig, TargetConfig};
use crate::base::{ErrCode, clock};
use crate::buf::{MemPool, PoolConfig};
use crate::hooks::traits::Transport;
use crate::meta::raft::storage;
use crate::meta::schema::keys::{self, G_CLUSTER, G_FEATURES, G_MAP_EPOCH, G_PLUGINS, G_SETTINGS};
use crate::meta::schema::records::{ClusterInfoV1, PluginListV1, encode_v1};
use crate::net::client::{Endpoint, Family, RpcClient};
use crate::net::retry::{BACKOFF_MAX_MS, BACKOFF_START_MS, retryable};
use crate::proto::frame::{FrameHeader, FrameKind, MSG_NODE_CALL, OutFrame};
use crate::proto::handshake::{SERVER_KEY, derive_secret};
use crate::proto::peer::{ClusterMap, JOIN_META_AUTO, JOIN_META_NONE, JoinReq, NodeOp, NodeReply};
use crate::proto::{decode_body, encode_body};

/// 找 leader、等它就绪的总期限；连续跟随 leader_hint 最多几跳（同 MetaClient）。
const JOIN_TIMEOUT_MS: u64 = 30_000;
const MAX_HINT_HOPS: u32 = 3;

/// join 的参数（`wmfs join` 与 `wmfs dev up --nodes N` 都用它）。
pub struct JoinOpts {
    pub data_dir: PathBuf,
    pub seed: String,     // 任一节点的 rpc 地址（host:port，按 control_scheme）
    pub master: [u8; 32], // cluster.key 的内容
    pub addr: String,
    pub rpc_port: u16,
    pub mgmt_port: u16,
    pub mem: PoolConfig,
    pub failpoints_api: bool,
    pub debug_ops: bool,
    pub host_id: Option<String>,
    pub plugins: PluginListV1, // 本节点按 node.toml 装出来的插件清单（与 disable 对应）
    pub disable: Vec<String>,  // node.toml 的 `[plugins] disable`
    pub targets: Vec<TargetConfig>,
    pub meta_auto: bool, // --meta auto（现有元数据成员 < 3 时以 learner 加入）| none
    pub transports: Vec<Arc<dyn Transport>>,
}

/// join 失败：错误码与给人看的原因；`rejected` = leader 拒绝了（原因是它写的 `ErrorBody.msg`：插件清单不一致是 `EINVAL`，
/// 地址已被占用是 `EEXIST`）。
#[derive(Debug)]
pub struct JoinError {
    pub code: ErrCode,
    pub msg: String,
    pub rejected: bool,
}

fn fail(code: ErrCode, msg: impl Into<String>) -> JoinError {
    JoinError { code, msg: msg.into(), rejected: false }
}

/// 加入集群并建好数据目录（见文件头）；返回分到的节点号。
pub fn join_node(o: &JoinOpts) -> Result<u32, JoinError> {
    let d = &o.data_dir;
    check_new_node(d, &o.targets).map_err(|e| fail(ErrCode::EINVAL, e))?;
    let machine_id = read_fact("/etc/machine-id");
    let caps = o.targets.iter().map(|t| t.capacity_bytes()).collect::<Result<Vec<_>, _>>();
    let caps = caps.map_err(|e| fail(ErrCode::EINVAL, e))?;
    let req = JoinReq {
        hostname: read_fact("/proc/sys/kernel/hostname"),
        host_id: o.host_id.clone().unwrap_or_else(|| machine_id.clone()),
        machine_id,
        addrs: vec![("tcp".into(), format!("{}:{}", o.addr, o.rpc_port))],
        mgmt_addr: format!("{}:{}", o.addr, o.mgmt_port),
        meta_role: if o.meta_auto { JOIN_META_AUTO } else { JOIN_META_NONE },
        targets: o.targets.iter().zip(caps).map(|(t, c)| (t.path.clone(), t.labels.clone(), c, vec![])).collect(),
        features: vec![],
        plugins: o.plugins.plugins.clone(),
    };
    let (node_id, target_ids, map) = ask(o, &req)?;
    let node = NodeFile { format: 1, cluster_id: hex(&map.cluster_id), node_id, created_ms: clock::wall_ms() };
    let net = Listen { addr: &o.addr, rpc_port: o.rpc_port, mgmt_port: o.mgmt_port };
    let mut cfg = node_config(&net, &o.targets, o.host_id.clone());
    (cfg.failpoints_api, cfg.debug_ops, cfg.mem) = (o.failpoints_api, o.debug_ops, o.mem);
    cfg.plugins.disable = o.disable.clone();
    let local = |e: String| fail(ErrCode::EIO, format!("已加入集群（节点 {node_id}），但建数据目录失败：{e}"));
    write_common(d, &cfg, &o.master).map_err(local)?;
    let db = create_meta_db(d).map_err(local)?;
    storage::bootstrap_empty(&db, &seed_kv(&map)).map_err(local)?;
    crate::base::sys::fsync_dir(&d.join("meta")).map_err(|e| local(e.to_string()))?;
    write_superblocks(&o.targets, &target_ids, &node).map_err(local)?;
    write_node(d, &node).map_err(local)?;
    Ok(node_id)
}

/// 发 Join，跟随 leader、按 3.3.8 退避（见文件头）。
fn ask(o: &JoinOpts, req: &JoinReq) -> Result<(u32, Vec<u32>, ClusterMap), JoinError> {
    let pool = MemPool::new(&PoolConfig::small()).map_err(|e| fail(e, "内存池创建失败"))?;
    let key = KeyFile { key_id: SERVER_KEY, secret: derive_secret(&o.master, SERVER_KEY) };
    let net = NetConfig::default();
    let scheme = net.control_scheme.clone();
    let rpc = RpcClient::new(pool, o.transports.clone(), key, net).map_err(|e| fail(e, "建 RPC 客户端失败"))?;
    let seed = Endpoint { node: 0, addrs: vec![(scheme.clone(), o.seed.clone())] };
    let mut ep = seed.clone();
    let (deadline, mut backoff, mut hops) = (clock::mono_ms() + JOIN_TIMEOUT_MS, BACKOFF_START_MS, 0);
    let body = encode_body(&NodeOp::Join(req.clone()));
    loop {
        let f = OutFrame {
            hdr: FrameHeader::new(FrameKind::Request, MSG_NODE_CALL, 0, 0),
            body: body.clone(),
            bulk: vec![],
            recv_into: None,
        };
        let (code, msg) = match rpc.call(&ep, Family::Control, f, Duration::from_secs(10)) {
            Ok(f) => match f.error() {
                None => match decode_body::<NodeReply>(&f.body) {
                    Ok(NodeReply::Joined { node_id, target_ids, map }) => return Ok((node_id, target_ids, *map)),
                    _ => return Err(fail(ErrCode::INTERNAL, "Join 的回复不对")),
                },
                Some(e) if e.code == ErrCode::NOT_LEADER.0 && e.leader_hint.is_some() && hops < MAX_HINT_HOPS => {
                    ep = Endpoint { node: 0, addrs: vec![(scheme.clone(), e.leader_hint.unwrap_or_default())] };
                    hops += 1;
                    continue;
                }
                Some(e) if !retryable(ErrCode(e.code)) => {
                    return Err(JoinError { code: ErrCode(e.code), msg: e.msg, rejected: true });
                }
                Some(e) => (ErrCode(e.code), e.msg),
            },
            Err(ErrCode::AUTH_FAILED) => {
                return Err(fail(ErrCode::AUTH_FAILED, "握手被拒绝：--cluster-key 与这个集群的 cluster.key 不符"));
            }
            Err(e) => {
                ep = seed.clone(); // 连不上：hint 指的可能是刚被杀的 leader，回到种子重新问
                (e, e.to_string())
            }
        };
        if !retryable(code) {
            return Err(fail(code, msg));
        }
        if clock::mono_ms() + backoff > deadline {
            let why = format!("{} s 内没能加入：节点没在运行，或者地址写错了？", JOIN_TIMEOUT_MS / 1000);
            return Err(fail(code, format!("{msg}（{why}）")));
        }
        std::thread::sleep(Duration::from_millis(backoff));
        (backoff, hops) = ((backoff * 2).min(BACKOFF_MAX_MS), 0);
    }
}

/// 预写进新节点 kv 表的记录（`meta/raft/boot.rs` 的实现注记）：Joined 的集群图里的节点地址、key 表与 GLOBAL 的 cluster、settings、
/// features、plugins、map_epoch。快照安装时整个 kv 表被删掉重建。
fn seed_kv(m: &ClusterMap) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut kv: Vec<(Vec<u8>, Vec<u8>)> = vec![
        (keys::global(G_CLUSTER), encode_v1(&ClusterInfoV1 { cluster_id: m.cluster_id, created_ms: 0, format: 1 })),
        (keys::global(G_SETTINGS), encode_v1(&m.settings)),
        (keys::global(G_FEATURES), encode_v1(&m.features)),
        (keys::global(G_PLUGINS), encode_v1(&m.plugins)),
        (keys::global(G_MAP_EPOCH), encode_v1(&m.map_epoch)),
    ];
    kv.extend(m.nodes.iter().map(|(id, n)| (keys::node(*id), encode_v1(n))));
    kv.extend(m.keys.iter().map(|(id, k)| (keys::keymeta(*id), encode_v1(k))));
    kv.sort();
    kv
}
