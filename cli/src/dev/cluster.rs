//! `wmfs dev` 的多节点部分（design.md 7.2，roadmap M8）：`dev up --nodes N` 的第 2…N 个节点用 `wmfs join` 的本地侧加入（种子 = node1
//! 的 rpc 地址，cluster.key 就是 node1 的那个文件——`wmfs dev` 在单机上，3.4.3），起好之后等元数据成员齐了才返回（[`wait_members`]）；
//! `dev start n` 最后等这个节点追上（[`wait_caught_up`]）；`dev leader` 打印就绪 leader 的节点号（`L=$(wmfs dev leader); wmfs dev kill $L`）。
//! M9：`dev restart-all`（[`restart_all`]，3.7.2 第 4 条）。

use std::path::Path;
use std::time::Duration;

use wmfs_core::api::types::ClusterReply;
use wmfs_core::base::clock;
use wmfs_core::buf::PoolConfig;
use wmfs_core::init::{JoinOpts, Profile, join_node, read_master};
use wmfs_mgr::api::ApiClient;

use super::{admin_key, dev_target, health, mgmt_addr, node_dir, nodes, ports, running_pid, start, stop};
use crate::registry;

/// 等元数据成员齐、等一个重启的节点追上的期限。
const MEMBERS_TIMEOUT_MS: u64 = 60_000;
const CATCH_UP_TIMEOUT_MS: u64 = 30_000;

/// 第 n 个开发节点加入 node1 所在的集群（node1 已经在运行）。
pub fn join(d: &Path, n: u32, profile: Profile) -> Result<(), String> {
    let nd = node_dir(d, n);
    let (rpc_port, mgmt_port) = ports(n);
    let disable = profile.disabled_plugins();
    let o = JoinOpts {
        data_dir: nd.clone(),
        seed: format!("127.0.0.1:{}", ports(1).0),
        master: read_master(&node_dir(d, 1))?,
        addr: "127.0.0.1".into(),
        rpc_port,
        mgmt_port,
        mem: PoolConfig::dev_server(),
        failpoints_api: true,
        debug_ops: true,
        host_id: Some(format!("host-{n}")),
        plugins: registry::init_plugin_list(&disable)?,
        disable,
        targets: vec![dev_target(&nd)],
        meta_auto: true,
        transports: registry::client_transports()?,
    };
    join_node(&o).map(|_| ()).map_err(|e| format!("node{n} 加入集群失败：{}（{}）", e.msg, e.code))
}

/// 用全部开发节点的管理地址问（409 跟随 leader，连不上换下一个）。
fn api(d: &Path) -> Result<ApiClient, String> {
    let addrs: Vec<String> = nodes(d).iter().filter_map(|n| mgmt_addr(d, *n).ok()).collect();
    Ok(ApiClient::new(addrs, admin_key(d, 1)?))
}

/// 等元数据成员齐了（问 leader 的 `GET /api/v1/cluster`；还在选举时 503，接着等）：`count` 个节点的集群要有 3 个 voter（2 个节点保持
/// 1 voter + 1 learner，3.4.1.9）、不在 joint 里，而且指定为元数据成员的节点都已是那个身份、都追上了（节点记录也跟上了：之后取的集群
/// 警告不会还说"只有 1 个 voter"）。
pub fn wait_members(d: &Path, count: u32) -> Result<String, String> {
    let want = if count >= 3 { 3 } else { 1 };
    let api = api(d)?;
    let deadline = clock::mono_ms() + MEMBERS_TIMEOUT_MS;
    loop {
        let settled = |r: &ClusterReply| {
            let mut members = r.nodes.iter().filter(|n| n.member_wanted != "none");
            let members_ok = members.all(|n| n.member == n.member_wanted && n.caught_up);
            r.voters.len() >= want && r.voters_outgoing.is_empty() && members_ok
        };
        let last = match api.get::<ClusterReply>("/api/v1/cluster") {
            Ok(r) if settled(&r) => {
                let l = &r.learners;
                return Ok(format!(
                    "元数据：{} 个 voter {:?}，learner {l:?}，leader node{}",
                    r.voters.len(),
                    r.voters,
                    r.leader
                ));
            }
            Ok(r) => format!("voter {:?}，learner {:?}", r.voters, r.learners),
            Err(e) => e.to_string(),
        };
        if clock::mono_ms() > deadline {
            return Err(format!(
                "{} s 内元数据成员没有到齐（要 {want} 个 voter，元数据成员都追上；现在：{last}）",
                MEMBERS_TIMEOUT_MS / 1000
            ));
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// `wmfs dev start n` 的最后一步（实现注记（M8 审查））：有就绪的 leader、这个节点是元数据成员时，等到 leader 看到它追上（最多 30 s），
/// 于是演示里"该节点回来当 follower 并追平"在命令返回时就成立。问不到 leader（例如几个节点一起起来、还在选举）就不等。
pub fn wait_caught_up(d: &Path, n: u32) -> Result<(), String> {
    let api = api(d)?;
    let deadline = clock::mono_ms() + CATCH_UP_TIMEOUT_MS;
    loop {
        let Ok(r) = api.get::<ClusterReply>("/api/v1/cluster") else { return Ok(()) };
        let me = r.nodes.iter().find(|x| x.node_id == n);
        let Some(me) = me.filter(|x| x.member != "none" && !x.caught_up) else { return Ok(()) };
        if clock::mono_ms() > deadline {
            let (m, c) = (me.matched, r.commit);
            return Err(format!(
                "node{n} 起来了，但 30 s 内没有追上（leader 看到的 matched {m}，commit {c}；见 wmfs dev logs {n}）"
            ));
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// `wmfs dev leader`：就绪 leader 的开发节点号（只打印数字，演示里 `$(…)` 直接用）。
pub fn leader(d: &Path) -> Result<Vec<String>, String> {
    let found = nodes(d).into_iter().find(|n| health(d, *n).is_ok_and(|h| h.meta_role == "leader"));
    found
        .map(|n| vec![n.to_string()])
        .ok_or_else(|| "现在没有就绪的元数据 leader（可以先 wmfs dev wait leader）".into())
}

/// `wmfs dev restart-all`：按各节点当前的 node.toml 依次重启全部节点——先 follower、最后 leader（SIGTERM 时它先让位），每个都等它追上；
/// 开始时集群是好的（都在运行、有就绪的 leader）就每重启一个都等链全部恢复 Serving（`dev wait chains-serving`）再重启下一个，不然冗余
/// 会一路降到 1 份。停着的节点直接起（`cluster plugins add|remove` 之后节点都因清单不一致退出了：改好各节点的 node.toml 再
/// restart-all）。最后等就绪的 leader 与元数据成员到齐，并确认没有节点起来之后又退出（例如 node.toml 还与集群插件清单不一致：退出码 3）。
pub fn restart_all(d: &Path) -> Result<Vec<String>, String> {
    let all = nodes(d);
    let leader = all.iter().copied().find(|n| health(d, *n).is_ok_and(|h| h.meta_role == "leader"));
    let healthy = leader.is_some() && all.iter().all(|n| running_pid(d, *n).is_some());
    let mut order: Vec<u32> = all.iter().copied().filter(|n| Some(*n) != leader).collect();
    order.extend(leader);
    let mut lines = Vec::new();
    for n in order {
        stop(d, n)?;
        lines.push(start(d, n)?);
        if healthy {
            lines.extend(super::wait::wait(d, &["chains-serving".to_string()], 120)?.pop());
        }
    }
    let count = all.len() as u32;
    lines.extend(super::wait::wait(d, &["leader".to_string()], 60)?);
    if count > 1 {
        lines.push(wait_members(d, count)?);
    }
    let deadline = clock::mono_ms() + 1500; // 清单不一致的节点在启动后约 1.2 s 退出（3.7.2：1 s 后退出）
    while clock::mono_ms() < deadline {
        if let Some(n) = all.iter().find(|n| running_pid(d, **n).is_none()) {
            return Err(format!("node{n} 起来之后又退出了（插件配置与集群插件清单不一致？看 wmfs dev logs {n}）"));
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    Ok(lines)
}
