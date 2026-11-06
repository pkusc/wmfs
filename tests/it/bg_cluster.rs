//! 后台任务在多节点集群上（design.md 3.10、4.12、3.2.5 R5；roadmap M11）：同一个进程里的多节点集群（[`Cluster`]，装上协调器、scrub 与
//! gc 的本地孤儿扫描），经管理 API：
//! - 换 leader 之后协调器从持久化的游标接着走：游标之前已经转换过的文件不再看；
//! - `wmfs task run scrub` 走 scrub 插件的第 2 段：非 L 的 Serving 成员上缺了的 chunk 从别的副本补上，changelog `ChecksumRepaired`
//!   （extra 的字段名与次序是金样）；
//! - 读到坏块之后不必等 scrub：数据节点马上把这个副本修好（ADR-0042 第 7 条）；
//! - 只做数据的节点（第 4 个）收不到任务控制（`controlled = false`），`task run scrub` 之后 task-idle 的判定照样成立（ADR-0042 第 12 条）。
//!
//! 不用 fail point（进程级），这些用例可以在同一个进程里并行跑。

use std::path::{Path, PathBuf};

use wmfs_core::api::types::{NodeInfoReply, STATE_DONE, TaskInfo, TaskSetReq};
use wmfs_core::base::types::ChunkId;
use wmfs_core::bg::progress::new_rec;
use wmfs_core::buf::{Lane, POOL_WAIT};
use wmfs_core::client::data_client::chunk_id;
use wmfs_core::data::format::{CMP_LZ4, CMP_NONE};
use wmfs_core::meta::cluster::chains::{SERVING, TARGET_UP};
use wmfs_core::meta::raft::membership::Members;
use wmfs_core::meta::schema::keys::{self, ROOT_INO};
use wmfs_core::meta::schema::records::{ChainRecV1, InodeV1, TargetRecV1, TaskRecV1, decode_v1, encode_v1};
use wmfs_core::proto::meta::OpenOut;
use wmfs_mgr::api::ApiClient;
use wmfs_plugins::local_fs::committed_path;
use wmfs_testkit::harness::bg::{api_at, attr, extra_keys, has_event, mkdir, set, status, task, tasks, text};
use wmfs_testkit::harness::cluster::Cluster;
use wmfs_testkit::harness::meta::{get_raw, put_raw};
use wmfs_testkit::harness::node::{get, put, wait_for, who};

/// 前 `n` 个节点的管理地址。
fn mgmt(c: &Cluster, n: usize) -> Vec<String> {
    (1..=n).map(|i| c.run(i).mgmt_addr.clone()).collect()
}

fn admin(c: &Cluster, addrs: &[String]) -> ApiClient {
    api_at(&c.node_dir(1), &addrs.join(","), "admin")
}

/// 节点 `addr` 自己的 `GET /api/v1/node`。
fn node_info(c: &Cluster, addr: &str) -> NodeInfoReply {
    admin(c, &[addr.to_string()]).get("/api/v1/node").unwrap()
}

/// 每个节点上的任务 `kind` 都满足 `f`。
fn on_every_node(c: &Cluster, addrs: &[String], kind: &str, f: impl Fn(&NodeInfoReply, &TaskInfo) -> bool) -> bool {
    addrs.iter().all(|a| {
        let n = node_info(c, a);
        n.tasks.iter().any(|t| t.kind == kind && f(&n, t))
    })
}

/// 就绪 leader 上的一个键（线性一致读）。
fn raw(c: &Cluster, key: &[u8]) -> Option<Vec<u8>> {
    get_raw(&c.run(c.wait_leader()).meta, key).ok().flatten()
}

fn target_rec(c: &Cluster, t: u32) -> Option<TargetRecV1> {
    raw(c, &keys::target(t)).and_then(|b| decode_v1(&b).ok())
}

/// 等 `n` 个 target 都报过心跳（之后新建的链才有 `n` 个成员）。
fn wait_targets(c: &Cluster, n: u32) {
    let up = || (1..=n).all(|t| target_rec(c, t).is_some_and(|r| r.state == TARGET_UP));
    assert!(wait_for(60, up), "{n} 个 target 都报过心跳");
}

/// 文件 `o` 的 chunk `idx` 与它的链此刻的成员（链序）。
fn chunk_of(c: &Cluster, o: &OpenOut, idx: u32) -> (ChunkId, Vec<(u32, u8)>) {
    let id = chunk_id(o.layout.as_ref().unwrap(), o.attr.ino, idx).unwrap();
    let chain: ChainRecV1 = decode_v1(&raw(c, &keys::chain(id.chain)).unwrap()).unwrap();
    (id, chain.members)
}

/// 等文件 `o` 的 chunk `idx` 的链有 `n` 个 Serving 成员，返回 chunk 与成员。
fn serving_chunk(c: &Cluster, o: &OpenOut, idx: u32, n: usize) -> (ChunkId, Vec<u32>) {
    let ok = || chunk_of(c, o, idx).1.iter().filter(|(_, s)| *s == SERVING).count() == n;
    assert!(wait_for(60, ok), "链有 {n} 个 Serving 成员：{:?}", chunk_of(c, o, idx).1);
    let (id, m) = chunk_of(c, o, idx);
    (id, m.into_iter().map(|(t, _)| t).collect())
}

/// target `t` 上 `id` 的已提交文件，与 `t` 所在的节点号。
fn replica(c: &Cluster, t: u32, id: &ChunkId) -> (PathBuf, u32) {
    let r = target_rec(c, t).unwrap();
    (committed_path(Path::new(&r.path), id), r.node)
}

/// 换 leader 之后协调器从持久化的游标接着走（3.10"断点续跑"）：把游标之前一个早就转换过的文件改回与声明不同，新 leader 这一轮不碰它
/// ——游标丢了、从头再走的话才会碰到。子树要超过一批（256 个目录项），DFS 游标才越过第一批；协调器限到每秒 10 个对象，第一批持久化之后、
/// 这一项走完之前有十几秒可以换 leader。
#[test]
fn a_new_leader_resumes_the_reconciler_from_the_persisted_cursor() {
    wmfs_testkit::harness::node::set_full_bg();
    let c = Cluster::start("bg-cursor", 3);
    let (cl, me) = (c.client("admin"), who(1000, 1000));
    let a = admin(&c, &mgmt(&c, 3));
    let p = mkdir(&cl, ROOT_INO, "p");
    let inos: Vec<u64> = (0..400).map(|i| put(&cl, &me, p, &format!("f{i:03}"), b"x").unwrap().attr.ino).collect();
    task(&a, "reconciler", "set", Some(TaskSetReq { bw: None, items: Some(10) }));
    let task_rec = || raw(&c, &keys::task("reconciler", "")).and_then(|b| decode_v1::<TaskRecV1>(&b).ok());
    let persisted = || task_rec().unwrap_or_else(new_rec).done;
    let done0 = persisted();
    set(&a, "/p", "compress=lz4").unwrap();
    assert!(wait_for(90, || persisted() >= done0 + 256), "第一批（256 个目录项）转换完、持久化了：{}", persisted());
    let victim = inos[10];
    let inode = || decode_v1::<InodeV1>(&raw(&c, &keys::inode(victim)).unwrap()).unwrap();
    let mut i = inode();
    assert_eq!(i.layout.as_ref().unwrap().compress, CMP_LZ4, "第一批里的文件早就转换过了");
    i.layout.as_mut().unwrap().compress = CMP_NONE; // 改回与声明不同（绕过协调器直接改 inode）
    let old = c.wait_leader();
    put_raw(&c.run(old).meta, keys::inode(victim), encode_v1(&i)).unwrap();
    let new = (1..=3).find(|n| *n != old).unwrap();
    c.run(old).meta.transfer_leader(new as u32);
    assert!(wait_for(20, || c.leader() == Some(new)), "换到 node{new}");
    let row = || attr(&status(&a, "/p"), "compress").actual.mismatched_files;
    assert!(wait_for(120, || row() == Some(1)), "新 leader 把剩下的转换完：还剩 {:?}", row());
    let idle = || tasks(&a).iter().any(|t| t.kind == "reconciler" && t.backlog == 0 && t.state == STATE_DONE);
    assert!(wait_for(60, idle), "这一项走完、队列空了");
    assert_eq!(inode().layout.unwrap().compress, CMP_NONE, "新 leader 从持久化的游标接着走：游标之前的不再看");
    assert_eq!(row(), Some(1));
}

/// `wmfs task run scrub` 走 scrub 插件的第 2 段（ADR-0040 第 10 条 ③ 的任务路径）：非 L 的 Serving 成员（这里是 head）上缺了一个 chunk
/// （像掉电丢了它）→ 这一轮从 L 的清单里找出来、从别的副本补上，changelog `ChecksumRepaired`。
#[test]
fn task_run_scrub_fills_in_a_chunk_a_serving_member_is_missing() {
    wmfs_testkit::harness::node::set_full_bg();
    let c = Cluster::start("bg-scrub2", 3);
    let (cl, me) = (c.client("admin"), who(1000, 1000));
    let addrs = mgmt(&c, 3);
    let a = admin(&c, &addrs);
    wait_targets(&c, 3);
    let data = text(1, 300_000);
    let o = put(&cl, &me, ROOT_INO, "f", &data).unwrap();
    let (id, members) = serving_chunk(&c, &o, 0, 3);
    let (path, _) = replica(&c, members[0], &id);
    std::fs::remove_file(&path).unwrap();
    let r0 = task(&a, "scrub", "run", None).runs;
    let idle = |_: &NodeInfoReply, t: &TaskInfo| t.runs >= r0 && t.state == STATE_DONE && t.backlog == 0;
    assert!(wait_for(60, || on_every_node(&c, &addrs, "scrub", idle)), "每个节点的 scrub 都照办了运行请求、做完了");
    assert!(path.exists(), "head 上缺的 chunk 从别的副本补上了");
    let t = members[0].to_string();
    assert!(has_event(&a, "ChecksumRepaired", Some(("target", t.as_str()))), "changelog 记了 ChecksumRepaired");
    let keys = extra_keys(&a, "ChecksumRepaired", Some(("target", t.as_str())));
    assert_eq!(keys, "chain,epoch,idx,target", "金样：extra 的字段名与次序（M11 审查，ADR-0042 第 2 条）");
    assert!(get(&cl, &me, o.attr.ino).unwrap() == data);
}

/// 读到坏块之后不必等 scrub（ADR-0042 第 7 条）：每个节点的 scrub 早就做完了第一轮（下一轮在 24 h 之后），一个副本的数据块坏了 → 优先读
/// 这个副本的客户端照旧换下一个副本读到对的数据；数据节点马上把它交给 repair 线程从别的副本修好，changelog `ChecksumRepaired`。
#[test]
fn a_bad_replica_found_by_a_read_is_repaired_without_waiting_for_scrub() {
    wmfs_testkit::harness::node::set_full_bg();
    let c = Cluster::start("bg-readfix", 3);
    let (cl, me) = (c.client("admin"), who(1000, 1000));
    let addrs = mgmt(&c, 3);
    let a = admin(&c, &addrs);
    wait_targets(&c, 3);
    let data = text(2, 300_000);
    let o = put(&cl, &me, ROOT_INO, "f", &data).unwrap();
    let (id, members) = serving_chunk(&c, &o, 0, 3);
    let first_pass = |_: &NodeInfoReply, t: &TaskInfo| t.pass >= 1 && t.state == STATE_DONE;
    assert!(wait_for(60, || on_every_node(&c, &addrs, "scrub", first_pass)), "scrub 的第一轮都做完了");
    let (path, node) = replica(&c, members[1], &id);
    let mut b = std::fs::read(&path).unwrap();
    let data_at = u16::from_le_bytes([b[10], b[11]]) as usize * 4096; // header_len_4k：数据区从这里开始（3.2.4）
    b[data_at + 10] ^= 0x40; // 第 0 块里的一个字节（同 wmfs debug corrupt-chunk）
    std::fs::write(&path, &b).unwrap();
    let len = data.len().min(o.layout.as_ref().unwrap().chunk_size as usize);
    let into = cl.rpc.pool().get(len, Lane::Normal, POOL_WAIT).unwrap();
    let (got, n) = cl.data.read(&o.cap, o.layout.as_ref().unwrap(), id, 0, len as u32, into, Some(node)).unwrap();
    assert!(got.as_slice()[..n as usize] == data[..n as usize], "换到下一个副本读到了对的数据");
    let t = members[1].to_string();
    assert!(wait_for(30, || has_event(&a, "ChecksumRepaired", Some(("target", t.as_str())))), "读到坏块之后马上修好了");
    assert!(std::fs::read(&path).unwrap() != b, "坏了的那份已经换掉");
}

/// 只做数据的节点（第 4 个：不在元数据 raft 里，已应用的 KV 里没有任务的控制字段）：`GET /api/v1/node` 回 `controlled = false`；
/// `wmfs task run scrub` 到不了它（运行请求号一直是 0），`wmfs dev wait task-idle` 对它只看这一轮做完、积压 0——这个判定照样成立
/// （同 cli/src/dev/wait.rs 的 `nodes_idle`）。
#[test]
fn a_data_only_node_is_not_controlled_and_task_idle_still_holds() {
    wmfs_testkit::harness::node::set_full_bg();
    let c = Cluster::start("bg-dataonly", 4);
    let addrs = mgmt(&c, 4);
    let a = admin(&c, &addrs[..3]);
    let controlled = || (0..4).map(|i| node_info(&c, &addrs[i]).controlled).collect::<Vec<_>>();
    assert!(wait_for(30, || controlled() == [true, true, true, false]), "只有第 4 个不受控制：{:?}", controlled());
    let r0 = task(&a, "scrub", "run", None).runs;
    assert!(r0 >= 1);
    let idle =
        |n: &NodeInfoReply, t: &TaskInfo| t.state == STATE_DONE && t.backlog == 0 && (t.runs >= r0 || !n.controlled);
    assert!(wait_for(60, || on_every_node(&c, &addrs, "scrub", idle)), "task-idle 的判定成立");
    let n4 = node_info(&c, &addrs[3]);
    assert_eq!(n4.tasks.iter().find(|t| t.kind == "scrub").map(|t| t.runs), Some(0), "只做数据的节点收不到运行请求");
}
