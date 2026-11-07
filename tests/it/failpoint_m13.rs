//! M13 新插件的 fail point（design.md 3.11、9.4：每个插件至少一个 fail point 用例），在一个装了 mclock 与全部后台任务的完整节点上：
//! - `plugin.mclock.qos`（core 包在入队外层）：这个请求以注入的码失败、计入 `wmfs_hook_errors_total{hook="qos",plugin="mclock"}`；清掉之后照常；
//! - `plugin.replica.codec` 在**客户端**的读上（DataClient 的 `read_order`，4.7 的 M13 注记）：codec 出错 → 退回"同主机优先、其余按链序"，
//!   读照样成功；
//! - `plugin.tiering.bg_task`、`plugin.rebalance.bg_task`（框架包在每一步外层）：这一步失败 → 计入 `wmfs_task_errors_total`，稍后从同一个
//!   游标重试——分层照样做完、rebalance 的这一轮照样结束。
//!
//! `plugin.cache-local-nvme.cache_store` 在客户端挂载进程里，用例在 `cli/tests/failpoint_fuse.rs`。fail point 是进程级的：单独一个测试程序，
//! 各段按顺序在同一个测试里跑。

use std::sync::Arc;
use std::time::Duration;

use wmfs_core::api::types::ClassReply;
use wmfs_core::api::{failpoint, metrics};
use wmfs_core::base::ErrCode;
use wmfs_core::buf::Lane;
use wmfs_core::client::data_client::DataClient;
use wmfs_core::meta::schema::keys::{self, ROOT_INO};
use wmfs_core::meta::schema::records::{InodeV1, decode_v1};
use wmfs_core::proto::meta::{MetaOp, O_RDWR};
use wmfs_plugins::replica::Replica;
use wmfs_testkit::harness::bg::{api, mkdir, set, task, tasks, text};
use wmfs_testkit::harness::meta::get_raw;
use wmfs_testkit::harness::node::{
    TempDir, chunk_of, client, init, opened, put, set_full_bg, set_mclock, start, wait_for, who, write_chunks,
};

fn hook_errors(hook: &str, plugin: &str) -> u64 {
    metrics::counter("wmfs_hook_errors_total", "", &[("hook", hook), ("plugin", plugin)]).get()
}

fn task_errors(task: &str) -> u64 {
    metrics::counter("wmfs_task_errors_total", "", &[("task", task)]).get()
}

#[test]
fn m13_plugin_fail_points() {
    set_full_bg();
    set_mclock();
    let d = TempDir::new("fp-m13");
    init(&d.0);
    let run = start(&d.0);
    let (a, c, me) = (api(&d.0, &run.mgmt_addr), client(&d.0, &run, "admin"), who(1000, 1000));
    let f = put(&c, &me, ROOT_INO, "f", &text(1, 300_000)).unwrap();
    let o = opened(c.meta.read(&me, MetaOp::Open { ino: f.attr.ino, flags: O_RDWR })).unwrap();

    // mclock：入队以注入的码失败
    let before = hook_errors("qos", "mclock");
    failpoint::set("plugin.mclock.qos", "return(EPERM)").unwrap();
    assert_eq!(write_chunks(&c, &o, 0, &[6; 10]), Err(ErrCode::EPERM), "plugin.mclock.qos：以注入的码失败");
    failpoint::clear("plugin.mclock.qos");
    assert!(hook_errors("qos", "mclock") > before, "钩子包装把它计为插件错误");
    assert!(write_chunks(&c, &o, 0, &[6; 10]).is_ok(), "清掉之后照常");

    // 客户端的 read_order 出错：退回链序，读照样成功
    let dc = DataClient::for_meta(c.rpc.clone(), c.meta.clone(), Duration::from_secs(10), Duration::from_secs(30));
    let dc = dc.with_codec(Arc::new(Replica::new())).with_host("host-9".into());
    let l = o.layout.as_ref().unwrap();
    let before = hook_errors("codec", "replica");
    failpoint::set("plugin.replica.codec", "return(EPERM)").unwrap();
    let into = c.rpc.pool().get(1 << 20, Lane::Normal, Duration::from_secs(5)).unwrap();
    let r = dc.read(&o.cap, l, chunk_of(&o, 0), 0, 1000, into, None).map(|(b, n)| b.as_slice()[..n as usize].to_vec());
    failpoint::clear("plugin.replica.codec");
    let got = r.unwrap();
    assert_eq!((got.len(), &got[..10]), (1000, &[6u8; 10][..]), "codec 出错：读照样成功");
    assert!(hook_errors("codec", "replica") > before, "read_order 确实出错了（走的是退回的那条路）");

    // tiering：一步失败 → 计入错误、稍后从同一个游标重试，分层照样做完
    let hot = a.send_raw::<ClassReply>("POST", "/api/v1/classes?name=hot", b"[redundancy]\nreplica = 1\n").unwrap();
    let cold = a.send_raw::<ClassReply>("POST", "/api/v1/classes?name=cold", b"[redundancy]\nreplica = 1\n").unwrap();
    let t = mkdir(&c, ROOT_INO, "t");
    set(&a, "/t", "class=hot").unwrap();
    let big = put(&c, &me, t, "big", &text(2, 300_000)).unwrap().attr.ino;
    let class_of = |ino: u64| -> u16 {
        let i: InodeV1 = decode_v1(&get_raw(&run.meta, &keys::inode(ino)).unwrap().unwrap()).unwrap();
        i.class_id
    };
    assert_eq!(class_of(big), hot.class.id);
    set(&a, "/t", "tier=hot->cold:size>100K&age>1s").unwrap();
    let e0 = task_errors("tiering");
    failpoint::set("plugin.tiering.bg_task", "times(1,return(EIO))").unwrap();
    let moved = wait_for(60, || {
        task(&a, "tiering", "run", None);
        wait_for(3, || class_of(big) == cold.class.id)
    });
    assert!(moved, "失败的一步之后照样分层");
    assert!(task_errors("tiering") > e0, "失败计入了 wmfs_task_errors_total");

    // rebalance：一步失败 → 计入错误，这一轮照样结束（单节点一个 target：没有可平衡的，只走一遍）
    let pass = || tasks(&a).into_iter().find(|t| t.kind == "rebalance").map_or(0, |t| t.pass);
    let (e0, p0) = (task_errors("rebalance"), pass());
    failpoint::set("plugin.rebalance.bg_task", "times(1,return(EIO))").unwrap();
    task(&a, "rebalance", "run", None);
    assert!(wait_for(60, || task_errors("rebalance") > e0 && pass() > p0), "失败计入了错误，这一轮照样结束");
    failpoint::clear("plugin.tiering.bg_task");
    failpoint::clear("plugin.rebalance.bg_task");
    c.meta.close();
    run.stop();
}
