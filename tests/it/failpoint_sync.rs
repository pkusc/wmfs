//! `sync.fetch.midway`（design.md 9.4、3.4.2.8）：重同步取回第一个 chunk 之后失败 → 这一轮作废、下一轮重跑（只拷有差异的），最后照样
//! 交 SyncDone，变 Serving 之后各成员一致。

use std::time::{Duration, Instant};

use wmfs_core::api::{failpoint, metrics};
use wmfs_core::base::lock;
use wmfs_core::meta::cluster::chains::{SERVING, SYNCING};
use wmfs_testkit::harness::craq::{CHUNK, Craq, chain};

const S: u8 = SERVING;

#[test]
fn a_sync_that_fails_midway_is_rerun_and_converges() {
    let c = Craq::start(3, &[(1, S), (2, S)]);
    for i in 0..5u32 {
        c.write(i, 0, &vec![i as u8 + 1; CHUNK as usize]).unwrap();
    }
    failpoint::set("sync.fetch.midway", "times(1,return(EIO))").unwrap();
    c.set_chains(&[(1, chain(&[(1, S), (2, S), (3, SYNCING)], 2, 1))]);
    let t0 = Instant::now();
    while !lock(&c.members[2].node.sync_done).contains(&(1, 3, 2)) {
        assert!(t0.elapsed() < Duration::from_secs(20), "20 s 内没有同步完");
        std::thread::sleep(Duration::from_millis(20));
    }
    let hits = metrics::counter("wmfs_failpoint_hits_total", "fail point 触发次数", &[("name", "sync.fetch.midway")]);
    assert_eq!(hits.get(), 1, "第一轮在取回第一个 chunk 之后失败，第二轮做完");
    c.set_chains(&[(1, chain(&[(1, S), (2, S), (3, S)], 3, 1))]);
    assert_eq!(c.check(Duration::from_secs(5)).len(), 5);
}
