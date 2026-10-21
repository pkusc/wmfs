//! 断开重连（roadmap M2 验收标准"断开重连测试通过"；同一套用例跑 tcp、mem 与 rdma——rdma 需要设备）：fail point
//! `plugin.<scheme>.transport=times(1,return(EIO))` 让下一次发送失败 = 这条连接作废（3.7.4），调用方按 3.3.8 的
//! "连接断开 → 重连重试"换一条新连接后成功。rdma 另有一份不需要设备、常跑的：假网卡上的 core 全套
//! （plugins/rdma/tests/stack.rs 的 `rdma_stack_failpoint_breaks_the_connection_and_retry_reconnects`）。
//!
//! fail point 是全进程的：这个文件里默认只跑一个用例（tcp 与 mem 依次跑；rdma 的那个 `#[ignore]`，只在 `--ignored` 时跑）。
//! 先建好连接、再设置 fail point，于是下一次发送一定是客户端的这个请求（服务端此时没有要发的帧）。

use std::time::Duration;

use wmfs_core::api::{failpoint, metrics};
use wmfs_core::base::ErrCode;
use wmfs_core::base::types::Role;
use wmfs_core::net::client::{Family, check_reply};
use wmfs_core::net::retry::retry;
use wmfs_core::proto::frame::MSG_PING;
use wmfs_testkit::harness::net::*;

fn reconnect_case(s: &TestServer) {
    let c = s.client(key(2), Role::Client);
    let ping = |attempts: &mut u32| {
        retry("ping", Duration::from_secs(30), |n| {
            *attempts = n;
            c.call(&s.ep, Family::Data, frame(MSG_PING, b"x"), T).and_then(check_reply)
        })
    };
    let mut n = 0;
    ping(&mut n).unwrap(); // 先把连接建好
    let name = format!("plugin.{}.transport", s.scheme);
    let hits = || metrics::counter("wmfs_failpoint_hits_total", "", &[("name", name.as_str())]).get();
    let before = hits();
    failpoint::set(&name, "times(1,return(EIO))").unwrap();
    let r = ping(&mut n);
    failpoint::clear(&name);
    assert!(r.is_ok(), "{}：重连重试之后成功：{:?}", s.scheme, r.err().unwrap_or(ErrCode(0)));
    assert!(n >= 1, "{}：第一次发送失败（连接作废），重连后的第 {} 次才成功", s.scheme, n + 1);
    assert_eq!(hits(), before + 1, "{}：fail point 正好触发一次", s.scheme);
}

#[test]
fn broken_connections_are_reestablished_and_retried() {
    for s in [tcp(), mem("reconnect")] {
        reconnect_case(&s);
    }
}

#[test]
#[ignore = "需要 RDMA 设备"]
fn rdma_broken_connections_are_reestablished_and_retried() {
    if let Some(s) = rdma() {
        reconnect_case(&s);
    }
}
