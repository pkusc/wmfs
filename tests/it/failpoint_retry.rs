//! fail point 用例（roadmap M1 验收标准）：客户端注入 30% 发送失败（`plugin.tcp.transport=pct(30,return(EIO))`，
//! 每次失败 = 这条连接作废），靠"连接断开 → 重连重试"（net/retry.rs，带上限与退避）全部成功。
//!
//! fail point 是全进程的，而这里只想让**客户端**出错（与演示命令一样：服务端是另一个进程）：所以测试把自己再启动一次
//! 当客户端子进程（只跑 `child_pings`，带环境变量 `WMFS_FAILPOINTS`），服务端留在父进程里。

use std::process::Command;
use std::time::Duration;

use wmfs_core::base::types::Role;
use wmfs_core::net::client::{Endpoint, Family, check_reply};
use wmfs_core::net::retry::retry;
use wmfs_core::proto::frame::MSG_PING;
use wmfs_testkit::harness::net::*;

const CHILD: &str = "WMFS_TEST_CHILD_ADDR";

#[test]
fn client_side_transport_failures_are_retried_until_all_succeed() {
    if std::env::var(CHILD).is_ok() {
        return; // 子进程里只跑 child_pings
    }
    let s = tcp();
    let out = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "child_pings", "--nocapture", "--test-threads", "1"])
        .env("WMFS_FAILPOINTS", "plugin.tcp.transport=pct(30,return(EIO))")
        .env(CHILD, &s.addr)
        .output()
        .unwrap();
    let text = format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    assert!(out.status.success(), "子进程失败：\n{text}");
    assert!(text.contains("100/100 ok"), "{text}");
    assert!(text.contains("连接断开 → 重连重试"), "日志里应有重连重试：\n{text}");
}

/// 客户端子进程：100 次 ping，每次都按 3.3.8 重试（总期限 60 s）。父进程里直接返回。
#[test]
fn child_pings() {
    let Ok(addr) = std::env::var(CHILD) else { return };
    let ep = Endpoint { node: 1, addrs: vec![("tcp".into(), addr)] };
    let c = client("tcp", key(2), Role::Client); // 连的是父进程里的服务端
    let mut ok = 0;
    for i in 0..100u32 {
        let r = retry("ping", Duration::from_secs(60), |_| {
            c.call(&ep, Family::Data, frame(MSG_PING, &i.to_le_bytes()), T).and_then(check_reply)
        });
        ok += r.is_ok() as u32;
    }
    println!("{ok}/100 ok");
    assert_eq!(ok, 100);
}
