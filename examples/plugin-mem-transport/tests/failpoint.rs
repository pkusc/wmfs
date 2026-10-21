//! fail point 用例：`plugin.mem.transport`（core 包在写线程的 `send` 外层，触发 = 这条连接作废）。
//! 连接作废之后，按 3.3.8 重试（core 的 `net::retry`）会重连并成功。fail point 是全进程的，所以单独一个测试进程。

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{AcceptAll, client, key, ping, server};
use plugin_mem_transport::MemTransport;
use wmfs_core::api::failpoint;
use wmfs_core::base::types::Role;
use wmfs_core::net::retry::retry;

#[test]
fn transport_fail_point_breaks_the_connection_and_retry_recovers() {
    // 这里只测传输的 fail point：认证一律接受（证明的校验在 tests/mem.rs 里测）
    let (_rpc, ep) = server(MemTransport::listening("mem-fp"), Arc::new(AcceptAll));
    let c = client(key(2), Role::Client);
    // 不论配置的是哪个错误码（包括 FEATURE_UNSUPPORTED），fail point 触发都让连接作废、走重连重试（3.7.4）
    for action in ["times(5,return(EIO))", "times(3,return(FEATURE_UNSUPPORTED))"] {
        failpoint::set("plugin.mem.transport", action).unwrap();
        let mut retries = 0;
        for _ in 0..20 {
            let r = retry("mem ping", Duration::from_secs(30), |n| {
                retries += (n > 0) as u32;
                ping(&c, &ep, b"")
            });
            assert!(r.is_ok(), "{action}：{:?}", r.err());
        }
        failpoint::clear("plugin.mem.transport");
        assert!(retries >= 1, "{action}：前几次发送失败，至少要重连重试一次");
    }
}
