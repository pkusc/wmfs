//! fail point 用例：`plugin.shared-key.authn`（core 包在 authn 调用外层，触发 = 这次认证被拒）。
//! fail point 是全进程的，所以单独一个测试进程。

use wmfs_core::api::failpoint;
use wmfs_core::base::ErrCode;
use wmfs_core::base::types::Role;
use wmfs_core::net::client::{Family, check_reply};
use wmfs_core::proto::frame::MSG_PING;
use wmfs_testkit::harness::net::*;

#[test]
fn authn_fail_point_rejects_the_handshake() {
    let s = tcp();
    let ping =
        || s.client(key(2), Role::Client).call(&s.ep, Family::Data, frame(MSG_PING, b""), T).and_then(check_reply);
    failpoint::set("plugin.shared-key.authn", "return(EIO)").unwrap();
    assert_eq!(ping().err(), Some(ErrCode::AUTH_FAILED), "fail point 触发 = 认证被拒，握手失败");
    failpoint::clear("plugin.shared-key.authn");
    assert!(ping().is_ok(), "清除后正常");
    let errs =
        wmfs_core::api::metrics::counter("wmfs_hook_errors_total", "", &[("hook", "authn"), ("plugin", "shared-key")]);
    assert!(errs.get() >= 1, "fail point 触发计为一次钩子错误");
}
