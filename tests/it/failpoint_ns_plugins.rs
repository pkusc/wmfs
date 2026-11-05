//! 插件的 fail point（design.md 3.7.4、4 章开头"每个插件至少一个 fail point 用例"）：`plugin.posix-mode.authz` 触发 = 以注入
//! 的码拒绝这次请求；`plugin.label-match.placement` 触发 = 这次放置失败（create 得到注入的码，mkdir 不受影响）。清除后恢复；
//! 钩子包装把触发计为插件错误（`wmfs_hook_errors_total`）。fail point 是进程级的，所以单独一个测试程序。
//! M10：`plugin.posix-acl.authz`、`plugin.path-caps.authz` 触发 = 以注入的码拒绝（与 posix-mode 相同：任何一个 Deny 都拒绝）；
//! `plugin.quota.quota_admission` 触发 = 这次准入被拒绝（受配额管着的 create 得到注入的码，子树外的不受影响）。

use wmfs_core::api::{failpoint, metrics};
use wmfs_core::base::ErrCode;
use wmfs_core::meta::attr_ops;
use wmfs_core::meta::attr_registry::split;
use wmfs_core::meta::schema::keys::ROOT_INO;
use wmfs_testkit::harness::ns::{Ns, OWNER, ROOT};

fn errors(hook: &str, plugin: &str) -> u64 {
    metrics::counter("wmfs_hook_errors_total", "", &[("hook", hook), ("plugin", plugin)]).get()
}

#[test]
fn authz_and_placement_fail_points() {
    let mut ns = Ns::new();
    failpoint::set("plugin.posix-mode.authz", "times(1,return(EPERM))").unwrap();
    assert_eq!(ns.mkdir(&OWNER, ROOT_INO, "a"), Err(ErrCode::EPERM), "以注入的码拒绝");
    assert!(ns.mkdir(&OWNER, ROOT_INO, "a").is_ok(), "times(1, …) 只触发一次");
    assert_eq!(errors("authz", "posix-mode"), 1);

    failpoint::set("plugin.label-match.placement", "return(ENOSPC)").unwrap();
    assert_eq!(ns.create(&OWNER, ROOT_INO, "f"), Err(ErrCode::ENOSPC), "放置失败：文件建不出来");
    assert!(ns.mkdir(&OWNER, ROOT_INO, "d").is_ok(), "目录不选链，不受影响");
    assert!(ns.inode(19).is_some_and(|i| i.kind == 2), "失败的 create 整体丢弃（ino 19 给了后来的 mkdir）");
    failpoint::clear("plugin.label-match.placement");
    assert!(ns.create(&OWNER, ROOT_INO, "f").is_ok());
    assert_eq!(errors("placement", "label-match"), 1);
    m10_authz_and_quota(&mut ns); // 同一个测试里接着做：fail point 是进程级的，并行的测试会互相干扰
}

fn m10_authz_and_quota(ns: &mut Ns) {
    for (plugin, code) in [("posix-acl", ErrCode::EPERM), ("path-caps", ErrCode::EROFS)] {
        failpoint::set(&format!("plugin.{plugin}.authz"), &format!("times(1,return({}))", code.name().unwrap()))
            .unwrap();
        assert_eq!(ns.mkdir(&OWNER, ROOT_INO, &format!("x-{plugin}")), Err(code), "{plugin}：以注入的码拒绝");
        assert!(ns.mkdir(&OWNER, ROOT_INO, &format!("x-{plugin}")).is_ok(), "{plugin}：只触发一次");
        assert_eq!(errors("authz", plugin), 1);
    }
    let q = ns.mkdir(&OWNER, ROOT_INO, "q").unwrap();
    let items = split("quota=1G").unwrap();
    ns.txn(&ROOT, |v, c| attr_ops::set(v, c, "/q", &items)).unwrap();
    failpoint::set("plugin.quota.quota_admission", "return(EDQUOT)").unwrap();
    assert_eq!(ns.create(&OWNER, q, "f"), Err(ErrCode::EDQUOT), "受配额管着的 create 被拒绝");
    assert!(ns.create(&OWNER, ROOT_INO, "g").is_ok(), "子树外没有配额，不问插件");
    failpoint::clear("plugin.quota.quota_admission");
    assert!(ns.create(&OWNER, q, "f").is_ok());
    assert_eq!(errors("quota_admission", "quota"), 1);
}
