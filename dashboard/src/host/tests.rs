//! 宿主的单元测试：路由的前缀匹配、manifest 校验（7 个必填字段、abi_version、钩子、fail point 声明）、调用包装与每个插件的 fail point
//! （`plugin.<名字>.<钩子>` 触发 = 这次不调用插件、以注入的码失败、计为插件错误；清掉之后照常）。

use super::*;

/// 编译期检查（design.md 0.2 第 3 条）：两个进程外钩子 trait 必须对象安全（宿主持有 `Box<dyn …>`）。
#[allow(dead_code)]
fn mgr_hook_traits_are_object_safe(_: &dyn EventSink, _: &dyn MgmtExt) {}

fn get(path: &str) -> HttpIn {
    HttpIn { method: "GET".into(), path: path.into(), query: String::new(), body: vec![] }
}

#[test]
fn router_matches_the_longest_prefix() {
    let mut r = Router::default();
    r.add("/", Box::new(|_| HttpOut::text(200, "home".into())));
    r.add("/dash", Box::new(|_| HttpOut::text(200, "dash".into())));
    r.add("/dash/each", Box::new(|_| HttpOut::text(200, "each".into())));
    let body = |p: &str| String::from_utf8(r.route(&get(p)).body).unwrap();
    assert_eq!(
        (body("/"), body("/dash/api"), body("/dash/each"), body("/dash")),
        ("home".into(), "dash".into(), "each".into(), "dash".into())
    );
    assert_eq!(r.route(&get("/nope")).status, 404, "/ 只匹配首页");
    assert_eq!(r.route(&get("/dashboard")).status, 404, "前缀按路径段匹配");
}

#[test]
fn manifests_are_checked() {
    let good = "name = \"x\"\nversion = \"1\"\nhooks = [\"event_sink\"]\naltitude = 0\nhot_path = false\nabi_version = 1\n\
                fail_points = [\"plugin.x.event_sink\"]\n[config_schema]\n";
    assert_eq!(check_manifest(good, "event_sink").unwrap(), "x");
    assert!(check_manifest(good, "mgmt_ext").unwrap_err().contains("hooks 里没有 mgmt_ext"));
    let v2 = good.replace("abi_version = 1", "abi_version = 2");
    assert!(check_manifest(&v2, "event_sink").unwrap_err().contains("abi_version 与 mgr 的 1 不一致"));
    for (from, to) in [
        ("version = \"1\"\n", ""),
        ("hot_path = false", "hot_path = true"),
        ("altitude = 0", "altitude = 5"),
        ("plugin.x.event_sink", "plugin.y.x"),
    ] {
        assert!(check_manifest(&good.replace(from, to), "event_sink").is_err(), "{from} → {to}");
    }
    assert!(check_manifest("name = ", "event_sink").is_err());
}

#[test]
fn every_plugin_has_a_fail_point_around_its_calls() {
    let pairs = [
        ("audit-log", "event_sink"),
        ("webhook", "event_sink"),
        ("alert", "event_sink"),
        ("alert", "mgmt_ext"),
        ("dashboard", "mgmt_ext"),
        ("prometheus-exporter", "mgmt_ext"),
    ];
    for (name, hook) in pairs {
        let h = Hooked::new(name, hook);
        let errs = || metrics::counter("wmfs_hook_errors_total", "", &[("hook", hook), ("plugin", name)]).get();
        let fp = format!("plugin.{name}.{hook}");
        wmfs_api::failpoint::set(&fp, "times(1,return(EIO))").unwrap();
        let called = std::cell::Cell::new(false);
        let f = || {
            called.set(true);
            Ok::<(), String>(())
        };
        let e = h.call(f).unwrap_err();
        assert!(!called.get() && e.contains(&fp) && e.contains("EIO"), "{fp}：不调用插件，以注入的码失败：{e}");
        assert_eq!(errs(), 1, "{fp}：计为插件错误");
        h.call(f).unwrap();
        assert!(called.get(), "times(1, …) 只触发一次");
        assert!(h.call(|| Err::<(), String>("坏了".into())).is_err());
        assert_eq!(errs(), 2, "插件自己出错也计数");
        wmfs_api::failpoint::clear(&fp);
    }
}
