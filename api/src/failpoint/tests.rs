//! fail point 的单元测试（从 failpoint.rs 拆出来，免得那个文件超过 400 行）。
//!
//! 测试在同一进程里并发运行、共享全局配置，所以每个测试用自己独有的名字。

use super::*;

#[test]
fn unconfigured_is_noop() {
    assert_eq!(fail_point("test.none"), Ok(()));
    assert!(!fail_point_hit("test.none"));
    assert_eq!(fail_point_value("test.none"), None);
}

#[test]
fn return_code_by_name() {
    set("test.ret", "return(EIO)").unwrap();
    assert_eq!(fail_point("test.ret"), Err(5));
    assert!(fail_point_hit("test.ret"));
    assert_eq!(fail_point_value("test.ret"), Some(5));
    clear("test.ret");
    assert_eq!(fail_point("test.ret"), Ok(()));
}

#[test]
fn internal_const_matches_code_table() {
    assert_eq!(codes::from_name("INTERNAL"), Some(INTERNAL));
}

#[test]
fn times_then_off() {
    set("test.times", "times(2,return(BUSY))").unwrap();
    assert_eq!(fail_point("test.times"), Err(1007));
    assert_eq!(fail_point("test.times"), Err(1007));
    assert_eq!(fail_point("test.times"), Ok(()));
    assert!(list().iter().any(|(n, a)| n == "test.times" && a == "times(2,return(BUSY))"));
}

#[test]
fn pct_bounds() {
    set("test.pct0", "pct(0,return(EIO))").unwrap();
    set("test.pct100", "pct(100,return(EIO))").unwrap();
    for _ in 0..200 {
        assert_eq!(fail_point("test.pct0"), Ok(()));
        assert_eq!(fail_point("test.pct100"), Err(5));
    }
    set("test.pct50", "pct(50,return(EIO))").unwrap();
    let hits = (0..2000).filter(|_| fail_point_hit("test.pct50")).count();
    assert!((700..1300).contains(&hits), "pct(50) 命中 {hits}/2000，偏差过大");
}

#[test]
fn sleep_and_corrupt() {
    set("test.sleep", "sleep(20)").unwrap();
    let t = std::time::Instant::now();
    assert_eq!(fail_point("test.sleep"), Ok(()));
    assert!(t.elapsed() >= Duration::from_millis(20));
    set("test.corrupt", "corrupt").unwrap();
    assert_eq!(fail_point("test.corrupt"), Ok(()), "corrupt 只对 fail_point_hit 有意义");
    assert!(fail_point_hit("test.corrupt"));
}

#[test]
fn values_can_be_negative_or_large() {
    set("test.val.neg", "return(-5000)").unwrap();
    assert_eq!(fail_point_value("test.val.neg"), Some(-5000));
    set("test.val.big", "return(200000)").unwrap();
    assert_eq!(fail_point_value("test.val.big"), Some(200000));
    // 当错误码用时：不在 1..=65535 里的一律按 INTERNAL；0 表示成功，绝不能变成 Err(0)
    assert_eq!(fail_point("test.val.big"), Err(INTERNAL), "超出 u16");
    assert_eq!(fail_point("test.val.neg"), Err(INTERNAL), "负数");
    set("test.val.zero", "return(0)").unwrap();
    assert_eq!(fail_point_value("test.val.zero"), Some(0));
    assert_eq!(fail_point("test.val.zero"), Err(INTERNAL), "0");
    set("test.val.num", "return(5)").unwrap();
    assert_eq!(fail_point("test.val.num"), Err(5), "整数形式的错误码照常返回");
}

#[test]
#[should_panic(expected = "test.panic 触发")]
fn panic_action() {
    set("test.panic", "panic").unwrap();
    let _ = fail_point("test.panic");
}

#[test]
fn bad_actions_are_rejected() {
    for bad in ["bogus", "return(NOPE)", "pct(101,off)", "times(x,off)", "sleep()", "return(EIO", "pct(5)"] {
        assert!(set("test.bad", bad).is_err(), "应拒绝：{bad}");
    }
    for name in ["", "a b", "a&b", "测试.点", "a=b", "a/b"] {
        assert!(set(name, "off").is_err(), "名字只能由字母、数字和 . _ - 组成：{name:?}");
    }
    check_name("plugin.shared-key.authn").unwrap();
    check_name("plugin.quota.quota_admission").unwrap();
    assert_eq!(fail_point("test.bad"), Ok(()), "解析失败不改变配置");
}

#[test]
fn config_string() {
    let v = parse_config(" a=off ; b=times(1,abort);;c=pct(30,return(EIO)) ").unwrap();
    assert_eq!(v.len(), 3);
    assert_eq!(v[1], ("b".to_string(), "times(1,abort)".to_string()));
    assert!(parse_config("noequals").is_err());
    assert!(parse_config("x=whatever").is_err());
    assert!(parse_config("a b=return(EIO)").is_err(), "名字含空白：与 set() 同一条规则");
    assert!(parse_config("=return(EIO)").is_err());
    assert!(parse_config("").unwrap().is_empty());
}

#[test]
fn load_from_env_format() {
    load_from("test.env.a=return(EIO); test.env.b=times(1,return(BUSY))").unwrap();
    assert_eq!(fail_point("test.env.a"), Err(5));
    assert_eq!(fail_point("test.env.b"), Err(1007));
    assert_eq!(fail_point("test.env.b"), Ok(()));
    assert!(load_from("test.env.c=return(EIO);test.env.d=bogus").is_err());
    assert_eq!(fail_point("test.env.c"), Ok(()), "有一项写错时整串都不生效");
}

#[test]
fn hits_are_counted() {
    set("test.count", "return(EIO)").unwrap();
    let c = metrics::counter("wmfs_failpoint_hits_total", "fail point 触发次数", &[("name", "test.count")]);
    let before = c.get();
    let _ = fail_point("test.count");
    let _ = fail_point("test.count");
    assert_eq!(c.get(), before + 2);
}

/// 子进程里运行的那一半：只有被 [`env_var_is_read_at_first_use`] 用环境变量重新运行时才做检查，平时直接通过。
#[test]
fn env_child() {
    if std::env::var("WMFS_FP_CHILD").is_err() {
        return;
    }
    assert_eq!(fail_point("test.child.x"), Err(5), "WMFS_FAILPOINTS 应在第一次用到 fail point 时生效");
}

/// 环境变量这条路：用 `--exact` 让测试程序重新运行自己的 `env_child`，带上 WMFS_FAILPOINTS（不需要 unsafe 的 set_var）。
#[test]
fn env_var_is_read_at_first_use() {
    let exe = std::env::current_exe().expect("找不到测试程序自己的路径");
    let run = |cfg: &str| {
        std::process::Command::new(&exe)
            .args(["--exact", "failpoint::tests::env_child", "--test-threads=1", "--nocapture"])
            .env("WMFS_FAILPOINTS", cfg)
            .env("WMFS_FP_CHILD", "1")
            .output()
            .expect("无法启动子进程")
    };
    let ok = run("test.child.x=return(EIO)");
    let out = String::from_utf8_lossy(&ok.stdout);
    assert!(ok.status.success() && out.contains("1 passed"), "子进程应通过：{out}");
    let bad = run("test.child.x=bogus");
    assert_eq!(bad.status.code(), Some(2), "写错的 WMFS_FAILPOINTS 应让整个进程以 2 退出");
    assert!(String::from_utf8_lossy(&bad.stderr).contains("WMFS_FAILPOINTS 解析失败"));
}
