//! fail point `meta.exec.before_propose` 与 `plugin.embedded-kv.meta_engine`（design.md 3.11、4.10）：在完整的元数据引擎上
//! （raft + 执行器线程，模拟掉电的内存后端）注入，确认它们生效、清除后恢复，且被拦下的批次一个 op 也没有应用。
//! fail point 是进程级的，所以单独一个测试程序，几种情形在一个用例里依次跑。

use wmfs_core::api::failpoint;
use wmfs_core::api::metrics;
use wmfs_core::base::ErrCode;
use wmfs_core::proto::meta::MetaReply;
use wmfs_testkit::harness::meta::{bootstrap, dkey, engine, get_raw, plugins, put_raw};

#[test]
fn exec_and_engine_fail_points() {
    let eng = engine(&bootstrap(&[1]), plugins());
    assert_eq!(put_raw(&eng, dkey("a"), vec![1]), Ok(MetaReply::Ok));

    failpoint::set("meta.exec.before_propose", "times(1,return(RETRY))").unwrap();
    assert_eq!(put_raw(&eng, dkey("b"), vec![2]), Err(ErrCode::RETRY), "批次在提议前被拦下：请求得到注入的错误码");
    assert_eq!(get_raw(&eng, &dkey("b")), Ok(None), "被拦下的批次没有进日志");
    assert_eq!(put_raw(&eng, dkey("b"), vec![2]), Ok(MetaReply::Ok), "times(1, …) 只触发一次");

    failpoint::set("plugin.embedded-kv.meta_engine", "return(EIO)").unwrap();
    assert_eq!(put_raw(&eng, dkey("c"), vec![3]), Err(ErrCode::EIO), "txn 外层的 fail point");
    assert_eq!(get_raw(&eng, &dkey("a")), Err(ErrCode::EIO), "read 外层的 fail point");
    failpoint::clear("plugin.embedded-kv.meta_engine");
    assert_eq!(get_raw(&eng, &dkey("b")), Ok(Some(vec![2])));
    assert_eq!(get_raw(&eng, &dkey("c")), Ok(None));

    let hits = |n: &str| metrics::counter("wmfs_failpoint_hits_total", "fail point 触发次数", &[("name", n)]).get();
    assert_eq!((hits("meta.exec.before_propose"), hits("plugin.embedded-kv.meta_engine")), (1, 2));
    let errs = metrics::counter("wmfs_hook_errors_total", "", &[("hook", "meta_engine"), ("plugin", "embedded-kv")]);
    assert_eq!(errs.get(), 2, "钩子包装把 fail point 触发计为错误");
    eng.shutdown();
}
