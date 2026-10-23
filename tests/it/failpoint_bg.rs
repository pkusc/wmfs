//! 后台任务框架的 fail point（design.md 3.10、3.11）：`plugin.gc.bg_task`（这一步失败 → 计入 `wmfs_task_errors_total`，1 s 后从同一个
//! 游标重试）与 `bg.step.crash_after_effect`（副作用已经发生、游标还没持久化：之后从旧游标重放必须无害——gc 的 DeleteEpoch 与删记录
//! 都是幂等的）。fail point 是进程级的：单独一个测试程序。

use wmfs_core::api::{failpoint, metrics};
use wmfs_core::base::types::ChunkId;
use wmfs_core::meta::schema::keys::{self, GC_EPOCH, ROOT_INO};
use wmfs_plugins::local_fs::committed_path;
use wmfs_testkit::harness::meta::get_raw;
use wmfs_testkit::harness::node::{client, init, put, set_settings, start, target_dir, wait_for, who};

#[test]
fn a_failed_or_crashed_step_is_retried_and_replaying_it_is_harmless() {
    let d = std::env::temp_dir().join(format!("wmfs-fp-bg-{}", std::process::id()));
    init(&d);
    let run = start(&d);
    set_settings(&run.meta, |s| s.epoch_gc_grace_ms = 0);
    let errors = metrics::counter("wmfs_task_errors_total", "", &[("task", "gc")]);
    let before = errors.get();
    failpoint::set("plugin.gc.bg_task", "times(1,return(EIO))").unwrap();
    failpoint::set("bg.step.crash_after_effect", "times(1,return(EIO))").unwrap();
    let c = client(&d, &run, "admin");
    let me = who(1000, 1000);
    let o = put(&c, &me, ROOT_INO, "f", &[7u8; 300_000]).unwrap();
    put(&c, &me, ROOT_INO, "f", b"x").unwrap(); // 截断到 0：epoch 1 作废
    let (ino, chain) = (o.attr.ino, o.layout.as_ref().unwrap().chains[0]);
    let chunk0 = committed_path(&target_dir(&d), &ChunkId { chain, ino, epoch: 1, idx: 0 });
    let dir = chunk0.parent().unwrap().to_path_buf(); // epoch 1 的目录
    assert!(wait_for(20, || !dir.exists()), "旧 epoch 最终被删除");
    assert!(wait_for(10, || errors.get() >= before + 2), "两次失败都计入了 wmfs_task_errors_total");
    assert!(get_raw(&run.meta, &keys::gc(GC_EPOCH, ino, 1)).unwrap().is_none(), "重放之后记录也删了，没有出错");
    let hits = |n: &str| metrics::counter("wmfs_failpoint_hits_total", "", &[("name", n)]).get();
    assert_eq!((hits("plugin.gc.bg_task"), hits("bg.step.crash_after_effect")), (1, 1));
    run.stop();
    std::fs::remove_dir_all(&d).unwrap();
}
