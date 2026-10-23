//! 客户端的 fail point（design.md 3.11、9.4，roadmap M6），设在挂载进程的环境变量 `WMFS_FAILPOINTS` 里：
//! - `client.flush.before_commit=times(1,return(EIO))`：写完数据、提交长度之前失败一次 → 这次 close 报 EIO（不静默），数据留在写回
//!   缓冲里；之后再写出去成功，内容完整（不会悄悄丢弃）。
//! - `client.flush.before_commit=abort`：写完数据、提交长度之前挂载进程崩溃 → 写进链但没提交长度的数据在长度之外不可见
//!   （放松 ⑩：文件长度还是 0）。
//! - `plugin.cache-memory.cache_store=return(EIO)`：缓存插件每次都失败 → 读照常从数据节点读，内容正确（缓存只是加速）；挂载日志
//!   （debug 级）里有"cache_store 出错……绕过缓存"——确认 fail point 真的触发了、读走的是绕过缓存那条路。

mod common;

use std::io::Write;

use std::sync::Mutex;

use common::{Node, bytes, read_all, sh, wait_for};

/// 这几个用例一个一个跑：别的用例的线程在 fork 子进程（起挂载、跑 sh），子进程 exec 时关掉继承来的 fd，内核会对这个测试打开着
/// 的文件发一次 FLUSH（每次 close 都发），`times(1, …)` 的 fail point 就可能被那次 FLUSH 用掉。
static SERIAL: Mutex<()> = Mutex::new(());

fn size_of(n: &Node, path: &str) -> u64 {
    n.meta_attr(path).size
}

#[test]
fn a_failed_commit_is_reported_at_close_and_the_data_is_kept() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let n = Node::start("fp-commit");
    let m = n.mount("m", &[("WMFS_FAILPOINTS", "client.flush.before_commit=times(1,return(EIO))")]);
    let data = bytes(1, 300_000);
    let mut f = std::fs::File::create(m.p("f")).unwrap();
    f.write_all(&data).unwrap();
    let r = f.sync_all(); // fsync 走同一条写出去的路：第一次提交失败
    assert_eq!(r.map_err(|e| e.raw_os_error()), Err(Some(5)), "提交之前失败：fsync 报 EIO");
    drop(f); // close：这一次 fail point 已经用完，数据照常写出去
    let other = n.mount("o", &[]);
    assert!(wait_for(10, || size_of(&n, "/f") == data.len() as u64), "数据没有被悄悄丢掉");
    assert!(read_all(&other.p("f")) == data, "内容完整");
    m.unmount();
    other.unmount();
    n.stop();
}

#[test]
fn a_crash_before_committing_the_length_hides_the_written_data() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let n = Node::start("fp-abort");
    let m = n.mount("m", &[("WMFS_FAILPOINTS", "client.flush.before_commit=abort")]);
    let (rc, _) = sh(&m.mp, "head -c 300000 /dev/zero | tr '\\0' 'z' > f");
    assert_ne!(rc, 0, "close 时挂载进程崩溃：写者看到错误");
    assert_eq!(size_of(&n, "/f"), 0, "写进链但没提交长度的数据在长度之外不可见（放松 ⑩）");
    drop(m); // 崩溃的挂载点：Drop 里 lazy 卸载
    n.stop();
}

#[test]
fn a_failing_cache_store_only_bypasses_the_cache() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let n = Node::start("fp-cache");
    let w = n.mount("w", &[]);
    std::fs::write(w.p("f"), bytes(2, 3 << 20)).unwrap();
    let r = n.mount("r", &[("WMFS_FAILPOINTS", "plugin.cache-memory.cache_store=return(EIO)"), ("WMFS_LOG", "debug")]);
    assert!(read_all(&r.p("f")) == bytes(2, 3 << 20), "缓存插件出错：照常从数据节点读，内容正确");
    assert!(r.log_text().contains("cache_store 出错"), "fail point 触发了、读绕过了缓存：{}", r.log_text());
    w.unmount();
    r.unmount();
    n.stop();
}
