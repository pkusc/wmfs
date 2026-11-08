//! 可观测性基座里 core 自己的部分（design.md 3.11）：[`trace`]——span 环形缓冲与慢请求日志；[`stages`]——慢日志里一个请求的各阶段耗时
//! （ADR-0045 第 9 条）。指标注册表与 fail point 在 wmfs-api（core 与进程外的 wmfs-mgr 共用）。

pub mod stages;
pub mod trace;

/// 本进程此刻的线程数（`/proc/self/status` 的 `Threads:` 一行；读不到是 0）：`/metrics` 的 `wmfs_threads` 与 `GET /api/v1/node` 用。
pub fn threads() -> u64 {
    let s = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    s.lines().find_map(|l| l.strip_prefix("Threads:")).and_then(|v| v.trim().parse().ok()).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    #[test]
    fn counts_our_own_threads() {
        let t = std::thread::spawn(|| std::thread::sleep(std::time::Duration::from_millis(200)));
        assert!(super::threads() >= 2, "测试线程加上刚起的这个");
        t.join().unwrap();
    }
}
