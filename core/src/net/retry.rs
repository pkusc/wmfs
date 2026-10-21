//! 重试（design.md 3.3.8）：可重试的错误按指数退避重试——10 ms 起、×2、上限 1 s，直到成功、遇到不可重试的错误，
//! 或超过总期限（挂载参数 `--op-timeout`，默认 120 s；CLI 命令用更短的期限）。
//!
//! 可重试：1000–1007（NOT_LEADER、RETRY、NOT_READY、STALE_CHAIN、CHUNK_DIRTY、NOT_SERVING、FENCED、BUSY）、
//! 1019 SHUTTING_DOWN、本地产生的 `ECONNRESET`（连接断开）与 `ETIMEDOUT`（调用超时）。1013 BAD_CAP、1014 STALE_LAYOUT
//! 要先刷新 cap / layout，1008–1011 有各自的处理（3.3.7），这些由 MetaClient / DataClient 自己处理。
//! 每次重试写一行日志；连接断开时写"连接断开 → 重连重试"（下一次调用会自动建新连接）。

use std::time::Duration;

use crate::base::{ErrCode, clock, log};

/// 退避的起点与上限（毫秒）。
pub const BACKOFF_START_MS: u64 = 10;
pub const BACKOFF_MAX_MS: u64 = 1000;

/// 按 3.3.8 可以直接退避重试的错误码。
pub fn retryable(c: ErrCode) -> bool {
    let cluster = (ErrCode::NOT_LEADER.0..=ErrCode::BUSY.0).contains(&c.0); // 1000–1007
    cluster || c == ErrCode::SHUTTING_DOWN || c == ErrCode::ECONNRESET || c == ErrCode::ETIMEDOUT
}

/// 反复调用 `op(第几次重试，从 0 起)`，直到成功、遇到不可重试的错误，或总期限 `total` 用完（返回最后一次的错误）。
/// `what` 写进日志，说明在重试什么（例如 "ping tcp://127.0.0.1:19100"）。
pub fn retry<T>(what: &str, total: Duration, mut op: impl FnMut(u32) -> Result<T, ErrCode>) -> Result<T, ErrCode> {
    let deadline = clock::mono_ms() + total.as_millis() as u64;
    let mut backoff = BACKOFF_START_MS;
    let mut attempt = 0;
    loop {
        match op(attempt) {
            Err(e) if retryable(e) && clock::mono_ms() + backoff < deadline => {
                let why = if e == ErrCode::ECONNRESET { "连接断开 → 重连重试" } else { "退避重试" };
                log::warn("retry", format_args!("{what}：{e}，{why}（第 {} 次，{backoff} ms 后）", attempt + 1));
                std::thread::sleep(Duration::from_millis(backoff));
                backoff = (backoff * 2).min(BACKOFF_MAX_MS);
                attempt += 1;
            }
            r => return r,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retryable_codes_match_design() {
        for c in [1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1019, 104, 110] {
            assert!(retryable(ErrCode(c)), "{c} 应可重试");
        }
        for c in [1008, 1009, 1010, 1011, 1012, 1013, 1014, 1016, 1017, 1018, 1020, 2, 5, 13] {
            assert!(!retryable(ErrCode(c)), "{c} 不应直接重试");
        }
    }

    #[test]
    fn retries_with_backoff_until_success_or_fatal() {
        let t = std::time::Instant::now();
        let r = retry("测试", Duration::from_secs(5), |n| if n < 3 { Err(ErrCode::ECONNRESET) } else { Ok(n) });
        assert_eq!(r, Ok(3));
        assert!(t.elapsed() >= Duration::from_millis(10 + 20 + 40), "退避 10、20、40 ms");
        let mut calls = 0;
        let r: Result<(), _> = retry("测试", Duration::from_secs(5), |_| {
            calls += 1;
            Err(ErrCode::AUTH_FAILED)
        });
        assert_eq!((r, calls), (Err(ErrCode::AUTH_FAILED), 1), "不可重试的错误立刻返回");
    }

    #[test]
    fn gives_up_at_the_deadline() {
        let t = std::time::Instant::now();
        let r: Result<(), _> = retry("测试", Duration::from_millis(200), |_| Err(ErrCode::BUSY));
        assert_eq!(r, Err(ErrCode::BUSY));
        assert!(t.elapsed() < Duration::from_secs(2));
    }
}
