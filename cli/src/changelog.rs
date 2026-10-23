//! `wmfs changelog tail [-n N] [-f]`（design.md 7.2、3.11）。
//!
//! 先问一次最新的 seq，再从 `最新 − N` 往后按页（每页最多 1000 条）读，**每读到一页就打印**（不攒在内存里：M11 起最多保留 1000 万条）；
//! `-f` 之后一直长轮询（`wait_ms=5000`：ApiClient 的单次请求超时是 10 s）打印新事件，直到被 Ctrl-C。服务端没挂住就回了空页（每个节点
//! 同时最多 2 个长轮询，第 3 个起立即回）→ 隔 1 s 再问（[`pause_ms`]），不忙等。每个事件一行：`#seq 时间(UTC) 操作 ino=…`，再带上
//! 非零的字段与 extra；`--json` 每页一行 `{"events": [...], "next", "last_seq"}`（一条事件也没有时输出一行空页，带着 last_seq）。

use std::time::Duration;

use wmfs_core::api::types::{ChangeEvent, ChangelogPage};
use wmfs_core::base::clock;
use wmfs_core::base::log::utc_string;
use wmfs_core::meta::changelog::PAGE_MAX;
use wmfs_mgr::api::{ApiClient, ApiError};

use crate::attr::{ApiArgs, client, failed};
use crate::out;

/// `-f` 的一次长轮询最多挂多久（ApiClient 的单次请求超时是 10 s）。
const WAIT_MS: u64 = 5000;

#[derive(clap::Subcommand)]
pub enum ChangelogCmd {
    /// 最近的 N 条事件（-f：之后一直等新事件）
    Tail {
        #[arg(short = 'n', default_value_t = 10)]
        n: u64,
        #[arg(short = 'f')]
        follow: bool,
        #[command(flatten)]
        api: ApiArgs,
    },
}

/// 一个事件一行。
pub fn line(e: &ChangeEvent) -> String {
    let mut s = format!("#{} {} {} ino={}", e.seq, utc_string(e.time_ms), e.op, e.ino);
    if e.parent != 0 || !e.name.is_empty() {
        s += &format!(" parent={} name={}", e.parent, e.name);
    }
    if e.new_parent != 0 || !e.new_name.is_empty() {
        s += &format!(" new_parent={} new_name={}", e.new_parent, e.new_name);
    }
    if e.size != 0 {
        s += &format!(" size={}", e.size);
    }
    s += &format!(" uid={}", e.uid);
    for (k, v) in &e.extra {
        s += &format!(" {k}={v}");
    }
    s
}

fn page(c: &ApiClient, after: u64, wait_ms: u64) -> Result<ChangelogPage, ApiError> {
    c.get(&format!("/api/v1/changelog?after={after}&limit={PAGE_MAX}&wait_ms={wait_ms}"))
}

/// `-f` 的一次长轮询回来之后、发下一次之前等多久（毫秒）：空页、且不到 1 s 就回来了 = 服务端没挂住（这个节点的长轮询名额满了）
/// → 等 1 s；有新事件、或者挂满了才回的空页 → 不等。
fn pause_ms(empty: bool, took_ms: u64) -> u64 {
    if empty && took_ms < 1000 { 1000 } else { 0 }
}

pub fn run_changelog(cmd: ChangelogCmd) -> i32 {
    let ChangelogCmd::Tail { n, follow, api } = cmd;
    let c = match crate::cluster_addrs(&api.cluster).and_then(|cl| client(&api, cl)) {
        Ok(c) => c,
        Err(e) => return failed(api.json, &ApiError { status: 0, code: "einval".into(), message: e }),
    };
    let print = |p: &ChangelogPage| {
        if api.json {
            out(serde_json::to_string(p).unwrap_or_default())
        } else {
            out(p.events.iter().map(line).collect::<Vec<_>>().join("\n"))
        }
    };
    let last = match page(&c, u64::MAX - 1, 0) {
        Ok(p) => p.last_seq, // after 很大：不取事件，只要最新的 seq
        Err(e) => return failed(api.json, &e),
    };
    // 从 最新 − N 往后按页读完已有的，每页立即打印
    let mut next = last.saturating_sub(n);
    let mut printed = false;
    while next < last {
        let p = match page(&c, next, 0) {
            Ok(p) => p,
            Err(e) => return failed(api.json, &e),
        };
        if p.events.is_empty() {
            break;
        }
        next = p.next;
        printed = true;
        if print(&p) != 0 {
            return 1;
        }
    }
    if api.json && !printed && print(&ChangelogPage { events: vec![], next, last_seq: last }) != 0 {
        return 1;
    }
    if !follow {
        return 0;
    }
    // -f：从读到的最后一条接着等（不另问一次最新的 seq，中间提交的不会漏）
    loop {
        let t0 = clock::mono_ms();
        let p = match page(&c, next, WAIT_MS) {
            Ok(p) => p,
            Err(e) => return failed(api.json, &e),
        };
        next = p.next;
        if !p.events.is_empty() && print(&p) != 0 {
            return 1;
        }
        std::thread::sleep(Duration::from_millis(pause_ms(p.events.is_empty(), clock::mono_ms() - t0)));
    }
}

#[cfg(test)]
mod tests {
    use super::pause_ms;

    #[test]
    fn follow_does_not_busy_loop_when_the_server_does_not_hold_the_poll() {
        assert_eq!(pause_ms(true, 3), 1000, "空页、立即回来了（长轮询名额满了）：等 1 s 再问");
        assert_eq!(pause_ms(true, 999), 1000);
        assert_eq!(pause_ms(true, 5000), 0, "挂满了才回的空页：接着问");
        assert_eq!(pause_ms(false, 3), 0, "有新事件：马上接着读");
    }
}
