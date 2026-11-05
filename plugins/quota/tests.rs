//! quota 的单元测试：manifest、文件数精确、硬限、软限的 SoftWarn 与宽限期过后按软限、max_growth 取最小、按类的 scope 看 per_class、
//! 不增长时只算额度、判定计数。

use wmfs_core::base::types::ReqCtx;
use wmfs_core::hooks::manifest::check_manifest_toml;
use wmfs_core::hooks::traits::QuotaScope;
use wmfs_core::meta::schema::records::RStatV1;

use super::*;

fn scope(dir: u64, hard: u64, soft: u64, files: u64, used: u64, nfiles: u64) -> QuotaScope {
    let used = RStatV1 { bytes: used, files: nfiles, dirs: 0, per_class: vec![(2, used / 2, 1)] };
    QuotaScope {
        dir,
        class: None,
        hard_bytes: hard,
        soft_bytes: soft,
        grace_ms: 60_000,
        hard_files: files,
        quantum: 1 << 20,
        used,
        soft_since_ms: 0,
    }
}

fn admit(scopes: &[QuotaScope], bytes: i64, files: i64, now_ms: u64) -> QuotaDecision {
    let who = ReqCtx::internal().who;
    let i = QuotaInput { who: &who, scopes, delta_bytes: bytes, delta_files: files, class_id: 2, now_ms };
    Quota::new().admit(&i)
}

const EDQUOT: ErrCode = ErrCode::EDQUOT;

#[test]
fn manifest_toml_matches_code() {
    check_manifest_toml(&MANIFEST, include_str!("manifest.toml")).unwrap();
}

#[test]
fn hard_limits_files_and_the_smallest_remaining_room() {
    let s = [scope(16, 100, 0, 3, 90, 2), scope(1, 1000, 0, 0, 500, 9)];
    assert_eq!(admit(&s, 0, 1, 0), QuotaDecision::Allow { max_growth: 10 }, "第 3 个文件可以建；额度取最小的剩余");
    assert_eq!(
        admit(&[scope(16, 100, 0, 3, 90, 3)], 0, 1, 0),
        QuotaDecision::Deny { dir: 16, code: EDQUOT },
        "文件数精确"
    );
    assert_eq!(admit(&s, 10, 0, 0), QuotaDecision::Allow { max_growth: 0 }, "正好用满");
    assert_eq!(admit(&s, 11, 0, 0), QuotaDecision::Deny { dir: 16, code: EDQUOT });
    assert_eq!(admit(&s, -50, 0, 0), QuotaDecision::Allow { max_growth: 10 }, "变小不拒绝");
    let over = [scope(16, 100, 0, 0, 120, 1)];
    assert_eq!(
        admit(&over, 0, 0, 0),
        QuotaDecision::Allow { max_growth: 0 },
        "已经超了（并发写者）：不增长也不拒绝，额度为 0"
    );
}

#[test]
fn soft_limits_warn_once_and_bind_after_the_grace_period() {
    let mut s = scope(16, 100, 80, 0, 70, 1);
    assert_eq!(
        admit(&[s.clone()], 15, 0, 1000),
        QuotaDecision::SoftWarn { dir: 16, max_growth: 15 },
        "越过软限：开始计时"
    );
    s.used.bytes = 85;
    s.soft_since_ms = 1000;
    assert_eq!(
        admit(&[s.clone()], 10, 0, 30_000),
        QuotaDecision::Allow { max_growth: 5 },
        "宽限期内按硬限，不再报 SoftWarn"
    );
    assert_eq!(admit(&[s.clone()], 1, 0, 61_000), QuotaDecision::Deny { dir: 16, code: EDQUOT }, "宽限期过后按软限");
    assert_eq!(admit(&[s], 0, 0, 61_000), QuotaDecision::Allow { max_growth: 0 }, "额度为 0");
}

#[test]
fn class_scopes_count_only_their_class() {
    let mut s = scope(16, 100, 0, 1, 180, 7); // per_class 里类 2 用了 90 字节、1 个文件
    s.class = Some(2);
    assert_eq!(admit(&[s.clone()], 10, 0, 0), QuotaDecision::Allow { max_growth: 0 });
    assert_eq!(admit(&[s.clone()], 11, 0, 0), QuotaDecision::Deny { dir: 16, code: EDQUOT });
    assert_eq!(admit(&[s], 0, 1, 0), QuotaDecision::Deny { dir: 16, code: EDQUOT }, "这个类已经有 1 个文件");
}

#[test]
fn decisions_are_counted() {
    let c = |r| metrics::counter("wmfs_quota_decisions_total", "", &[("result", r)]).get();
    let denied = || metrics::counter("wmfs_quota_denied_total", "", &[("dir", "77")]).get();
    let (a, d, n) = (c("allow"), c("deny"), denied());
    admit(&[scope(77, 10, 0, 0, 0, 0)], 1, 0, 0);
    admit(&[scope(77, 10, 0, 0, 0, 0)], 11, 0, 0);
    assert!(c("allow") > a && c("deny") > d && denied() > n);
}
