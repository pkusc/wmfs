//! alert 的单元测试：规则的解析（内置规则、规则文件的覆盖与追加、写错的规则、操作名按操作码表核对）与虚拟时钟下的状态机（指标型的 for
//! 防抖、resolved、sum 与按序列的 rate——节点缺一轮再回来不误报——、grows——协调器积压一直增长才告警，ADR-0045 第 10 条——、事件型的
//! firing 与过了 for 的 resolved）。

use super::rules::{self, Agg, Cond};
use super::*;
use crate::host::check_manifest;

fn sample(name: &str, labels: &str, value: f64) -> Sample {
    Sample { name: name.into(), labels: labels.into(), value }
}

fn state(a: &Alerts, name: &str) -> State {
    a.snapshot().alerts.into_iter().find(|x| x.name == name).unwrap().state
}

#[test]
fn rules_parse_and_files_override_defaults() {
    let d = rules::defaults();
    let names: Vec<&str> = d.iter().map(|r| r.name.as_str()).collect();
    for n in [
        "target_offline",
        "chain_degraded",
        "no_meta_leader",
        "audit_stalled",
        "policy_violation",
        "changelog_gap",
        "clock_skew",
    ] {
        assert!(names.contains(&n), "内置规则 {n}");
    }
    let t = d.iter().find(|r| r.name == "target_offline").unwrap();
    assert_eq!(t.for_ms, 60_000, "4.14：target 离线 > 60 s");
    let b = d.iter().find(|r| r.name == "reconciler_backlog").unwrap();
    let Cond::Metric(x) = &b.cond else { panic!() };
    assert_eq!((x.agg, b.for_ms), (Agg::Grows, 3_600_000), "4.14：协调器积压 1 h 内持续增长（ADR-0045 第 10 条）");
    let Cond::Metric(x) = &d.iter().find(|r| r.name == "audit_stalled").unwrap().cond else { panic!() };
    assert_eq!(
        (x.metric.as_str(), x.labels.clone(), x.agg),
        ("wmfs_mgr_sink_stalled_seconds", vec![("sink".into(), "audit-log".into())], Agg::Any)
    );
    let f = "[[rule]]\nname = \"target_offline\"\nexpr = \"wmfs_targets_offline > 0\"\nfor = \"2s\"\n\
             [[rule]]\nname = \"mine\"\nevent = \"create|Unlink\"\nfor = \"500ms\"\nwebhook = \"http://127.0.0.1:1/x\"\n\
             [[rule]]\nname = \"long\"\nevent = \"PolicyViolation\"\nfor = \"1d\"\n";
    let all = rules::with_file(f).unwrap();
    assert_eq!(all.len(), d.len() + 2, "同名替换、别的追加");
    assert_eq!(all.iter().find(|r| r.name == "target_offline").unwrap().for_ms, 2000);
    let mine = all.iter().find(|r| r.name == "mine").unwrap();
    assert_eq!(
        (mine.for_ms, mine.cond.clone()),
        (500, Cond::Event { ops: vec!["Create".into(), "Unlink".into()], kind: None }),
        "操作名换成规范写法"
    );
    assert_eq!(all.iter().find(|r| r.name == "long").unwrap().for_ms, 86_400_000, "时长写法同属性（d）");
    for bad in [
        "[[rule]]\nname = \"x\"\n",
        "[[rule]]\nname = \"x\"\nexpr = \"a > 1\"\nevent = \"Create\"\n",
        "[[rule]]\nname = \"x\"\nexpr = \"a 1\"\n",
        "[[rule]]\nname = \"x\"\nexpr = \"a{b=\\\"c\\\" > 1\"\n",
        "[[rule]]\nname = \"x\"\nexpr = \"a > 1\"\nfor = \"5 days\"\n",
        "[[rule]]\nname = \"x\"\nexpr = \"a > 1\"\nbogus = 1\n",
        "[[rule]]\nname = \"x\"\nevent = \"PolicyViolaton\"\n",
        "[[rule]]\nname = \"x\"\nexpr = \"a > 1\"\nwebhook = \"ftp://h/x\"\n",
    ] {
        assert!(rules::parse(bad).is_err(), "应拒绝：{bad}");
    }
    assert!(
        rules::parse("[[rule]]\nname = \"x\"\nevent = \"PolicyViolaton\"\n").unwrap_err().contains("不认识的操作名")
    );
    assert_eq!(check_manifest(include_str!("manifest.toml"), "mgmt_ext").unwrap(), "alert");
    assert_eq!(check_manifest(include_str!("manifest.toml"), "event_sink").unwrap(), "alert");
}

#[test]
fn metric_rules_debounce_fire_and_resolve() {
    let rs = rules::parse(
        "[[rule]]\nname = \"off\"\nexpr = \"wmfs_targets_offline > 0\"\nfor = \"60s\"\n\
         [[rule]]\nname = \"noleader\"\nexpr = \"sum(wmfs_raft_is_leader) < 1\"\n\
         [[rule]]\nname = \"denied\"\nexpr = \"rate(wmfs_quota_denied_total{dir=\\\"18\\\"}) > 0.5\"\n",
    )
    .unwrap();
    let mut a = Alerts::new(rs);
    let up = |off: f64, leader: f64, denied: f64| {
        vec![
            sample("wmfs_targets_offline", "node=\"a\"", off),
            sample("wmfs_raft_is_leader", "node=\"a\"", leader),
            sample("wmfs_raft_is_leader", "node=\"b\"", 0.0),
            sample("wmfs_quota_denied_total", "node=\"a\",dir=\"18\"", denied),
            sample("wmfs_quota_denied_total", "node=\"a\",dir=\"19\"", 1000.0),
        ]
    };
    assert!(a.tick(&up(0.0, 1.0, 0.0), 0).is_empty());
    assert_eq!(state(&a, "off"), State::Inactive);
    a.tick(&up(1.0, 1.0, 0.0), 1_000);
    assert_eq!(state(&a, "off"), State::Pending, "条件成立：先 pending");
    assert!(a.tick(&up(1.0, 1.0, 0.0), 50_000).is_empty(), "还没满 60 s");
    assert_eq!(a.tick(&up(1.0, 1.0, 0.0), 61_000), vec![0], "持续 60 s → firing");
    assert_eq!(state(&a, "off"), State::Firing);
    assert_eq!(a.tick(&up(0.0, 1.0, 0.0), 66_000), vec![0]);
    assert_eq!(state(&a, "off"), State::Resolved);
    a.tick(&up(1.0, 1.0, 0.0), 70_000);
    a.tick(&up(0.0, 1.0, 0.0), 75_000);
    assert_eq!(state(&a, "off"), State::Inactive, "没满 for 就不成立了：pending 回到 inactive");
    assert_eq!(a.tick(&[], 80_000), vec![1], "sum：节点都连不上，和是 0 → for 0 立即 firing");
    let msg = a.snapshot().alerts[1].message.clone();
    assert!(msg.contains("现在的值 0）") && !msg.contains("-0"), "空集合求和是 0，不是 -0：{msg}");
    assert!(a.tick(&up(0.0, 0.0, 0.0), 85_000).is_empty(), "全部节点都不是 leader：照样成立");
    assert_eq!(state(&a, "noleader"), State::Firing);
    a.tick(&up(0.0, 0.0, 0.0), 90_000);
    a.tick(&up(0.0, 0.0, 10.0), 95_000);
    assert_eq!(state(&a, "denied"), State::Firing, "rate：(10 − 0) / 5 s = 2/s > 0.5（dir 19 的不算）");
    a.tick(&up(0.0, 0.0, 10.0), 100_000);
    assert_eq!(state(&a, "denied"), State::Resolved, "没有新增 → rate 0");
    a.tick(&up(0.0, 0.0, 1.0), 105_000);
    assert_eq!(state(&a, "denied"), State::Resolved, "计数器变小（节点重启）按 0");
}

/// 一个节点有一轮没抓到（被冻住、超时）、下一轮回来：它的计数器值不能被当成这一轮的增量。
#[test]
fn rate_ignores_a_node_that_missed_a_round() {
    let rs = rules::parse("[[rule]]\nname = \"d\"\nexpr = \"rate(wmfs_quota_denied_total) > 0\"\n").unwrap();
    let mut a = Alerts::new(rs);
    let s = |n: &str, v: f64| sample("wmfs_quota_denied_total", &format!("node=\"{n}\",dir=\"18\""), v);
    a.tick(&[s("a", 5.0), s("b", 3.0)], 0);
    a.tick(&[s("b", 3.0)], 5_000); // a 这一轮没抓到
    a.tick(&[s("a", 5.0), s("b", 3.0)], 10_000);
    a.tick(&[s("a", 5.0), s("b", 3.0)], 15_000);
    assert_eq!(state(&a, "d"), State::Inactive, "没有新的拒绝：不误报");
    a.tick(&[s("a", 6.0), s("b", 3.0)], 20_000);
    assert_eq!(state(&a, "d"), State::Firing, "真的多了一次拒绝");
}

/// grows（ADR-0045 第 10 条）：协调器积压 1 h 内持续增长才告警——进行中的大迁移（积压一直在减）、涨了又回落、持平都不告警；firing 之后
/// 还在增长就继续，1 h 内不再增长就 resolved，清零立即 resolved。
#[test]
fn grows_fires_only_when_the_backlog_keeps_growing() {
    let rs = rules::parse(
        "[[rule]]\nname = \"b\"\nexpr = \"grows(wmfs_task_backlog{task=\\\"reconciler\\\"}) > 0\"\nfor = \"1h\"\n",
    )
    .unwrap();
    let h = 3_600_000;
    let at = |v: f64| {
        let s = |n: &str, v: f64| sample("wmfs_task_backlog", &format!("node=\"{n}\",task=\"reconciler\""), v);
        vec![s("a", v), s("b", 0.0), sample("wmfs_task_backlog", "node=\"a\",task=\"scrub\"", 1e9)]
    };
    let mut a = Alerts::new(rs.clone());
    for k in 0..=12 {
        assert!(a.tick(&at(100_000.0 - 5_000.0 * k as f64), k * h / 4).is_empty());
    }
    assert_eq!(state(&a, "b"), State::Pending, "积压一直在减（进行中的大迁移）：每满 1 h 从现在重新计时，不告警");

    let mut a = Alerts::new(rs.clone());
    a.tick(&at(100.0), 0);
    a.tick(&at(500.0), h / 2);
    assert!(a.tick(&at(100.0), h).is_empty(), "满 1 h 时和那时一样多（涨了又回落）：重新计时");
    assert!(a.tick(&at(100.0), 2 * h).is_empty(), "持平");
    assert_eq!(state(&a, "b"), State::Pending);

    let mut a = Alerts::new(rs);
    a.tick(&at(100.0), 0);
    a.tick(&at(150.0), h / 2);
    assert!(a.tick(&at(200.0), h - 1).is_empty(), "还没满 1 h");
    assert_eq!(a.tick(&at(201.0), h), vec![0], "满 1 h、比那时多（别的节点的 0、别的任务不算）→ firing");
    assert!(a.snapshot().alerts[0].message.contains("从 100 增长到 201"), "{:?}", a.snapshot().alerts[0]);
    assert!(a.tick(&at(300.0), 2 * h).is_empty());
    assert_eq!(state(&a, "b"), State::Firing, "1 h 里还在增长：继续");
    assert_eq!(a.tick(&at(250.0), 3 * h), vec![0]);
    assert_eq!(state(&a, "b"), State::Resolved, "1 h 里没有再增长");
    a.tick(&at(250.0), 3 * h + 5_000);
    assert_eq!(a.tick(&at(400.0), 4 * h + 5_000), vec![0], "又增长了 1 h");
    assert_eq!(a.tick(&at(0.0), 4 * h + 10_000), vec![0], "积压清零：立即 resolved");
    assert_eq!(state(&a, "b"), State::Resolved);
}

#[test]
fn event_rules_fire_on_an_event_and_resolve_after_quiet_time() {
    let rs =
        rules::parse("[[rule]]\nname = \"q\"\nevent = \"PolicyViolation\"\nkind = \"quota_soft\"\nfor = \"10s\"\n")
            .unwrap();
    let mut a = Alerts::new(rs);
    let ev = |kind: &str| ChangeEvent {
        seq: 7,
        op: "PolicyViolation".into(),
        extra: vec![("kind".into(), kind.into())],
        ..Default::default()
    };
    assert!(a.event(&ev("qos_reservation_unmet"), 0).is_empty(), "kind 不对");
    assert_eq!(a.event(&ev("quota_soft"), 1_000), vec![0]);
    assert!(a.event(&ev("quota_soft"), 5_000).is_empty(), "已经 firing：不重复通知");
    let s = a.snapshot().alerts[0].clone();
    assert_eq!((s.state, s.since_ms, s.value), (State::Firing, 1_000, 2.0));
    assert!(s.message.contains("#7 PolicyViolation") && s.message.contains("kind=quota_soft"));
    assert!(a.tick(&[], 14_000).is_empty(), "最后一个事件之后还没满 10 s");
    assert_eq!(a.tick(&[], 15_000), vec![0]);
    assert_eq!(state(&a, "q"), State::Resolved);
    let r = serde_json::to_string(&a.snapshot()).unwrap();
    assert!(
        r.starts_with(r#"{"alerts":[{"name":"q","rule":"PolicyViolation","state":"resolved","since_ms":15000,"#),
        "{r}"
    );
    for i in 0..40 {
        a.event(&ChangeEvent { seq: 100 + i, op: "Create".into(), ..Default::default() }, 20_000);
    }
    let recent = a.snapshot().recent;
    assert_eq!((recent.len(), recent[0].seq, recent[29].seq), (RECENT_EVENTS, 110, 139), "只留最近 30 条，旧的在前");
}
