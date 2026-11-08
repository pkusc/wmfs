//! changelog 循环的单元测试：假的管理 API（tiny_http，127.0.0.1:0）上跑宿主的循环——先投递、投递成功才提交游标；410 缺口按回复里的
//! `oldest` 跳过并显式计数；"多久没有前进"的 gauge；audit-log 没有游标时宿主不建、游标出现之后才投；webhook 的攒批（不到 100 条等 1 s）。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use wmfs_api::types::KeyFile;

use super::*;

/// 假的管理 API 的状态：最新的 seq、最早还在的 seq、各消费者的游标。
#[derive(Default)]
struct Fake {
    last: u64,
    oldest: u64,
    cursors: HashMap<String, u64>,
}

fn serve(f: Arc<Mutex<Fake>>) -> ApiClient {
    let s = tiny_http::Server::http("127.0.0.1:0").unwrap();
    let addr = s.server_addr().to_ip().unwrap().to_string();
    std::thread::spawn(move || {
        for mut req in s.incoming_requests() {
            let url = req.url().to_string();
            let (path, q) = url.split_once('?').unwrap_or((&url, ""));
            let p = |k: &str| wmfs_api::url::param(q, k);
            let mut body = String::new();
            let _ = req.as_reader().read_to_string(&mut body);
            let mut st = f.lock().unwrap();
            let (code, v) = match (req.method().as_str(), path) {
                ("GET", "/api/v1/changelog") => {
                    let after: u64 = p("after").unwrap().parse().unwrap();
                    let limit: u64 = p("limit").map_or(1000, |l| l.parse().unwrap());
                    if after + 1 < st.oldest {
                        (410, serde_json::json!({"error": "gone", "message": "已修剪", "oldest": st.oldest}))
                    } else {
                        let evs: Vec<ChangeEvent> = (after + 1..=st.last.min(after.saturating_add(limit)))
                            .map(|seq| ChangeEvent { seq, audited: true, ..Default::default() })
                            .collect();
                        (200, serde_json::json!({"events": evs, "next": 0, "last_seq": st.last}))
                    }
                }
                ("GET", _) => match st.cursors.get(&p("name").unwrap()) {
                    Some(seq) => (200, serde_json::json!({"name": "x", "seq": seq, "updated_ms": 0})),
                    None => (404, serde_json::json!({"error": "enoent", "message": "没有这个游标"})),
                },
                _ => {
                    let c: CursorReq = serde_json::from_str(&body).unwrap();
                    st.cursors.insert(c.name.clone(), c.seq);
                    (200, serde_json::json!({"name": c.name, "seq": c.seq, "updated_ms": 0}))
                }
            };
            drop(st);
            let _ = req.respond(tiny_http::Response::from_string(v.to_string()).with_status_code(code));
        }
    });
    ApiClient::new(vec![addr], KeyFile { key_id: 1, secret: [0; 32] })
}

/// 记下投到的 seq；前 `fails` 次投递失败。
struct Rec(Arc<Mutex<Vec<u64>>>, usize);

impl EventSink for Rec {
    fn manifest_toml(&self) -> &'static str {
        ""
    }
    fn deliver(&mut self, b: &[ChangeEvent]) -> Result<(), String> {
        if self.1 > 0 {
            self.1 -= 1;
            return Err("坏了".into());
        }
        self.0.lock().unwrap().extend(b.iter().map(|e| e.seq));
        Ok(())
    }
}

fn sink(name: &str, fails: usize, hold_ms: u64, own_cursor: bool) -> (Sink, Arc<Mutex<Vec<u64>>>) {
    let got = Arc::new(Mutex::new(Vec::new()));
    let hold = Duration::from_millis(hold_ms);
    let s = Sink::new(Box::new(Rec(got.clone(), fails)), Hooked::new(name, "event_sink"), hold, own_cursor);
    (s, got)
}

fn fake(last: u64, oldest: u64, cursors: &[(&str, u64)]) -> Arc<Mutex<Fake>> {
    let cursors = cursors.iter().map(|(n, s)| (n.to_string(), *s)).collect();
    Arc::new(Mutex::new(Fake { last, oldest, cursors }))
}

fn cursor_of(f: &Arc<Mutex<Fake>>, name: &str) -> Option<u64> {
    f.lock().unwrap().cursors.get(name).copied()
}

#[test]
fn a_failed_delivery_is_not_committed_and_gaps_are_counted() {
    let f = fake(5, 1, &[("ev-a", 0)]);
    let api = serve(f.clone());
    let (a, got) = sink("ev-a", 1, 0, true);
    let mut v = vec![a];
    assert!(v[0].find_start(&api));
    round(&api, &mut v);
    assert_eq!(cursor_of(&f, "ev-a"), Some(0), "投递失败：游标不前进");
    assert!(v[0].next_try > Instant::now(), "退避");
    v[0].next_try = Instant::now();
    round(&api, &mut v);
    assert_eq!((cursor_of(&f, "ev-a"), got.lock().unwrap().clone()), (Some(5), vec![1, 2, 3, 4, 5]), "整批重投");

    // 缺口：1..=10 已被修剪，410 回复里 oldest = 11
    let f = fake(20, 11, &[("ev-b", 0)]);
    let api = serve(f.clone());
    let (b, got) = sink("ev-b", 0, 0, true);
    let mut v = vec![b];
    assert!(v[0].find_start(&api));
    round(&api, &mut v);
    let gaps = metrics::counter("wmfs_mgr_changelog_gaps_total", "", &[("sink", "ev-b")]).get();
    assert_eq!((v[0].pos, gaps), (10, 1), "410：跳到还在的最早一条之前、显式计数");
    round(&api, &mut v);
    assert_eq!(got.lock().unwrap().clone(), (11..=20).collect::<Vec<u64>>());
    assert_eq!(cursor_of(&f, "ev-b"), Some(20));
}

#[test]
fn a_new_sink_starts_at_the_latest_seq_and_stalled_seconds_grow_while_behind() {
    let f = fake(7, 1, &[("ev-c", 0)]);
    let api = serve(f.clone());
    let (n, _) = sink("ev-new", 0, 0, true);
    let (c, _) = sink("ev-c", 100, 0, true);
    let mut v = vec![n, c];
    assert!(v[0].find_start(&api));
    assert_eq!((v[0].pos, cursor_of(&f, "ev-new")), (7, Some(7)), "第一次运行：从最新的 seq 起并马上提交游标");
    assert!(v[1].find_start(&api));
    v[1].moved = Instant::now() - Duration::from_secs(700);
    round(&api, &mut v);
    let g = |s: &str| metrics::gauge("wmfs_mgr_sink_stalled_seconds", "", &[("sink", s)]).get();
    assert!(g("ev-c") >= 600.0, "落后又 700 s 没有前进：{}", g("ev-c"));
    assert_eq!(g("ev-new"), 0.0, "不落后");
}

#[test]
fn audit_log_waits_for_core_to_create_its_cursor() {
    let f = fake(5, 1, &[]);
    let api = serve(f.clone());
    let (a, got) = sink("ev-audit", 0, 0, false);
    let mut v = vec![a];
    assert!(v[0].find_start(&api));
    v[0].next_try = Instant::now();
    round(&api, &mut v);
    assert_eq!((v[0].active, cursor_of(&f, "ev-audit")), (false, None), "没有游标：宿主不建、不投递");
    assert!(got.lock().unwrap().is_empty() && v[0].next_try > Instant::now() + Duration::from_secs(20), "30 s 后再看");
    f.lock().unwrap().cursors.insert("ev-audit".into(), 3); // core 在 audit 第一次启用时建了游标
    v[0].next_try = Instant::now();
    round(&api, &mut v);
    assert_eq!((got.lock().unwrap().clone(), cursor_of(&f, "ev-audit")), (vec![4, 5], Some(5)), "从游标接着投");
}

#[test]
fn a_held_sink_batches_up_to_one_second_unless_it_has_100() {
    let f = fake(3, 1, &[("ev-hold", 0)]);
    let api = serve(f.clone());
    let (h, got) = sink("ev-hold", 0, 1000, true);
    let mut v = vec![h];
    assert!(v[0].find_start(&api));
    round(&api, &mut v);
    assert!(got.lock().unwrap().is_empty() && v[0].held.is_some(), "不到 100 条：先攒着");
    v[0].held = Some(Instant::now() - Duration::from_secs(2));
    v[0].next_try = Instant::now();
    round(&api, &mut v);
    assert_eq!(got.lock().unwrap().clone(), [1, 2, 3], "攒满 1 s：投");
    f.lock().unwrap().last = 150;
    round(&api, &mut v);
    assert_eq!(got.lock().unwrap().len(), 150, "够 100 条：马上投");
}
