//! 指标注册表与 Prometheus 文本输出（design.md 3.11、7.3）。
//!
//! 用法：启动阶段（或第一次用到时）注册，把拿到的 `Arc<Counter>` 等句柄存起来；
//! 热路径上只做原子加减，不加锁。同名同标签重复注册返回同一个句柄。
//! 命名 `wmfs_<子系统>_<量>_<单位>`；标签基数要受控（路径标签只用于设置了配额/qos 的策略根目录）。
//! `GET /metrics`（M1 起）调用 [`render_prometheus`]。

use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

/// 只增不减的计数器。
#[derive(Debug, Default)]
pub struct Counter {
    v: AtomicU64,
}

impl Counter {
    pub fn inc(&self) {
        self.v.fetch_add(1, Ordering::Relaxed);
    }
    pub fn add(&self, n: u64) {
        self.v.fetch_add(n, Ordering::Relaxed);
    }
    pub fn get(&self) -> u64 {
        self.v.load(Ordering::Relaxed)
    }
}

/// 可增可减的当前值（内部存 f64 的位模式）。
#[derive(Debug, Default)]
pub struct Gauge {
    bits: AtomicU64,
}

impl Gauge {
    pub fn set(&self, v: f64) {
        self.bits.store(v.to_bits(), Ordering::Relaxed);
    }
    pub fn add(&self, d: f64) {
        add_f64(&self.bits, d);
    }
    pub fn get(&self) -> f64 {
        f64::from_bits(self.bits.load(Ordering::Relaxed))
    }
}

/// 直方图：每个桶各自计数，输出时再累加成 Prometheus 要求的累计形式。
#[derive(Debug)]
pub struct Histogram {
    buckets: &'static [f64],
    counts: Vec<AtomicU64>,
    sum_bits: AtomicU64,
    count: AtomicU64,
}

impl Histogram {
    pub fn observe(&self, v: f64) {
        if let Some(i) = self.buckets.iter().position(|b| v <= *b) {
            self.counts[i].fetch_add(1, Ordering::Relaxed);
        }
        add_f64(&self.sum_bits, v);
        self.count.fetch_add(1, Ordering::Relaxed);
    }
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }
    pub fn sum(&self) -> f64 {
        f64::from_bits(self.sum_bits.load(Ordering::Relaxed))
    }
}

/// 常用的耗时分桶（秒）：100 µs … 10 s。
pub const SECONDS_BUCKETS: &[f64] = &[0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0];

fn add_f64(bits: &AtomicU64, d: f64) {
    let mut cur = bits.load(Ordering::Relaxed);
    loop {
        let new = (f64::from_bits(cur) + d).to_bits();
        match bits.compare_exchange_weak(cur, new, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return,
            Err(now) => cur = now,
        }
    }
}

#[derive(Clone)]
enum Metric {
    Counter(Arc<Counter>),
    Gauge(Arc<Gauge>),
    Histogram(Arc<Histogram>),
}

struct Entry {
    name: &'static str,
    help: &'static str,
    labels: Vec<(&'static str, String)>,
    metric: Metric,
}

fn registry() -> &'static Mutex<Vec<Entry>> {
    static REG: OnceLock<Mutex<Vec<Entry>>> = OnceLock::new();
    REG.get_or_init(|| Mutex::new(Vec::new()))
}

/// 找同名同标签的已有条目；没有就登记 `make`。同名但类型不同是程序错误。返回的是同一个 Arc 句柄。
fn register(name: &'static str, help: &'static str, labels: &[(&'static str, &str)], make: Metric) -> Metric {
    use std::mem::discriminant;
    let mut reg = registry().lock().unwrap_or_else(|e| e.into_inner());
    if reg.iter().any(|e| e.name == name && discriminant(&e.metric) != discriminant(&make)) {
        panic!("不变量破坏：指标 {name} 以不同类型重复注册");
    }
    let want: Vec<(&'static str, String)> = labels.iter().map(|(k, v)| (*k, v.to_string())).collect();
    if let Some(e) = reg.iter().find(|e| e.name == name && e.labels == want) {
        return e.metric.clone();
    }
    reg.push(Entry { name, help, labels: want, metric: make.clone() });
    make
}

/// 注册（或取回）一个计数器。
pub fn counter(name: &'static str, help: &'static str, labels: &[(&'static str, &str)]) -> Arc<Counter> {
    match register(name, help, labels, Metric::Counter(Arc::new(Counter::default()))) {
        Metric::Counter(c) => c,
        _ => unreachable!(),
    }
}

/// 注册（或取回）一个 gauge。
pub fn gauge(name: &'static str, help: &'static str, labels: &[(&'static str, &str)]) -> Arc<Gauge> {
    match register(name, help, labels, Metric::Gauge(Arc::new(Gauge::default()))) {
        Metric::Gauge(g) => g,
        _ => unreachable!(),
    }
}

/// 注册（或取回）一个直方图；`buckets` 必须升序。
pub fn histogram(
    name: &'static str,
    help: &'static str,
    labels: &[(&'static str, &str)],
    buckets: &'static [f64],
) -> Arc<Histogram> {
    let h = Histogram {
        buckets,
        counts: buckets.iter().map(|_| AtomicU64::new(0)).collect(),
        sum_bits: AtomicU64::new(0f64.to_bits()),
        count: AtomicU64::new(0),
    };
    match register(name, help, labels, Metric::Histogram(Arc::new(h))) {
        Metric::Histogram(h) => h,
        _ => unreachable!(),
    }
}

/// 输出全部指标的 Prometheus 文本格式（GET /metrics）。同名指标的 HELP/TYPE 只写一次。
pub fn render_prometheus(out: &mut String) {
    let reg = registry().lock().unwrap_or_else(|e| e.into_inner());
    let mut done: Vec<&str> = Vec::new();
    for first in reg.iter() {
        if done.contains(&first.name) {
            continue;
        }
        done.push(first.name);
        let kind = match first.metric {
            Metric::Counter(_) => "counter",
            Metric::Gauge(_) => "gauge",
            Metric::Histogram(_) => "histogram",
        };
        let _ = writeln!(out, "# HELP {} {}", first.name, escape(first.help, false));
        let _ = writeln!(out, "# TYPE {} {}", first.name, kind);
        for e in reg.iter().filter(|e| e.name == first.name) {
            match &e.metric {
                Metric::Counter(c) => sample(out, e.name, "", &e.labels, None, &c.get().to_string()),
                Metric::Gauge(g) => sample(out, e.name, "", &e.labels, None, &fmt_f64(g.get())),
                Metric::Histogram(h) => {
                    let mut acc = 0u64;
                    for (i, b) in h.buckets.iter().enumerate() {
                        acc += h.counts[i].load(Ordering::Relaxed);
                        sample(out, e.name, "_bucket", &e.labels, Some(&fmt_f64(*b)), &acc.to_string());
                    }
                    sample(out, e.name, "_bucket", &e.labels, Some("+Inf"), &h.count().to_string());
                    sample(out, e.name, "_sum", &e.labels, None, &fmt_f64(h.sum()));
                    sample(out, e.name, "_count", &e.labels, None, &h.count().to_string());
                }
            }
        }
    }
}

fn sample(out: &mut String, name: &str, suffix: &str, labels: &[(&str, String)], le: Option<&str>, value: &str) {
    out.push_str(name);
    out.push_str(suffix);
    if !labels.is_empty() || le.is_some() {
        out.push('{');
        let mut first = true;
        for (k, v) in labels {
            if !first {
                out.push(',');
            }
            first = false;
            let _ = write!(out, "{}=\"{}\"", k, escape(v, true));
        }
        if let Some(le) = le {
            if !first {
                out.push(',');
            }
            let _ = write!(out, "le=\"{le}\"");
        }
        out.push('}');
    }
    let _ = writeln!(out, " {value}");
}

/// Prometheus 文本的转义：反斜杠、换行；标签值里另外转义双引号。
fn escape(s: &str, quote: bool) -> String {
    let mut o = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '\\' => o.push_str("\\\\"),
            '\n' => o.push_str("\\n"),
            '"' if quote => o.push_str("\\\""),
            c => o.push(c),
        }
    }
    o
}

fn fmt_f64(v: f64) -> String {
    if v.is_nan() {
        "NaN".to_string()
    } else if v.is_infinite() {
        if v > 0.0 { "+Inf".to_string() } else { "-Inf".to_string() }
    } else {
        format!("{v}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rendered() -> String {
        let mut s = String::new();
        render_prometheus(&mut s);
        s
    }

    #[test]
    fn counter_is_shared_by_name_and_labels() {
        let a = counter("wmfs_test_a_total", "测试计数器", &[("op", "x")]);
        let b = counter("wmfs_test_a_total", "测试计数器", &[("op", "x")]);
        let c = counter("wmfs_test_a_total", "测试计数器", &[("op", "y")]);
        a.inc();
        b.add(2);
        c.inc();
        assert_eq!(a.get(), 3);
        let s = rendered();
        assert!(s.contains("# TYPE wmfs_test_a_total counter\n"));
        assert!(s.contains("wmfs_test_a_total{op=\"x\"} 3\n"));
        assert!(s.contains("wmfs_test_a_total{op=\"y\"} 1\n"));
        assert_eq!(s.matches("# HELP wmfs_test_a_total").count(), 1, "同名指标的 HELP 只写一次");
    }

    #[test]
    fn gauge_set_add_and_plain_name() {
        let g = gauge("wmfs_test_gauge", "测试 gauge", &[]);
        g.set(1.5);
        g.add(-0.25);
        assert_eq!(g.get(), 1.25);
        assert!(rendered().contains("wmfs_test_gauge 1.25\n"));
    }

    #[test]
    fn histogram_is_cumulative() {
        const B: &[f64] = &[0.1, 1.0];
        let h = histogram("wmfs_test_seconds", "测试直方图", &[("op", "r")], B);
        h.observe(0.0625);
        h.observe(0.5);
        h.observe(7.0);
        let s = rendered();
        assert!(s.contains("wmfs_test_seconds_bucket{op=\"r\",le=\"0.1\"} 1\n"));
        assert!(s.contains("wmfs_test_seconds_bucket{op=\"r\",le=\"1\"} 2\n"));
        assert!(s.contains("wmfs_test_seconds_bucket{op=\"r\",le=\"+Inf\"} 3\n"));
        assert!(s.contains("wmfs_test_seconds_count{op=\"r\"} 3\n"));
        assert!(s.contains("wmfs_test_seconds_sum{op=\"r\"} 7.5625\n"));
    }

    #[test]
    fn label_values_are_escaped() {
        counter("wmfs_test_esc_total", "转义", &[("dir", "a\"b\\c\nd")]).inc();
        assert!(rendered().contains("wmfs_test_esc_total{dir=\"a\\\"b\\\\c\\nd\"} 1\n"));
    }

    #[test]
    #[should_panic(expected = "不同类型")]
    fn same_name_different_kind_panics() {
        counter("wmfs_test_kind", "x", &[]);
        gauge("wmfs_test_kind", "x", &[]);
    }
}
