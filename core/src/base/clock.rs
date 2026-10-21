//! 时钟（design.md 0.2 第 6 条、3.11）：所有租约、超时与时间戳的唯一来源。
//!
//! - [`mono_ms`]：单调时钟，用于租约与超时。读数 = 1000 + 从本进程**第一次调用 `mono_ms`** 起经过的毫秒数
//!   （起点不是 0，所以读数永远不会是 0，不会和"未设置"混淆）。只用来比较先后、计算间隔，不要当成"进程运行时长"；
//!   想让起点贴近进程启动，就在 main 开头先调用一次。
//! - [`wall_ms`] / [`wall_ts`]：墙钟（Unix 毫秒），用于 mtime、cap 过期。
//!
//! 测试可以用 fail point 注入偏移与速率（值写在 `return(<整数>)` 里，可以为负），运行中随时 set / clear：
//! - `clock.mono.rate_ppm`（速率偏差，百万分之一）：**从设置的那一刻起**按 `1 + R/1e6` 的速率走
//!   （R 限制在 -1_000_000..=1_000_000_000，-1_000_000 = 停走）；清除后恢复正常速率，已经多走或少走的部分保留。
//! - `clock.mono.skew_ms`（偏移毫秒）：读数加上 N。N 变大 → 时钟立刻前跳；N 变小（含清除）→ 时钟停走，直到真实时间追上。
//! - `clock.wall.skew_ms`（墙钟偏移毫秒）：墙钟读数加上 N（墙钟本来就可能被 NTP 往回调，所以不做单调保护）。
//!
//! 单调时钟**永不倒退**（[`mono_ms`] 返回"至今为止最大的读数"），所以 `mono_ms() - t0` 不会下溢。
//! 例如 `WMFS_FAILPOINTS="clock.mono.rate_ppm=return(200000)"` 让本进程的单调时钟快 20%。

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use wmfs_api::failpoint::fail_point_value;

use super::types::Ts;

/// 单调时钟第一次读数的值。
pub const MONO_START_MS: u64 = 1000;

fn start() -> Instant {
    static START: OnceLock<Instant> = OnceLock::new();
    *START.get_or_init(Instant::now)
}

/// 速率注入的基点：最近一次速率变化时的真实经过毫秒数 `real`、当时的读数 `mono`（不含 skew），以及之后生效的速率。
struct Anchor {
    real: u64,
    mono: u64,
    rate_ppm: i64,
}

static ANCHOR: Mutex<Anchor> = Mutex::new(Anchor { real: 0, mono: 0, rate_ppm: 0 });
static INJECTED: AtomicBool = AtomicBool::new(false); // 注入过速率或偏移之后一直为 true（只会发生在测试里）
static LAST: AtomicU64 = AtomicU64::new(0); // 返回过的最大读数

/// 单调时钟（毫秒）：含 fail point 注入的速率与偏移，永不倒退。
pub fn mono_ms() -> u64 {
    let rate = fail_point_value("clock.mono.rate_ppm").unwrap_or(0).clamp(-1_000_000, 1_000_000_000);
    let skew = fail_point_value("clock.mono.skew_ms").unwrap_or(0);
    if rate == 0 && skew == 0 && !INJECTED.load(Ordering::Relaxed) {
        // 从来没有注入过（生产环境永远走这里）：读数就是真实经过的时间，不加锁
        return not_before(MONO_START_MS + real_ms());
    }
    INJECTED.store(true, Ordering::Relaxed);
    let mut a = ANCHOR.lock().unwrap_or_else(|e| e.into_inner());
    let real = real_ms(); // 在锁里读真实时间：基点的 real 只增不减
    if rate != a.rate_ppm {
        // 速率变了：以"按旧速率算到此刻的读数"为新基点，新速率只作用于之后的时间，读数不会突然跳一大段
        *a = Anchor { real, mono: scaled(&a, real), rate_ppm: rate };
    }
    let v = (scaled(&a, real) as i64).saturating_add(skew).max(0) as u64;
    drop(a);
    not_before(MONO_START_MS + v)
}

/// 从第一次调用起真实经过的毫秒数。
fn real_ms() -> u64 {
    start().elapsed().as_millis() as u64
}

/// 按基点的速率，把真实经过时间 `real` 换算成读数（不含 skew）。
fn scaled(a: &Anchor, real: u64) -> u64 {
    let d = real.saturating_sub(a.real) as i128 * (1_000_000 + a.rate_ppm as i128) / 1_000_000;
    a.mono.saturating_add(u64::try_from(d).unwrap_or(u64::MAX))
}

/// 返回 max(v, 返回过的最大读数)，并记下它：单调时钟永不倒退（负的 skew 表现为"时钟停走一会儿"）。
fn not_before(v: u64) -> u64 {
    let last = LAST.load(Ordering::Relaxed);
    if v <= last {
        return last;
    }
    LAST.fetch_max(v, Ordering::Relaxed).max(v)
}

/// 墙钟：Unix 时间的毫秒数（含 fail point 注入的偏移）。
pub fn wall_ms() -> u64 {
    let real = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0);
    let skew = fail_point_value("clock.wall.skew_ms").unwrap_or(0);
    real.saturating_add(skew).max(0) as u64
}

/// 墙钟时间戳（秒 + 纳秒，毫秒精度）：mtime/ctime 等用它。
pub fn wall_ts() -> Ts {
    let ms = wall_ms();
    Ts { secs: (ms / 1000) as i64, nanos: ((ms % 1000) * 1_000_000) as u32 }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;
    use std::time::Duration;
    use wmfs_api::failpoint;

    fn ms(d: Duration) -> u64 {
        d.as_millis() as u64
    }

    // 前半：正常读数；后半是 fail point 用例：运行中注入、清除墙钟偏移与单调时钟的速率/偏移（design.md 3.11"时钟可注入"）。
    // 这几个 fail point 是全进程共享的，会影响同时读时钟的其他断言，所以全部放在同一个测试里顺序执行。
    // 测量真实时间用 std::time::Instant（只在测试里），用来核对时钟的"斜率"。
    #[test]
    fn clock_readings_and_failpoint_injection() {
        let a = mono_ms();
        assert!(a >= MONO_START_MS, "读数从 1000 起，永远不是 0");
        sleep(Duration::from_millis(5));
        assert!(mono_ms() >= a + 5);
        let w = wall_ms();
        assert!(w > 1_700_000_000_000, "墙钟应在 2023 年之后：{w}");
        let ts = wall_ts();
        assert!(ts.secs > 1_700_000_000 && ts.nanos < 1_000_000_000);

        let w0 = wall_ms();
        failpoint::set("clock.wall.skew_ms", "return(-600000)").unwrap();
        let w1 = wall_ms();
        failpoint::clear("clock.wall.skew_ms");
        assert!(w1 + 590_000 < w0 + 1000 && w1 + 610_000 > w0, "墙钟应被拨慢 10 分钟：{w0} → {w1}");

        // 速率 +100%：设置的那一刻读数不跳；之后按两倍速走；清除后读数不倒退，恢复一倍速
        let t0 = Instant::now();
        let p0 = mono_ms();
        failpoint::set("clock.mono.rate_ppm", "return(1000000)").unwrap();
        let t = Instant::now();
        let f0 = mono_ms();
        let e0 = ms(t0.elapsed());
        assert!(f0 >= p0 && f0 - p0 <= e0 + 2, "打开速率注入时读数不应突然跳一段：{p0} → {f0}（真实只过了 {e0} ms）");
        sleep(Duration::from_millis(100));
        let f1 = mono_ms();
        let e = ms(t.elapsed());
        assert!(f1 - f0 >= 198 && f1 - f0 <= 2 * e + 2, "两倍速：真实 {e} ms，时钟走了 {} ms", f1 - f0);
        failpoint::clear("clock.mono.rate_ppm");
        let c0 = mono_ms();
        assert!(c0 >= f1, "清除速率注入后读数不得减小：{f1} → {c0}");
        let t = Instant::now();
        sleep(Duration::from_millis(50));
        let c1 = mono_ms();
        let e = ms(t.elapsed());
        assert!(c1 - c0 >= 48 && c1 - c0 <= e + 2, "恢复一倍速：真实 {e} ms，时钟走了 {} ms", c1 - c0);

        // 偏移 +300 ms：立刻前跳；清除后停走而不是倒退
        let s0 = mono_ms();
        failpoint::set("clock.mono.skew_ms", "return(300)").unwrap();
        let s1 = mono_ms();
        assert!(s1 >= s0 + 300, "偏移应让时钟前跳 300 ms：{s0} → {s1}");
        failpoint::clear("clock.mono.skew_ms");
        let s2 = mono_ms();
        assert!(s2 >= s1, "清除偏移后读数不得减小：{s1} → {s2}");
        // 负偏移与 times()（第二次读时已经失效）：读数都不减小
        failpoint::set("clock.mono.skew_ms", "return(-200)").unwrap();
        let s3 = mono_ms();
        failpoint::set("clock.mono.skew_ms", "times(1,return(500))").unwrap();
        let s4 = mono_ms();
        let s5 = mono_ms();
        failpoint::clear("clock.mono.skew_ms");
        assert!(s3 >= s2 && s4 >= s3 + 200 && s5 >= s4, "读数应单调不减：{s2} {s3} {s4} {s5}");
    }
}
