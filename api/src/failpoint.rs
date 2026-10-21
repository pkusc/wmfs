//! 故障注入点 fail point（design.md 3.11、9.4）。是函数而不是宏；core 与 wmfs-mgr 共用。
//!
//! 代码在关键步骤调用 `fail_point("名字")?`（或 [`fail_point_hit`]）；没有配置任何 fail point 时
//! 只多一次 Relaxed 原子读。打开方式：
//! - 环境变量 `WMFS_FAILPOINTS="chunk.pending.torn=times(1,abort);net.drop.to.2=return(BUSY)"`
//!   （进程第一次用到 fail point 时读取；写错了打印原因并以退出码 2 结束**整个进程**，绝不静默忽略）；
//! - 运行时 [`set`] / [`clear`]（M1 起经管理 API `POST /api/v1/failpoints` 与 `wmfs failpoint set` 调用）。
//!
//! 动作：`off` | `return(<错误码名或整数>)` | `panic` | `abort` | `sleep(<毫秒>)` | `corrupt`
//!      | `pct(<0..=100>,<动作>)` | `times(<次数>,<动作>)`
//! - [`fail_point`]：`return(c)` → `Err(c)`（c 不在 1..=65535 里时按 `INTERNAL`：0 在去重表与 changelog 里表示成功，
//!   不能当错误码返回）；panic / abort / sleep 在函数内部执行；corrupt 不做事；
//! - [`fail_point_hit`]：`return(..)` 与 `corrupt` 返回 true（drop、corrupt 这类动作由调用方自己做）；
//! - [`fail_point_value`]：`return(<整数>)` 的整数值（可以为负、可以大于 u16），给时钟注入偏移与速率用。
//!
//! 名字按完整字符串匹配（例如 `plugin.quota.quota_admission`、`net.drop.to.2`）。

use std::collections::HashMap;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Once, OnceLock, RwLock};
use std::time::Duration;

use crate::{codes, metrics};

/// 解析后的动作树。
enum Action {
    Off,
    Return(i64),
    Panic,
    Abort,
    Sleep(u64),
    Corrupt,
    Pct(u32, Box<Action>),
    Times(AtomicU64, Box<Action>), // 剩余次数，用完就等同 off
}

enum Outcome {
    Nothing,
    Return(i64),
    Corrupt,
}

struct Point {
    text: String, // 配置时的原文（list() 原样返回）
    action: Action,
    hits: Arc<metrics::Counter>,
}

/// `return(c)` 的 c 不能当错误码用（0、负数、超出 u16）时按 INTERNAL（1020，内部不变量破坏）。
const INTERNAL: u16 = 1020;

const UNINIT: u8 = 0; // 还没读过环境变量
const IDLE: u8 = 1; // 没有任何 fail point
const ACTIVE: u8 = 2; // 至少配置了一个
static STATE: AtomicU8 = AtomicU8::new(UNINIT);
static ENV_ONCE: Once = Once::new();

fn points() -> &'static RwLock<HashMap<String, Arc<Point>>> {
    static P: OnceLock<RwLock<HashMap<String, Arc<Point>>>> = OnceLock::new();
    P.get_or_init(|| RwLock::new(HashMap::new()))
}

/// 热路径：未配置时只有这一次原子读。
fn lookup(name: &str) -> Option<Arc<Point>> {
    let mut st = STATE.load(Ordering::Relaxed);
    if st == UNINIT {
        load_env();
        st = STATE.load(Ordering::Relaxed);
    }
    if st != ACTIVE {
        return None;
    }
    points().read().unwrap_or_else(|e| e.into_inner()).get(name).cloned()
}

fn load_env() {
    ENV_ONCE.call_once(|| {
        if let Ok(s) = std::env::var("WMFS_FAILPOINTS")
            && let Err(e) = load_from(&s)
        {
            // 配置写错不是"不变量破坏"，不用 panic（那只会杀死当前线程）：说明原因后结束整个进程
            eprintln!("环境变量 WMFS_FAILPOINTS 解析失败：{e}");
            std::process::exit(2);
        }
        let _ = STATE.compare_exchange(UNINIT, IDLE, Ordering::Relaxed, Ordering::Relaxed);
    });
}

/// 按环境变量的格式（`名字=动作;名字=动作`）一次配置多个 fail point；任何一项写错都不改变已有配置。
fn load_from(s: &str) -> Result<(), String> {
    for (name, action) in parse_config(s)? {
        insert(&name, &action)?;
    }
    Ok(())
}

fn insert(name: &str, text: &str) -> Result<(), String> {
    let action = parse_action(text)?;
    let mut map = points().write().unwrap_or_else(|e| e.into_inner());
    if matches!(action, Action::Off) {
        map.remove(name);
    } else {
        let hits = metrics::counter("wmfs_failpoint_hits_total", "fail point 触发次数", &[("name", name)]);
        map.insert(name.to_string(), Arc::new(Point { text: text.trim().to_string(), action, hits }));
    }
    STATE.store(if map.is_empty() { IDLE } else { ACTIVE }, Ordering::Relaxed);
    Ok(())
}

/// 执行一个 fail point：`return(c)` 时返回 `Err(c)`（c 不在 1..=65535 里时按 INTERNAL）。
pub fn fail_point(name: &str) -> Result<(), u16> {
    let Some(p) = lookup(name) else { return Ok(()) };
    match eval(&p.action, name, &p.hits) {
        Outcome::Return(c) => Err(u16::try_from(c).ok().filter(|c| *c != 0).unwrap_or(INTERNAL)),
        _ => Ok(()),
    }
}

/// 只问"这次是否触发"：`return(..)` 或 `corrupt` 触发时返回 true。
pub fn fail_point_hit(name: &str) -> bool {
    let Some(p) = lookup(name) else { return false };
    !matches!(eval(&p.action, name, &p.hits), Outcome::Nothing)
}

/// 取 `return(<整数>)` 配置的整数值（时钟偏移、速率等）；没有配置或这次没触发时返回 None。
pub fn fail_point_value(name: &str) -> Option<i64> {
    let p = lookup(name)?;
    match eval(&p.action, name, &p.hits) {
        Outcome::Return(v) => Some(v),
        _ => None,
    }
}

/// 设置（或用 `off` 删除）一个 fail point。名字或动作写错时返回中文原因，不改变已有配置。
pub fn set(name: &str, action: &str) -> Result<(), String> {
    load_env();
    check_name(name)?;
    insert(name, action)
}

/// 名字不能为空、不能含空白（环境变量与 set() 用同一条规则）。
fn check_name(name: &str) -> Result<(), String> {
    if name.is_empty() || name.contains(char::is_whitespace) {
        return Err(format!("fail point 名字不能为空或含空白：{name:?}"));
    }
    Ok(())
}

/// 删除一个 fail point。
pub fn clear(name: &str) {
    load_env();
    let _ = insert(name, "off");
}

/// 当前配置：(名字, 动作原文)，按名字排序。
pub fn list() -> Vec<(String, String)> {
    load_env();
    let map = points().read().unwrap_or_else(|e| e.into_inner());
    let mut v: Vec<(String, String)> = map.iter().map(|(k, p)| (k.clone(), p.text.clone())).collect();
    v.sort();
    v
}

/// 解析 `名字=动作;名字=动作` 形式的配置（环境变量的格式），并校验每个动作。
pub fn parse_config(s: &str) -> Result<Vec<(String, String)>, String> {
    let mut out = Vec::new();
    for item in s.split(';').map(str::trim).filter(|x| !x.is_empty()) {
        let (name, action) = item.split_once('=').ok_or_else(|| format!("缺少 '='：{item}"))?;
        let (name, action) = (name.trim(), action.trim());
        check_name(name)?;
        parse_action(action)?;
        out.push((name.to_string(), action.to_string()));
    }
    Ok(out)
}

fn parse_action(s: &str) -> Result<Action, String> {
    let s = s.trim();
    match s {
        "off" => return Ok(Action::Off),
        "panic" => return Ok(Action::Panic),
        "abort" => return Ok(Action::Abort),
        "corrupt" => return Ok(Action::Corrupt),
        _ => {}
    }
    let open = s.find('(').filter(|_| s.ends_with(')')).ok_or_else(|| format!("不认识的动作：{s}"))?;
    let (head, arg) = (&s[..open], &s[open + 1..s.len() - 1]);
    match head {
        "return" => {
            let a = arg.trim();
            match codes::from_name(a) {
                Some(c) => Ok(Action::Return(c as i64)),
                None => a
                    .parse::<i64>()
                    .map(Action::Return)
                    .map_err(|_| format!("return() 里应是错误码名（如 EIO、BUSY）或整数：{a}")),
            }
        }
        "sleep" => arg.trim().parse::<u64>().map(Action::Sleep).map_err(|_| format!("sleep() 里应是毫秒数：{s}")),
        "pct" | "times" => {
            let (n, rest) = arg.split_once(',').ok_or_else(|| format!("{head}() 需要两个参数：{s}"))?;
            let n: u64 = n.trim().parse().map_err(|_| format!("{head}() 的第一个参数应是非负整数：{s}"))?;
            let inner = Box::new(parse_action(rest)?);
            if head == "pct" {
                if n > 100 {
                    return Err(format!("pct() 的百分比应在 0..=100：{s}"));
                }
                Ok(Action::Pct(n as u32, inner))
            } else {
                Ok(Action::Times(AtomicU64::new(n), inner))
            }
        }
        _ => Err(format!("不认识的动作：{s}")),
    }
}

fn eval(a: &Action, name: &str, hits: &metrics::Counter) -> Outcome {
    match a {
        Action::Off => Outcome::Nothing,
        Action::Return(c) => {
            hits.inc();
            Outcome::Return(*c)
        }
        Action::Panic => {
            hits.inc();
            panic!("fail point {name} 触发：panic");
        }
        Action::Abort => {
            hits.inc();
            eprintln!("fail point {name} 触发：abort");
            std::process::abort();
        }
        Action::Sleep(ms) => {
            hits.inc();
            std::thread::sleep(Duration::from_millis(*ms));
            Outcome::Nothing
        }
        Action::Corrupt => {
            hits.inc();
            Outcome::Corrupt
        }
        Action::Pct(p, inner) => {
            if rand_below(100) < *p {
                eval(inner, name, hits)
            } else {
                Outcome::Nothing
            }
        }
        Action::Times(left, inner) => {
            if left.try_update(Ordering::Relaxed, Ordering::Relaxed, |x| x.checked_sub(1)).is_ok() {
                eval(inner, name, hits)
            } else {
                Outcome::Nothing
            }
        }
    }
}

/// pct() 用的伪随机数。wmfs-api 不依赖 core，所以这里用标准库 RandomState 播种的 xorshift；
/// 只用于"这次要不要触发"，不用于任何安全用途。
fn rand_below(n: u32) -> u32 {
    use std::cell::Cell;
    use std::hash::BuildHasher;
    thread_local! {
        static S: Cell<u64> = Cell::new(
            std::collections::hash_map::RandomState::new().hash_one(std::thread::current().id()) | 1,
        );
    }
    S.with(|s| {
        let mut x = s.get();
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        s.set(x);
        (x % n as u64) as u32
    })
}

#[cfg(test)]
mod tests;
