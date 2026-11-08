//! 分级日志与给 raft-rs 的 `SlogBridge`（design.md 2.2、3.4.1.1）。
//!
//! 用法：`log::info("raft", format_args!("applied {}", idx))` 或 `log::warn("init", "……")`；
//! 不写宏（design.md 0.2 第 2 条），所以是普通函数，消息参数实现 `Display` 即可（&str、String、format_args!）。
//! 级别由环境变量 `WMFS_LOG`（error / warn / info / debug，默认 info）决定；默认写 stderr，
//! [`set_file`] 之后改为追加写文件（例如 `<data_dir>/logs/server.log`）。
//! 每行一条：`2026-10-21T01:41:27.123Z INFO  raft: 消息`（UTC 时间）。

use std::fmt::{self, Display, Write as _};
use std::fs::{File, OpenOptions};
use std::io::Write as _;
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU8, Ordering};

use slog::KV as _;

use super::{ErrCode, clock};
pub use wmfs_api::units::utc_string; // 实现注记（M14）：挪进 wmfs-api（wmfs-mgr 的日志也用它），这里重新导出

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
    Error = 1,
    Warn = 2,
    Info = 3,
    Debug = 4,
}

impl Level {
    pub fn parse(s: &str) -> Option<Level> {
        match s.trim().to_ascii_lowercase().as_str() {
            "error" => Some(Level::Error),
            "warn" | "warning" => Some(Level::Warn),
            "info" => Some(Level::Info),
            "debug" | "trace" => Some(Level::Debug),
            _ => None,
        }
    }
    fn tag(self) -> &'static str {
        match self {
            Level::Error => "ERROR",
            Level::Warn => "WARN ",
            Level::Info => "INFO ",
            Level::Debug => "DEBUG",
        }
    }
}

static LEVEL: AtomicU8 = AtomicU8::new(0); // 0 = 还没读环境变量
static FILE: Mutex<Option<File>> = Mutex::new(None);

/// 当前级别（第一次调用时读 `WMFS_LOG`）。
pub fn level() -> Level {
    match LEVEL.load(Ordering::Relaxed) {
        1 => Level::Error,
        2 => Level::Warn,
        3 => Level::Info,
        4 => Level::Debug,
        _ => {
            // 0 = 还没读环境变量
            let l = std::env::var("WMFS_LOG").ok().and_then(|s| Level::parse(&s)).unwrap_or(Level::Info);
            set_level(l);
            l
        }
    }
}

pub fn set_level(l: Level) {
    LEVEL.store(l as u8, Ordering::Relaxed);
}

pub fn enabled(l: Level) -> bool {
    l <= level()
}

/// 之后的日志追加写到 `path`（文件不存在就创建；目录要事先建好）。
pub fn set_file(path: &Path) -> Result<(), ErrCode> {
    let f = OpenOptions::new().create(true).append(true).open(path)?;
    *FILE.lock().unwrap_or_else(|e| e.into_inner()) = Some(f);
    Ok(())
}

pub fn error(component: &str, msg: impl Display) {
    log(Level::Error, component, msg);
}
pub fn warn(component: &str, msg: impl Display) {
    log(Level::Warn, component, msg);
}
pub fn info(component: &str, msg: impl Display) {
    log(Level::Info, component, msg);
}
pub fn debug(component: &str, msg: impl Display) {
    log(Level::Debug, component, msg);
}

/// 写一条日志；整行先拼好再一次写出，多线程不会交错。写日志本身失败时忽略（无处可报）。
pub fn log(l: Level, component: &str, msg: impl Display) {
    if !enabled(l) {
        return;
    }
    let mut line = utc_string(clock::wall_ms());
    let _ = writeln!(line, " {} {}: {}", l.tag(), component, msg);
    let mut f = FILE.lock().unwrap_or_else(|e| e.into_inner());
    match f.as_mut() {
        Some(file) => {
            let _ = file.write_all(line.as_bytes());
        }
        None => {
            let _ = std::io::stderr().write_all(line.as_bytes());
        }
    }
}

/// 把 raft-rs 的 slog 日志转到我们自己的日志：`slog::Logger::root(SlogBridge, slog::o!())`（3.4.1.1）。
pub struct SlogBridge;

impl slog::Drain for SlogBridge {
    type Ok = ();
    type Err = slog::Never;
    fn log(&self, r: &slog::Record<'_>, values: &slog::OwnedKVList) -> Result<(), slog::Never> {
        let l = match r.level() {
            slog::Level::Critical | slog::Level::Error => Level::Error,
            slog::Level::Warning => Level::Warn,
            slog::Level::Info => Level::Info,
            slog::Level::Debug | slog::Level::Trace => Level::Debug,
        };
        if enabled(l) {
            let mut kv = KvText(format!("{}", r.msg()));
            let _ = r.kv().serialize(r, &mut kv);
            let _ = values.serialize(r, &mut kv);
            log(l, "raft", kv.0);
        }
        Ok(())
    }
}

/// 把 slog 的键值对拼成 ` key=value`。
struct KvText(String);

impl slog::Serializer for KvText {
    fn emit_arguments(&mut self, key: slog::Key, val: &fmt::Arguments<'_>) -> slog::Result {
        let _ = write!(self.0, " {key}={val}");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn level_parsing() {
        assert_eq!(Level::parse("DEBUG"), Some(Level::Debug));
        assert_eq!(Level::parse("warning"), Some(Level::Warn));
        assert_eq!(Level::parse("loud"), None);
        assert!(Level::Error < Level::Debug);
    }

    #[test]
    fn slog_bridge_writes_through_without_panicking() {
        let logger = slog::Logger::root(SlogBridge, slog::o!("raft_id" => 1));
        slog::info!(logger, "became leader"; "term" => 2);
        slog::debug!(logger, "debug line");
    }
}
