//! audit-log（event_sink，design.md 4.13）：只取 `audited` 的 changelog 记录，按 JSON Lines（一行一个 `ChangeEvent`，带 seq 便于下游去重）
//! 追加到 `<mgr 目录>/audit/audit-YYYYMMDD.log`（UTC 日期；超过 100 MiB 换 `audit-YYYYMMDD.1.log`、`.2.log`……），每批 `fsync` 之后才算
//! 投递成功、由宿主提交游标 `audit-log`——至少一次投递：mgr 在写完、提交游标之前崩溃，重启后这一批会再写一次（按 seq 去重）。
//! 这个游标在 `audit` 第一次启用时已由 core 建好（3.8；没有就不投递，宿主不替它建），mgr 没运行期间的记录按游标保留（最多 7 天 /
//! 1000 万条）；超出部分被修剪时宿主读回显式的 410 并告警（`host/events.rs`，告警 `changelog_gap`），不是静默丢失。
//! 不留半行（实现注记（M14 审查））：写失败（例如 ENOSPC）就把文件截回写之前的长度；打开时文件末尾不是换行（上次写到一半进程崩了）就先截掉
//! 那半行——它所在的那一批没有提交游标，会整批重投。于是每一行都是完整的 JSON，按 seq 去重总能做。指标 `wmfs_audit_log_records_total`。

use std::fs::File;
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use wmfs_api::metrics::{self, Counter};
use wmfs_api::types::ChangeEvent;

use crate::host::EventSink;

/// 一个审计文件写到多大就换下一个（4.13）。
const ROTATE_BYTES: u64 = 100 << 20;

pub struct AuditLog {
    dir: PathBuf,
    written: Arc<Counter>,
}

impl AuditLog {
    pub fn new(dir: &Path) -> AuditLog {
        let written = metrics::counter("wmfs_audit_log_records_total", "写进审计日志文件的记录数", &[]);
        AuditLog { dir: dir.to_path_buf(), written }
    }

    /// 这一批写进哪个文件：今天（UTC）的、还没到 100 MiB 的那个（`audit-YYYYMMDD.log`、`.1.log`、`.2.log`……）。
    fn file(&self) -> PathBuf {
        let day = ymd(crate::now_ms());
        let mut i = 0;
        loop {
            let name = if i == 0 { format!("audit-{day}.log") } else { format!("audit-{day}.{i}.log") };
            let p = self.dir.join(name);
            if std::fs::metadata(&p).map_or(true, |m| m.len() < ROTATE_BYTES) {
                return p;
            }
            i += 1;
        }
    }
}

/// 墙钟毫秒 → UTC 日期 `YYYYMMDD`（审计文件名）。
pub fn ymd(ms: u64) -> String {
    wmfs_api::units::utc_string(ms)[..10].replace('-', "")
}

/// 文件末尾不是换行（见文件头）：截到最后一个换行之后。返回截完的长度（从文件末尾往前每次读 4 KiB 找换行）。
fn drop_partial_line(f: &File) -> std::io::Result<u64> {
    let len = f.metadata()?.len();
    let (mut end, mut buf) = (len, [0u8; 4096]);
    while end > 0 {
        let start = end.saturating_sub(buf.len() as u64);
        let chunk = &mut buf[..(end - start) as usize];
        f.read_exact_at(chunk, start)?;
        if let Some(i) = chunk.iter().rposition(|b| *b == b'\n') {
            end = start + i as u64 + 1;
            break;
        }
        end = start;
    }
    if end < len {
        f.set_len(end)?;
    }
    Ok(end)
}

impl EventSink for AuditLog {
    fn manifest_toml(&self) -> &'static str {
        include_str!("manifest.toml")
    }

    fn deliver(&mut self, batch: &[ChangeEvent]) -> Result<(), String> {
        let lines: Vec<String> =
            batch.iter().filter(|e| e.audited).filter_map(|e| serde_json::to_string(e).ok()).collect();
        if lines.is_empty() {
            return Ok(());
        }
        let path = self.file();
        let err = |e: std::io::Error| format!("写审计日志 {} 失败：{e}", path.display());
        std::fs::create_dir_all(&self.dir).map_err(err)?;
        let new = !path.exists();
        let mut f = std::fs::OpenOptions::new().create(true).read(true).append(true).open(&path).map_err(err)?;
        let start = drop_partial_line(&f).map_err(err)?;
        if let Err(e) = f.write_all((lines.join("\n") + "\n").as_bytes()).and_then(|()| f.sync_data()) {
            let _ = f.set_len(start); // 写失败：截回写之前的长度，不留半行（见文件头）
            return Err(err(e));
        }
        if new {
            std::fs::File::open(&self.dir).and_then(|d| d.sync_all()).map_err(err)?; // 新文件的目录项也落盘
        }
        self.written.add(lines.len() as u64);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_only_audited_events_as_json_lines() {
        assert_eq!(crate::host::check_manifest(include_str!("manifest.toml"), "event_sink").unwrap(), "audit-log");
        assert_eq!(
            (ymd(0), ymd(1_700_000_000_000), ymd(951_782_400_000)),
            ("19700101".into(), "20231114".into(), "20000229".into())
        );
        let dir = std::env::temp_dir().join(format!("wmfs-audit-test-{}", std::process::id()));
        let mut a = AuditLog::new(&dir);
        let ev =
            |seq, audited| ChangeEvent { seq, op: "Create".into(), name: "f".into(), audited, ..Default::default() };
        a.deliver(&[ev(1, true), ev(2, false), ev(3, true)]).unwrap();
        a.deliver(&[ev(4, false)]).unwrap();
        let text = std::fs::read_to_string(a.file()).unwrap();
        let seqs: Vec<u64> = text.lines().map(|l| serde_json::from_str::<ChangeEvent>(l).unwrap().seq).collect();
        assert_eq!(seqs, [1, 3], "只取 audited 的，一行一个");
        assert!(text.contains(r#""op":"Create""#), "演示里 grep 的就是它");
        // 上次写到一半进程崩了：文件末尾留着半行。再投递之后每一行都能解析（半行被截掉，它那一批会整批重投）
        let half = format!("{text}{{\"seq\":4,\"op\":\"Cre");
        std::fs::write(a.file(), half).unwrap();
        a.deliver(&[ev(4, true), ev(5, true)]).unwrap();
        let text = std::fs::read_to_string(a.file()).unwrap();
        let seqs: Vec<u64> = text.lines().map(|l| serde_json::from_str::<ChangeEvent>(l).unwrap().seq).collect();
        assert_eq!(seqs, [1, 3, 4, 5], "半行被截掉，后面的照常追加");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
