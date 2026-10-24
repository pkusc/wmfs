//! 快照（3.4.1.7）的单测：pin 的导出与复用、"向谁发了哪个快照"的记录、快照文件的往返与损坏检测。安装（规则 M5）的端到端在
//! testkit 的确定性模拟与多节点用例里。

use std::sync::Arc;

use redb::{Database, Durability};

use super::*;
use crate::meta::raft::storage::bootstrap;
use crate::proto::peer::SnapHeader;

fn db() -> Arc<Database> {
    let db = Database::builder().create_with_backend(redb::backends::InMemoryBackend::new()).unwrap();
    let kv = vec![(b"\x2fa".to_vec(), b"1".to_vec()), (b"\x2fb".to_vec(), vec![7; 300_000])];
    bootstrap(&db, &[1, 2, 3], &kv).unwrap();
    Arc::new(db)
}

fn dir(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("wmfs-snap-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

#[test]
fn export_pins_a_consistent_view_and_reuses_it() {
    let db = db();
    let pins = Pins::default();
    let s = pins.export(&db, 0, 2, 1, 7).unwrap();
    let m = s.get_metadata();
    assert_eq!((m.index, m.term, m.get_conf_state().voters.clone()), (1, 1, vec![1, 2, 3]));
    let d: SnapDesc = decode_body(&s.data).unwrap();
    assert_eq!((d.index, d.term, d.source_node), (1, 1, 7), "raft 消息里只有 SnapDesc：内容不进 raft 消息");
    let mut tx = db.begin_write().unwrap(); // 之后的写不影响已经 pin 住的视图（MVCC）
    tx.set_durability(Durability::Immediate);
    tx.open_table(KV).unwrap().insert(b"\x2fc".as_slice(), b"later".as_slice()).unwrap();
    tx.commit().unwrap();
    let pin = pins.find(1, 1).unwrap();
    let mut got = Vec::new();
    let (n, crc) = pin
        .frames(&mut |kv| {
            got.extend(kv);
            Ok(())
        })
        .unwrap();
    assert_eq!(n as usize, got.len());
    assert!(!got.iter().any(|(k, _)| k == b"\x2fc"), "pin 之后写的键不在快照里");
    assert_eq!(pin.header().unwrap().pairs_total, n);
    assert_ne!(crc, 0);
    let again = pins.export(&db, 0, 2, 1, 7).unwrap();
    assert_eq!(again.get_metadata().index, 1, "满足条件的 pin 直接复用");
    assert!(
        pins.export(&db, 0, 9, 1, 7).is_ok(),
        "接收方不在 pin 的成员配置里：另开一个 pin（这里新 pin 照样不含 9，只是不复用旧的）"
    );
    assert_eq!(lock(&pins.pins).len(), 2);
    assert!(pins.find(9, 9).is_none());
}

#[test]
fn sent_snapshots_are_reported_once_and_time_out() {
    let pins = Pins::default();
    pins.note_sent(2, 10, 3);
    pins.note_sent(3, 10, 3);
    pins.note_sent(2, 11, 3); // 同一个对端只记最新的
    assert_eq!(pins.take_sent(10, 3), vec![3]);
    assert!(pins.take_sent(10, 3).is_empty(), "报告过就忘掉");
    let now = clock::mono_ms();
    assert!(!pins.overdue(2, now), "刚发出：还没到 60 s");
    assert_eq!(pins.take_sent(11, 3), vec![2]);
    // 看门狗（坑 P11）：60 s 还没下载完 → 报告一次失败，之后忘掉它
    pins.note_sent(2, 12, 3);
    assert!(!pins.overdue(2, now + SENT_TIMEOUT_MS - 1000), "59 s：还在等");
    assert!(!pins.overdue(3, now + SENT_TIMEOUT_MS + 1000), "没给节点 3 发过快照");
    assert!(pins.overdue(2, now + SENT_TIMEOUT_MS + 1000), "61 s 还没下载完：报告失败");
    assert!(!pins.overdue(2, now + SENT_TIMEOUT_MS + 2000), "报告过就忘掉：不再重复报告");
    assert!(pins.take_sent(12, 3).is_empty(), "snap-serve 之后发完也不再报告");
}

#[test]
fn snapshot_files_round_trip_and_damage_is_detected() {
    let db = db();
    let pins = Pins::default();
    pins.export(&db, 0, 2, 1, 1).unwrap();
    let pin = pins.find(1, 1).unwrap();
    let d = dir("file");
    let mut w = SnapWriter::create(&d, pin.header().unwrap()).unwrap();
    let mut frames = 0;
    let (pairs, crc32c) = pin
        .frames(&mut |kv| {
            frames += 1;
            w.pairs(&kv)
        })
        .unwrap();
    assert!(frames >= 1);
    let d2 = dir("file-bad");
    let bad = SnapWriter::create(&d2, pin.header().unwrap()).unwrap();
    assert_eq!(
        bad.finish(&SnapTrailer { pairs, crc32c: crc32c ^ 1 }).err(),
        Some(ErrCode::EIO),
        "条数或 crc 对不上：不落盘"
    );
    let path = w.finish(&SnapTrailer { pairs, crc32c }).unwrap();
    assert_eq!(path, file_path(&d, 1, 1));
    let mut got = Vec::new();
    let h = read_file(&path, &mut |k, v| {
        got.push((k.to_vec(), v.to_vec()));
        Ok(())
    })
    .unwrap();
    assert_eq!((h.index, h.pairs_total, got.len() as u64), (1, pairs, pairs));
    assert!(got.iter().any(|(k, v)| k == b"\x2fb" && v.len() == 300_000));
    let mut bytes = std::fs::read(&path).unwrap();
    let mid = bytes.len() / 2;
    bytes[mid] ^= 0x40;
    std::fs::write(&path, &bytes).unwrap();
    assert_eq!(read_file(&path, &mut |_, _| Ok(())).err(), Some(ErrCode::EIO), "翻转一个字节：crc 不对");
    std::fs::write(&path, &bytes[..bytes.len() - 5]).unwrap();
    assert_eq!(read_file(&path, &mut |_, _| Ok(())).err(), Some(ErrCode::EIO), "截短：读不到结尾");
    assert!(std::fs::read_dir(&d2).unwrap().next().is_none(), "校验不过的临时文件已删掉");
    std::fs::remove_dir_all(&d).unwrap();
    std::fs::remove_dir_all(&d2).unwrap();
}

/// 快照文件的字节布局（3.4.1.7 与文件头的实现注记）：魔数、u32 小端长度 + postcard(SnapHeader)、每条记录 klen/vlen 小端、结尾；
/// 文件只给本用户读写（0600：它是整个 kv 表的副本）；启动时的清理删掉全部残留（含 .tmp），别的文件不碰。
#[test]
fn snapshot_file_layout_is_fixed() {
    let d = dir("layout");
    let h = SnapHeader { index: 5, term: 2, state_version: 4, conf_state: vec![8, 1], pairs_total: 1 };
    let mut w = SnapWriter::create(&d, h.clone()).unwrap();
    w.pairs(&[(b"k".to_vec(), b"vv".to_vec())]).unwrap();
    let crc = record_crc(0, b"k", b"vv");
    let path = w.finish(&SnapTrailer { pairs: 1, crc32c: crc }).unwrap();
    let hb = encode_body(&h);
    let mut want = b"WMFSSNP1".to_vec();
    want.extend((hb.len() as u32).to_le_bytes());
    want.extend(&hb);
    want.extend([1, 0, 0, 0, 2, 0, 0, 0]);
    want.extend(b"kvv");
    want.extend(b"WMFSEND!");
    want.extend(1u64.to_le_bytes());
    want.extend(crc.to_le_bytes());
    assert_eq!(std::fs::read(&path).unwrap(), want);
    use std::os::unix::fs::PermissionsExt;
    assert_eq!(std::fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o600, "只给本用户读写");
    std::fs::write(d.join("snap-recv-9-9.snap.tmp"), b"half").unwrap();
    std::fs::write(d.join("other"), b"x").unwrap();
    cleanup(&d, false);
    assert!(path.exists(), "刚写的快照文件不到 10 min：不删");
    cleanup(&d, true);
    let left: Vec<_> = std::fs::read_dir(&d).unwrap().map(|e| e.unwrap().file_name()).collect();
    assert_eq!(left, vec![std::ffi::OsString::from("other")], "启动时删掉全部快照残留，别的文件不碰");
    std::fs::remove_dir_all(&d).unwrap();
}
