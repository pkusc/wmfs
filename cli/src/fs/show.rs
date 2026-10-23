//! `wmfs fs` 的输出（给人看的文字与 `--json`）：stat、ls、错误信息。

use serde_json::json;
use wmfs_core::base::ErrCode;
use wmfs_core::base::clock::ts_ms;
use wmfs_core::base::log::utc_string;
use wmfs_core::base::units::fmt_size;
use wmfs_core::meta::ns::common::{S_ISGID, S_ISUID, S_ISVTX};
use wmfs_core::meta::schema::records::{
    F_INLINE, KIND_BLK, KIND_CHR, KIND_DIR, KIND_FIFO, KIND_FILE, KIND_SOCK, KIND_SYMLINK, LayoutV1,
};
use wmfs_core::proto::meta::{AttrOut, MetaReply};
use wmfs_core::proto::peer::ClusterMap;

use super::Done;

/// 错误码的说明：POSIX errno 用系统的说明（例如 "Directory not empty"），集群错误码用名字。
pub fn strerror(code: ErrCode) -> String {
    if !code.is_posix() {
        return code.to_string();
    }
    let s = std::io::Error::from_raw_os_error(code.0 as i32).to_string();
    s.split(" (os error").next().unwrap_or(&s).to_string()
}

/// 创建类请求的回复里的 inode 号。
pub fn ino_of(r: &MetaReply) -> u64 {
    match r {
        MetaReply::Entry { attr } => attr.ino,
        MetaReply::Opened(o) => o.attr.ino,
        _ => 0,
    }
}

fn kind_name(k: u8) -> (&'static str, &'static str, char) {
    match k {
        KIND_FILE => ("file", "普通文件", '-'),
        KIND_DIR => ("dir", "目录", 'd'),
        KIND_SYMLINK => ("symlink", "符号链接", 'l'),
        KIND_FIFO => ("fifo", "fifo", 'p'),
        KIND_SOCK => ("socket", "socket", 's'),
        KIND_CHR => ("chardev", "字符设备", 'c'),
        KIND_BLK => ("blockdev", "块设备", 'b'),
        _ => ("unknown", "未知", '?'),
    }
}

/// `-rw-r--r--` 这样的权限串（含 setuid/setgid/粘滞位）。
fn mode_str(kind: u8, mode: u32) -> String {
    let mut s = String::from(kind_name(kind).2);
    let special = [(S_ISUID, 's', 'S'), (S_ISGID, 's', 'S'), (S_ISVTX, 't', 'T')];
    for (i, shift) in [6, 3, 0].into_iter().enumerate() {
        let b = (mode >> shift) & 7;
        s.push(if b & 4 != 0 { 'r' } else { '-' });
        s.push(if b & 2 != 0 { 'w' } else { '-' });
        let (bit, with_x, without_x) = special[i];
        s.push(match (mode & bit != 0, b & 1 != 0) {
            (true, true) => with_x,
            (true, false) => without_x,
            (false, true) => 'x',
            (false, false) => '-',
        });
    }
    s
}

pub fn stat(path: &str, a: &AttrOut, layout: Option<&LayoutV1>, map: &ClusterMap) -> Done {
    let (kind, kind_zh, _) = kind_name(a.kind);
    let mut text = format!(
        "{path}\n  类型 {kind_zh}  ino={}  nlink={}  mode={:04o}（{}）  uid={}  gid={}  size={}  change_id={}\n  \
         mtime {}  ctime {}  btime {}",
        a.ino,
        a.nlink,
        a.mode,
        mode_str(a.kind, a.mode),
        a.uid,
        a.gid,
        a.size,
        a.change_id,
        utc_string(ts_ms(a.mtime)),
        utc_string(ts_ms(a.ctime)),
        utc_string(ts_ms(a.btime)),
    );
    let chain = |id: u32| map.chains.iter().find(|(i, _)| *i == id).map(|(_, c)| c);
    let mut chains_json = vec![];
    if let Some(l) = layout {
        let ids: Vec<String> = l.chains.iter().map(|c| format!("chain {c}")).collect();
        let actual: Vec<String> = l
            .chains
            .iter()
            .filter_map(|id| chain(*id).map(|c| format!("chain {id} 实际 {} 个成员", c.members.len())))
            .collect();
        let inline = if a.flags & F_INLINE != 0 { "yes（内容存在元数据里）" } else { "no" };
        text += &format!(
            "\n  layout：chunk={}、epoch={}、{} 条链（{}），声明 {} 副本（{}）\n  inline={inline}（阈值 {}）",
            fmt_size(l.chunk_size as u64),
            l.epoch,
            l.chains.len(),
            ids.join("、"),
            l.replicas,
            actual.join("，"),
            fmt_size(l.inline_max as u64),
        );
        for id in &l.chains {
            let members: Vec<u32> = chain(*id).map(|c| c.members.iter().map(|(t, _)| *t).collect()).unwrap_or_default();
            chains_json.push(json!({"id": id, "replicas": chain(*id).map_or(0, |c| c.replicas), "members": members}));
        }
    }
    let layout_json = layout.map(|l| {
        json!({"epoch": l.epoch, "chunk_size": l.chunk_size, "chains": l.chains, "replicas": l.replicas,
               "inline_max": l.inline_max, "checksum": l.checksum})
    });
    let j = json!({
        "path": path, "ino": a.ino, "kind": kind, "mode": a.mode, "nlink": a.nlink, "uid": a.uid, "gid": a.gid,
        "size": a.size, "change_id": a.change_id, "flags": a.flags, "rdev": a.rdev, "inline": a.flags & F_INLINE != 0,
        "mtime_ms": ts_ms(a.mtime), "ctime_ms": ts_ms(a.ctime), "btime_ms": ts_ms(a.btime),
        "layout": layout_json, "chains": chains_json,
    });
    Done { text, json: j }
}

/// `ls`（`long` = `-l`：权限、链接数、属主、大小、mtime）。
pub fn ls(entries: Vec<(Vec<u8>, u64, u8, Option<AttrOut>)>, long: bool) -> Done {
    let mut lines = Vec::new();
    let mut js = Vec::new();
    for (name, ino, kind, attr) in entries {
        let name = String::from_utf8_lossy(&name).into_owned();
        let mut j = json!({"name": name, "ino": ino, "kind": kind_name(kind).0});
        match attr.filter(|_| long) {
            Some(a) => {
                lines.push(format!(
                    "{} {:>3} {:>6} {:>6} {:>10} {} {name}",
                    mode_str(a.kind, a.mode),
                    a.nlink,
                    a.uid,
                    a.gid,
                    a.size,
                    utc_string(ts_ms(a.mtime)),
                ));
                j["mode"] = json!(a.mode);
                j["nlink"] = json!(a.nlink);
                j["uid"] = json!(a.uid);
                j["gid"] = json!(a.gid);
                j["size"] = json!(a.size);
                j["mtime_ms"] = json!(ts_ms(a.mtime));
            }
            None => lines.push(name),
        }
        js.push(j);
    }
    Done { text: lines.join("\n"), json: json!({ "entries": js }) }
}
