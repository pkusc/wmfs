//! init 与 join 的 target 部分（design.md 3.4.3、3.2.3）：解析 `--target PATH[:label,label]`、拒绝重复、嵌套或已有超级块的 target、
//! 每个 target 的初始 TargetRecV1 与 TARGET 超级块。从 `init.rs` 拆出来（每个文件 ≤ 400 行），说明见那里的文件头。

use std::path::Path;

use super::NodeFile;
use crate::base::config::TargetConfig;
use crate::base::sys;
use crate::data::superblock::TargetSuperblock;
use crate::meta::cluster::chains::TARGET_UP;
use crate::meta::schema::records::TargetRecV1;

/// 给每个 target 写超级块（见文件头的实现注记）：init 的 target_id 依次为 1..=n，与初始 KV 的 TargetRecV1 一致；join 的取自
/// `Joined.target_ids`。
pub(crate) fn write_superblocks(targets: &[TargetConfig], ids: &[u32], node: &NodeFile) -> Result<(), String> {
    let cluster_id = node.cluster_id_bytes()?;
    for (t, &target_id) in targets.iter().zip(ids) {
        let dir = Path::new(&t.path);
        let sb_path = dir.join("TARGET");
        std::fs::create_dir_all(dir).map_err(|e| format!("建 target 目录 {} 失败：{e}", t.path))?;
        let sb = TargetSuperblock {
            features_compat: 0,
            features_ro_compat: 0,
            features_incompat: 0,
            cluster_id,
            target_id,
            node_id: node.node_id,
            created_ms: node.created_ms,
        };
        sys::atomic_write(&sb_path, &sb.encode()).map_err(|e| format!("写 {} 失败：{e}", sb_path.display()))?;
    }
    Ok(())
}

/// 一个 `[[target]]` 的初始 TargetRecV1（见 `initial_kv` 的实现注记）。
pub(crate) fn target_rec(t: &TargetConfig, node: u32, created_ms: u64) -> Result<TargetRecV1, String> {
    Ok(TargetRecV1 {
        node,
        path: t.path.clone(),
        labels: t.labels.clone(),
        phys_devs: vec![],
        capacity: t.capacity_bytes()?,
        boot_id: 0,
        state: TARGET_UP,
        last_up_ms: created_ms,
        read_only: false,
    })
}

/// 解析 `--target PATH[:label,label]`（3.4.3）：路径换成绝对路径，标签只能由小写字母、数字与 `-` 组成。
/// 路径里不能有 `:`（它分隔标签）。容量不限（`capacity = "0"`；`--target` 没有容量的写法）。
pub fn parse_target(s: &str) -> Result<TargetConfig, String> {
    let (path, labels) = s.split_once(':').unwrap_or((s, ""));
    if path.is_empty() {
        return Err(format!("--target {s:?}：缺少路径"));
    }
    let path = std::path::absolute(path).map_err(|e| format!("--target {s:?}：{e}"))?;
    let labels: Vec<String> = labels.split(',').filter(|l| !l.is_empty()).map(String::from).collect();
    let ok = |l: &String| l.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
    if let Some(bad) = labels.iter().find(|l| !ok(l)) {
        return Err(format!("--target {s:?}：标签 {bad:?} 只能由小写字母、数字与 - 组成"));
    }
    Ok(TargetConfig { path: path.display().to_string(), labels, capacity: "0".into() })
}

/// 同一个节点的 target 不能重复，也不能一个在另一个里面（按路径的各段比较，例如 /data 与 /data/t2）；目录里不能已经有超级块。
pub(crate) fn check_targets(ts: &[TargetConfig]) -> Result<(), String> {
    for (i, a) in ts.iter().enumerate() {
        if Path::new(&a.path).join("TARGET").exists() {
            return Err(format!("{} 里已经有 target 超级块（属于别的集群？）：拒绝初始化", a.path));
        }
        for b in &ts[i + 1..] {
            let (pa, pb) = (Path::new(&a.path), Path::new(&b.path));
            if pa == pb {
                return Err(format!("target {} 给了两次", a.path));
            }
            if pa.starts_with(pb) || pb.starts_with(pa) {
                return Err(format!("target {} 与 {} 一个在另一个里面：每个 target 要用各自的目录", a.path, b.path));
            }
        }
    }
    Ok(())
}
