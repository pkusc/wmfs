//! changelog 操作码表 v1（ADR-0029、design.md 3.2.8）：`ChangeRecV1.op`（u16，持久化）与 `AuditSpec.ops`
//! （`audit=ops=create+unlink`）用的"数值 ↔ 名字"。core 与 wmfs-mgr 共用。
//!
//! 规则（ADR-0029）：数值**显式写出**，不从任何 Rust 枚举的顺序推导；0 无效；**只追加、永不改号、永不复用**。
//! 名字就是 `ChangeEvent.op` 的 JSON 值；按名字查数值时**不区分大小写**（`ops=create+unlink`、`ops=setattr` 都能解析）。
//! 追加新码不需要新 ADR，但必须同时更新 design.md 3.2.8 的说明与 `tests/golden/constants.golden`。
//! 不写 changelog 的修改：SessionHeartbeat、SetAtime、SetLock、DebugPut、消费者游标提交、fail point。
//! 旧版 mgr 遇到不认识的数时显示 `op#<数>`，不报错。

/// 全部操作码：(数值, 名字)，按数值升序。1–14 命名空间与会话，100–107 属性与系统事件，200–215 管理操作。
pub const CHANGE_OPS: &[(u16, &str)] = &[
    (1, "SessionOpen"),
    (2, "SessionClose"),
    (3, "Create"),
    (4, "Mkdir"),
    (5, "Mknod"),
    (6, "Symlink"),
    (7, "Link"),
    (8, "Unlink"), // ttl 删除也用它（extra 写 reason=ttl）；进了回收站时 extra 写 trash=<回收站里的新名字>
    (9, "Rmdir"),
    (10, "Rename"),
    (11, "SetAttr"), // 改 size 的 SetAttr 也是它
    (12, "SetXattr"),
    (13, "RemoveXattr"),
    (14, "SetSize"), // 写入提交：CommitWrite、AppendReserve、Fallocate（extra 写 via=<操作名>）
    (100, "AttrSet"),
    (101, "AttrUnset"),
    (102, "SessionEvicted"),
    (103, "PolicyViolation"), // extra 写 kind=<种类>
    (104, "SwitchLayout"),
    (105, "ChecksumRepaired"),
    (106, "DataLossDetected"),
    (107, "TrashExpire"),
    (200, "ClassCreate"),
    (201, "ClassUpdate"),
    (202, "ClassDelete"),
    (203, "KeyCreate"),
    (204, "KeyRevoke"),
    (205, "DataKeyCreate"),
    (206, "NodeJoin"),
    (207, "NodeDrain"),
    (208, "NodeRemove"),
    (209, "NodePromote"),
    (210, "ChainForceServing"),
    (211, "ClusterSet"),
    (212, "PluginListChange"),
    (213, "PluginListReset"),
    (214, "FeatureEnable"),
    (215, "TaskControl"), // extra 写 action=<动作>
];

/// 数值 → 名字；不认识的数（含 0）返回 None。
pub fn op_name(code: u16) -> Option<&'static str> {
    CHANGE_OPS.iter().find(|(c, _)| *c == code).map(|(_, n)| *n)
}

/// 名字 → 数值，**不区分大小写**（"create"、"Create"、"CREATE" 都是 3）。
pub fn op_code(name: &str) -> Option<u16> {
    CHANGE_OPS.iter().find(|(_, n)| n.eq_ignore_ascii_case(name)).map(|(c, _)| *c)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_unique_sorted_and_nonzero() {
        for w in CHANGE_OPS.windows(2) {
            assert!(w[0].0 < w[1].0, "操作码表必须按数值严格升序（于是数值唯一）：{:?} {:?}", w[0], w[1]);
        }
        assert!(CHANGE_OPS.iter().all(|(c, _)| *c != 0), "0 无效，不能出现在表里");
        assert_eq!(op_name(0), None);
    }

    #[test]
    fn names_unique_ignoring_case() {
        for (i, (_, a)) in CHANGE_OPS.iter().enumerate() {
            for (_, b) in &CHANGE_OPS[i + 1..] {
                assert!(!a.eq_ignore_ascii_case(b), "名字（不区分大小写）重复：{a} {b}");
            }
        }
    }

    #[test]
    fn round_trip_and_case_insensitive() {
        for (c, n) in CHANGE_OPS {
            assert_eq!(op_name(*c), Some(*n));
            assert_eq!(op_code(op_name(*c).unwrap()), Some(*c));
            assert_eq!(op_code(&n.to_lowercase()), Some(*c));
            assert_eq!(op_code(&n.to_uppercase()), Some(*c));
        }
        assert_eq!(op_code("create"), Some(3));
        assert_eq!(op_code("setattr"), Some(11));
        assert_eq!(op_code("nosuchop"), None);
        assert_eq!(op_name(15), None);
    }

    #[test]
    fn values_match_adr_0029() {
        // ADR-0029 表里写死的几个数（表一旦发布就不能改号）
        assert_eq!(op_code("SessionOpen"), Some(1));
        assert_eq!(op_code("SetSize"), Some(14));
        assert_eq!(op_code("AttrSet"), Some(100));
        assert_eq!(op_code("TrashExpire"), Some(107));
        assert_eq!(op_code("ClassCreate"), Some(200));
        assert_eq!(op_code("TaskControl"), Some(215));
        assert_eq!(CHANGE_OPS.len(), 14 + 8 + 16);
    }
}
