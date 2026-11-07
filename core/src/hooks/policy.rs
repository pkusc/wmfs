//! 策略求值点（design.md 3.7.7，为二期 WASM 预留；`FileFacts` 见 3.12）。
//!
//! 首发只有 `BuiltinPolicy`（第 5 章的简单表达式，M7 起）。四个求值点都**不在每次读写路径上**：
//! CreatePlacement 在 create 时求值、结果写进 layout；TierCondition 在 tiering 任务里；AuditFilter 在写 changelog 时
//! （结果按 `(目录, attr_epoch)` 缓存）；AttrValidate 在 `wmfs set` 时。属性值以 `wasm:` 开头 → 首发返回
//! `FEATURE_UNSUPPORTED`（"二期功能"），不静默忽略。二期把 BuiltinPolicy 换成"WASM 优先、内置兜底"，调用点不变。

use crate::api::ops::op_name;
use crate::base::types::Ts;
use crate::meta::attrs::EffectiveAttrs;
use crate::meta::schema::records::ChangeRecV1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PolicyPoint {
    CreatePlacement,
    TierCondition,
    AuditFilter,
    AttrValidate,
}

pub struct PolicyInput<'a> {
    pub point: PolicyPoint,
    pub attrs: &'a EffectiveAttrs,
    pub file: Option<&'a FileFacts>,
    pub event: Option<&'a ChangeRecV1>,
    pub kv: Option<(&'a str, &'a str)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PolicyOutput {
    Default,
    Class(u16),
    Bool(bool),
    Reject(String),
}

pub trait PolicyEvaluator: Send + Sync {
    fn eval(&self, input: &PolicyInput) -> PolicyOutput;
}

/// 首发的内置策略求值器。实现注记（M5）：先只回"默认"（后台任务框架要一个求值器）；第 5 章的简单表达式在 M7 起加入。
/// 实现注记（M12）：AuditFilter（写 changelog 时，`meta/changelog.rs`）——`audit` 属性生效、操作在审计范围里（没写 `ops=` 时是命名空间
/// 修改、setattr、xattr、写入长度与属性变更：[`AUDIT_DEFAULT`]）、调用者在 `uids=` 里（没写 = 所有人）→ `Bool(true)`。其余求值点
/// 仍回"默认"（tier 条件在 M13；属性值的校验在属性注册表里做完了）。
pub struct BuiltinPolicy;

/// `audit=on` 没写 `ops=` 时审计的操作（ADR-0029 的名字）。
const AUDIT_DEFAULT: &[&str] = &[
    "Create",
    "Mkdir",
    "Mknod",
    "Symlink",
    "Link",
    "Unlink",
    "Rmdir",
    "Rename",
    "SetAttr",
    "SetXattr",
    "RemoveXattr",
    "SetSize",
    "AttrSet",
    "AttrUnset",
];

impl PolicyEvaluator for BuiltinPolicy {
    fn eval(&self, input: &PolicyInput) -> PolicyOutput {
        match (input.point, input.event) {
            (PolicyPoint::AuditFilter, Some(e)) => {
                let a = &input.attrs.audit;
                let op_in = if a.ops.is_empty() {
                    op_name(e.op).is_some_and(|n| AUDIT_DEFAULT.contains(&n))
                } else {
                    a.ops.contains(&e.op)
                };
                PolicyOutput::Bool(a.on && op_in && (a.uids.is_empty() || a.uids.contains(&e.uid)))
            }
            _ => PolicyOutput::Default,
        }
    }
}

/// 策略求值用（3.12）
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileFacts {
    pub ino: u64,
    pub size: u64,
    pub atime: Ts,
    pub mtime: Ts,
    pub btime: Ts,
    pub class_id: u16,
    pub uid: u32,
}
