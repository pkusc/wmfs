//! 策略求值点（design.md 3.7.7，为二期 WASM 预留；`FileFacts` 见 3.12）。
//!
//! 首发只有 `BuiltinPolicy`（第 5 章的简单表达式，M7 起）。四个求值点都**不在每次读写路径上**：
//! CreatePlacement 在 create 时求值、结果写进 layout；TierCondition 在 tiering 任务里；AuditFilter 在写 changelog 时
//! （结果按 `(目录, attr_epoch)` 缓存）；AttrValidate 在 `wmfs set` 时。属性值以 `wasm:` 开头 → 首发返回
//! `FEATURE_UNSUPPORTED`（"二期功能"），不静默忽略。二期把 BuiltinPolicy 换成"WASM 优先、内置兜底"，调用点不变。

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
