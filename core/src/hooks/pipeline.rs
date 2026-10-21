//! 按 altitude 排好的插件与统一的调用包装（design.md 3.7.4、3.12）。
//!
//! 每次钩子调用都包一层：`fail_point("plugin.<name>.<hook>")?`（名字在注册时拼好存起来，热路径不分配）→ 计时 →
//! 调用 → 超预算计入 `wmfs_hook_over_budget_total{hook,plugin}` → 错误计数 `wmfs_hook_errors_total`。
//!
//! 接口骨架（M0）：签名以这里为准，实现在 M1（框架）与 M4 起（各钩子）。

use std::sync::Arc;

use crate::base::ErrCode;
use crate::hooks::policy::PolicyEvaluator;
use crate::hooks::traits::{
    AuthzInput, ChainView, CodecPlugin, PlacementInput, QosPlugin, QuotaDecision, QuotaInput, TransformKind,
    TransformPlugin,
};
use crate::meta::schema::records::PluginListV1;

/// 按 altitude 排好的插件；每次调用都经过 3.7.4 的包装。
pub struct Pipeline {/* PluginSet */}

#[allow(unused_variables)] // 骨架：函数体在 M1/M4 实现，实现后删除这一行
impl Pipeline {
    /// 串行；任一 Deny → Err；全部 Abstain → EACCES
    pub fn authz(&self, input: &AuthzInput) -> Result<(), ErrCode> {
        unimplemented!("M4：Pipeline::authz")
    }
    /// 按 3.7.2 合并多个插件的结果
    pub fn quota(&self, input: &QuotaInput) -> QuotaDecision {
        unimplemented!("M10：Pipeline::quota")
    }
    /// 依次 rank
    pub fn place(&self, input: &PlacementInput, candidates: Vec<ChainView>) -> Result<Vec<ChainView>, ErrCode> {
        unimplemented!("M4：Pipeline::place")
    }
    pub fn qos(&self) -> &Arc<dyn QosPlugin> {
        unimplemented!("M5：Pipeline::qos")
    }
    pub fn codec(&self, id: u8) -> Option<&Arc<dyn CodecPlugin>> {
        unimplemented!("M5：Pipeline::codec")
    }
    pub fn transform(&self, kind: TransformKind, alg: u8) -> Option<&Arc<dyn TransformPlugin>> {
        unimplemented!("M5：Pipeline::transform")
    }
    pub fn policy(&self) -> &dyn PolicyEvaluator {
        unimplemented!("M7：Pipeline::policy")
    }
    /// 3.7.2
    pub fn plugin_list(&self) -> PluginListV1 {
        unimplemented!("M1：Pipeline::plugin_list")
    }
}
// meta/ns/common.rs 里的便捷函数（命名空间代码只用它们，M4）：
//   authz(v, c, op, target: Option<&InodeV1>, parent: Option<&InodeV1>) -> Result<(), ErrCode>
//   new_layout(v, c, eff, ino, size_hint) -> Result<LayoutV1, ErrCode>   // 候选链 + gen_chains + Pipeline::place + 取 k 条（3.4.2.1）
