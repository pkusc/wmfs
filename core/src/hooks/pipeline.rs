//! 按 altitude 排好的插件与统一的调用包装（design.md 3.7.4、3.12）。
//!
//! 每次钩子调用都包一层（[`HookCall`]）：`fail_point("plugin.<name>.<hook>")?`（名字在注册时拼好存起来，热路径不分配）→
//! 计时 → 调用 → 超预算计入 `wmfs_hook_over_budget_total{hook,plugin}` → 错误计数 `wmfs_hook_errors_total`。
//! transport 的包装在 core 的连接写线程里（`net/conn.rs`，包在 `Conn::send` 外层：触发 = 这条连接作废）；收帧那一半由
//! 传输插件自己计时（读到帧头之后开始，读完 body + bulk 时调用 [`HookCall::exit`]，不过 fail point，3.7.4）。
//!
//! 实现：M1 有 authn、plugin_list 与 transport 的包装；其余钩子随各自的里程碑实现（函数体里写着里程碑号）。

use std::sync::Arc;
use std::time::Instant;

use wmfs_api::failpoint::fail_point;
use wmfs_api::metrics::{self, Counter, Histogram};

use crate::base::ErrCode;
use crate::hooks::manifest::{Hook, Manifest};
use crate::hooks::policy::PolicyEvaluator;
use crate::hooks::registry::PluginSet;
use crate::hooks::traits::{
    AuthnInput, AuthnPlugin, AuthnResult, AuthzInput, ChainView, CodecPlugin, ConnIdentity, KeyLookup, PlacementInput,
    QosPlugin, QuotaDecision, QuotaInput, TransformKind, TransformPlugin, Transport,
};
use crate::meta::schema::records::PluginListV1;

/// 一个插件在一个钩子上的调用包装：fail point 名与指标句柄在注册时准备好（3.7.4）。
pub struct HookCall {
    fp: String, // plugin.<name>.<hook>
    budget_ns: u64,
    calls: Arc<Counter>,
    secs: Arc<Histogram>,
    over: Arc<Counter>,
    errs: Arc<Counter>,
}

impl HookCall {
    pub fn new(m: &Manifest, hook: Hook) -> HookCall {
        let l = [("hook", hook.name()), ("plugin", m.name)];
        HookCall {
            fp: format!("plugin.{}.{}", m.name, hook.name()),
            budget_ns: m.budget_ns,
            calls: metrics::counter("wmfs_hook_calls_total", "钩子调用次数", &l),
            secs: metrics::histogram("wmfs_hook_seconds", "钩子单次调用耗时", &l, metrics::SECONDS_BUCKETS),
            over: metrics::counter("wmfs_hook_over_budget_total", "钩子调用超出时间预算的次数", &l),
            errs: metrics::counter("wmfs_hook_errors_total", "钩子调用出错（含 fail point 触发）的次数", &l),
        }
    }

    /// 调用前：先过 fail point；配置了 `return(码)` 时返回 `Err(码)`（并计为一次错误），调用方不再调用插件。
    pub fn enter(&self) -> Result<Instant, ErrCode> {
        fail_point(&self.fp).map_err(|c| {
            self.errs.inc();
            ErrCode(c)
        })?;
        Ok(Instant::now())
    }

    /// 调用后：计数、计时、超预算计数、错误计数（不过 fail point：传输插件给收帧计时也直接用它）。
    pub fn exit(&self, t0: Instant, ok: bool) {
        let ns = t0.elapsed().as_nanos() as u64;
        self.calls.inc();
        self.secs.observe(ns as f64 / 1e9);
        if self.budget_ns > 0 && ns > self.budget_ns {
            self.over.inc();
        }
        if !ok {
            self.errs.inc();
        }
    }
}

/// 按 altitude 排好的插件；每次调用都经过 3.7.4 的包装。
pub struct Pipeline {
    set: PluginSet,
    authn: Vec<(Arc<dyn AuthnPlugin>, HookCall)>, // altitude 升序；每个插件配好自己的调用包装
}

impl Pipeline {
    /// 由校验过的插件集合建管线（`PluginSet::validate` 之后）。
    pub fn new(set: PluginSet) -> Pipeline {
        let authn = set.authn_plugins().iter().map(|p| (p.clone(), HookCall::new(p.manifest(), Hook::Authn))).collect();
        Pipeline { set, authn }
    }

    pub fn transports(&self) -> &[Arc<dyn Transport>] {
        self.set.transports()
    }

    /// authn（3.7.2）：altitude 升序；任一 Reject 即拒绝、第一个 Accept 生效；全部 Abstain → 拒绝。
    /// 接受时返回连接身份与接受它的插件算出的 `server_proof`；拒绝一律 `AUTH_FAILED`（fail point 触发也按拒绝处理）。
    pub fn authn(&self, input: &AuthnInput, keys: &dyn KeyLookup) -> Result<(ConnIdentity, [u8; 32]), ErrCode> {
        for (p, call) in &self.authn {
            let t0 = call.enter().map_err(|_| ErrCode::AUTH_FAILED)?; // fail point 触发 = 拒绝
            match p.authenticate(input, keys) {
                AuthnResult::Abstain => call.exit(t0, true), // 弃权：问下一个插件
                AuthnResult::Reject => {
                    call.exit(t0, true); // 拒绝是正常的判定，不算插件出错
                    return Err(ErrCode::AUTH_FAILED);
                }
                AuthnResult::Accept(id) => {
                    let sp = p.server_proof(input, keys);
                    call.exit(t0, sp.is_some()); // 接受了却给不出 server_proof = 插件的 bug：计为错误，按拒绝处理
                    return sp.map(|sp| (id, sp)).ok_or(ErrCode::AUTH_FAILED);
                }
            }
        }
        Err(ErrCode::AUTH_FAILED) // 全部弃权 → 拒绝
    }

    /// 串行；任一 Deny → Err；全部 Abstain → EACCES
    pub fn authz(&self, _input: &AuthzInput) -> Result<(), ErrCode> {
        unimplemented!("M4：Pipeline::authz")
    }
    /// 按 3.7.2 合并多个插件的结果
    pub fn quota(&self, _input: &QuotaInput) -> QuotaDecision {
        unimplemented!("M10：Pipeline::quota")
    }
    /// 依次 rank
    pub fn place(&self, _input: &PlacementInput, _candidates: Vec<ChainView>) -> Result<Vec<ChainView>, ErrCode> {
        unimplemented!("M4：Pipeline::place")
    }
    pub fn qos(&self) -> &Arc<dyn QosPlugin> {
        unimplemented!("M5：Pipeline::qos")
    }
    pub fn codec(&self, _id: u8) -> Option<&Arc<dyn CodecPlugin>> {
        unimplemented!("M5：Pipeline::codec")
    }
    pub fn transform(&self, _kind: TransformKind, _alg: u8) -> Option<&Arc<dyn TransformPlugin>> {
        unimplemented!("M5：Pipeline::transform")
    }
    pub fn policy(&self) -> &dyn PolicyEvaluator {
        unimplemented!("M7：Pipeline::policy")
    }
    /// 3.7.2
    pub fn plugin_list(&self) -> PluginListV1 {
        self.set.plugin_list()
    }
}
// meta/ns/common.rs 里的便捷函数（命名空间代码只用它们，M4）：
//   authz(v, c, op, target: Option<&InodeV1>, parent: Option<&InodeV1>) -> Result<(), ErrCode>
//   new_layout(v, c, eff, ino, size_hint) -> Result<LayoutV1, ErrCode>   // 候选链 + gen_chains + Pipeline::place + 取 k 条（3.4.2.1）
