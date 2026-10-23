//! 按 altitude 排好的插件与统一的调用包装（design.md 3.7.4、3.12）。
//!
//! 每次钩子调用都包一层（[`HookCall`]）：`fail_point("plugin.<name>.<hook>")?`（名字在注册时拼好存起来，热路径不分配）→
//! 计时 → 调用 → 超预算计入 `wmfs_hook_over_budget_total{hook,plugin}` → 错误计数 `wmfs_hook_errors_total`。
//! transport 的包装在 core 的连接写线程里（`net/conn.rs`，包在 `Conn::send` 外层：触发 = 这条连接作废）；收帧那一半由
//! 传输插件自己计时（读到帧头之后开始，读完 body + bulk 时调用 [`HookCall::exit`]，不过 fail point，3.7.4）。
//!
//! 实现：M1 有 authn、plugin_list 与 transport 的包装；M4 有 authz 与 placement；其余钩子随各自的里程碑实现（函数体里写着里程碑号）。
//! 实现注记（M5）：qos、codec、transform、chunk_store 的插件连同各自的调用包装（[`HookCall`]）在这里登记好；骨架里的
//! `qos()`、`codec()`、`transform()` 照旧返回插件本身，调用方要经包装时用 `*_call()` 拿到 (插件, 包装)，再用
//! [`HookCall::call`] 调一次插件方法（enter 过 fail point `plugin.<名字>.<钩子>` → 调用 → exit 计数计时）。`policy()` 先回一个
//! 只给"默认"答案的 [`BuiltinPolicy`]（第 5 章的表达式在 M7；后台调度器也经它拿求值器，只有这一个来源）。

use std::sync::Arc;
use std::time::Instant;

use wmfs_api::failpoint::fail_point;
use wmfs_api::metrics::{self, Counter, Histogram};

use crate::base::ErrCode;
use crate::hooks::manifest::{Hook, Manifest};
use crate::hooks::policy::{BuiltinPolicy, PolicyEvaluator};
use crate::hooks::registry::PluginSet;
use crate::hooks::traits::{
    AuthnInput, AuthnPlugin, AuthnResult, AuthzInput, AuthzPlugin, ChainView, ChunkStore, CodecPlugin, ConnIdentity,
    Decision, KeyLookup, PlacementInput, PlacementPlugin, QosPlugin, QuotaDecision, QuotaInput, TransformKind,
    TransformPlugin, Transport,
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

    /// 经这层包装调一次插件方法：[`enter`](HookCall::enter)（fail point 触发时不调用 `f`，直接返回注入的码）→ `f` →
    /// [`exit`](HookCall::exit)。实现注记（M5）：新增（只增）。
    pub fn call<T>(&self, f: impl FnOnce() -> Result<T, ErrCode>) -> Result<T, ErrCode> {
        let t0 = self.enter()?;
        let r = f();
        self.exit(t0, r.is_ok());
        r
    }
}

/// 按 altitude 排好的插件；每次调用都经过 3.7.4 的包装。
pub struct Pipeline {
    set: PluginSet,
    authn: Vec<(Arc<dyn AuthnPlugin>, HookCall)>, // altitude 升序；每个插件配好自己的调用包装
    authz: Vec<(Arc<dyn AuthzPlugin>, HookCall)>,
    placement: Vec<(Arc<dyn PlacementPlugin>, HookCall)>,
    qos: Option<(Arc<dyn QosPlugin>, HookCall)>,
    codecs: Vec<(Arc<dyn CodecPlugin>, HookCall)>,
    transforms: Vec<(Arc<dyn TransformPlugin>, HookCall)>,
    chunk_store: Option<(Arc<dyn ChunkStore>, HookCall)>,
    policy: BuiltinPolicy,
}

impl Pipeline {
    /// 由校验过的插件集合建管线（`PluginSet::validate` 之后）。
    pub fn new(set: PluginSet) -> Pipeline {
        let authn = set.authn_plugins().iter().map(|p| (p.clone(), HookCall::new(p.manifest(), Hook::Authn))).collect();
        let authz = set.authz_plugins().iter().map(|p| (p.clone(), HookCall::new(p.manifest(), Hook::Authz))).collect();
        let placement =
            set.placement_plugins().iter().map(|p| (p.clone(), HookCall::new(p.manifest(), Hook::Placement))).collect();
        let qos = set.qos_plugins().first().map(|p| (p.clone(), HookCall::new(p.manifest(), Hook::Qos)));
        let codecs =
            set.codec_plugins().iter().map(|p| (p.clone(), HookCall::new(p.manifest(), Hook::Codec))).collect();
        let transforms =
            set.transform_plugins().iter().map(|p| (p.clone(), HookCall::new(p.manifest(), Hook::Transform))).collect();
        let chunk_store = set.chunk_store_plugin().map(|p| (p.clone(), HookCall::new(p.manifest(), Hook::ChunkStore)));
        Pipeline { set, authn, authz, placement, qos, codecs, transforms, chunk_store, policy: BuiltinPolicy }
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

    /// authz（3.7.2）：altitude 升序串行；任一 Deny 即拒绝（用它给的码）；否则有 Allow 即允许；全部 Abstain（含一个
    /// authz 插件都没有）→ `EACCES`（失败即关闭）。fail point `plugin.<名字>.authz` 触发 = 以注入的码拒绝。
    pub fn authz(&self, input: &AuthzInput) -> Result<(), ErrCode> {
        let mut allowed = false;
        for (p, call) in &self.authz {
            let t0 = call.enter()?;
            let d = p.check(input);
            call.exit(t0, true); // 拒绝是正常的判定，不算插件出错
            match d {
                Decision::Deny(c) => return Err(c),
                Decision::Allow => allowed = true,
                Decision::Abstain => {}
            }
        }
        if allowed { Ok(()) } else { Err(ErrCode::EACCES) }
    }
    /// 按 3.7.2 合并多个插件的结果
    pub fn quota(&self, _input: &QuotaInput) -> QuotaDecision {
        unimplemented!("M10：Pipeline::quota")
    }
    /// placement（3.7.2）：altitude 升序，依次过滤/排序同一个候选链列表；core 再取前 k 条（3.4.2.1）。
    /// 没有 placement 插件时候选原样返回（core 已按 class/replicas 预先筛过）。
    pub fn place(&self, input: &PlacementInput, mut candidates: Vec<ChainView>) -> Result<Vec<ChainView>, ErrCode> {
        for (p, call) in &self.placement {
            let t0 = call.enter()?;
            let r = p.rank(input, &mut candidates);
            call.exit(t0, r.is_ok());
            r?;
        }
        Ok(candidates)
    }
    /// 唯一活跃的 qos 插件。只在数据节点上调用：服务端装配时已检查恰好有一个（没有 = 不变量破坏）。
    pub fn qos(&self) -> &Arc<dyn QosPlugin> {
        self.qos_call().unwrap_or_else(|| panic!("不变量破坏：服务端装配时已检查恰好有一个 qos 插件")).0
    }
    /// qos 插件连同它的调用包装（没有 qos 插件 → None）。
    pub fn qos_call(&self) -> Option<(&Arc<dyn QosPlugin>, &HookCall)> {
        self.qos.as_ref().map(|(p, h)| (p, h))
    }
    pub fn codec(&self, id: u8) -> Option<&Arc<dyn CodecPlugin>> {
        self.codec_call(id).map(|(p, _)| p)
    }
    /// 按编号找 codec 插件，连同它的调用包装。
    pub fn codec_call(&self, id: u8) -> Option<(&Arc<dyn CodecPlugin>, &HookCall)> {
        self.codecs.iter().find(|(p, _)| p.codec_id() == id).map(|(p, h)| (p, h))
    }
    pub fn transform(&self, kind: TransformKind, alg: u8) -> Option<&Arc<dyn TransformPlugin>> {
        self.transform_call(kind, alg).map(|(p, _)| p)
    }
    /// 按类别与算法编号（chunk 头 / layout 里的值）找 transform 插件，连同它的调用包装。
    pub fn transform_call(&self, kind: TransformKind, alg: u8) -> Option<(&Arc<dyn TransformPlugin>, &HookCall)> {
        self.transforms.iter().find(|(p, _)| p.kind() == kind && p.alg_id() == alg).map(|(p, h)| (p, h))
    }
    /// 唯一的 chunk_store 插件连同它的调用包装（没有 → None；服务端装配时检查）。
    pub fn chunk_store(&self) -> Option<(&Arc<dyn ChunkStore>, &HookCall)> {
        self.chunk_store.as_ref().map(|(p, h)| (p, h))
    }
    pub fn policy(&self) -> &dyn PolicyEvaluator {
        &self.policy
    }
    /// 3.7.2
    pub fn plugin_list(&self) -> PluginListV1 {
        self.set.plugin_list()
    }
}
// meta/ns/common.rs 里的便捷函数（命名空间代码只用它们，M4）：
//   authz(v, c, op, target, parent, victim) -> Result<(), ErrCode>      // 实现注记（M4）：多一个 victim（unlink/rename 的粘滞位），
//                                                                       // inode 用 ns::common::Node（号 + 记录：InodeView 要 ino）
//   new_layout(v, c, eff, ino, epoch, size_hint) -> Result<LayoutV1, ErrCode>   // 候选链 + gen_chains + Pipeline::place + 取 k 条（3.4.2.1）
