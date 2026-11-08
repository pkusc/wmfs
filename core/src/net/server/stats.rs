//! 一个消息族的服务端指标 [`FamilyStats`]（design.md 3.11；`Responder` 与 `StreamTx` 共用）。
//! 实现注记（M13 收尾）：从 `server.rs` 拆出来（那个文件接近 400 行，`Responder` 要加回复完成的回调，ADR-0044 第 5 条）。

use std::sync::{Arc, Mutex};
use std::time::Instant;

use wmfs_api::metrics::{self, Counter, Histogram};

use crate::base::ErrCode;

/// 一个消息族的服务端指标：`wmfs_rpc_requests_total{family,op,code}` 与 `wmfs_rpc_latency_seconds{family,op}`
/// （3.11：注册时加锁，热路径只做原子加）。code = OK 的计数器与直方图在创建时注册；某个错误码第一次出现时才注册它的计数器，
/// 之后从本族自己的小表里取（这把锁只在错误回复上用）。实现注记（M1）：`op` 暂时等于 family，各服务解出具体操作后再细分。
pub(super) struct FamilyStats {
    pub(super) family: &'static str,
    ok: Arc<Counter>,
    latency: Arc<Histogram>,
    errors: Mutex<Vec<(ErrCode, Arc<Counter>)>>,
}

impl FamilyStats {
    pub(super) fn new(family: &'static str) -> FamilyStats {
        let l = [("family", family), ("op", family)];
        let latency =
            metrics::histogram("wmfs_rpc_latency_seconds", "服务端处理请求的耗时", &l, metrics::SECONDS_BUCKETS);
        FamilyStats { family, ok: requests(family, "OK"), latency, errors: Mutex::new(Vec::new()) }
    }

    /// 记一个处理完的请求（`code` = 0 表示成功），耗时从收到请求算起。
    pub(super) fn record(&self, code: ErrCode, t0: Instant) {
        self.latency.observe(t0.elapsed().as_secs_f64());
        if code == ErrCode(0) {
            return self.ok.inc();
        }
        let mut errs = self.errors.lock().unwrap_or_else(|e| e.into_inner());
        match errs.iter().find(|(c, _)| *c == code) {
            Some((_, h)) => h.inc(),
            None => {
                let h = requests(self.family, &code.to_string());
                h.inc();
                errs.push((code, h));
            }
        }
    }
}

fn requests(family: &'static str, code: &str) -> Arc<Counter> {
    let l = [("family", family), ("op", family), ("code", code)];
    metrics::counter("wmfs_rpc_requests_total", "服务端处理的请求数", &l)
}
