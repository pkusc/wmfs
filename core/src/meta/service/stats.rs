//! `wmfs_meta_ops_total{op,code}`（design.md 3.3.4 的 M3 注记）：元数据服务处理的请求按 (操作, 结果码) 计数，并经 `Responder` 回复。
//! 实现注记（M11）：从 service.rs 挪出来（每个文件 ≤ 400 行）。

use std::sync::{Arc, Mutex};

use wmfs_api::metrics::{self, Counter};

use crate::base::{ErrCode, lock};
use crate::net::server::Responder;
use crate::proto::encode_body;
use crate::proto::meta::MetaReply;

/// `wmfs_meta_ops_total{op,code}`：每个 (操作, 结果码) 的计数器第一次出现时注册，之后从这张小表里取。
#[derive(Default)]
pub(super) struct OpStats {
    counters: Mutex<Vec<(&'static str, ErrCode, Arc<Counter>)>>,
}

impl OpStats {
    pub(super) fn reply(&self, r: Responder, op: &'static str, res: Result<MetaReply, ErrCode>) {
        self.reply_raw(r, op, res.map(|reply| encode_body(&reply)));
    }

    pub(super) fn reply_raw(&self, r: Responder, op: &'static str, res: Result<Vec<u8>, ErrCode>) {
        let code = res.as_ref().err().copied().unwrap_or(ErrCode(0));
        self.counter(op, code).inc();
        match res {
            Ok(body) => r.reply(body, vec![]),
            Err(c) => r.error(c, 0),
        }
    }

    pub(super) fn counter(&self, op: &'static str, code: ErrCode) -> Arc<Counter> {
        let mut v = lock(&self.counters);
        if let Some((_, _, c)) = v.iter().find(|(o, c, _)| *o == op && *c == code) {
            return c.clone();
        }
        let name = if code == ErrCode(0) { "OK".to_string() } else { code.to_string() };
        let c = metrics::counter("wmfs_meta_ops_total", "元数据服务处理的请求数", &[("op", op), ("code", &name)]);
        v.push((op, code, c.clone()));
        c
    }
}
