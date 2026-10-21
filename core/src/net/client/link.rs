//! RpcClient 的连接管理（design.md 3.12、3.3.8）：建连与握手、每条连接的读线程（按 req_id 把回复交给等待者）、
//! 连接断开时结束它上面的在途调用，以及 `rpc-timer` 线程。是 `client.rs` 的子模块，可以直接用 RpcClient 的私有字段。

use std::sync::{Arc, Weak};
use std::time::Duration;

use crossbeam_channel::{Sender, bounded};

use super::{RpcClient, Waiter};
use crate::base::{ErrCode, clock, log};
use crate::hooks::traits::Conn;
use crate::net::conn::{self, ConnHandle, ConnOpts, OnUnsent, recv_checked};
use crate::net::handshake::{self, ClientSide};
use crate::proto::frame::{FLAG_LAST_CHUNK, FrameKind, InFrame};

impl RpcClient {
    /// 取（必要时新建）到 `scheme://addr` 的连接；建连与握手在 `deadline` 之前完成。
    pub(super) fn connection(&self, scheme: &str, addr: &str, deadline: u64) -> Result<ConnHandle, ErrCode> {
        let key = (scheme.to_string(), addr.to_string());
        if let Some(h) = self.conns().get(&key).filter(|h| !h.is_closed()) {
            return Ok(h.clone());
        }
        let Some((t, hook)) = self.transports.iter().find(|(t, _)| t.scheme() == scheme) else {
            log::warn("rpc", format_args!("本进程没有 {scheme} 传输插件，连不了 {scheme}://{addr}"));
            return Err(ErrCode::FEATURE_UNSUPPORTED);
        };
        let left = Duration::from_millis(deadline.saturating_sub(clock::mono_ms()).max(1));
        let c = t.connect(addr, self.pool.clone(), left).map_err(|e| {
            log::warn("rpc", format_args!("连接 {scheme}://{addr} 失败：{e}"));
            if e == ErrCode::EINVAL { e } else { ErrCode::ECONNRESET } // 地址本身写错不重试；其余都按"连接断开"
        })?;
        let (ready_tx, ready_rx) = bounded(1);
        let (me, k) = (self.me.clone(), key.clone());
        // 请求帧超过这个传输的上限、没发出去：只让这一个调用失败（3.3.8），由 rpc-timer 结束它
        let me2 = self.me.clone();
        let on_unsent: OnUnsent = Arc::new(move |id, e| {
            if let Some(rpc) = me2.upgrade() {
                rpc.fail_soon(id, e);
            }
        });
        let opts = ConnOpts {
            queue: self.net.send_queue as usize,
            keepalive: true,
            hook: hook.clone(),
            on_unsent: Some(on_unsent),
        };
        let h = conn::spawn(c, opts, Box::new(move |h, c| reader(me, k, h, c, ready_tx)))?;
        if let Err(e) = ready_rx.recv_timeout(left).unwrap_or(Err(ErrCode::ETIMEDOUT)) {
            h.close();
            return Err(e);
        }
        let mut conns = self.conns();
        if let Some(o) = conns.get(&key).filter(|o| !o.is_closed()) {
            h.close(); // 两个线程同时建连：用先登记的那条
            return Ok(o.clone());
        }
        conns.insert(key, h.clone());
        Ok(h)
    }

    /// 读线程收到一帧回复。回复的 bulk 被丢弃了的帧（本端内存池 100 ms 内没有空槽，`bulk_dropped`）按 `BUSY` 交出
    /// （可重试）：缺数据的回复不能当成功（3.6.4 实现注记）。
    fn deliver(&self, f: InFrame) {
        let (id, kind, last) = (f.hdr.req_id, f.hdr.kind, f.hdr.flags & FLAG_LAST_CHUNK != 0);
        let r = if f.bulk_dropped { Err(ErrCode::BUSY) } else { Ok(f) };
        match kind {
            FrameKind::Response => {
                if let Some(p) = self.untrack(id) {
                    p.w.finish(r); // 找不到 = 保活 Ping 的回复，或已经超时的调用：忽略
                }
            }
            FrameKind::StreamChunk => self.stream_chunk(id, r, last),
            _ => {} // 服务端从不向客户端发请求：忽略
        }
    }

    /// 流的一帧：交给 StreamRx、把期限往后推；最后一帧（或出错）之后从在途表里摘掉。
    fn stream_chunk(&self, id: u64, r: Result<InFrame, ErrCode>, last: bool) {
        let end = last || r.is_err();
        {
            let mut m = self.track();
            let Some(p) = m.get_mut(&id) else { return };
            let Waiter::Stream(tx) = &p.w else { return };
            p.deadline = clock::mono_ms() + p.timeout_ms + 1;
            let _ = tx.send(r);
            if !end {
                return;
            }
            m.remove(&id);
        }
        self.update_gauge();
    }

    /// 一条连接断了：从连接表摘掉（如果还是它），它上面的在途调用全部以 ECONNRESET 结束。
    fn lost(&self, key: &(String, String), conn_id: u64) {
        {
            let mut conns = self.conns();
            if conns.get(key).is_some_and(|h| h.id() == conn_id) {
                conns.remove(key);
            }
        }
        for p in self.take_pending(|p| p.conn == conn_id) {
            p.w.finish(Err(ErrCode::ECONNRESET));
        }
    }
}

/// 客户端连接的读线程：先握手（结果经 `ready` 告诉建连的线程），再把收到的回复按 req_id 交出去。
fn reader(
    me: Weak<RpcClient>,
    key: (String, String),
    h: ConnHandle,
    c: Arc<dyn Conn>,
    ready: Sender<Result<(), ErrCode>>,
) {
    let r = match me.upgrade() {
        Some(rpc) => handshake::client(&h, &*c, &ClientSide { key: &rpc.key, role: rpc.role, cluster_id: [0; 16] }),
        None => Err(ErrCode::ECONNRESET),
    };
    let r = r.map_err(|e| match e {
        ErrCode::AUTH_FAILED | ErrCode::PROTO_UNSUPPORTED | ErrCode::FEATURE_UNSUPPORTED => e,
        _ => {
            log::debug("rpc", format_args!("与 {} 握手时连接出错：{e}", h.peer()));
            ErrCode::ECONNRESET
        }
    });
    let ok = r.is_ok();
    let _ = ready.send(r);
    if ok {
        let err = loop {
            let f = match recv_checked(&*c) {
                Ok(f) => f,
                Err(e) => break e,
            };
            match me.upgrade() {
                Some(rpc) => rpc.deliver(f),
                None => break ErrCode::ECONNRESET,
            }
        };
        log::debug("rpc", format_args!("到 {} 的连接断开：{err}", h.peer()));
    }
    h.close();
    if let Some(rpc) = me.upgrade() {
        rpc.lost(&key, h.id());
    }
}

/// rpc-timer：每 100 ms 扫描在途表，到期的调用以它的 `fail`（一般是 ETIMEDOUT）结束（唤醒 call/wait，或调用 cb）。
pub(super) fn timer(me: Weak<RpcClient>) {
    loop {
        std::thread::sleep(Duration::from_millis(100));
        let Some(rpc) = me.upgrade() else { return };
        let now = clock::mono_ms();
        for p in rpc.take_pending(|p| p.deadline <= now) {
            let code = p.fail;
            p.w.finish(Err(code));
        }
    }
}
