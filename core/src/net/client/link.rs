//! RpcClient 的连接管理（design.md 3.12、3.3.8）：建连与握手、每条连接的读线程（按 req_id 把回复交给等待者）、
//! 连接断开时结束它上面的在途调用，以及 `rpc-timer` 线程。是 `client.rs` 的子模块，可以直接用 RpcClient 的私有字段。

use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::Duration;

use crossbeam_channel::{Sender, bounded};

use super::{RpcClient, Waiter};
use crate::base::{ErrCode, clock, log};
use crate::hooks::traits::Conn;
use crate::net::conn::{self, ConnHandle, ConnOpts, OnUnsent, recv_checked};
use crate::net::handshake::{self, ClientSide};
use crate::proto::frame::{FLAG_LAST_CHUNK, FrameKind, InFrame};

/// 一次正在进行的建连（单飞）：建连的线程做完把结果放进 `result` 并唤醒等待者。
#[derive(Default)]
pub(super) struct Dial {
    result: Mutex<Option<Result<ConnHandle, ErrCode>>>,
    cv: Condvar,
}

impl Dial {
    /// 建连的线程交出结果并唤醒全部等待者（只有第一次交出的算数）。
    fn publish(&self, r: Result<ConnHandle, ErrCode>) {
        let mut g = self.result.lock().unwrap_or_else(|e| e.into_inner());
        if g.is_none() {
            *g = Some(r);
        }
        drop(g);
        self.cv.notify_all();
    }

    /// 等建连的线程给出结果，最多等到 `deadline`（超时 `ETIMEDOUT`）。
    fn wait(&self, deadline: u64) -> Result<ConnHandle, ErrCode> {
        let left = Duration::from_millis(deadline.saturating_sub(clock::mono_ms()));
        let g = self.result.lock().unwrap_or_else(|e| e.into_inner());
        let (g, _) = self.cv.wait_timeout_while(g, left, |r| r.is_none()).unwrap_or_else(|e| e.into_inner());
        g.clone().unwrap_or(Err(ErrCode::ETIMEDOUT))
    }
}

/// 建连的线程离开 `connection` 时（正常返回，或传输插件 panic 时的栈展开）一定交出结果、摘掉 dialing 里的项：
/// 否则之后到这个对端的调用都会白等到各自的期限。正常路径上结果已经交出过，这里的 `ECONNRESET` 不起作用。
struct DialGuard<'a> {
    rpc: &'a RpcClient,
    key: &'a (String, String),
    dial: Arc<Dial>,
}

impl Drop for DialGuard<'_> {
    fn drop(&mut self) {
        self.dial.publish(Err(ErrCode::ECONNRESET));
        self.rpc.dialing().remove(self.key);
    }
}

impl RpcClient {
    /// 取（必要时新建）到 `scheme://addr` 的连接；建连与握手在 `deadline` 之前完成。
    /// 建连按 `(scheme, 地址)` 单飞（ADR-0031 第 11 条）：同一对端同时只有一个线程建连，其余线程等它的结果——成功就共享
    /// 这条连接；失败就都拿到同一个错误（之后各自按 3.3.8 重试）。等待也以各自的 `deadline` 为限（超时 `ETIMEDOUT`）。
    pub(super) fn connection(&self, scheme: &str, addr: &str, deadline: u64) -> Result<ConnHandle, ErrCode> {
        let key = (scheme.to_string(), addr.to_string());
        if let Some(h) = self.live_conn(&key) {
            return Ok(h);
        }
        let (dial, mine) = {
            let mut m = self.dialing();
            // 拿着 dialing 锁再查一次连接表：建连的线程先登记连接、再交出结果、最后才摘掉 dialing 里的项，所以这里两边都
            // 查不到 = 确实没人在建（第一次查完之后别人恰好建好的连接在这里查得到）。加锁顺序只有 dialing → conns 一种。
            if let Some(h) = self.live_conn(&key) {
                return Ok(h);
            }
            match m.get(&key) {
                Some(d) => (d.clone(), false),
                None => {
                    let d = Arc::new(Dial::default());
                    m.insert(key.clone(), d.clone());
                    (d, true)
                }
            }
        }; // dialing 锁在这里放开：等待别人的结果时不拿着它
        if !mine {
            return dial.wait(deadline); // 别人在建：等它的结果
        }
        let guard = DialGuard { rpc: self, key: &key, dial };
        let r = self.dial(scheme, addr, &key, deadline);
        guard.dial.publish(r.clone());
        r // guard 在这里被丢弃：摘掉 dialing 里的项
    }

    /// 连接表里到 `key` 的、还没断的连接。
    fn live_conn(&self, key: &(String, String)) -> Option<ConnHandle> {
        self.conns().get(key).filter(|h| !h.is_closed()).cloned()
    }

    /// 真正建连：传输插件 `connect` → 起读/写线程 → 等读线程完成握手 → 登记进连接表。
    fn dial(&self, scheme: &str, addr: &str, key: &(String, String), deadline: u64) -> Result<ConnHandle, ErrCode> {
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
        // 单飞 + 拿着 dialing 锁再查一次（见 connection）保证同一对端只有一个线程建连；下面是第二道保险：
        // 表里已经有一条还没断的连接，就关掉自己这条、用先登记的那条（不让任何一条连接从表里消失却不被关闭）
        let mut conns = self.conns();
        if let Some(o) = conns.get(key).filter(|o| !o.is_closed()) {
            h.close();
            return Ok(o.clone());
        }
        conns.insert(key.clone(), h.clone());
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
