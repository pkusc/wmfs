//! core 对传输插件 `Conn` 的包装（design.md 3.7.3、1.2 规则 2）：读线程 + 写线程 + 有界发送队列；其他代码只用它。
//!
//! 一个连接的 socket 只由它的写线程写、读线程读；别的线程发帧只是 `ConnHandle::send` = `try_send` 进该连接的
//! 有界发送队列（1024 帧），队列满 → 关闭连接并返回 `ECONNRESET`（3.3.8），所以执行器、io、repl 线程永远不会
//! 被一个不读 socket 的客户端卡住。
//!
//! 接口骨架（M0）：签名以这里为准，实现在 M1。

use crate::base::ErrCode;
use crate::hooks::traits::ConnIdentity;
use crate::proto::frame::OutFrame;

pub struct ConnHandle {/* id、Arc<dyn Conn>、Sender<OutFrame>（bounded 1024）、认证后的 ConnIdentity */}

#[allow(unused_variables)] // 骨架：函数体在 M1 实现，实现后删除这一行
impl ConnHandle {
    /// send = try_send：队列满 → 关闭连接并返回 ECONNRESET（3.3.8）
    pub fn send(&self, f: OutFrame) -> Result<(), ErrCode> {
        unimplemented!("M1：ConnHandle::send")
    }
    pub fn close(&self) {
        unimplemented!("M1：ConnHandle::close")
    }
    pub fn identity(&self) -> &ConnIdentity {
        unimplemented!("M1：ConnHandle::identity")
    }
}
