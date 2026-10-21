//! 服务端对一个请求的回复句柄 `Responder`（design.md 3.12、3.6.3）。
//!
//! 按值传递，恰好用一次；回复只是 `try_send` 进连接的发送队列（队列满就断开这条连接，对端会重试），
//! 所以回复永远不会阻塞执行器或 io 线程。被 Drop 而没有回复时自动回 `EIO` 并计数 `wmfs_req_dropped_total`。
//! 服务端的 accept、连接读/写线程、握手服务端、角色权限表与分发也在这个文件（M1）。
//!
//! 接口骨架（M0）：签名以这里为准，实现在 M1。

use crate::base::ErrCode;
use crate::buf::BufSlice;
use crate::hooks::traits::ConnIdentity;
use crate::net::conn::ConnHandle;

/// 服务端对一个请求的回复句柄：按值传递，恰好用一次
#[allow(dead_code)] // 骨架：字段在 M1 实现后才被读取，届时删除这一行
pub struct Responder {
    conn: ConnHandle,
    req_id: u64,
    msg_type: u16,
    trace_id: u64,
    span_id: u16,
    done: bool,
}

#[allow(unused_variables)] // 骨架：函数体在 M1 实现，实现后删除这一行
impl Responder {
    /// 编成 Response 帧，ConnHandle::send（try_send，不阻塞）
    pub fn reply(self, body: Vec<u8>, bulk: Vec<BufSlice>) {
        unimplemented!("M1：Responder::reply")
    }
    /// ERROR 帧；leader_hint 由 core 自动填
    pub fn error(self, code: ErrCode, extra: u64) {
        unimplemented!("M1：Responder::error")
    }
    pub fn identity(&self) -> &ConnIdentity {
        unimplemented!("M1：Responder::identity")
    }
}

impl Drop for Responder {
    fn drop(&mut self) {
        // 没回复过 → 回 EIO，计数 wmfs_req_dropped_total（M1）
    }
}
