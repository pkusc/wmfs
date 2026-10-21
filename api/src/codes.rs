//! 错误码的名字 ↔ 数值表（design.md 3.3.7）。
//!
//! 1–255 直接用 Linux errno（`ECONNRESET`、`ETIMEDOUT` 由 RpcClient 在本地产生：连接断开、调用超时）；
//! 集群错误从 1000 起（客户端怎么处理见 design.md 3.3.7 的表）。core 的 `ErrCode::XXX` 常量与 fail point 的
//! `return(<码名>)` 都以这张表为准（core 有单元测试逐个核对），所以两边认识同一套名字。
//! 新增或改动错误码 = 线上协议变化，需要 ADR。

/// 全部错误码：(数值, 名字)，按数值升序。
pub const ALL: &[(u16, &str)] = &[
    (1, "EPERM"),
    (2, "ENOENT"),
    (4, "EINTR"),
    (5, "EIO"),
    (7, "E2BIG"),
    (9, "EBADF"),
    (11, "EAGAIN"),
    (12, "ENOMEM"),
    (13, "EACCES"),
    (16, "EBUSY"),
    (17, "EEXIST"),
    (18, "EXDEV"),
    (19, "ENODEV"),
    (20, "ENOTDIR"),
    (21, "EISDIR"),
    (22, "EINVAL"),
    (25, "ENOTTY"),
    (27, "EFBIG"),
    (28, "ENOSPC"),
    (30, "EROFS"),
    (31, "EMLINK"),
    (34, "ERANGE"),
    (36, "ENAMETOOLONG"),
    (37, "ENOLCK"),
    (38, "ENOSYS"),
    (39, "ENOTEMPTY"),
    (40, "ELOOP"),
    (61, "ENODATA"),
    (75, "EOVERFLOW"),
    (95, "EOPNOTSUPP"),
    (104, "ECONNRESET"),
    (110, "ETIMEDOUT"),
    (116, "ESTALE"),
    (122, "EDQUOT"),
    (1000, "NOT_LEADER"),        // 不是元数据 leader（ErrorBody.leader_hint）
    (1001, "RETRY"),             // 结果未知或批次作废：用同一个 seq 退避重试
    (1002, "NOT_READY"),         // leader 尚未就绪 / 节点启动中
    (1003, "STALE_CHAIN"),       // 链版本不一致（extra = 当前 chain_ver）
    (1004, "CHUNK_DIRTY"),       // 本成员有未提交版本且不是 last serving（extra = last serving target）
    (1005, "NOT_SERVING"),       // 本成员不是 Serving / 链不可用
    (1006, "FENCED"),            // 数据节点 target 租约已失效、自我隔离中
    (1007, "BUSY"),              // 内存池/队列满、chunk 等待队列满
    (1008, "NEED_FULL"),         // 后继缺少增量转发的基础版本（只在服务端之间）
    (1009, "CHUNK_NOT_FOUND"),   // 该 chunk 从未写过
    (1010, "CHECKSUM_MISMATCH"), // 本副本该块校验失败
    (1011, "CORRUPT"),           // chunk 已被标为损坏
    (1012, "SESSION_EVICTED"),   // 会话已被驱逐
    (1013, "BAD_CAP"),           // cap 校验失败或过期
    (1014, "STALE_LAYOUT"),      // CommitWrite 等的 epoch 与当前 layout 不符
    // 1015 保留：首发不使用（F_SETLKW 改为客户端轮询后不再需要）
    (1016, "AUTH_FAILED"),         // 认证失败
    (1017, "PROTO_UNSUPPORTED"),   // 协议版本不兼容
    (1018, "FEATURE_UNSUPPORTED"), // 不认识的消息/变体/特性/ABI
    (1019, "SHUTTING_DOWN"),       // 对端正在退出
    (1020, "INTERNAL"),            // 内部不变量破坏
];

/// 数值 → 名字；不认识的码返回 None（未知码照样可以透传）。
pub fn name(code: u16) -> Option<&'static str> {
    ALL.iter().find(|(c, _)| *c == code).map(|(_, n)| *n)
}

/// 名字 → 数值（区分大小写，例如 "EIO"、"BUSY"）。
pub fn from_name(name: &str) -> Option<u16> {
    ALL.iter().find(|(_, n)| *n == name).map(|(c, _)| *c)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_sorted_and_unique() {
        for w in ALL.windows(2) {
            assert!(w[0].0 < w[1].0, "错误码表必须按数值严格升序：{:?} {:?}", w[0], w[1]);
        }
        for (i, (_, a)) in ALL.iter().enumerate() {
            for (_, b) in &ALL[i + 1..] {
                assert_ne!(a, b, "错误码名字重复");
            }
        }
    }

    #[test]
    fn name_round_trip() {
        for (c, n) in ALL {
            assert_eq!(name(*c), Some(*n));
            assert_eq!(from_name(n), Some(*c));
        }
        assert_eq!(name(1015), None, "1015 保留不用");
        assert_eq!(name(9999), None);
        assert_eq!(from_name("eio"), None, "名字区分大小写");
    }

    #[test]
    fn spot_values_match_design() {
        // design.md 3.3.7 里写死的几个数值
        assert_eq!(from_name("EDQUOT"), Some(122));
        assert_eq!(from_name("ENODEV"), Some(19));
        assert_eq!(from_name("NOT_LEADER"), Some(1000));
        assert_eq!(from_name("AUTH_FAILED"), Some(1016));
        assert_eq!(from_name("INTERNAL"), Some(1020));
    }
}
