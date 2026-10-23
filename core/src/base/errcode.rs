//! 错误码 `ErrCode`（design.md 3.3.7）：`pub struct ErrCode(pub u16)`，不用 enum，未知码也能透传。
//!
//! 1–255 直接用 Linux errno；集群错误从 1000 起。名字表在 `wmfs_api::codes`（fail point 与 wmfs-mgr 也用它），
//! 这里是带类型的常量；单元测试逐个核对两边一致。网络与磁盘结果一律转成 `ErrCode`，不得 `unwrap()`（design.md 0.2 第 5 条）。

use std::fmt;

use wmfs_api::codes;

#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ErrCode(pub u16);

impl ErrCode {
    pub const EPERM: ErrCode = ErrCode(1);
    pub const ENOENT: ErrCode = ErrCode(2);
    pub const EINTR: ErrCode = ErrCode(4);
    pub const EIO: ErrCode = ErrCode(5);
    pub const E2BIG: ErrCode = ErrCode(7);
    pub const EBADF: ErrCode = ErrCode(9);
    pub const EAGAIN: ErrCode = ErrCode(11);
    pub const ENOMEM: ErrCode = ErrCode(12);
    pub const EACCES: ErrCode = ErrCode(13);
    pub const EBUSY: ErrCode = ErrCode(16);
    pub const EEXIST: ErrCode = ErrCode(17);
    pub const EXDEV: ErrCode = ErrCode(18);
    pub const ENODEV: ErrCode = ErrCode(19);
    pub const ENOTDIR: ErrCode = ErrCode(20);
    pub const EISDIR: ErrCode = ErrCode(21);
    pub const EINVAL: ErrCode = ErrCode(22);
    pub const ENOTTY: ErrCode = ErrCode(25);
    pub const EFBIG: ErrCode = ErrCode(27);
    pub const ENOSPC: ErrCode = ErrCode(28);
    pub const EROFS: ErrCode = ErrCode(30);
    pub const EMLINK: ErrCode = ErrCode(31);
    pub const ERANGE: ErrCode = ErrCode(34);
    pub const ENAMETOOLONG: ErrCode = ErrCode(36);
    pub const ENOLCK: ErrCode = ErrCode(37);
    pub const ENOSYS: ErrCode = ErrCode(38);
    pub const ENOTEMPTY: ErrCode = ErrCode(39);
    pub const ELOOP: ErrCode = ErrCode(40);
    pub const ENODATA: ErrCode = ErrCode(61);
    pub const EOVERFLOW: ErrCode = ErrCode(75);
    pub const EOPNOTSUPP: ErrCode = ErrCode(95);
    pub const ECONNRESET: ErrCode = ErrCode(104);
    pub const ETIMEDOUT: ErrCode = ErrCode(110);
    pub const ESTALE: ErrCode = ErrCode(116);
    pub const EDQUOT: ErrCode = ErrCode(122);
    pub const NOT_LEADER: ErrCode = ErrCode(1000);
    pub const RETRY: ErrCode = ErrCode(1001);
    pub const NOT_READY: ErrCode = ErrCode(1002);
    pub const STALE_CHAIN: ErrCode = ErrCode(1003);
    pub const CHUNK_DIRTY: ErrCode = ErrCode(1004);
    pub const NOT_SERVING: ErrCode = ErrCode(1005);
    pub const FENCED: ErrCode = ErrCode(1006);
    pub const BUSY: ErrCode = ErrCode(1007);
    pub const NEED_FULL: ErrCode = ErrCode(1008);
    pub const CHUNK_NOT_FOUND: ErrCode = ErrCode(1009);
    pub const CHECKSUM_MISMATCH: ErrCode = ErrCode(1010);
    pub const CORRUPT: ErrCode = ErrCode(1011);
    pub const SESSION_EVICTED: ErrCode = ErrCode(1012);
    pub const BAD_CAP: ErrCode = ErrCode(1013);
    pub const STALE_LAYOUT: ErrCode = ErrCode(1014);
    pub const AUTH_FAILED: ErrCode = ErrCode(1016);
    pub const PROTO_UNSUPPORTED: ErrCode = ErrCode(1017);
    pub const FEATURE_UNSUPPORTED: ErrCode = ErrCode(1018);
    pub const SHUTTING_DOWN: ErrCode = ErrCode(1019);
    pub const INTERNAL: ErrCode = ErrCode(1020);

    /// 名字（例如 "EIO"）；不认识的码返回 None。
    pub fn name(self) -> Option<&'static str> {
        codes::name(self.0)
    }

    /// 由名字取码（例如 fail point 的 `return(EIO)`）。
    pub fn from_name(name: &str) -> Option<ErrCode> {
        codes::from_name(name).map(ErrCode)
    }

    /// 1..=255 是 POSIX errno（这类结果会写进去重表，design.md 3.4.1.4）。
    pub fn is_posix(self) -> bool {
        (1..=255).contains(&self.0)
    }
}

impl fmt::Display for ErrCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.name() {
            Some(n) => write!(f, "{n}"),
            None if self.is_posix() => {
                // 名字表里没有的 errno：附上系统的说明，例如"错误码 98（Address already in use）"
                let s = std::io::Error::from_raw_os_error(self.0 as i32).to_string();
                write!(f, "错误码 {}（{}）", self.0, s.split(" (os error").next().unwrap_or(&s))
            }
            None => write!(f, "错误码 {}", self.0),
        }
    }
}

impl fmt::Debug for ErrCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ErrCode({}={})", self.name().unwrap_or("?"), self.0)
    }
}

impl std::error::Error for ErrCode {}

/// 磁盘与网络错误：有 errno 就原样用（1..=255），否则按 EIO。
impl From<std::io::Error> for ErrCode {
    fn from(e: std::io::Error) -> ErrCode {
        match e.raw_os_error() {
            Some(n) if (1..=255).contains(&n) => ErrCode(n as u16),
            _ => match e.kind() {
                std::io::ErrorKind::TimedOut => ErrCode::ETIMEDOUT,
                std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe => ErrCode::ECONNRESET,
                _ => ErrCode::EIO,
            },
        }
    }
}

/// 失败：错误码 + 给人看的中文原因（空 = 用错误码名）。管理 API 的端点回 `{"error": 码名, "message": 原因}`（7.1）；实现注记（M7）：
/// 属性字符串的切分（`meta::attr_registry::split`）要分出 `EINVAL`/`EOPNOTSUPP`/`FEATURE_UNSUPPORTED`，也返回它（0.2 第 5 条的 M7 注记）。
#[derive(Debug)]
pub struct Failed {
    pub code: ErrCode,
    pub msg: String,
}

impl Failed {
    pub fn new(code: ErrCode, msg: impl Into<String>) -> Failed {
        Failed { code, msg: msg.into() }
    }
}

impl From<ErrCode> for Failed {
    fn from(code: ErrCode) -> Failed {
        Failed { code, msg: String::new() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_and_display() {
        assert_eq!(ErrCode::EIO.to_string(), "EIO");
        assert_eq!(ErrCode::NOT_LEADER.name(), Some("NOT_LEADER"));
        assert_eq!(ErrCode(4321).to_string(), "错误码 4321");
        assert_eq!(ErrCode(98).to_string(), "错误码 98（Address already in use）", "名字表里没有的 errno 附上系统说明");
        assert_eq!(format!("{:?}", ErrCode::EDQUOT), "ErrCode(EDQUOT=122)");
        assert_eq!(ErrCode::from_name("BUSY"), Some(ErrCode::BUSY));
        assert!(ErrCode::EAGAIN.is_posix());
        assert!(!ErrCode::RETRY.is_posix());
    }

    #[test]
    fn every_const_matches_the_api_table() {
        // 防止 wmfs_api::codes 与这里的常量漂移：每个常量的名字与数值都必须和名字表一致，且一个不少
        let all = [
            (ErrCode::EPERM, "EPERM"),
            (ErrCode::ENOENT, "ENOENT"),
            (ErrCode::EINTR, "EINTR"),
            (ErrCode::EIO, "EIO"),
            (ErrCode::E2BIG, "E2BIG"),
            (ErrCode::EBADF, "EBADF"),
            (ErrCode::EAGAIN, "EAGAIN"),
            (ErrCode::ENOMEM, "ENOMEM"),
            (ErrCode::EACCES, "EACCES"),
            (ErrCode::EBUSY, "EBUSY"),
            (ErrCode::EEXIST, "EEXIST"),
            (ErrCode::EXDEV, "EXDEV"),
            (ErrCode::ENODEV, "ENODEV"),
            (ErrCode::ENOTDIR, "ENOTDIR"),
            (ErrCode::EISDIR, "EISDIR"),
            (ErrCode::EINVAL, "EINVAL"),
            (ErrCode::ENOTTY, "ENOTTY"),
            (ErrCode::EFBIG, "EFBIG"),
            (ErrCode::ENOSPC, "ENOSPC"),
            (ErrCode::EROFS, "EROFS"),
            (ErrCode::EMLINK, "EMLINK"),
            (ErrCode::ERANGE, "ERANGE"),
            (ErrCode::ENAMETOOLONG, "ENAMETOOLONG"),
            (ErrCode::ENOLCK, "ENOLCK"),
            (ErrCode::ENOSYS, "ENOSYS"),
            (ErrCode::ENOTEMPTY, "ENOTEMPTY"),
            (ErrCode::ELOOP, "ELOOP"),
            (ErrCode::ENODATA, "ENODATA"),
            (ErrCode::EOVERFLOW, "EOVERFLOW"),
            (ErrCode::EOPNOTSUPP, "EOPNOTSUPP"),
            (ErrCode::ECONNRESET, "ECONNRESET"),
            (ErrCode::ETIMEDOUT, "ETIMEDOUT"),
            (ErrCode::ESTALE, "ESTALE"),
            (ErrCode::EDQUOT, "EDQUOT"),
            (ErrCode::NOT_LEADER, "NOT_LEADER"),
            (ErrCode::RETRY, "RETRY"),
            (ErrCode::NOT_READY, "NOT_READY"),
            (ErrCode::STALE_CHAIN, "STALE_CHAIN"),
            (ErrCode::CHUNK_DIRTY, "CHUNK_DIRTY"),
            (ErrCode::NOT_SERVING, "NOT_SERVING"),
            (ErrCode::FENCED, "FENCED"),
            (ErrCode::BUSY, "BUSY"),
            (ErrCode::NEED_FULL, "NEED_FULL"),
            (ErrCode::CHUNK_NOT_FOUND, "CHUNK_NOT_FOUND"),
            (ErrCode::CHECKSUM_MISMATCH, "CHECKSUM_MISMATCH"),
            (ErrCode::CORRUPT, "CORRUPT"),
            (ErrCode::SESSION_EVICTED, "SESSION_EVICTED"),
            (ErrCode::BAD_CAP, "BAD_CAP"),
            (ErrCode::STALE_LAYOUT, "STALE_LAYOUT"),
            (ErrCode::AUTH_FAILED, "AUTH_FAILED"),
            (ErrCode::PROTO_UNSUPPORTED, "PROTO_UNSUPPORTED"),
            (ErrCode::FEATURE_UNSUPPORTED, "FEATURE_UNSUPPORTED"),
            (ErrCode::SHUTTING_DOWN, "SHUTTING_DOWN"),
            (ErrCode::INTERNAL, "INTERNAL"),
        ];
        for (c, n) in all {
            assert_eq!(c.name(), Some(n), "ErrCode::{n} 的数值与 wmfs_api::codes 不一致");
        }
        assert_eq!(all.len(), wmfs_api::codes::ALL.len(), "名字表里有常量没覆盖到的错误码");
    }

    #[test]
    fn posix_codes_equal_linux_errno() {
        // 1..=255 直接用 Linux errno（3.3.7）：与 libc 逐个核对。两边的名字表就算一起被改错，这里也会发现。
        let posix = [
            (ErrCode::EPERM, libc::EPERM),
            (ErrCode::ENOENT, libc::ENOENT),
            (ErrCode::EINTR, libc::EINTR),
            (ErrCode::EIO, libc::EIO),
            (ErrCode::E2BIG, libc::E2BIG),
            (ErrCode::EBADF, libc::EBADF),
            (ErrCode::EAGAIN, libc::EAGAIN),
            (ErrCode::ENOMEM, libc::ENOMEM),
            (ErrCode::EACCES, libc::EACCES),
            (ErrCode::EBUSY, libc::EBUSY),
            (ErrCode::EEXIST, libc::EEXIST),
            (ErrCode::EXDEV, libc::EXDEV),
            (ErrCode::ENODEV, libc::ENODEV),
            (ErrCode::ENOTDIR, libc::ENOTDIR),
            (ErrCode::EISDIR, libc::EISDIR),
            (ErrCode::EINVAL, libc::EINVAL),
            (ErrCode::ENOTTY, libc::ENOTTY),
            (ErrCode::EFBIG, libc::EFBIG),
            (ErrCode::ENOSPC, libc::ENOSPC),
            (ErrCode::EROFS, libc::EROFS),
            (ErrCode::EMLINK, libc::EMLINK),
            (ErrCode::ERANGE, libc::ERANGE),
            (ErrCode::ENAMETOOLONG, libc::ENAMETOOLONG),
            (ErrCode::ENOLCK, libc::ENOLCK),
            (ErrCode::ENOSYS, libc::ENOSYS),
            (ErrCode::ENOTEMPTY, libc::ENOTEMPTY),
            (ErrCode::ELOOP, libc::ELOOP),
            (ErrCode::ENODATA, libc::ENODATA),
            (ErrCode::EOVERFLOW, libc::EOVERFLOW),
            (ErrCode::EOPNOTSUPP, libc::EOPNOTSUPP),
            (ErrCode::ECONNRESET, libc::ECONNRESET),
            (ErrCode::ETIMEDOUT, libc::ETIMEDOUT),
            (ErrCode::ESTALE, libc::ESTALE),
            (ErrCode::EDQUOT, libc::EDQUOT),
        ];
        for (c, e) in posix {
            assert_eq!(c.0 as i32, e, "{c} 应等于 Linux errno {e}");
        }
        let n = wmfs_api::codes::ALL.iter().filter(|(c, _)| ErrCode(*c).is_posix()).count();
        assert_eq!(posix.len(), n, "名字表里有没核对过的 POSIX 错误码");
    }

    #[test]
    fn from_io_error() {
        let e = std::io::Error::from_raw_os_error(libc::ENOSPC);
        assert_eq!(ErrCode::from(e), ErrCode::ENOSPC);
        let e = std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "x");
        assert_eq!(ErrCode::from(e), ErrCode::EIO);
        let e = std::io::Error::new(std::io::ErrorKind::TimedOut, "x");
        assert_eq!(ErrCode::from(e), ErrCode::ETIMEDOUT);
    }
}
