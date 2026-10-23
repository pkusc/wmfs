//! FUSE 适配层的纯函数（design.md 8.1、8.3、8.7）：错误码映射、属性的种类与时间换算、根目录的虚拟 xattr 的判定、环境变量里的
//! 作业 id。文件系统对象 `WmfsFs` 在 `mod.rs`。
//!
//! - **错误码**（8.7）：POSIX 码原样；可重试的集群码与连接错误在 `--op-timeout` 内已经重试过，到这里一律 `EIO`；
//!   `SESSION_EVICTED` → `EIO`；`BAD_CAP` 二次失败、`AUTH_FAILED` → `EACCES`；layout 无法刷新（`STALE_LAYOUT`）→ `ESTALE`；
//!   `FEATURE_UNSUPPORTED`/`PROTO_UNSUPPORTED` → `EOPNOTSUPP`。
//! - **虚拟 xattr `user.wmfs.mgmt`**（只在根目录）：本地应答，值是逗号分隔的当前管理地址（集群图里各节点的 mgmt 地址），给 CLI 的
//!   路径换算用（7.2）；setxattr/removexattr 它 → `EPERM`；不出现在 listxattr 里。

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuser::{Errno, FileType};
use wmfs_core::base::ErrCode;
use wmfs_core::base::types::Ts;
use wmfs_core::meta::schema::keys::ROOT_INO;
use wmfs_core::meta::schema::records::{KIND_BLK, KIND_CHR, KIND_DIR, KIND_FIFO, KIND_SOCK, KIND_SYMLINK};

/// 根目录上的虚拟 xattr（7.2）。
pub const MGMT_XATTR: &[u8] = b"user.wmfs.mgmt";

/// 这个 (inode, 名字) 是不是根目录上的虚拟 xattr。
pub fn is_virtual(ino: u64, name: &[u8]) -> bool {
    ino == ROOT_INO && name == MGMT_XATTR
}

/// environ（NUL 分隔的 `K=V`）里变量 `var` 的值开头的数字（`PBS_JOBID=1234.server` → 1234）。
pub fn job_in(env: &[u8], var: &str) -> Option<u64> {
    env.split(|b| *b == 0).find_map(|kv| {
        let v = kv.strip_prefix(var.as_bytes())?.strip_prefix(b"=")?;
        let digits: Vec<u8> = v.iter().copied().take_while(u8::is_ascii_digit).collect();
        std::str::from_utf8(&digits).ok()?.parse().ok()
    })
}

/// 错误码映射（见文件头）。
pub fn errno(e: ErrCode) -> Errno {
    match e {
        ErrCode::ECONNRESET | ErrCode::ETIMEDOUT => Errno::EIO, // 连不上集群：已经在 --op-timeout 内重试过
        ErrCode(c @ 1..=255) => Errno::from_i32(c as i32),
        ErrCode::FEATURE_UNSUPPORTED | ErrCode::PROTO_UNSUPPORTED => Errno::EOPNOTSUPP,
        ErrCode::BAD_CAP | ErrCode::AUTH_FAILED => Errno::EACCES,
        ErrCode::STALE_LAYOUT => Errno::ESTALE,
        _ => Errno::EIO,
    }
}

pub fn file_type(kind: u8) -> FileType {
    match kind {
        KIND_DIR => FileType::Directory,
        KIND_SYMLINK => FileType::Symlink,
        KIND_FIFO => FileType::NamedPipe,
        KIND_SOCK => FileType::Socket,
        KIND_CHR => FileType::CharDevice,
        KIND_BLK => FileType::BlockDevice,
        _ => FileType::RegularFile,
    }
}

pub fn systime(t: Ts) -> SystemTime {
    if t.secs >= 0 {
        UNIX_EPOCH + Duration::new(t.secs as u64, t.nanos)
    } else {
        UNIX_EPOCH - Duration::from_secs(t.secs.unsigned_abs()) + Duration::from_nanos(t.nanos as u64)
    }
}

/// 内核给的时间 → Ts。
pub fn ts_of(t: SystemTime) -> Ts {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => Ts { secs: d.as_secs() as i64, nanos: d.subsec_nanos() },
        Err(e) => {
            let d = e.duration();
            let (secs, nanos) = (-(d.as_secs() as i64), d.subsec_nanos());
            if nanos == 0 { Ts { secs, nanos } } else { Ts { secs: secs - 1, nanos: 1_000_000_000 - nanos } }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_ids_and_error_codes_and_times() {
        let env = b"A=1\0PBS_JOBID=1234.server\0SLURM_JOB_ID=77\0";
        assert_eq!(job_in(env, "PBS_JOBID"), Some(1234), "取开头的数字");
        assert_eq!(job_in(env, "SLURM_JOB_ID"), Some(77));
        assert_eq!(job_in(env, "WMFS_JOB_ID"), None);
        assert_eq!(errno(ErrCode::ENOENT), Errno::ENOENT, "POSIX 码原样");
        assert_eq!(errno(ErrCode::SESSION_EVICTED), Errno::EIO);
        assert_eq!(errno(ErrCode::ETIMEDOUT), Errno::EIO, "重试超时：EIO（像 NFS hard 挂载）");
        assert_eq!(errno(ErrCode::BAD_CAP), Errno::EACCES);
        assert_eq!(errno(ErrCode::STALE_LAYOUT), Errno::ESTALE);
        assert_eq!(errno(ErrCode::FEATURE_UNSUPPORTED), Errno::EOPNOTSUPP);
        for t in [Ts { secs: 5, nanos: 7 }, Ts { secs: -5, nanos: 7 }, Ts { secs: -1, nanos: 0 }] {
            assert_eq!(ts_of(systime(t)), t, "时间往返：{t:?}");
        }
        assert!(is_virtual(ROOT_INO, MGMT_XATTR) && !is_virtual(2, MGMT_XATTR));
    }
}
