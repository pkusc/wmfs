//! 命名空间操作的测试工具（design.md 9.1"POSIX 元数据语义"）：在一个直接改 `BTreeMap` 的视图上（init 的初始 KV）跑
//! `meta::ns` 的闭包——每个操作一个"批次"：从当前 KV 建集群图快照、执行、失败就整体丢弃它的写入（与执行器的操作层一样）。
//! 不经 raft，所以快、可重复；raft 之上的行为（掉电重启、rename 的原子可见性、去重、会话）由 meta_ns_engine.rs 与
//! fail point 测试覆盖。管线与 `wmfs server` 默认装的一样：posix-mode（authz）+ label-match（placement）。
//! 根目录的属主是 (1000, 1001)（`sample_init`），/home 与 /scratch 是 1777。

use std::collections::BTreeMap;
use std::sync::Arc;

use wmfs_core::base::ErrCode;
use wmfs_core::base::types::{Principal, ReqCtx, Role, Ts};
use wmfs_core::hooks::pipeline::Pipeline;
use wmfs_core::hooks::traits::ConnIdentity;
use wmfs_core::init::initial_kv;
use wmfs_core::meta::attrs::AttrResolver;
use wmfs_core::meta::cluster::map;
use wmfs_core::meta::engine::txn::{KvRead, KvScan, KvTxn, OpCtx};
use wmfs_core::meta::ns;
use wmfs_core::meta::schema::keys;
use wmfs_core::meta::schema::records::{InodeV1, decode_v1};
use wmfs_core::proto::meta::{AttrOut, MetaOp, MetaReply};

use super::meta::{ns_pipeline, quiet_logs, sample_init};

/// 测试里的 KV 视图。
#[derive(Clone, Default)]
pub struct View(pub BTreeMap<Vec<u8>, Vec<u8>>);

impl KvRead for View {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ErrCode> {
        Ok(self.0.get(key).cloned())
    }
}
impl KvTxn for View {
    fn put(&mut self, key: &[u8], value: Vec<u8>) {
        self.0.insert(key.to_vec(), value);
    }
    fn delete(&mut self, key: &[u8]) {
        self.0.remove(key);
    }
}
impl KvScan for View {
    fn scan(&self, start: &[u8], end: &[u8], limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ErrCode> {
        if start >= end {
            return Ok(vec![]);
        }
        Ok(self.0.range(start.to_vec()..end.to_vec()).take(limit).map(|(k, v)| (k.clone(), v.clone())).collect())
    }
}

/// 发请求的人：uid、gid、附加组。
#[derive(Clone, Debug)]
pub struct Who {
    pub uid: u32,
    pub gid: u32,
    pub groups: Vec<u32>,
}

/// init 里根目录的属主。
pub const OWNER: Who = Who { uid: 1000, gid: 1001, groups: vec![] };
pub const ROOT: Who = Who { uid: 0, gid: 0, groups: vec![] };

pub fn user(uid: u32, gid: u32) -> Who {
    Who { uid, gid, groups: vec![] }
}

pub struct Ns {
    pub kv: View,
    pipeline: Pipeline,
    attrs: AttrResolver,
    pub now: i64, // 每个操作之后 + 1 秒（于是能看出 mtime/ctime 是不是被更新了）
}

impl Default for Ns {
    fn default() -> Ns {
        Ns::new()
    }
}

impl Ns {
    pub fn new() -> Ns {
        quiet_logs();
        let kv = View(initial_kv(&sample_init()).into_iter().collect());
        Ns { kv, pipeline: ns_pipeline(), attrs: AttrResolver::default(), now: 1_800_000_000 }
    }

    fn ctx(&mut self, who: &Who) -> ReqCtx {
        self.now += 1;
        let conn = Arc::new(ConnIdentity {
            key_id: 1,
            role: Role::Admin,
            tenant: 0,
            uid_ranges: vec![(0, u32::MAX)],
            root_squash: false,
            path_caps: vec![],
        });
        let w = Principal { uid: who.uid, gid: who.gid, groups: who.groups.clone(), tenant: 0, job_id: 9, conn };
        let now = Ts { secs: self.now, nanos: 0 };
        ReqCtx { who: w, session: 7, seq: 1, pid: 1, trace_id: 3, now, now_ms: self.now as u64 * 1000, internal: false }
    }

    /// 一个修改型操作：在 KV 的副本上执行，成功才换上（失败整体丢弃，与执行器的操作层一样）。
    pub fn op(&mut self, who: &Who, op: MetaOp) -> Result<MetaReply, ErrCode> {
        let req = self.ctx(who);
        let f = ns::txn_op(op).expect("ns::txn_op：这个操作 M4 应该已经实现了");
        let mut v = self.kv.clone();
        let m = map::build(&self.kv, Some(1))?;
        let c = OpCtx { req: &req, pipeline: &self.pipeline, attrs: &self.attrs, map: &m, settings: &m.settings };
        let r = f(&mut v, &c);
        if r.is_ok() {
            self.kv = v;
        }
        r
    }

    /// 一个只读操作。
    pub fn read(&mut self, who: &Who, op: MetaOp) -> Result<MetaReply, ErrCode> {
        let req = self.ctx(who);
        let m = map::build(&self.kv, Some(1))?;
        let c = OpCtx { req: &req, pipeline: &self.pipeline, attrs: &self.attrs, map: &m, settings: &m.settings };
        ns::read(&self.kv, &c, &op)
    }

    /// 按路径解析出 inode 号（以 root 身份）。
    pub fn ino(&mut self, path: &str) -> Result<u64, ErrCode> {
        match self.read(&ROOT, MetaOp::ResolvePath { path: path.as_bytes().to_vec() })? {
            MetaReply::Resolved { ino, .. } => Ok(ino),
            other => panic!("ResolvePath 的回复不对：{other:?}"),
        }
    }

    /// 某个 inode 的记录（直接读 KV）。
    pub fn inode(&self, ino: u64) -> Option<InodeV1> {
        self.kv.0.get(&keys::inode(ino)).map(|b| decode_v1(b).unwrap())
    }

    pub fn attr(&mut self, ino: u64) -> AttrOut {
        match self.read(&ROOT, MetaOp::GetAttr { ino }) {
            Ok(MetaReply::Entry { attr }) => attr,
            other => panic!("GetAttr({ino}) 的回复不对：{other:?}"),
        }
    }

    /// 目录里的名字（按字节序，readdir 一次取完）。
    pub fn ls(&mut self, dir: u64) -> Vec<String> {
        match self.read(&ROOT, MetaOp::ReadDir { ino: dir, after: vec![], limit: 1024, plus: false }) {
            Ok(MetaReply::Dir { entries, eof: true }) => {
                entries.into_iter().map(|e| String::from_utf8(e.name).unwrap()).collect()
            }
            other => panic!("ReadDir({dir}) 的回复不对：{other:?}"),
        }
    }

    /// 在 `parent` 下建目录（mode 0755、umask 0），返回 inode 号。
    pub fn mkdir(&mut self, who: &Who, parent: u64, name: &str) -> Result<u64, ErrCode> {
        let op = MetaOp::Mkdir { parent, name: name.as_bytes().to_vec(), mode: 0o755, umask: 0 };
        match self.op(who, op)? {
            MetaReply::Entry { attr } => Ok(attr.ino),
            other => panic!("Mkdir 的回复不对：{other:?}"),
        }
    }

    /// 在 `parent` 下建普通文件（mode 0644、umask 0、O_EXCL），返回 inode 号。
    pub fn create(&mut self, who: &Who, parent: u64, name: &str) -> Result<u64, ErrCode> {
        let op = MetaOp::Create { parent, name: name.as_bytes().to_vec(), mode: 0o644, umask: 0, excl: true };
        match self.op(who, op)? {
            MetaReply::Opened(o) => Ok(o.attr.ino),
            other => panic!("Create 的回复不对：{other:?}"),
        }
    }

    /// 某个键空间里的全部 (key, value)。
    pub fn tag(&self, tag: u8) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.kv.0.range(vec![tag]..vec![tag + 1]).map(|(k, v)| (k.clone(), v.clone())).collect()
    }
}
