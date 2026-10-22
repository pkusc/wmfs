//! 执行器的批次覆盖层 [`BatchView`] 与只读视图 [`ReadView`]（design.md 3.4.1.4、3.5.1）。
//!
//! `BatchView` = 批次开始时的 redb 读事务 + 两层 `BTreeMap<key, Some(value) | None(删除)>`：批次层只活一个批次
//! （同一时刻 raft 里最多一个批次在途，所以构建下一批时上一批已经应用，不需要跨批状态）；操作层装当前闭包的写入，
//! 闭包失败就整体丢弃（原子性）。闭包只拿得到 `get/put/delete`，没有 scan（3.4.1.4 规则 1：写操作只做点查）。
//! 写集字节数 = 批次层里全部 KvOp 的 key + value 长度之和（同 key 只算最后一次），执行器据此在 ≥ 768 KiB 时结束本批。
//! 执行器线程上的 redb 读错误一律 fail-stop（0.2 第 5 条）。
//!
//! `ReadView` 是只读路径（meta-read 线程、后台任务）用的视图：点查 + 有上限的范围扫描；redb 读错误回 `EIO`。

use std::collections::BTreeMap;

use redb::{ReadOnlyTable, ReadTransaction};

use crate::base::ErrCode;
use crate::meta::engine::txn::{KvRead, KvScan, KvTxn};
use crate::meta::raft::storage::KV;
use crate::meta::raft::{or_abort, redb_eio};
use crate::meta::schema::keys::T_CHAIN;
use crate::meta::schema::records::KvOp;

type Table = ReadOnlyTable<&'static [u8], &'static [u8]>;
type Overlay = BTreeMap<Vec<u8>, Option<Vec<u8>>>;

pub struct BatchView {
    base: Table,
    batch: Overlay,
    op: Overlay,
    bytes: usize,
    topology: bool,
}

/// 一个 KvOp 的编码字节数（key + value）。
fn op_len(k: &[u8], v: &Option<Vec<u8>>) -> usize {
    k.len() + v.as_ref().map_or(0, Vec::len)
}

impl BatchView {
    pub fn new(rtx: &ReadTransaction) -> BatchView {
        let base = or_abort(rtx.open_table(KV), "打开 kv 表");
        BatchView { base, batch: Overlay::new(), op: Overlay::new(), bytes: 0, topology: false }
    }

    /// 开始一个操作（清空操作层）。
    pub fn begin_op(&mut self) {
        self.op.clear();
    }

    /// 操作成功：操作层并入批次层，更新写集字节数；写了 CHAIN 键（生成了新链）= 拓扑变了（规则 5）。
    pub fn commit_op(&mut self) {
        for (k, v) in std::mem::take(&mut self.op) {
            self.topology |= k.first() == Some(&T_CHAIN);
            self.bytes += op_len(&k, &v);
            if let Some(old) = self.batch.get(&k) {
                self.bytes -= op_len(&k, old);
            }
            self.batch.insert(k, v);
        }
    }

    /// 操作失败：丢弃它的全部写入。
    pub fn rollback_op(&mut self) {
        self.op.clear();
    }

    pub fn is_empty(&self) -> bool {
        self.batch.is_empty() && self.op.is_empty()
    }

    /// 写集的实际编码字节数（批次层）。
    pub fn encoded_len(&self) -> usize {
        self.bytes
    }

    /// 本批写过 CHAIN 键（create / 截断到 0 时 gen_chains 生成了新链）：执行完它就结束本批（3.4.1.4 规则 5）。
    pub fn topology_changed(&self) -> bool {
        self.topology
    }

    /// 已按 key 排序、同 key 只留最后一次（3.2.9）。
    pub fn into_ops(self) -> Vec<KvOp> {
        let to_op = |(key, v): (Vec<u8>, Option<Vec<u8>>)| match v {
            Some(value) => KvOp::Put { key, value },
            None => KvOp::Delete { key },
        };
        self.batch.into_iter().map(to_op).collect()
    }
}

impl KvRead for BatchView {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ErrCode> {
        if let Some(v) = self.op.get(key).or_else(|| self.batch.get(key)) {
            return Ok(v.clone());
        }
        Ok(or_abort(self.base.get(key), "读 kv").map(|g| g.value().to_vec()))
    }
}

impl KvTxn for BatchView {
    fn put(&mut self, key: &[u8], value: Vec<u8>) {
        self.op.insert(key.to_vec(), Some(value));
    }
    fn delete(&mut self, key: &[u8]) {
        self.op.insert(key.to_vec(), None);
    }
}

/// 只读视图：线性一致读在 ReadIndex 之后用它（MetaEngine::read）。
pub struct ReadView {
    t: Table,
}

impl ReadView {
    pub fn new(rtx: &ReadTransaction) -> Result<ReadView, ErrCode> {
        rtx.open_table(KV).map(|t| ReadView { t }).map_err(|e| redb_eio("打开 kv 表", e))
    }
}

impl KvRead for ReadView {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ErrCode> {
        let g = self.t.get(key).map_err(|e| redb_eio("读 kv", e))?;
        Ok(g.map(|g| g.value().to_vec()))
    }
}

impl KvScan for ReadView {
    fn scan(&self, start: &[u8], end: &[u8], limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ErrCode> {
        let mut out = Vec::new();
        if start >= end {
            return Ok(out);
        }
        for r in self.t.range::<&[u8]>(start..end).map_err(|e| redb_eio("扫描 kv", e))?.take(limit) {
            let (k, v) = r.map_err(|e| redb_eio("扫描 kv", e))?;
            out.push((k.value().to_vec(), v.value().to_vec()));
        }
        Ok(out)
    }
}
