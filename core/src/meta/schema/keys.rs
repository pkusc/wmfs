//! 元数据 KV 的 key 编码（design.md 3.2.7，ADR-0020）。
//!
//! key 第 1 字节是键空间标签；key 里的整数一律**大端**（字典序 = 数值序，范围扫描就是前缀扫描）——
//! 这是全仓库"多字节整数小端"规则的唯一例外（3.2.1）。value = 1 字节记录版本号 + postcard（INLINE 例外，是原始字节）。
//! `0x00`、`0x08`、`0x09`、`0x1D`–`0x1E` 与 `0xFF` 不使用；`0x20`–`0x2E` 保留给二期（快照 `0x20`、WASM 模块 `0x21`……）。
//! **插件不能拥有任何键空间**（理念文档 4.3.2）。

pub const T_INODE: u8 = 0x01; // 01 ino:u64 → InodeV1
pub const T_DENTRY: u8 = 0x02; // 02 parent:u64 name → DentryV1；readdir = 前缀扫描 02 parent，按名字字节序
pub const T_XATTR: u8 = 0x03; // 03 ino:u64 → XattrsV1（含 system.posix_acl_*；合计 ≤ 64 KiB）
pub const T_INLINE: u8 = 0x04; // 04 ino:u64 → 原始文件内容；不变式：len(INLINE) ≤ inode.size，[len, size) 读为 0
pub const T_DIRATTR: u8 = 0x05; // 05 dir:u64 → AttrMapV1（目录属性，值已规范化）
pub const T_RSTAT: u8 = 0x06; // 06 dir:u64 → RStatV1（递归计数，3.9）
pub const T_QUOTA_STATE: u8 = 0x07; // 07 dir:u64 class:u16 → QuotaStateV1（class = 0xFFFF 表示总量配额）
pub const T_LOCKS: u8 = 0x0A; // 0A ino:u64 → LockTableV1
pub const T_SESSION: u8 = 0x0B; // 0B session:u64 → SessionRecV1
pub const T_DEDUP: u8 = 0x0C; // 0C session:u64 seq:u64 → DedupV1（编码后的回复，3.5.2）
pub const T_GC: u8 = 0x0D; // 0D kind:u8 ino:u64 epoch:u32 → GcRecV1；kind 1 = 孤儿 inode（epoch 写 0），kind 2 = 废弃 epoch
pub const T_CHANGELOG: u8 = 0x0E; // 0E seq:u64 → ChangeRecV1
pub const T_TRASH_INDEX: u8 = 0x0F; // 0F expire_ms:u64 ino:u64 → 空（回收站到期索引）
pub const T_NODE: u8 = 0x10; // 10 node:u32 → NodeRecV1
pub const T_TARGET: u8 = 0x11; // 11 target:u32 → TargetRecV1
pub const T_CHAIN: u8 = 0x12; // 12 chain:u32 → ChainRecV1
pub const T_CLASS: u8 = 0x13; // 13 class:u16 → StorageClassV1
pub const T_CLASS_NAME: u8 = 0x14; // 14 name → ClassIdV1 { id: u16 }
pub const T_TASK: u8 = 0x15; // 15 kind name → TaskRecV1（kind/name 为 utf8，中间用 0x00 分隔）
pub const T_KEYMETA: u8 = 0x16; // 16 key_id:u32 → KeyMetaV1（角色、租户、uid 范围、root_squash、path_caps、撤销标记；不含秘密）
pub const T_DATAKEY: u8 = 0x17; // 17 datakey:u32 → DataKeyV1（用 K_wrap 包裹的数据加密密钥）
pub const T_QOS: u8 = 0x18; // 18 dir:u64 → QosSpecV1（由 qos 属性派生，集群图里下发给数据节点）
pub const T_RECONCILE_Q: u8 = 0x19; // 19 seq:u64 → ReconcileItemV1 { dir: u64, reason: u8 }
pub const T_CURSOR: u8 = 0x1A; // 1A name → CursorV1 { seq: u64, updated_ms: u64 }（changelog 消费者游标）
pub const T_EVICTED: u8 = 0x1B; // 1B session:u64 → EvictedV1 { evicted_ms: u64, map_epoch: u64 }（保留 24 h）
pub const T_DATAKEY_NAME: u8 = 0x1C; // 1C name → DataKeyIdV1 { id: u32 }
pub const T_GLOBAL: u8 = 0x1F; // 1F name → 见下面的 G_* 名字
pub const T_DEBUG: u8 = 0x2F; // 2F key → 原始字节；只有 wmfs debug kv（服务端开 debug_ops）读写

/// QUOTA_STATE 的 class = 0xFFFF 表示总量配额。
pub const QUOTA_CLASS_TOTAL: u16 = 0xFFFF;
/// GC 记录的 kind。
pub const GC_ORPHAN: u8 = 1;
pub const GC_EPOCH: u8 = 2;

// ---- GLOBAL 的 name（utf8）；计数器的 value 也是"版本字节 + postcard(u64/u32/u16)" ----
pub const G_NEXT_INO: &str = "next_ino"; // u64
pub const G_NEXT_NODE: &str = "next_node"; // u32
pub const G_NEXT_TARGET: &str = "next_target"; // u32
pub const G_NEXT_CHAIN: &str = "next_chain"; // u32
pub const G_NEXT_CLASS: &str = "next_class"; // u16
pub const G_NEXT_KEY: &str = "next_key"; // u32
pub const G_NEXT_DATAKEY: &str = "next_datakey"; // u32
pub const G_NEXT_CHANGELOG: &str = "next_changelog"; // u64
pub const G_NEXT_RECONCILE: &str = "next_reconcile"; // u64
pub const G_ATTR_EPOCH: &str = "attr_epoch"; // u64
pub const G_MAP_EPOCH: &str = "map_epoch"; // u64
pub const G_CLUSTER: &str = "cluster"; // ClusterInfoV1
pub const G_FEATURES: &str = "features"; // FeatureSetV1
pub const G_SETTINGS: &str = "settings"; // ClusterSettingsV1
pub const G_PLUGINS: &str = "plugins"; // PluginListV1，集群插件清单（3.7.2）
pub const G_SETTINGS_PREV: &str = "settings_prev"; // ClusterSettingsV1：调小租约之前的值（ADR-0038 第 5 条，M9 起）

// ---- 保留 inode：1 = 根；2..=15 保留；普通 inode 从 16 开始分配、单调、永不复用 ----
pub const ROOT_INO: u64 = 1;
pub const FIRST_INO: u64 = 16;
// id 与计数器的初值（init 与所有读者共同遵守）：node / target / chain / class / key / datakey 的 id 都从 1 开始，
// 0 保留表示"无"；changelog 与 reconcile 的 seq 从 1 开始；attr_epoch、map_epoch 初值 1。
// key_id 0 不在 KEYMETA 里：它固定是服务端之间用的 Server 角色（3.3.2）。

/// 拼一个 key：标签字节 + 各段字节依次相连。
fn make_key(tag: u8, parts: &[&[u8]]) -> Vec<u8> {
    let mut v = Vec::with_capacity(1 + parts.iter().map(|p| p.len()).sum::<usize>());
    v.push(tag);
    for p in parts {
        v.extend_from_slice(p);
    }
    v
}

pub fn inode(ino: u64) -> Vec<u8> {
    make_key(T_INODE, &[&ino.to_be_bytes()])
}
pub fn dentry(parent: u64, name: &[u8]) -> Vec<u8> {
    make_key(T_DENTRY, &[&parent.to_be_bytes(), name])
}
/// readdir 的前缀：`02 parent`。
pub fn dentry_prefix(parent: u64) -> Vec<u8> {
    make_key(T_DENTRY, &[&parent.to_be_bytes()])
}
pub fn xattr(ino: u64) -> Vec<u8> {
    make_key(T_XATTR, &[&ino.to_be_bytes()])
}
pub fn inline(ino: u64) -> Vec<u8> {
    make_key(T_INLINE, &[&ino.to_be_bytes()])
}
pub fn dirattr(dir: u64) -> Vec<u8> {
    make_key(T_DIRATTR, &[&dir.to_be_bytes()])
}
pub fn rstat(dir: u64) -> Vec<u8> {
    make_key(T_RSTAT, &[&dir.to_be_bytes()])
}
pub fn quota_state(dir: u64, class: u16) -> Vec<u8> {
    make_key(T_QUOTA_STATE, &[&dir.to_be_bytes(), &class.to_be_bytes()])
}
pub fn locks(ino: u64) -> Vec<u8> {
    make_key(T_LOCKS, &[&ino.to_be_bytes()])
}
pub fn session(session: u64) -> Vec<u8> {
    make_key(T_SESSION, &[&session.to_be_bytes()])
}
pub fn dedup(session: u64, seq: u64) -> Vec<u8> {
    make_key(T_DEDUP, &[&session.to_be_bytes(), &seq.to_be_bytes()])
}
pub fn gc(kind: u8, ino: u64, epoch: u32) -> Vec<u8> {
    make_key(T_GC, &[&[kind], &ino.to_be_bytes(), &epoch.to_be_bytes()])
}
/// [`gc`] 的反过来：`0D kind:u8 ino:u64 epoch:u32`（1 + 1 + 8 + 4 = 14 字节）→ (kind, ino, epoch)；不是这个形状 → None。
/// 实现注记（M5）：新增（只增）。
pub fn parse_gc(k: &[u8]) -> Option<(u8, u64, u32)> {
    if k.len() != 14 || k[0] != T_GC {
        return None;
    }
    let ino = u64::from_be_bytes(k[2..10].try_into().ok()?);
    let epoch = u32::from_be_bytes(k[10..14].try_into().ok()?);
    Some((k[1], ino, epoch))
}
pub fn changelog(seq: u64) -> Vec<u8> {
    make_key(T_CHANGELOG, &[&seq.to_be_bytes()])
}
pub fn trash_index(expire_ms: u64, ino: u64) -> Vec<u8> {
    make_key(T_TRASH_INDEX, &[&expire_ms.to_be_bytes(), &ino.to_be_bytes()])
}
pub fn node(id: u32) -> Vec<u8> {
    make_key(T_NODE, &[&id.to_be_bytes()])
}
pub fn target(id: u32) -> Vec<u8> {
    make_key(T_TARGET, &[&id.to_be_bytes()])
}
pub fn chain(id: u32) -> Vec<u8> {
    make_key(T_CHAIN, &[&id.to_be_bytes()])
}
pub fn class(id: u16) -> Vec<u8> {
    make_key(T_CLASS, &[&id.to_be_bytes()])
}
pub fn class_name(name: &str) -> Vec<u8> {
    make_key(T_CLASS_NAME, &[name.as_bytes()])
}
/// `15 kind 00 name`
pub fn task(kind: &str, name: &str) -> Vec<u8> {
    make_key(T_TASK, &[kind.as_bytes(), &[0], name.as_bytes()])
}
pub fn keymeta(key_id: u32) -> Vec<u8> {
    make_key(T_KEYMETA, &[&key_id.to_be_bytes()])
}
pub fn datakey(id: u32) -> Vec<u8> {
    make_key(T_DATAKEY, &[&id.to_be_bytes()])
}
pub fn qos(dir: u64) -> Vec<u8> {
    make_key(T_QOS, &[&dir.to_be_bytes()])
}
pub fn reconcile(seq: u64) -> Vec<u8> {
    make_key(T_RECONCILE_Q, &[&seq.to_be_bytes()])
}
pub fn cursor(name: &str) -> Vec<u8> {
    make_key(T_CURSOR, &[name.as_bytes()])
}
pub fn evicted(session: u64) -> Vec<u8> {
    make_key(T_EVICTED, &[&session.to_be_bytes()])
}
pub fn datakey_name(name: &str) -> Vec<u8> {
    make_key(T_DATAKEY_NAME, &[name.as_bytes()])
}
pub fn global(name: &str) -> Vec<u8> {
    make_key(T_GLOBAL, &[name.as_bytes()])
}
pub fn debug(key: &[u8]) -> Vec<u8> {
    make_key(T_DEBUG, &[key])
}

/// 某个键空间的范围扫描区间 `[tag, tag + 1)`。
pub fn tag_range(tag: u8) -> (Vec<u8>, Vec<u8>) {
    (vec![tag], vec![tag + 1])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn big_endian_keeps_numeric_order() {
        let mut v: Vec<u64> = vec![1, 255, 256, 65_536, 1 << 40, u64::MAX, 16, 17];
        let mut keys: Vec<Vec<u8>> = v.iter().map(|i| inode(*i)).collect();
        v.sort();
        keys.sort();
        assert_eq!(keys, v.iter().map(|i| inode(*i)).collect::<Vec<_>>());
    }

    #[test]
    fn dentry_prefix_scan_and_layouts() {
        assert_eq!(dentry(16, b"abc"), [&[0x02][..], &16u64.to_be_bytes(), b"abc"].concat());
        assert!(dentry(16, b"x").starts_with(&dentry_prefix(16)));
        assert!(!dentry(17, b"x").starts_with(&dentry_prefix(16)));
        assert_eq!(task("gc", "local"), b"\x15gc\x00local".to_vec());
        assert_eq!(quota_state(5, QUOTA_CLASS_TOTAL)[9..], [0xFF, 0xFF]);
        assert_eq!(gc(GC_EPOCH, 1, 2), vec![0x0D, 2, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2]);
        assert_eq!(tag_range(T_INODE), (vec![1], vec![2]));
    }

    #[test]
    fn tags_are_unique_and_not_reserved() {
        let tags = [
            T_INODE,
            T_DENTRY,
            T_XATTR,
            T_INLINE,
            T_DIRATTR,
            T_RSTAT,
            T_QUOTA_STATE,
            T_LOCKS,
            T_SESSION,
            T_DEDUP,
            T_GC,
            T_CHANGELOG,
            T_TRASH_INDEX,
            T_NODE,
            T_TARGET,
            T_CHAIN,
            T_CLASS,
            T_CLASS_NAME,
            T_TASK,
            T_KEYMETA,
            T_DATAKEY,
            T_QOS,
            T_RECONCILE_Q,
            T_CURSOR,
            T_EVICTED,
            T_DATAKEY_NAME,
            T_GLOBAL,
            T_DEBUG,
        ];
        for (i, a) in tags.iter().enumerate() {
            assert!(![0x00, 0x08, 0x09, 0x1D, 0x1E, 0xFF].contains(a), "{a:#x} 是不使用的标签");
            assert!(!(0x20..=0x2E).contains(a), "{a:#x} 是二期保留标签");
            assert!(!tags[i + 1..].contains(a), "{a:#x} 重复");
        }
    }
}
