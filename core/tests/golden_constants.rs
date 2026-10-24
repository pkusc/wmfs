//! 金样：线格式与磁盘格式里的数值常量（msg_type、flags、feature 位、记录种类与标志、键空间标签、GLOBAL 名字、
//! chunk 头里的算法编号、cap 权限位、错误码表……）。这些数一旦发布就不能改，但只靠消息/记录样本覆盖不到全部
//! （例如样本只用到了几个 msg_type）——所以每个常量在这里单独一行（小端字节；名字常量是 UTF-8 字节）。

mod common;

use common::Golden;
use wmfs_core::base::types::Role;
use wmfs_core::data::format::*;
use wmfs_core::data::superblock::{TARGET_FORMAT_VERSION, TARGET_INCOMPAT_KNOWN, TARGET_MAGIC, TARGET_SB_LEN};
use wmfs_core::meta::cluster::chains::{SERVING, SYNCING, TARGET_OFFLINE, TARGET_UP};
use wmfs_core::meta::ns::acl;
use wmfs_core::meta::ns::xattr::{ACL_ACCESS, ACL_DEFAULT};
use wmfs_core::meta::schema::keys::*;
use wmfs_core::meta::schema::raft_state::{KEY_APPLIED, KEY_CONF_STATE, KEY_FORMAT, KEY_HARD_STATE, KEY_SNAP_META};
use wmfs_core::meta::schema::records::{
    F_HAS_ACL, F_INLINE, F_ORPHAN, F_TRASH_DIR, KIND_BLK, KIND_CHR, KIND_DIR, KIND_FIFO, KIND_FILE, KIND_SOCK,
    KIND_SYMLINK, LOCK_EOF, REC_V1,
};
use wmfs_core::proto::cap::{CAP_MAC_LEN, RIGHT_ADMIN, RIGHT_READ, RIGHT_WRITE};
use wmfs_core::proto::features::*;
use wmfs_core::proto::frame::*;
use wmfs_core::proto::meta::{
    DEDUP_WINDOW, O_ACCMODE, O_RDONLY, O_RDWR, O_WRONLY, RENAME_EXCHANGE, RENAME_NOREPLACE, RENAME_WHITEOUT,
    SET_ATIME_MAX_ITEMS, XATTR_CREATE, XATTR_REPLACE,
};

#[test]
fn golden_constants() {
    let mut g = Golden::new("constants");
    let u8s: &[(&str, u8)] = &[
        ("PROTO_MAJOR", PROTO_MAJOR),
        ("FrameKind.Request", FrameKind::Request.as_u8()),
        ("FrameKind.Response", FrameKind::Response.as_u8()),
        ("FrameKind.OneWay", FrameKind::OneWay.as_u8()),
        ("FrameKind.StreamChunk", FrameKind::StreamChunk.as_u8()),
        ("Role.Admin", Role::Admin.as_u8()),
        ("Role.Client", Role::Client.as_u8()),
        ("Role.Server", Role::Server.as_u8()),
        ("REC_V1", REC_V1),
        ("chain_member.SERVING", SERVING), // M9 追加：3.2.8 写了数值的成员状态与 target 状态（0.2 第 11 条）
        ("chain_member.SYNCING", SYNCING),
        ("target_state.UP", TARGET_UP),
        ("target_state.OFFLINE", TARGET_OFFLINE),
        ("KIND_FILE", KIND_FILE),
        ("KIND_DIR", KIND_DIR),
        ("KIND_SYMLINK", KIND_SYMLINK),
        ("KIND_FIFO", KIND_FIFO),
        ("KIND_SOCK", KIND_SOCK),
        ("KIND_CHR", KIND_CHR),
        ("KIND_BLK", KIND_BLK),
        ("GC_ORPHAN", GC_ORPHAN),
        ("GC_EPOCH", GC_EPOCH),
        ("BLK_COMPRESSED", BLK_COMPRESSED),
        ("BLK_ENCRYPTED", BLK_ENCRYPTED),
        ("CK_CRC32C", CK_CRC32C),
        ("CK_XXH3", CK_XXH3),
        ("CK_BLAKE3", CK_BLAKE3),
        ("CMP_NONE", CMP_NONE),
        ("CMP_LZ4", CMP_LZ4),
        ("CMP_ZSTD", CMP_ZSTD),
        ("ENC_NONE", ENC_NONE),
        ("ENC_AES256GCM", ENC_AES256GCM),
        ("RIGHT_READ", RIGHT_READ),
        ("RIGHT_WRITE", RIGHT_WRITE),
        ("RIGHT_ADMIN", RIGHT_ADMIN),
    ];
    for (n, v) in u8s {
        g.add(n, vec![*v]);
    }
    let tags: &[(&str, u8)] = &[
        ("T_INODE", T_INODE),
        ("T_DENTRY", T_DENTRY),
        ("T_XATTR", T_XATTR),
        ("T_INLINE", T_INLINE),
        ("T_DIRATTR", T_DIRATTR),
        ("T_RSTAT", T_RSTAT),
        ("T_QUOTA_STATE", T_QUOTA_STATE),
        ("T_LOCKS", T_LOCKS),
        ("T_SESSION", T_SESSION),
        ("T_DEDUP", T_DEDUP),
        ("T_GC", T_GC),
        ("T_CHANGELOG", T_CHANGELOG),
        ("T_TRASH_INDEX", T_TRASH_INDEX),
        ("T_NODE", T_NODE),
        ("T_TARGET", T_TARGET),
        ("T_CHAIN", T_CHAIN),
        ("T_CLASS", T_CLASS),
        ("T_CLASS_NAME", T_CLASS_NAME),
        ("T_TASK", T_TASK),
        ("T_KEYMETA", T_KEYMETA),
        ("T_DATAKEY", T_DATAKEY),
        ("T_QOS", T_QOS),
        ("T_RECONCILE_Q", T_RECONCILE_Q),
        ("T_CURSOR", T_CURSOR),
        ("T_EVICTED", T_EVICTED),
        ("T_DATAKEY_NAME", T_DATAKEY_NAME),
        ("T_GLOBAL", T_GLOBAL),
        ("T_DEBUG", T_DEBUG),
    ];
    for (n, v) in tags {
        g.add(n, vec![*v]);
    }
    let u16s: &[(&str, u16)] = &[
        ("MSG_HELLO", MSG_HELLO),
        ("MSG_CHALLENGE", MSG_CHALLENGE),
        ("MSG_AUTH", MSG_AUTH),
        ("MSG_WELCOME", MSG_WELCOME),
        ("MSG_PING", MSG_PING),
        ("MSG_META_CALL", MSG_META_CALL),
        ("MSG_DATA_CALL", MSG_DATA_CALL),
        ("MSG_PEER_CALL", MSG_PEER_CALL),
        ("MSG_NODE_CALL", MSG_NODE_CALL),
        ("MSG_RAFT", MSG_RAFT),
        ("MSG_SNAPSHOT_FETCH", MSG_SNAPSHOT_FETCH),
        ("MSG_PLUGIN_FIRST", MSG_PLUGIN_FIRST),
        ("MSG_PLUGIN_LAST", MSG_PLUGIN_LAST),
        ("FLAG_HAS_BULK", FLAG_HAS_BULK),
        ("FLAG_BULK_REMOTE", FLAG_BULK_REMOTE),
        ("FLAG_REPLY_REMOTE", FLAG_REPLY_REMOTE),
        ("FLAG_ERROR", FLAG_ERROR),
        ("FLAG_LAST_CHUNK", FLAG_LAST_CHUNK),
        ("QUOTA_CLASS_TOTAL", QUOTA_CLASS_TOTAL),
        ("CHUNK_FORMAT_VERSION", CHUNK_FORMAT_VERSION),
        ("TARGET_FORMAT_VERSION", TARGET_FORMAT_VERSION),
    ];
    for (n, v) in u16s {
        g.add(n, v.to_le_bytes().to_vec());
    }
    let u32s: &[(&str, u32)] = &[
        ("MAX_BODY", MAX_BODY),
        ("MAX_BULK", MAX_BULK),
        ("F_INLINE", F_INLINE),
        ("F_ORPHAN", F_ORPHAN),
        ("F_HAS_ACL", F_HAS_ACL),
        ("F_TRASH_DIR", F_TRASH_DIR),
        ("INCOMPAT_BLOCK_COMPRESS", INCOMPAT_BLOCK_COMPRESS),
        ("INCOMPAT_BLOCK_ENCRYPT", INCOMPAT_BLOCK_ENCRYPT),
        ("INCOMPAT_KNOWN", INCOMPAT_KNOWN),
        ("RO_COMPAT_KNOWN", RO_COMPAT_KNOWN),
        ("TARGET_INCOMPAT_KNOWN", TARGET_INCOMPAT_KNOWN),
        ("MAX_BLOCK_SIZE", MAX_BLOCK_SIZE),
        ("MIN_CHUNK_SIZE", MIN_CHUNK_SIZE),
        ("MAX_CHUNK_SIZE", MAX_CHUNK_SIZE),
    ];
    for (n, v) in u32s {
        g.add(n, v.to_le_bytes().to_vec());
    }
    let u64s: &[(&str, u64)] = &[
        ("HEADER_LEN", HEADER_LEN as u64),
        ("CHUNK_HEADER_LEN", CHUNK_HEADER_LEN as u64),
        ("BLOCK_ENTRY_LEN", BLOCK_ENTRY_LEN as u64),
        ("TARGET_SB_LEN", TARGET_SB_LEN as u64),
        ("CAP_MAC_LEN", CAP_MAC_LEN as u64),
        ("FEAT_BULK_REMOTE", FEAT_BULK_REMOTE),
        ("FEAT_STRONG", FEAT_STRONG),
        ("FEAT_LOCKS", FEAT_LOCKS),
        ("FEAT_INLINE", FEAT_INLINE),
        ("FEAT_KNOWN", FEAT_KNOWN),
        ("LOCK_EOF", LOCK_EOF),
        ("ROOT_INO", ROOT_INO),
        ("FIRST_INO", FIRST_INO),
        ("DEDUP_WINDOW", DEDUP_WINDOW),
    ];
    for (n, v) in u64s {
        g.add(n, v.to_le_bytes().to_vec());
    }
    let names: &[(&str, &[u8])] = &[
        ("MAGIC", &MAGIC),
        ("CHUNK_MAGIC", &CHUNK_MAGIC),
        ("TARGET_MAGIC", &TARGET_MAGIC),
        ("G_NEXT_INO", G_NEXT_INO.as_bytes()),
        ("G_NEXT_NODE", G_NEXT_NODE.as_bytes()),
        ("G_NEXT_TARGET", G_NEXT_TARGET.as_bytes()),
        ("G_NEXT_CHAIN", G_NEXT_CHAIN.as_bytes()),
        ("G_NEXT_CLASS", G_NEXT_CLASS.as_bytes()),
        ("G_NEXT_KEY", G_NEXT_KEY.as_bytes()),
        ("G_NEXT_DATAKEY", G_NEXT_DATAKEY.as_bytes()),
        ("G_NEXT_CHANGELOG", G_NEXT_CHANGELOG.as_bytes()),
        ("G_NEXT_RECONCILE", G_NEXT_RECONCILE.as_bytes()),
        ("G_ATTR_EPOCH", G_ATTR_EPOCH.as_bytes()),
        ("G_MAP_EPOCH", G_MAP_EPOCH.as_bytes()),
        ("G_CLUSTER", G_CLUSTER.as_bytes()),
        ("G_FEATURES", G_FEATURES.as_bytes()),
        ("G_SETTINGS", G_SETTINGS.as_bytes()),
        ("G_PLUGINS", G_PLUGINS.as_bytes()),
        ("G_SETTINGS_PREV", G_SETTINGS_PREV.as_bytes()), // M9 追加（ADR-0038 第 5 条）
        ("raft_state.KEY_HARD_STATE", KEY_HARD_STATE.as_bytes()),
        ("raft_state.KEY_CONF_STATE", KEY_CONF_STATE.as_bytes()),
        ("raft_state.KEY_SNAP_META", KEY_SNAP_META.as_bytes()),
        ("raft_state.KEY_APPLIED", KEY_APPLIED.as_bytes()),
        ("raft_state.KEY_FORMAT", KEY_FORMAT.as_bytes()),
    ];
    for (n, v) in names {
        g.add(n, v.to_vec());
    }
    for (code, name) in wmfs_api::codes::ALL {
        g.add(&format!("errcode.{name}"), code.to_le_bytes().to_vec());
    }
    for name in RESERVED_FEATURE_NAMES {
        g.add(&format!("feature_name.{name}"), name.as_bytes().to_vec());
    }
    // M1 追加（ADR-0029）：changelog 操作码表 v1，只追加、永不改号
    for (code, name) in wmfs_api::ops::CHANGE_OPS {
        g.add(&format!("changelog_op.{name}"), code.to_le_bytes().to_vec());
    }
    // M4 追加：Rename / SetXattr 的标志（沿用 Linux 的数值）、存进 KV 的 ACL 格式（与 Linux 的 xattr 相同）、root_squash 的 uid
    let m4: &[(&str, u32)] = &[
        ("RENAME_NOREPLACE", RENAME_NOREPLACE),
        ("RENAME_EXCHANGE", RENAME_EXCHANGE),
        ("RENAME_WHITEOUT", RENAME_WHITEOUT),
        ("XATTR_CREATE", XATTR_CREATE),
        ("XATTR_REPLACE", XATTR_REPLACE),
        ("acl.ACL_VERSION", acl::ACL_VERSION),
        ("acl.USER_OBJ", acl::USER_OBJ as u32),
        ("acl.USER", acl::USER as u32),
        ("acl.GROUP_OBJ", acl::GROUP_OBJ as u32),
        ("acl.GROUP", acl::GROUP as u32),
        ("acl.MASK", acl::MASK as u32),
        ("acl.OTHER", acl::OTHER as u32),
        ("NOBODY", wmfs_core::meta::service::NOBODY),
    ];
    for (n, v) in m4 {
        g.add(n, v.to_le_bytes().to_vec());
    }
    g.add("xattr.ACL_ACCESS", ACL_ACCESS.to_vec());
    g.add("xattr.ACL_DEFAULT", ACL_DEFAULT.to_vec());
    // M5 追加：Open.flags 的访问方式（沿用 Linux open(2) 的数值）、codec 编号
    for (n, v) in [("O_RDONLY", O_RDONLY), ("O_WRONLY", O_WRONLY), ("O_RDWR", O_RDWR), ("O_ACCMODE", O_ACCMODE)] {
        g.add(n, v.to_le_bytes().to_vec());
    }
    g.add("CODEC_REPLICA", vec![wmfs_core::hooks::traits::CODEC_REPLICA]);
    // M6 追加：一次 SetAtime 最多几项
    g.add("SET_ATIME_MAX_ITEMS", (SET_ATIME_MAX_ITEMS as u32).to_le_bytes().to_vec());
    g.check();
}
