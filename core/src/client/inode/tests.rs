//! InodeState 的纯逻辑：属性只前进；"见过的 change_id"只在恰好 + 1 时前进，重新 Open 不前进它；inline 副本带着版本；
//! layout 的 epoch 变了作废 cap 与"还没 SyncChunks"的 chunk；io 权一次只给一个人。

use super::*;
use crate::base::types::Ts;

fn attr(change_id: u64, size: u64, inline: bool) -> AttrOut {
    let t = Ts::default();
    AttrOut {
        ino: 5,
        kind: 1,
        mode: 0o644,
        uid: 1000,
        gid: 1000,
        nlink: 1,
        size,
        atime: t,
        mtime: t,
        ctime: t,
        btime: t,
        rdev: 0,
        change_id,
        epoch: 1,
        consistency: 1,
        ttl_ms: 1000,
        flags: if inline { F_INLINE } else { 0 },
    }
}

fn out(a: AttrOut, epoch: u32, inline: Option<&[u8]>) -> OpenOut {
    let layout = LayoutV1 {
        epoch,
        chunk_size: 4 << 20,
        block_size: 64 << 10,
        chains: vec![1],
        replicas: 1,
        codec: 1,
        checksum: 1,
        compress: 0,
        compress_level: 0,
        encrypt: 0,
        encrypt_key: 0,
        inline_max: 64 << 10,
    };
    OpenOut {
        attr: a,
        layout: Some(layout),
        cap: vec![],
        inline: inline.map(|d| d.to_vec()),
        quota_end: 0,
        atime_due: false,
    }
}

#[test]
fn attrs_only_move_forward_and_seen_needs_exactly_plus_one() {
    let mut st = InodeState::new(attr(5, 0, false));
    st.seen_change_id = 5;
    st.mine(&attr(7, 200, false)); // 中间夹着别人的修改（6）：不算见过
    assert_eq!((st.seen_change_id, st.attr.change_id, st.attr.size), (5, 7, 200));
    st.mine(&attr(6, 100, false)); // 晚到的旧回复：属性不往回走（"见过的"按 + 1 规则只会保守地更旧）
    assert_eq!((st.attr.change_id, st.attr.size), (7, 200));
    st.install(&out(attr(9, 0, false), 2, None)); // STALE_LAYOUT 之后的重新 Open：换上新的，不前进"见过的"
    assert_eq!((st.attr.change_id, st.seen_change_id), (9, 6));
    st.install(&out(attr(8, 50, false), 3, None)); // 更旧的 OpenOut：整个不要（连 layout）
    assert_eq!((st.attr.change_id, st.layout.as_ref().map(|l| l.epoch)), (9, Some(2)));
}

#[test]
fn a_new_epoch_drops_the_caps_and_the_unsynced_chunks() {
    let mut st = InodeState::new(attr(1, 0, false));
    st.install(&out(attr(1, 0, false), 1, None));
    st.wcap = Cap::new(vec![1, 2, 3]);
    st.unsynced.insert(3);
    st.install(&out(attr(2, 0, false), 1, None)); // 同一个 epoch：都留着
    assert!(!st.wcap.bytes.is_empty() && st.unsynced.contains(&3));
    st.install(&out(attr(3, 0, true), 2, Some(b""))); // 截断到 0：新 epoch
    assert!(st.wcap.bytes.is_empty() && st.unsynced.is_empty(), "旧 epoch 的 cap 与 chunk 都不再有用");
}

#[test]
fn the_inline_copy_carries_its_version() {
    let mut st = InodeState::new(attr(1, 0, true));
    st.install(&out(attr(2, 3, true), 1, Some(b"abc")));
    assert_eq!(st.inline_now().map(|v| v.as_slice()), Some(&b"abc"[..]));
    st.install(&out(attr(4, 6, true), 1, None)); // 只写打开：属性新了，回复不带内容
    assert!(st.inline_now().is_none(), "副本是 2 的、属性是 4 的：不知道当前内容");
    st.install(&out(attr(4, 6, true), 1, Some(b"abcdef")));
    st.patch_inline(&attr(5, 7, true), &[(6, b"X".to_vec())]); // 自己的提交，恰好 + 1：补丁打在副本上
    st.mine(&attr(5, 7, true));
    assert_eq!(st.inline_now().map(|v| v.as_slice()), Some(&b"abcdefX"[..]));
    st.patch_inline(&attr(7, 8, true), &[(7, b"Y".to_vec())]); // 中间夹着别人的 6：副本留在 5
    st.mine(&attr(7, 8, true));
    assert!(st.inline_now().is_none());
}

#[test]
fn io_is_given_to_one_holder_at_a_time() {
    let inode = Inode::new(attr(1, 0, false));
    let g = inode.io();
    assert!(inode.try_io().is_none(), "别人拿着：flusher 这一轮跳过");
    drop(g);
    assert!(inode.try_io().is_some());
}
