//! 锁表纯函数的单元测试：随机的加锁 / 解锁序列与逐字节的暴力模型对比（3.5.3："约 120 行纯函数，单测与暴力模型对比"），
//! 另有"到文件尾"与区间运算不溢出的几个边界。

use super::*;
use crate::base::rand::XorShift64;

/// 一个字节上谁持有什么锁：(会话, owner) → 写锁吗。
type Byte = std::collections::BTreeMap<(u64, u64), bool>;

const N: u64 = 24; // 模型看 [0, N] 这 N + 1 格：第 N 格代表 [N, 文件尾]（区间的 end 要么 < N，要么是 LOCK_EOF）

fn rec(s: u64, o: u64, start: u64, end: u64, write: bool) -> LockRecV1 {
    LockRecV1 { session: s, owner: o, pid: (s * 10 + o) as u32, start, end, write }
}

/// 区间 → 模型里的格子（见 N 的说明）。
fn cells(start: u64, end: u64) -> std::ops::RangeInclusive<usize> {
    start as usize..=(if end == LOCK_EOF { N } else { end }) as usize
}

/// 暴力模型：每个字节一张表。
fn model_apply(m: &mut [Byte], r: &LockRecV1, unlock: bool) -> bool {
    let (lo, hi) = (*cells(r.start, r.end).start(), *cells(r.start, r.end).end());
    let conflict = (lo..=hi).any(|b| m[b].iter().any(|(k, w)| *k != (r.session, r.owner) && (*w || r.write)));
    if !unlock && conflict {
        return false;
    }
    for byte in m.iter_mut().take(hi + 1).skip(lo) {
        if unlock {
            byte.remove(&(r.session, r.owner));
        } else {
            byte.insert((r.session, r.owner), r.write);
        }
    }
    true
}

/// 锁表按字节展开成模型的样子，并检查不变式（同一所有者不相交、同类不相邻、按 (start, session, owner) 排好、end ≤ LOCK_EOF）。
fn expand(locks: &[LockRecV1]) -> Vec<Byte> {
    let mut m = vec![Byte::new(); N as usize + 1];
    for (i, l) in locks.iter().enumerate() {
        assert!(l.start <= l.end && l.end <= LOCK_EOF, "区间越界：{l:?}");
        for other in &locks[i + 1..] {
            if same_owner(l, other) {
                assert!(!overlaps(l, other), "同一所有者的锁相交：{l:?} {other:?}");
                let adjacent = l.end.checked_add(1) == Some(other.start) || other.end.checked_add(1) == Some(l.start);
                assert!(!(adjacent && l.write == other.write), "同类的相邻锁没有合并：{l:?} {other:?}");
            }
        }
        for b in cells(l.start, l.end) {
            m[b].insert((l.session, l.owner), l.write);
        }
    }
    let mut sorted = locks.to_vec();
    sorted.sort_by_key(|l| (l.start, l.session, l.owner));
    assert_eq!(sorted, locks, "锁表要按 (start, session, owner) 排序");
    m
}

#[test]
fn random_sequences_match_the_byte_model() {
    let mut rng = XorShift64::new(0x9E37_79B9_7F4A_7C15);
    for _round in 0..300 {
        let (mut locks, mut model) = (Vec::new(), vec![Byte::new(); N as usize + 1]);
        for _ in 0..40 {
            let (s, o) = (1 + rng.below(2), rng.below(2));
            let start = rng.below(N);
            let end = if rng.below(5) == 0 { LOCK_EOF } else { start + rng.below(N - start) };
            let (write, unlock) = (rng.below(2) == 0, rng.below(4) == 0);
            let r = rec(s, o, start, end, write);
            let ok = model_apply(&mut model, &r, unlock);
            match apply(&locks, &r, unlock) {
                Ok(t) => {
                    assert!(ok, "模型说冲突、实现却接受了：{r:?} 在 {locks:?}");
                    locks = t;
                }
                Err(e) => {
                    assert!(!ok && e == ErrCode::EAGAIN, "实现拒绝了（{e}），模型却接受：{r:?} 在 {locks:?}");
                }
            }
            assert_eq!(expand(&locks), model, "逐字节不一致：最后一步 {r:?}（解锁 {unlock}）");
            // 到文件尾的锁一直保持 end = LOCK_EOF
            assert!(locks.iter().all(|l| l.end < N || l.end == LOCK_EOF));
        }
    }
}

#[test]
fn eof_ranges_merge_and_split_without_overflow() {
    let a = rec(1, 1, 0, LOCK_EOF, true);
    let t = apply(&[], &a, false).unwrap();
    // 同一所有者在中间改成读锁：写锁被切成两段，最后一段仍到文件尾
    let t = apply(&t, &rec(1, 1, 10, 19, false), false).unwrap();
    assert_eq!(t.len(), 3);
    assert_eq!((t[0].start, t[0].end, t[0].write), (0, 9, true));
    assert_eq!((t[1].start, t[1].end, t[1].write), (10, 19, false));
    assert_eq!((t[2].start, t[2].end, t[2].write), (20, LOCK_EOF, true));
    // 解锁到文件尾：只剩前两段
    let t = apply(&t, &rec(1, 1, 20, LOCK_EOF, false), true).unwrap();
    assert_eq!(t.iter().map(|l| (l.start, l.end)).collect::<Vec<_>>(), vec![(0, 9), (10, 19)]);
    // 别的所有者：与写锁冲突 → EAGAIN；与读锁不冲突（读读共享）
    assert_eq!(apply(&t, &rec(2, 7, 5, 5, false), false), Err(ErrCode::EAGAIN));
    let t2 = apply(&t, &rec(2, 7, 12, LOCK_EOF, false), false).unwrap();
    assert_eq!(conflict(&t2, &rec(3, 1, 100, LOCK_EOF, true)).map(|l| (l.session, l.end)), Some((2, LOCK_EOF)));
    // 解锁不存在的区间永远成功
    assert_eq!(apply(&[], &rec(9, 9, 0, LOCK_EOF, false), true), Ok(vec![]));
    assert_eq!(range(5, u64::MAX), Ok((5, LOCK_EOF)), "≥ i64::MAX 的 end 归一为 LOCK_EOF");
    assert_eq!(range(6, 5), Err(ErrCode::EINVAL));
}
