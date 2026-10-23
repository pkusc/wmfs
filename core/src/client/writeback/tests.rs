//! 写回缓冲的纯函数：区间合并、本地末尾、把本地数据盖到读结果上（先在途、后脏区）、槽位上限按池算。

use super::*;
use crate::buf::PoolConfig;

#[test]
fn merge_extent_keeps_sorted_disjoint_ranges() {
    let mut v = vec![];
    merge_extent(&mut v, 10, 5);
    merge_extent(&mut v, 0, 3);
    assert_eq!(v, [(0, 3), (10, 5)]);
    merge_extent(&mut v, 3, 7); // 正好把两段连起来
    assert_eq!(v, [(0, 15)]);
    merge_extent(&mut v, 20, 1);
    merge_extent(&mut v, 14, 3); // 与前一段重叠
    assert_eq!(v, [(0, 17), (20, 1)]);
    merge_extent(&mut v, 0, 100); // 盖住全部
    assert_eq!(v, [(0, 100)]);
}

fn chunk(pool: &MemPool, cs: usize, ext: &[(u32, u32)], fill: u8) -> DirtyChunk {
    let mut buf = pool.get(cs, Lane::Normal, Duration::from_secs(1)).unwrap();
    buf.as_mut_slice().fill(fill);
    DirtyChunk { buf, extents: ext.to_vec() }
}

#[test]
fn local_end_and_overlay_take_newer_data_over_flying_data() {
    let pool = MemPool::new(&PoolConfig { s_mb: 1, m_slots: 2, l_slots: 1, repl_lane: false }).unwrap();
    let cs = 64u32; // 每个 chunk 64 字节（S 档）
    let mut d = DirtyFile::default();
    assert!(d.is_empty() && d.local_end(cs) == 0);
    let old = chunk(&pool, 64, &[(0, 40)], b'a');
    d.flying.push(Flying { idx: 1, data: old.buf.freeze(), extents: old.extents });
    d.chunks.insert(1, chunk(&pool, 64, &[(30, 20)], b'b'));
    d.chunks.insert(3, chunk(&pool, 64, &[(0, 4)], b'c'));
    assert_eq!(d.local_end(cs), 3 * 64 + 4, "本地写到的末尾：最后一个 chunk 的最后一段");
    let (mut out, mut mask) = (vec![0u8; 80], vec![false; 80]);
    d.overlay(cs, 60, &mut out, &mut mask); // 文件内 [60, 140)
    assert_eq!(&out[4..34], &[b'a'; 30][..], "在途的数据 [64, 94)");
    assert_eq!(&out[34..54], &[b'b'; 20][..], "脏区盖在途：[94, 114) 是后写的");
    assert!(mask[4..54].iter().all(|m| *m) && !mask[..4].iter().any(|m| *m) && !mask[54..].iter().any(|m| *m));
}

#[test]
fn slots_are_sixty_percent_of_the_m_and_l_slots() {
    let pool = MemPool::new(&PoolConfig { s_mb: 1, m_slots: 9, l_slots: 1, repl_lane: false }).unwrap();
    let s = Slots::for_pool(&pool);
    assert_eq!(s.max, 6, "(9 + 1) × 60%");
    assert!((0..6).all(|_| s.try_take()) && !s.try_take(), "占满 6 个之后再要就没有了");
    s.release(6);
    assert_eq!(s.used(), 0);
}
