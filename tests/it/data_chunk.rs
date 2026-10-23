//! chunk 格式与单节点数据路径（design.md 9.1 的"chunk 格式"一行、3.2.4、3.2.5、3.4.2.2、3.4.2.9），不经 RPC：
//! - 随机的写（多段、跨块、部分块）、截断与读，和一个"直接改 Vec"的参考模型逐字节比对（带种子，失败时打印种子）；
//!   同一组用例在内存版 MemTargetStore 与真实目录上的 local-fs 上各跑一遍；
//! - 头与块表：版本号从 `incarnation << 40` 起单调增加、logical_len、块大小、洞、校验算法；cap 的参数与 chunk 对不上 → EINVAL；
//! - 把 pending 文件在任意字节处截断（掉电后它还留在 p/ 里）后执行启动清理 → p/ 清空、读到的仍是旧版本（不出现损坏读）、之后的写
//!   照常；已提交文件只落盘了一部分 → 判为损坏，绝不返回错误数据；翻转任意比特 → 头、块表与数据区里的比特一律被发现（判为
//!   Corrupt），其余（填充区）不影响读；
//! - Corrupt 的 chunk（R5）：读一律拒绝；要保留任何旧块的写（部分写，或只把别的块整块覆盖）与截断到 s > 0 → EIO，标记仍在；
//!   覆盖整个 chunk、截断到 0 照常执行并清除标记。
//!
//! 种子个数：`WMFS_DATA_SEEDS`（默认 40；MemTargetStore 与 local-fs 各跑这么多个）。

use std::sync::Arc;

use wmfs_core::base::ErrCode;
use wmfs_core::base::rand::XorShift64;
use wmfs_core::data::format::{CHUNK_HEADER_LEN, ChunkHeader, decode_table, table_len_of};
use wmfs_core::hooks::traits::{ChunkStore, TargetStore};
use wmfs_plugins::local_fs::LocalFs;
use wmfs_testkit::harness::data::{MemTargetStore, chunk, node_on, read_at, truncate, write_at};

const CS: u32 = 256 << 10; // 4 个 64 KiB 的块

fn seeds() -> u64 {
    std::env::var("WMFS_DATA_SEEDS").ok().and_then(|s| s.parse().ok()).unwrap_or(40)
}

fn random_bytes(r: &mut XorShift64, n: usize) -> Vec<u8> {
    let mut v = vec![0u8; n];
    r.fill(&mut v);
    v
}

/// 一个随机操作序列：每步之后读一个随机区间，与参考模型比对；版本号严格递增。
fn run_model(store: Arc<dyn TargetStore>, seed: u64) {
    let node = node_on(store, 1);
    let mut r = XorShift64::new(seed);
    let mut model: Vec<Vec<u8>> = vec![vec![]; 3]; // 每个 chunk 的内容（长度 = logical_len）
    let mut last_version = [0u64; 3];
    for step in 0..60 {
        let idx = r.below(3) as u32;
        let m = &mut model[idx as usize];
        let ctx = format!("种子 {seed} 第 {step} 步（chunk {idx}）");
        let v = if r.below(5) == 0 {
            let new_len = if r.below(3) == 0 { 0 } else { r.below(CS as u64 + 1) as u32 };
            let v = truncate(&node, CS, idx, new_len).unwrap_or_else(|e| panic!("{ctx}：截断失败 {e}"));
            m.truncate(new_len as usize);
            if v == 0 {
                assert_eq!(last_version[idx as usize], 0, "{ctx}：只有从未写过的 chunk 截断才回 0");
                continue;
            }
            v
        } else {
            let mut extents = Vec::new();
            let mut data = Vec::new();
            for _ in 0..1 + r.below(3) {
                let off = r.below(CS as u64) as u32;
                let len = 1 + r.below((CS - off).min(150_000) as u64) as u32;
                let d = random_bytes(&mut r, len as usize);
                if m.len() < (off + len) as usize {
                    m.resize((off + len) as usize, 0);
                }
                m[off as usize..(off + len) as usize].copy_from_slice(&d);
                extents.push((off, len));
                data.extend_from_slice(&d);
            }
            write_at(&node, CS, idx, &extents, &data).unwrap_or_else(|e| panic!("{ctx}：写失败 {e}"))
        };
        assert!(v > last_version[idx as usize], "{ctx}：版本号必须单调递增（{v} ≤ {}）", last_version[idx as usize]);
        last_version[idx as usize] = v;
        let off = r.below(CS as u64) as u32;
        let len = r.below((CS - off) as u64 + 1) as u32;
        let (got, ver) = read_at(&node, idx, off, len).unwrap_or_else(|e| panic!("{ctx}：读失败 {e}"));
        let mut want = m.get(off as usize..).unwrap_or_default().to_vec();
        want.resize(len as usize, 0);
        want.truncate(len as usize);
        assert_eq!(ver, v, "{ctx}：读到的是刚提交的版本");
        assert!(got == want, "{ctx}：读 [{off}, +{len}) 与参考模型不一致");
    }
}

#[test]
fn random_writes_truncates_and_reads_match_a_reference_model() {
    for seed in 1..=seeds() {
        run_model(Arc::new(MemTargetStore::new()), seed);
    }
}

#[test]
fn the_same_model_runs_on_real_local_fs() {
    let dir = std::env::temp_dir().join(format!("wmfs-data-chunk-{}", std::process::id()));
    for seed in 1..=seeds() {
        let _ = std::fs::remove_dir_all(&dir);
        let store = LocalFs::new().open_target(&dir, 1).unwrap();
        run_model(store, seed);
    }
    std::fs::remove_dir_all(&dir).unwrap();
}

/// 已提交文件的头与块表。
fn header_of(store: &MemTargetStore, idx: u32) -> (ChunkHeader, Vec<wmfs_core::data::format::BlockEntry>) {
    let b = store.committed_bytes(&chunk(idx)).unwrap();
    let fixed: [u8; CHUNK_HEADER_LEN] = b[..CHUNK_HEADER_LEN].try_into().unwrap();
    let table = &b[CHUNK_HEADER_LEN..CHUNK_HEADER_LEN + table_len_of(&fixed)];
    (ChunkHeader::decode(&fixed, table).unwrap(), decode_table(table).unwrap())
}

#[test]
fn header_versions_holes_and_layout_params() {
    let store = MemTargetStore::new();
    let node = node_on(Arc::new(store.clone()), 1);
    assert_eq!(write_at(&node, CS, 0, &[(70_000, 10)], &[7; 10]), Ok(1 << 40), "第一个版本 = incarnation 1 << 40");
    let (h, t) = header_of(&store, 0);
    assert_eq!(
        (h.chunk_version, h.logical_len, h.block_size, h.nblocks, h.checksum_alg),
        (1 << 40, 70_010, 65536, 4, 1)
    );
    assert_eq!((h.ino, h.epoch, h.chunk_idx, h.chain_id), (18, 1, 0, 1));
    assert_eq!(t.iter().map(|e| e.stored_len).collect::<Vec<_>>(), vec![0, 70_010 - 65536, 0, 0], "只有被写的块有数据");
    assert_eq!(read_at(&node, 0, 0, 70_010).unwrap().0[..65536], [0u8; 65536][..], "洞读为 0");
    assert_eq!(write_at(&node, CS, 0, &[(65_536, 10)], &[8; 10]), Ok((1 << 40) + 1));
    assert_eq!(truncate(&node, CS, 0, 65_540), Ok((1 << 40) + 2), "截断也是一个新版本");
    let (h, t) = header_of(&store, 0);
    assert_eq!((h.logical_len, t[1].plain_len), (65_540, 4), "边界块重新编码，之后的部分丢掉");
    assert_eq!(read_at(&node, 0, 65_536, 10).unwrap().0, [8, 8, 8, 8, 0, 0, 0, 0, 0, 0], "截断点之后读为 0");
    assert_eq!(write_at(&node, CS, 0, &[(65_550, 2)], &[9; 2]), Ok((1 << 40) + 3));
    assert_eq!(
        read_at(&node, 0, 65_536, 16).unwrap().0,
        [8, 8, 8, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 9, 9],
        "截掉的部分不会再冒出来"
    );
    assert_eq!(truncate(&node, CS, 1, 100), Ok(0), "截断从未写过的 chunk：什么也不做");
    assert_eq!(read_at(&node, 1, 0, 10), Err(ErrCode::CHUNK_NOT_FOUND));
    let node3 = node_on(Arc::new(store.clone()), 3);
    assert_eq!(write_at(&node3, CS, 0, &[(0, 1)], &[1]), Ok(3 << 40), "唯一幸存者换了 incarnation：版本跳到 3 << 40");
    let mut bad = wmfs_testkit::harness::data::cap(CS);
    bad.chunk_size = 128 << 10;
    let j = wmfs_core::data::write::WriteJob {
        chunk: chunk(0),
        target: 1,
        cap: bad,
        kind: wmfs_core::data::write::JobKind::Truncate { new_len: 0 },
        bulk: None,
        reply: None,
    };
    assert_eq!(wmfs_core::data::write::run(&node3, &j), Err(ErrCode::EINVAL), "cap 的 chunk_size 与这个 chunk 不同");
}

/// 写一个 v1（全部块有数据）并让它落盘，再在一个独立的存储里做出完整的 v2 文件（拿来截断成"写了一半的 pending"）。
fn v1_and_v2() -> (MemTargetStore, Vec<u8>, Vec<u8>) {
    let store = MemTargetStore::new();
    let node = node_on(Arc::new(store.clone()), 1);
    let v1 = random_bytes(&mut XorShift64::new(7), CS as usize);
    write_at(&node, CS, 0, &[(0, CS)], &v1).unwrap();
    wmfs_testkit::harness::data::sync(&node, &[0]);
    let other = MemTargetStore::new();
    let n2 = node_on(Arc::new(other.clone()), 1);
    write_at(&n2, CS, 0, &[(0, CS)], &v1).unwrap();
    write_at(&n2, CS, 0, &[(1000, 5000)], &[9; 5000]).unwrap();
    (store, v1, other.committed_bytes(&chunk(0)).unwrap())
}

#[test]
fn a_pending_file_cut_at_any_byte_is_cleaned_up_and_never_read() {
    let (store, v1, v2_file) = v1_and_v2();
    let mut r = XorShift64::new(11);
    let mut cuts: Vec<usize> = (0..300).map(|_| r.below(v2_file.len() as u64) as usize).collect();
    cuts.extend([0, 1, CHUNK_HEADER_LEN, 4096, v2_file.len() - 1, v2_file.len()]);
    for k in cuts {
        store.put_pending(&chunk(0), v2_file[..k].to_vec()); // 写到第 k 个字节时掉电
        let after = store.crash();
        assert_eq!(after.pending_count(), 1, "掉电之后 p/ 里写了一半的 pending 文件还在");
        after.clear_temp().unwrap(); // 启动清理（R5）
        assert_eq!(after.pending_count(), 0, "启动清理把它扔掉");
        let node = node_on(Arc::new(after), 1);
        assert_eq!(read_at(&node, 0, 0, CS).unwrap().0, v1, "pending 截在第 {k} 字节：读到的仍是落盘的旧版本");
        write_at(&node, CS, 0, &[(0, 10)], &[k as u8; 10]).unwrap();
        let mut want = v1.clone();
        want[..10].fill(k as u8);
        assert_eq!(read_at(&node, 0, 0, CS).unwrap().0, want, "之后的写照常（残留的 pending 不影响它）");
    }
}

#[test]
fn a_committed_file_that_only_partly_reached_the_disk_is_reported_not_returned() {
    let (store, v1, _) = v1_and_v2();
    let len = store.committed_len(&chunk(0)).unwrap();
    let mut r = XorShift64::new(13);
    let mut cuts: Vec<usize> = (0..300).map(|_| r.below(len as u64) as usize).collect();
    cuts.extend([0, 255, 256, 600, 4096, len - 1]);
    for k in cuts {
        let node = node_on(Arc::new(store.crash_torn(&chunk(0), k)), 1);
        match read_at(&node, 0, 0, CS) {
            Ok((got, _)) => assert_eq!(got, v1, "只落了前 {k} 字节：要么报错，要么是完整的旧数据"),
            Err(e) => assert!(e == ErrCode::CORRUPT || e == ErrCode::CHECKSUM_MISMATCH, "第 {k} 字节：{e}"),
        }
    }
}

#[test]
fn every_flipped_bit_in_header_table_or_data_is_caught() {
    let (store, v1, _) = v1_and_v2();
    let (h, t) = header_of(&store, 0);
    let meaningful = |byte: usize| {
        byte < CHUNK_HEADER_LEN + t.len() * 72
            || (0..t.len()).any(|i| {
                let a = h.block_offset(i as u32) as usize;
                byte >= a && byte < a + t[i].stored_len as usize
            })
    };
    let len = store.committed_len(&chunk(0)).unwrap();
    let mut r = XorShift64::new(17);
    for i in 0..1500 {
        let bit =
            if i < 300 { r.below(((CHUNK_HEADER_LEN + 4 * 72) * 8) as u64) } else { r.below(len as u64 * 8) } as usize;
        store.flip_bit(&chunk(0), bit);
        let node = node_on(Arc::new(store.clone()), 1);
        let got = read_at(&node, 0, 0, CS);
        store.flip_bit(&chunk(0), bit); // 翻回来
        match got {
            Ok((d, _)) => {
                assert!(!meaningful(bit / 8), "翻转第 {bit} 个比特（第 {} 字节）没有被发现", bit / 8);
                assert_eq!(d, v1, "填充区里的比特不影响数据");
            }
            Err(e) => assert!(e == ErrCode::CORRUPT || e == ErrCode::CHECKSUM_MISMATCH, "第 {bit} 个比特：{e}"),
        }
    }
}

#[test]
fn corrupt_chunks_refuse_reads_and_keep_old_block_writes_until_rewritten() {
    let (store, v1, _) = v1_and_v2();
    let node = node_on(Arc::new(store.clone()), 1);
    let (h, _) = header_of(&store, 0);
    store.flip_bit(&chunk(0), (h.block_offset(2) as usize + 100) * 8); // 第 2 块的数据坏了
    assert_eq!(read_at(&node, 0, 0, 1000).map(|r| r.0), Ok(v1[..1000].to_vec()), "没读到坏块之前照常");
    assert_eq!(read_at(&node, 0, 0, CS), Err(ErrCode::CHECKSUM_MISMATCH), "第一次读到坏块：校验和不符");
    assert_eq!(read_at(&node, 0, 0, 10), Err(ErrCode::CORRUPT), "之后整个 chunk 的读一律拒绝");
    assert_eq!(write_at(&node, CS, 0, &[(0, 10)], &[1; 10]), Err(ErrCode::EIO), "部分写要保留旧块：单副本修不了");
    let block0 = vec![1u8; 65536];
    let r = write_at(&node, CS, 0, &[(0, 65536)], &block0);
    assert_eq!(r, Err(ErrCode::EIO), "只把第 0 块整块覆盖：第 1–3 块（含坏的第 2 块）要原样保留，R5 不许");
    assert_eq!(read_at(&node, 0, 0, 10), Err(ErrCode::CORRUPT), "标记仍在");
    assert_eq!(truncate(&node, CS, 0, 100), Err(ErrCode::EIO), "截到 s > 0 要保留旧块");
    assert!(truncate(&node, CS, 0, 0).is_ok(), "截断到 0 不需要旧块：照常执行");
    assert_eq!(read_at(&node, 0, 0, 10).map(|r| r.0), Ok(vec![0; 10]), "提交新版本后清除 Corrupt 标记");
    // 头坏了：只接受整个 chunk 的覆盖写
    let node2 = node_on(Arc::new(store.clone()), 1);
    write_at(&node2, CS, 1, &[(0, 100)], &[5; 100]).unwrap();
    store.flip_bit(&chunk(1), 30 * 8); // 固定头里的 ino 字段
    assert_eq!(read_at(&node2, 1, 0, 10), Err(ErrCode::CORRUPT));
    assert_eq!(write_at(&node2, CS, 1, &[(0, 10)], &[1; 10]), Err(ErrCode::EIO), "不知道旧块在哪：部分写只能拒绝");
    let full = vec![3u8; CS as usize];
    assert!(write_at(&node2, CS, 1, &[(0, CS)], &full).is_ok(), "覆盖整个 chunk 照常");
    assert_eq!(read_at(&node2, 1, 0, CS).map(|r| r.0), Ok(full));
}

/// 内存后端下模拟掉电（9.3、roadmap M5 的验收）：随机的写、截断与 fsync（SyncChunks）之后随时掉电；重启（启动清理）之后，
/// 每个 chunk 的内容正好是它最后一次 fsync 时的内容（fsync 过的数据不丢），从没 fsync 过的读不到（CHUNK_NOT_FOUND）——
/// 绝不会读到别的东西。
#[test]
fn power_loss_keeps_exactly_what_was_fsynced() {
    for seed in 1..=seeds() {
        let mut r = XorShift64::new(seed);
        let store = MemTargetStore::new();
        let node = node_on(Arc::new(store.clone()), 1);
        let mut model: Vec<Vec<u8>> = vec![vec![]; 2];
        let mut synced: Vec<Option<Vec<u8>>> = vec![None; 2];
        let crash_at = 5 + r.below(40);
        for _ in 0..crash_at {
            let idx = r.below(2) as u32;
            let m = &mut model[idx as usize];
            match r.below(6) {
                0 => {
                    wmfs_testkit::harness::data::sync(&node, &[idx]);
                    if store.committed_len(&chunk(idx)).is_some() {
                        synced[idx as usize] = Some(m.clone());
                    }
                }
                1 => {
                    let n = r.below(CS as u64 + 1) as u32;
                    if truncate(&node, CS, idx, n).unwrap() > 0 {
                        m.truncate(n as usize);
                    }
                }
                _ => {
                    let off = r.below(CS as u64 - 1) as u32;
                    let len = 1 + r.below((CS - off) as u64) as u32;
                    let d = random_bytes(&mut r, len as usize);
                    write_at(&node, CS, idx, &[(off, len)], &d).unwrap();
                    if m.len() < (off + len) as usize {
                        m.resize((off + len) as usize, 0);
                    }
                    m[off as usize..(off + len) as usize].copy_from_slice(&d);
                }
            }
        }
        let after = store.crash();
        after.clear_temp().unwrap();
        let node = node_on(Arc::new(after), 1);
        for idx in 0..2u32 {
            let got = read_at(&node, idx, 0, CS);
            match &synced[idx as usize] {
                None => assert_eq!(got.err(), Some(ErrCode::CHUNK_NOT_FOUND), "种子 {seed} chunk {idx}：没 fsync 过"),
                Some(want) => {
                    let mut want = want.clone();
                    want.resize(CS as usize, 0);
                    assert!(got.unwrap().0 == want, "种子 {seed} chunk {idx}：掉电后应正好是最后一次 fsync 时的内容");
                }
            }
        }
    }
}
