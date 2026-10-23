//! M6 第一步修的两处 M5 遗留（在完整节点上，客户端走真实的 tcp；FUSE 里换不了 uid，所以用 admin key 声明不同的 uid）：
//! - ADR-0035 第 9 条：按旧的 inline 视图截断（`epoch_seen = 0`、没发 TruncateChunk）一个刚被别人转成 chunk 的文件 →
//!   `STALE_LAYOUT`；重新 Open、按 chunk 截断再 SetAttr → 成功，再扩长读到 0（不会冒出 chunk 里的旧数据）。
//! - ADR-0036（已打开的句柄学 NFS）：属主建 0444 文件后照常写入并提交、写过 64 KiB（Uninline）、续写 cap、经写句柄 ftruncate
//!   （`TruncateHandle`）、Fallocate；按路径 truncate 与写方式的 Open 仍按当前权限 → `EACCES`；其他用户的写句柄在 chmod o-w
//!   之后提交 / 续期 / ftruncate → `EACCES`。

use wmfs_core::base::ErrCode;
use wmfs_core::base::rand::XorShift64;
use wmfs_core::meta::schema::keys::ROOT_INO;
use wmfs_core::meta::schema::records::F_INLINE;
use wmfs_core::proto::meta::{MetaOp, MetaReply, O_RDWR, O_WRONLY, SetAttrIn};
use wmfs_testkit::harness::node::{
    TempDir, chunk_of, client, commit, get, init, opened, put, start, who, write_chunks,
};

fn random(seed: u64, n: usize) -> Vec<u8> {
    let mut v = vec![0u8; n];
    XorShift64::new(seed).fill(&mut v);
    v
}

fn size(size: u64, epoch_seen: u32) -> SetAttrIn {
    SetAttrIn { mode: None, uid: None, gid: None, size: Some(size), atime: None, mtime: None, epoch_seen }
}

fn chmod(m: u32) -> SetAttrIn {
    SetAttrIn { mode: Some(m), size: None, ..size(0, 0) }
}

#[test]
fn probe_truncate_with_a_stale_inline_view_after_uninline() {
    let d = TempDir::new("handle-stale"); // 丢弃时删掉（失败时也删）
    init(&d.0);
    let run = start(&d.0);
    let c = client(&d.0, &run, "admin");
    let me = who(1000, 1000);
    let ino = put(&c, &me, ROOT_INO, "f", b"small").unwrap().attr.ino;
    // A 打开 inline 文件，留着旧视图
    let a = opened(c.meta.read(&me, MetaOp::Open { ino, flags: O_RDWR })).unwrap();
    assert!(a.attr.flags & F_INLINE != 0);
    // B 把它写过 64 KiB：Uninline + 按 chunk 写 + CommitWrite
    let data = random(9, 100_000);
    let b = opened(c.meta.read(&me, MetaOp::Uninline { ino })).unwrap();
    write_chunks(&c, &b, 0, &data).unwrap();
    commit(&c, &me, &b, data.len() as u64, vec![]).unwrap();
    // A 按旧视图截断（以为还是 inline：不发 TruncateChunk，epoch_seen = 0）
    let r = c.meta.write(&me, MetaOp::SetAttr { ino, set: size(10, 0) });
    assert_eq!(r.err(), Some(ErrCode::STALE_LAYOUT), "已经不是 inline：epoch_seen = 0 也要 STALE_LAYOUT");
    let r = c.meta.write(&me, MetaOp::TruncateHandle { ino, size: 10, epoch_seen: 0 });
    assert_eq!(r.err(), Some(ErrCode::STALE_LAYOUT), "TruncateHandle 走同一段代码");
    // A 重新 Open、按 chunk 截断再 SetAttr
    let a = opened(c.meta.read(&me, MetaOp::Open { ino, flags: O_RDWR })).unwrap();
    assert_eq!(a.attr.flags & F_INLINE, 0, "重新 Open 看到的是 chunk 文件");
    let epoch = a.layout.as_ref().unwrap().epoch;
    c.data.truncate(&a.cap, chunk_of(&a, 0), 10).unwrap();
    opened(c.meta.write(&me, MetaOp::SetAttr { ino, set: size(10, epoch) })).unwrap();
    opened(c.meta.write(&me, MetaOp::SetAttr { ino, set: size(100_000, epoch) })).unwrap();
    let got = get(&c, &me, ino).unwrap();
    assert_eq!(&got[..10], &data[..10]);
    assert!(got[10..].iter().all(|x| *x == 0), "扩长之后读到 0：chunk 里 10 之后的旧数据已清零");
    c.meta.close();
    run.stop();
}

#[test]
fn owner_handles_ignore_mode_bits_while_others_get_eacces_after_chmod() {
    let d = TempDir::new("handle-nfs"); // 丢弃时删掉（失败时也删）
    init(&d.0);
    let run = start(&d.0);
    let c = client(&d.0, &run, "admin");
    let (owner, other) = (who(1000, 1000), who(2000, 2000));
    // 属主建 0444 的文件（git、cp 的做法），用建文件时拿到的句柄写入并提交（close）
    let create = MetaOp::Create { parent: ROOT_INO, name: b"ro".to_vec(), mode: 0o444, umask: 0, excl: true };
    let o = opened(c.meta.write(&owner, create)).unwrap();
    let ino = o.attr.ino;
    assert_eq!(o.attr.mode & 0o777, 0o444);
    commit(&c, &owner, &o, 5, vec![(0, b"hello".to_vec())]).expect("属主经写句柄提交：不看权限位");
    // 写过 64 KiB：Uninline 与 CommitWrite 都是句柄操作
    let u = opened(c.meta.read(&owner, MetaOp::Uninline { ino })).expect("属主 Uninline");
    let big = random(3, 70_000);
    write_chunks(&c, &u, 0, &big).unwrap();
    commit(&c, &owner, &u, big.len() as u64, vec![]).unwrap();
    assert_eq!(get(&c, &owner, ino).unwrap(), big, "内容逐字节正确（0444：属主能读）");
    let r = c.meta.read(&owner, MetaOp::RenewCap { ino, write: true });
    assert!(matches!(r, Ok(MetaReply::Cap { .. })), "属主续写 cap 不重查权限位：{r:?}");
    let e = u.layout.as_ref().unwrap().epoch;
    let t = c.meta.write(&owner, MetaOp::TruncateHandle { ino, size: 1000, epoch_seen: e });
    assert_eq!(opened(t).expect("属主经写句柄 ftruncate").attr.size, 1000);
    let fa = c.meta.write(&owner, MetaOp::Fallocate { ino, offset: 0, len: 2000, mode: 0 });
    assert!(fa.is_ok(), "属主经句柄 fallocate：{fa:?}");
    let path = c.meta.write(&owner, MetaOp::SetAttr { ino, set: size(10, e) });
    assert_eq!(path.err(), Some(ErrCode::EACCES), "按路径 truncate 属主自己的 0444 文件仍 EACCES");
    let w = c.meta.read(&owner, MetaOp::Open { ino, flags: O_WRONLY });
    assert_eq!(w.err(), Some(ErrCode::EACCES), "打开本身照常检查：属主不能以写方式打开没有写权限位的已有文件");
    // 其他用户：0666 时打开来写，属主 chmod o-w 之后提交 / 续期 / ftruncate → EACCES
    c.meta.write(&owner, MetaOp::SetAttr { ino, set: chmod(0o666) }).unwrap();
    let theirs = opened(c.meta.read(&other, MetaOp::Open { ino, flags: O_WRONLY })).unwrap();
    commit(&c, &other, &theirs, 2000, vec![]).expect("权限还在时照常提交");
    c.meta.write(&owner, MetaOp::SetAttr { ino, set: chmod(0o664) }).unwrap();
    assert_eq!(commit(&c, &other, &theirs, 3000, vec![]).err(), Some(ErrCode::EACCES), "去掉 o-w 之后提交 EACCES");
    let r = c.meta.read(&other, MetaOp::RenewCap { ino, write: true });
    assert_eq!(r.err(), Some(ErrCode::EACCES), "续期也按当前权限");
    let t = c.meta.write(&other, MetaOp::TruncateHandle { ino, size: 1, epoch_seen: e });
    assert_eq!(t.err(), Some(ErrCode::EACCES), "其他用户经写句柄 ftruncate 也 EACCES");
    let r = c.meta.read(&other, MetaOp::RenewCap { ino, write: false });
    assert!(r.is_ok(), "读 cap 续期：其他人还有 r：{r:?}");
    c.meta.close();
    run.stop();
}
