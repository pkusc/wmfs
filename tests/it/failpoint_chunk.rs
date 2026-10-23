//! 数据路径的 fail point（design.md 3.11、4.9、9.4；每个插件至少一个 fail point 用例，4 章开头），在一个完整的节点上：
//! `chunk.pending.eio`、`chunk.commit.before_rename`（写失败、已提交版本不受影响、pending 删掉了）、`chunk.read.eio`、
//! `chunk.read.corrupt`（读出的字节被翻转一个比特 → 校验和不符 → 单副本 EIO，chunk 标为 Corrupt，整块覆盖写之后恢复）、
//! `chunk.sync.eio`（另有服务端 Uninline 的 SyncChunks 失败 → Uninline 报错、文件仍是 inline、内容不变：清掉 INLINE 之前数据必须
//! 已持久，ADR-0034 第 3 条），以及钩子包装自动拥有的 `plugin.local-fs.chunk_store`、`plugin.crc32c.transform`、`plugin.fifo.qos`、
//! `plugin.replica.codec`（hb 线程的 statfs 也经 chunk_store 的包装，所以这几个用"一直触发、用完清掉"，而不是只触发一次）。
//! fail point 是进程级的：单独一个测试程序，各段按顺序在同一个测试里跑。

use std::time::Duration;

use wmfs_core::api::{failpoint, metrics};
use wmfs_core::base::ErrCode;
use wmfs_core::buf::Lane;
use wmfs_core::meta::schema::keys::ROOT_INO;
use wmfs_core::meta::schema::records::F_INLINE;
use wmfs_core::proto::meta::{MetaOp, MetaReply, O_RDONLY, O_RDWR, OpenOut};
use wmfs_testkit::harness::node::{Client, chunk_of, client, init, opened, put, start, target_dir, who, write_chunks};

fn read0(c: &Client, o: &OpenOut) -> Result<Vec<u8>, ErrCode> {
    let into = c.rpc.pool().get(1 << 20, Lane::Normal, Duration::from_secs(5)).unwrap();
    let l = o.layout.as_ref().unwrap();
    c.data.read(&o.cap, l, chunk_of(o, 0), 0, 1000, into, None).map(|(b, n)| b.as_slice()[..n as usize].to_vec())
}

fn hook_errors(hook: &str, plugin: &str) -> u64 {
    metrics::counter("wmfs_hook_errors_total", "", &[("hook", hook), ("plugin", plugin)]).get()
}

#[test]
fn data_path_fail_points() {
    let d = std::env::temp_dir().join(format!("wmfs-fp-chunk-{}", std::process::id()));
    init(&d);
    let run = start(&d);
    let c = client(&d, &run, "admin");
    let me = who(1000, 1000);
    let v1 = vec![1u8; 100_000];
    let o = put(&c, &me, ROOT_INO, "f", &v1).unwrap();
    let o = opened(c.meta.read(&me, MetaOp::Open { ino: o.attr.ino, flags: O_RDWR })).unwrap();
    assert_eq!(read0(&c, &o).unwrap(), v1[..1000]);

    failpoint::set("chunk.pending.eio", "times(1,return(EIO))").unwrap();
    assert_eq!(write_chunks(&c, &o, 0, &[2; 10]), Err(ErrCode::EIO), "写 pending 失败");
    assert_eq!(read0(&c, &o).unwrap(), v1[..1000], "已提交版本不受影响");

    failpoint::set("chunk.commit.before_rename", "times(1,return(EIO))").unwrap();
    assert_eq!(write_chunks(&c, &o, 0, &[3; 10]), Err(ErrCode::EIO), "rename 之前失败");
    assert_eq!(read0(&c, &o).unwrap(), v1[..1000]);
    assert_eq!(std::fs::read_dir(target_dir(&d).join("p")).unwrap().count(), 0, "失败的 pending 文件删掉了");

    failpoint::set("chunk.read.eio", "times(1,return(EIO))").unwrap();
    assert_eq!(read0(&c, &o), Err(ErrCode::EIO));
    assert_eq!(read0(&c, &o).unwrap(), v1[..1000], "只触发一次");

    failpoint::set("chunk.read.corrupt", "times(1,corrupt)").unwrap();
    assert_eq!(read0(&c, &o), Err(ErrCode::EIO), "读出的字节被翻转：校验和不符，单副本修不了 → EIO");
    assert_eq!(read0(&c, &o), Err(ErrCode::EIO), "chunk 已标为 Corrupt：读一律拒绝（直到重启后重新校验或整块重写）");
    assert_eq!(write_chunks(&c, &o, 0, &[4; 10]), Err(ErrCode::EIO), "Corrupt 的 chunk：要保留旧块的写 → EIO");
    write_chunks(&c, &o, 0, &vec![5u8; 4 << 20]).unwrap(); // 整块覆盖：照常执行，提交后清除标记
    assert_eq!(read0(&c, &o).unwrap(), vec![5u8; 1000]);

    failpoint::set("chunk.sync.eio", "times(1,return(EIO))").unwrap();
    let l = o.layout.as_ref().unwrap();
    assert_eq!(c.data.sync(&o.cap, l.chains[0], vec![chunk_of(&o, 0)]), Err(ErrCode::EIO));
    assert_eq!(c.data.sync(&o.cap, l.chains[0], vec![chunk_of(&o, 0)]), Ok(()));
    // 服务端 Uninline 的 SyncChunks 失败：不清 INLINE（数据还没持久）
    let small = put(&c, &me, ROOT_INO, "s", b"keep me").unwrap();
    failpoint::set("chunk.sync.eio", "times(1,return(EIO))").unwrap();
    assert_eq!(c.meta.read(&me, MetaOp::Uninline { ino: small.attr.ino }).err(), Some(ErrCode::EIO));
    let after = opened(c.meta.read(&me, MetaOp::Open { ino: small.attr.ino, flags: O_RDONLY })).unwrap();
    assert!(after.attr.flags & F_INLINE != 0, "fdatasync 失败：文件仍是 inline");
    assert_eq!(after.inline.as_deref(), Some(&b"keep me"[..]), "内容不变");
    let again = c.meta.read(&me, MetaOp::Uninline { ino: small.attr.ino }).unwrap();
    assert!(matches!(again, MetaReply::Opened(u) if u.attr.flags & F_INLINE == 0), "之后再 Uninline 正常");

    let cases: [(&str, &str, &str); 4] = [
        ("plugin.local-fs.chunk_store", "chunk_store", "local-fs"),
        ("plugin.crc32c.transform", "transform", "crc32c"),
        ("plugin.replica.codec", "codec", "replica"),
        ("plugin.fifo.qos", "qos", "fifo"),
    ];
    for (fp, hook, plugin) in cases {
        let before = hook_errors(hook, plugin);
        failpoint::set(fp, "return(EPERM)").unwrap();
        assert_eq!(write_chunks(&c, &o, 0, &[6; 10]), Err(ErrCode::EPERM), "{fp}：以注入的码失败");
        failpoint::clear(fp);
        assert!(hook_errors(hook, plugin) > before, "{fp}：钩子包装把它计为插件错误");
        assert!(write_chunks(&c, &o, 0, &[6; 10]).is_ok(), "{fp}：清掉之后照常");
    }
    c.meta.close();
    run.stop();
    std::fs::remove_dir_all(&d).unwrap();
}
