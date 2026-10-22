//! fail point `meta.reply.drop`（design.md 3.11、9.4"客户端重试恰好一次"；roadmap M4 的验收标准"服务端'弄丢'一次回复时，
//! 客户端重试得到原结果而不是 EEXIST"）：服务端执行完 mkdir、批次提交之后吞掉回复；客户端等一个 RPC 超时（这里 1 s）后用
//! 同一个 seq 重试，去重表返回原结果。之后同名的新 mkdir（新的 seq）照常得到 EEXIST；只读请求、会话的关闭与打开不受这个
//! fail point 影响。最后一段是 MetaClient 的在途窗口按 seq 计（3.4.1.4 规则 4）：一个请求卡在超时重试里的时候，同一会话里
//! 别的线程做完 300 个修改，它的去重记录也不会被挤掉（服务端写 seq 时点删 seq − 256）。
//! fail point 是进程级的，所以单独一个测试程序，几段按顺序在同一个测试里跑。

use std::sync::Arc;
use std::time::{Duration, Instant};

use wmfs_core::api::{failpoint, metrics};
use wmfs_core::base::ErrCode;
use wmfs_core::meta::schema::keys::ROOT_INO;
use wmfs_core::proto::meta::{MetaOp, MetaReply};
use wmfs_testkit::harness::meta::{bootstrap, engine, plugins};
use wmfs_testkit::harness::meta_rpc::{caller, meta_client, meta_server};

fn mk(name: &str) -> MetaOp {
    MetaOp::Mkdir { parent: ROOT_INO, name: name.as_bytes().to_vec(), mode: 0o755, umask: 0 }
}

fn hits() -> u64 {
    metrics::counter("wmfs_failpoint_hits_total", "fail point 触发次数", &[("name", "meta.reply.drop")]).get()
}

fn dedup() -> u64 {
    metrics::counter("wmfs_meta_dedup_hits_total", "", &[]).get()
}

#[test]
fn a_dropped_reply_is_retried_with_the_same_seq_and_hits_the_dedup_table() {
    let eng = engine(&bootstrap(&[1]), plugins());
    let s = meta_server(eng.clone(), None);
    let c = meta_client(std::slice::from_ref(&s.ep), 1, Duration::from_secs(1));
    let who = caller(1000, 1001);

    failpoint::set("meta.reply.drop", "times(1,return(EIO))").unwrap();
    let t0 = Instant::now();
    let r = c.write(&who, mk("b"));
    let took = t0.elapsed();
    let MetaReply::Entry { attr } = r.unwrap() else { panic!("mkdir 应该成功（不是 File exists）") };
    assert!(took >= Duration::from_secs(1), "等了一个 RPC 超时才重试：{took:?}");
    assert_eq!((hits(), dedup()), (1, 1), "回复被吞了一次；重试命中去重表");
    assert_eq!(attr.kind, 2);
    let back = c.read(&who, MetaOp::Lookup { parent: ROOT_INO, name: b"b".to_vec() }).unwrap();
    assert!(matches!(back, MetaReply::Entry { attr: a } if a.ino == attr.ino), "只建了一个目录");
    assert_eq!(c.write(&who, mk("b")), Err(ErrCode::EEXIST), "新的 seq：照常执行");

    failpoint::set("meta.reply.drop", "return(EIO)").unwrap();
    let t0 = Instant::now();
    assert!(matches!(c.read(&who, MetaOp::GetAttr { ino: attr.ino }), Ok(MetaReply::Entry { .. })));
    c.close(); // 会话的关闭与打开不经去重表，也不受影响
    let c = meta_client(std::slice::from_ref(&s.ep), 1, Duration::from_secs(3));
    assert!(t0.elapsed() < Duration::from_millis(900), "只读请求、关闭与打开会话的回复都没有被吞");
    failpoint::clear("meta.reply.drop");
    assert_eq!(hits(), 1);

    // 在途窗口按 seq 计：卡在超时重试里的请求（它的 seq 最小）挡住"seq ≥ 它 + 256"的新请求，直到它结束
    let c = Arc::new(c);
    failpoint::set("meta.reply.drop", "times(1,return(EIO))").unwrap();
    let stuck = {
        let (c, who) = (c.clone(), who.clone());
        std::thread::spawn(move || c.write(&who, mk("stuck")))
    };
    let deadline = Instant::now() + Duration::from_secs(10);
    while hits() < 2 {
        assert!(Instant::now() < deadline, "10 s 内 stuck 的回复应该被吞掉");
        std::thread::sleep(Duration::from_millis(5));
    }
    for i in 0..300 {
        let r = c.write(&who, mk(&format!("n{i}")));
        assert!(matches!(r, Ok(MetaReply::Entry { .. })), "第 {i} 个：{r:?}");
    }
    let r = stuck.join().unwrap();
    assert!(matches!(r, Ok(MetaReply::Entry { .. })), "卡住的请求重试时仍命中去重表（不是 File exists）：{r:?}");
    assert_eq!((hits(), dedup()), (2, 2));
    c.close();
    eng.shutdown();
}
