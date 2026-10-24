//! snap-recv 的排队（`RecvQueue`）：下载期间到的 MsgSnapshot 不丢（M9 验证发现：leader 报告上一次下载失败之后约 1 ms 就重发，常常比
//! 这边的 snap-recv 发现流断了还早；丢掉的话 leader 停在 Snapshot 状态，要等 60 s 的看门狗才再发）。

use super::*;

fn snap(index: u64) -> Message {
    let mut m = Message::default();
    m.set_msg_type(MessageType::MsgSnapshot);
    m.mut_snapshot().mut_metadata().index = index;
    m
}

fn index(m: Option<Message>) -> Option<u64> {
    m.map(|m| m.get_snapshot().get_metadata().index)
}

#[test]
fn a_snapshot_resent_during_a_failing_download_is_downloaded_next() {
    let mut q = RecvQueue::default();
    assert_eq!(index(q.arrive(snap(10))), Some(10), "空闲：马上下载");
    assert_eq!(index(q.arrive(snap(10))), None, "正在下载：记下 leader 报告失败之后重发的那条，不丢");
    assert_eq!(index(q.done(false, 10)), Some(10), "这次下载失败了：接着下载记下的那条");
    assert_eq!(index(q.done(true, 10)), None, "下载好了、没有记下别的：空闲");
    assert_eq!(index(q.arrive(snap(11))), Some(11), "空闲之后到的：马上下载");
}

#[test]
fn only_the_newest_waiting_snapshot_is_kept_and_a_stale_one_is_dropped_after_a_success() {
    let mut q = RecvQueue::default();
    q.arrive(snap(10));
    for i in [11, 12] {
        assert_eq!(index(q.arrive(snap(i))), None);
    }
    assert_eq!(index(q.done(false, 10)), Some(12), "只留最新的一条");
    q.arrive(snap(12));
    assert_eq!(index(q.done(true, 12)), None, "刚下载好的不比它旧：重复的那条丢掉（不再下载一遍）");
    assert_eq!(index(q.arrive(snap(13))), Some(13), "丢掉之后是空闲的");
    q.arrive(snap(14));
    assert_eq!(index(q.done(true, 13)), Some(14), "比刚下载好的新：接着下载");
}
