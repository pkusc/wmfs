//! 会话内存状态 `Live` 的单元测试（3.4.1.10、3.5.6）：新会话算收到过全集；心跳的增量；换过 leader（term 变了）→ 要全集；
//! SessionOpen 的重试不清掉已经知道的打开文件；关闭后忘掉。

use super::*;

#[test]
fn live_sessions_track_open_files_per_term() {
    let live = Live::default();
    live.touch(7, 1, 100);
    assert!(!live.heartbeat(7, 1, 200, vec![(10, true), (11, false)], vec![], None), "新会话：不用发全集");
    live.touch(7, 1, 250); // SessionOpen 的重试
    assert_eq!(live.open_files(7), vec![(10, true), (11, false)], "重试不清掉已经知道的打开文件");
    assert!(!live.heartbeat(7, 1, 300, vec![], vec![10], None));
    assert_eq!(live.open_files(7), vec![(11, false)]);
    assert!(live.heartbeat(7, 2, 400, vec![], vec![], None), "换过 leader：要全集");
    assert!(!live.heartbeat(7, 2, 500, vec![], vec![], Some(vec![(12, true)])), "收到全集");
    assert_eq!(live.open_files(7), vec![(12, true)]);
    assert!(live.heartbeat(8, 2, 500, vec![], vec![], None), "不认识的会话（例如 leader 刚换过）：要全集");
    live.touch(7, 3, 600);
    assert!(live.heartbeat(7, 3, 700, vec![], vec![], None), "旧 term 的状态作废");
    live.forget(7);
    assert_eq!(live.open_files(7), vec![]);
}
